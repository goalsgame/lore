// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use lore_base::lore_spawn_core;
use lore_base::lore_spawn_net;
use lore_base::runtime::LORE_CONTEXT;
use lore_revision::runtime::execution_context;
use lore_transport::quic::net_runtime::NetRuntime;
use opentelemetry::KeyValue;
use quinn::AckFrequencyConfig;
use quinn::EndpointConfig;
use quinn::ServerConfig;
use quinn::VarInt;
use quinn::crypto::rustls::HandshakeData;
use quinn::crypto::rustls::QuicServerConfig;
use socket2::Domain;
use socket2::Protocol;
use socket2::Type;
use tokio_metrics::TaskMonitor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::debug;
use tracing::info;
use tracing::info_span;
use tracing::trace;
use tracing::warn;

use crate::protocol::attribute_map::AttributeMap;
use crate::protocol::attribute_map::ConnectionId;
use crate::quic::StreamDataHandler;
use crate::quic::StreamHandlerFactory;
use crate::quic::initial_cwnd::InitialCwndBytes;
use crate::quic::quinn::config::QuinnConfig;
use crate::quic::quinn::config::crypto_config;
use crate::quic::quinn::metrics::track_connection_stats;
use crate::telemetry::OtelTokioTaskMetrics;
use crate::util::cert_metrics::parse_certificate_info;
use crate::util::cert_metrics::start_certificate_metrics;

const METRICS_QUINN_SERVER_LABEL: &str = "quinn_server_name";

/// How often a draining connection checks whether its last request has been answered.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Application close code and reason sent to peers when the server shuts down.
const SHUTDOWN_CLOSE_CODE: u32 = 0;
const SHUTDOWN_CLOSE_REASON: &[u8] = b"server shutting down";

pub struct QuinnServer {
    endpoint: quinn::Endpoint,
    /// Cancelled when the server starts draining: connections stop reading new requests.
    draining: CancellationToken,
    /// One task per accepted connection, each ending once its connection is closed.
    connections: TaskTracker,
}

impl QuinnServer {
    pub fn start(mut settings: QuinnConfig) -> anyhow::Result<Self> {
        let span = info_span!(
            "quinn_server",
            quinn_server_name = settings.server_metrics_name
        );
        let _guard = span.enter();
        info!("Initializing quinn quic server from {settings:?}");

        // Initialize certificate expiry metrics
        let cert_path = settings
            .cert_chain
            .clone()
            .unwrap_or(settings.cert_file.clone());
        if let Some(cert_info) = parse_certificate_info(&cert_path) {
            info!(
                cert_path = %cert_info.cert_path.display(),
                subject = %cert_info.subject,
                serial = %cert_info.serial,
                expiry_timestamp = cert_info.expiry_timestamp,
                "Parsed certificate for expiry metrics"
            );
            start_certificate_metrics(cert_info, settings.metrics_frequency);
        } else {
            warn!(
                cert_path = %cert_path.display(),
                "Could not parse certificate for expiry metrics, skipping"
            );
        }

        let endpoint_config = EndpointConfig::default();
        let mut server_config = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(
            crypto_config(&settings)?,
        )?));

        if let Some(transport_config) = Arc::get_mut(&mut server_config.transport) {
            transport_config.max_concurrent_uni_streams(0_u8.into());
            transport_config
                .max_concurrent_bidi_streams(VarInt::from_u64(settings.max_bidi_streams)?);
            transport_config.max_idle_timeout(Some(settings.idle_timeout.try_into()?));
            transport_config.keep_alive_interval(Some(settings.keep_alive));

            let bandwidth_estimate = settings.transport_bits_per_second / 8;
            let recv_window = (bandwidth_estimate / 1000) * settings.transport_rtt;
            // Any stream can use at most 25% of available recv window
            let stream_recv_window = recv_window / 4;
            let send_window = recv_window;
            info!("QUIC stream recv window: {stream_recv_window}");
            info!("QUIC recv window: {recv_window}");
            info!("QUIC send window: {send_window}");
            transport_config
                .stream_receive_window(VarInt::from_u32(stream_recv_window as u32))
                .receive_window(VarInt::from_u32(recv_window as u32))
                .send_window(send_window as u64)
                .datagram_receive_buffer_size(None)
                .datagram_send_buffer_size(0);
            transport_config.congestion_controller_factory(
                settings.congestion_algorithm.controller_factory(
                    settings.initial_cwnd.map(InitialCwndBytes::get),
                    send_window as u64,
                ),
            );
            transport_config
                .datagram_receive_buffer_size(None)
                .datagram_send_buffer_size(0);

            let mut ack_freq_config = AckFrequencyConfig::default();
            ack_freq_config
                .reordering_threshold(VarInt::from_u32(lore_transport::quic::PACKET_THRESHOLD - 1));
            transport_config
                .packet_threshold(lore_transport::quic::PACKET_THRESHOLD)
                .time_threshold(lore_transport::quic::TIME_THRESHOLD)
                .max_rtt(Duration::from_millis(lore_transport::quic::MAX_RTT_MS))
                .ack_frequency_config(Some(ack_freq_config));
        }

        // A caller that needs this port on TCP as well binds both itself and hands the UDP half
        // over, because there is no way to reserve a number across two binds. Its socket carries
        // its own options, so the reuse flags below are deliberately not applied to it.
        let socket: std::net::UdpSocket = if let Some(socket) = settings.socket.take() {
            socket
        } else {
            let socket = socket2::Socket::new(
                Domain::for_address(settings.address),
                Type::DGRAM,
                Some(Protocol::UDP),
            )?;

            // Reuse must be configured before bind.
            socket.reuse_address()?;

            // set_reuse_port is not implemented on Windows
            #[cfg(target_family = "unix")]
            socket.set_reuse_port(true)?;

            socket.bind(&socket2::SockAddr::from(settings.address))?;
            socket.into()
        };

        // `NetRuntime` places every task quinn spawns on net, including the connection driver
        // created when an `Incoming` is awaited. The guard covers what it cannot: the socket is
        // handed to `tokio::net::UdpSocket::from_std`, which registers with whichever reactor is
        // current here. Removing it would register net's socket on core's reactor.
        let endpoint = {
            let _guard = lore_base::runtime::net_runtime().enter();
            quinn::Endpoint::new(
                endpoint_config,
                Some(server_config),
                socket,
                Arc::new(NetRuntime),
            )?
        };

        let draining = CancellationToken::new();
        let connections = TaskTracker::new();
        run_loop(
            endpoint.clone(),
            settings,
            draining.clone(),
            connections.clone(),
        )?;

        Ok(Self {
            endpoint,
            draining,
            connections,
        })
    }

    /// Stop accepting connections and close the open ones once they are idle.
    ///
    /// New handshakes are refused at once. Every open connection stops reading new requests,
    /// answers the ones it has already read, and is then closed with application code 0, which
    /// a client treats as a reason to reconnect. Connections still busy after `timeout` are
    /// closed regardless, as [`QuinnServer::close`] does.
    pub async fn shutdown(&self, timeout: Duration) {
        self.endpoint.set_server_config(None);
        self.draining.cancel();
        self.connections.close();
        if tokio::time::timeout(timeout, self.connections.wait())
            .await
            .is_err()
        {
            warn!(
                remaining = self.connections.len(),
                "QUIC connections still busy after {timeout:?}, closing them"
            );
        }
        self.close().await;
    }

    /// Gracefully close all connections on this endpoint. Sends `CONNECTION_CLOSE`
    /// frames to all peers (causing accept loops to return `None`) and waits for
    /// the frames to be delivered.
    pub async fn close(&self) {
        self.draining.cancel();
        self.endpoint
            .close(VarInt::from_u32(SHUTDOWN_CLOSE_CODE), SHUTDOWN_CLOSE_REASON);
        self.endpoint.wait_idle().await;
    }
}

fn run_loop(
    endpoint: quinn::Endpoint,
    settings: QuinnConfig,
    draining: CancellationToken,
    connections: TaskTracker,
) -> anyhow::Result<()> {
    let stream_handler_factory = Arc::new(settings.stream_handler_factory);
    let metrics_interval = settings.metrics_frequency;

    let monitor = TaskMonitor::new();
    let otel_bridge = OtelTokioTaskMetrics::new(
        &lore_telemetry::meter("quicsrv_tokio_task"),
        vec![KeyValue::new(
            METRICS_QUINN_SERVER_LABEL,
            settings.server_metrics_name,
        )],
    );

    {
        // The `instrument` function from `tracing` conflicts with the `instrument` function from
        // the `TaskMonitor` so we need to use a scoped import.
        use tracing::instrument::Instrument;
        let monitor = monitor.clone();
        lore_spawn_core!(
            LORE_CONTEXT.scope(
                execution_context(),
                async move {
                    for task_metrics in monitor.intervals() {
                        otel_bridge.record(task_metrics);
                        tokio::time::sleep(metrics_interval).await;
                    }
                }
                .in_current_span(),
            )
        );
    }

    info!("Creating {} listener(s)", &settings.num_listeners);
    for i in 0..settings.num_listeners {
        // The `instrument` function from `tracing` conflicts with the `instrument` function from
        // the `TaskMonitor` so we need to use a scoped import.
        use tracing::instrument::Instrument;
        let endpoint = endpoint.clone();
        let monitor = monitor.clone();
        let stream_handler_factory = stream_handler_factory.clone();
        let draining = draining.clone();
        let connections = connections.clone();

        lore_spawn_net!(
            LORE_CONTEXT.scope(
                execution_context(),
                async move {
                    debug!("Spawned task {i} is waiting to accept connections");
                    while let Some(connection) = endpoint.accept().await {
                        debug!("Spawned task {i} received a connection: {connection:?}");

                        let monitor = monitor.clone();
                        let stream_handler_factory = stream_handler_factory.clone();
                        let draining = draining.clone();

                        lore_spawn_net!(
                            connections.track_future(
                                LORE_CONTEXT.scope(
                                    execution_context(),
                                    async move {
                                        if let Err(error) = handle_conn(
                                            connection,
                                            monitor.clone(),
                                            metrics_interval,
                                            stream_handler_factory.clone(),
                                            draining,
                                        )
                                        .await
                                        {
                                            warn!("Error handling connection: {error}");
                                        }
                                    }
                                    .in_current_span(),
                                )
                            )
                        );
                    }
                }
                .in_current_span(),
            )
        );
    }

    Ok(())
}

fn get_protocol(connection: &quinn::Connection) -> Result<String, anyhow::Error> {
    let handshake_data = connection
        .handshake_data()
        .and_then(|h| h.downcast::<HandshakeData>().ok())
        .ok_or(anyhow!("No handshake data"))?;

    handshake_data
        .protocol
        .map(String::from_utf8)
        .transpose()
        .map_err(|e| anyhow!("Failed to decode protocol: {e:?}"))?
        .ok_or(anyhow!("No protocol found on request"))
}

#[tracing::instrument(
    name = "urc-quic",
    skip_all,
    fields(connection_id, protocol, correlation_id, repository_id)
)]
async fn handle_conn(
    conn: quinn::Incoming,
    monitor: TaskMonitor,
    connection_metrics_interval: Duration,
    stream_handler_factory: Arc<Box<dyn StreamHandlerFactory>>,
    draining: CancellationToken,
) -> anyhow::Result<()> {
    let connection = conn.await?;

    let protocol = get_protocol(&connection)?;

    let connection_id = connection.stable_id();

    let conn_span = tracing::Span::current();
    conn_span.record("connection_id", connection_id);
    conn_span.record("protocol", &protocol);

    info!(
        remote_address = %connection.remote_address(),
        "Established connection",
    );

    let Some((service_name, service_builder)) =
        stream_handler_factory.get_stream_handler_builder(&protocol)
    else {
        warn!("Protocol {protocol} is not supported by the stream handler factory");
        return Err(anyhow!(
            "Received connection for unsupported protocol: {protocol:?}"
        ));
    };

    let context = Arc::new(AttributeMap::default());
    context.insert(conn_span.clone());
    context.insert(ConnectionId(connection_id));

    let _stats_guard = track_connection_stats(
        service_name,
        &connection,
        context.clone(),
        connection_metrics_interval,
    );

    // Create the stream handler once per connection so per-connection state
    // (e.g. SessionMap in StorageServiceV4) is shared across all streams.
    let connection_handler: Arc<Box<dyn StreamDataHandler>> =
        Arc::new(service_builder(context.clone()));

    // Streams still reading requests.
    let readers = TaskTracker::new();
    // Streams whose responses may not yet have reached the peer.
    let writers = TaskTracker::new();
    // Cancelled once every request read from this connection has been answered.
    let answered = CancellationToken::new();

    // Keeps accepting requests until the connection closes or errors, or the server drains
    loop {
        trace!("Waiting for stream on connection id: {connection_id}");
        let stream = tokio::select! {
            biased;
            () = draining.cancelled() => break,
            stream = connection.accept_bi() => stream,
        };

        let (send, mut recv) = match stream {
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                info!("Connection closed");
                return Ok(());
            }
            Err(e) => {
                return Err(anyhow!(e));
            }
            Ok(s) => {
                debug!(
                    stream_id = s.0.id().to_string(),
                    "Stream opened for connection"
                );
                s
            }
        };

        trace!("Spawning task to handle request");

        let execution = execution_context();

        // The `instrument` function from `tracing` conflicts with the `instrument` function from
        // the `TaskMonitor` so we need to use a scoped import.
        let handle_future = {
            use tracing::instrument::Instrument;

            let handler = connection_handler.clone();
            let draining = draining.clone();
            let answered = answered.clone();
            let reader = readers.token();
            let writer = writers.token();
            let connection = connection.clone();
            let send = Arc::new(tokio::sync::Mutex::new(send));

            async move {
                // Get the request off the transport
                match handler
                    .handle_stream(&mut recv, send.clone(), &draining)
                    .await
                {
                    Ok(_) => {
                        debug!("Successfully handled request for connection");
                    }
                    Err(e) => {
                        warn!("Error handling request: {e:?}");

                        if let Err(e) = handler.close(&mut recv, Some(u32::MAX)).await {
                            warn!("Error closing transport: {}", e);
                        }
                    }
                }
                drop(reader);
                if draining.is_cancelled() {
                    answered.cancelled().await;
                    let delivered = {
                        let mut send = send.lock().await;
                        send.finish().ok().map(|()| send.stopped())
                    };
                    if let Some(delivered) = delivered {
                        let _ = delivered.await;
                    }
                    drop(writer);
                    // Held open: dropping it stops the peer's sends, failing requests a reconnect
                    // retries.
                    connection.closed().await;
                }
            }
            .instrument(info_span!("handle_stream"))
        };

        // Transport read only: `handle_stream` lifts the request off the wire and
        // hands processing to core from inside the stream handler.
        lore_spawn_net!(monitor.instrument(LORE_CONTEXT.scope(execution.clone(), handle_future)));
    }

    readers.close();
    readers.wait().await;
    while connection_handler.in_flight() > 0 {
        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
    }
    answered.cancel();
    writers.close();
    writers.wait().await;
    info!("Connection drained, closing");
    connection.close(VarInt::from_u32(SHUTDOWN_CLOSE_CODE), SHUTDOWN_CLOSE_REASON);
    Ok(())
}
