// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fmt::Display;
use std::fmt::Formatter;
use std::pin::pin;
use std::sync::Arc;

use bytes::Bytes;
use lore_base::lore_spawn;
use lore_error_set::prelude::*;
use lore_revision::service_state::ServiceStateImpl;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::interface::LoreEvent;
use crate::interface::LoreLogLevel;
use crate::remote::message::MessageToClient;
use crate::remote::message::MessageToServer;
use crate::remote::message::blocking_read_message;
use crate::remote::message::encode_message;
use crate::remote::message::write_payload;
use crate::remote::network::UdsStream;

/// Construct and poll the handler inside the service-owned resolver scope.
/// Message payloads carry only the caller environment and ordinary command data.
#[lore_macro::test_pub]
pub(crate) async fn invoke_service_command(
    command: MessageToServer,
    callback: crate::interface::LoreEventCallback,
    service_state: &ServiceStateImpl,
) -> i32 {
    crate::call::with_provider_resolver(service_state.provider_registry().clone(), async move {
        let handler = pin!(command.invoke(callback));
        handler.await
    })
    .await
}

#[derive(Debug, Copy, Clone)]
pub struct ConnectionId(pub usize);

impl Display for ConnectionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[error_set]
pub enum ConnectionError {}

#[derive(Debug, Error)]
#[error("connection {connection_id}: {error}")]
pub struct ConnectionErrorWithId {
    connection_id: ConnectionId,
    error: String,
}

impl ConnectionErrorWithId {
    pub fn new(error: ConnectionError, connection_id: ConnectionId) -> Self {
        Self {
            connection_id,
            error: error.to_string(),
        }
    }
}

/// Serves the command a client sends over `connection` on a task of its own, relaying the
/// command's events and then its status back over the same connection.
pub fn serve_connection(
    accept_state: Arc<ServiceStateImpl>,
    id: ConnectionId,
    connection: UdsStream,
) {
    let connection = IpcConnection::new(id, connection, accept_state.clone());
    spawn_connection(accept_state, id, connection);
}
#[cfg(unix)]
#[lore_macro::test_pub]
pub(crate) fn serve_owned_connection(
    owner: Arc<crate::service::owner::ServiceOwner>,
    id: ConnectionId,
    connection: UdsStream,
) {
    let state = owner.state().clone();
    let mut connection = IpcConnection::new(id, connection, state.clone());
    connection.owner = Some(owner);
    spawn_connection(state, id, connection);
}
fn spawn_connection(
    accept_state: Arc<ServiceStateImpl>,
    id: ConnectionId,
    connection: IpcConnection,
) {
    #[cfg(unix)]
    let mut connection = connection;
    #[cfg(unix)]
    if let Some(actor) = connection.owner.as_ref().and_then(|owner| owner.actor()) {
        let Ok(Some(lease)) = actor.track_socket(&connection.connection) else {
            return;
        };
        connection.lease = Some(Arc::new(lease));
    }
    let counted = accept_state.increment_connections();
    #[cfg(unix)]
    let runtime = connection
        .owner
        .as_ref()
        .and_then(|owner| owner.actor())
        .and_then(|actor| actor.runtime());
    let work = async move {
        let _counted = counted;
        if let Err(error) = connection.handle_connection().await {
            accept_state.push_log(
                LoreLogLevel::Error,
                format!("Failed to handle connection {id}: {error}"),
            );
        }
    };
    #[cfg(unix)]
    if let Some(runtime) = runtime {
        spawn_on(runtime, work);
        return;
    }
    lore_spawn!(work);
}

#[cfg(unix)]
#[allow(clippy::disallowed_methods)]
fn spawn_on(
    runtime: tokio::runtime::Handle,
    work: impl std::future::Future<Output = ()> + Send + 'static,
) {
    runtime.spawn(lore_base::runtime::spawned_task(work));
}

struct IpcConnection {
    id: ConnectionId,
    connection: UdsStream,
    service_state: Arc<ServiceStateImpl>,
    #[cfg(unix)]
    owner: Option<Arc<crate::service::owner::ServiceOwner>>,
    #[cfg(unix)]
    lease: Option<Arc<crate::service::actor::ConnectionLease>>,
}

impl IpcConnection {
    #[allow(clippy::disallowed_methods)]
    fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> tokio::task::JoinHandle<T> {
        #[cfg(unix)]
        if let Some(runtime) = self
            .owner
            .as_ref()
            .and_then(|owner| owner.actor())
            .and_then(|actor| actor.runtime())
        {
            return runtime.spawn_blocking(lore_base::runtime::spawned_blocking_task(work));
        }
        lore_base::lore_spawn_blocking!(work)
    }
    fn new(id: ConnectionId, connection: UdsStream, service_state: Arc<ServiceStateImpl>) -> Self {
        Self {
            id,
            connection,
            service_state,
            #[cfg(unix)]
            owner: None,
            #[cfg(unix)]
            lease: None,
        }
    }

    async fn send_message(
        &self,
        mut stream: UdsStream,
        payload: Vec<u8>,
    ) -> Result<(), ConnectionError> {
        #[cfg(unix)]
        let lease = self.lease.clone();
        self.blocking(move || {
            #[cfg(unix)]
            let _lease = lease;
            write_payload(stream.writer(), &payload)
        })
        .await
        .internal("failed writing")?
        .forward::<ConnectionError>("writing message")?;
        Ok(())
    }

    async fn handle_connection(self) -> Result<(), ConnectionError> {
        let mut connection = self.connection.try_clone().internal("cloning connection")?;
        #[cfg(unix)]
        let lease = self.lease.clone();
        // Owned Unix connections use the run's bounded blocking pool. Each
        // connection releases its initial read worker before issuing replies.
        let message: Option<(MessageToServer, Bytes)> = self
            .blocking(move || {
                #[cfg(unix)]
                let _lease = lease;
                blocking_read_message(connection.reader())
            })
            .await
            .internal("failed reading")?
            .forward::<ConnectionError>("reading message")?;

        let Some((command, payload)) = message else {
            return Ok(());
        };

        //TODO(UCS-16094): Determine if this should be unbounded or bounded
        // Create a channel so the callback task can send messages to this network thread, so they
        // can be forwarded to the client.
        let (to_client_sender, mut to_client_receiver) = mpsc::unbounded_channel::<Vec<u8>>();

        let invoke_state = Arc::clone(&self.service_state);
        #[cfg(unix)]
        let admitted = if matches!(
            &command.command,
            crate::remote::command::LoreCommand::ServiceStatus(_)
                | crate::remote::command::LoreCommand::ServiceStop(_)
        ) {
            invoke_state.try_begin_control_invocation()
        } else {
            invoke_state.try_begin_invocation()
        };
        #[cfg(not(unix))]
        let admitted = invoke_state.try_begin_invocation();
        let Some(invocation) = admitted else {
            #[cfg(unix)]
            if self.reply_to_retired_stop(&command.command).await? {
                return Ok(());
            }
            return Err(ConnectionError::internal(
                "service command admission is closed or control capacity is full",
            ));
        };
        #[cfg(unix)]
        let owner = self.owner.clone();
        #[cfg(unix)]
        let invocation_lease = self.lease.clone();
        #[cfg(unix)]
        let runtime = owner
            .as_ref()
            .and_then(|owner| owner.actor())
            .and_then(|actor| actor.runtime());
        let invocation_work = crate::service::state::with_invocation(invocation, async move {
            #[cfg(unix)]
            let _lease = invocation_lease;
            #[cfg(unix)]
            let scope_state = invoke_state.clone();
            let work = async move {
                let sender = to_client_sender.clone();
                let callback_state = Arc::clone(&invoke_state);

                // Note: this callback is intentionally NOT wrapped with .with_defaults().
                // It is the server-side event forwarder that must pass every LoreEvent
                // (including Error and Log) to the remote client so the client's own
                // wrapped callback can handle them. Wrapping here would swallow those
                // events on the server side and they would never reach the remote.
                //
                // The event is encoded here rather than where it is sent: a `LoreBytes` it carries
                // is valid only while the callback runs.
                let callback: crate::interface::LoreEventCallback =
                    Some(Box::new(move |event: &LoreEvent| {
                        if let Err(error) =
                            to_client_sender.send(encode_message(&MessageToClient::Event(event)))
                        {
                            callback_state.push_log(
                                LoreLogLevel::Error,
                                format!("Failed to send Event message to connection task: {error}"),
                            );
                        }
                    }));
                let cli_result = invoke_service_command(command, callback, &invoke_state).await;
                // Holds the views of any `LoreBytes` the command carried.
                drop(payload);

                if let Err(error) = sender.send(encode_message(
                    &MessageToClient::<LoreEvent>::ApiResult(cli_result),
                )) {
                    invoke_state.push_log(
                        LoreLogLevel::Error,
                        format!("Failed to send ApiResult message to connection task: {error}"),
                    );
                }
            };
            #[cfg(not(unix))]
            work.await;
            #[cfg(unix)]
            {
                if let Some(owner) = owner {
                    if crate::service::state::with_owner(owner, Box::pin(work))
                        .await
                        .is_err()
                    {
                        scope_state
                            .push_log(LoreLogLevel::Error, "service owner/ticket mismatch".into());
                    }
                } else {
                    work.await;
                }
            }
        });
        #[cfg(unix)]
        if let Some(runtime) = runtime {
            spawn_on(runtime, invocation_work);
        } else {
            lore_base::lore_spawn!(invocation_work);
        }
        #[cfg(not(unix))]
        lore_base::lore_spawn!(invocation_work);

        while let Some(message) = to_client_receiver.recv().await {
            let stream = self.connection.try_clone().internal("cloning connection")?;
            if let Err(error) = self.send_message(stream, message).await {
                self.service_state.push_log(
                    LoreLogLevel::Error,
                    format!(
                        "Failed to send message to client: {}",
                        ConnectionErrorWithId::new(error, self.id)
                    ),
                );
            }
        }

        Ok(())
    }

    #[cfg(unix)]
    async fn reply_to_retired_stop(
        &self,
        command: &crate::remote::command::LoreCommand,
    ) -> Result<bool, ConnectionError> {
        let Some(owner) = self.owner.as_ref() else {
            return Ok(false);
        };
        if !matches!(command, crate::remote::command::LoreCommand::ServiceStop(_))
            || owner.status().0 != crate::service::owner::Phase::ReadyToExit
            || owner.stop_request().is_none_or(|stop| !stop.is_current())
            || owner.actor().is_some_and(|actor| actor.failure().is_some())
        {
            return Ok(false);
        }
        // Retirement is proven, but this accepted connection can finish its
        // initial read after control admission closes. A stop needs only the
        // acknowledgement; it must not reopen admission or affect another run.
        let complete = LoreEvent::Complete(lore_revision::event::LoreCompleteEventData {
            status: 0,
            error: Default::default(),
        });
        let end = LoreEvent::End(Default::default());
        for reply in [
            MessageToClient::Event(&complete),
            MessageToClient::Event(&end),
            MessageToClient::ApiResult(0),
        ] {
            let stream = self.connection.try_clone().internal("cloning connection")?;
            self.send_message(stream, encode_message(&reply)).await?;
        }
        Ok(true)
    }
}
