// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_server::quic::quinn::QuinnConfigBuilder;
use lore_server::quic::quinn::QuinnServer;
use lore_server::quic::tests::TEST_PROTOCOL;
use lore_server::quic::tests::TestHandlerFactory;
use lore_server::tls::ReloadableCertificate;
use lore_server::tls::reload_certificates;
use lore_transport::quic::client::insecure_client_auth::SkipServerVerification;
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use rustls::pki_types::pem::PemObject;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::store::test_support::test_store_create;

/// A certificate and key on disk, rewritten in place as a renewal would.
struct CertificateFiles {
    _dir: TempDir,
    cert: PathBuf,
    key: PathBuf,
}

impl CertificateFiles {
    fn new() -> Self {
        let dir = TempDir::new("lore-server-tls-reload-");
        let cert = dir.path().join("server.crt");
        let key = dir.path().join("server.key");
        Self {
            _dir: dir,
            cert,
            key,
        }
    }

    /// Write a fresh self-signed certificate and its key, returning the certificate.
    fn renew(&self) -> CertificateDer<'static> {
        let generated =
            lore_transport::tls::generate_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(&self.cert, &generated.cert_pem).unwrap();
        std::fs::write(&self.key, &generated.key_pem).unwrap();
        CertificateDer::from_pem_slice(generated.cert_pem.as_bytes()).unwrap()
    }

    fn load(&self) -> Arc<ReloadableCertificate> {
        ReloadableCertificate::load(vec![self.cert.clone()], self.key.clone()).unwrap()
    }
}

fn leaf(certificate: &ReloadableCertificate) -> CertificateDer<'static> {
    certificate
        .current()
        .end_entity_cert()
        .unwrap()
        .clone()
        .into_owned()
}

#[test]
fn reload_presents_the_renewed_certificate() {
    let files = CertificateFiles::new();
    let first = files.renew();
    let certificate = files.load();
    assert_eq!(leaf(&certificate), first);

    let renewed = files.renew();
    certificate.reload().expect("a valid renewal reloads");

    assert_eq!(leaf(&certificate), renewed);
}

#[test]
fn an_invalid_renewal_is_refused_and_the_previous_certificate_kept() {
    let files = CertificateFiles::new();
    let served = files.renew();
    let certificate = files.load();

    std::fs::write(&files.cert, "not a certificate").unwrap();
    assert!(certificate.reload().is_err(), "garbage is refused");
    assert_eq!(leaf(&certificate), served);

    let other = lore_transport::tls::generate_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&files.cert, &other.cert_pem).unwrap();
    let mismatch = certificate
        .reload()
        .expect_err("a certificate paired with another certificate's key is refused");
    assert!(
        format!("{mismatch:#}").contains("KeyMismatch"),
        "got {mismatch:#}"
    );
    assert_eq!(leaf(&certificate), served);

    std::fs::remove_file(&files.key).unwrap();
    assert!(certificate.reload().is_err(), "a missing key is refused");
    assert_eq!(leaf(&certificate), served);
}

#[test]
fn reload_certificates_reloads_every_certificate_in_use() {
    let files = [CertificateFiles::new(), CertificateFiles::new()];
    let certificates: Vec<_> = files
        .iter()
        .map(|files| {
            files.renew();
            files.load()
        })
        .collect();

    let renewed: Vec<_> = files.iter().map(CertificateFiles::renew).collect();
    reload_certificates();

    for (certificate, renewed) in certificates.iter().zip(renewed) {
        assert_eq!(leaf(certificate), renewed);
    }
}

fn free_tcp_port() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn insecure_client_config(alpn: &[u8]) -> rustls::ClientConfig {
    let mut config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    config
}

async fn connect_tls(
    address: SocketAddr,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let connector = tokio_rustls::TlsConnector::from(Arc::new(insecure_client_config(b"h2")));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(tcp) = tokio::net::TcpStream::connect(address).await
                && let Ok(tls) = connector
                    .connect(ServerName::try_from("localhost").unwrap(), tcp)
                    .await
            {
                return tls;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the gRPC listener comes up")
}

fn presented(
    stream: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
) -> CertificateDer<'static> {
    stream.get_ref().1.peer_certificates().unwrap()[0].clone()
}

/// A gRPC listener presents a reloaded certificate to new connections, while a connection made
/// before the reload carries on.
#[tokio::test]
async fn grpc_presents_a_reloaded_certificate_to_new_connections() {
    let files = CertificateFiles::new();
    let first = files.renew();
    let address = free_tcp_port();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = lore_spawn!(lore_server::grpc::server::serve_maintenance(
        Default::default(),
        address,
        Some(files.cert.clone()),
        Some(files.key.clone()),
        None,
        async move {
            let _ = stopped.await;
        },
    ));

    let mut before = connect_tls(address).await;
    assert_eq!(presented(&before), first);
    assert_eq!(before.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));

    let renewed = files.renew();
    reload_certificates();

    let after = connect_tls(address).await;
    assert_eq!(presented(&after), renewed);

    before
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
        .await
        .unwrap();
    let mut frame_header = [0u8; 9];
    tokio::time::timeout(Duration::from_secs(5), before.read_exact(&mut frame_header))
        .await
        .expect("the earlier connection still answers")
        .unwrap();
    assert_eq!(frame_header[3], 0x04, "the server opens with SETTINGS");

    drop((before, after));
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("the listener shuts down")
        .unwrap()
        .unwrap();
}

/// A QUIC endpoint presents a reloaded certificate to new connections, while a connection made
/// before the reload carries on.
#[tokio::test]
async fn quic_presents_a_reloaded_certificate_to_new_connections() {
    let (immutable_store, mutable_store, execution) =
        test_store_create().await.expect("Failed to create store");
    lore_spawn!(LORE_CONTEXT.scope(execution, async move {
        let files = CertificateFiles::new();
        let first = files.renew();
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let server_address = socket.local_addr().unwrap();
        let server = QuinnServer::start(
            QuinnConfigBuilder::new()
                .socket(socket)
                .cert_file(files.cert.clone())
                .pkey_file(files.key.clone())
                .stream_handler_factory(Box::new(TestHandlerFactory::new(
                    immutable_store,
                    mutable_store,
                )))
                .build()
                .unwrap(),
        )
        .unwrap();

        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(insecure_client_config(TEST_PROTOCOL.as_bytes())).unwrap(),
        )));
        let connect = || async {
            endpoint
                .connect(server_address, "localhost")
                .unwrap()
                .await
                .unwrap()
        };
        let presented = |connection: &quinn::Connection| {
            connection
                .peer_identity()
                .unwrap()
                .downcast::<Vec<CertificateDer<'static>>>()
                .unwrap()[0]
                .clone()
        };

        let before = connect().await;
        assert_eq!(presented(&before), first);

        let renewed = files.renew();
        reload_certificates();

        let after = connect().await;
        assert_eq!(presented(&after), renewed);
        assert!(before.close_reason().is_none());
        before
            .open_bi()
            .await
            .expect("the earlier connection still opens streams");

        server.close().await;
    }))
    .await
    .unwrap();
}
