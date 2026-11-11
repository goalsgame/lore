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

/// Every configured certificate file must yield a certificate, so a renewal that empties the
/// chain file is refused rather than leaving a leaf-only chain in use.
#[test]
fn an_emptied_chain_file_is_refused() {
    let files = CertificateFiles::new();
    let served = files.renew();
    let chain = files.cert.with_file_name("chain.pem");
    let issuer = lore_transport::tls::generate_self_signed(vec!["issuer".into()]).unwrap();
    std::fs::write(&chain, &issuer.cert_pem).unwrap();
    let certificate =
        ReloadableCertificate::load(vec![files.cert.clone(), chain.clone()], files.key.clone())
            .unwrap();
    assert_eq!(certificate.current().cert.len(), 2);

    std::fs::write(&chain, "").unwrap();
    assert!(
        certificate.reload().is_err(),
        "an empty chain file is refused"
    );
    assert_eq!(leaf(&certificate), served);
    assert_eq!(certificate.current().cert.len(), 2);
}

/// Dropping the accepted-connection stream releases connections whose handshake has not
/// completed, rather than leaving them waiting on a silent client.
#[tokio::test]
async fn dropping_the_tls_incoming_releases_unfinished_handshakes() {
    let files = CertificateFiles::new();
    files.renew();
    let config = lore_server::tls::grpc_server_tls_config(files.load(), None, false).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = lore_server::tls::tls_incoming(
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        config,
    );

    let mut silent = tokio::net::TcpStream::connect(address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(incoming);

    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), silent.read(&mut byte))
        .await
        .expect("the unfinished handshake is abandoned");
    assert!(matches!(read, Ok(0) | Err(_)), "got {read:?}");
}

#[test]
fn a_malformed_chain_certificate_is_refused() {
    let files = CertificateFiles::new();
    let served = files.renew();
    let chain = files.cert.with_file_name("chain.pem");
    let issuer = lore_transport::tls::generate_self_signed(vec!["issuer".into()]).unwrap();
    std::fs::write(&chain, &issuer.cert_pem).unwrap();
    let certificate =
        ReloadableCertificate::load(vec![files.cert.clone(), chain.clone()], files.key.clone())
            .unwrap();

    std::fs::write(
        &chain,
        "-----BEGIN CERTIFICATE-----\nAAECAwQF\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    assert!(
        certificate.reload().is_err(),
        "a malformed certificate is refused"
    );
    assert_eq!(leaf(&certificate), served);
    assert_eq!(certificate.current().cert.len(), 2);
}

/// Which client certificate a test client presents.
#[derive(Clone, Copy, Debug)]
enum ClientIdentity {
    None,
    Trusted,
    Untrusted,
}

fn client_config(identity: ClientIdentity) -> rustls::ClientConfig {
    let builder = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new());
    let data = lore_server::quic::tests::test_data_path();
    let mut config = match identity {
        ClientIdentity::None => builder.with_no_client_auth(),
        ClientIdentity::Trusted | ClientIdentity::Untrusted => {
            let (cert, key) = match identity {
                ClientIdentity::Trusted => ("test_client_cert.pem", "test_client_key.pem"),
                _ => ("untrusted_cert.pem", "untrusted_key.pem"),
            };
            builder
                .with_client_auth_cert(
                    lore_transport::tls::load_certs(data.join(cert)).unwrap(),
                    lore_transport::tls::load_private_key(data.join(key)).unwrap(),
                )
                .unwrap()
        }
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

/// Whether a listener with the given client-certificate policy completes a handshake with a
/// client presenting `identity`.
async fn handshake_accepted(
    certificate: Arc<ReloadableCertificate>,
    client_auth_required: bool,
    identity: ClientIdentity,
) -> bool {
    use tokio_stream::StreamExt as _;

    let ca = lore_server::quic::tests::test_data_path().join("test_ca.pem");
    let config =
        lore_server::tls::grpc_server_tls_config(certificate, Some(&ca), client_auth_required)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut incoming = std::pin::pin!(lore_server::tls::tls_incoming(
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        config,
    ));

    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config(identity)));
    let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
    let _client = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await;
    matches!(
        tokio::time::timeout(Duration::from_secs(1), incoming.next()).await,
        Ok(Some(Ok(_)))
    )
}

/// Client-certificate policy is enforced by the listener, before and after a reload of the
/// server certificate: required authentication accepts only trusted certificates, optional
/// authentication also accepts none, and neither accepts an untrusted one.
#[tokio::test]
async fn client_certificate_policy_holds_across_a_reload() {
    let files = CertificateFiles::new();
    files.renew();
    let certificate = files.load();

    for reloaded in [false, true] {
        if reloaded {
            files.renew();
            certificate.reload().unwrap();
        }
        for (required, identity, expected) in [
            (true, ClientIdentity::Trusted, true),
            (true, ClientIdentity::None, false),
            (true, ClientIdentity::Untrusted, false),
            (false, ClientIdentity::Trusted, true),
            (false, ClientIdentity::None, true),
            (false, ClientIdentity::Untrusted, false),
        ] {
            assert_eq!(
                handshake_accepted(certificate.clone(), required, identity).await,
                expected,
                "required={required} identity={identity:?} reloaded={reloaded}"
            );
        }
    }
}
