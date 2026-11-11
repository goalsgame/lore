// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use anyhow::Context as _;
use lore_base::lore_spawn;
use lore_error_set::prelude::*;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rustls::crypto::CryptoProvider;
use rustls::server::ClientHello;
use rustls::server::ResolvesServerCert;
use rustls::sign::CertifiedKey;
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::transport::Certificate;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Identity;
use tracing::debug;
use tracing::error;
use tracing::info;

/// Referenced by config toml in several places - be careful of how you rename
/// these fields
#[derive(Clone, Debug, Deserialize)]
pub struct CertificateSettings {
    pub cert_chain: Option<PathBuf>,
    pub cert_file: PathBuf,
    pub pkey_file: PathBuf,
}

#[error_set]
pub enum LoadClientTlsError {}

pub fn load_client_tls(
    settings: CertificateSettings,
) -> Result<ClientTlsConfig, LoadClientTlsError> {
    let client_cert = std::fs::read(settings.cert_file).internal("Error loading Client Cert")?;
    let client_key = std::fs::read(settings.pkey_file).internal("Error loading Client Key")?;
    let client_identity = Identity::from_pem(client_cert, client_key);

    let mut tls = ClientTlsConfig::new().identity(client_identity);
    if let Some(ca_cert_path) = settings.cert_chain {
        let ca_cert = std::fs::read(ca_cert_path).internal("Error loading CA Cert")?;
        tls = tls.ca_certificate(Certificate::from_pem(ca_cert));
    }

    Ok(tls)
}

/// Live [`ReloadableCertificate`]s, re-read together by [`reload_certificates`].
static RELOADABLE: Mutex<Vec<Weak<ReloadableCertificate>>> = Mutex::new(Vec::new());

/// A server certificate chain and private key read from disk, which a TLS listener presents to
/// every handshake and which [`ReloadableCertificate::reload`] replaces without a restart.
///
/// Handshakes started after a reload present the new certificate; established connections keep
/// the one they negotiated.
pub struct ReloadableCertificate {
    /// Concatenated in order to form the presented chain, leaf first.
    cert_files: Vec<PathBuf>,
    key_file: PathBuf,
    current: RwLock<Arc<CertifiedKey>>,
}

impl ReloadableCertificate {
    /// Read the certificate and key, and register them with [`reload_certificates`].
    pub fn load(cert_files: Vec<PathBuf>, key_file: PathBuf) -> anyhow::Result<Arc<Self>> {
        let current = read_certified_key(&cert_files, &key_file)?;
        let certificate = Arc::new(Self {
            cert_files,
            key_file,
            current: RwLock::new(Arc::new(current)),
        });
        let mut reloadable = RELOADABLE.lock();
        reloadable.retain(|certificate| certificate.strong_count() > 0);
        reloadable.push(Arc::downgrade(&certificate));
        Ok(certificate)
    }

    /// Re-read the certificate and key. On error the certificate in use is kept.
    pub fn reload(&self) -> anyhow::Result<()> {
        let reloaded = read_certified_key(&self.cert_files, &self.key_file)?;
        *self.current.write() = Arc::new(reloaded);
        Ok(())
    }

    /// The certificate and key presented to new handshakes.
    pub fn current(&self) -> Arc<CertifiedKey> {
        self.current.read().clone()
    }

    pub fn cert_files(&self) -> &[PathBuf] {
        &self.cert_files
    }
}

impl std::fmt::Debug for ReloadableCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadableCertificate")
            .field("cert_files", &self.cert_files)
            .field("key_file", &self.key_file)
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for ReloadableCertificate {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

fn read_certified_key(cert_files: &[PathBuf], key_file: &Path) -> anyhow::Result<CertifiedKey> {
    let mut chain = Vec::new();
    for cert_file in cert_files {
        let certs = lore_transport::tls::load_certs(cert_file)
            .with_context(|| format!("reading certificate {}", cert_file.display()))?;
        if certs.is_empty() {
            anyhow::bail!("no certificate found in {}", cert_file.display());
        }
        chain.extend(certs);
    }
    let key = lore_transport::tls::load_private_key(key_file)
        .with_context(|| format!("reading private key {}", key_file.display()))?;
    let provider = CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    CertifiedKey::from_der(chain, key, &provider)
        .with_context(|| format!("pairing {cert_files:?} with {}", key_file.display()))
}

/// Re-read every live [`ReloadableCertificate`] from disk, logging each outcome. A certificate
/// that fails to load is logged and left in use. Returns how many failed.
pub fn reload_certificates() -> usize {
    let certificates: Vec<_> = {
        let mut reloadable = RELOADABLE.lock();
        reloadable.retain(|certificate| certificate.strong_count() > 0);
        reloadable.iter().filter_map(Weak::upgrade).collect()
    };
    let mut failed = 0;
    for certificate in certificates {
        match certificate.reload() {
            Ok(()) => info!(cert_files = ?certificate.cert_files, "Reloaded TLS certificate"),
            Err(error) => {
                failed += 1;
                error!(
                    cert_files = ?certificate.cert_files,
                    error = format!("{error:#}"),
                    "Failed to reload TLS certificate, still serving the previous one"
                );
            }
        }
    }
    failed
}

/// A rustls server configuration for a gRPC listener presenting `certificate`.
///
/// With `client_ca`, client certificates are verified against it; they are required when
/// `client_auth_required`, and otherwise accepted but optional.
pub fn grpc_server_tls_config(
    certificate: Arc<ReloadableCertificate>,
    client_ca: Option<&Path>,
    client_auth_required: bool,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let builder = rustls::ServerConfig::builder();
    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some(client_ca) => {
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(
                lore_transport::tls::load_certs(client_ca)
                    .with_context(|| format!("reading client CA {}", client_ca.display()))?,
            );
            let verifier = rustls::server::WebPkiClientVerifier::builder(roots.into());
            let verifier = if client_auth_required {
                verifier
            } else {
                verifier.allow_unauthenticated()
            };
            builder.with_client_cert_verifier(verifier.build()?)
        }
    };
    let mut config = builder.with_cert_resolver(certificate);
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// Completes the TLS handshake for each connection `incoming` yields, without letting a slow
/// handshake hold up the next accept. At most [`TLS_MAX_HANDSHAKES`] run at once, each for at most
/// [`TLS_HANDSHAKE_TIMEOUT`]. Dropping the returned stream stops accepting and abandons the
/// handshakes in progress.
pub fn tls_incoming<IO, E>(
    incoming: impl Stream<Item = Result<IO, E>> + Send + 'static,
    config: Arc<rustls::ServerConfig>,
) -> impl Stream<Item = Result<tokio_rustls::server::TlsStream<IO>, E>> + Send + 'static
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    E: Send + 'static,
{
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let (accepted, receiver) = tokio::sync::mpsc::channel(TLS_ACCEPT_BACKLOG);
    let handshakes = Arc::new(tokio::sync::Semaphore::new(TLS_MAX_HANDSHAKES));
    lore_spawn!(async move {
        tokio::pin!(incoming);
        loop {
            let next = tokio::select! {
                () = accepted.closed() => break,
                next = incoming.next() => next,
            };
            match next {
                None => break,
                Some(Err(error)) => {
                    if accepted.send(Err(error)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(io)) => {
                    let permit = tokio::select! {
                        () = accepted.closed() => break,
                        permit = handshakes.clone().acquire_owned() => permit,
                    };
                    let Ok(permit) = permit else { break };
                    let acceptor = acceptor.clone();
                    let accepted = accepted.clone();
                    lore_spawn!(async move {
                        let handshake =
                            tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(io));
                        let stream = tokio::select! {
                            () = accepted.closed() => return,
                            stream = handshake => stream,
                        };
                        drop(permit);
                        match stream {
                            Ok(Ok(stream)) => {
                                let _ = accepted.send(Ok(stream)).await;
                            }
                            Ok(Err(error)) => debug!("TLS handshake failed: {error}"),
                            Err(_elapsed) => debug!("TLS handshake timed out"),
                        }
                    });
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(receiver)
}

/// Handshaken connections waiting for the server to take them.
const TLS_ACCEPT_BACKLOG: usize = 128;

/// Handshakes a TLS listener runs at once; further connections wait to be accepted.
pub const TLS_MAX_HANDSHAKES: usize = 1024;

/// Time a client has to complete the TLS handshake before its connection is dropped.
pub const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
