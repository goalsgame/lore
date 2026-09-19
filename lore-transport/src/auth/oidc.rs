// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Interactive OIDC login: Authorization Code with PKCE over a loopback
//! redirect (GOALS fork).
//!
//! This is a plain, standards-only OpenID Connect client. Nothing in it is
//! specific to any one provider: the issuer, the client ID, the scopes and the
//! loopback port all come from the `oidc://` auth URL the server advertises in
//! `[environment.endpoint].auth_url`, and every endpoint it talks to comes from
//! that issuer's discovery document, fetched at run time.
//!
//! # Flow
//!
//! [`OidcAuthentication`] fits the existing start/poll shape of
//! [`Authentication`] without the orchestration in
//! `lore-revision/src/auth/login.rs` changing at all:
//!
//! - [`start_auth_session`](Authentication::start_auth_session) generates the
//!   PKCE verifier and the OAuth `state`, binds the loopback listener, fetches
//!   discovery, and returns the authorization URL for the caller to open in a
//!   browser (or print under `--no-browser`). The listener waits for its single
//!   callback on a background task, so the call returns immediately.
//! - [`poll_auth_session`](Authentication::poll_auth_session) checks that
//!   background task's mailbox without blocking. `Ok(None)` means "not yet",
//!   which is exactly what the caller's five-second poll loop expects. A
//!   callback that carries `error=` (the user declined consent, say) is a real
//!   failure and is reported as one rather than read as "not yet".
//!
//! # Why the loopback redirect rather than the device grant
//!
//! RFC 8252 §7.3. The authorization code never leaves the machine that started
//! the login: it arrives on `127.0.0.1` and is redeemed with a PKCE verifier
//! that only this process holds. The device grant (RFC 8628 §5.4) is phishable
//! by design — a user can be induced to approve a code an attacker initiated —
//! and the provider GOALS runs does not advertise a
//! `device_authorization_endpoint` for this client anyway.
//!
//! # Authorization tokens
//!
//! This is a Tier 1 deployment in the terms of
//! `docs/proposals/2026-08-20-oidc-oauth2-authentication.md`: one access token
//! for the Lore server, sent everywhere, with per-repository decisions made
//! server-side from the token's group claim. There is no RFC 8693 token
//! exchange broker, so
//! [`exchange_for_repository`](Authentication::exchange_for_repository) and
//! [`exchange_for_custom_resource`](Authentication::exchange_for_custom_resource)
//! pass the authentication token straight through. See those methods for why
//! that is the contract-correct answer rather than an error.

use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lore_base::error::NotAuthenticated;
use lore_base::error::NotAuthorized;
use lore_base::error::NotSupported;
use lore_base::lore_debug;
use lore_base::lore_spawn_net;
use lore_base::lore_trace;
use lore_base::types::RepositoryId;
use parking_lot::Mutex;
use ring::digest::SHA256;
use ring::digest::digest;
use ring::rand::SecureRandom;
use ring::rand::SystemRandom;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use url::Url;

use crate::error::ProtocolError;
use crate::traits::Authentication;
use crate::types::AuthSession;
use crate::types::AuthenticationToken;
use crate::types::AuthorizationToken;
use crate::types::ResolvedUser;

/// The auth URL scheme this implementation is registered under.
pub const SCHEME: &str = "oidc";

/// Appended to the auth URL's origin and path to reach the discovery document
/// (OpenID Connect Discovery 1.0 §4, RFC 8414 §3).
const DISCOVERY_SUFFIX: &str = "/.well-known/openid-configuration";

/// Loopback port used when the auth URL names none.
///
/// A redirect URI is registered at the provider ahead of time and an OAuth
/// provider matches it exactly, so this cannot be an ephemeral port the way
/// RFC 8252 §7.3 would otherwise prefer.
const DEFAULT_REDIRECT_PORT: u16 = 8765;

/// Loopback path used when the auth URL names none.
const DEFAULT_REDIRECT_PATH: &str = "/callback";

/// Scopes requested when the auth URL names none.
///
/// `offline_access` is in the default set because a conforming provider's
/// access tokens are short-lived, and without a refresh token every expiry
/// sends the user back through a browser login.
const DEFAULT_SCOPE: &str = "openid profile email offline_access";

/// Cap on a discovery or token response held in memory. Generous beside any
/// real document, so the only ones refused are ones no provider would send.
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Cap on the request head read from the loopback socket. A redirect carrying
/// an authorization code is a few hundred bytes; anything past this is not the
/// callback.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// How much of a rejected response body reaches the log. The body is whatever
/// the endpoint chose to send, so it is neither trustworthy nor necessarily
/// small.
const LOGGED_BODY_LIMIT: usize = 512;

/// How long the loopback listener waits for its callback before giving up and
/// releasing the port.
///
/// Longer than the caller's own poll budget on purpose: the listener releasing
/// the port first would turn a slow login into a confusing "port in use" on the
/// next attempt. This is the backstop for a session nobody ever polls again.
const CALLBACK_LISTEN_TIMEOUT: Duration = Duration::from_secs(600);

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Auth URL configuration
// ---------------------------------------------------------------------------

/// Everything this client needs, parsed out of the advertised auth URL.
///
/// Carrying the configuration in the auth URL keeps it where the rest of the
/// endpoint configuration already lives — one server-side
/// `[environment.endpoint].auth_url` string — rather than compiling a client ID
/// and an issuer into the binary. The auth URL is the only channel the existing
/// `Authentication` trait gives an implementation, since every method receives
/// it and nothing else about the deployment.
#[derive(Clone, Debug, PartialEq, Eq)]
struct OidcConfig {
    /// Absolute URL of the discovery document.
    discovery_url: String,
    /// Host every discovered endpoint must be served from.
    host: String,
    /// The public client ID this client presents.
    client_id: String,
    /// Space-separated scope list requested at authorization.
    scope: String,
    /// The pre-registered redirect URI, exactly as sent to the provider.
    redirect_uri: String,
    /// Loopback address the redirect URI resolves to.
    redirect_addr: SocketAddr,
    /// Path component of the redirect URI.
    redirect_path: String,
}

/// Parses an `oidc://` auth URL.
///
/// ```text
/// oidc://<discovery-host>[:port]/<discovery-path>?client_id=…&scope=…&redirect_port=…&redirect_path=…
/// ```
///
/// The origin and path name the *discovery base*: the document is fetched from
/// `https://<host>[:port]/<path>/.well-known/openid-configuration`. That is the
/// standard `<issuer>/.well-known/openid-configuration` construction for a
/// provider whose issuer is its discovery base, and it also covers providers
/// that serve a per-client discovery document at a path the issuer value does
/// not name. The scheme is `oidc` rather than `https` only so the registry can
/// tell this implementation apart from the legacy one; the fetch itself is
/// always HTTPS.
fn parse_auth_url(auth_url: &str) -> Result<OidcConfig, ProtocolError> {
    let url = Url::parse(auth_url)
        .map_err(|e| ProtocolError::internal(format!("invalid OIDC auth URL '{auth_url}': {e}")))?;

    if url.scheme() != SCHEME {
        return Err(ProtocolError::internal(format!(
            "OIDC auth URL must use the '{SCHEME}://' scheme, got '{}://'",
            url.scheme()
        )));
    }

    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| {
            ProtocolError::internal(format!("OIDC auth URL '{auth_url}' names no host"))
        })?
        .to_string();

    let authority = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.clone(),
    };
    // `Url::path()` is already percent-encoded, and the path percent-encode set
    // leaves characters such as `(` and `)` alone -- some providers put a
    // literal `(*)` in the per-client discovery path and reject the escaped
    // form, so the path has to survive verbatim.
    let path = url.path().trim_end_matches('/');

    let query: HashMap<String, String> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    let client_id = query
        .get("client_id")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ProtocolError::internal(format!(
                "OIDC auth URL '{auth_url}' has no 'client_id' query parameter"
            ))
        })?
        .clone();

    let scope = query
        .get("scope")
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| DEFAULT_SCOPE.to_string());

    let redirect_port = match query.get("redirect_port") {
        Some(value) => value.parse::<u16>().map_err(|e| {
            ProtocolError::internal(format!(
                "OIDC auth URL '{auth_url}' has an invalid 'redirect_port' ('{value}'): {e}"
            ))
        })?,
        None => DEFAULT_REDIRECT_PORT,
    };
    if redirect_port == 0 {
        return Err(ProtocolError::internal(
            "OIDC auth URL 'redirect_port' must not be 0: the redirect URI is registered at the \
             provider ahead of time, so the port cannot be chosen at run time"
                .to_string(),
        ));
    }

    let redirect_path = match query.get("redirect_path") {
        Some(value) if value.starts_with('/') => value.clone(),
        Some(value) => format!("/{value}"),
        None => DEFAULT_REDIRECT_PATH.to_string(),
    };

    // RFC 8252 §8.3: the IP literal, never `localhost`, which can resolve to an
    // interface other than the loopback one.
    let redirect_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), redirect_port);

    Ok(OidcConfig {
        discovery_url: format!("https://{authority}{path}{DISCOVERY_SUFFIX}"),
        host,
        client_id,
        scope,
        redirect_uri: format!("http://127.0.0.1:{redirect_port}{redirect_path}"),
        redirect_addr,
        redirect_path,
    })
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// The parts of a discovery document this client reads.
#[derive(Clone, Debug, Deserialize)]
struct Discovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    #[serde(default)]
    code_challenge_methods_supported: Vec<String>,
}

/// Checks a discovery document against the auth URL it was fetched from.
///
/// The document decides where an authorization code and, worse, a code exchange
/// are sent, so a substituted one is the whole attack. Pinning every endpoint to
/// the host the document itself came from is what stops that: an attacker who
/// can serve this host already has the tokens.
fn validate_discovery(discovery: &Discovery, config: &OidcConfig) -> Result<(), ProtocolError> {
    let same_host = |what: &str, endpoint: &str| -> Result<(), ProtocolError> {
        let url = Url::parse(endpoint).map_err(|e| {
            ProtocolError::internal(format!(
                "OIDC discovery at {} has a {what} that is not a URL ('{endpoint}'): {e}",
                config.discovery_url
            ))
        })?;
        if url.scheme() != "https" {
            return Err(ProtocolError::internal(format!(
                "OIDC discovery at {} names a non-HTTPS {what} ('{endpoint}')",
                config.discovery_url
            )));
        }
        if url.host_str() != Some(config.host.as_str()) {
            return Err(ProtocolError::internal(format!(
                "OIDC discovery at {} names a {what} on a different host ('{endpoint}'); \
                 refusing to follow it off '{}'",
                config.discovery_url, config.host
            )));
        }
        Ok(())
    };

    same_host("issuer", &discovery.issuer)?;
    same_host("authorization_endpoint", &discovery.authorization_endpoint)?;
    same_host("token_endpoint", &discovery.token_endpoint)?;

    // An empty list is an omission, not a statement -- PKCE support is not
    // advertised by every provider that has it. A non-empty list that leaves
    // S256 out is a statement, and downgrading to `plain` is not on offer.
    if !discovery.code_challenge_methods_supported.is_empty()
        && !discovery
            .code_challenge_methods_supported
            .iter()
            .any(|method| method == "S256")
    {
        return Err(ProtocolError::internal(format!(
            "OIDC provider at {} does not support the S256 PKCE challenge method (advertised: \
             {:?})",
            config.discovery_url, discovery.code_challenge_methods_supported
        )));
    }

    Ok(())
}

type DiscoveryCache = Mutex<HashMap<String, Arc<Discovery>>>;

fn discovery_cache() -> &'static DiscoveryCache {
    static CACHE: OnceLock<DiscoveryCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Fetches and caches the discovery document for one auth URL.
///
/// Cached per discovery URL for the life of the process: every trait method
/// needs the endpoints, and re-fetching on each one would put an extra round
/// trip on the provider in front of every token operation. Two callers racing
/// on a cold cache both fetch and the second overwrites the first with an
/// identical document, which is cheaper than holding a lock across the network.
async fn discovery_for(config: &OidcConfig) -> Result<Arc<Discovery>, ProtocolError> {
    if let Some(cached) = discovery_cache().lock().get(&config.discovery_url).cloned() {
        return Ok(cached);
    }

    lore_debug!("Fetching OIDC discovery from {}", config.discovery_url);
    let body = http_get(&config.discovery_url).await?;
    let discovery: Discovery = serde_json::from_str(&body).map_err(|e| {
        ProtocolError::internal(format!(
            "OIDC discovery at {} is not a discovery document: {e} ({})",
            config.discovery_url,
            body_excerpt(&body)
        ))
    })?;
    validate_discovery(&discovery, config)?;

    let discovery = Arc::new(discovery);
    discovery_cache()
        .lock()
        .insert(config.discovery_url.clone(), discovery.clone());
    Ok(discovery)
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// The head of a response body, for diagnostics.
fn body_excerpt(body: &str) -> String {
    match body.char_indices().nth(LOGGED_BODY_LIMIT) {
        Some((end, _)) => format!("{}… ({} bytes total)", &body[..end], body.len()),
        None => body.to_string(),
    }
}

/// One pooled client for every fetch, following no redirects.
///
/// Redirects are refused rather than followed because both fetches here are
/// security decisions about *where*: a discovery document that can bounce the
/// client to another origin, or a token request that can be replayed onto one,
/// is the same hole the endpoint host pinning closes.
fn http_client() -> Result<&'static reqwest::Client, ProtocolError> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_webpki_certs(true)
        .tls_built_in_native_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(crate::user_agent())
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
        .build()
        .map_err(|e| {
            ProtocolError::internal(format!("failed to build the OIDC HTTP client: {e}"))
        })?;
    Ok(CLIENT.get_or_init(|| client))
}

/// Reads a response body, refusing anything past [`MAX_RESPONSE_BYTES`].
///
/// `Content-Length` is consulted first when the endpoint offers one, but it is
/// a claim rather than a fact, so the accumulating read is what enforces the
/// cap.
async fn read_capped_body(response: &mut reqwest::Response) -> Result<String, ProtocolError> {
    if let Some(declared) = response.content_length()
        && declared > MAX_RESPONSE_BYTES as u64
    {
        return Err(ProtocolError::internal(format!(
            "OIDC response declares {declared} bytes, over the {MAX_RESPONSE_BYTES} byte cap"
        )));
    }

    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| ProtocolError::internal(format!("failed to read the OIDC response: {e}")))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(ProtocolError::internal(format!(
                "OIDC response exceeded the {MAX_RESPONSE_BYTES} byte cap"
            )));
        }
        body.extend_from_slice(&chunk);
    }

    String::from_utf8(body)
        .map_err(|e| ProtocolError::internal(format!("OIDC response was not valid UTF-8: {e}")))
}

async fn http_get(url: &str) -> Result<String, ProtocolError> {
    let mut response = http_client()?
        .get(url)
        .send()
        .await
        .map_err(|e| ProtocolError::internal(format!("GET {url} failed: {e}")))?;
    let status = response.status();
    let body = read_capped_body(&mut response).await?;
    if !status.is_success() {
        return Err(ProtocolError::internal(format!(
            "GET {url} returned {status}: {}",
            body_excerpt(&body)
        )));
    }
    Ok(body)
}

// ---------------------------------------------------------------------------
// PKCE
// ---------------------------------------------------------------------------

/// Returns `bytes` bytes of cryptographically secure randomness, base64url
/// encoded without padding.
///
/// Every character of the output is in base64url's alphabet, which is a subset
/// of RFC 3986's unreserved set, so the result is a valid PKCE `code_verifier`
/// and needs no escaping anywhere it is used.
fn random_token(bytes: usize) -> Result<String, ProtocolError> {
    let mut buffer = vec![0u8; bytes];
    SystemRandom::new()
        .fill(&mut buffer)
        .map_err(|_| ProtocolError::internal("failed to read the system random source"))?;
    Ok(URL_SAFE_NO_PAD.encode(&buffer))
}

/// Generates a PKCE `code_verifier` (RFC 7636 §4.1).
///
/// 32 random bytes encode to 43 base64url characters, the minimum the RFC
/// allows and 256 bits of entropy — well past the 128-bit floor §7.1 asks for.
fn generate_code_verifier() -> Result<String, ProtocolError> {
    random_token(32)
}

/// Derives the S256 `code_challenge` for a verifier (RFC 7636 §4.2).
fn code_challenge_s256(code_verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, code_verifier.as_bytes()).as_ref())
}

/// Builds the authorization request URL (RFC 6749 §4.1.1, RFC 7636 §4.3).
fn authorization_url(
    discovery: &Discovery,
    config: &OidcConfig,
    state: &str,
    code_challenge: &str,
) -> Result<String, ProtocolError> {
    let mut url = Url::parse(&discovery.authorization_endpoint).map_err(|e| {
        ProtocolError::internal(format!(
            "the authorization_endpoint is not a URL ('{}'): {e}",
            discovery.authorization_endpoint
        ))
    })?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", &config.redirect_uri)
        .append_pair("scope", &config.scope)
        .append_pair("state", state)
        .append_pair("code_challenge", code_challenge)
        .append_pair("code_challenge_method", "S256");
    Ok(url.into())
}

// ---------------------------------------------------------------------------
// Loopback callback listener
// ---------------------------------------------------------------------------

/// What the browser delivered to the loopback listener.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Callback {
    /// An authorization response (RFC 6749 §4.1.2).
    Code { code: String, state: String },
    /// An authorization error response (RFC 6749 §4.1.2.1). The user declining
    /// consent arrives here, and it is a failure rather than "not yet".
    Error {
        error: String,
        description: Option<String>,
        state: Option<String>,
    },
    /// The listener itself gave up: it timed out, or the socket failed.
    Failed(String),
}

/// Mailbox a pending session's listener drops its single result into.
type CallbackSlot = Arc<Mutex<Option<Callback>>>;

const CALLBACK_PAGE: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<title>Lore</title></head><body style=\"font-family:system-ui,sans-serif;margin:4rem auto;\
max-width:32rem\"><h1>Signed in</h1><p>Lore has received your login. You can close this tab \
and return to the terminal.</p></body></html>";

const ERROR_PAGE: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<title>Lore</title></head><body style=\"font-family:system-ui,sans-serif;margin:4rem auto;\
max-width:32rem\"><h1>Sign-in failed</h1><p>Return to the terminal for the details.</p>\
</body></html>";

fn http_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Extracts the request target from an HTTP request head.
///
/// Only `GET` is accepted: the redirect that carries an authorization code is a
/// `GET`, and nothing else has any business on this socket.
fn request_target(head: &str) -> Option<&str> {
    let request_line = head.lines().next()?;
    let mut parts = request_line.split(' ');
    let method = parts.next()?;
    let target = parts.next()?;
    (method == "GET").then_some(target)
}

/// Reads the authorization response out of a redirect's request target.
///
/// Returns `None` for a target on another path — a browser fetching
/// `/favicon.ico` alongside the redirect must not be mistaken for the callback.
fn callback_from_target(target: &str, expected_path: &str) -> Option<Callback> {
    // The target is origin-form (RFC 9112 §3.2.1); a base is needed only to
    // parse it, and is never used for anything else.
    let url = Url::parse("http://127.0.0.1").ok()?.join(target).ok()?;
    if url.path() != expected_path {
        return None;
    }

    let query: HashMap<String, String> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    if let Some(error) = query.get("error") {
        return Some(Callback::Error {
            error: error.clone(),
            description: query.get("error_description").cloned(),
            state: query.get("state").cloned(),
        });
    }

    match (query.get("code"), query.get("state")) {
        (Some(code), Some(state)) => Some(Callback::Code {
            code: code.clone(),
            state: state.clone(),
        }),
        // A request on the callback path with neither an error nor a code is
        // not an authorization response. Treat it as noise rather than as a
        // failure, so a stray probe cannot cancel a login in flight.
        _ => None,
    }
}

/// Reads one request head off the socket, answers it, and returns its target.
async fn read_request(stream: &mut TcpStream) -> Result<String, String> {
    let mut head = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|e| format!("failed to read the loopback callback request: {e}"))?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&buffer[..read]);
        if head.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if head.len() > MAX_REQUEST_BYTES {
            return Err(format!(
                "the loopback callback request exceeded {MAX_REQUEST_BYTES} bytes"
            ));
        }
    }
    String::from_utf8(head).map_err(|e| format!("the loopback callback request was not UTF-8: {e}"))
}

/// Serves the loopback listener until the authorization response arrives.
///
/// Requests that are not the callback are answered `404` and the listener keeps
/// waiting: browsers routinely fetch `/favicon.ico` against the redirect
/// origin, and letting that end the login would make the flow flaky for no
/// reason.
async fn serve_callback(listener: TcpListener, slot: CallbackSlot, expected_path: String) {
    let outcome = match tokio::time::timeout(
        CALLBACK_LISTEN_TIMEOUT,
        accept_callback(&listener, &expected_path),
    )
    .await
    {
        Ok(callback) => callback,
        Err(_) => Callback::Failed(format!(
            "no authorization response reached the loopback listener within {} seconds",
            CALLBACK_LISTEN_TIMEOUT.as_secs()
        )),
    };
    *slot.lock() = Some(outcome);
}

async fn accept_callback(listener: &TcpListener, expected_path: &str) -> Callback {
    loop {
        let mut stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                return Callback::Failed(format!("the loopback listener failed to accept: {e}"));
            }
        };

        let head = match read_request(&mut stream).await {
            Ok(head) => head,
            Err(e) => return Callback::Failed(e),
        };

        let callback =
            request_target(&head).and_then(|target| callback_from_target(target, expected_path));

        let response = match &callback {
            Some(Callback::Code { .. }) => http_response("200 OK", CALLBACK_PAGE),
            Some(_) => http_response("400 Bad Request", ERROR_PAGE),
            None => http_response("404 Not Found", ERROR_PAGE),
        };
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;

        if let Some(callback) = callback {
            return callback;
        }
    }
}

// ---------------------------------------------------------------------------
// Pending sessions
// ---------------------------------------------------------------------------

/// One interactive login between `start_auth_session` and the poll that
/// completes it.
struct PendingSession {
    client_state: String,
    /// The OAuth `state` this session sent, to be matched against the one that
    /// comes back (RFC 6749 §10.12).
    oauth_state: String,
    code_verifier: String,
    config: OidcConfig,
    discovery: Arc<Discovery>,
    slot: CallbackSlot,
    listener: tokio::task::AbortHandle,
    /// When the listener gives up, after which the entry is garbage.
    expires_at: Instant,
}

impl Drop for PendingSession {
    fn drop(&mut self) {
        // Releases the loopback port: the listener owns it, and nothing else
        // can bind it until that task is gone.
        self.listener.abort();
    }
}

type SessionMap = Mutex<HashMap<String, PendingSession>>;

fn sessions() -> &'static SessionMap {
    static SESSIONS: OnceLock<SessionMap> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Drops sessions whose listener has given up.
///
/// The poll loop that drives a login has no way to tell an implementation that
/// it stopped polling, so a login the user abandons would otherwise hold the
/// loopback port for the life of the process and make the next attempt fail to
/// bind.
fn purge_expired_sessions() {
    let now = Instant::now();
    sessions()
        .lock()
        .retain(|_, session| session.expires_at > now);
}

// ---------------------------------------------------------------------------
// Token endpoint
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// Maps an OAuth error response to the closest protocol error (RFC 6749 §5.2).
///
/// The typed variants carry no message of their own, so the provider's own
/// wording is logged rather than lost: callers that branch on the variant get
/// what they need, and `-v` still shows what the provider actually said.
fn token_error(body: &str, status: reqwest::StatusCode) -> ProtocolError {
    let Ok(parsed) = serde_json::from_str::<TokenErrorResponse>(body) else {
        return ProtocolError::internal(format!(
            "the OIDC token endpoint returned {status}: {}",
            body_excerpt(body)
        ));
    };
    let detail = match &parsed.error_description {
        Some(description) => format!("{} ({description})", parsed.error),
        None => parsed.error.clone(),
    };
    lore_debug!("The OIDC token endpoint returned {status}: {detail}");
    match parsed.error.as_str() {
        "access_denied" | "unauthorized_client" | "insufficient_scope" => {
            ProtocolError::from(NotAuthorized)
        }
        "invalid_grant" => ProtocolError::from(NotAuthenticated),
        _ => ProtocolError::internal(format!("the OIDC token request failed: {detail}")),
    }
}

/// Posts a form-encoded grant to the token endpoint (RFC 6749 §3.2).
///
/// No client secret is sent. This is a public client (RFC 6749 §2.1): the
/// binary ships to every developer, so a secret in it would not be one, and
/// PKCE is what authenticates the exchange instead.
async fn post_token_request(
    token_endpoint: &str,
    form: &[(&str, &str)],
) -> Result<TokenResponse, ProtocolError> {
    let mut response = http_client()?
        .post(token_endpoint)
        .form(form)
        .send()
        .await
        .map_err(|e| {
            ProtocolError::internal(format!(
                "the OIDC token request to {token_endpoint} failed: {e}"
            ))
        })?;

    let status = response.status();
    let body = read_capped_body(&mut response).await?;
    if !status.is_success() {
        return Err(token_error(&body, status));
    }

    serde_json::from_str::<TokenResponse>(&body).map_err(|e| {
        ProtocolError::internal(format!(
            "the OIDC token endpoint returned an unreadable response: {e} ({})",
            body_excerpt(&body)
        ))
    })
}

// ---------------------------------------------------------------------------
// Claims
// ---------------------------------------------------------------------------

/// The claims this client reads out of a token it already holds.
///
/// Decoded without verifying the signature, which is deliberate and safe here:
/// the token arrived over TLS from the token endpoint this client pinned, and
/// nothing security-relevant is decided from these values — the server verifies
/// the signature, the issuer and the audience before it honours anything. This
/// mirrors `lore_credential::insecure_decode_token`, and uses the same
/// `jsonwebtoken` crate the server verifies with, rather than adding a second
/// JWT library to the workspace.
#[derive(Debug, Default, Deserialize)]
struct TokenClaims {
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    given_name: Option<String>,
    #[serde(default)]
    family_name: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    exp: Option<u64>,
}

impl TokenClaims {
    /// The best display name the token offers, falling back to the subject.
    ///
    /// A conforming provider need not send `name`: OpenID Connect Core §5.1
    /// makes every profile claim optional, and plenty of providers send only
    /// the parts.
    fn display_name(&self) -> String {
        if let Some(name) = self.name.as_ref().filter(|name| !name.is_empty()) {
            return name.clone();
        }
        let full = [self.given_name.as_deref(), self.family_name.as_deref()]
            .into_iter()
            .flatten()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if !full.is_empty() {
            return full;
        }
        for claim in [self.preferred_username.as_ref(), self.email.as_ref()] {
            if let Some(value) = claim.filter(|value| !value.is_empty()) {
                return value.clone();
            }
        }
        self.sub.clone().unwrap_or_default()
    }
}

/// Decodes a JWT's claims without verifying its signature.
fn decode_claims(token: &str) -> Result<TokenClaims, ProtocolError> {
    let header = jsonwebtoken::decode_header(token)
        .map_err(|e| ProtocolError::internal(format!("unreadable JWT header: {e}")))?;
    let key = jsonwebtoken::DecodingKey::from_secret(&[]);
    let mut validation = jsonwebtoken::Validation::new(header.alg);
    validation.insecure_disable_signature_validation();
    validation.required_spec_claims.clear();
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    jsonwebtoken::decode::<TokenClaims>(token, &key, &validation)
        .map(|data| data.claims)
        .map_err(|e| ProtocolError::internal(format!("unreadable JWT claims: {e}")))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The domains the access token may be presented to.
///
/// Derived from the token's own claims by `lore-credential`, which is also what
/// the orchestration layer does with it, so an implementation that filled this
/// in differently would only disagree with the value that actually gets stored.
fn acceptable_root_domains(token: &str) -> Vec<String> {
    lore_credential::insecure_decode_token(token)
        .map(|decoded| decoded.claims.acceptable_root_domains())
        .unwrap_or_default()
}

/// Turns a token response into the authentication token the caller stores.
fn authentication_token(response: TokenResponse) -> Result<AuthenticationToken, ProtocolError> {
    let claims = decode_claims(&response.access_token)?;
    let user_id = claims
        .sub
        .clone()
        .filter(|sub| !sub.is_empty())
        .ok_or_else(|| {
            ProtocolError::internal(
                "the OIDC access token carries no 'sub' claim, so it identifies nobody",
            )
        })?;

    // `exp` is authoritative where the token has one; `expires_in` is the
    // fallback, and it is relative to now rather than to the token's issuance.
    let expires_ms = match claims.exp {
        Some(exp) => exp.saturating_mul(1000),
        None => {
            now_ms().saturating_add(response.expires_in.unwrap_or_default().saturating_mul(1000))
        }
    };

    Ok(AuthenticationToken {
        user_name: claims.display_name(),
        user_id,
        expires_ms,
        acceptable_root_domains: acceptable_root_domains(&response.access_token),
        refresh_token: response.refresh_token,
        token: response.access_token,
    })
}

// ---------------------------------------------------------------------------
// The implementation
// ---------------------------------------------------------------------------

/// Standards-only OIDC authentication, registered under the [`SCHEME`] scheme.
#[derive(Default)]
pub struct OidcAuthentication;

#[async_trait]
impl Authentication for OidcAuthentication {
    /// Starts the browser flow and returns the URL to visit.
    ///
    /// The loopback listener is bound here, before the URL is handed back, so a
    /// port that cannot be bound is reported before a browser opens rather than
    /// after the user has authenticated into a redirect that goes nowhere.
    async fn start_auth_session(
        &self,
        auth_url: &str,
        client_state: &str,
        _correlation_id: &str,
    ) -> Result<AuthSession, ProtocolError> {
        purge_expired_sessions();

        let config = parse_auth_url(auth_url)?;
        let discovery = discovery_for(&config).await?;

        let code_verifier = generate_code_verifier()?;
        let code_challenge = code_challenge_s256(&code_verifier);
        let oauth_state = random_token(32)?;
        let session_code = random_token(16)?;

        // Built before the port is bound: a failure here would otherwise leave
        // a listener nothing is tracking holding the port.
        let login_url = authorization_url(&discovery, &config, &oauth_state, &code_challenge)?;

        // Bound synchronously, and as a `std` listener, for two reasons: the
        // bind result is needed before this call returns, and a tokio listener
        // belongs to the reactor it was created on, which is not the runtime
        // the serving task ends up on.
        let std_listener = std::net::TcpListener::bind(config.redirect_addr).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                ProtocolError::internal(format!(
                    "cannot listen on {} for the login redirect: the port is already in use. \
                     The redirect URI is registered with the identity provider, so this port \
                     cannot be chosen at run time -- close whatever is holding it (another \
                     `lore login` in progress, most likely) and try again.",
                    config.redirect_addr
                ))
            } else {
                ProtocolError::internal(format!(
                    "cannot listen on {} for the login redirect: {e}",
                    config.redirect_addr
                ))
            }
        })?;
        std_listener.set_nonblocking(true).map_err(|e| {
            ProtocolError::internal(format!("cannot prepare the login redirect listener: {e}"))
        })?;

        let slot: CallbackSlot = Arc::new(Mutex::new(None));
        let listener_slot = slot.clone();
        let expected_path = config.redirect_path.clone();
        let listener = lore_spawn_net!(async move {
            match TcpListener::from_std(std_listener) {
                Ok(listener) => serve_callback(listener, listener_slot, expected_path).await,
                Err(e) => {
                    *listener_slot.lock() = Some(Callback::Failed(format!(
                        "cannot start the login redirect listener: {e}"
                    )));
                }
            }
        });

        sessions().lock().insert(
            session_code.clone(),
            PendingSession {
                client_state: client_state.to_string(),
                oauth_state,
                code_verifier,
                config,
                discovery,
                slot,
                listener: listener.abort_handle(),
                expires_at: Instant::now() + CALLBACK_LISTEN_TIMEOUT,
            },
        );

        Ok(AuthSession {
            session_code,
            login_url,
        })
    }

    /// Checks, without blocking, whether the browser has come back yet.
    ///
    /// `Ok(None)` is "not yet", which is what keeps the caller's poll loop
    /// running. Everything else is terminal: a mismatched `state`, an
    /// authorization error, or a failed exchange all end the login here rather
    /// than being left to time out.
    async fn poll_auth_session(
        &self,
        auth_url: &str,
        client_state: &str,
        session_code: &str,
        _correlation_id: &str,
    ) -> Result<Option<AuthenticationToken>, ProtocolError> {
        // Everything needed for the exchange is copied out under the lock, and
        // the entry is left in place: the network round trip must not hold a
        // lock, and a session is only removed once it is finished with.
        let (callback, oauth_state, code_verifier, config, discovery) = {
            let map = sessions().lock();
            let session = map.get(session_code).ok_or_else(|| {
                ProtocolError::internal(
                    "no login session is in flight for this session code; it has already \
                     completed, or the listener gave up waiting",
                )
            })?;
            if session.client_state != client_state {
                return Err(ProtocolError::internal(
                    "the login session's client state does not match",
                ));
            }
            let Some(callback) = session.slot.lock().clone() else {
                lore_trace!("OIDC callback has not arrived yet");
                return Ok(None);
            };
            (
                callback,
                session.oauth_state.clone(),
                session.code_verifier.clone(),
                session.config.clone(),
                session.discovery.clone(),
            )
        };

        // Any outcome below is terminal, so the session (and with it the
        // loopback port) is released before the result is reported.
        let finish = |result: Result<Option<AuthenticationToken>, ProtocolError>| {
            sessions().lock().remove(session_code);
            result
        };

        let (code, state) = match callback {
            Callback::Code { code, state } => (code, state),
            Callback::Error {
                error, description, ..
            } => {
                let detail = match description {
                    Some(description) => format!("{error} ({description})"),
                    None => error.clone(),
                };
                // Reported as a plain failure with the provider's own wording:
                // nothing branches on the variant here, and "the login was
                // declined" is what the person at the terminal needs to read.
                return finish(Err(ProtocolError::internal(format!(
                    "the identity provider did not complete the login: {detail}"
                ))));
            }
            Callback::Failed(reason) => {
                return finish(Err(ProtocolError::internal(reason)));
            }
        };

        // RFC 6749 §10.12. A response whose `state` is not the one this session
        // sent is somebody else's, and its code is not redeemed.
        if state != oauth_state {
            return finish(Err(ProtocolError::internal(
                "the login response carried the wrong 'state'; refusing to redeem its \
                 authorization code",
            )));
        }

        lore_debug!(
            "Redeeming the OIDC authorization code at {}",
            discovery.token_endpoint
        );
        let response = match post_token_request(
            &discovery.token_endpoint,
            &[
                ("grant_type", "authorization_code"),
                ("code", &code),
                ("redirect_uri", &config.redirect_uri),
                ("client_id", &config.client_id),
                ("code_verifier", &code_verifier),
            ],
        )
        .await
        {
            Ok(response) => response,
            Err(e) => return finish(Err(e)),
        };

        lore_debug!("OIDC login for {auth_url} complete");
        finish(authentication_token(response).map(Some))
    }

    /// Not available: exchanging a foreign token for one of the provider's is
    /// RFC 8693 token exchange, which needs a broker this deployment does not
    /// run, and which the provider does not advertise.
    ///
    /// `lore login --token` with `--token-type lore` does not reach here — the
    /// orchestration layer validates and stores such a token directly — so this
    /// only refuses the cases that genuinely have no answer.
    async fn exchange_external_token(
        &self,
        _auth_url: &str,
        _token: &str,
        token_type: &str,
        _correlation_id: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(ProtocolError::from(NotSupported {
            operation: format!(
                "exchanging a '{token_type}' token: this OIDC deployment runs no RFC 8693 token \
                 exchange endpoint. Use `lore login` for an interactive login, or \
                 `lore login --token <jwt> --token-type lore` with a token the provider already \
                 issued."
            ),
        }))
    }

    /// Redeems a refresh token for a new access token (RFC 6749 §6).
    ///
    /// Providers differ on rotation: some return a replacement refresh token
    /// with every response and invalidate the one presented, some return none
    /// and keep the original valid. Both are handled by carrying the presented
    /// token forward when the response omits one, so the returned
    /// `refresh_token` is always the one to store next.
    async fn refresh_authentication(
        &self,
        auth_url: &str,
        refresh_token: &str,
        _correlation_id: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        let config = parse_auth_url(auth_url)?;
        let discovery = discovery_for(&config).await?;

        let mut response = post_token_request(
            &discovery.token_endpoint,
            &[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
                ("client_id", &config.client_id),
            ],
        )
        .await?;

        if response.refresh_token.is_none() {
            response.refresh_token = Some(refresh_token.to_string());
        }
        authentication_token(response)
    }

    /// Returns the authentication token unchanged, scoped to nothing further.
    ///
    /// This deployment is Tier 1: one access token reaches every service, and
    /// the server decides per repository from the token's group claim rather
    /// than from a repository-scoped token. There is no RFC 8693 broker to
    /// exchange against and no resource indicator the provider would honour, so
    /// the honest implementation of "exchange for this repository" is to hand
    /// back the same bearer.
    ///
    /// Failing instead would be wrong rather than merely conservative: every
    /// connection runs `auth::exchange::exchange` before it opens, so a
    /// `NotSupported` here would make every repository operation fail, not just
    /// the ones that want narrower scope.
    async fn exchange_for_repository(
        &self,
        auth_url: &str,
        authn_token: &str,
        repository: RepositoryId,
        correlation_id: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        self.exchange_for_custom_resource(
            auth_url,
            authn_token,
            &repository.to_string(),
            correlation_id,
        )
        .await
    }

    /// Returns the authentication token unchanged. See
    /// [`exchange_for_repository`](Self::exchange_for_repository) for why.
    async fn exchange_for_custom_resource(
        &self,
        _auth_url: &str,
        authn_token: &str,
        resource_id: &str,
        _correlation_id: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        lore_trace!(
            "OIDC deployment is Tier 1; passing the access token through for {resource_id}"
        );
        let claims = decode_claims(authn_token)?;
        Ok(AuthorizationToken {
            expires_ms: claims.exp.unwrap_or_default().saturating_mul(1000),
            acceptable_root_domains: acceptable_root_domains(authn_token),
            token: authn_token.to_string(),
        })
    }

    /// Resolves what the bearer's own token says, and nothing else.
    ///
    /// OIDC has no directory: `/userinfo` describes only the bearer, so a live
    /// call could not answer for anybody else either, and would cost a round
    /// trip to learn what this token already carries. Identities other than the
    /// bearer's are simply not returned — the CLI already falls back to printing
    /// the raw identifier for anyone it cannot name.
    async fn get_user_info(
        &self,
        _auth_url: &str,
        authz_token: &str,
        _repository: RepositoryId,
        user_ids: &[String],
        _correlation_id: &str,
    ) -> Result<Vec<ResolvedUser>, ProtocolError> {
        let claims = decode_claims(authz_token)?;
        let Some(subject) = claims.sub.clone().filter(|sub| !sub.is_empty()) else {
            return Ok(Vec::new());
        };
        Ok(user_ids
            .iter()
            .filter(|user_id| **user_id == subject)
            .map(|user_id| ResolvedUser {
                user_id: user_id.clone(),
                user_name: claims.display_name(),
            })
            .collect())
    }

    /// The reverse lookup, answerable only for the bearer. See
    /// [`get_user_info`](Self::get_user_info).
    async fn get_user_id(
        &self,
        _auth_url: &str,
        authz_token: &str,
        _repository: RepositoryId,
        display_name: &str,
        _correlation_id: &str,
    ) -> Result<Option<ResolvedUser>, ProtocolError> {
        let claims = decode_claims(authz_token)?;
        let Some(subject) = claims.sub.clone().filter(|sub| !sub.is_empty()) else {
            return Ok(None);
        };
        let matches = [
            Some(claims.display_name()),
            claims.preferred_username.clone(),
            claims.email.clone(),
            Some(subject.clone()),
        ];
        if matches
            .into_iter()
            .flatten()
            .any(|candidate| candidate == display_name)
        {
            return Ok(Some(ResolvedUser {
                user_id: subject,
                user_name: claims.display_name(),
            }));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The GOALS deployment's auth URL, in the shape an operator writes it.
    const TEST_AUTH_URL: &str = "oidc://idp.example.com/-/tenant-id\
?client_id=lore-cli\
&scope=openid+profile+email+groups+offline_access+lore-server%3Aaccess";

    fn goals_config() -> OidcConfig {
        parse_auth_url(TEST_AUTH_URL).expect("the test auth URL parses")
    }

    fn goals_discovery() -> Discovery {
        serde_json::from_str(TEST_DISCOVERY).expect("the discovery document parses")
    }

    /// Verbatim from the deployment's provider, trimmed to the keys read here
    /// plus a few that are not.
    const TEST_DISCOVERY: &str = r#"{
        "issuer": "https://idp.example.com/-/",
        "authorization_endpoint": "https://idp.example.com/-/tenant-id/oauth/authorize",
        "token_endpoint": "https://idp.example.com/-/tenant-id/oauth/token",
        "userinfo_endpoint": "https://idp.example.com/-/tenant-id/oauth/userinfo",
        "end_session_endpoint": "https://idp.example.com/-/tenant-id/oauth/endsession",
        "jwks_uri": "https://idp.example.com/-/tenant-id/.well-known/openid-configuration/keys",
        "scopes_supported": ["openid", "email", "lore-server:access", "offline_access", "profile"],
        "response_types_supported": ["code"],
        "code_challenge_methods_supported": ["plain", "S256"]
    }"#;

    // -- auth URL parsing ---------------------------------------------------

    #[test]
    fn the_discovery_url_is_the_auth_urls_origin_and_path() {
        let config = goals_config();
        assert_eq!(
            config.discovery_url,
            "https://idp.example.com/-/tenant-id/.well-known/openid-configuration"
        );
        assert_eq!(config.host, "idp.example.com");
        assert_eq!(config.client_id, "lore-cli");
    }

    /// The provider serves its per-client discovery path with a literal `(*)`
    /// and rejects the percent-encoded form, so the path must survive parsing
    /// and re-serialization untouched.
    #[test]
    fn a_parenthesised_path_segment_is_not_escaped() {
        let config = goals_config();
        assert!(
            config.discovery_url.contains("tenant-id"),
            "the literal path segment survives: {}",
            config.discovery_url
        );
        assert!(!config.discovery_url.contains("%28"));
        assert!(!config.discovery_url.contains("%2A"));
    }

    #[test]
    fn scopes_come_from_the_auth_url_and_keep_their_separators() {
        let config = goals_config();
        assert_eq!(
            config.scope,
            "openid profile email groups offline_access lore-server:access"
        );
    }

    #[test]
    fn the_redirect_uri_defaults_to_the_registered_loopback() {
        let config = goals_config();
        assert_eq!(config.redirect_uri, "http://127.0.0.1:8765/callback");
        assert_eq!(config.redirect_addr, "127.0.0.1:8765".parse().unwrap());
        assert_eq!(config.redirect_path, "/callback");
    }

    #[test]
    fn the_redirect_port_and_path_are_configurable() {
        let config = parse_auth_url(
            "oidc://idp.example.com/realm?client_id=lore&redirect_port=9999&redirect_path=cb",
        )
        .expect("an auth URL with an explicit redirect parses");
        assert_eq!(config.redirect_uri, "http://127.0.0.1:9999/cb");
        assert_eq!(config.redirect_path, "/cb");
    }

    #[test]
    fn the_scope_defaults_to_the_standard_set_with_offline_access() {
        let config = parse_auth_url("oidc://idp.example.com/realm?client_id=lore")
            .expect("an auth URL without scopes parses");
        assert_eq!(config.scope, DEFAULT_SCOPE);
        assert!(config.scope.contains("offline_access"));
    }

    #[test]
    fn a_port_in_the_auth_url_reaches_the_discovery_url() {
        let config = parse_auth_url("oidc://idp.example.com:8443/realm?client_id=lore")
            .expect("an auth URL with a port parses");
        assert_eq!(
            config.discovery_url,
            "https://idp.example.com:8443/realm/.well-known/openid-configuration"
        );
    }

    #[test]
    fn a_trailing_slash_does_not_double_up_in_the_discovery_url() {
        let config = parse_auth_url("oidc://idp.example.com/realm/?client_id=lore")
            .expect("an auth URL with a trailing slash parses");
        assert_eq!(
            config.discovery_url,
            "https://idp.example.com/realm/.well-known/openid-configuration"
        );
    }

    /// The token store keys entries by the auth URL and files them under the
    /// domain it parses out of it, so an `oidc://` URL has to yield the
    /// provider's host the same way an `https://` one does. If it did not, a
    /// stored token would never be found again.
    #[test]
    fn the_auth_url_yields_the_providers_domain_to_the_token_store() {
        assert_eq!(
            lore_credential::get_domain_or_empty(TEST_AUTH_URL),
            "idp.example.com"
        );
    }

    /// The registry dispatches on the scheme, so the advertised URL has to
    /// route to this implementation and not to the legacy one.
    #[test]
    fn the_auth_url_routes_to_this_implementation() {
        assert_eq!(
            crate::auth::authentication::parse_scheme(TEST_AUTH_URL).expect("it has a scheme"),
            SCHEME
        );
        crate::auth::authentication::find(TEST_AUTH_URL)
            .expect("the scheme is registered as a builtin");
    }

    #[test]
    fn an_auth_url_without_a_client_id_is_refused() {
        let error = parse_auth_url("oidc://idp.example.com/realm")
            .expect_err("a missing client_id must be refused");
        assert!(error.to_string().contains("client_id"), "{error}");
    }

    #[test]
    fn an_auth_url_on_another_scheme_is_refused() {
        let error = parse_auth_url("https://idp.example.com/realm?client_id=lore")
            .expect_err("another scheme must be refused");
        assert!(error.to_string().contains("oidc://"), "{error}");
    }

    #[test]
    fn a_zero_redirect_port_is_refused() {
        let error = parse_auth_url("oidc://idp.example.com/realm?client_id=lore&redirect_port=0")
            .expect_err("port 0 must be refused");
        assert!(error.to_string().contains("redirect_port"), "{error}");
    }

    // -- discovery ----------------------------------------------------------

    #[test]
    fn the_discovery_document_parses_into_the_endpoints_used() {
        let discovery = goals_discovery();
        assert_eq!(discovery.issuer, "https://idp.example.com/-/");
        assert_eq!(
            discovery.token_endpoint,
            "https://idp.example.com/-/tenant-id/oauth/token"
        );
        validate_discovery(&discovery, &goals_config()).expect("the real document validates");
    }

    #[test]
    fn a_discovery_document_pointing_off_host_is_refused() {
        let mut discovery = goals_discovery();
        discovery.token_endpoint = "https://attacker.example.com/oauth/token".to_string();
        let error = validate_discovery(&discovery, &goals_config())
            .expect_err("an off-host token endpoint must be refused");
        assert!(error.to_string().contains("different host"), "{error}");
    }

    #[test]
    fn a_discovery_document_downgrading_to_http_is_refused() {
        let mut discovery = goals_discovery();
        discovery.authorization_endpoint =
            "http://idp.example.com/-/tenant-id/oauth/authorize".to_string();
        let error = validate_discovery(&discovery, &goals_config())
            .expect_err("a plaintext endpoint must be refused");
        assert!(error.to_string().contains("non-HTTPS"), "{error}");
    }

    #[test]
    fn a_provider_advertising_only_plain_pkce_is_refused() {
        let mut discovery = goals_discovery();
        discovery.code_challenge_methods_supported = vec!["plain".to_string()];
        let error = validate_discovery(&discovery, &goals_config())
            .expect_err("a provider without S256 must be refused");
        assert!(error.to_string().contains("S256"), "{error}");
    }

    #[test]
    fn a_provider_advertising_no_pkce_methods_is_accepted() {
        // An omitted list is an omission, not a statement that PKCE is absent.
        let mut discovery = goals_discovery();
        discovery.code_challenge_methods_supported = Vec::new();
        validate_discovery(&discovery, &goals_config())
            .expect("an unadvertised challenge method list is not a refusal");
    }

    // -- PKCE ---------------------------------------------------------------

    #[test]
    fn a_code_verifier_is_within_the_rfc_7636_shape() {
        let verifier = generate_code_verifier().expect("randomness is available");
        assert!(
            (43..=128).contains(&verifier.len()),
            "length {} is outside 43..=128",
            verifier.len()
        );
        assert!(
            verifier
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~')),
            "'{verifier}' contains characters outside the unreserved set"
        );
    }

    #[test]
    fn two_code_verifiers_differ() {
        let first = generate_code_verifier().expect("randomness is available");
        let second = generate_code_verifier().expect("randomness is available");
        assert_ne!(first, second);
    }

    /// The worked example from RFC 7636 Appendix B: this pins the challenge
    /// derivation itself, not just that it produces something.
    #[test]
    fn the_s256_challenge_matches_the_rfc_7636_test_vector() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assert_eq!(
            code_challenge_s256(verifier),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn the_challenge_is_a_function_of_the_verifier() {
        let verifier = generate_code_verifier().expect("randomness is available");
        assert_eq!(
            code_challenge_s256(&verifier),
            code_challenge_s256(&verifier),
            "the same verifier must derive the same challenge"
        );
        let other = generate_code_verifier().expect("randomness is available");
        assert_ne!(code_challenge_s256(&verifier), code_challenge_s256(&other));
    }

    // -- authorization URL --------------------------------------------------

    #[test]
    fn the_authorization_url_carries_every_required_parameter() {
        let url = authorization_url(&goals_discovery(), &goals_config(), "st4te", "ch4llenge")
            .expect("the authorization URL builds");
        let parsed = Url::parse(&url).expect("it is a URL");
        let query: HashMap<_, _> = parsed.query_pairs().into_owned().collect();

        assert_eq!(parsed.host_str(), Some("idp.example.com"));
        assert_eq!(parsed.path(), "/-/tenant-id/oauth/authorize");
        assert_eq!(query.get("response_type").map(String::as_str), Some("code"));
        assert_eq!(query.get("client_id").map(String::as_str), Some("lore-cli"));
        assert_eq!(
            query.get("redirect_uri").map(String::as_str),
            Some("http://127.0.0.1:8765/callback")
        );
        assert_eq!(
            query.get("scope").map(String::as_str),
            Some("openid profile email groups offline_access lore-server:access")
        );
        assert_eq!(query.get("state").map(String::as_str), Some("st4te"));
        assert_eq!(
            query.get("code_challenge").map(String::as_str),
            Some("ch4llenge")
        );
        assert_eq!(
            query.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
    }

    #[test]
    fn the_authorization_url_keeps_the_endpoints_literal_path() {
        let url = authorization_url(&goals_discovery(), &goals_config(), "s", "c")
            .expect("the authorization URL builds");
        assert!(url.contains("/-/tenant-id/oauth/authorize"), "{url}");
    }

    // -- callback parsing ---------------------------------------------------

    #[test]
    fn a_redirect_carrying_a_code_is_the_callback() {
        assert_eq!(
            callback_from_target("/callback?code=abc123&state=xyz", "/callback"),
            Some(Callback::Code {
                code: "abc123".to_string(),
                state: "xyz".to_string(),
            })
        );
    }

    #[test]
    fn a_percent_encoded_code_is_decoded_once() {
        let callback = callback_from_target("/callback?code=a%2Bb%2Fc%3D&state=s", "/callback");
        assert_eq!(
            callback,
            Some(Callback::Code {
                code: "a+b/c=".to_string(),
                state: "s".to_string(),
            })
        );
    }

    #[test]
    fn a_redirect_carrying_an_error_is_a_failure_not_a_wait() {
        assert_eq!(
            callback_from_target(
                "/callback?error=access_denied&error_description=User+said+no&state=xyz",
                "/callback"
            ),
            Some(Callback::Error {
                error: "access_denied".to_string(),
                description: Some("User said no".to_string()),
                state: Some("xyz".to_string()),
            })
        );
    }

    #[test]
    fn a_request_on_another_path_is_not_the_callback() {
        assert_eq!(callback_from_target("/favicon.ico", "/callback"), None);
        assert_eq!(
            callback_from_target("/other?code=abc&state=xyz", "/callback"),
            None
        );
    }

    #[test]
    fn a_request_on_the_callback_path_with_no_parameters_is_ignored() {
        assert_eq!(callback_from_target("/callback", "/callback"), None);
        assert_eq!(
            callback_from_target("/callback?code=abc", "/callback"),
            None
        );
    }

    #[test]
    fn only_get_requests_are_read() {
        assert_eq!(
            request_target("GET /callback?code=a&state=b HTTP/1.1\r\nHost: x\r\n\r\n"),
            Some("/callback?code=a&state=b")
        );
        assert_eq!(
            request_target("POST /callback HTTP/1.1\r\nHost: x\r\n\r\n"),
            None
        );
        assert_eq!(request_target(""), None);
        assert_eq!(request_target("garbage\r\n\r\n"), None);
    }

    // -- claims -------------------------------------------------------------

    /// Builds an unsigned, well-formed JWT. Signature verification is the
    /// server's job, so a test token needs only a decodable shape.
    fn test_jwt(claims: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"at+JWT"}"#);
        let claims = URL_SAFE_NO_PAD.encode(claims);
        format!("{header}.{claims}.not-a-real-signature")
    }

    /// The claim set a real access token from this provider carries.
    fn goals_access_token() -> String {
        test_jwt(
            r#"{
                "iss": "https://idp.example.com/-/",
                "aud": "lore-server",
                "sub": "user-0001",
                "exp": 2000000000,
                "scope": "lore-server:access",
                "email": "dev@example.com",
                "given_name": "Dev",
                "family_name": "Eloper",
                "groups": ["horde-admins", "horde-users"]
            }"#,
        )
    }

    #[test]
    fn claims_are_read_out_of_an_access_token() {
        let claims = decode_claims(&goals_access_token()).expect("the token decodes");
        assert_eq!(claims.sub.as_deref(), Some("user-0001"));
        assert_eq!(claims.email.as_deref(), Some("dev@example.com"));
        assert_eq!(claims.exp, Some(2_000_000_000));
    }

    #[test]
    fn a_display_name_falls_back_through_the_profile_claims() {
        let named = decode_claims(&test_jwt(r#"{"sub":"s","name":"Ada Lovelace"}"#)).unwrap();
        assert_eq!(named.display_name(), "Ada Lovelace");

        let parts = decode_claims(&goals_access_token()).unwrap();
        assert_eq!(parts.display_name(), "Dev Eloper");

        let username =
            decode_claims(&test_jwt(r#"{"sub":"s","preferred_username":"ada"}"#)).unwrap();
        assert_eq!(username.display_name(), "ada");

        let email = decode_claims(&test_jwt(r#"{"sub":"s","email":"ada@example.com"}"#)).unwrap();
        assert_eq!(email.display_name(), "ada@example.com");

        let bare = decode_claims(&test_jwt(r#"{"sub":"s"}"#)).unwrap();
        assert_eq!(bare.display_name(), "s", "the subject is the last resort");
    }

    /// A token with no `exp` must still decode: OpenID Connect requires `exp`
    /// on an ID token but a provider's access token is not bound by that, and
    /// refusing here would fail a login that the server would have accepted.
    #[test]
    fn a_token_without_an_expiry_still_decodes() {
        let claims = decode_claims(&test_jwt(r#"{"sub":"s"}"#)).expect("it decodes");
        assert_eq!(claims.exp, None);
    }

    #[test]
    fn a_token_response_becomes_the_stored_authentication_token() {
        let token = authentication_token(TokenResponse {
            access_token: goals_access_token(),
            refresh_token: Some("refresh-me".to_string()),
            expires_in: Some(3600),
        })
        .expect("the response converts");

        assert_eq!(token.user_id, "user-0001");
        assert_eq!(token.user_name, "Dev Eloper");
        assert_eq!(
            token.expires_ms, 2_000_000_000_000,
            "the token's own exp wins over expires_in, in milliseconds"
        );
        assert_eq!(token.refresh_token.as_deref(), Some("refresh-me"));
    }

    #[test]
    fn a_token_response_without_an_exp_falls_back_to_expires_in() {
        let before = now_ms();
        let token = authentication_token(TokenResponse {
            access_token: test_jwt(r#"{"sub":"s"}"#),
            refresh_token: None,
            expires_in: Some(60),
        })
        .expect("the response converts");
        assert!(
            token.expires_ms >= before + 60_000,
            "expires_in is relative to now"
        );
    }

    #[test]
    fn a_token_response_identifying_nobody_is_refused() {
        let error = authentication_token(TokenResponse {
            access_token: test_jwt(r#"{"exp":2000000000}"#),
            refresh_token: None,
            expires_in: None,
        })
        .expect_err("a subjectless token must be refused");
        assert!(error.to_string().contains("sub"), "{error}");
    }

    // -- token endpoint errors ----------------------------------------------

    #[test]
    fn an_oauth_error_response_maps_to_the_matching_protocol_error() {
        let denied = token_error(
            r#"{"error":"access_denied","error_description":"consent refused"}"#,
            reqwest::StatusCode::BAD_REQUEST,
        );
        assert!(denied.is_not_authorized(), "{denied}");

        let bad_grant = token_error(
            r#"{"error":"invalid_grant"}"#,
            reqwest::StatusCode::BAD_REQUEST,
        );
        assert!(bad_grant.is_not_authenticated(), "{bad_grant}");

        let unreadable = token_error(
            "<html>gateway timeout</html>",
            reqwest::StatusCode::BAD_GATEWAY,
        );
        assert!(unreadable.to_string().contains("502"), "{unreadable}");
    }

    // -- trait behaviour ----------------------------------------------------

    #[tokio::test]
    async fn the_repository_exchange_passes_the_access_token_through() {
        let token = goals_access_token();
        let authz = OidcAuthentication
            .exchange_for_repository(TEST_AUTH_URL, &token, RepositoryId::default(), "corr")
            .await
            .expect("a Tier 1 exchange succeeds");
        assert_eq!(authz.token, token, "the same bearer comes back");
        assert_eq!(authz.expires_ms, 2_000_000_000_000);
    }

    #[tokio::test]
    async fn the_custom_resource_exchange_passes_the_access_token_through() {
        let token = goals_access_token();
        let authz = OidcAuthentication
            .exchange_for_custom_resource(TEST_AUTH_URL, &token, "anything-at-all", "corr")
            .await
            .expect("a Tier 1 exchange succeeds");
        assert_eq!(authz.token, token);
    }

    #[tokio::test]
    async fn an_external_token_exchange_reports_not_supported() {
        let error = OidcAuthentication
            .exchange_external_token(TEST_AUTH_URL, "some-token", "epic", "corr")
            .await
            .expect_err("there is no broker to exchange against");
        assert!(error.is_not_supported(), "{error}");
    }

    #[tokio::test]
    async fn user_info_answers_for_the_bearer_and_nobody_else() {
        let token = goals_access_token();
        let resolved = OidcAuthentication
            .get_user_info(
                TEST_AUTH_URL,
                &token,
                RepositoryId::default(),
                &["user-0001".to_string(), "someone-else".to_string()],
                "corr",
            )
            .await
            .expect("resolution succeeds");
        assert_eq!(resolved.len(), 1, "only the bearer is resolvable");
        assert_eq!(resolved[0].user_id, "user-0001");
        assert_eq!(resolved[0].user_name, "Dev Eloper");
    }

    #[tokio::test]
    async fn a_user_id_resolves_from_the_bearers_own_names() {
        let token = goals_access_token();
        let auth = OidcAuthentication;
        for name in ["Dev Eloper", "dev@example.com", "user-0001"] {
            let resolved = auth
                .get_user_id(
                    TEST_AUTH_URL,
                    &token,
                    RepositoryId::default(),
                    name,
                    "corr",
                )
                .await
                .expect("resolution succeeds");
            assert_eq!(
                resolved.map(|user| user.user_id),
                Some("user-0001".to_string()),
                "'{name}' names the bearer"
            );
        }

        let stranger = auth
            .get_user_id(
                TEST_AUTH_URL,
                &token,
                RepositoryId::default(),
                "Somebody Else",
                "corr",
            )
            .await
            .expect("resolution succeeds");
        assert!(stranger.is_none(), "nobody else can be named");
    }

    /// Registers a session whose callback has already landed, so a poll runs
    /// straight into the checks that guard the code exchange.
    fn plant_session(session_code: &str, client_state: &str, callback: Callback) {
        let idle = lore_spawn_net!(async {});
        sessions().lock().insert(
            session_code.to_string(),
            PendingSession {
                client_state: client_state.to_string(),
                oauth_state: "the-real-state".to_string(),
                code_verifier: "verifier".to_string(),
                config: goals_config(),
                discovery: Arc::new(goals_discovery()),
                slot: Arc::new(Mutex::new(Some(callback))),
                listener: idle.abort_handle(),
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );
    }

    /// RFC 6749 §10.12. A code arriving under somebody else's `state` is not
    /// redeemed, and the attempt does not silently read as "not yet".
    #[tokio::test]
    async fn a_callback_with_the_wrong_state_is_refused_and_the_code_is_not_redeemed() {
        plant_session(
            "state-mismatch-session",
            "client-state",
            Callback::Code {
                code: "attacker-code".to_string(),
                state: "not-the-real-state".to_string(),
            },
        );

        let error = OidcAuthentication
            .poll_auth_session(
                TEST_AUTH_URL,
                "client-state",
                "state-mismatch-session",
                "corr",
            )
            .await
            .expect_err("a mismatched state must be refused");

        assert!(error.to_string().contains("state"), "{error}");
        assert!(
            !sessions().lock().contains_key("state-mismatch-session"),
            "the session is released rather than left holding the port"
        );
    }

    /// A poll that names the right session with the wrong client state is a
    /// caller mix-up, not a wait.
    #[tokio::test]
    async fn a_poll_with_a_mismatched_client_state_is_refused() {
        plant_session(
            "client-state-mismatch-session",
            "the-right-client-state",
            Callback::Code {
                code: "code".to_string(),
                state: "the-real-state".to_string(),
            },
        );

        let error = OidcAuthentication
            .poll_auth_session(
                TEST_AUTH_URL,
                "a-different-client-state",
                "client-state-mismatch-session",
                "corr",
            )
            .await
            .expect_err("a mismatched client state must be refused");
        assert!(error.to_string().contains("client state"), "{error}");

        sessions().lock().remove("client-state-mismatch-session");
    }

    /// An `error=` in the redirect is the user declining consent (or the
    /// provider refusing), and must end the login rather than be read as "the
    /// browser has not come back yet".
    #[tokio::test]
    async fn a_declined_consent_ends_the_poll_loop_rather_than_timing_out() {
        plant_session(
            "declined-session",
            "client-state",
            Callback::Error {
                error: "access_denied".to_string(),
                description: Some("the user said no".to_string()),
                state: Some("the-real-state".to_string()),
            },
        );

        let error = OidcAuthentication
            .poll_auth_session(TEST_AUTH_URL, "client-state", "declined-session", "corr")
            .await
            .expect_err("a declined login must be reported");

        assert!(error.to_string().contains("access_denied"), "{error}");
        assert!(error.to_string().contains("the user said no"), "{error}");
        assert!(!sessions().lock().contains_key("declined-session"));
    }

    /// The listener's own failure (it timed out, or the socket broke) is
    /// likewise terminal.
    #[tokio::test]
    async fn a_listener_failure_ends_the_poll_loop() {
        plant_session(
            "listener-failed-session",
            "client-state",
            Callback::Failed("the listener gave up".to_string()),
        );

        let error = OidcAuthentication
            .poll_auth_session(
                TEST_AUTH_URL,
                "client-state",
                "listener-failed-session",
                "corr",
            )
            .await
            .expect_err("a listener failure must be reported");
        assert!(error.to_string().contains("gave up"), "{error}");
    }

    /// The other half of the contract: an empty mailbox is `Ok(None)`, which is
    /// what keeps the caller's poll loop going.
    #[tokio::test]
    async fn a_session_whose_callback_has_not_landed_reports_not_yet() {
        let idle = lore_spawn_net!(async {});
        sessions().lock().insert(
            "pending-session".to_string(),
            PendingSession {
                client_state: "client-state".to_string(),
                oauth_state: "the-real-state".to_string(),
                code_verifier: "verifier".to_string(),
                config: goals_config(),
                discovery: Arc::new(goals_discovery()),
                slot: Arc::new(Mutex::new(None)),
                listener: idle.abort_handle(),
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );

        let pending = OidcAuthentication
            .poll_auth_session(TEST_AUTH_URL, "client-state", "pending-session", "corr")
            .await
            .expect("an empty mailbox is not an error");
        assert!(pending.is_none(), "not yet");
        assert!(
            sessions().lock().contains_key("pending-session"),
            "an unfinished session stays in flight"
        );

        sessions().lock().remove("pending-session");
    }

    /// A session the listener has given up on is garbage; leaving it in the map
    /// would hold the loopback port for the life of the process.
    #[tokio::test]
    async fn an_expired_session_is_purged_so_its_port_can_be_bound_again() {
        let idle = lore_spawn_net!(async {});
        sessions().lock().insert(
            "expired-session".to_string(),
            PendingSession {
                client_state: "client-state".to_string(),
                oauth_state: "the-real-state".to_string(),
                code_verifier: "verifier".to_string(),
                config: goals_config(),
                discovery: Arc::new(goals_discovery()),
                slot: Arc::new(Mutex::new(None)),
                listener: idle.abort_handle(),
                expires_at: Instant::now() - Duration::from_secs(1),
            },
        );

        purge_expired_sessions();
        assert!(!sessions().lock().contains_key("expired-session"));
    }

    #[tokio::test]
    async fn polling_an_unknown_session_is_an_error_not_a_wait() {
        let error = OidcAuthentication
            .poll_auth_session(TEST_AUTH_URL, "client-state", "no-such-session", "corr")
            .await
            .expect_err("an unknown session code must be reported");
        assert!(error.to_string().contains("session"), "{error}");
    }

    // -- the loopback listener, end to end over 127.0.0.1 -------------------

    /// Drives the real listener over a real loopback socket: bind, connect,
    /// send a redirect, and read what the browser would see.
    async fn drive_listener(request: &str, expected_path: &str) -> (Option<Callback>, String) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral loopback port binds");
        let address = listener.local_addr().expect("it has an address");
        let slot: CallbackSlot = Arc::new(Mutex::new(None));

        let served = {
            let slot = slot.clone();
            let expected_path = expected_path.to_string();
            // Not `lore_spawn!`: the listener above belongs to this test's
            // reactor, and the shared lore runtime is a different one. The
            // lint's reason -- propagating LORE_CONTEXT -- has nothing to
            // carry here.
            #[allow(clippy::disallowed_methods)]
            tokio::spawn(async move { serve_callback(listener, slot, expected_path).await })
        };

        let mut stream = TcpStream::connect(address).await.expect("it connects");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("the request is sent");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("the response is read");

        // Only wait for the task where the request was a terminal one; a
        // non-callback request leaves the listener waiting for the next.
        if slot.lock().is_some() {
            served.await.expect("the listener finished");
        } else {
            served.abort();
        }
        let outcome = slot.lock().clone();
        (outcome, response)
    }

    #[tokio::test]
    async fn the_listener_captures_a_real_redirect_and_answers_the_browser() {
        let (outcome, response) = drive_listener(
            "GET /callback?code=the-code&state=the-state HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            "/callback",
        )
        .await;

        assert_eq!(
            outcome,
            Some(Callback::Code {
                code: "the-code".to_string(),
                state: "the-state".to_string(),
            })
        );
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("Signed in"), "{response}");
    }

    #[tokio::test]
    async fn the_listener_captures_a_denied_consent() {
        let (outcome, response) = drive_listener(
            "GET /callback?error=access_denied&error_description=nope&state=s HTTP/1.1\r\n\r\n",
            "/callback",
        )
        .await;

        assert_eq!(
            outcome,
            Some(Callback::Error {
                error: "access_denied".to_string(),
                description: Some("nope".to_string()),
                state: Some("s".to_string()),
            })
        );
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }

    #[tokio::test]
    async fn the_listener_shrugs_off_a_favicon_request_and_keeps_waiting() {
        let (outcome, response) = drive_listener(
            "GET /favicon.ico HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            "/callback",
        )
        .await;

        assert_eq!(outcome, None, "a favicon fetch must not end the login");
        assert!(response.starts_with("HTTP/1.1 404"), "{response}");
    }
}
