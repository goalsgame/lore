// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Actual lazy auth setup under the explicit revision owner boundary. The
//! channel is never requested; supplied credentials avoid credential-store IO.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use lore_base::env::AUTH_PATH_VAR;
use lore_base::env::CallEnvironment;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::Partition;
use lore_base::types::RepositoryId;
use lore_base::types::VerifyResult;
use lore_io::lease::IoLease;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::protocol::RemoteConnections;
use lore_revision::relay::EventDispatcher;
use lore_transport::auth::AuthEndpoint;
use lore_transport::grpc::GRPCAuth;
use lore_transport::grpc::GRPCConnection;
use lore_transport::grpc::RequestLoggerLayer;
use lore_transport::*;
use tower::Layer;

const AUTHN_ONE: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UifQ.signature";
const AUTHN_TWO: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJzY29wZSI6InJlYWQifQ.signature";
const AUTHZ_ONE: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJhdXRoeiI6Im9uZSJ9.signature";
const AUTHZ_TWO: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJhdXRoeiI6InR3byJ9.signature";

struct Observation {
    token: String,
    detached: bool,
    credential_environment: bool,
    parent_io: bool,
}
struct AuthenticationProbe {
    observations: parking_lot::Mutex<Vec<Observation>>,
}
impl AuthenticationProbe {
    fn exchange(&self, token: &str) -> Result<AuthorizationToken, ProtocolError> {
        let context = lore_revision::runtime::try_execution_context().unwrap();
        self.observations.lock().push(Observation {
            token: token.to_owned(),
            detached: context.caller_state().is_none() && context.dispatcher.weak_sender.is_none(),
            credential_environment: context.globals().identity.as_str() == "alice"
                && context.globals().identity_token() == AUTHN_ONE
                && lore_base::env::var(AUTH_PATH_VAR).as_deref() == Some("/owner-context-fixture"),
            parent_io: IoLease::current().is_some(),
        });
        Ok(AuthorizationToken {
            token: if token == AUTHN_ONE {
                AUTHZ_ONE
            } else {
                AUTHZ_TWO
            }
            .to_owned(),
            expires_ms: 0,
            acceptable_root_domains: vec!["example.com".to_owned()],
        })
    }
}

fn unsupported() -> ProtocolError {
    ProtocolError::internal("fixture only exchanges supplied credentials")
}
#[async_trait]
impl Authentication for AuthenticationProbe {
    async fn start_auth_session(
        &self,
        _url: &str,
        _state: &str,
        _correlation: &str,
    ) -> Result<AuthSession, ProtocolError> {
        Err(unsupported())
    }
    async fn poll_auth_session(
        &self,
        _url: &str,
        _state: &str,
        _code: &str,
        _correlation: &str,
    ) -> Result<AuthSessionPoll, ProtocolError> {
        Err(unsupported())
    }
    async fn exchange_external_token(
        &self,
        _url: &str,
        _token: &str,
        _kind: &str,
        _correlation: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }
    async fn authenticate_client(
        &self,
        _url: &str,
        _id: &str,
        _secret: &str,
        _correlation: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }
    async fn refresh_authentication(
        &self,
        _url: &str,
        _token: &str,
        _correlation: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }
    async fn exchange_for_repository(
        &self,
        _url: &str,
        token: &str,
        _repository: RepositoryId,
        _correlation: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        self.exchange(token)
    }
    async fn exchange_for_custom_resource(
        &self,
        _url: &str,
        token: &str,
        _resource: &str,
        _correlation: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        self.exchange(token)
    }
}

type Auth = Arc<parking_lot::RwLock<GRPCAuth>>;
static NEXT: AtomicU64 = AtomicU64::new(1);
static FIXTURES: OnceLock<parking_lot::Mutex<HashMap<RepositoryId, Weak<Fixture>>>> =
    OnceLock::new();
fn fixtures() -> &'static parking_lot::Mutex<HashMap<RepositoryId, Weak<Fixture>>> {
    FIXTURES.get_or_init(Default::default)
}
struct Fixture {
    repository: RepositoryId,
    grpc: GRPCConnection,
    endpoint: AuthEndpoint,
    probe: Arc<AuthenticationProbe>,
    credentials: OnceLock<Arc<SuppliedCredentials>>,
    remote_url: String,
    auth: parking_lot::Mutex<Option<Auth>>,
    lazy_auth: parking_lot::Mutex<Option<Auth>>,
    sessions: parking_lot::Mutex<Vec<(String, bool)>>,
    entered: tokio::sync::Notify,
    resume: tokio::sync::watch::Sender<bool>,
    stopped: AtomicBool,
    fail: bool,
    synchronous_detached: AtomicBool,
}
impl Fixture {
    fn new(fail: bool) -> Arc<Self> {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let mut repository = [0; 16];
        repository[..8].copy_from_slice(&id.to_le_bytes());
        repository[15] = 99;
        let repository = repository.into();
        let probe = Arc::new(AuthenticationProbe {
            observations: Default::default(),
        });
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
        let grpc = GRPCConnection::for_test(
            "https://example.com".parse().unwrap(),
            RequestLoggerLayer {}.layer(channel),
        );
        let fixture = Arc::new(Self {
            repository,
            grpc,
            endpoint: AuthEndpoint::new(
                format!("https://auth.example.com/owner-context/{id}"),
                probe.clone(),
            ),
            probe,
            credentials: OnceLock::new(),
            remote_url: format!("ownercontextfixture{id}://example.com"),
            auth: Default::default(),
            lazy_auth: Default::default(),
            sessions: Default::default(),
            entered: Default::default(),
            resume: tokio::sync::watch::channel(false).0,
            stopped: AtomicBool::new(false),
            fail,
            synchronous_detached: AtomicBool::new(false),
        });
        fixtures()
            .lock()
            .insert(repository, Arc::downgrade(&fixture));
        lore_transport::add(
            &format!("ownercontextfixture{id}"),
            Arc::new(SeedProtocol(Arc::downgrade(&fixture))),
        )
        .unwrap();
        fixture
    }
    fn credentials(&self) -> &Arc<SuppliedCredentials> {
        self.credentials.get().unwrap()
    }
    async fn source(&self) -> OwnerConnectionSource {
        // A real ordinary seed connection establishes the immutable principal.
        // Only the seed is in the global cache; the source retains no connection.
        let seed = Arc::new(
            ExecutionContext::new_client(
                LoreGlobalArgs {
                    identity: "alice".into(),
                    identity_token: AUTHN_ONE.into(),
                    ..Default::default()
                },
                EventDispatcher::no_dispatch(),
            )
            .with_environment(CallEnvironment {
                values: [None, Some("/owner-context-fixture".into())],
            }),
        );
        let connection = LORE_CONTEXT
            .scope(
                seed.clone(),
                lore_transport::connect(
                    &self.remote_url,
                    "alice",
                    self.repository,
                    1,
                    AUTHN_ONE,
                    "",
                    lore_transport::auth::AuthMode::Auto,
                ),
            )
            .await
            .unwrap();
        self.credentials
            .set(connection.credentials().clone())
            .unwrap();
        let result = LORE_CONTEXT.scope(seed, connection.owner_source()).await;
        lore_transport::remove_connection(connection);
        result.unwrap()
    }
    async fn join_auth(&self) {
        let auth = self.auth.lock().take();
        let lazy = self.lazy_auth.lock().take();
        for auth in [auth, lazy].into_iter().flatten() {
            let task = auth.write().refresher.take();
            if let Some(task) = task {
                task.abort();
                let stopped = tokio::time::timeout(Duration::from_secs(10), task)
                    .await
                    .unwrap();
                assert!(stopped.is_err_and(|error| error.is_cancelled()));
            }
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fixtures().lock().remove(&self.repository);
    }
}
struct Cleanup(Arc<Fixture>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let mut auth = self.0.auth.lock();
        self.0.stopped.store(true, Ordering::Release);
        self.0.resume.send_replace(true);
        let first = auth.take();
        let lazy = self.0.lazy_auth.lock().take();
        for auth in [first, lazy].into_iter().flatten() {
            if let Some(task) = auth.write().refresher.take() {
                task.abort();
            }
        }
    }
}
fn connecting(
    source: OwnerConnectionSource,
    repository: RepositoryId,
    _maximum: usize,
) -> BoxFuture<'static, Result<Arc<Connection>, ProtocolError>> {
    let fixture = fixtures()
        .lock()
        .get(&repository)
        .unwrap()
        .upgrade()
        .unwrap();
    let context = lore_revision::runtime::try_execution_context().unwrap();
    fixture.synchronous_detached.store(
        context.caller_state().is_none() && context.dispatcher.weak_sender.is_none(),
        Ordering::Release,
    );
    async move {
        source.check_environment()?;
        assert_eq!(source.identity(), "alice");
        let mut resume = fixture.resume.subscribe();
        let auth = fixture
            .grpc
            .repository_authz(
                &fixture.endpoint,
                "alice",
                repository,
                fixture.credentials(),
            )
            .await;
        {
            let mut retained = fixture.auth.lock();
            if fixture.stopped.load(Ordering::Acquire) {
                if let Some(task) = auth.write().refresher.take() {
                    task.abort();
                }
                return Err(ProtocolError::internal("fixture stopped"));
            }
            *retained = Some(auth);
        }
        fixture.entered.notify_one();
        while !*resume.borrow() {
            if resume.changed().await.is_err() {
                return Err(ProtocolError::internal("fixture stopped"));
            }
        }
        if fixture.fail {
            Err(ProtocolError::internal("explicit owner connection failure"))
        } else {
            Ok(Connection::detached(vec![Arc::new(SessionProbe(
                Arc::downgrade(&fixture),
            ))]))
        }
    }
    .boxed()
}
fn caller() -> (Arc<ExecutionContext>, Weak<()>, Weak<()>) {
    let opaque = Arc::new(());
    let opaque_weak = Arc::downgrade(&opaque);
    let callback = Arc::new(());
    let callback_weak = Arc::downgrade(&callback);
    let dispatcher = EventDispatcher::new(Some(Box::new(move |_| {
        let _retained = Arc::strong_count(&callback);
    })));
    let mut execution = ExecutionContext::new_client(
        LoreGlobalArgs {
            identity: "alice".into(),
            identity_token: AUTHN_ONE.into(),
            ..Default::default()
        },
        dispatcher,
    )
    .with_environment(CallEnvironment {
        values: [None, Some("/owner-context-fixture".into())],
    });
    execution.set_caller_state(opaque);
    (Arc::new(execution), opaque_weak, callback_weak)
}
async fn eventually(check: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn owner_auth_releases_cancelled_caller_without_drain_and_preserves_rotation_and_finite_io() {
    let fixture = Fixture::new(false);
    let _cleanup = Cleanup(fixture.clone());
    let (caller, opaque, callback) = caller();
    let caller_weak = Arc::downgrade(&caller);
    let connections = RemoteConnections::with_connect_for_background_owner(
        connecting,
        fixture.source().await,
        &caller,
    )
    .await
    .unwrap();
    let parent = Arc::new(());
    let parent_weak = Arc::downgrade(&parent);
    let lease = IoLease::new(parent);
    let mut request = Box::pin(LORE_CONTEXT.scope(
        caller.clone(),
        lease.scope(connections.connection(fixture.repository)),
    ));
    tokio::select! {
        result = &mut request => panic!("setup ended before pause: {}", result.is_ok()),
        _ = fixture.entered.notified() => {},
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("auth setup did not start"),
    }
    drop(request);
    drop(caller);
    drop(lease);
    // No dispatcher complete/drain: retaining the caller would also retain its callback channel.
    eventually(|| {
        caller_weak.upgrade().is_none()
            && opaque.upgrade().is_none()
            && callback.upgrade().is_none()
    })
    .await;
    assert!(fixture.synchronous_detached.load(Ordering::Acquire));
    assert!(
        parent_weak.upgrade().is_some(),
        "finite shared connect still owns IO"
    );
    fixture.resume.send_replace(true);
    eventually(|| parent_weak.upgrade().is_none()).await;
    assert!(connections.connection(fixture.repository).await.is_ok());
    fixture.credentials().update(AUTHN_TWO, "");
    eventually(|| {
        fixture
            .auth
            .lock()
            .as_ref()
            .unwrap()
            .read()
            .authorization_token
            == AUTHZ_TWO
    })
    .await;
    {
        let seen = fixture.probe.observations.lock();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].token, AUTHN_ONE);
        assert_eq!(seen[1].token, AUTHN_TWO);
        assert!(
            seen.iter()
                .all(|seen| seen.detached && seen.credential_environment)
        );
        assert!(seen[0].parent_io);
        assert!(!seen[1].parent_io);
    }
    fixture.join_auth().await;
}
#[tokio::test]
async fn owner_connect_errors_return_under_the_original_caller_context() {
    let fixture = Fixture::new(true);
    let _cleanup = Cleanup(fixture.clone());
    let (caller, _, _) = caller();
    let connections = RemoteConnections::with_connect_for_background_owner(
        connecting,
        fixture.source().await,
        &caller,
    )
    .await
    .unwrap();
    fixture.resume.send_replace(true);
    LORE_CONTEXT
        .scope(caller.clone(), async {
            let error = connections
                .connection(fixture.repository)
                .await
                .err()
                .unwrap();
            assert!(format!("{error:?}").contains("explicit owner connection failure"));
            assert!(Arc::ptr_eq(
                &lore_revision::runtime::try_execution_context().unwrap(),
                &caller
            ));
            caller.dispatcher.drain().await;
        })
        .await;
    fixture.join_auth().await;
}

struct SessionProbe(Weak<Fixture>);
#[async_trait]
impl Storage for SessionProbe {
    async fn session_start(
        &self,
        _partition: Partition,
        correlation: &str,
    ) -> Result<u32, ProtocolError> {
        let fixture = self.0.upgrade().unwrap();
        let context = lore_revision::runtime::try_execution_context().unwrap();
        fixture.sessions.lock().push((
            correlation.to_owned(),
            context.caller_state().is_none() && context.dispatcher.weak_sender.is_none(),
        ));
        let auth = fixture
            .grpc
            .custom_resource_authz(
                &fixture.endpoint,
                "alice",
                "lazy-storage",
                fixture.credentials(),
            )
            .await;
        let mut slot = fixture.lazy_auth.lock();
        if fixture.stopped.load(Ordering::Acquire) {
            if let Some(task) = auth.write().refresher.take() {
                task.abort();
            }
        } else {
            *slot = Some(auth);
        }
        Ok(1)
    }
    async fn session_stop(&self, _session: u32) -> Result<(), ProtocolError> {
        Ok(())
    }
    async fn get(
        &self,
        _session: u32,
        _address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        Err(unsupported())
    }
    async fn put(
        &self,
        _session: u32,
        _address: Address,
        _fragment: Fragment,
        _payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        Err(unsupported())
    }
    async fn query(&self, _session: u32, _addresses: &[Address]) -> Result<Bytes, ProtocolError> {
        Err(unsupported())
    }
    async fn verify(
        &self,
        _session: u32,
        _address: &Address,
        _heal: bool,
    ) -> Result<VerifyResult, ProtocolError> {
        Err(unsupported())
    }
    async fn copy(
        &self,
        _session: u32,
        _partition: Partition,
        _address: Address,
        _context: Context,
    ) -> Result<(), ProtocolError> {
        Err(unsupported())
    }
}
#[tokio::test]
async fn lazy_store_session_uses_owner_auth_and_current_caller_correlation() {
    let fixture = Fixture::new(false);
    let _cleanup = Cleanup(fixture.clone());
    let (owner, _, _) = caller();
    let connections = RemoteConnections::with_connect_for_background_owner(
        connecting,
        fixture.source().await,
        &owner,
    )
    .await
    .unwrap();
    fixture.resume.send_replace(true);
    let store = lore_revision::store::remote::RemoteImmutableStore::from_connections(connections);
    let (caller, opaque, callback) = caller();
    assert_ne!(
        owner.globals().correlation_id,
        caller.globals().correlation_id
    );
    let expected = caller.globals().correlation_id.to_string();
    let parent = Arc::new(());
    let weak_parent = Arc::downgrade(&parent);
    let lease = IoLease::new(parent);
    let session = LORE_CONTEXT
        .scope(
            caller.clone(),
            lease.scope(store.session(fixture.repository)),
        )
        .await
        .unwrap();
    drop(caller);
    drop(lease);
    eventually(|| {
        opaque.upgrade().is_none()
            && callback.upgrade().is_none()
            && weak_parent.upgrade().is_none()
    })
    .await;
    {
        let sessions = fixture.sessions.lock();
        assert!(!sessions.is_empty());
        assert!(
            sessions
                .iter()
                .all(|(correlation, detached)| correlation == &expected && *detached)
        );
    }
    assert!(
        fixture
            .probe
            .observations
            .lock()
            .iter()
            .all(|seen| seen.detached && seen.credential_environment)
    );
    drop(session);
    fixture.join_auth().await;
}
#[tokio::test]
async fn owner_setup_covers_lazy_custom_authorization_and_restores_caller_context() {
    let fixture = Fixture::new(false);
    let _cleanup = Cleanup(fixture.clone());
    let (owner, _, _) = caller();
    let connections = RemoteConnections::with_connect_for_background_owner(
        connecting,
        fixture.source().await,
        &owner,
    )
    .await
    .unwrap();
    let (caller, _, _) = caller();
    LORE_CONTEXT
        .scope(caller.clone(), async {
            let auth = connections
                .owner_setup(async {
                    Ok::<_, ProtocolError>(
                        Box::pin(fixture.grpc.custom_resource_authz(
                            &fixture.endpoint,
                            "alice",
                            "lazy-admin",
                            fixture.credentials(),
                        ))
                        .await,
                    )
                })
                .await
                .unwrap();
            assert_eq!(auth.read().authorization_token, AUTHZ_ONE);
            *fixture.lazy_auth.lock() = Some(auth);
            assert!(Arc::ptr_eq(
                &lore_revision::runtime::try_execution_context().unwrap(),
                &caller
            ));
            assert!(
                fixture
                    .probe
                    .observations
                    .lock()
                    .iter()
                    .all(|seen| seen.detached && seen.credential_environment)
            );
            caller.dispatcher.drain().await;
        })
        .await;
    fixture.join_auth().await;
}

struct SeedProtocol(Weak<Fixture>);
struct SeedEnvironment;
#[async_trait]
impl Environment for SeedEnvironment {
    async fn get(&self) -> Result<EnvironmentConfig, ProtocolError> {
        Ok(EnvironmentConfig::default())
    }
}
#[async_trait]
impl Protocol for SeedProtocol {
    async fn environment(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
    ) -> Result<Arc<dyn Environment>, ProtocolError> {
        Ok(Arc::new(SeedEnvironment))
    }
    async fn storage(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
        _auth: &AuthEndpoint,
        _identity: &str,
        _partition: Partition,
        _index: usize,
        _credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Storage>, ProtocolError> {
        Ok(Arc::new(SessionProbe(self.0.clone())))
    }
    async fn revision(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
        _auth: &AuthEndpoint,
        _identity: &str,
        _repository: RepositoryId,
        _credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Revision>, ProtocolError> {
        Err(unsupported())
    }
    async fn repository(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
        _auth: &AuthEndpoint,
        _identity: &str,
        _credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Repository>, ProtocolError> {
        Err(unsupported())
    }
    async fn admin(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
        _auth: &AuthEndpoint,
        _identity: &str,
        _repository: RepositoryId,
        _credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Admin>, ProtocolError> {
        Err(unsupported())
    }
    async fn lock(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
        _auth: &AuthEndpoint,
        _identity: &str,
        _repository: RepositoryId,
        _credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Lock>, ProtocolError> {
        Err(unsupported())
    }
}
