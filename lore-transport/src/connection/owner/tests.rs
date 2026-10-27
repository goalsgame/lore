// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use bytes::Bytes;
use lore_base::env::CallEnvironment;
use lore_base::runtime::LORE_CONTEXT;
use lore_io::lease::IoLease;
use tokio::sync::Notify;

use super::*;

struct TestContext(CallEnvironment);
fn context(store: &str) -> Arc<TestContext> {
    lore_base::env::find_context_environment_with(|context| {
        context
            .downcast_ref::<TestContext>()
            .map(|context| &context.0)
    });
    Arc::new(TestContext(CallEnvironment {
        values: [None, Some(store.to_string())],
    }))
}
#[derive(Default)]
struct Pause {
    reached: Notify,
    resume: Notify,
}
struct Observation {
    connection: Weak<Connection>,
    credentials: Weak<SuppliedCredentials>,
    identity: String,
    repository: RepositoryId,
    tokens: (String, String),
}
#[derive(Default)]
struct State {
    environment_calls: AtomicUsize,
    environment: Mutex<EnvironmentConfig>,
    environment_pause: Mutex<Option<Arc<Pause>>>,
    storage_pause: Mutex<Option<Arc<Pause>>>,
    fail_storage: AtomicBool,
    observations: Mutex<Vec<Observation>>,
}
struct TestProtocol(Arc<State>);
struct TestEnvironment(Arc<State>);
struct NoRequests;
fn unsupported() -> ProtocolError {
    ProtocolError::internal("fixture serves connection setup only")
}
#[async_trait::async_trait]
impl Environment for TestEnvironment {
    async fn get(&self) -> Result<EnvironmentConfig, ProtocolError> {
        self.0.environment_calls.fetch_add(1, Ordering::Relaxed);
        let pause = self.0.environment_pause.lock().take();
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
        Ok(self.0.environment.lock().clone())
    }
}
#[async_trait::async_trait]
impl Protocol for TestProtocol {
    async fn environment(
        &self,
        _connection: Weak<Connection>,
        _url: &str,
    ) -> Result<Arc<dyn Environment>, ProtocolError> {
        Ok(Arc::new(TestEnvironment(self.0.clone())))
    }
    async fn storage(
        &self,
        connection: Weak<Connection>,
        _url: &str,
        _auth: &AuthEndpoint,
        identity: &str,
        repository: RepositoryId,
        _index: usize,
        credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Storage>, ProtocolError> {
        self.0.observations.lock().push(Observation {
            connection,
            credentials: Arc::downgrade(credentials),
            identity: identity.to_string(),
            repository,
            tokens: credentials.tokens(),
        });
        let pause = self.0.storage_pause.lock().take();
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
        if self.0.fail_storage.load(Ordering::Acquire) {
            Err(unsupported())
        } else {
            Ok(Arc::new(NoRequests))
        }
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
#[async_trait::async_trait]
impl Storage for NoRequests {
    async fn session_start(
        &self,
        _partition: Partition,
        _correlation_id: &str,
    ) -> Result<u32, ProtocolError> {
        Err(unsupported())
    }
    async fn session_stop(&self, _session_id: u32) -> Result<(), ProtocolError> {
        Err(unsupported())
    }
    async fn get(
        &self,
        _session_id: u32,
        _address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        Err(unsupported())
    }
    async fn put(
        &self,
        _session_id: u32,
        _address: Address,
        _fragment: Fragment,
        _payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        Err(unsupported())
    }
    async fn query(&self, _session_id: u32, _address: &[Address]) -> Result<Bytes, ProtocolError> {
        Err(unsupported())
    }
    async fn verify(
        &self,
        _session_id: u32,
        _address: &Address,
        _heal: bool,
    ) -> Result<VerifyResult, ProtocolError> {
        Err(unsupported())
    }
    async fn copy(
        &self,
        _session_id: u32,
        _source_partition: Partition,
        _source_address: Address,
        _target_context: Context,
    ) -> Result<(), ProtocolError> {
        Err(unsupported())
    }
}

struct Fixture {
    url: String,
    context: Arc<TestContext>,
    state: Arc<State>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let index = NEXT.fetch_add(1, Ordering::Relaxed);
        let scheme = format!("ownersource{index}");
        let state = Arc::new(State::default());
        add(&scheme, Arc::new(TestProtocol(state.clone()))).unwrap();
        Self {
            url: Url::parse(&format!("{scheme}://fixture.invalid"))
                .unwrap()
                .to_string(),
            context: context(&format!("/owner-source-{index}")),
            state,
        }
    }
    async fn seed(&self, identity: &str, token: &str) -> Arc<Connection> {
        LORE_CONTEXT
            .scope(
                self.context.clone(),
                connect(
                    &self.url,
                    identity,
                    [11; 16].into(),
                    1,
                    token,
                    "",
                    AuthMode::Auto,
                ),
            )
            .await
            .unwrap()
    }
    async fn source(&self, connection: &Connection) -> OwnerConnectionSource {
        LORE_CONTEXT
            .scope(self.context.clone(), connection.owner_source())
            .await
            .unwrap()
    }
    async fn open(
        &self,
        source: &OwnerConnectionSource,
        repository: RepositoryId,
    ) -> Arc<Connection> {
        let connection = LORE_CONTEXT
            .scope(self.context.clone(), connect_owned(source, repository, 1))
            .await
            .unwrap();
        connection.ensure_storage_connected().await.unwrap();
        connection
    }
    fn assert_cached_only(&self, expected: &Arc<Connection>) {
        let connections: Vec<_> = CONNECTION_MAP
            .lock()
            .as_ref()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.0 == self.url)
            .map(|(_, connection)| connection.clone())
            .collect();
        assert_eq!(
            connections.len(),
            1,
            "owner connections add no cache entries in any mode"
        );
        assert!(Arc::ptr_eq(&connections[0], expected));
    }
    fn cached(&self) -> Option<Arc<Connection>> {
        LORE_CONTEXT.sync_scope(self.context.clone(), || {
            find_connection(&self.url, "alice", true, AuthMode::Auto)
        })
    }
}
async fn eventually(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn reached(pause: &Pause) {
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn owned_reconnect_and_module_share_live_credentials_without_touching_ordinary_cache() {
    let fixture = Fixture::new();
    let seed = fixture.seed("alice", "one").await;
    let source = fixture.source(&seed).await;
    let first = fixture.open(&source, [12; 16].into()).await;
    assert!(!Arc::ptr_eq(&seed, &first));
    assert!(Arc::ptr_eq(seed.credentials(), first.credentials()));
    fixture.assert_cached_only(&seed);
    assert!(Arc::ptr_eq(&fixture.cached().unwrap(), &seed));
    seed.credentials().update("two", "");
    let second = fixture.open(&source, [13; 16].into()).await;
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(seed.credentials(), second.credentials()));
    let module = LORE_CONTEXT
        .scope(
            fixture.context.clone(),
            second.connect_module([14; 16].into()),
        )
        .await
        .unwrap();
    module.ensure_storage_connected().await.unwrap();
    assert!(module.owned);
    assert!(Arc::ptr_eq(seed.credentials(), module.credentials()));
    assert_eq!(module.credentials().tokens().0, "two");
    assert!(module.ensure_repository_connected().await.is_err());
    assert!(
        Arc::ptr_eq(&fixture.cached().unwrap(), &seed),
        "owned invalidation removes no ordinary cache entry"
    );
    drop(first);
    drop(second);
    drop(module);
    fixture.assert_cached_only(&seed);
    assert!(Arc::ptr_eq(&fixture.cached().unwrap(), &seed));
    let observations = fixture.state.observations.lock();
    assert!(observations.iter().all(|seen| {
        seen.identity == "alice"
            && seen
                .credentials
                .upgrade()
                .is_some_and(|credentials| Arc::ptr_eq(&credentials, seed.credentials()))
    }));
    assert!(
        observations
            .iter()
            .any(|seen| seen.repository == RepositoryId::from([14; 16]) && seen.tokens.0 == "two")
    );
    drop(observations);
    remove_connection(seed);
}

#[tokio::test]
async fn rotation_during_setup_never_writes_an_older_snapshot_back() {
    let fixture = Fixture::new();
    let seed = fixture.seed("alice", "one").await;
    let source = fixture.source(&seed).await;
    let pause = Arc::new(Pause::default());
    *fixture.state.environment_pause.lock() = Some(pause.clone());
    let request = LORE_CONTEXT.scope(
        fixture.context.clone(),
        connect_owned(&source, [15; 16].into(), 1),
    );
    tokio::pin!(request);
    tokio::select! {
        _ = pause.reached.notified() => {},
        _ = &mut request => panic!("setup did not pause"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("setup did not start"),
    }
    seed.credentials().update("two", "");
    pause.resume.notify_one();
    let connection = request.await.unwrap();
    connection.ensure_storage_connected().await.unwrap();
    assert_eq!(seed.credentials().tokens().0, "two");
    assert!(Arc::ptr_eq(seed.credentials(), connection.credentials()));
    remove_connection(seed);
}

#[tokio::test]
async fn source_does_not_adopt_a_later_unrelated_cache_credential_object() {
    let fixture = Fixture::new();
    let seed = fixture.seed("alice", "one").await;
    let source = fixture.source(&seed).await;
    remove_connection(seed.clone());
    let pause = Arc::new(Pause::default());
    *fixture.state.environment_pause.lock() = Some(pause.clone());
    let request = LORE_CONTEXT.scope(
        fixture.context.clone(),
        connect_owned(&source, [16; 16].into(), 1),
    );
    tokio::pin!(request);
    tokio::select! {
        _ = pause.reached.notified() => {},
        _ = &mut request => panic!("owner setup did not pause"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("owner setup did not start"),
    }
    // An ordinary connection installs a different authority while owner setup waits.
    let replacement = fixture.seed("alice", "replacement").await;
    assert!(!Arc::ptr_eq(seed.credentials(), replacement.credentials()));
    seed.credentials().update("source-rotation", "");
    pause.resume.notify_one();
    let owned = request.await.unwrap();
    owned.ensure_storage_connected().await.unwrap();
    assert_eq!(owned.credentials().tokens().0, "source-rotation");
    assert_eq!(replacement.credentials().tokens().0, "replacement");
    fixture.assert_cached_only(&replacement);
    assert!(Arc::ptr_eq(&fixture.cached().unwrap(), &replacement));
    remove_connection(replacement);
}

#[tokio::test]
async fn environment_and_mode_mismatch_fail_before_owner_io() {
    let fixture = Fixture::new();
    let seed = fixture.seed("alice", "one").await;
    let source = fixture.source(&seed).await;
    let before = fixture.state.environment_calls.load(Ordering::Relaxed);
    let wrong = context("/another-credential-store");
    assert!(
        LORE_CONTEXT
            .scope(wrong, connect_owned(&source, [17; 16].into(), 1))
            .await
            .is_err()
    );
    assert_eq!(
        fixture.state.environment_calls.load(Ordering::Relaxed),
        before
    );
    remove_connection(seed);
    let anonymous = fixture.seed("", "").await;
    let source = fixture.source(&anonymous).await;
    anonymous.credentials().update("new-supplied-mode", "");
    let before = fixture.state.environment_calls.load(Ordering::Relaxed);
    assert!(
        LORE_CONTEXT
            .scope(
                fixture.context.clone(),
                connect_owned(&source, [17; 16].into(), 1)
            )
            .await
            .is_err()
    );
    assert_eq!(
        fixture.state.environment_calls.load(Ordering::Relaxed),
        before
    );
    remove_connection(anonymous);
}

#[tokio::test]
async fn anonymous_source_refuses_new_authentication_instead_of_resolving_an_identity() {
    let fixture = Fixture::new();
    let seed = fixture.seed("", "").await;
    let source = fixture.source(&seed).await;
    fixture.state.environment.lock().endpoint = Some(Endpoint {
        auth_url: Some("https://auth.example.com".into()),
        ..Default::default()
    });
    let before = fixture.state.observations.lock().len();
    let result = LORE_CONTEXT
        .scope(
            fixture.context.clone(),
            connect_owned(&source, [18; 16].into(), 1),
        )
        .await;
    assert!(result.is_err_and(|error| error.is_not_authenticated()));
    assert_eq!(fixture.state.observations.lock().len(), before);
    remove_connection(seed);
}

#[tokio::test]
async fn cancelled_owner_wait_retains_actual_setup_io_until_the_subtask_finishes() {
    struct Parent;
    let fixture = Fixture::new();
    let seed = fixture.seed("alice", "one").await;
    let source = fixture.source(&seed).await;
    let pause = Arc::new(Pause::default());
    *fixture.state.storage_pause.lock() = Some(pause.clone());
    let parent = Arc::new(Parent);
    let weak = Arc::downgrade(&parent);
    let lease = IoLease::new(parent);
    let context = fixture.context.clone();
    let request = lore_base::lore_spawn!(LORE_CONTEXT.scope(
        context,
        lease.scope(async move {
            let connection = connect_owned(&source, [19; 16].into(), 1).await?;
            connection.ensure_storage_connected().await
        })
    ));
    reached(&pause).await;
    request.abort();
    assert!(request.await.is_err_and(|error| error.is_cancelled()));
    drop(lease);
    assert!(weak.upgrade().is_some());
    let owned = fixture
        .state
        .observations
        .lock()
        .last()
        .unwrap()
        .connection
        .clone();
    assert!(owned.upgrade().is_some());
    pause.resume.notify_one();
    eventually(|| weak.upgrade().is_none() && owned.upgrade().is_none()).await;
    fixture.assert_cached_only(&seed);
    assert!(Arc::ptr_eq(&fixture.cached().unwrap(), &seed));
    remove_connection(seed);
}

#[tokio::test]
async fn source_capture_waits_for_actual_successful_storage_setup() {
    let fixture = Fixture::new();
    let pause = Arc::new(Pause::default());
    *fixture.state.storage_pause.lock() = Some(pause.clone());
    let seed = fixture.seed("alice", "one").await;
    let capture = LORE_CONTEXT.scope(fixture.context.clone(), seed.owner_source());
    tokio::pin!(capture);
    tokio::select! {
        _ = &mut capture => panic!("source minted before setup completed"),
        _ = pause.reached.notified() => {},
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("storage setup did not start"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut capture)
            .await
            .is_err()
    );
    pause.resume.notify_one();
    let source = capture.await.unwrap();
    assert!(Arc::ptr_eq(&source.credentials, seed.credentials()));
    remove_connection(seed.clone());
    let failed = Fixture::new();
    failed.state.fail_storage.store(true, Ordering::Release);
    let seed = failed.seed("alice", "one").await;
    assert!(
        LORE_CONTEXT
            .scope(failed.context.clone(), seed.owner_source())
            .await
            .is_err()
    );
    remove_connection(seed);
}

#[tokio::test]
async fn altered_public_diagnostics_cannot_mint_a_different_recorded_principal() {
    let fixture = Fixture::new();
    let seed = fixture.seed("alice", "one").await;
    let source = fixture.source(&seed).await;
    remove_connection(seed.clone());
    let raw = tokio::time::timeout(Duration::from_secs(10), async {
        let mut candidate = seed;
        loop {
            match Arc::try_unwrap(candidate) {
                Ok(connection) => break connection,
                Err(connection) => {
                    candidate = connection;
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .unwrap();
    let mut raw = raw;
    raw.identity = "mallory".into();
    assert!(
        LORE_CONTEXT
            .scope(fixture.context.clone(), raw.owner_source())
            .await
            .is_err()
    );
    raw.identity = source.identity().to_string();
    raw.remote_url = Url::parse("lore://another.invalid").unwrap();
    assert!(
        LORE_CONTEXT
            .scope(fixture.context.clone(), raw.owner_source())
            .await
            .is_err()
    );
}
