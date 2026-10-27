// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Real owner-source protocol fixture serving verified immutable objects without a network account.

use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_base::types::RepositoryId;
use lore_base::types::VerifyResult;
use lore_revision::interface::ExecutionContext;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::working_bootstrap::ImportReadPolicy;
use lore_revision::repository::working_bootstrap::RemoteContents;
use lore_transport::auth::AuthEndpoint;
use lore_transport::*;

fn unsupported() -> ProtocolError {
    ProtocolError::internal("read-only hydration fixture")
}
static NEXT: AtomicU64 = AtomicU64::new(1);
pub struct RemoteFixture {
    repository: Arc<RepositoryContext>,
    payload_hashes: Vec<Hash>,
    payload_gets: AtomicUsize,
    payload_denied: AtomicBool,
    sessions_detached: AtomicBool,
    metadata_gets: AtomicUsize,
    url: String,
}
impl RemoteFixture {
    pub fn new(repository: Arc<RepositoryContext>, payload_hashes: Vec<Hash>) -> Arc<Self> {
        let scheme = format!("workinghydration{}", NEXT.fetch_add(1, Ordering::Relaxed));
        let fixture = Arc::new(Self {
            repository,
            payload_hashes,
            payload_gets: AtomicUsize::new(0),
            payload_denied: AtomicBool::new(false),
            sessions_detached: AtomicBool::new(true),
            metadata_gets: AtomicUsize::new(0),
            url: format!("{scheme}://example.com"),
        });
        lore_transport::add(&scheme, Arc::new(FixtureProtocol(Arc::downgrade(&fixture)))).unwrap();
        fixture
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn payload_gets(&self) -> usize {
        self.payload_gets.load(Ordering::SeqCst)
    }
    pub fn set_payload_denied(&self, denied: bool) {
        self.payload_denied.store(denied, Ordering::SeqCst);
    }
    pub fn sessions_detached(&self) -> bool {
        self.sessions_detached.load(Ordering::SeqCst)
    }
    pub fn metadata_gets(&self) -> usize {
        self.metadata_gets.load(Ordering::SeqCst)
    }
    pub async fn contents(self: &Arc<Self>, execution: Arc<ExecutionContext>) -> RemoteContents {
        let execution = Arc::new(execution.for_background_owner().await);
        LORE_CONTEXT
            .scope(execution, async {
                let connection = lore_transport::connect(
                    &self.url,
                    "fixture",
                    self.repository.id,
                    1,
                    "",
                    "",
                    Default::default(),
                )
                .await
                .unwrap();
                let source = connection.owner_source().await.unwrap();
                lore_transport::remove_connection(connection);
                RemoteContents::new(
                    source,
                    ImportReadPolicy::from_repository(&self.repository).unwrap(),
                )
                .unwrap()
            })
            .await
    }
}
struct FixtureStorage {
    fixture: Weak<RemoteFixture>,
    partition: Partition,
}
#[async_trait]
impl Storage for FixtureStorage {
    async fn session_start(
        &self,
        _partition: Partition,
        _correlation: &str,
    ) -> Result<u32, ProtocolError> {
        let fixture = self.fixture.upgrade().ok_or_else(unsupported)?;
        let execution = lore_revision::lore::execution_context();
        if execution.caller_state().is_some() || execution.dispatcher.weak_sender.is_some() {
            fixture.sessions_detached.store(false, Ordering::SeqCst);
        }
        Ok(1)
    }
    async fn session_stop(&self, _session: u32) -> Result<(), ProtocolError> {
        Ok(())
    }
    async fn get(
        &self,
        _session: u32,
        address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        let server = self.fixture.upgrade().ok_or_else(unsupported)?;
        if server.payload_hashes.contains(&address.hash) {
            server.payload_gets.fetch_add(1, Ordering::SeqCst);
            if server.payload_denied.load(Ordering::SeqCst) {
                return Err(ProtocolError::internal("fixture payload unavailable"));
            }
        } else {
            server.metadata_gets.fetch_add(1, Ordering::SeqCst);
        }
        let stored = server
            .repository
            .immutable_store()
            .get(self.partition, *address)
            .await
            .map_err(|error| ProtocolError::internal(error.to_string()))?;
        Ok((stored.fragment, stored.payload.ok_or_else(unsupported)?))
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

struct FixtureProtocol(Weak<RemoteFixture>);
struct SeedEnvironment;
#[async_trait]
impl Environment for SeedEnvironment {
    async fn get(&self) -> Result<EnvironmentConfig, ProtocolError> {
        Ok(EnvironmentConfig::default())
    }
}
#[async_trait]
impl Protocol for FixtureProtocol {
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
        partition: Partition,
        _index: usize,
        _credentials: &Arc<SuppliedCredentials>,
    ) -> Result<Arc<dyn Storage>, ProtocolError> {
        Ok(Arc::new(FixtureStorage {
            fixture: self.0.clone(),
            partition,
        }))
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
