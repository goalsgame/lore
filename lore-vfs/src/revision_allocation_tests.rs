// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use lore_storage::Fragment;
use lore_storage::ImmutableStore;
use lore_storage::Partition;
use lore_storage::StoreError;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::StoreObliterateStats;
use lore_storage::immutable_store::CopyBehavior;
use lore_storage::local::store_lock::StoreGuard;
use lore_storage::local::store_lock::StoreLock;
use tokio::sync::Notify;
use tokio::sync::Semaphore;

use super::*;

/// Delegates real immutable reads, pausing only the selected cold child state.
struct PausedStore {
    inner: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    armed: AtomicBool,
    entered: Notify,
    release: Semaphore,
    pauses: AtomicUsize,
}
#[async_trait]
impl ImmutableStore for PausedStore {
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }
    fn isolates_partitions(&self) -> bool {
        self.inner.isolates_partitions()
    }
    fn read_scope(&self) -> StoreMatch {
        self.inner.read_scope()
    }
    fn query_scope(&self) -> StoreMatch {
        self.inner.query_scope()
    }
    fn try_join_claim(&self) -> Option<StoreGuard> {
        self.inner.try_join_claim()
    }
    fn store_lock(&self) -> Option<Arc<StoreLock>> {
        self.inner.store_lock()
    }
    async fn hold_for_command(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        self.inner.clone().hold_for_command().await
    }
    async fn hold_for_task(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        self.inner.clone().hold_for_task().await
    }
    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        if partition == self.partition
            && address == self.address
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.pauses.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.clone().get(partition, address).await
    }
    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        self.inner.clone().get_metadata(partition, address).await
    }
    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        self.inner
            .clone()
            .query(partition, addresses, results)
            .await
    }
    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
        force: bool,
    ) -> Result<(), StoreError> {
        self.inner
            .clone()
            .put(partition, address, fragment, payload, force)
            .await
    }
    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        self.inner
            .clone()
            .obliterate(partition, address, stats)
            .await
    }
    async fn evict(
        self: Arc<Self>,
        max_capacity: usize,
        sync_data: bool,
        sink: Option<lore_storage::gc_event::GcEventSinkRef>,
    ) -> Result<usize, StoreError> {
        self.inner
            .clone()
            .evict(max_capacity, sync_data, sink)
            .await
    }
    async fn compact(
        self: Arc<Self>,
        max_size: usize,
        at: Option<usize>,
        sync_data: bool,
        sink: Option<lore_storage::gc_event::GcEventSinkRef>,
    ) -> Result<Option<usize>, StoreError> {
        self.inner
            .clone()
            .compact(max_size, at, sync_data, sink)
            .await
    }
    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        self.inner.clone().compact_resume_at().await
    }
    fn max_query_batch(&self) -> Option<usize> {
        self.inner.max_query_batch()
    }
    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        self.inner.clone().flush(sync_data).await
    }
    async fn verify(self: Arc<Self>, heal: bool) -> Result<(), StoreError> {
        self.inner.clone().verify(heal).await
    }
    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        self.inner
            .clone()
            .copy(
                source_partition,
                source_address,
                destination_partition,
                destination_context,
                behavior,
            )
            .await
    }
}

struct AllocationFixture {
    backend: Arc<RevisionBackend>,
    store: Arc<PausedStore>,
    known: Node,
    known_file: Node,
    allowed: Arc<AtomicBool>,
}
impl AllocationFixture {
    async fn new() -> Self {
        let fixture = Fixture::new().await;
        let cold = fixture.repository(181);
        let known = fixture.repository(182);
        let parent = fixture.repository(183);
        let cold_state = State::new();
        file(&cold_state, &cold, ROOT_NODE, "data", b"cold bytes").await;
        let cold_pin = freeze(&cold, &cold_state).await;
        let known_state = State::new();
        file(&known_state, &known, ROOT_NODE, "data", b"known bytes").await;
        let known_pin = freeze(&known, &known_state).await;
        let state = State::new();
        link(
            &state, &parent, ROOT_NODE, "known", &known, known_pin, ROOT_NODE,
        )
        .await;
        link(
            &state, &parent, ROOT_NODE, "cold", &cold, cold_pin, ROOT_NODE,
        )
        .await;
        let pin = freeze(&parent, &state).await;
        let store = Arc::new(PausedStore {
            inner: fixture.immutable,
            partition: cold.id,
            address: Address::zero_context_hash(cold_pin),
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
            pauses: AtomicUsize::new(0),
        });
        let allowed = Arc::new(AtomicBool::new(true));
        let policy = allowed.clone();
        let repository = Arc::new(
            RepositoryContext::new_server_context(store.clone(), fixture.mutable, parent.id)
                .with_link_read(Arc::new(move |_| policy.load(Ordering::SeqCst))),
        );
        let backend = Arc::new(RevisionBackend::new(repository, pin).await.unwrap());
        let known = backend.lookup(&backend.root().key, b"known").await.unwrap();
        let known_file = backend.lookup(&known.key, b"data").await.unwrap();
        Self {
            backend,
            store,
            known,
            known_file,
            allowed,
        }
    }
    async fn paused_lookup(&self) -> tokio::task::JoinHandle<Result<Node, Error>> {
        self.store.armed.store(true, Ordering::SeqCst);
        let backend = self.backend.clone();
        let task =
            lore_base::lore_spawn!(
                async move { backend.lookup(&backend.root().key, b"cold").await }
            );
        tokio::time::timeout(Duration::from_secs(5), self.store.entered.notified())
            .await
            .unwrap();
        assert_eq!(self.store.pauses.load(Ordering::SeqCst), 1);
        task
    }
    async fn assert_known_reads(&self) {
        assert_eq!(
            self.backend
                .lookup(&self.backend.root().key, b"known")
                .await
                .unwrap(),
            self.known
        );
        assert_eq!(
            self.backend
                .read(&self.known_file.key, 0, 64)
                .await
                .unwrap(),
            b"known bytes"
        );
    }
}

#[tokio::test]
async fn retired_allocator_rechecks_after_child_metadata_io_without_changing_provenance() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = AllocationFixture::new().await;
            let before = fixture.backend.checkpoint_state().await.unwrap();
            let task = fixture.paused_lookup().await;
            let epoch = fixture.backend.mutation_gate.epoch().unwrap();
            let seal = fixture.backend.mutation_gate.seal(epoch).unwrap();
            fixture.backend.owners.lock().unwrap().allocation = AllocationState::Retired;
            drop(seal);
            fixture.store.release.add_permits(1);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), task)
                    .await
                    .unwrap()
                    .unwrap(),
                Err(Error::Stale)
            );
            assert_eq!(fixture.backend.checkpoint_state().await.unwrap(), before);
            assert_eq!(
                fixture
                    .backend
                    .lookup(&fixture.backend.root().key, b"cold")
                    .await,
                Err(Error::Stale)
            );
            assert_eq!(fixture.store.pauses.load(Ordering::SeqCst), 1);
            fixture.assert_known_reads().await;
            fixture.allowed.store(false, Ordering::SeqCst);
            assert_eq!(
                fixture.backend.read(&fixture.known_file.key, 0, 64).await,
                Err(Error::PermissionDenied)
            );
            let opaque = fixture
                .backend
                .lookup(&fixture.backend.root().key, b"cold")
                .await
                .unwrap();
            assert_eq!(opaque.key.exposure, 0);
            fixture
                .backend
                .revalidate(&opaque.key, false)
                .await
                .unwrap();
            assert_eq!(
                fixture.backend.revalidate(&opaque.key, true).await,
                Err(Error::PermissionDenied)
            );
            assert_eq!(
                fixture.backend.lookup(&opaque.key, b"data").await,
                Err(Error::PermissionDenied)
            );
            assert_eq!(
                fixture.backend.checkpoint_state().await.unwrap_err(),
                Error::PermissionDenied
            );
        })
        .await;
}

#[tokio::test]
async fn active_allocator_rechecks_seal_after_child_io_and_retries_one_route() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = AllocationFixture::new().await;
            let before = fixture.backend.checkpoint_state().await.unwrap();
            let early_seal = fixture
                .backend
                .mutation_gate
                .seal(fixture.backend.mutation_gate.epoch().unwrap())
                .unwrap();
            fixture.store.armed.store(true, Ordering::SeqCst);
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    fixture.backend.lookup(&fixture.backend.root().key, b"cold")
                )
                .await
                .unwrap(),
                Err(Error::Busy)
            );
            assert_eq!(fixture.store.pauses.load(Ordering::SeqCst), 0);
            drop(early_seal);
            let task = fixture.paused_lookup().await;
            let seal = fixture
                .backend
                .mutation_gate
                .seal(fixture.backend.mutation_gate.epoch().unwrap())
                .unwrap();
            fixture.store.release.add_permits(1);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), task)
                    .await
                    .unwrap()
                    .unwrap(),
                Err(Error::Busy)
            );
            assert_eq!(fixture.backend.checkpoint_state().await.unwrap(), before);
            fixture.assert_known_reads().await;
            drop(seal);
            let child = fixture
                .backend
                .lookup(&fixture.backend.root().key, b"cold")
                .await
                .unwrap();
            assert_eq!(child.key.exposure, 2);
            assert_eq!(
                fixture
                    .backend
                    .lookup(&fixture.backend.root().key, b"cold")
                    .await
                    .unwrap(),
                child
            );
            assert_eq!(fixture.backend.owners.lock().unwrap().next, 3);
        })
        .await;
}

#[tokio::test]
async fn prepared_allocator_is_checkpointable_but_unknown_links_remain_unsupported() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = AllocationFixture::new().await;
            let bytes = fixture.backend.checkpoint_state().await.unwrap();
            let mut budget = crate::transition_prepare::Budget::new(Default::default()).unwrap();
            let prepared = checkpoint::prepare_bounded(
                &fixture.backend,
                &bytes,
                &mut budget,
                AllocationState::Prepared,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                prepared.owners.lock().unwrap().allocation,
                AllocationState::Prepared
            );
            assert_eq!(prepared.checkpoint_state().await.unwrap(), bytes);
            fixture.store.armed.store(true, Ordering::SeqCst);
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    prepared.lookup(&prepared.root().key, b"cold")
                )
                .await
                .unwrap(),
                Err(Error::Unsupported)
            );
            assert_eq!(fixture.store.pauses.load(Ordering::SeqCst), 0);
            fixture.store.armed.store(false, Ordering::SeqCst);
            assert_eq!(
                prepared
                    .lookup(&prepared.root().key, b"known")
                    .await
                    .unwrap(),
                fixture.known
            );
            assert_eq!(
                prepared.read(&fixture.known_file.key, 0, 64).await.unwrap(),
                b"known bytes"
            );
            assert_eq!(prepared.checkpoint_state().await.unwrap(), bytes);
        })
        .await;
}

#[tokio::test]
async fn independently_restored_allocator_stays_active_when_source_is_retired() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = AllocationFixture::new().await;
            let bytes = fixture.backend.checkpoint_state().await.unwrap();
            let restored = fixture
                .backend
                .prepare_restore(&bytes)
                .await
                .unwrap()
                .unwrap();
            let restored = restored
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            assert_eq!(
                restored.owners.lock().unwrap().allocation,
                AllocationState::Active
            );
            assert!(Arc::ptr_eq(
                &restored.mutation_gate,
                &fixture.backend.mutation_gate
            ));
            fixture.backend.owners.lock().unwrap().allocation = AllocationState::Retired;
            let cold = restored
                .lookup(&restored.root().key, b"cold")
                .await
                .unwrap();
            assert_eq!(cold.key.exposure, 2);
            assert_eq!(restored.owners.lock().unwrap().next, 3);
            assert_eq!(fixture.backend.owners.lock().unwrap().next, 2);
            assert_eq!(
                fixture
                    .backend
                    .lookup(&fixture.backend.root().key, b"cold")
                    .await,
                Err(Error::Stale)
            );
            assert_eq!(fixture.backend.checkpoint_state().await.unwrap(), bytes);
            fixture.assert_known_reads().await;
        })
        .await;
}
