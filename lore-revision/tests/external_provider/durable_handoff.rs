// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_storage::Hash;
use lore_storage::MutableStore;
use lore_storage::Partition;
use lore_storage::StoreError;
use lore_storage::local::store_lock::StoreGuard;
use lore_storage::store_types::KeyType;
use lore_storage::store_types::KeyValueStream;

use super::*;

struct FlushStore {
    inner: Arc<dyn MutableStore>,
    flags: Mutex<Vec<bool>>,
    fail: AtomicBool,
    block: AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[async_trait]
impl MutableStore for FlushStore {
    async fn hold_for_command(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        self.inner.clone().hold_for_command().await
    }
    fn try_join_claim(&self) -> Option<StoreGuard> {
        self.inner.try_join_claim()
    }
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        kind: KeyType,
    ) -> Result<Hash, StoreError> {
        self.inner.clone().load(partition, key, kind).await
    }
    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        kind: KeyType,
    ) -> Result<(), StoreError> {
        self.inner.clone().store(partition, key, value, kind).await
    }
    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        kind: KeyType,
    ) -> Result<Hash, StoreError> {
        self.inner
            .clone()
            .compare_and_swap(partition, key, expected, value, kind)
            .await
    }
    async fn list(
        self: Arc<Self>,
        partition: Partition,
        kind: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        self.inner.clone().list(partition, kind).await
    }
    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        self.flags.lock().push(sync_data);
        if self.block.load(Ordering::SeqCst) {
            self.started.notify_one();
            self.resume.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(StoreError::internal("injected store flush failure"));
        }
        self.inner.clone().flush(sync_data).await
    }
}
async fn fixture() -> (
    Arc<RepositoryContext>,
    Arc<FlushStore>,
    Arc<lore_revision::interface::ExecutionContext>,
) {
    let (immutable, mutable, execution) = test_store_create().await.unwrap();
    let mutable = Arc::new(FlushStore {
        inner: mutable,
        flags: Mutex::new(Vec::new()),
        fail: AtomicBool::new(false),
        block: AtomicBool::new(false),
        started: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    (
        in_memory::repository(immutable, mutable.clone()),
        mutable,
        execution,
    )
}
fn provider(durable: bool) -> FakeProvider {
    FakeProvider {
        durable_handoff: durable,
        holds_state: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn opt_in_dispatch_forces_sync_before_callback_while_volatile_policy_is_unchanged() {
    for durable in [false, true] {
        let (repository, stores, execution) = fixture().await;
        LORE_CONTEXT
            .scope(execution, async {
                let provider = provider(durable);
                let operation = provider.begin_operation().await.unwrap();
                assert_eq!(operation.requires_durable_commit_handoff(), durable);
                assert!(
                    !repository
                        .file_system()
                        .begin_operation()
                        .await
                        .unwrap()
                        .requires_durable_commit_handoff()
                );
                let committed = State::new();
                lore_revision::commit::hand_over_committed(
                    &repository,
                    &operation,
                    &committed,
                    Some(lore_revision::state::NodeMapping::root(
                        repository.clone(),
                        committed.clone(),
                    )),
                )
                .await
                .unwrap();
                assert_eq!(*stores.flags.lock(), [durable]);
                assert_eq!(provider.events.lock().commits.len(), 1);
                operation.finalize().await.unwrap();
                stores.flags.lock().clear();
                let ordinary = repository.file_system().begin_operation().await.unwrap();
                lore_revision::commit::hand_over_committed(
                    &repository,
                    &ordinary,
                    &committed,
                    None,
                )
                .await
                .unwrap();
                assert_eq!(*stores.flags.lock(), [false]);
                ordinary.finalize().await.unwrap();
                let metadata_only = FakeProvider {
                    durable_handoff: true,
                    ..Default::default()
                };
                assert!(
                    !metadata_only
                        .begin_operation()
                        .await
                        .unwrap()
                        .requires_durable_commit_handoff()
                );
            })
            .await;
    }
}

#[tokio::test]
async fn ignored_flush_failure_prevents_callback_and_successful_finalization() {
    for durable in [false, true] {
        let (repository, stores, execution) = fixture().await;
        LORE_CONTEXT
            .scope(execution, async {
                let provider = provider(durable);
                let operation = provider.begin_operation().await.unwrap();
                stores.fail.store(true, Ordering::SeqCst);
                let committed = State::new();
                assert!(
                    lore_revision::commit::hand_over_committed(
                        &repository,
                        &operation,
                        &committed,
                        Some(lore_revision::state::NodeMapping::root(
                            repository.clone(),
                            committed.clone()
                        ))
                    )
                    .await
                    .is_err()
                );
                assert_eq!(*stores.flags.lock(), [durable]);
                assert!(provider.events.lock().commits.is_empty());
                assert!(operation.finalize().await.is_err());
                assert_eq!(
                    provider.events.lock().outcomes,
                    [OperationOutcome::Failed, OperationOutcome::Aborted]
                );
            })
            .await;
    }
}

#[tokio::test]
async fn cancelled_flush_prevents_callback_and_latches_failed_critical_handoff() {
    let (repository, stores, execution) = fixture().await;
    LORE_CONTEXT
        .scope(execution, async {
            let provider = provider(true);
            let operation = provider.begin_operation().await.unwrap();
            stores.block.store(true, Ordering::SeqCst);
            let committed = State::new();
            {
                let handoff = lore_revision::commit::hand_over_committed(
                    &repository,
                    &operation,
                    &committed,
                    Some(lore_revision::state::NodeMapping::root(
                        repository.clone(),
                        committed.clone(),
                    )),
                );
                tokio::pin!(handoff);
                tokio::select! {
                    _ = stores.started.notified() => {},
                    result = &mut handoff => panic!("flush did not block: {result:?}"),
                }
            }
            assert_eq!(*stores.flags.lock(), [true]);
            assert!(provider.events.lock().commits.is_empty());
            assert!(operation.finalize().await.is_err());
            assert_eq!(
                provider.events.lock().outcomes,
                [OperationOutcome::Failed, OperationOutcome::Aborted]
            );
        })
        .await;
}
