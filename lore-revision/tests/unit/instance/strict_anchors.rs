// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_revision::instance;
use lore_revision::instance::strict_anchors;
use lore_revision::repository::RepositoryContext;
use lore_storage::MutableStore;
use lore_storage::Partition;
use lore_storage::StoreError;
use lore_storage::local::store_lock::StoreGuard;
use lore_storage::store_types::KeyType;
use lore_storage::store_types::KeyValueStream;

use crate::helper::default_repository_creation_args;
use crate::helper::test_store_create;

struct FaultStore {
    inner: Arc<dyn MutableStore>,
    fault: Mutex<Option<(Hash, StoreError)>>,
}
#[async_trait]
impl MutableStore for FaultStore {
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
        {
            let mut fault = self.fault.lock().unwrap();
            if fault.as_ref().is_some_and(|(selected, _)| *selected == key) {
                return Err(fault.take().unwrap().1);
            }
        }
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
        self.inner.clone().flush(sync_data).await
    }
}

async fn put(
    repository: &RepositoryContext,
    store: &Arc<dyn MutableStore>,
    function: &str,
    value: Hash,
) {
    let (key, kind) = instance::anchor_key(repository.salt(), function, repository.instance_id);
    store
        .clone()
        .store(repository.id, key, value, kind)
        .await
        .unwrap();
}

#[tokio::test]
async fn reads_exact_instance_values_and_canonical_empty_anchors() {
    let (immutable, store, execution) = test_store_create().await.unwrap();
    LORE_CONTEXT
        .scope(execution, async {
            let repository = RepositoryContext::new(default_repository_creation_args(
                immutable.clone(),
                store.clone(),
            ));
            let mut other_args = default_repository_creation_args(immutable, store.clone());
            other_args.id = repository.id;
            let other = RepositoryContext::new(other_args);
            let branch = [7; 16].into();
            put(
                &repository,
                &store,
                instance::ANCHOR_CURRENT_BRANCH,
                Hash::from_context(branch),
            )
            .await;
            let empty = strict_anchors::read(&repository).await.unwrap();
            assert_eq!(empty.current, Hash::default());
            assert_eq!(empty.staged, None);
            assert_eq!(empty.repository, repository.id);
            assert_eq!(empty.instance, repository.instance_id);
            assert_eq!(empty.branch, branch);
            assert!(matches!(
                strict_anchors::read(&other).await,
                Err(strict_anchors::Error::InvalidBranch)
            ));

            let current = Hash::from_u64(12);
            let staged = Hash::from_u64(13);
            put(&repository, &store, instance::ANCHOR_CURRENT, current).await;
            put(&repository, &store, instance::ANCHOR_STAGED, staged).await;
            let actual = strict_anchors::read(&repository).await.unwrap();
            assert_eq!(actual.current, current);
            assert_eq!(actual.staged, Some(staged));
            put(
                &repository,
                &store,
                instance::ANCHOR_STAGED,
                Hash::default(),
            )
            .await;
            assert_eq!(
                strict_anchors::read(&repository).await.unwrap().staged,
                None
            );
        })
        .await;
}

#[tokio::test]
async fn rejects_missing_malformed_branch_and_zero_identity() {
    let (immutable, store, execution) = test_store_create().await.unwrap();
    LORE_CONTEXT
        .scope(execution, async {
            let mut args = default_repository_creation_args(immutable.clone(), store.clone());
            args.instance_id = Default::default();
            assert!(matches!(
                strict_anchors::read(&RepositoryContext::new(args)).await,
                Err(strict_anchors::Error::InvalidIdentity)
            ));
            let repository =
                RepositoryContext::new(default_repository_creation_args(immutable, store.clone()));
            assert!(matches!(
                strict_anchors::read(&repository).await,
                Err(strict_anchors::Error::InvalidBranch)
            ));
            put(
                &repository,
                &store,
                instance::ANCHOR_CURRENT_BRANCH,
                Hash::from([9; 32]),
            )
            .await;
            assert!(matches!(
                strict_anchors::read(&repository).await,
                Err(strict_anchors::Error::InvalidBranch)
            ));
        })
        .await;
}

#[tokio::test]
async fn payload_loss_and_io_errors_are_not_absence_at_any_anchor() {
    let (immutable, inner, execution) = test_store_create().await.unwrap();
    LORE_CONTEXT
        .scope(execution, async {
            let fault = Arc::new(FaultStore {
                inner: inner.clone(),
                fault: Mutex::new(None),
            });
            let repository =
                RepositoryContext::new(default_repository_creation_args(immutable, fault.clone()));
            put(
                &repository,
                &inner,
                instance::ANCHOR_CURRENT_BRANCH,
                Hash::from_context([7; 16].into()),
            )
            .await;
            for function in [
                instance::ANCHOR_CURRENT,
                instance::ANCHOR_CURRENT_BRANCH,
                instance::ANCHOR_STAGED,
            ] {
                let (key, _) =
                    instance::anchor_key(repository.salt(), function, repository.instance_id);
                *fault.fault.lock().unwrap() =
                    Some((key, StoreError::internal("injected read failure")));
                assert!(matches!(
                    strict_anchors::read(&repository).await,
                    Err(strict_anchors::Error::Store(StoreError::Internal(_)))
                ));
                *fault.fault.lock().unwrap() = Some((
                    key,
                    StoreError::from(lore_base::error::PayloadNotFound::from(Hash::from_u64(99))),
                ));
                assert!(matches!(
                    strict_anchors::read(&repository).await,
                    Err(strict_anchors::Error::Store(StoreError::PayloadNotFound(_)))
                ));
                *fault.fault.lock().unwrap() =
                    Some((key, StoreError::from(lore_base::error::NotAuthorized)));
                assert!(matches!(
                    strict_anchors::read(&repository).await,
                    Err(strict_anchors::Error::Store(StoreError::NotAuthorized(_)))
                ));
            }
        })
        .await;
}
