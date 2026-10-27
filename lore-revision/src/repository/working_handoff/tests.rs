// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_base::test_util::TempDir;
use lore_storage::Hash;
use lore_storage::KeyType;
use lore_storage::Partition;
use lore_storage::test_util::PausePoint;

use super::*;
use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperationImpl;
use crate::fs::provider_registry::Limits;
use crate::fs::provider_registry::ProviderRegistry;
use crate::fs::provider_registry::ProviderResolver;
use crate::fs::provider_registry::Registration;
use crate::interface::ExecutionContext;
use crate::runtime::LORE_CONTEXT;

struct Provider(crate::fs::os::OsFilesystem);
#[async_trait::async_trait]
impl FilesystemProvider for Provider {
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        FilesystemProvider::begin_operation(&self.0).await
    }
    async fn read_metadata_file(
        &self,
        path: &crate::util::path::RelativePath,
        _maximum: usize,
    ) -> Result<Vec<u8>, FsError> {
        Err(lore_base::error::FileNotFound {
            resource: path.to_string(),
        }
        .into())
    }
}
struct Fixture {
    _target: TempDir,
    _metadata: TempDir,
    registry: Arc<ProviderRegistry>,
    registration: Registration,
    execution: Arc<ExecutionContext>,
}
impl Fixture {
    async fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let target = TempDir::new("lore-command-handoff-root-");
        let metadata = TempDir::new("lore-command-handoff-metadata-");
        let registry = ProviderRegistry::new(Limits::default());
        let resolver: Arc<dyn ProviderResolver> = registry.clone();
        let mut execution = ExecutionContext::default();
        execution.set_provider_resolver(&resolver);
        let execution = Arc::new(execution);
        let instance = LORE_CONTEXT
            .scope(execution.clone(), async {
                let token = RepositoryWriteToken::acquire(target.path()).await;
                let repository = create_local(
                    target.path(),
                    &token,
                    [101; 16].into(),
                    [102; 16].into(),
                    "main".into(),
                    Default::default(),
                    false,
                )
                .await
                .unwrap();
                repository.flush(true).await.unwrap();
                repository.instance_id
            })
            .await;
        // Keep the normal stores and anchors, while exercising the real external
        // metadata discovery and the actual loader/facade path.
        let external = metadata.path().join("metadata");
        std::fs::rename(target.path().join(DOT_LORE), &external).unwrap();
        std::fs::set_permissions(&external, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = RepositoryConfig {
            vfs: Some(VfsConfig {
                vfs_type: VfsType::ExperimentalWorking,
            }),
            ..Default::default()
        };
        std::fs::write(external.join(CONFIG), toml::to_string(&config).unwrap()).unwrap();
        for name in [ID, INSTANCE, CONFIG] {
            std::fs::set_permissions(external.join(name), std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        let registration = registry.reserve(target.path(), instance).unwrap();
        registration
            .reserve_metadata(&external, [101; 16].into())
            .unwrap();
        registration
            .publish(Arc::new(Provider(crate::fs::os::OsFilesystem::new(
                target.path(),
            ))))
            .unwrap();
        Self {
            _target: target,
            _metadata: metadata,
            registry,
            registration,
            execution,
        }
    }
    async fn load(&self, access: RepositoryAccess) -> Arc<RepositoryContext> {
        load_and_connect(self.registration.target().path(), access)
            .await
            .unwrap()
    }
}
async fn until(mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn exact_loaded_writer_excludes_normal_stores_and_reads_strict_anchors() {
    let fixture = Fixture::new().await;
    LORE_CONTEXT
        .scope(fixture.execution.clone(), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let expected = strict_anchors::read(&repository).await.unwrap();
            let handoff = repository.try_working_handoff().unwrap();
            assert_eq!(handoff.read_anchors().await.unwrap(), expected);
            assert!(
                load_and_connect(
                    fixture.registration.target().path(),
                    RepositoryAccess::ReadOnly
                )
                .await
                .is_err()
            );
            assert!(
                repository
                    .read_mutable_store()
                    .list(Partition::default(), KeyType::BranchMetadata)
                    .await
                    .is_err()
            );
            assert!(
                repository
                    .immutable_store()
                    .get(Partition::default(), Default::default())
                    .await
                    .is_err()
            );
            drop(handoff);
            repository
                .read_mutable_store()
                .list(Partition::default(), KeyType::BranchMetadata)
                .await
                .unwrap();
        })
        .await;
}

#[tokio::test]
async fn sibling_loads_and_retained_listing_producers_refuse_without_waiting() {
    let fixture = Fixture::new().await;
    LORE_CONTEXT
        .scope(fixture.execution.clone(), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let other = fixture.load(RepositoryAccess::ReadOnly).await;
            assert!(repository.try_working_handoff().is_err());
            drop(other);
            let sibling = load_and_connect_with_token(
                fixture.registration.target().path(),
                RepositoryAccess::ReadWrite,
                Some(repository.try_write_token().unwrap().share()),
            )
            .await
            .unwrap();
            assert!(repository.try_working_handoff().is_err());
            drop(sibling);
            let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
            let parent = IoLease::new(Arc::new(()));
            let stream = parent
                .scope(
                    repository
                        .read_mutable_store()
                        .list(Partition::default(), KeyType::BranchMetadata),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), pause.reached.notified())
                .await
                .unwrap();
            drop(stream.channel());
            assert!(repository.try_working_handoff().is_err());
            pause.resume.notify_one();
            // Poll the nonwaiting admission until all actual producers complete.
            let handoff = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(handoff) = repository.try_working_handoff() {
                        break handoff;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            handoff.read_anchors().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn tokenless_foreign_and_unproven_contexts_cannot_handoff() {
    let fixture = Fixture::new().await;
    LORE_CONTEXT
        .scope(fixture.execution.clone(), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let stripped = repository.without_command_locks();
            assert!(stripped.try_working_handoff().is_err());
            let without_claims =
                stripped.with_write_token(repository.try_write_token().unwrap().share());
            assert!(without_claims.try_working_handoff().is_err());
            let other = TempDir::new("lore-command-handoff-other-");
            let foreign = RepositoryWriteToken::acquire(other.path()).await;
            let wrong_token = repository
                .to_filter_context(repository.filter.clone())
                .with_write_token(foreign);
            assert!(wrong_token.try_working_handoff().is_err());
            let mut wrong_partition = repository.to_filter_context(repository.filter.clone());
            wrong_partition.id = [103; 16].into();
            assert!(wrong_partition.try_working_handoff().is_err());
            let same_repo_link = repository.mounted(
                repository.id,
                MountKind::Link,
                Err(lore_transport::ProtocolError::from(
                    lore_base::error::NoRemote,
                )),
            );
            assert!(same_repo_link.try_working_handoff().is_err());
            let owner_context = RepositoryContext::new_with_state(
                None,
                repository
                    .working_resources
                    .as_ref()
                    .unwrap()
                    .immutable
                    .clone(),
                repository
                    .working_resources
                    .as_ref()
                    .unwrap()
                    .mutable
                    .clone(),
                repository.id,
                repository.instance_id,
                RemoteState::Offline,
                Arc::default(),
                None,
            );
            assert!(owner_context.working_admission.is_none());
            assert!(owner_context.working_resources.is_none());
            assert!(owner_context.try_working_handoff().is_err());
        })
        .await;
}

#[tokio::test]
async fn retirement_or_changed_metadata_refuses_handoff_and_existing_anchor_reads() {
    let fixture = Fixture::new().await;
    LORE_CONTEXT
        .scope(fixture.execution.clone(), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let path = repository
                .working_admission
                .as_ref()
                .unwrap()
                .location()
                .metadata()
                .path()
                .join(ID);
            let bytes = std::fs::read(&path).unwrap();
            std::fs::write(&path, [104; 16]).unwrap();
            assert!(repository.try_working_handoff().is_err());
            std::fs::write(&path, &bytes).unwrap();
            let handoff = repository.try_working_handoff().unwrap();
            let instance_path = path.with_file_name(INSTANCE);
            let instance_bytes = std::fs::read(&instance_path).unwrap();
            std::fs::write(&instance_path, [105; 16]).unwrap();
            assert!(handoff.read_anchors().await.is_err());
            std::fs::write(&instance_path, instance_bytes).unwrap();
            handoff.read_anchors().await.unwrap();
            fixture.registration.retire().unwrap();
            assert!(handoff.read_anchors().await.is_err());
            drop(handoff);
            assert!(repository.try_working_handoff().is_err());
            assert!(
                fixture
                    .registry
                    .resolve_bootstrap(repository.working_admission.as_ref().unwrap().location())
                    .is_err()
            );
        })
        .await;
}

#[tokio::test]
async fn malformed_anchor_is_an_error_and_is_not_repaired_by_inspection() {
    let fixture = Fixture::new().await;
    LORE_CONTEXT
        .scope(fixture.execution.clone(), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let (key, kind) = crate::instance::anchor_key(
                repository.salt(),
                crate::instance::ANCHOR_CURRENT_BRANCH,
                repository.instance_id,
            );
            let malformed = Hash::from([117; 32]);
            repository
                .try_write_mutable_store()
                .unwrap()
                .store(repository.id, key, malformed, kind)
                .await
                .unwrap();
            let handoff = repository.try_working_handoff().unwrap();
            assert!(handoff.read_anchors().await.is_err());
            drop(handoff);
            assert_eq!(
                repository
                    .read_mutable_store()
                    .load(repository.id, key, kind)
                    .await
                    .unwrap(),
                malformed
            );
        })
        .await;
}

struct Completion {
    client: Arc<tokio::sync::Mutex<()>>,
    store: Arc<lore_storage::local::store_lock::StoreLock>,
    dropped: Arc<AtomicBool>,
}
impl Drop for Completion {
    fn drop(&mut self) {
        assert!(
            self.client.try_lock().is_ok(),
            "actual Client must release before inherited completion"
        );
        assert!(
            !self.store.is_held(),
            "actual store claims must release before inherited completion"
        );
        self.dropped.store(true, Ordering::Release);
    }
}
#[tokio::test]
async fn actual_submitted_work_retains_handoff_resources_after_caller_cancellation() {
    let fixture = Fixture::new().await;
    LORE_CONTEXT
        .scope(fixture.execution.clone(), async {
            let repository = fixture.load(RepositoryAccess::ReadWrite).await;
            let client = write_token::write_mutex_for_path(fixture.registration.target().path());
            let store = repository
                .working_resources
                .as_ref()
                .unwrap()
                .immutable
                .store_lock()
                .unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let parent = IoLease::new(Arc::new(Completion {
                client: client.clone(),
                store: store.clone(),
                dropped: dropped.clone(),
            }));
            let handoff = parent
                .sync_scope(|| repository.try_working_handoff())
                .unwrap();
            drop(parent);
            let weak = Arc::downgrade(&handoff);
            let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
            // Private access exercises the same propagated IO owner used by strict
            // reads, with an actual producer whose completion can be held deterministically.
            let scope = IoLease::new(handoff.clone());
            let stream = scope
                .scope(
                    handoff
                        .anchors
                        .read_mutable_store()
                        .list(Partition::default(), KeyType::BranchMetadata),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), pause.reached.notified())
                .await
                .unwrap();
            drop(stream.channel());
            drop(repository);
            drop(handoff);
            drop(scope);
            assert!(weak.upgrade().is_some());
            assert!(client.try_lock().is_err());
            assert!(store.is_held());
            assert!(!dropped.load(Ordering::Acquire));
            pause.resume.notify_one();
            until(|| dropped.load(Ordering::Acquire)).await;
            assert!(weak.upgrade().is_none());
            assert!(client.try_lock().is_ok());
            assert!(!store.is_held());
        })
        .await;
}

mod rebase;
