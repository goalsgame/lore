// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_base::test_util::TempDir;
use lore_storage::Address;
use lore_storage::CallerBuffer;
use lore_storage::Hash;
use lore_storage::KeyType;
use lore_storage::Partition;
use lore_storage::local::immutable_store;
use lore_storage::local::mutable_store::LocalMutableStore;
use lore_storage::local::mutable_store::MutableStoreSettings;
use lore_storage::test_util::PausePoint;

use super::*;
use crate::fs::filesystem_provider::FilesystemProvider;
use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperationImpl;
use crate::fs::provider_registry::Limits;
use crate::fs::provider_registry::ProviderRegistry;
use crate::fs::provider_registry::ProviderResolver;
use crate::fs::provider_registry::Registration;
use crate::instance::InstanceId;
use crate::repository::RepositoryConfig;
use crate::repository::VfsConfig;
use crate::repository::VfsType;

mod probe;
use probe::ProbeImmutable;
use probe::Stats;

struct Provider;
#[async_trait::async_trait]
impl FilesystemProvider for Provider {
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal(
            "store-adapter fixture has no filesystem operations",
        ))
    }
}
fn admitted() -> (
    TempDir,
    TempDir,
    Registration,
    Arc<ProviderRegistry>,
    BoundLoad,
) {
    use std::os::unix::fs::PermissionsExt;
    let target = TempDir::new("lore-command-stores-target-");
    let metadata = TempDir::new("lore-command-stores-metadata-");
    std::fs::set_permissions(metadata.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let registry = ProviderRegistry::new(Limits::default());
    let instance = InstanceId::generate();
    let repository: lore_base::types::RepositoryId = [94; 16].into();
    let config = RepositoryConfig {
        vfs: Some(VfsConfig {
            vfs_type: VfsType::ExperimentalWorking,
        }),
        ..Default::default()
    };
    for (name, bytes) in [
        (crate::repository::ID, repository.data().to_vec()),
        (crate::repository::INSTANCE, instance.data().to_vec()),
        (
            crate::repository::CONFIG,
            toml::to_string(&config).unwrap().into_bytes(),
        ),
    ] {
        let path = metadata.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let owner = registry.reserve(target.path(), instance).unwrap();
    owner.reserve_metadata(metadata.path(), repository).unwrap();
    let location = owner.bootstrap_location().unwrap();
    owner.publish(Arc::new(Provider)).unwrap();
    let (_provider, load) = registry.resolve_bootstrap(&location).unwrap().into_parts();
    (target, metadata, owner, registry, load)
}
async fn raw_stores() -> (TempDir, Arc<dyn ImmutableStore>, Arc<LocalMutableStore>) {
    let directory = TempDir::new("lore-command-stores-data-");
    let immutable = immutable_store::create(
        Some(directory.path()),
        immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        immutable_store::ImmutableStoreSettings::default(),
    )
    .await
    .unwrap();
    let mutable = Arc::new(
        LocalMutableStore::new(
            Some(directory.path()),
            MutableStoreSettings {
                flush_delay_seconds: 0,
                ..Default::default()
            },
            immutable.clone(),
        )
        .await
        .unwrap(),
    );
    (directory, immutable, mutable)
}
#[tokio::test]
async fn specialized_methods_and_policy_overrides_reach_the_inner_store() {
    let (_target, _metadata, _owner, _registry, load) = admitted();
    let (_data, raw, mutable) = raw_stores().await;
    let stats = Arc::new(Stats::default());
    let expected_lock = raw.store_lock().unwrap();
    let expected_scope = (
        raw.isolates_partitions(),
        raw.read_scope(),
        raw.query_scope(),
        raw.max_query_batch(),
    );
    let (immutable, _mutable) = wrap(
        Arc::new(ProbeImmutable {
            inner: raw,
            stats: stats.clone(),
        }),
        mutable,
        Arc::new(load),
    );
    assert_eq!(
        (
            immutable.isolates_partitions(),
            immutable.read_scope(),
            immutable.query_scope(),
            immutable.max_query_batch()
        ),
        expected_scope
    );
    assert!(Arc::ptr_eq(
        &immutable.store_lock().unwrap(),
        &expected_lock
    ));
    let hold = immutable.clone().hold_for_task().await.unwrap();
    assert!(immutable.try_join_claim().is_some());
    drop(hold);
    let mut bytes = [0; 8];
    // SAFETY: the stack buffer lives through this awaited read and its buffer
    // handle is dropped before the stack memory can be reused.
    let mut destination = unsafe { CallerBuffer::new(bytes.as_mut_ptr(), bytes.len()) };
    assert!(
        immutable
            .get_into(Partition::default(), Address::default(), &mut destination)
            .await
            .is_err()
    );
    let calls = stats.calls.lock();
    assert!(calls.contains(&"isolates_partitions"));
    assert!(calls.contains(&"read_scope"));
    assert!(calls.contains(&"query_scope"));
    assert!(calls.contains(&"hold_for_task"));
    assert!(calls.contains(&"try_join_claim"));
    assert!(calls.contains(&"get_into"));
    assert!(!calls.contains(&"hold_for_command"));
    assert!(!calls.contains(&"get"));
    assert!(
        stats.dropped_under_io.load(Ordering::Acquire),
        "last delegate resources drop inside the operation scope"
    );
}
#[tokio::test]
async fn retired_admission_never_reaches_inner_store_operations() {
    let (_target, _metadata, owner, _registry, load) = admitted();
    let (_data, raw, mutable) = raw_stores().await;
    let stats = Arc::new(Stats::default());
    let raw_mutable = mutable.clone();
    let (immutable, mutable) = wrap(
        Arc::new(ProbeImmutable {
            inner: raw,
            stats: stats.clone(),
        }),
        mutable,
        Arc::new(load),
    );
    owner.retire().unwrap();
    assert!(immutable.clone().hold_for_task().await.is_err());
    assert!(immutable.clone().verify(false).await.is_err());
    assert!(immutable.try_join_claim().is_none());
    assert!(!immutable.clone().is_available(Duration::from_secs(1)).await);
    assert_eq!(immutable.clone().fragment_count().await, None);
    immutable.clone().stop_gc(false).await;
    assert!(stats.calls.lock().is_empty());
    assert!(
        mutable
            .store(
                Partition::default(),
                Hash::from_u64(2),
                Hash::from_u64(3),
                KeyType::BranchMetadata
            )
            .await
            .is_err()
    );
    assert!(
        raw_mutable
            .load(
                Partition::default(),
                Hash::from_u64(2),
                KeyType::BranchMetadata
            )
            .await
            .is_err()
    );
}
struct Parent;
#[tokio::test]
async fn actual_list_producers_retain_facade_work_after_the_call_and_receiver_drop() {
    let (_target, _metadata, _owner, _registry, load) = admitted();
    let (_data, immutable, mutable) = raw_stores().await;
    mutable
        .clone()
        .store(
            Partition::default(),
            Hash::from_u64(7),
            Hash::from_u64(8),
            KeyType::BranchMetadata,
        )
        .await
        .unwrap();
    mutable.clone().flush(true).await.unwrap();
    let (immutable, mutable) = wrap(immutable, mutable, Arc::new(load));
    let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
    let parent = Arc::new(Parent);
    let weak = Arc::downgrade(&parent);
    let scope = IoLease::new(parent);
    let stream = scope
        .scope(mutable.list(Partition::default(), KeyType::BranchMetadata))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
        .await
        .unwrap();
    drop(scope);
    drop(immutable);
    drop(stream.channel());
    assert!(weak.upgrade().is_some());
    pause.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn working_command_flush_waits_for_admission_release_and_survives_cancellation() {
    for cancelled in [false, true] {
        let (_target, _metadata, _owner, registry, load) = admitted();
        let location = load.location().clone();
        let (_data, raw, mutable) = raw_stores().await;
        let pause = Arc::new(probe::FlushPause::default());
        let stats = Arc::new(Stats {
            flush_pause: Some(pause.clone()),
            ..Default::default()
        });
        let load = Arc::new(load);
        let (immutable, mutable) = wrap(
            Arc::new(ProbeImmutable {
                inner: raw,
                stats: stats.clone(),
            }),
            mutable,
            load.clone(),
        );
        let final_store = Arc::downgrade(&immutable);
        let mut repository = crate::repository::RepositoryContext::new_with_state(
            None,
            immutable,
            mutable,
            location.repository(),
            location.instance(),
            crate::repository::RemoteState::Offline,
            Arc::default(),
            None,
        );
        repository.working_admission = Some(load);
        let mut flush = Box::pin(repository.post_command_flush(true));
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = pause.reached.notified() => {},
                () = &mut flush => panic!("command flush completed before its store operation"),
            }
        })
        .await
        .unwrap();
        let (_, next) = registry.resolve_bootstrap(&location).unwrap().into_parts();
        assert!(matches!(
            next.exclude_command(),
            Err(crate::fs::provider_registry::Error::AdmissionBusy)
        ));
        if cancelled {
            drop(flush);
            drop(repository);
            assert!(final_store.upgrade().is_some());
            assert!(matches!(
                next.exclude_command(),
                Err(crate::fs::provider_registry::Error::AdmissionBusy)
            ));
            pause.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    match next.exclude_command() {
                        Ok(exclusive) => break exclusive,
                        Err(crate::fs::provider_registry::Error::AdmissionBusy) => {
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("flush admission did not reopen: {error}"),
                    }
                }
            })
            .await
            .unwrap();
        } else {
            pause.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(10), flush)
                .await
                .unwrap();
            drop(repository);
            assert!(final_store.upgrade().is_none());
        }
        next.exclude_command().unwrap();
        assert!(stats.calls.lock().contains(&"flush"));
    }
}
