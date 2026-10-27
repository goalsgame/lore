// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Compatibility fixtures acquire real ordinary recovery after prepared stores.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore::working_mount::Error;
use lore::working_mount::Options;
use lore::working_mount::OwnerLimits;
use lore::working_mount::PreparationError;
use lore::working_mount::StartupInput;
use lore::working_mount::Supervisor;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_vfs::core::BackendFuture;
use lore_vfs::core::NodeKey;
use lore_vfs::overlay::Access;
use lore_vfs::overlay::AccessGate;
use lore_vfs::recovery::Recovery;
use lore_vfs::working_storage::WorkingStorage;

fn limits() -> lore_vfs::recovery::Limits {
    OwnerLimits::default().recovery
}
struct Pause {
    paused: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl AccessGate for Pause {
    fn authorize<'a>(&'a self, _key: &'a NodeKey, _access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let resumed = self.resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if self.paused.load(Ordering::Acquire) {
                self.entered.notify_one();
                resumed.await;
            }
            Ok(())
        })
    }
}
struct Release(Arc<Pause>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.paused.store(false, Ordering::Release);
        self.0.resume.notify_waiters();
    }
}

#[tokio::test]
async fn admitted_recovery_remains_exclusive_through_setup_and_model_construction() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let root = TempDir::new("working-admission-transfer-");
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Default::default()).unwrap();
            let input = super::working_mount::input(&root, &registry).await;
            let storage = input.storage.clone();
            let path = storage.recovery_directory().to_path_buf();
            assert!(matches!(
                Recovery::open_existing(&path, limits()),
                Err(lore_vfs::recovery::Error::Busy)
            ));

            let gate = Arc::new(Pause {
                paused: AtomicBool::new(true),
                entered: Default::default(),
                resume: Default::default(),
            });
            let release = Release(gate.clone());
            let mut setup = Box::pin(supervisor.prepare_instance(
                input,
                gate.clone(),
                Default::default(),
            ));
            tokio::select! {
                _result = &mut setup => panic!("setup bypassed policy pause"),
                _ = gate.entered.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(20)) => panic!("setup did not reach policy"),
            }
            assert!(matches!(
                Recovery::open_existing(&path, limits()),
                Err(lore_vfs::recovery::Error::Busy)
            ));

            drop(release);
            let prepared = tokio::time::timeout(Duration::from_secs(20), setup)
                .await.unwrap().unwrap();
            let handle = prepared.handle();
            assert!(matches!(
                Recovery::open_existing(&path, limits()),
                Err(lore_vfs::recovery::Error::Busy)
            ));
            drop(prepared);
            supervisor.retire_unstarted(&handle).await.unwrap();
            Recovery::open_existing(&path, limits()).unwrap();
        })
        .await;
}

#[tokio::test]
async fn failed_take_is_sticky_and_limit_mismatch_precedes_cache_and_session() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let root = TempDir::new("working-admission-sticky-");
            let registry = ProviderRegistry::new(Default::default());
            let input = super::working_mount::input(&root, &registry).await;
            let wrong = lore_vfs::recovery::Limits {
                max_checkpoint_bytes: 1024,
            };
            assert!(matches!(
                input.test_claim_recovery(wrong),
                Err(Error::InvalidLimits)
            ));
            assert!(matches!(
                input.test_claim_recovery(limits()),
                Err(Error::Closed)
            ));
            let file = input.storage.recovery_directory().join("owner.lock");
            let before = std::fs::metadata(&file).unwrap().modified().unwrap();
            let supervisor = Supervisor::new(registry.clone(), Default::default()).unwrap();
            let storage = input.storage.clone();
            let cache = root.join("must-not-create-cache");
            let error = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(super::working_mount::ExplicitEdits),
                    Default::default(),
                    Options {
                        digest_cache: Some(cache.clone()),
                    },
                )
                .await
                .err()
                .unwrap();
            let PreparationError::Retiring {
                handle,
                error: Error::Closed,
            } = error
            else {
                panic!("consumed recovery reopened")
            };
            assert!(!cache.exists());
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            assert_eq!(std::fs::metadata(file).unwrap().modified().unwrap(), before);
            supervisor.retire_unstarted(&handle).await.unwrap();
            let root = TempDir::new("working-admission-limit-");
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Default::default()).unwrap();
            let input = super::working_mount::input(&root, &registry).await;
            let storage = input.storage.clone();
            let cache = root.join("cache");
            let error = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(super::working_mount::ExplicitEdits),
                    OwnerLimits {
                        recovery: wrong,
                        ..Default::default()
                    },
                    Options {
                        digest_cache: Some(cache.clone()),
                    },
                )
                .await
                .err()
                .unwrap();
            let PreparationError::Retiring {
                handle,
                error: Error::InvalidLimits,
            } = error
            else {
                panic!("recovery limit mismatch accepted")
            };
            assert!(!cache.exists());
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            supervisor.retire_unstarted(&handle).await.unwrap();
        })
        .await;
}
fn damage(path: &std::path::Path, kind: u8) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let file = path.join(match kind {
        0 => "checkpoint",
        1 => "pending",
        _ => "transition",
    });
    std::fs::write(&file, b"retained invalid facts").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    file
}
#[tokio::test]
async fn corruption_and_unfinished_facts_are_checked_at_admission_and_before_setup_io() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            for after in [false, true] {
                for kind in 0..3 {
                    let root = TempDir::new("working-admission-facts-");
                    let registry = ProviderRegistry::new(Default::default());
                    let (prepared, storage) =
                        super::working_mount::input_parts(&root, &registry).await;
                    let input = if after {
                        Some(
                            StartupInput::test_admit_prepared(prepared, storage.clone(), limits())
                                .unwrap(),
                        )
                    } else {
                        let file = damage(storage.recovery_directory(), kind);
                        assert!(
                            StartupInput::test_admit_prepared(prepared, storage.clone(), limits())
                                .is_err()
                        );
                        assert_eq!(std::fs::read(file).unwrap(), b"retained invalid facts");
                        None
                    };
                    if let Some(input) = input {
                        let file = damage(storage.recovery_directory(), kind);
                        let cache = root.join("cache");
                        let supervisor =
                            Supervisor::new(registry.clone(), Default::default()).unwrap();
                        let error = supervisor
                            .prepare_instance_with_options(
                                input,
                                Arc::new(super::working_mount::ExplicitEdits),
                                Default::default(),
                                Options {
                                    digest_cache: Some(cache.clone()),
                                },
                            )
                            .await
                            .err()
                            .unwrap();
                        let PreparationError::Retiring { handle, .. } = error else {
                            panic!("admitted invalid input")
                        };
                        assert!(!cache.exists());
                        assert_eq!(std::fs::read(file).unwrap(), b"retained invalid facts");
                        supervisor.retire_unstarted(&handle).await.unwrap();
                    }
                    assert_eq!(storage.retained_sessions().unwrap(), 0);
                }
            }
        })
        .await;
}
#[tokio::test]
async fn same_ids_do_not_adopt_a_substituted_storage_owner() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let root = TempDir::new("working-admission-storage-");
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Default::default()).unwrap();
            let mut input = super::working_mount::input(&root, &registry).await;
            let original = input.storage.clone();
            let other = WorkingStorage::create_new(
                &root.join("other"),
                input.prepared.location.target().path(),
                original.repository(),
                original.instance(),
                Default::default(),
            )
            .unwrap();
            input.storage = other.clone();
            let cache = root.join("cache");
            let error = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(super::working_mount::ExplicitEdits),
                    Default::default(),
                    Options {
                        digest_cache: Some(cache.clone()),
                    },
                )
                .await
                .err()
                .unwrap();
            let PreparationError::Retiring {
                handle,
                error: Error::InvalidBinding,
            } = error
            else {
                panic!("foreign storage adopted")
            };
            assert!(!cache.exists());
            assert_eq!(original.retained_sessions().unwrap(), 0);
            assert_eq!(other.retained_sessions().unwrap(), 0);
            supervisor.retire_unstarted(&handle).await.unwrap();
        })
        .await;
}
#[tokio::test]
async fn a_structurally_valid_foreign_original_root_is_not_admitted() {
    LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            use lore_vfs::revision::RevisionBackend;
            let root = TempDir::new("working-admission-original-");
            let registry = ProviderRegistry::new(Default::default());
            let (prepared, storage) = super::working_mount::input_parts(&root, &registry).await;
            let foreign = prepared.repository.to_link_context([218; 16].into()).await;
            let backend = Arc::new(
                RevisionBackend::new(foreign, Default::default())
                    .await
                    .unwrap(),
            );
            let base = backend.root();
            let namespace = lore_vfs::namespace::Namespace::new(
                backend,
                base,
                Arc::new(super::working_mount::ExplicitEdits),
                Default::default(),
            )
            .unwrap();
            let snapshot = namespace.snapshot(0).await.unwrap();
            let mut recovery = Recovery::open(storage.recovery_directory(), limits()).unwrap();
            recovery.publish(&snapshot).unwrap();
            drop(recovery);
            let bytes = std::fs::read(storage.recovery_directory().join("checkpoint")).unwrap();
            assert!(matches!(
                StartupInput::test_admit_prepared(prepared, storage.clone(), limits()),
                Err(Error::InvalidBinding)
            ));
            assert_eq!(
                std::fs::read(storage.recovery_directory().join("checkpoint")).unwrap(),
                bytes
            );
            assert_eq!(storage.retained_sessions().unwrap(), 0);
        })
        .await;
}
