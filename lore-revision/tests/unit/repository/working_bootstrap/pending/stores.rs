// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Actual store jobs retain the pending owner after their caller disappears.
#![allow(clippy::disallowed_methods)]

mod bound;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::repository::working_bootstrap::Selection;
use lore_revision::repository::working_bootstrap::StoreInitializer;
use lore_revision::repository::working_bootstrap::StoreMode;

use super::*;
use crate::fs::filesystem_provider::test_execution_with;

struct StoreParent {
    registration: Option<Arc<Registration>>,
    mutex: Arc<tokio::sync::Mutex<()>>,
    done: std::sync::mpsc::Sender<bool>,
}
impl Drop for StoreParent {
    fn drop(&mut self) {
        let registration = self.registration.take().unwrap();
        let safe = matches!(
            registration.bootstrap_location(),
            Err(lore_revision::fs::provider_registry::Error::Retired)
        ) && self.mutex.try_lock().is_ok();
        // The next reservation may reuse these paths once completion arrives.
        drop(registration);
        let _closed = self.done.send(safe);
    }
}
struct Caller(std::sync::mpsc::Sender<()>);
impl Drop for Caller {
    fn drop(&mut self) {
        let _closed = self.0.send(());
    }
}
fn selection() -> Selection {
    Selection {
        branch: [62; 16].into(),
        branch_name: "main".into(),
        revision: Hash::default(),
    }
}
fn execution() -> Arc<lore_revision::interface::ExecutionContext> {
    test_execution_with(LoreGlobalArgs {
        offline: 1,
        ..Default::default()
    })
}
async fn pending_metadata(
    fixture: &Fixture,
    mode: Mode,
) -> lore_revision::repository::working_bootstrap::PendingMetadata {
    MetadataInitializer::new(1)
        .unwrap()
        .prepare_pending(fixture.registration.clone(), fixture.token().await, mode)
        .await
        .unwrap()
}
fn assert_blocked(fixture: &Fixture, mutex: &tokio::sync::Mutex<()>) {
    assert!(mutex.try_lock().is_err());
    let called = Arc::new(AtomicBool::new(false));
    assert!(matches!(
        fixture.registration.publish(callback(called.clone())),
        Err(lore_revision::fs::provider_registry::Error::AdmissionBusy)
    ));
    assert!(!called.load(Ordering::Acquire));
}
fn reregister(fixture: Fixture, instance: InstanceId) -> Fixture {
    let Fixture {
        _root,
        target,
        metadata,
        registry,
        registration,
    } = fixture;
    drop(registration);
    let registration = Arc::new(registry.reserve(&target, instance).unwrap());
    registration
        .reserve_metadata(&metadata, [51; 16].into())
        .unwrap();
    Fixture {
        _root,
        target,
        metadata,
        registry,
        registration,
    }
}

#[tokio::test]
async fn pending_store_create_and_reuse_keep_exclusion_until_terminal_context_drop() {
    LORE_CONTEXT
        .scope(execution(), async {
            let mut fixture = Fixture::new();
            std::fs::write(fixture.target.join("untouched"), b"source bytes").unwrap();
            let instance = fixture
                .registration
                .bootstrap_location()
                .unwrap()
                .instance();
            for fresh in [true, false] {
                let token = fixture.token().await;
                let mutex = client_mutex(&token);
                let (done, completed) = std::sync::mpsc::channel();
                let parent = Arc::new(StoreParent {
                    registration: Some(fixture.registration.clone()),
                    mutex: mutex.clone(),
                    done,
                });
                let weak = Arc::downgrade(&parent);
                let lease = IoLease::new(parent);
                let metadata = lease
                    .scope(MetadataInitializer::new(1).unwrap().prepare_pending(
                        fixture.registration.clone(),
                        token,
                        if fresh {
                            Mode::CreateNew(config())
                        } else {
                            Mode::Reuse
                        },
                    ))
                    .await
                    .unwrap();
                drop(lease);
                let initializer = StoreInitializer::new(1).unwrap();
                let context = tokio::time::timeout(
                    Duration::from_secs(20),
                    initializer.prepare_pending(
                        metadata,
                        if fresh {
                            StoreMode::CreateNew(selection())
                        } else {
                            StoreMode::Reuse
                        },
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(context.selection(), &selection());
                assert_eq!(context.location().instance(), instance);
                assert_eq!(context.location().target().path(), fixture.target);
                assert_blocked(&fixture, &mutex);
                assert!(weak.upgrade().is_some());
                assert!(completed.try_recv().is_err());
                assert!(fixture.metadata.join("immutable").is_dir());
                assert!(fixture.metadata.join("mutable").is_dir());
                assert_eq!(
                    std::fs::read(fixture.target.join("untouched")).unwrap(),
                    b"source bytes"
                );
                drop(context);
                assert!(completed.recv_timeout(Duration::from_secs(10)).unwrap());
                assert!(weak.upgrade().is_none());
                assert!(mutex.try_lock().is_ok());
                if fresh {
                    fixture = reregister(fixture, instance);
                }
            }
        })
        .await;
}

#[test]
fn cancelled_running_pending_store_job_retains_both_parents_and_client_after_runtime_drop() {
    cancelled_store_job(false);
}
fn cancelled_store_job(bound: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = Fixture::new();
    let token = runtime.block_on(fixture.token());
    let mutex = client_mutex(&token);
    let (done, completed) = std::sync::mpsc::channel();
    let parent = Arc::new(StoreParent {
        registration: Some(fixture.registration.clone()),
        mutex: mutex.clone(),
        done,
    });
    let weak = Arc::downgrade(&parent);
    let lease = IoLease::new(parent);
    let metadata = runtime
        .block_on(
            lease.scope(MetadataInitializer::new(1).unwrap().prepare_pending(
                fixture.registration.clone(),
                token,
                Mode::CreateNew(config()),
            )),
        )
        .unwrap();
    drop(lease);
    let expected = if bound {
        let root = fixture._root.join("backing");
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let location = fixture.registration.bootstrap_location().unwrap();
        lore_revision::repository::working_bootstrap::BackingBinding::write_new(
            location.metadata().path(),
            location.repository(),
            location.instance(),
            &root,
        )
        .unwrap();
        Some(root)
    } else {
        None
    };
    let initializer = StoreInitializer::new(1).unwrap();
    let (entered, entering) = std::sync::mpsc::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    let resume = Resume(Some(resume));
    initializer.pause_metadata_recheck(move || {
        entered.send(()).unwrap();
        resuming.recv().unwrap();
    });
    let (caller_done, caller_completed) = std::sync::mpsc::channel();
    let caller = Arc::new(Caller(caller_done));
    let caller_weak = Arc::downgrade(&caller);
    let caller = IoLease::new(caller);
    let command = runtime.spawn(LORE_CONTEXT.scope(execution(), async move {
        caller
            .scope(async move {
                match expected {
                    Some(root) => {
                        initializer
                            .prepare_pending_bound(
                                metadata,
                                StoreMode::CreateNew(selection()),
                                root,
                            )
                            .await
                    }
                    None => {
                        initializer
                            .prepare_pending(metadata, StoreMode::CreateNew(selection()))
                            .await
                    }
                }
            })
            .await
    }));
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match entering.try_recv() {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("actual store job lost: {error}"),
                }
            }
        })
        .await
        .unwrap();
    });
    command.abort();
    runtime.block_on(async {
        assert!(matches!(command.await, Err(error) if error.is_cancelled()));
    });
    runtime.shutdown_timeout(Duration::ZERO);
    assert_blocked(&fixture, &mutex);
    assert!(weak.upgrade().is_some());
    assert!(caller_weak.upgrade().is_some());
    assert!(caller_completed.try_recv().is_err());
    assert!(completed.try_recv().is_err());
    assert!(!fixture.metadata.join("immutable").exists());
    drop(resume);
    assert!(completed.recv_timeout(Duration::from_secs(10)).unwrap());
    caller_completed
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert!(caller_weak.upgrade().is_none());
    assert!(
        !fixture
            .metadata
            .join(".working-store-initializing")
            .exists()
    );
}

#[tokio::test]
async fn pending_store_validation_error_retires_without_creating_stores_or_repairing_metadata() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new();
            let metadata = pending_metadata(&fixture, Mode::CreateNew(config())).await;
            let path = fixture.metadata.join("config.toml");
            std::fs::write(&path, b"not valid metadata").unwrap();
            let result = StoreInitializer::new(1)
                .unwrap()
                .prepare_pending(metadata, StoreMode::CreateNew(selection()))
                .await;
            assert!(matches!(result, Err(PendingError::Retired(_))));
            assert_eq!(std::fs::read(path).unwrap(), b"not valid metadata");
            assert!(!fixture.metadata.join("immutable").exists());
            assert!(!fixture.metadata.join("mutable").exists());
            assert!(
                !fixture
                    .metadata
                    .join(".working-store-initializing")
                    .exists()
            );
            assert!(matches!(
                fixture.registration.bootstrap_location(),
                Err(lore_revision::fs::provider_registry::Error::Retired)
            ));
            let token = tokio::time::timeout(Duration::from_secs(1), fixture.token())
                .await
                .unwrap();
            drop(token);
        })
        .await;
}
