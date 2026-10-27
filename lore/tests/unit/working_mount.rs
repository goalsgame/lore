// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Private fixtures exercise ownership through actual metadata and backing files.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;
use std::time::Duration;

use lore::working_mount::Error;
use lore::working_mount::Limits;
use lore::working_mount::Phase;
use lore::working_mount::StartupInput;
use lore::working_mount::Supervisor;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::ProviderResolver;
use lore_revision::instance::InstanceId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::VfsConfig;
use lore_revision::repository::VfsType;
use lore_revision::repository::working_bootstrap::MetadataInitializer;
use lore_revision::repository::working_bootstrap::Mode;
use lore_revision::repository::working_bootstrap::Selection;
use lore_revision::repository::working_bootstrap::StoreInitializer;
use lore_revision::repository::working_bootstrap::StoreMode;
use lore_vfs::working_storage::WorkingStorage;

pub(super) fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        },
        lore_revision::relay::EventDispatcher::no_dispatch(),
        "supervisor-user".into(),
    ))
}
pub(super) async fn input(
    root: &std::path::Path,
    registry: &Arc<ProviderRegistry>,
) -> StartupInput {
    input_with_recovery_limits(
        root,
        registry,
        lore::working_mount::OwnerLimits::default().recovery,
    )
    .await
}
pub(super) async fn input_with_recovery_limits(
    root: &std::path::Path,
    registry: &Arc<ProviderRegistry>,
    limits: lore_vfs::recovery::Limits,
) -> StartupInput {
    let (prepared, storage) = input_parts(root, registry).await;
    StartupInput::test_admit_prepared(prepared, storage, limits).unwrap()
}
pub(super) async fn input_parts(
    root: &std::path::Path,
    registry: &Arc<ProviderRegistry>,
) -> (
    lore_revision::repository::working_bootstrap::PreparedWorkingContext,
    Arc<WorkingStorage>,
) {
    use std::os::unix::fs::PermissionsExt;
    let target = root.join("mount");
    let metadata = root.join("metadata");
    std::fs::create_dir(&target).unwrap();
    std::fs::create_dir(&metadata).unwrap();
    std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o700)).unwrap();
    let instance = InstanceId::generate();
    let registration = Arc::new(registry.reserve(&target, instance).unwrap());
    registration
        .reserve_metadata(&metadata, [113; 16].into())
        .unwrap();
    let token = RepositoryWriteToken::acquire(registration.target().path()).await;
    let config = RepositoryConfig {
        vfs: Some(VfsConfig {
            vfs_type: VfsType::ExperimentalWorking,
        }),
        ..Default::default()
    };
    let metadata = MetadataInitializer::new(1)
        .unwrap()
        .prepare(registration, token, Mode::CreateNew(config))
        .await
        .unwrap();
    let prepared = StoreInitializer::new(1)
        .unwrap()
        .prepare(
            metadata,
            StoreMode::CreateNew(Selection {
                branch: [114; 16].into(),
                branch_name: "main".into(),
                revision: Default::default(),
            }),
        )
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(
        &prepared.execution,
        &lore_revision::runtime::execution_context()
    ));
    assert_eq!(prepared.execution.user_id().await, "supervisor-user");
    assert!(prepared.execution.caller_state().is_none());
    let storage = WorkingStorage::open(
        &root.join("backing"),
        &target,
        [113; 16].into(),
        instance,
        Default::default(),
    )
    .unwrap();
    (prepared, storage)
}

#[tokio::test]
async fn cancelled_startup_retains_slot_and_identity_until_explicit_cleanup() {
    LORE_CONTEXT
        .scope(execution(), async {
            let root = TempDir::new("lore-supervisor-cancel-");
            let registry = ProviderRegistry::new(Default::default());
            let supervisor =
                Supervisor::new(registry.clone(), Limits { max_instances: 1 }).unwrap();
            let startup = supervisor
                .reserve_startup(input(&root, &registry).await)
                .unwrap();
            let handle = startup.handle();
            assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Starting);
            assert!(matches!(
                supervisor.retire_unstarted(&handle).await,
                Err(Error::Busy)
            ));
            supervisor.cancel_startup(&handle).unwrap();
            assert!(matches!(startup.check_live(), Err(Error::Closed)));
            assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Retiring);
            assert!(matches!(
                registry.reserve(&root.join("mount"), InstanceId::generate()),
                Err(lore_revision::fs::provider_registry::Error::Overlap)
            ));
            drop(startup);
            tokio::time::timeout(
                Duration::from_secs(10),
                supervisor.retire_unstarted(&handle),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(matches!(supervisor.status(&handle), Err(Error::Missing)));
            let _replacement = registry
                .reserve(&root.join("mount"), InstanceId::generate())
                .unwrap();
        })
        .await;
}

#[tokio::test]
async fn concurrent_admission_is_bounded_retiring_counts_and_handles_do_not_alias() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let first = TempDir::new("lore-supervisor-first-");
        let second = TempDir::new("lore-supervisor-second-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits { max_instances: 1 }).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut threads = Vec::new();
        for prepared in [
            input(&first, &registry).await,
            input(&second, &registry).await,
        ] {
            let supervisor = supervisor.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                supervisor.reserve_startup(prepared)
            }));
        }
        let mut admitted = None;
        let mut rejected = None;
        for thread in threads {
            match thread.join().unwrap() {
                Ok(startup) => {
                    assert!(admitted.is_none());
                    admitted = Some(startup);
                }
                Err(value) => {
                    assert!(matches!(value.error, Error::Capacity));
                    rejected = Some(*value.input);
                }
            }
        }
        let startup = admitted.unwrap();
        let handle = startup.handle();
        drop(startup);
        let failure = match supervisor.reserve_startup(rejected.take().unwrap()) {
            Err(error) => error,
            Ok(_unexpected) => panic!("retiring record still consumes capacity"),
        };
        assert!(matches!(failure.error, Error::Capacity));
        let other = Supervisor::new(registry.clone(), Default::default()).unwrap();
        assert!(matches!(other.status(&handle), Err(Error::WrongOwner)));
        tokio::time::timeout(
            Duration::from_secs(10),
            supervisor.retire_unstarted(&handle),
        )
        .await
        .unwrap()
        .unwrap();
        let next = supervisor.reserve_startup(*failure.input).unwrap();
        let next_handle = next.handle();
        assert!(next_handle.id() > handle.id());
        assert!(matches!(supervisor.status(&handle), Err(Error::Missing)));
        drop(next);
        supervisor.retire_unstarted(&next_handle).await.unwrap();
    }))
    .await;
}

#[tokio::test]
async fn foreign_registry_rejection_returns_ownership_without_fallback() {
    LORE_CONTEXT
        .scope(execution(), async {
            let root = TempDir::new("lore-supervisor-foreign-");
            let registry = ProviderRegistry::new(Default::default());
            let wrong = Supervisor::new(
                ProviderRegistry::new(Default::default()),
                Default::default(),
            )
            .unwrap();
            let rejected = match wrong.reserve_startup(input(&root, &registry).await) {
                Err(error) => error,
                Ok(_unexpected) => panic!("foreign owner admitted"),
            };
            assert!(matches!(rejected.error, Error::InvalidBinding));
            assert!(matches!(
                registry.resolve_location(&rejected.input.prepared.location),
                Err(lore_revision::fs::provider_registry::Error::NotReady)
            ));
            let correct = Supervisor::new(registry, Default::default()).unwrap();
            let startup = correct.reserve_startup(*rejected.input).unwrap();
            let handle = startup.handle();
            drop(startup);
            correct.retire_unstarted(&handle).await.unwrap();
        })
        .await;
}

#[test]
fn dropping_supervisor_retains_registration_through_cancelled_actual_backing_io() {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    let root = TempDir::new("lore-supervisor-drop-");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let registry = ProviderRegistry::new(Default::default());
    let prepared = runtime.block_on(LORE_CONTEXT.scope(execution(), input(&root, &registry)));
    let repository = prepared.storage.repository();
    let instance = prepared.storage.instance();
    let target = prepared.prepared.location.target().path().to_path_buf();
    let backing = prepared.storage.root().to_path_buf();
    let session = prepared.storage.begin_session().unwrap();
    let stores = session.store_directory().to_path_buf();
    let factory = session.factory();
    let store = runtime.block_on(factory.create()).unwrap();
    let supervisor = Supervisor::new(registry.clone(), Default::default()).unwrap();
    let startup = supervisor.reserve_startup(prepared).unwrap();
    let (entered, entering) = std::sync::mpsc::channel();
    let (release, releasing) = std::sync::mpsc::channel();
    runtime.spawn_blocking(move || {
        entered.send(()).unwrap();
        releasing.recv().unwrap();
    });
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let mut write = Box::pin(store.write(0, b"late backing bytes"));
    runtime.block_on(std::future::poll_fn(|_context| {
        assert!(
            write
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        Poll::Ready(())
    }));
    drop(write);
    drop(store);
    drop(factory);
    drop(session);
    drop(startup);
    drop(supervisor);
    runtime.shutdown_timeout(Duration::ZERO);
    assert!(matches!(
        registry.reserve(&target, instance),
        Err(lore_revision::fs::provider_registry::Error::Overlap)
    ));
    assert!(matches!(
        WorkingStorage::open(&backing, &target, repository, instance, Default::default()),
        Err(lore_vfs::working_storage::Error::Busy)
    ));
    release.send(()).unwrap();
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let reopened = observer.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(storage) = WorkingStorage::open(
                    &backing,
                    &target,
                    repository,
                    instance,
                    Default::default(),
                ) {
                    break storage;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    });
    let started = std::time::Instant::now();
    loop {
        if let Ok(registration) = registry.reserve(&target, instance) {
            drop(registration);
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10));
        std::thread::yield_now();
    }
    let file = std::fs::read_dir(stores)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert_eq!(std::fs::read(file).unwrap(), b"late backing bytes");
    drop(reopened);
}

struct ContextGate(Arc<ExecutionContext>);
impl lore_vfs::overlay::AccessGate for ContextGate {
    fn authorize<'a>(
        &'a self,
        _key: &'a lore_vfs::core::NodeKey,
        _access: lore_vfs::overlay::Access,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        assert!(Arc::ptr_eq(
            &self.0,
            &lore_revision::runtime::execution_context()
        ));
        Box::pin(async {
            tokio::task::yield_now().await;
            assert!(Arc::ptr_eq(
                &self.0,
                &lore_revision::runtime::execution_context()
            ));
            Ok(())
        })
    }
}

pub(super) struct ExplicitEdits;
impl lore_vfs::overlay::AccessGate for ExplicitEdits {
    fn authorize<'a>(
        &'a self,
        _key: &'a lore_vfs::core::NodeKey,
        _access: lore_vfs::overlay::Access,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn prepared_owner_retirement_checkpoints_bytes_and_reopens_independently() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        use lore::working_mount::OwnerLimits;
        use lore_vfs::linux_working::OpenFlags;
        let root = TempDir::new("lore-supervisor-prepared-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let instance = input.prepared.location.instance();
        let gate = Arc::new(ContextGate(input.prepared.execution.clone()));
        let prepared = tokio::time::timeout(
            Duration::from_secs(30),
            supervisor.prepare_instance(input, gate, OwnerLimits::default()),
        )
        .await
        .unwrap()
        .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let flags = OpenFlags {
            write: true,
            append: false,
            truncate: false,
        };
        let (file, _) = model.create(1, b"retained", 0o644, flags).await.unwrap();
        assert_eq!(model.write(file, 0, b"retirement bytes").await.unwrap(), 16);
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        tokio::time::timeout(
            Duration::from_secs(30),
            supervisor.retire_unstarted(&handle),
        )
        .await
        .unwrap()
        .unwrap();
        let registration = Arc::new(registry.reserve(&root.join("mount"), instance).unwrap());
        registration
            .reserve_metadata(&root.join("metadata"), [113; 16].into())
            .unwrap();
        let token = RepositoryWriteToken::acquire(registration.target().path()).await;
        let metadata = MetadataInitializer::new(1)
            .unwrap()
            .prepare(registration, token, Mode::Reuse)
            .await
            .unwrap();
        let prepared = StoreInitializer::new(1)
            .unwrap()
            .prepare(metadata, StoreMode::Reuse)
            .await
            .unwrap();
        let storage = WorkingStorage::open(
            &root.join("backing"),
            &root.join("mount"),
            [113; 16].into(),
            instance,
            Default::default(),
        )
        .unwrap();
        let prepared = supervisor
            .prepare_instance(
                StartupInput::test_admit_prepared(
                    prepared,
                    storage,
                    lore::working_mount::OwnerLimits::default().recovery,
                )
                .unwrap(),
                Arc::new(ExplicitEdits),
                OwnerLimits::default(),
            )
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let attrs = model.lookup(1, b"retained").await.unwrap();
        let file = model
            .open(
                attrs.entry.inode,
                OpenFlags {
                    write: false,
                    ..flags
                },
            )
            .await
            .unwrap();
        assert_eq!(model.read(file, 0, 64).await.unwrap(), b"retirement bytes");
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        tokio::time::timeout(
            Duration::from_secs(30),
            supervisor.retire_unstarted(&handle),
        )
        .await
        .unwrap()
        .unwrap();
    }))
    .await;
}

struct BlockAuthorization(Arc<tokio::sync::Notify>);
impl lore_vfs::overlay::AccessGate for BlockAuthorization {
    fn authorize<'a>(
        &'a self,
        _key: &'a lore_vfs::core::NodeKey,
        _access: lore_vfs::overlay::Access,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(async {
            self.0.notify_one();
            futures::future::pending().await
        })
    }
}

#[tokio::test]
async fn cancelled_preparation_fences_publication_and_drains_startup_before_release() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-model-cancel-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let began = Arc::new(tokio::sync::Notify::new());
        let mut prepare = Box::pin(supervisor.prepare_instance(input, Arc::new(BlockAuthorization(began.clone())), Default::default()));
        tokio::select! {
            biased;
            _ = began.notified() => {},
            _result = &mut prepare => panic!("authorization must stay pending"),
            _ = tokio::time::sleep(Duration::from_secs(20)) => panic!("preparation did not authorize"),
        }
        // The cancellation fence runs on future destruction, while the owned
        // preparation thread retains its ticket through runtime teardown.
        drop(prepare);
        let handle = supervisor.instances().unwrap().pop().unwrap().0;
        assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Retiring);
        assert!(registry.reserve(&root.join("mount"), InstanceId::generate()).is_err());
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match supervisor.retire_unstarted(&handle).await {
                    Err(Error::Busy) => tokio::task::yield_now().await,
                    result => break result.unwrap(),
                }
            }
        }).await.unwrap();
        registry.reserve(&root.join("mount"), InstanceId::generate()).unwrap();
    })).await;
}

#[tokio::test]
async fn failed_retirement_keeps_original_error_and_reservation_on_retry() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        use lore::working_mount::OwnerLimits;
        use lore_vfs::linux_working::OpenFlags;
        let root = TempDir::new("lore-supervisor-final-error-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let limits = OwnerLimits {
            recovery: lore_vfs::recovery::Limits {
                max_checkpoint_bytes: 1024,
            },
            ..Default::default()
        };
        let input = input_with_recovery_limits(&root, &registry, limits.recovery).await;
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), limits)
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let (file, _) = model
            .create(
                1,
                b"larger-than-checkpoint",
                0o644,
                OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model.write(file, 0, &[42; 2048]).await.unwrap();
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        let first = tokio::time::timeout(
            Duration::from_secs(20),
            supervisor.retire_unstarted(&handle),
        )
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
        let second = tokio::time::timeout(
            Duration::from_secs(20),
            supervisor.retire_unstarted(&handle),
        )
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
        assert_eq!(first, second);
        assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Retiring);
        assert!(
            registry
                .reserve(&root.join("mount"), InstanceId::generate())
                .is_err()
        );
    }))
    .await;
}

#[tokio::test]
async fn cancelled_runtime_drain_retains_claims_and_retry_finishes_same_retirement() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-runtime-drain-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let prepared = supervisor
            .prepare_instance(
                input(&root, &registry).await,
                Arc::new(ExplicitEdits),
                Default::default(),
            )
            .await
            .unwrap();
        let handle = prepared.handle();
        let runtime = prepared.test_runtime();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        runtime.spawn_blocking(move || {
            let _sent = started_tx.send(());
            release_rx.recv().unwrap();
        });
        tokio::time::timeout(Duration::from_secs(10), started_rx)
            .await
            .unwrap()
            .unwrap();
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                let _sent = self.0.take().unwrap().send(());
            }
        }
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let guard = OnDrop(Some(dropped_tx));
        runtime.spawn(async move {
            let _guard = guard;
            futures::future::pending::<()>().await;
        });
        drop(runtime);
        drop(prepared);
        let mut retirement = Box::pin(supervisor.retire_unstarted(&handle));
        tokio::select! {
            _=dropped_rx=>{},
            result=&mut retirement=>panic!("retirement completed before actual worker: {result:?}"),
            _=tokio::time::sleep(Duration::from_secs(20))=>panic!("runtime shutdown did not begin"),
        }
        drop(retirement);
        assert!(
            registry
                .reserve(&root.join("mount"), InstanceId::generate())
                .is_err()
        );
        release_tx.send(()).unwrap();
        tokio::time::timeout(
            Duration::from_secs(20),
            supervisor.retire_unstarted(&handle),
        )
        .await
        .unwrap()
        .unwrap();
        registry
            .reserve(&root.join("mount"), InstanceId::generate())
            .unwrap();
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[test]
fn mount_table_rejects_moved_replacement_stacked_malformed_and_oversized_views() {
    use std::path::Path;

    use lore::working_mount::test_mount_table_decision as safe;
    let target = Path::new("/owned mount");
    let owned = b"40 1 0:1 / /owned\\040mount rw - fuse.lore source rw\n";
    assert!(safe(owned, target, 40).unwrap());
    let replacement = b"41 1 0:2 / /owned\\040mount rw - fuse.other source rw\n";
    assert!(!safe(replacement, target, 40).unwrap());
    let mut stacked = owned.to_vec();
    stacked.extend_from_slice(replacement);
    assert!(!safe(&stacked, target, 40).unwrap());
    let mut moved = b"40 1 0:1 / /elsewhere rw - fuse.lore source rw\n".to_vec();
    moved.extend_from_slice(replacement);
    assert!(!safe(&moved, target, 40).unwrap());
    assert!(
        !safe(
            b"40 1 0:1 / /elsewhere rw - fuse.lore source rw\n",
            target,
            40
        )
        .unwrap()
    );
    assert!(safe(b"", target, 40).unwrap());
    assert!(
        safe(
            b"40 1 0:1 / /owned\\999mount rw - fuse.lore source rw\n",
            target,
            40
        )
        .is_err()
    );
    assert!(safe(b"40 1 0:1 / /owned rw", target, 40).is_err());
    assert!(safe(&vec![b'x'; 4 * 1024 * 1024 + 1], target, 40).is_err());
}

#[cfg(target_os = "linux")]
fn mount_ids() -> (u32, u32) {
    // SAFETY: getuid/getgid have no argument preconditions.
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_publishes_ready_and_retires_with_durable_bytes() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-mounted-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let location = input.prepared.location.clone();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        assert!(matches!(
            registry.resolve_location(&location),
            Err(lore_revision::fs::provider_registry::Error::NotReady)
        ));
        let (uid, gid) = mount_ids();
        let handle = tokio::time::timeout(Duration::from_secs(20), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Ready);
        assert!(registry.resolve_location(&location).unwrap().holds_state());
        let path = root.join("mount/persisted");
        tokio::time::timeout(
            Duration::from_secs(10),
            tokio::fs::write(&path, b"supervised mount bytes"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            tokio::fs::read(&path).await.unwrap(),
            b"supervised mount bytes"
        );
        supervisor.request_retirement(&handle).unwrap();
        assert!(matches!(
            registry.resolve_location(&location),
            Err(lore_revision::fs::provider_registry::Error::Retired)
        ));
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        assert!(!path.exists());
        assert!(supervisor.instances().unwrap().is_empty());
        let registration = Arc::new(
            registry
                .reserve(&root.join("mount"), location.instance())
                .unwrap(),
        );
        registration
            .reserve_metadata(&root.join("metadata"), location.repository())
            .unwrap();
        let token = RepositoryWriteToken::acquire(registration.target().path()).await;
        let metadata = MetadataInitializer::new(1)
            .unwrap()
            .prepare(registration, token, Mode::Reuse)
            .await
            .unwrap();
        let prepared = StoreInitializer::new(1)
            .unwrap()
            .prepare(metadata, StoreMode::Reuse)
            .await
            .unwrap();
        let storage = WorkingStorage::open(
            &root.join("backing"),
            &root.join("mount"),
            location.repository(),
            location.instance(),
            Default::default(),
        )
        .unwrap();
        let prepared = supervisor
            .prepare_instance(
                StartupInput::test_admit_prepared(
                    prepared,
                    storage,
                    lore::working_mount::OwnerLimits::default().recovery,
                )
                .unwrap(),
                Arc::new(ExplicitEdits),
                Default::default(),
            )
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let entry = model.lookup(1, b"persisted").await.unwrap();
        let file = model
            .open(
                entry.entry.inode,
                lore_vfs::linux_working::OpenFlags {
                    write: false,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            model.read(file, 0, 100).await.unwrap(),
            b"supervised mount bytes"
        );
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_cancel_before_publication_never_resolves_provider() {
    Box::pin(LORE_CONTEXT.scope(execution(),async {
        let root=TempDir::new("lore-supervisor-publish-cancel-");
        let registry=ProviderRegistry::new(Default::default());
        let supervisor=Supervisor::new(registry.clone(),Limits::default()).unwrap();
        let input=input(&root,&registry).await;
        let location=input.prepared.location.clone();
        let prepared=supervisor.prepare_instance(input,Arc::new(ExplicitEdits),Default::default()).await.unwrap();
        let handle=prepared.handle();
        let (entered,release)=prepared.test_pause_publication();
        let (uid,gid)=mount_ids();let mut mounting=Box::pin(prepared.mount(uid,gid));
        tokio::select! {
            result=entered=>result.unwrap(),
            result=&mut mounting=>panic!("mount returned before publication pause: {result:?}"),
            _=tokio::time::sleep(Duration::from_secs(20))=>panic!("mount did not reach publication"),
        }
        supervisor.request_retirement(&handle).unwrap();
        assert!(matches!(registry.resolve_location(&location),Err(lore_revision::fs::provider_registry::Error::Retired)));
        release.send(()).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(20),mounting).await.unwrap().is_err());
        tokio::time::timeout(Duration::from_secs(20),async {
            loop {match supervisor.retire_instance(&handle).await {Err(Error::Busy)=>tokio::task::yield_now().await,result=>break result.unwrap()}}
        }).await.unwrap();
        assert!(supervisor.instances().unwrap().is_empty());
    })).await;
}

pub(super) async fn reuse_input(
    root: &TempDir,
    registry: &Arc<ProviderRegistry>,
    instance: InstanceId,
) -> StartupInput {
    let registration = Arc::new(registry.reserve(&root.join("mount"), instance).unwrap());
    registration
        .reserve_metadata(&root.join("metadata"), [113; 16].into())
        .unwrap();
    let token = RepositoryWriteToken::acquire(registration.target().path()).await;
    let metadata = MetadataInitializer::new(1)
        .unwrap()
        .prepare(registration, token, Mode::Reuse)
        .await
        .unwrap();
    let prepared = StoreInitializer::new(1)
        .unwrap()
        .prepare(metadata, StoreMode::Reuse)
        .await
        .unwrap();
    let storage = WorkingStorage::open(
        &root.join("backing"),
        &root.join("mount"),
        [113; 16].into(),
        instance,
        Default::default(),
    )
    .unwrap();
    StartupInput::test_admit_prepared(
        prepared,
        storage,
        lore::working_mount::OwnerLimits::default().recovery,
    )
    .unwrap()
}
async fn replace_normal_anchor(
    repository: &Arc<lore_revision::repository::RepositoryContext>,
    path: &std::path::Path,
    committed: bool,
) -> lore_base::types::Hash {
    let token = RepositoryWriteToken::acquire(path).await;
    let repository = Arc::new(repository.without_command_locks().with_write_token(token));
    let state = lore_revision::state::State::new();
    state.set_parent_self(Default::default());
    state.set_revision_number(u64::from(committed));
    let revision = state
        .serialize(repository.clone(), repository.try_write_token().unwrap())
        .await
        .unwrap();
    assert!(!revision.is_zero());
    lore_revision::instance::store_current_anchor(&repository, revision)
        .await
        .unwrap();
    repository.flush(true).await.unwrap();
    revision
}

#[tokio::test]
async fn newer_normal_root_anchor_refuses_restore_without_changing_checkpoint() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-anchor-mismatch-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let instance = input.prepared.location.instance();
        let repository = input.prepared.repository.clone();
        let recovery = input.storage.recovery_directory().to_path_buf();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let (file, _) = model
            .create(
                1,
                b"unadopted",
                0o644,
                lore_vfs::linux_working::OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model
            .write(file, 0, b"keep checkpoint bytes")
            .await
            .unwrap();
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        let checkpoint = std::fs::read(recovery.join("checkpoint")).unwrap();
        let advanced = replace_normal_anchor(&repository, &root.join("mount"), true).await;
        drop(repository);
        let input = reuse_input(&root, &registry, instance).await;
        assert_eq!(input.prepared.selection.revision, advanced);
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            supervisor.prepare_instance(input, Arc::new(ExplicitEdits), Default::default()),
        )
        .await
        .unwrap();
        let lore::working_mount::PreparationError::Retiring {
            handle,
            error: Error::AnchorMismatch,
        } = result.err().unwrap()
        else {
            panic!("restore must reject an unadopted root anchor")
        };
        assert_eq!(
            std::fs::read(recovery.join("checkpoint")).unwrap(),
            checkpoint
        );
        assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Retiring);
        assert!(
            registry
                .reserve(&root.join("mount"), InstanceId::generate())
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            std::fs::read(recovery.join("checkpoint")).unwrap(),
            checkpoint
        );
    }))
    .await;
}

#[tokio::test]
async fn staged_underlay_and_selected_anchor_restore_the_same_committed_root() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-staged-root-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let mut input = input(&root, &registry).await;
        let instance = input.prepared.location.instance();
        let staged =
            replace_normal_anchor(&input.prepared.repository, &root.join("mount"), false).await;
        input.prepared.selection.revision = staged;
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let handle = prepared.handle();
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        let input = reuse_input(&root, &registry, instance).await;
        assert_eq!(input.prepared.selection.revision, staged);
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let handle = prepared.handle();
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_normal_stage_commit_restores_receipt_and_unstaged_bytes() {
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    let registry = ProviderRegistry::new(Default::default());
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    let mut execution = execution();
    Arc::get_mut(&mut execution)
        .unwrap()
        .set_provider_resolver(&resolver);
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let root = TempDir::new("lore-supervisor-real-commit-");
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let instance = input.prepared.location.instance();
        let location = input.prepared.location.clone();
        let cache = root.join("verified-digests");
        let prepared = supervisor
            .prepare_instance_with_options(
                input,
                Arc::new(ExplicitEdits),
                Default::default(),
                lore::working_mount::Options {
                    digest_cache: Some(cache.clone()),
                },
            )
            .await
            .unwrap();
        let model = prepared.test_model();
        let (uid, gid) = mount_ids();
        let handle = tokio::time::timeout(Duration::from_secs(20), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(root.join("mount/selected"), b"committed bytes")
            .await
            .unwrap();
        tokio::fs::write(root.join("mount/bystander"), b"not selected")
            .await
            .unwrap();
        let repository = lore_revision::repository::load_and_connect(
            root.join("mount"),
            lore_revision::repository::RepositoryAccess::ReadWrite,
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            &repository.file_system(),
            &registry.resolve_location(&location).unwrap()
        ));
        let token = repository.try_write_token().unwrap();
        lore_revision::file::stage::stage(
            repository.clone(),
            token,
            LoreArray::from_vec(vec![LoreString::from(&root.join("mount/selected"))]),
            lore_revision::stage::StageOptions {
                scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let revision = tokio::time::timeout(
            Duration::from_secs(30),
            lore_revision::commit::commit_boxed(
                repository.clone(),
                token,
                lore_revision::commit::CommitOptions::new("supervised partial commit".into()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!revision.is_zero());
        let selected = model.lookup(1, b"selected").await.unwrap();
        let digest = model
            .digest(selected.entry.inode)
            .await
            .unwrap()
            .expect("durable commit receipt supplies overlay SHA256");
        use lore_vfs::core::Backend;
        let pinned = lore_vfs::revision::RevisionBackend::new(repository.clone(), revision)
            .await
            .unwrap()
            .with_digest_index(lore_vfs::digest_index::DigestIndex::new(&cache));
        let immutable = pinned
            .lookup(&pinned.root().key, b"selected")
            .await
            .unwrap();
        assert_eq!(pinned.digest(&immutable.key).await.unwrap(), Some(digest));
        let no_cache = lore_vfs::revision::RevisionBackend::new(repository.clone(), revision)
            .await
            .unwrap();
        let uncached = no_cache
            .lookup(&no_cache.root().key, b"selected")
            .await
            .unwrap();
        assert_eq!(no_cache.digest(&uncached.key).await.unwrap(), None);
        let bystander = model.lookup(1, b"bystander").await.unwrap();
        assert_eq!(model.digest(bystander.entry.inode).await.unwrap(), None);
        let state = lore_revision::state::State::deserialize(repository.clone(), revision)
            .await
            .unwrap();
        assert!(
            state
                .find_relative_node_link(repository.clone(), 0, "selected")
                .await
                .is_ok()
        );
        assert!(
            state
                .find_relative_node_link(repository.clone(), 0, "bystander")
                .await
                .is_err()
        );
        // Exercise the actual kernel xattr path, which must report the same
        // generation-exact bytes as the bound provider receipt.
        let selected_path =
            std::ffi::CString::new(root.join("mount/selected").as_os_str().as_encoded_bytes())
                .unwrap();
        let name = std::ffi::CString::new(lore_vfs::digest::XATTR_NAME).unwrap();
        let mut actual = [0u8; 32];
        // SAFETY: both strings are NUL terminated; the writable buffer length is exact.
        let size = unsafe {
            libc::getxattr(
                selected_path.as_ptr(),
                name.as_ptr(),
                actual.as_mut_ptr().cast(),
                actual.len(),
            )
        };
        assert_eq!(size, 32);
        assert_eq!(actual, digest);
        drop(state);
        // Optional cache publication may fail after durable commit adoption.
        // Replacing this fixture cache with a file forces that separate failure.
        std::fs::remove_dir_all(&cache).unwrap();
        std::fs::write(&cache, b"not a cache directory").unwrap();
        tokio::fs::write(root.join("mount/selected"), b"second revision")
            .await
            .unwrap();
        assert_eq!(model.digest(selected.entry.inode).await.unwrap(), None);
        lore_revision::file::stage::stage(
            repository.clone(),
            token,
            LoreArray::from_vec(vec![LoreString::from(&root.join("mount/selected"))]),
            lore_revision::stage::StageOptions {
                scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let second_revision = tokio::time::timeout(
            Duration::from_secs(30),
            lore_revision::commit::commit_boxed(
                repository.clone(),
                token,
                lore_revision::commit::CommitOptions::new(
                    "commit despite optional cache failure".into(),
                ),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let second_digest = model
            .digest(selected.entry.inode)
            .await
            .unwrap()
            .expect("cache failure cannot erase durable live receipt");
        assert_ne!(second_digest, digest);
        let digest = second_digest;
        let unavailable_cache =
            lore_vfs::revision::RevisionBackend::new(repository.clone(), second_revision)
                .await
                .unwrap()
                .with_digest_index(lore_vfs::digest_index::DigestIndex::new(&cache));
        let immutable = unavailable_cache
            .lookup(&unavailable_cache.root().key, b"selected")
            .await
            .unwrap();
        assert_eq!(
            unavailable_cache.digest(&immutable.key).await.unwrap(),
            None
        );
        drop(repository);
        drop(model);
        supervisor.request_retirement(&handle).unwrap();
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        let prepared = supervisor
            .prepare_instance(
                reuse_input(&root, &registry, instance).await,
                Arc::new(ExplicitEdits),
                Default::default(),
            )
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let selected = model.lookup(1, b"selected").await.unwrap();
        assert_eq!(
            model.digest(selected.entry.inode).await.unwrap(),
            Some(digest)
        );
        let bystander = model.lookup(1, b"bystander").await.unwrap();
        let file = model
            .open(
                bystander.entry.inode,
                lore_vfs::linux_working::OpenFlags {
                    write: false,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(model.read(file, 0, 100).await.unwrap(), b"not selected");
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
async fn mounted_supervisor_unexpected_kernel_end_withdraws_provider() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-external-end-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let location = input.prepared.location.clone();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let (uid, gid) = mount_ids();
        let handle = tokio::time::timeout(Duration::from_secs(20), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        assert!(registry.resolve_location(&location).is_ok());
        let path = root.join("mount");
        let unmounted = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new("fusermount3")
                .kill_on_drop(true)
                .arg("-u")
                .arg("--")
                .arg(path)
                .status(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(unmounted.success());
        tokio::time::timeout(Duration::from_secs(10), async {
            while supervisor.status(&handle).unwrap().phase != Phase::Retiring {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            registry.resolve_location(&location),
            Err(lore_revision::fs::provider_registry::Error::Retired)
        ));
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        assert!(supervisor.instances().unwrap().is_empty());
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_drop_retires_healthy_owner_despite_another_retained_failure() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let failed = TempDir::new("lore-supervisor-drop-failed-");
        let healthy = TempDir::new("lore-supervisor-drop-healthy-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let bad = supervisor
            .prepare_instance(
                input_with_recovery_limits(
                    &failed,
                    &registry,
                    lore_vfs::recovery::Limits {
                        max_checkpoint_bytes: 1024,
                    },
                )
                .await,
                Arc::new(ExplicitEdits),
                lore::working_mount::OwnerLimits {
                    recovery: lore_vfs::recovery::Limits {
                        max_checkpoint_bytes: 1024,
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let bad_handle = bad.handle();
        let model = bad.test_model();
        let (file, _) = model
            .create(
                1,
                b"oversized",
                0o644,
                lore_vfs::linux_working::OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model.write(file, 0, &[9; 2048]).await.unwrap();
        model.release(file).await.unwrap();
        drop(model);
        drop(bad);
        assert!(
            tokio::time::timeout(
                Duration::from_secs(20),
                supervisor.retire_instance(&bad_handle)
            )
            .await
            .unwrap()
            .is_err()
        );
        let good = input(&healthy, &registry).await;
        let location = good.prepared.location.clone();
        let good = supervisor
            .prepare_instance(good, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let (uid, gid) = mount_ids();
        tokio::time::timeout(Duration::from_secs(20), good.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(healthy.join("mount/retained"), b"healthy final checkpoint")
            .await
            .unwrap();
        drop(supervisor);
        // All records are fenced before Drop schedules independent drain jobs.
        assert!(matches!(
            registry.resolve_location(&location),
            Err(lore_revision::fs::provider_registry::Error::Retired)
        ));
        let replacement = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match registry.reserve(&healthy.join("mount"), InstanceId::generate()) {
                    Ok(registration) => break registration,
                    Err(lore_revision::fs::provider_registry::Error::Overlap) => {
                        tokio::task::yield_now().await;
                    }
                    Err(lore_revision::fs::provider_registry::Error::Io(error))
                        if matches!(error.raw_os_error(), Some(libc::ENODEV | libc::ENOTCONN)) =>
                    {
                        // Request admission closes before the kernel mount disappears.
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected healthy reservation error: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert!(!healthy.join("mount/retained").exists());
        assert!(
            registry
                .reserve(&failed.join("mount"), InstanceId::generate())
                .is_err()
        );
        let storage = WorkingStorage::open(
            &healthy.join("backing"),
            &healthy.join("mount"),
            location.repository(),
            location.instance(),
            Default::default(),
        )
        .unwrap();
        let recovery =
            lore_vfs::recovery::Recovery::open(storage.recovery_directory(), Default::default())
                .unwrap();
        assert!(recovery.load().unwrap().is_some());
        drop(replacement);
    }))
    .await;
}

async fn seed_linked_selection(
    input: &mut StartupInput,
) -> (lore_base::types::RepositoryId, lore_base::types::BranchId) {
    use lore_revision::node::Node;
    use lore_revision::node::NodeFlags;
    let token = RepositoryWriteToken::acquire(input.prepared.location.target().path()).await;
    let repository = Arc::new(
        input
            .prepared
            .repository
            .without_command_locks()
            .with_write_token(token),
    );
    let token = repository.try_write_token().unwrap();
    let child_id = [115; 16].into();
    let child_branch = [116; 16].into();
    let child = repository.to_link_context(child_id).await;
    let child_state = lore_revision::state::State::new();
    let file = child_state
        .node_add(
            child.clone(),
            0,
            Node {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string("content"),
                ..Default::default()
            },
            "content",
        )
        .await
        .unwrap();
    child_state
        .node_mark_staged(
            child.clone(),
            file,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(child_branch).unwrap();
    let child_pin = lore_revision::commit::commit_in_memory_revision(
        child.clone(),
        token,
        child_state,
        metadata,
        Default::default(),
        child_branch,
    )
    .await
    .unwrap();
    let state = lore_revision::state::State::new();
    let mount = state
        .node_add(
            repository.clone(),
            0,
            Node {
                flags: NodeFlags::Link.bits(),
                address: lore_base::types::Address {
                    context: child_id.into(),
                    hash: child_pin,
                },
                name_hash: lore_storage::hash::hash_string("vendor"),
                ..Default::default()
            },
            "vendor",
        )
        .await
        .unwrap();
    state
        .link_add(
            repository.clone(),
            child_id,
            child_branch,
            child_pin,
            mount,
            lore_revision::link::LinkFlags::NoFlags,
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            mount,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata
        .set_branch(input.prepared.selection.branch)
        .unwrap();
    let pin = lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        token,
        state,
        metadata,
        Default::default(),
        input.prepared.selection.branch,
    )
    .await
    .unwrap();
    lore_revision::instance::store_current_anchor(&repository, pin)
        .await
        .unwrap();
    repository.flush(true).await.unwrap();
    input.prepared.selection.revision = pin;
    (child_id, child_branch)
}

async fn advance_child_latest(
    repository: &Arc<lore_revision::repository::RepositoryContext>,
    child_id: lore_base::types::RepositoryId,
    branch: lore_base::types::BranchId,
) {
    let child = repository.to_link_context(child_id).await;
    let previous = lore_revision::branch::load_latest(child.clone(), branch)
        .await
        .unwrap();
    let state = lore_revision::state::State::new();
    state.set_parent_self(previous);
    state.set_revision_number(2);
    let revision = state
        .serialize(child.clone(), repository.try_write_token().unwrap())
        .await
        .unwrap();
    assert_ne!(revision, previous);
    lore_revision::branch::store_latest(
        child,
        branch,
        previous,
        revision,
        lore_revision::branch::BranchLatestStatus::Divergent,
    )
    .await
    .unwrap();
    repository.flush(true).await.unwrap();
}

#[tokio::test]
async fn linked_restore_keeps_pinned_exposure_when_child_branch_advances_independently() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-child-tip-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let mut input = input(&root, &registry).await;
        let (child_id, branch) = seed_linked_selection(&mut input).await;
        let instance = input.prepared.location.instance();
        let repository = input.prepared.repository.clone();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let vendor = model.lookup(1, b"vendor").await.unwrap();
        let content = model.lookup(vendor.entry.inode, b"content").await.unwrap();
        assert_ne!(vendor.entry.inode, 1);
        assert_ne!(content.entry.inode, 1);
        drop(model);
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        let token = RepositoryWriteToken::acquire(&root.join("mount")).await;
        let repository = Arc::new(repository.without_command_locks().with_write_token(token));
        advance_child_latest(&repository, child_id, branch).await;
        drop(repository);
        let prepared = supervisor
            .prepare_instance(
                reuse_input(&root, &registry, instance).await,
                Arc::new(ExplicitEdits),
                Default::default(),
            )
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let vendor = model.lookup(1, b"vendor").await.unwrap();
        assert!(model.lookup(vendor.entry.inode, b"content").await.is_ok());
        drop(model);
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_linked_commit_preserves_scoped_head_when_child_tip_advances() {
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    let registry = ProviderRegistry::new(Default::default());
    let resolver: Arc<dyn ProviderResolver> = registry.clone();
    let mut execution = execution();
    Arc::get_mut(&mut execution)
        .unwrap()
        .set_provider_resolver(&resolver);
    Box::pin(LORE_CONTEXT.scope(execution, async {
        let root = TempDir::new("lore-supervisor-linked-commit-");
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let mut input = input(&root, &registry).await;
        let (child_id, branch) = seed_linked_selection(&mut input).await;
        let instance = input.prepared.location.instance();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let model = prepared.test_model();
        let (uid, gid) = mount_ids();
        let handle = tokio::time::timeout(Duration::from_secs(20), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(root.join("mount/vendor/content"), b"committed linked bytes")
            .await
            .unwrap();
        let repository = lore_revision::repository::load_and_connect(
            root.join("mount"),
            lore_revision::repository::RepositoryAccess::ReadWrite,
        )
        .await
        .unwrap();
        let token = repository.try_write_token().unwrap();
        lore_revision::file::stage::stage(
            repository.clone(),
            token,
            LoreArray::from_vec(vec![LoreString::from(&root.join("mount/vendor/content"))]),
            lore_revision::stage::StageOptions {
                scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(30),
            lore_revision::commit::commit_boxed(
                repository.clone(),
                token,
                lore_revision::commit::CommitOptions::new("supervised linked commit".into()),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let vendor = model.lookup(1, b"vendor").await.unwrap();
        let content = model.lookup(vendor.entry.inode, b"content").await.unwrap();
        let digest = model.digest(content.entry.inode).await.unwrap().unwrap();
        // Another workspace advances the child's branch. This is not evidence
        // that this pinned exposure adopted that independent revision.
        advance_child_latest(&repository, child_id, branch).await;
        drop(repository);
        drop(model);
        supervisor.request_retirement(&handle).unwrap();
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        let prepared = supervisor
            .prepare_instance(
                reuse_input(&root, &registry, instance).await,
                Arc::new(ExplicitEdits),
                Default::default(),
            )
            .await
            .unwrap();
        let handle = prepared.handle();
        let model = prepared.test_model();
        let vendor = model.lookup(1, b"vendor").await.unwrap();
        let content = model.lookup(vendor.entry.inode, b"content").await.unwrap();
        assert_eq!(
            model.digest(content.entry.inode).await.unwrap(),
            Some(digest)
        );
        let file = model
            .open(
                content.entry.inode,
                lore_vfs::linux_working::OpenFlags {
                    write: false,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            model.read(file, 0, 100).await.unwrap(),
            b"committed linked bytes"
        );
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
    }))
    .await;
}

#[tokio::test]
async fn startup_racing_terminal_admission_closure_has_one_atomic_outcome() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-admission-race-");
        let rejected_root = TempDir::new("lore-supervisor-closed-input-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let prepared_input = input(&root, &registry).await;
        let candidate = input(&rejected_root, &registry).await;
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let admission = supervisor.clone();
        let start_barrier = barrier.clone();
        let start = std::thread::spawn(move || {
            start_barrier.wait();
            admission.reserve_startup(prepared_input)
        });
        let closure = supervisor.clone();
        let close = std::thread::spawn(move || {
            barrier.wait();
            closure.close_startup_admission()
        });
        close.join().unwrap().unwrap();
        let ticket = match start.join().unwrap() {
            Ok(ticket) => Some(ticket),
            Err(rejected) => {
                assert!(matches!(rejected.error, Error::Closed));
                None
            }
        };
        let rejected = supervisor.reserve_startup(candidate).err().unwrap();
        assert!(matches!(rejected.error, Error::Closed));
        assert!(supervisor.request_retirement_all().unwrap().is_empty());
        if let Some(ticket) = &ticket {
            assert!(matches!(ticket.check_live(), Err(Error::Closed)));
        }
        drop(ticket);
        let report = tokio::time::timeout(Duration::from_secs(20), supervisor.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert!(report.failures.is_empty());
        assert!(supervisor.instances().unwrap().is_empty());
        assert!(
            registry
                .reserve(&rejected_root.join("mount"), InstanceId::generate())
                .is_err(),
            "rejected input ownership belongs to the caller"
        );
        drop(rejected);
        registry
            .reserve(&rejected_root.join("mount"), InstanceId::generate())
            .unwrap();
    }))
    .await;
}

#[tokio::test]
async fn cancelled_supervisor_drain_keeps_closed_admission_and_waits_startup_ticket() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-drain-ticket-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let startup = supervisor
            .reserve_startup(input(&root, &registry).await)
            .unwrap();
        let mut draining = Box::pin(supervisor.close_and_drain());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut draining)
                .await
                .is_err()
        );
        drop(draining);
        assert!(matches!(startup.check_live(), Err(Error::Closed)));
        assert_eq!(supervisor.instances().unwrap().len(), 1);
        assert!(
            registry
                .reserve(&root.join("mount"), InstanceId::generate())
                .is_err()
        );
        drop(startup);
        let report = tokio::time::timeout(Duration::from_secs(20), supervisor.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.completed.len(), 1);
        assert!(report.failures.is_empty());
        assert!(supervisor.instances().unwrap().is_empty());
        let replacement_root = TempDir::new("lore-supervisor-still-closed-");
        let replacement = input(&replacement_root, &registry).await;
        let rejected = supervisor.reserve_startup(replacement).err().unwrap();
        assert!(matches!(rejected.error, Error::Closed));
    }))
    .await;
}

#[tokio::test]
async fn supervisor_drain_reports_failed_owner_and_finishes_independent_healthy_record() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let failed = TempDir::new("lore-supervisor-drain-failed-");
        let healthy = TempDir::new("lore-supervisor-drain-healthy-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let bad = supervisor
            .prepare_instance(
                input_with_recovery_limits(
                    &failed,
                    &registry,
                    lore_vfs::recovery::Limits {
                        max_checkpoint_bytes: 1024,
                    },
                )
                .await,
                Arc::new(ExplicitEdits),
                lore::working_mount::OwnerLimits {
                    recovery: lore_vfs::recovery::Limits {
                        max_checkpoint_bytes: 1024,
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let model = bad.test_model();
        let (file, _) = model
            .create(
                1,
                b"too-large",
                0o644,
                lore_vfs::linux_working::OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model.write(file, 0, &[3; 2048]).await.unwrap();
        model.release(file).await.unwrap();
        drop(model);
        drop(bad);
        let good = supervisor
            .reserve_startup(input(&healthy, &registry).await)
            .unwrap();
        let good_id = good.handle().id();
        drop(good);
        let report = tokio::time::timeout(Duration::from_secs(30), supervisor.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.completed.len(), 1);
        assert_eq!(report.completed[0].id(), good_id);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(supervisor.instances().unwrap().len(), 1);
        registry
            .reserve(&healthy.join("mount"), InstanceId::generate())
            .unwrap();
        assert!(
            registry
                .reserve(&failed.join("mount"), InstanceId::generate())
                .is_err()
        );
    }))
    .await;
}

#[tokio::test]
async fn concurrent_supervisor_drains_share_startup_completion_without_stale_failures() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-multi-drain-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let ticket = supervisor
            .reserve_startup(input(&root, &registry).await)
            .unwrap();
        let mut first = Box::pin(supervisor.close_and_drain());
        let mut second = Box::pin(supervisor.close_and_drain());
        tokio::select! {
            result=&mut first=>panic!("first drain ignored active startup: {result:?}"),
            result=&mut second=>panic!("second drain ignored active startup: {result:?}"),
            _=tokio::time::sleep(Duration::from_millis(20))=>{},
        }
        drop(ticket);
        let (first, second) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(first, second)
        })
        .await
        .unwrap();
        let first = first.unwrap();
        let second = second.unwrap();
        assert!(first.failures.is_empty());
        assert!(second.failures.is_empty());
        assert_eq!(first.completed.len(), 1);
        assert_eq!(second.completed.len(), 1);
        assert!(supervisor.instances().unwrap().is_empty());
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_stale_provider_and_finished_operation_keep_terminal_reservation() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-stale-capabilities-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let location = input.prepared.location.clone();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let model = prepared.test_model();
        let (file, _) = model
            .create(
                1,
                b"preserved",
                0o644,
                lore_vfs::linux_working::OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            model
                .write(file, 0, b"retained namespace stores")
                .await
                .unwrap(),
            25
        );
        // Writer release must finish before the provider freezes the namespace.
        model.release(file).await.unwrap();
        drop(model);
        let (uid, gid) = mount_ids();
        let handle = tokio::time::timeout(Duration::from_secs(20), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tokio::fs::read(root.join("mount/preserved")).await.unwrap(),
            b"retained namespace stores"
        );
        let stale = registry.resolve_location(&location).unwrap();
        let finished = stale.begin_operation().await.unwrap();
        finished.abort();
        supervisor.request_retirement(&handle).unwrap();
        tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
            .await
            .unwrap()
            .unwrap();
        assert!(supervisor.instances().unwrap().is_empty());
        assert!(stale.begin_operation().await.is_err());
        assert!(matches!(
            registry.resolve_location(&location),
            Err(lore_revision::fs::provider_registry::Error::Retired)
        ));
        assert!(matches!(
            registry.reserve(&root.join("mount"), location.instance()),
            Err(lore_revision::fs::provider_registry::Error::Overlap)
        ));
        assert!(matches!(
            WorkingStorage::open(
                &root.join("backing"),
                &root.join("mount"),
                location.repository(),
                location.instance(),
                Default::default()
            ),
            Err(lore_vfs::working_storage::Error::Busy)
        ));
        drop(stale);
        assert!(matches!(
            registry.reserve(&root.join("mount"), location.instance()),
            Err(lore_revision::fs::provider_registry::Error::Overlap)
        ));
        drop(finished);
        let _replacement = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match registry.reserve(&root.join("mount"), location.instance()) {
                    Ok(value) => break value,
                    Err(lore_revision::fs::provider_registry::Error::Overlap) => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected reservation failure: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        // Reservation release is the barrier: neither backing nor recovery lock
        // may still be Busy once the replacement registration succeeds.
        let storage = WorkingStorage::open(
            &root.join("backing"),
            &root.join("mount"),
            location.repository(),
            location.instance(),
            Default::default(),
        )
        .unwrap();
        let recovery =
            lore_vfs::recovery::Recovery::open(storage.recovery_directory(), Default::default())
                .unwrap();
        assert!(recovery.load().unwrap().is_some());
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux /dev/fuse and unprivileged mount support"]
async fn mounted_supervisor_terminal_drain_waits_ready_owner_and_keeps_admission_closed() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let root = TempDir::new("lore-supervisor-terminal-mount-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let input = input(&root, &registry).await;
        let instance = input.prepared.location.instance();
        let prepared = supervisor
            .prepare_instance(input, Arc::new(ExplicitEdits), Default::default())
            .await
            .unwrap();
        let (uid, gid) = mount_ids();
        tokio::time::timeout(Duration::from_secs(20), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap();
        tokio::fs::write(root.join("mount/retained"), b"terminal checkpoint")
            .await
            .unwrap();
        let report = tokio::time::timeout(Duration::from_secs(30), supervisor.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.completed.len(), 1);
        assert!(report.failures.is_empty());
        assert!(supervisor.instances().unwrap().is_empty());
        assert!(!root.join("mount/retained").exists());
        let input = reuse_input(&root, &registry, instance).await;
        let rejected = supervisor.reserve_startup(input).err().unwrap();
        assert!(matches!(rejected.error, Error::Closed));
        let next = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let ticket = next.reserve_startup(*rejected.input).unwrap();
        drop(ticket);
        let report = tokio::time::timeout(Duration::from_secs(20), next.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.completed.len(), 1);
        assert!(report.failures.is_empty());
    }))
    .await;
}

#[tokio::test]
async fn digest_cache_rejects_unsafe_and_managed_paths_before_creating_directories() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::fs::symlink;
        let root = TempDir::new("lore-supervisor-cache-refuse-");
        let other = TempDir::new("lore-supervisor-cache-managed-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let (input, other_input) = tokio::join!(input(&root, &registry), input(&other, &registry));
        let instance = input.prepared.location.instance();
        let managed_other = other.join("backing");
        let other = supervisor.reserve_startup(other_input).unwrap();
        let other_handle = other.handle();
        let unsafe_dir = root.join("unsafe-cache");
        std::fs::create_dir(&unsafe_dir).unwrap();
        std::fs::set_permissions(&unsafe_dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        let private = root.join("private-cache");
        std::fs::create_dir(&private).unwrap();
        let alias = root.join("cache-alias");
        symlink(&private, &alias).unwrap();
        let cases = [
            root.join("metadata/must-not-create"),
            managed_other.join("must-not-create"),
            alias,
            unsafe_dir.clone(),
            std::path::PathBuf::from("relative-cache"),
            std::path::PathBuf::from(format!("/{}", "x".repeat(4096))),
        ];
        let mut next = Some(input);
        for path in cases {
            let input = match next.take() {
                Some(input) => input,
                None => reuse_input(&root, &registry, instance).await,
            };
            let result = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(ExplicitEdits),
                    Default::default(),
                    lore::working_mount::Options {
                        digest_cache: Some(path.clone()),
                    },
                )
                .await;
            let lore::working_mount::PreparationError::Retiring { handle, error } =
                result.err().unwrap()
            else {
                panic!("cache failure must retain a discoverable startup")
            };
            assert!(matches!(error, Error::Cache(_) | Error::Overlap));
            assert_eq!(supervisor.status(&handle).unwrap().phase, Phase::Retiring);
            tokio::time::timeout(Duration::from_secs(20), supervisor.retire_instance(&handle))
                .await
                .unwrap()
                .unwrap();
        }
        assert!(!root.join("metadata/must-not-create").exists());
        assert!(!managed_other.join("must-not-create").exists());
        assert_eq!(std::fs::read_dir(&unsafe_dir).unwrap().count(), 0);
        assert_eq!(
            std::fs::metadata(&unsafe_dir).unwrap().permissions().mode() & 0o777,
            0o777
        );
        drop(other);
        tokio::time::timeout(
            Duration::from_secs(20),
            supervisor.retire_instance(&other_handle),
        )
        .await
        .unwrap()
        .unwrap();
    }))
    .await;
}

#[tokio::test]
async fn digest_cache_is_shared_but_future_owned_roots_cannot_overlap_it() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        let first = TempDir::new("lore-supervisor-shared-cache-first-");
        let second = TempDir::new("lore-supervisor-shared-cache-second-");
        let cache = TempDir::new("lore-supervisor-shared-cache-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let (first, second) = tokio::join!(input(&first, &registry), input(&second, &registry));
        let first = supervisor
            .prepare_instance_with_options(
                first,
                Arc::new(ExplicitEdits),
                Default::default(),
                lore::working_mount::Options {
                    digest_cache: Some(cache.to_path_buf()),
                },
            )
            .await
            .unwrap();
        let second = supervisor
            .prepare_instance_with_options(
                second,
                Arc::new(ExplicitEdits),
                Default::default(),
                lore::working_mount::Options {
                    digest_cache: Some(cache.to_path_buf()),
                },
            )
            .await
            .unwrap();
        let nested = cache.join("overlapping-owner");
        std::fs::create_dir(&nested).unwrap();
        let input = input(&nested, &registry).await;
        let rejected = supervisor.reserve_startup(input).err().unwrap();
        assert!(matches!(rejected.error, Error::Overlap));
        drop(rejected);
        drop(first);
        drop(second);
        let report = tokio::time::timeout(Duration::from_secs(30), supervisor.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        assert!(report.failures.is_empty());
        assert_eq!(report.completed.len(), 2);
    }))
    .await;
}

async fn install_unavailable_content(input: &mut StartupInput) {
    let token = RepositoryWriteToken::acquire(input.prepared.location.target().path()).await;
    let repository = Arc::new(
        input
            .prepared
            .repository
            .without_command_locks()
            .with_write_token(token),
    );
    let state = lore_revision::state::State::new();
    state
        .node_add(
            repository.clone(),
            0,
            lore_revision::node::Node {
                flags: lore_revision::node::NodeFlags::File.bits(),
                size: 8,
                address: lore_base::types::Address {
                    hash: lore_base::types::Hash::from_u64(12345),
                    context: [12; 16].into(),
                },
                name_hash: lore_storage::hash::hash_string("unavailable"),
                ..Default::default()
            },
            "unavailable",
        )
        .await
        .unwrap();
    state.set_revision_number(1);
    let revision = state
        .serialize(repository.clone(), repository.try_write_token().unwrap())
        .await
        .unwrap();
    lore_revision::instance::store_current_anchor(&repository, revision)
        .await
        .unwrap();
    repository.flush(true).await.unwrap();
    input.prepared.selection.revision = revision;
}

#[tokio::test]
async fn absent_and_empty_digest_indexes_never_hydrate_unavailable_file_content() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        for configured in [false, true] {
            let root = TempDir::new("lore-supervisor-cache-no-hydration-");
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
            let mut input = input(&root, &registry).await;
            install_unavailable_content(&mut input).await;
            let cache = root.join("digests");
            let prepared = supervisor
                .prepare_instance_with_options(
                    input,
                    Arc::new(ExplicitEdits),
                    Default::default(),
                    lore::working_mount::Options {
                        digest_cache: configured.then(|| cache.clone()),
                    },
                )
                .await
                .unwrap();
            let handle = prepared.handle();
            let model = prepared.test_model();
            let file = model.lookup(1, b"unavailable").await.unwrap();
            assert_eq!(model.digest(file.entry.inode).await.unwrap(), None);
            if configured {
                assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
            } else {
                assert!(!cache.exists());
            }
            let reader = model
                .open(
                    file.entry.inode,
                    lore_vfs::linux_working::OpenFlags {
                        write: false,
                        append: false,
                        truncate: false,
                    },
                )
                .await
                .unwrap();
            assert!(
                model.read(reader, 0, 8).await.is_err(),
                "the fixture's content is actually unavailable"
            );
            model.release(reader).await.unwrap();
            drop(model);
            drop(prepared);
            tokio::time::timeout(Duration::from_secs(30), supervisor.retire_instance(&handle))
                .await
                .unwrap()
                .unwrap();
        }
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux mount table inspection"]
async fn mounted_supervisor_cache_revalidation_refuses_unsafe_directory_before_session_creation() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new("lore-supervisor-cache-revalidate-");
        let registry = ProviderRegistry::new(Default::default());
        let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
        let cache = root.join("digests");
        let prepared = supervisor
            .prepare_instance_with_options(
                input(&root, &registry).await,
                Arc::new(ExplicitEdits),
                Default::default(),
                lore::working_mount::Options {
                    digest_cache: Some(cache.clone()),
                },
            )
            .await
            .unwrap();
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o777)).unwrap();
        let (uid, gid) = mount_ids();
        let failure = tokio::time::timeout(Duration::from_secs(10), prepared.mount(uid, gid))
            .await
            .unwrap()
            .unwrap_err();
        let lore::working_mount::PreparationError::Retiring {
            handle,
            error: Error::Cache(_),
        } = failure
        else {
            panic!("mount must refuse changed cache safety")
        };
        lore::mount::validate_mountpoint(&root.join("mount"), Some(&root.join("metadata")))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match supervisor.retire_instance(&handle).await {
                    Err(Error::Busy) => tokio::task::yield_now().await,
                    result => break result.unwrap(),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::metadata(&cache).unwrap().permissions().mode() & 0o777,
            0o777
        );
        assert!(supervisor.instances().unwrap().is_empty());
    }))
    .await;
}

#[tokio::test]
async fn supervisor_rejects_metadata_ancestor_mountpoints_before_session_construction() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        for component in [".LoRe", ".uRc"] {
            let root = TempDir::new("lore-supervisor-metadata-target-");
            let base = root.join(component);
            std::fs::create_dir(&base).unwrap();
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
            let input = input(&base, &registry).await;
            let storage = input.storage.clone();
            let result = tokio::time::timeout(
                Duration::from_secs(20),
                supervisor.prepare_instance(input, Arc::new(ExplicitEdits), Default::default()),
            )
            .await
            .unwrap();
            let lore::working_mount::PreparationError::Retiring {
                handle,
                error: Error::Mountpoint(_),
            } = result.err().unwrap()
            else {
                panic!("metadata ancestor must refuse startup")
            };
            assert_eq!(storage.retained_sessions().unwrap(), 0);
            drop(storage);
            tokio::time::timeout(Duration::from_secs(20), supervisor.retire_instance(&handle))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(std::fs::read_dir(base.join("mount")).unwrap().count(), 0);
        }
    }))
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Linux mount table inspection"]
async fn mounted_supervisor_revalidates_nonempty_or_replaced_target_without_mounting() {
    Box::pin(LORE_CONTEXT.scope(execution(), async {
        for replaced in [false, true] {
            let root = TempDir::new("lore-supervisor-changed-target-");
            let registry = ProviderRegistry::new(Default::default());
            let supervisor = Supervisor::new(registry.clone(), Limits::default()).unwrap();
            let prepared = supervisor
                .prepare_instance(
                    input(&root, &registry).await,
                    Arc::new(ExplicitEdits),
                    Default::default(),
                )
                .await
                .unwrap();
            let target = root.join("mount");
            let original = root.join("original-target");
            if replaced {
                std::fs::rename(&target, &original).unwrap();
                std::fs::create_dir(&target).unwrap();
            } else {
                std::fs::write(target.join("user-file"), b"preserve underlay bytes").unwrap();
            }
            let (uid, gid) = mount_ids();
            let failure = tokio::time::timeout(Duration::from_secs(10), prepared.mount(uid, gid))
                .await
                .unwrap()
                .unwrap_err();
            let lore::working_mount::PreparationError::Retiring {
                handle,
                error: Error::Mountpoint(_),
            } = failure
            else {
                panic!("changed target must refuse session creation")
            };
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    match supervisor.retire_instance(&handle).await {
                        Err(Error::Busy) => tokio::task::yield_now().await,
                        result => break result.unwrap(),
                    }
                }
            })
            .await
            .unwrap();
            if replaced {
                assert!(original.is_dir());
                assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
            } else {
                assert_eq!(
                    std::fs::read(target.join("user-file")).unwrap(),
                    b"preserve underlay bytes"
                );
            }
            assert!(supervisor.instances().unwrap().is_empty());
        }
    }))
    .await;
}
