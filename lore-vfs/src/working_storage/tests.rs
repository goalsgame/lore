// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Fixtures deliberately occupy their private runtime pool and manipulate private files.
#![allow(clippy::disallowed_methods)]

use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::PermissionsExt;
use std::task::Poll;

use super::*;
use crate::core::Backend;
use crate::core::BackendPage;
use crate::core::Error as CoreError;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::linux_working::Model;
use crate::linux_working::OpenFlags;
use crate::namespace::Namespace;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::recovery::Recovery;
use crate::storage_budget::StorageBudget;

fn repository() -> RepositoryId {
    [23; 16].into()
}
fn directory() -> crate::digest_index::tests::Directory {
    let directory = crate::digest_index::tests::Directory::new();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory.0)
        .unwrap();
    directory
}
fn mount(directory: &Path) -> PathBuf {
    let path = directory.join("mount");
    fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
    path
}
fn open_storage(
    root: &Path,
    mount: &Path,
    instance: InstanceId,
    limits: Limits,
) -> Arc<WorkingStorage> {
    WorkingStorage::open(root, mount, repository(), instance, limits).unwrap()
}

#[tokio::test]
async fn factories_and_open_stores_keep_the_exclusive_root_alive() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("storage");
    let instance = InstanceId::generate();
    let storage = open_storage(&root, &mount, instance, Limits::default());
    let weak = Arc::downgrade(&storage);
    let session = storage.begin_session().unwrap();
    let factory = session.factory();
    drop(session);
    drop(storage);
    assert!(matches!(
        WorkingStorage::open(&root, &mount, repository(), instance, Limits::default()),
        Err(Error::Busy)
    ));
    let store = factory.create().await.unwrap();
    drop(factory);
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        WorkingStorage::open(&root, &mount, repository(), instance, Limits::default()),
        Err(Error::Busy)
    ));
    store.write(0, b"retained").await.unwrap();
    assert_eq!(store.read(0, 8).await.unwrap(), b"retained");
    drop(store);
    assert!(weak.upgrade().is_none());
    let reopened = open_storage(&root, &mount, instance, Limits::default());
    assert_eq!(reopened.retained_sessions().unwrap(), 1);
    let later = reopened.begin_session().unwrap();
    assert_ne!(
        later.directory().file_name().unwrap(),
        "run-0000000000000001"
    );
    assert!(
        root.join("sessions/run-0000000000000001/stores/store-0000000000000001")
            .exists()
    );
}

#[tokio::test]
async fn concurrent_store_creation_and_failed_creates_consume_the_file_bound() {
    let directory = directory();
    let mount = mount(&directory.0);
    let instance = InstanceId::generate();
    let storage = open_storage(
        &directory.0.join("storage"),
        &mount,
        instance,
        Limits {
            max_store_files_per_session: 2,
            ..Limits::default()
        },
    );
    let session = storage.begin_session().unwrap();
    let factory = session.factory();
    let unpolled = factory.create();
    assert_eq!(session.attempted_stores(), 0);
    drop(unpolled);
    let (first, second, third) = tokio::join!(factory.create(), factory.create(), factory.create());
    let results = [first, second, third];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(CoreError::ResourceLimit)))
            .count(),
        1
    );
    assert_eq!(session.attempted_stores(), 2);
    assert_eq!(fs::read_dir(session.store_directory()).unwrap().count(), 2);
    let failed = storage.begin_session().unwrap();
    let existing = failed.store_directory().join("store-0000000000000001");
    fs::write(&existing, b"preserve").unwrap();
    fs::set_permissions(&existing, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(matches!(
        failed.factory().create().await,
        Err(CoreError::Io)
    ));
    assert_eq!(fs::read(existing).unwrap(), b"preserve");
    assert_eq!(failed.attempted_stores(), 1);
    assert!(failed.factory().create().await.is_ok());
    assert!(matches!(
        failed.factory().create().await,
        Err(CoreError::ResourceLimit)
    ));
    assert_eq!(failed.attempted_stores(), 2);
}

#[test]
fn cancellation_cannot_refund_an_admitted_file_creation() {
    let directory = directory();
    let mount = mount(&directory.0);
    let storage = open_storage(
        &directory.0.join("storage"),
        &mount,
        InstanceId::generate(),
        Limits {
            max_store_files_per_session: 1,
            ..Limits::default()
        },
    );
    let session = storage.begin_session().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started, notice) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = released.recv();
        });
        notice
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let factory = session.factory();
        let mut creation = factory.create();
        std::future::poll_fn(|context| {
            assert!(matches!(creation.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        assert_eq!(session.attempted_stores(), 1);
        drop(creation);
        release.send(()).unwrap();
        worker.await.unwrap();
        assert!(matches!(
            factory.create().await,
            Err(CoreError::ResourceLimit)
        ));
        assert_eq!(session.attempted_stores(), 1);
    });
}

#[test]
fn retained_sessions_and_failed_startup_are_never_refunded_or_deleted() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("storage");
    let instance = InstanceId::generate();
    let limits = Limits {
        max_retained_sessions: 1,
        ..Limits::default()
    };
    let storage = open_storage(&root, &mount, instance, limits);
    let occupied = root.join("sessions/run-0000000000000001");
    create_directory(&occupied).unwrap();
    assert!(matches!(storage.begin_session(), Err(Error::Io(_))));
    assert_eq!(storage.retained_sessions().unwrap(), 1);
    assert!(matches!(storage.begin_session(), Err(Error::SessionLimit)));
    assert!(occupied.exists());
    drop(storage);
    let storage = open_storage(&root, &mount, instance, limits);
    assert_eq!(storage.retained_sessions().unwrap(), 1);
    assert!(matches!(storage.begin_session(), Err(Error::SessionLimit)));
    assert!(storage.recovery_directory().is_dir());
}

#[test]
fn identity_layout_permissions_and_overlapping_paths_fail_without_repairs() {
    let directory = directory();
    let mount = mount(&directory.0);
    let root = directory.0.join("storage");
    let instance = InstanceId::generate();
    assert!(matches!(
        WorkingStorage::open(
            &mount.join("hidden"),
            &mount,
            repository(),
            instance,
            Limits::default()
        ),
        Err(Error::Overlap)
    ));
    assert!(!mount.join("hidden").exists());
    assert!(matches!(
        WorkingStorage::open(
            &directory.0,
            &mount,
            repository(),
            instance,
            Limits::default()
        ),
        Err(Error::Overlap)
    ));
    let empty = directory.0.join("empty");
    create_directory(&empty).unwrap();
    assert!(matches!(
        WorkingStorage::open(&empty, &mount, repository(), instance, Limits::default()),
        Err(Error::Incomplete)
    ));
    assert_eq!(fs::read_dir(&empty).unwrap().count(), 0);
    let alias = directory.0.join("alias");
    std::os::unix::fs::symlink(&empty, &alias).unwrap();
    assert!(matches!(
        WorkingStorage::open(&alias, &mount, repository(), instance, Limits::default()),
        Err(Error::UnsafePath)
    ));
    let owner = open_storage(&root, &mount, instance, Limits::default());
    assert_eq!(owner.repository(), repository());
    assert_eq!(owner.instance(), instance);
    drop(owner);
    let marker = root.join("identity");
    let original = fs::read(&marker).unwrap();
    assert!(matches!(
        WorkingStorage::open(&root, &mount, [24; 16].into(), instance, Limits::default()),
        Err(Error::IdentityMismatch)
    ));
    assert!(matches!(
        WorkingStorage::open(
            &root,
            &mount,
            repository(),
            InstanceId::generate(),
            Limits::default()
        ),
        Err(Error::IdentityMismatch)
    ));
    assert_eq!(fs::read(&marker).unwrap(), original);
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&marker, &corrupt).unwrap();
    assert!(matches!(
        WorkingStorage::open(&root, &mount, repository(), instance, Limits::default()),
        Err(Error::Corrupt)
    ));
    assert_eq!(fs::read(&marker).unwrap(), corrupt);
    fs::write(&marker, &original).unwrap();
    fs::write(root.join("unexpected"), b"keep").unwrap();
    assert!(matches!(
        WorkingStorage::open(&root, &mount, repository(), instance, Limits::default()),
        Err(Error::UnknownEntry)
    ));
    assert_eq!(fs::read(root.join("unexpected")).unwrap(), b"keep");
    fs::remove_file(root.join("unexpected")).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(matches!(
        WorkingStorage::open(&root, &mount, repository(), instance, Limits::default()),
        Err(Error::UnsafePath)
    ));
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(WorkingStorage::open(&root, &mount, repository(), instance, Limits::default()).is_ok());
}

struct Empty;
impl Backend for Empty {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(CoreError::NotFound) })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async {
            Ok(BackendPage {
                entries: Vec::new(),
                next_offset: None,
            })
        })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("new local files have no backend payload") })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Ok(None) })
    }
}
struct Allow;
impl AccessGate for Allow {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
fn root_node() -> Node {
    Node {
        key: NodeKey {
            repository: repository(),
            revision: lore_base::types::Address::zero_context_hash([2; 32].into()),
            node: 0,
            exposure: 1,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}

#[tokio::test]
async fn model_fsync_restarts_from_checkpoint_into_fresh_session_files() {
    let directory = directory();
    let mount = mount(&directory.0);
    let path = directory.0.join("storage");
    let instance = InstanceId::generate();
    let storage = open_storage(&path, &mount, instance, Limits::default());
    let session = storage.begin_session().unwrap();
    let old_stores = session.store_directory().to_path_buf();
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Empty),
            root_node(),
            Arc::new(Allow),
            Default::default(),
        )
        .unwrap(),
    );
    let recovery = Recovery::open(storage.recovery_directory(), Default::default()).unwrap();
    let model = Model::new(
        namespace,
        session.factory(),
        Arc::new(StorageBudget::new(4096)),
        recovery,
        Default::default(),
    )
    .await
    .unwrap();
    let (handle, _) = model
        .create(
            1,
            b"data",
            0o644,
            OpenFlags {
                write: true,
                append: false,
                truncate: false,
            },
        )
        .await
        .unwrap();
    model.write(handle, 0, b"checkpointed").await.unwrap();
    model.fsync(handle, false).await.unwrap();
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
    drop(model);
    drop(session);
    drop(storage);
    let storage = open_storage(&path, &mount, instance, Limits::default());
    let fresh = storage.begin_session().unwrap();
    assert_ne!(old_stores, fresh.store_directory());
    let recovery = Recovery::open(storage.recovery_directory(), Default::default()).unwrap();
    let model = Model::restore(
        Arc::new(Empty),
        root_node(),
        Arc::new(Allow),
        Default::default(),
        fresh.factory(),
        Arc::new(StorageBudget::new(4096)),
        recovery,
        Default::default(),
    )
    .await
    .unwrap();
    let file = model.lookup(1, b"data").await.unwrap();
    let handle = model
        .open(
            file.entry.inode,
            OpenFlags {
                write: false,
                append: false,
                truncate: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(model.read(handle, 0, 100).await.unwrap(), b"checkpointed");
    assert!(model.budget().reserved_bytes() <= 4096);
    assert_eq!(fs::read_dir(&old_stores).unwrap().count(), 1);
    assert_eq!(fs::read_dir(fresh.store_directory()).unwrap().count(), 1);
    assert_eq!(storage.retained_sessions().unwrap(), 2);
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn replacing_a_session_path_with_a_symlink_cannot_redirect_store_creation() {
    let directory = directory();
    let mount = mount(&directory.0);
    let owner = open_storage(
        &directory.0.join("storage"),
        &mount,
        InstanceId::generate(),
        Limits::default(),
    );
    let session = owner.begin_session().unwrap();
    let moved = owner.root().join("sessions/run-0000000000000002");
    fs::rename(session.directory(), &moved).unwrap();
    std::os::unix::fs::symlink(&moved, session.directory()).unwrap();
    assert!(matches!(
        session.factory().create().await,
        Err(CoreError::Io)
    ));
    assert_eq!(session.attempted_stores(), 1);
    assert_eq!(fs::read_dir(moved.join("stores")).unwrap().count(), 0);
    fs::remove_file(session.directory()).unwrap();
    fs::rename(&moved, session.directory()).unwrap();
    assert!(session.factory().create().await.is_ok());
}

#[test]
fn drain_waits_for_cancelled_queued_io_and_permanently_fences_new_jobs() {
    let directory = directory();
    let mount = mount(&directory.0);
    let owner = open_storage(
        &directory.0.join("storage"),
        &mount,
        InstanceId::generate(),
        Limits {
            max_io_jobs: 1,
            ..Limits::default()
        },
    );
    let session = owner.begin_session().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started, notice) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = released.recv();
        });
        notice
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let mut create = session.create();
        std::future::poll_fn(|context| {
            assert!(create.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(create);
        let mut drain = Box::pin(owner.close_and_drain_io());
        std::future::poll_fn(|context| {
            assert!(drain.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(drain);
        assert!(matches!(session.create().await, Err(CoreError::Io)));
        release.send(()).unwrap();
        worker.await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            owner.close_and_drain_io(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(fs::read_dir(session.store_directory()).unwrap().count(), 1);
        assert!(matches!(session.create().await, Err(CoreError::Io)));
    });
}

#[tokio::test]
async fn closing_io_rejects_a_previously_acquired_capacity_permit() {
    let directory = directory();
    let mount = mount(&directory.0);
    let owner = open_storage(
        &directory.0.join("storage"),
        &mount,
        InstanceId::generate(),
        Limits::default(),
    );
    let session = owner.begin_session().unwrap();
    let permit = io_permit(&session.inner).await.unwrap();
    owner.close_and_drain_io().await.unwrap();
    assert!(matches!(
        execute_io(session.inner.clone(), permit, |_session| {
            panic!("closed storage admitted an OS job")
        })
        .await,
        Err::<(), _>(CoreError::Io)
    ));
}

#[test]
fn cancelled_os_jobs_keep_the_root_locked_after_all_outer_owners_drop() {
    for operation in 0..3 {
        let directory = directory();
        let mount = mount(&directory.0);
        let path = directory.0.join("storage");
        let instance = InstanceId::generate();
        let limits = Limits {
            max_io_jobs: 1,
            ..Limits::default()
        };
        let owner = open_storage(&path, &mount, instance, limits);
        let weak = Arc::downgrade(&owner);
        let io = owner.io.clone();
        let session = owner.begin_session().unwrap();
        let factory = session.factory();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = if operation == 0 {
                None
            } else {
                let store = factory.create().await.unwrap();
                store.write(0, b"before").await.unwrap();
                Some(store)
            };
            let (started, notice) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let worker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = released.recv();
            });
            notice
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let mut pending: std::pin::Pin<Box<dyn Future<Output = ()> + '_>> = match operation {
                0 => Box::pin(async {
                    let _ = factory.create().await;
                }),
                1 => Box::pin(async {
                    let _ = store.as_ref().unwrap().write(0, b"queued").await;
                }),
                _ => Box::pin(async {
                    let _ = store.as_ref().unwrap().read(0, 6).await;
                }),
            };
            std::future::poll_fn(|context| {
                assert!(matches!(pending.as_mut().poll(context), Poll::Pending));
                Poll::Ready(())
            })
            .await;
            assert_eq!(io.available_permits(), 0);
            drop(pending);
            drop(store);
            drop(factory);
            drop(session);
            drop(owner);
            assert!(
                weak.upgrade().is_some(),
                "queued OS job lost its storage lease"
            );
            assert!(matches!(
                WorkingStorage::open(&path, &mount, repository(), instance, limits),
                Err(Error::Busy)
            ));
            release.send(()).unwrap();
            worker.await.unwrap();
            let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    match WorkingStorage::open(&path, &mount, repository(), instance, limits) {
                        Ok(owner) => break owner,
                        Err(Error::Busy) => tokio::task::yield_now().await,
                        Err(error) => panic!("storage did not reopen after queued IO: {error:?}"),
                    }
                }
            })
            .await
            .unwrap();
            assert!(weak.upgrade().is_none());
            assert_eq!(io.available_permits(), 1);
            let stored = path.join("sessions/run-0000000000000001/stores/store-0000000000000001");
            let expected = match operation {
                0 => &b""[..],
                1 => &b"queued"[..],
                _ => &b"before"[..],
            };
            assert_eq!(fs::read(stored).unwrap(), expected);
            assert_eq!(reopened.retained_sessions().unwrap(), 1);
        });
    }
}

#[path = "open_modes_tests.rs"]
mod open_modes;

#[path = "owner_validation_tests.rs"]
mod owner_validation;
