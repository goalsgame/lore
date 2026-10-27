// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use super::*;
use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::linux_working::Limits;
use crate::linux_working::Model;
use crate::linux_working::OpenFlags;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::SparseStore;
use crate::storage_budget::StorageBudget;

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-checkpoint-owner-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn root() -> Node {
    Node {
        key: NodeKey {
            repository: [1; 16].into(),
            revision: Default::default(),
            node: 0,
            exposure: 0,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}
struct Base;
impl Backend for Base {
    fn lookup<'a>(&'a self, _key: &'a NodeKey, _name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(core::Error::NotFound) })
    }
    fn list<'a>(
        &'a self,
        _key: &'a NodeKey,
        _offset: u64,
        _limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async {
            Ok(BackendPage {
                entries: vec![],
                next_offset: None,
            })
        })
    }
    fn read<'a>(
        &'a self,
        _key: &'a NodeKey,
        _offset: u64,
        _size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("empty base payload read") })
    }
    fn digest<'a>(&'a self, _key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Ok(None) })
    }
}
struct Gate;
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _key: &'a NodeKey, _access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
#[derive(Default)]
struct Store(Mutex<Vec<u8>>);
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            Ok(self.0.lock().unwrap()[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut data = self.0.lock().unwrap();
            data.resize(offset as usize + bytes.len(), 0);
            data[offset as usize..].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}
struct Factory;
impl StoreFactory for Factory {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async { Ok(Arc::new(Store::default()) as Arc<dyn SparseStore>) })
    }
}
fn namespace() -> Arc<Namespace> {
    Arc::new(Namespace::new(Arc::new(Base), root(), Arc::new(Gate), Default::default()).unwrap())
}
fn owner(directory: &Directory, namespace: Arc<Namespace>) -> Arc<CheckpointOwner> {
    CheckpointOwner::new(
        namespace,
        Arc::new(Factory),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        1 << 20,
    )
}

#[tokio::test]
async fn exact_freeze_pins_drop_before_success_and_spawn_failure_releases_admission() {
    let directory = Directory::new();
    let namespace = namespace();
    let owner = owner(&directory, namespace.clone());
    let phase = namespace.freeze_operation().await.unwrap();
    let pin = phase.pin();
    drop(phase);
    assert!(namespace.freeze_operation().await.is_err());
    owner.publish_frozen(pin).await.unwrap();
    drop(namespace.freeze_operation().await.unwrap());
    assert_eq!(
        owner.recovery.lock().unwrap().original_root().unwrap(),
        Some(root())
    );
    owner.fail_spawn.store(true, Ordering::Release);
    let lease = namespace.freeze_operation().await.unwrap();
    assert!(matches!(
        owner.publish_frozen(lease).await,
        Err(Error::Spawn(_))
    ));
    owner.drain().unwrap();
    assert!(!owner.is_poisoned());
    drop(namespace.freeze_operation().await.unwrap());
    owner.fail_spawn.store(false, Ordering::Release);
    owner.publish().await.unwrap();
}
#[tokio::test]
async fn close_fences_capture_already_ready_for_local_admission() {
    let directory = Directory::new();
    let namespace = namespace();
    let owner = owner(&directory, namespace.clone());
    let pause = Arc::new(AdmissionPause {
        started: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    *owner.before_admission.lock().unwrap() = Some(pause.clone());
    let lease = namespace.freeze_operation().await.unwrap();
    let publication = owner.publish_frozen(lease);
    tokio::pin!(publication);
    tokio::select! {result=&mut publication=>panic!("admission did not wait: {result:?}"),_=pause.started.notified()=>{}}
    owner.close();
    owner.drain().unwrap();
    assert!(owner.recovery.lock().unwrap().load().unwrap().is_none());
    pause.resume.notify_one();
    assert!(matches!(publication.await, Err(Error::Closed)));
    drop(namespace.freeze_operation().await.unwrap());
}
#[tokio::test]
async fn wrong_namespace_does_not_publish_or_poison() {
    let directory = Directory::new();
    let ns = namespace();
    let owner = owner(&directory, ns.clone());
    let other = namespace();
    assert!(matches!(
        owner
            .publish_frozen(other.freeze_operation().await.unwrap())
            .await,
        Err(Error::WrongNamespace)
    ));
    assert!(owner.recovery.lock().unwrap().load().unwrap().is_none());
    assert!(!owner.is_poisoned());
    let lease = ns.freeze_operation().await.unwrap();
    owner.publish_frozen(lease).await.unwrap();
    drop(ns.freeze_operation().await.unwrap());
}
#[tokio::test]
async fn partial_and_uncertain_publications_preserve_data_and_poison_mutation_admission() {
    for boundary in [
        crate::recovery::Boundary::Write,
        crate::recovery::Boundary::DirectorySync,
    ] {
        let directory = Directory::new();
        let ns = namespace();
        let model = Model::new(
            ns.clone(),
            Arc::new(Factory),
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            Limits::default(),
        )
        .await
        .unwrap();
        let owner = model.checkpoint_owner();
        let (handle, entry) = model
            .create(
                1,
                b"file",
                0o644,
                OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model.write(handle, 0, b"preserved").await.unwrap();
        model.release(handle).await.unwrap();
        let dir = model.opendir(1).await.unwrap();
        owner.recovery.lock().unwrap().test_fault(Some(boundary));
        assert!(model.fsync(dir, true).await.is_err());
        assert!(owner.is_poisoned());
        assert!(
            model
                .create(
                    1,
                    b"blocked",
                    0o644,
                    OpenFlags {
                        write: true,
                        append: false,
                        truncate: false
                    }
                )
                .await
                .is_err()
        );
        let file = ns
            .materialized_file(entry.entry.inode)
            .await
            .unwrap()
            .unwrap();
        let read = file.open(Access::Read).await.unwrap();
        assert_eq!(file.read(read, 0, 30).await.unwrap(), b"preserved");
        file.release(read).await.unwrap();
        assert!(owner.recovery.lock().unwrap().original_root().is_err());
        model.shutdown().await.unwrap();
    }
}
#[test]
fn admitted_publisher_retains_freeze_and_actual_storage_lease_after_runtime_drop() {
    use crate::working_storage::WorkingStorage;
    let directory = Directory::new();
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let storage_path = directory.0.join("storage");
    let instance = lore_revision::instance::InstanceId::generate();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ns = namespace();
    let storage = WorkingStorage::open(
        &storage_path,
        &mountpoint,
        [1; 16].into(),
        instance,
        Default::default(),
    )
    .unwrap();
    let session = storage.begin_session().unwrap();
    let factory = session.factory();
    let owner = CheckpointOwner::new(
        ns.clone(),
        factory.clone(),
        Recovery::open(storage.recovery_directory(), Default::default()).unwrap(),
        1 << 20,
    );
    let pause = Arc::new(PublishPause {
        started: tokio::sync::Notify::new(),
        resumed: Mutex::new(false),
        wake: Condvar::new(),
    });
    *owner.pause.lock().unwrap() = Some(pause.clone());
    runtime.block_on(async{let phase=ns.freeze_operation().await.unwrap();let publication=owner.publish_frozen(phase.pin());tokio::pin!(publication);tokio::select!{result=&mut publication=>panic!("publisher did not pause: {result:?}"),_=pause.started.notified()=>{}}drop(phase);assert!(ns.freeze_operation().await.is_err());});
    owner.close();
    let wait_owner = owner.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(wait_owner.drain());
    });
    drop(owner);
    drop(factory);
    drop(session);
    drop(storage);
    drop(runtime);
    assert!(matches!(
        WorkingStorage::open(
            &storage_path,
            &mountpoint,
            [1; 16].into(),
            instance,
            Default::default()
        ),
        Err(crate::working_storage::Error::Busy)
    ));
    assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
    pause.resume();
    receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        drop(ns.freeze_operation().await.unwrap());
    });
    drop(runtime);
    let start = std::time::Instant::now();
    let reopened = loop {
        match WorkingStorage::open(
            &storage_path,
            &mountpoint,
            [1; 16].into(),
            instance,
            Default::default(),
        ) {
            Ok(storage) => break storage,
            Err(crate::working_storage::Error::Busy)
                if start.elapsed() < Duration::from_secs(5) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("storage lease did not release: {error}"),
        }
    };
    let recovery = Recovery::open(reopened.recovery_directory(), Default::default()).unwrap();
    assert_eq!(recovery.original_root().unwrap(), Some(root()));
}

#[tokio::test]
async fn capture_limit_is_retryable_and_preserves_dirty_data() {
    let directory = Directory::new();
    let ns = namespace();
    let owner = owner(&directory, ns.clone());
    let low = CheckpointOwner::new(
        ns.clone(),
        Arc::new(Factory),
        Recovery::open(directory.0.join("low"), Default::default()).unwrap(),
        0,
    );
    let entry = ns
        .create_file(
            1,
            b"dirty",
            0o644,
            Arc::new(Store::default()),
            Default::default(),
        )
        .await
        .unwrap();
    let file = ns.materialized_file(entry.inode).await.unwrap().unwrap();
    let handle = file.open(Access::Write).await.unwrap();
    file.write(handle, 0, b"kept").await.unwrap();
    file.release(handle).await.unwrap();
    assert!(matches!(
        low.publish().await,
        Err(Error::Namespace(NamespaceError::Backend(
            core::Error::ResourceLimit
        )))
    ));
    assert!(!low.is_poisoned());
    assert!(low.recovery.lock().unwrap().load().unwrap().is_none());
    let handle = file.open(Access::Read).await.unwrap();
    assert_eq!(file.read(handle, 0, 20).await.unwrap(), b"kept");
    file.release(handle).await.unwrap();
    owner.publish().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn prepublication_permission_failure_is_retryable() {
    use std::os::unix::fs::PermissionsExt;
    // Root's DAC override cannot exercise a permission-denied open.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let directory = Directory::new();
    let owner = owner(&directory, namespace());
    let path = directory.0.join("recovery");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o0)).unwrap();
    let result = owner.publish().await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(
        result,
        Err(Error::Recovery(recovery::Error::Io(_)))
    ));
    assert!(!owner.is_poisoned());
    owner.publish().await.unwrap();
}

struct BlockingGate(tokio::sync::Notify);
impl AccessGate for BlockingGate {
    fn authorize<'a>(&'a self, _key: &'a NodeKey, _access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn closing_cancels_remote_capture_before_admission_without_acknowledgment() {
    let directory = Directory::new();
    let gate = Arc::new(BlockingGate(tokio::sync::Notify::new()));
    let ns =
        Arc::new(Namespace::new(Arc::new(Base), root(), gate.clone(), Default::default()).unwrap());
    let owner = owner(&directory, ns.clone());
    let lease = ns.freeze_operation().await.unwrap();
    let publication = owner.publish_frozen(lease);
    tokio::pin!(publication);
    tokio::select! {result=&mut publication=>panic!("capture did not block: {result:?}"),_=gate.0.notified()=>{}}
    owner.close();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), publication)
            .await
            .unwrap(),
        Err(Error::Closed)
    ));
    owner.drain().unwrap();
    assert!(owner.recovery.lock().unwrap().load().unwrap().is_none());
    drop(ns.freeze_operation().await.unwrap());
}

#[test]
fn retirement_guard_is_allocation_exact_and_drops_after_factory_and_recovery() {
    struct OrderedFactory(Arc<AtomicBool>);
    impl Drop for OrderedFactory {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    impl StoreFactory for OrderedFactory {
        fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
            Box::pin(async { Ok(Arc::new(Store::default()) as Arc<dyn SparseStore>) })
        }
    }
    struct Reservation {
        factory: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
        recovery: PathBuf,
    }
    impl Drop for Reservation {
        fn drop(&mut self) {
            assert!(self.factory.load(Ordering::Acquire));
            let recovery = Recovery::open(&self.recovery, Default::default())
                .expect("recovery lock drops before terminal reservation");
            drop(recovery);
            self.released.store(true, Ordering::Release);
        }
    }
    let directory = Directory::new();
    let factory = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let recovery = directory.0.join("recovery");
    let owner = CheckpointOwner::new(
        namespace(),
        Arc::new(OrderedFactory(factory.clone())),
        Recovery::open(&recovery, Default::default()).unwrap(),
        1 << 20,
    );
    let reservation = Arc::new(Reservation {
        factory,
        released: released.clone(),
        recovery,
    });
    owner.retain_retirement_guard(reservation.clone()).unwrap();
    owner.retain_retirement_guard(reservation.clone()).unwrap();
    assert!(matches!(
        owner.retain_retirement_guard(Arc::new(())),
        Err(Error::Core(core::Error::Busy))
    ));
    drop(reservation);
    assert!(!released.load(Ordering::Acquire));
    drop(owner);
    assert!(released.load(Ordering::Acquire));
}

#[cfg(unix)]
#[path = "tests/destruction.rs"]
mod destruction;

#[path = "source_publication/progress_tests.rs"]
mod source_progress_tests;
