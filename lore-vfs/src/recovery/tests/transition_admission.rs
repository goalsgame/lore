// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::capture_memory::Footprint;
use crate::linux_working::Model;
use crate::linux_working::StoreFactory;
use crate::storage_budget::StorageBudget;

fn ledger(snapshot: &NamespaceSnapshot) -> CaptureMemory {
    let mut memory = CaptureMemory::new(CaptureLimits {
        max_buffer_bytes: 1024 * 1024,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1024 * 1024,
    })
    .unwrap();
    memory
        .retain(Footprint::snapshot(snapshot).unwrap())
        .unwrap();
    memory
}
fn marker(directory: &Path, kind: u8) {
    let marker = directory.join("transition");
    match kind {
        0 => fs::create_dir(&marker).unwrap(),
        1..=4 => {
            fs::create_dir(&marker).unwrap();
            fs::write(
                marker.join(["", "source", "candidate", "intent.pending", "intent"][kind as usize]),
                b"partial or corrupt facts",
            )
            .unwrap();
        }
        5 => fs::write(&marker, b"not a directory").unwrap(),
        6 => symlink(directory.join("missing-target"), &marker).unwrap(),
        7 => {
            use std::os::unix::ffi::OsStrExt;
            let path = std::ffi::CString::new(marker.as_os_str().as_bytes()).unwrap();
            // SAFETY: the pathname is NUL-terminated and its storage lives for the call.
            assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        }
        _ => unreachable!(),
    }
}
fn inventory(directory: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}
#[test]
fn any_marker_object_blocks_open_before_lock_creation_without_following_or_repair() {
    for kind in 0..8 {
        let temp = Temp::new();
        let path = temp.0.join("journal");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        marker(&path, kind);
        let before = inventory(&path);
        assert!(matches!(
            Recovery::open(&path, Limits::default()),
            Err(Error::TransitionPending)
        ));
        assert_eq!(inventory(&path), before);
        assert!(!path.join("owner.lock").exists());
        assert!(!path.join("pending").exists());
        assert!(!path.join("missing-target").exists());
    }
}
#[test]
fn only_absence_is_clean_and_probe_never_creates_artifacts() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    assert!(crate::recovery::transition_admission::check(&path).is_ok());
    assert!(!path.exists());
    fs::create_dir(&path).unwrap();
    assert!(crate::recovery::transition_admission::check(&path).is_ok());
    assert!(inventory(&path).is_empty());
    let file = temp.0.join("file");
    fs::write(&file, b"not a directory").unwrap();
    assert!(matches!(
        crate::recovery::transition_admission::check(&file),
        Err(Error::Io(_))
    ));
}
#[test]
fn preopened_owner_preserves_readonly_facts_but_refuses_original_root_and_writes() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&image(1)).unwrap();
    let previous = fs::read(path.join("checkpoint")).unwrap();
    marker(&path, 4);
    assert!(matches!(
        recovery.original_root(),
        Err(Error::TransitionPending)
    ));
    assert!(matches!(
        recovery.publish(&image(2)),
        Err(Error::TransitionPending)
    ));
    let snapshot = image(2);
    assert!(matches!(
        recovery.publish_retained(snapshot.clone(), &mut ledger(&snapshot)),
        Err(Error::TransitionPending)
    ));
    let loaded = recovery
        .load_exact_budgeted(&mut ledger(&image(1)))
        .unwrap()
        .unwrap();
    assert_eq!(loaded.bytes(), previous);
    assert_eq!(recovery.load().unwrap().unwrap().generation, 1);
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
    assert_eq!(
        fs::read(path.join("transition/intent")).unwrap(),
        b"partial or corrupt facts"
    );
    assert!(!path.join("pending").exists());
}
struct FactorySpy(AtomicU64);
impl StoreFactory for FactorySpy {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(CoreError::Io)
        })
    }
}
#[tokio::test]
async fn actual_model_restore_refuses_marker_before_allocating_private_stores() {
    let temp = Temp::new();
    let backend = Arc::new(Empty);
    let gate = Arc::new(Gate(AtomicBool::new(true)));
    let namespace =
        Namespace::new(backend.clone(), root(), gate.clone(), Default::default()).unwrap();
    let store = Arc::new(
        FileStore::create(&temp.0.join("source-data"))
            .await
            .unwrap(),
    );
    let entry = namespace
        .create_file(1, b"file", 0o644, store.clone(), FileLimits::default())
        .await
        .unwrap();
    let file = namespace
        .file(entry.inode, store, FileLimits::default())
        .await
        .unwrap();
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"kept").await.unwrap();
    file.release(writer).await.unwrap();
    let snapshot = namespace.snapshot(4096).await.unwrap();
    assert_eq!(snapshot.files.len(), 1);
    let path = temp.0.join("journal");
    let mut recovery = Recovery::open(&path, Limits::default()).unwrap();
    recovery.publish(&snapshot).unwrap();
    let previous = fs::read(path.join("checkpoint")).unwrap();
    marker(&path, 1);
    let factory = Arc::new(FactorySpy(AtomicU64::new(0)));
    assert!(matches!(
        Model::restore(
            backend,
            root(),
            gate,
            Default::default(),
            factory.clone(),
            Arc::new(StorageBudget::new(4096)),
            recovery,
            Default::default()
        )
        .await,
        Err(crate::linux_working::Error::Recovery(
            Error::TransitionPending
        ))
    ));
    assert_eq!(factory.0.load(Ordering::SeqCst), 0);
    assert_eq!(fs::read(path.join("checkpoint")).unwrap(), previous);
    assert!(!path.join("pending").exists());
}

#[tokio::test]
async fn ordinary_publication_marker_error_poison_fences_the_actual_model_owner() {
    let temp = Temp::new();
    let path = temp.0.join("journal");
    let recovery = Recovery::open(&path, Limits::default()).unwrap();
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Empty),
            root(),
            Arc::new(Gate(AtomicBool::new(true))),
            Default::default(),
        )
        .unwrap(),
    );
    let factory = Arc::new(FactorySpy(AtomicU64::new(0)));
    let model = Model::new(
        namespace,
        factory,
        Arc::new(StorageBudget::new(4096)),
        recovery,
        Default::default(),
    )
    .await
    .unwrap();
    let directory = model.opendir(1).await.unwrap();
    marker(&path, 0);
    // request() checks owner liveness again after the failed publication.
    assert!(matches!(
        model.fsync(directory, true).await,
        Err(crate::linux_working::Error::Poisoned)
    ));
    assert!(model.checkpoint_owner().is_poisoned());
    assert!(model.getattr(1).await.is_err());
    assert!(!path.join("pending").exists());
    model.release(directory).await.unwrap();
    model.shutdown().await.unwrap();
}
