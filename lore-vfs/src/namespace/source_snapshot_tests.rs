// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::*;
use crate::core::BackendFuture;
use crate::namespace::file_access::tests::Policy;
use crate::namespace::file_access::tests::fixture;

#[derive(Default)]
struct Pause {
    block: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl Pause {
    async fn wait(&self) {
        if self.block.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
    }
}
struct Gate {
    policy: Arc<Policy>,
    pause: Arc<Pause>,
}
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.pause.wait().await;
            self.policy.authorize(key, access).await
        })
    }
}
fn paused_binding(namespace: &mut Arc<Namespace>, policy: Arc<Policy>) -> Arc<Pause> {
    let pause = Arc::new(Pause::default());
    let ns = Arc::get_mut(namespace).unwrap();
    let old = ns.current_binding().unwrap();
    // Private fixed-binding fixture, not an activation or policy replacement API.
    ns.routing = super::super::routing::Routing::new(Arc::new(NamespaceBinding {
        backend: old.backend.clone(),
        gate: Arc::new(Gate {
            policy: policy.clone(),
            pause: pause.clone(),
        }),
        policy,
    }));
    pause
}
#[derive(Default)]
struct Store {
    bytes: Mutex<Vec<u8>>,
    pause: Pause,
}
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.pause.wait().await;
            Ok(self.bytes.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut all = self.bytes.lock().await;
            all.resize(offset as usize + bytes.len(), 0);
            all[offset as usize..offset as usize + bytes.len()].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}

#[tokio::test]
async fn blocked_policy_releases_namespace_and_header_keeps_its_coherent_names() {
    let mut f = fixture(false).await;
    let pause = paused_binding(&mut f.namespace, f.current.clone());
    f.old.denied.store(true, Ordering::SeqCst);
    pause.block.store(true, Ordering::SeqCst);
    let mut capture = Box::pin(f.namespace.snapshot(4096));
    tokio::select! { result = &mut capture => panic!("unexpected {result:?}"), () = pause.entered.notified() => {} }
    assert!(f.namespace.state.try_lock().is_ok());
    f.namespace.rename(1, b"file", 1, b"new").await.unwrap();
    pause.resume.notify_one();
    let snapshot = capture.await.unwrap();
    assert_eq!(
        snapshot
            .nodes
            .iter()
            .find(|node| node.entry.inode == 1)
            .unwrap()
            .names
            .as_ref()
            .unwrap()[0]
            .0,
        b"file"
    );
    assert_eq!(f.namespace.lookup(1, b"new").await.unwrap().inode, f.inode);
    assert_eq!(snapshot.files[&f.inode].metadata.writers, 0);
}

#[tokio::test]
async fn cancellation_or_revocation_drops_all_guards_without_namespace_reacquisition() {
    let mut f = fixture(false).await;
    let pause = paused_binding(&mut f.namespace, f.current.clone());
    pause.block.store(true, Ordering::SeqCst);
    let mut capture = Box::pin(f.namespace.snapshot(4096));
    tokio::select! { result = &mut capture => panic!("unexpected {result:?}"), () = pause.entered.notified() => {} }
    drop(capture);
    let writer = f.overlay.open(Access::Write).await.unwrap();
    f.overlay.release(writer).await.unwrap();
    pause.block.store(true, Ordering::SeqCst);
    let mut capture = Box::pin(f.namespace.snapshot(4096));
    tokio::select! { result = &mut capture => panic!("unexpected {result:?}"), () = pause.entered.notified() => {} }
    f.current.denied.store(true, Ordering::SeqCst);
    pause.resume.notify_one();
    assert!(capture.await.is_err());
    f.current.denied.store(false, Ordering::SeqCst);
    assert!(f.namespace.freeze_operation().await.is_ok());
}

#[tokio::test]
async fn private_store_wait_keeps_namespace_available_and_restores_exact_bytes() {
    let f = fixture(false).await;
    let store = Arc::new(Store::default());
    let entry = f
        .namespace
        .create_file(1, b"local", 0o644, store.clone(), FileLimits::default())
        .await
        .unwrap();
    let file = f
        .namespace
        .materialized_file(entry.inode)
        .await
        .unwrap()
        .unwrap();
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"private").await.unwrap();
    // Keeping the writer open is valid for a normal source checkpoint/fsync.
    store.pause.block.store(true, Ordering::SeqCst);
    let mut capture = Box::pin(f.namespace.snapshot(4096));
    tokio::select! { result = &mut capture => panic!("unexpected {result:?}"), () = store.pause.entered.notified() => {} }
    assert!(f.namespace.state.try_lock().is_ok());
    f.namespace.create_directory(1, b"later").await.unwrap();
    let mut later_write = Box::pin(file.write(writer, 0, b"changed"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut later_write)
            .await
            .is_err()
    );
    store.pause.resume.notify_one();
    let snapshot = capture.await.unwrap();
    later_write.await.unwrap();
    assert_eq!(snapshot.files[&entry.inode].metadata.writers, 0);
    assert!(!snapshot.nodes.iter().any(|node| {
        node.names
            .as_ref()
            .is_some_and(|names| names.iter().any(|(name, _)| name == b"later"))
    }));
    file.release(writer).await.unwrap();
    let backend = f.namespace.backend().unwrap();
    let stores = snapshot
        .files
        .keys()
        .map(|inode| (*inode, Arc::new(Store::default()) as Arc<dyn SparseStore>))
        .collect();
    let restored = Arc::new(
        Namespace::restore(
            backend,
            snapshot.original_root.clone(),
            f.current.clone(),
            snapshot,
            stores,
            Limits::default(),
            FileLimits::default(),
        )
        .await
        .unwrap(),
    );
    let freeze = restored.freeze_operation().await.unwrap();
    assert_eq!(
        restored
            .read_frozen(freeze.token(), entry.inode, 0, 7)
            .await
            .unwrap(),
        b"private"
    );
}

#[tokio::test]
async fn normal_model_fsync_with_writer_uses_current_policy_not_old_overlay_default() {
    let f = fixture(true).await;
    let model = f.model.unwrap();
    let handle = model
        .open(
            f.inode,
            crate::linux_working::OpenFlags {
                write: true,
                append: false,
                truncate: false,
            },
        )
        .await
        .unwrap();
    model.write(handle, 0, b"edit").await.unwrap();
    f.old.denied.store(true, Ordering::SeqCst);
    model.fsync(handle, false).await.unwrap();
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn transition_epoch_is_header_epoch_and_changes_are_rejected() {
    let mut f = fixture(false).await;
    let pause = paused_binding(&mut f.namespace, f.current.clone());
    let freeze = f.namespace.freeze_operation().await.unwrap();
    let before = f.namespace.mutation_epoch().unwrap();
    pause.block.store(true, Ordering::SeqCst);
    let mut capture = Box::pin(f.namespace.capture_source_snapshot(
        4096,
        Some(freeze.token()),
        None,
    ));
    tokio::select! { result = &mut capture => panic!("unexpected {result:?}"), () = pause.entered.notified() => {} }
    {
        let _mutation = f.namespace.mutation_gate.mutation().unwrap();
    }
    pause.resume.notify_one();
    assert!(matches!(
        capture.await,
        Err(NamespaceError::Backend(Error::Stale))
    ));
    assert_ne!(before, f.namespace.mutation_epoch().unwrap());
    drop(freeze);
    assert!(f.namespace.freeze_operation().await.is_ok());
}

#[tokio::test]
async fn detached_capture_uses_retired_gate_even_when_current_gate_allows() {
    let mut f = fixture(false).await;
    f.namespace.remove(1, b"file", false).await.unwrap();
    let ns = Arc::get_mut(&mut f.namespace).unwrap();
    let old = ns.current_binding().unwrap();
    let allowed = Arc::new(Policy::default());
    ns.routing = super::super::routing::Routing::new(Arc::new(NamespaceBinding {
        backend: old.backend.clone(),
        gate: allowed.clone(),
        policy: allowed,
    }));
    f.current.denied.store(true, Ordering::SeqCst);
    assert!(f.namespace.snapshot(4096).await.is_err());
    f.current.denied.store(false, Ordering::SeqCst);
    f.old.denied.store(true, Ordering::SeqCst);
    assert!(f.namespace.snapshot(4096).await.is_ok());
}

#[tokio::test]
async fn transition_capture_keeps_the_callers_consumed_budget() {
    let f = fixture(false).await;
    let freeze = f.namespace.freeze_operation().await.unwrap();
    let limits = crate::transition_prepare::Limits {
        max_path_bytes: 1024,
        ..Default::default()
    };
    let mut budget = Budget::new(limits).unwrap();
    budget.paths(1024).unwrap();
    assert!(matches!(
        f.namespace
            .capture_source_snapshot(4096, Some(freeze.token()), Some(&mut budget))
            .await,
        Err(NamespaceError::Backend(Error::ResourceLimit))
    ));
    drop(freeze);
    assert!(f.namespace.freeze_operation().await.is_ok());
}
