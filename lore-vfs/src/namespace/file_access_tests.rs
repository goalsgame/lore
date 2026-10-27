// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::NamedNode;
use crate::linux_working::Model;
use crate::linux_working::OpenFlags;
use crate::overlay::Limits as FileLimits;
use crate::overlay::SparseStore;

#[derive(Default)]
pub(crate) struct Policy {
    pub(crate) denied: AtomicBool,
    pause_write: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl AccessGate for Policy {
    fn authorize<'a>(&'a self, _: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if access == Access::Write && self.pause_write.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if self.denied.load(Ordering::SeqCst) {
                Err(Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}
pub(crate) struct Reader {
    reads: AtomicUsize,
}
fn key(node: u32) -> NodeKey {
    NodeKey {
        repository: [51; 16].into(),
        revision: lore_base::types::Address::default(),
        node,
        exposure: 1,
    }
}
pub(crate) fn root() -> Node {
    Node {
        key: key(0),
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}
fn data() -> Node {
    Node {
        key: key(1),
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    }
}
impl Backend for Reader {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { panic!("namespace uses bounded list") })
    }
    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        _: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            assert_eq!(*directory, key(0));
            Ok(BackendPage {
                entries: if offset == 0 {
                    vec![NamedNode {
                        name: b"file".to_vec(),
                        node: data(),
                    }]
                } else {
                    Vec::new()
                },
                next_offset: None,
            })
        })
    }
    fn read<'a>(
        &'a self,
        file: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            assert_eq!(*file, key(1));
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(b"base"[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { panic!("lease reads never hydrate/hash or query digest") })
    }
}
#[derive(Default)]
struct Store(tokio::sync::Mutex<Vec<u8>>);
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            Ok(self.0.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut all = self.0.lock().await;
            all.resize(offset as usize + bytes.len(), 0);
            all[offset as usize..offset as usize + bytes.len()].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}
struct Factory;
impl crate::linux_working::StoreFactory for Factory {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async { Ok(Arc::new(Store::default()) as Arc<dyn SparseStore>) })
    }
}
pub(crate) struct Fixture {
    pub(crate) namespace: Arc<Namespace>,
    pub(crate) overlay: Arc<OverlayFile>,
    pub(crate) inode: u64,
    pub(crate) old: Arc<Policy>,
    pub(crate) current: Arc<Policy>,
    pub(crate) old_reader: Arc<Reader>,
    current_reader: Arc<Reader>,
    pub(crate) model: Option<Arc<Model>>,
    _directory: crate::digest_index::tests::Directory,
}
pub(crate) async fn fixture(model: bool) -> Fixture {
    let directory = crate::digest_index::tests::Directory::new();
    std::fs::create_dir(&directory.0).unwrap();
    let old = Arc::new(Policy::default());
    let current = Arc::new(Policy::default());
    let old_reader = Arc::new(Reader {
        reads: AtomicUsize::new(0),
    });
    let current_reader = Arc::new(Reader {
        reads: AtomicUsize::new(0),
    });
    let namespace = Arc::new(
        Namespace::new(
            current_reader.clone(),
            root(),
            current.clone(),
            Limits::default(),
        )
        .unwrap(),
    );
    let entry = namespace.lookup(1, b"file").await.unwrap();
    let budget = Arc::new(crate::storage_budget::StorageBudget::new(4096));
    let model = if model {
        Some(
            Model::new(
                namespace.clone(),
                Arc::new(Factory),
                budget.clone(),
                crate::recovery::Recovery::open(directory.0.join("journal"), Default::default())
                    .unwrap(),
                crate::linux_working::Limits {
                    max_handles: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let old_binding = Namespace::new(old_reader.clone(), root(), old.clone(), Limits::default())
        .unwrap()
        .current_binding()
        .unwrap();
    let receipt = namespace.state.lock().await.nodes[&entry.inode]
        .receipt
        .clone();
    let overlay = Arc::new(
        OverlayFile::new(
            data(),
            old_reader.clone(),
            old_binding.gate.clone(),
            Arc::new(crate::storage_budget::BudgetedStore::new(
                Arc::new(Store::default()),
                budget,
            )),
            FileLimits::default(),
        )
        .unwrap()
        .with_receipt_cell(receipt)
        .with_mutation_gate(namespace.mutation_gate.clone())
        .with_operation_freeze(namespace.operation_freeze.clone()),
    );
    // A private fixture represents an admitted unchanged cell with an older
    // default route. This is not a publication or resource-adoption API.
    namespace
        .state
        .lock()
        .await
        .nodes
        .get_mut(&entry.inode)
        .unwrap()
        .file = Some(overlay.clone());
    Fixture {
        namespace,
        overlay,
        inode: entry.inode,
        old,
        current,
        old_reader,
        current_reader,
        model,
        _directory: directory,
    }
}
fn flags(write: bool) -> OpenFlags {
    OpenFlags {
        write,
        append: false,
        truncate: false,
    }
}

#[tokio::test]
async fn handles_pin_independent_policy_and_base_readers_while_sharing_private_bytes() {
    let f = fixture(false).await;
    let old = f.overlay.open(Access::Read).await.unwrap();
    let captured = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    let new = f
        .overlay
        .open_captured(&captured, Access::Read)
        .await
        .unwrap();
    f.namespace.confirm_file_access(&captured).await.unwrap();
    assert_eq!(f.overlay.read(old, 0, 4).await.unwrap(), b"base");
    assert_eq!(f.overlay.read(new, 0, 4).await.unwrap(), b"base");
    assert_eq!(f.old_reader.reads.load(Ordering::SeqCst), 1);
    assert_eq!(f.current_reader.reads.load(Ordering::SeqCst), 1);
    let writer = f
        .overlay
        .open_captured(&captured, Access::Write)
        .await
        .unwrap();
    f.overlay.write(writer, 0, b"data").await.unwrap();
    f.overlay.release(writer).await.unwrap();
    f.old.denied.store(true, Ordering::SeqCst);
    assert_eq!(
        f.overlay.read(old, 0, 4).await.err(),
        Some(Error::PermissionDenied)
    );
    assert_eq!(f.overlay.read(new, 0, 4).await.unwrap(), b"data");
    assert_eq!(f.overlay.metadata_for_handle(new).await.unwrap().size, 4);
    f.old.denied.store(false, Ordering::SeqCst);
    f.current.denied.store(true, Ordering::SeqCst);
    assert_eq!(f.overlay.read(old, 0, 4).await.unwrap(), b"data");
    assert_eq!(
        f.overlay.read(new, 0, 4).await.err(),
        Some(Error::PermissionDenied)
    );
    assert_eq!(f.old_reader.reads.load(Ordering::SeqCst), 1);
    assert_eq!(f.current_reader.reads.load(Ordering::SeqCst), 1);
    f.overlay.release(old).await.unwrap();
    f.overlay.release(new).await.unwrap();
}

#[tokio::test]
async fn exact_file_and_namespace_identity_reject_foreign_capabilities_with_same_keys() {
    let a = fixture(false).await;
    let b = fixture(false).await;
    let captured = a
        .namespace
        .capture_file_access(a.inode, &a.overlay)
        .await
        .unwrap();
    assert!(
        b.namespace
            .capture_file_access(b.inode, &a.overlay)
            .await
            .is_err()
    );
    assert!(b.namespace.confirm_file_access(&captured).await.is_err());
    assert_eq!(
        b.overlay.open_captured(&captured, Access::Read).await.err(),
        Some(Error::PermissionDenied)
    );
    let foreign = OverlayFile::new(
        data(),
        a.old_reader.clone(),
        a.old.clone(),
        Arc::new(Store::default()),
        FileLimits::default(),
    )
    .unwrap()
    .with_receipt_cell(a.overlay.receipt_cell());
    assert_eq!(
        foreign.open_captured(&captured, Access::Read).await.err(),
        Some(Error::PermissionDenied)
    );
    let mut tampered = captured;
    tampered.binding = Arc::new(NamespaceBinding {
        backend: a.namespace.current_binding().unwrap().backend.clone(),
        gate: a.namespace.current_binding().unwrap().gate.clone(),
        policy: a.namespace.current_binding().unwrap().policy.clone(),
    });
    assert!(a.namespace.confirm_file_access(&tampered).await.is_err());
}

#[tokio::test]
async fn opened_route_retains_neither_namespace_nor_overlay_or_freeze_cycles() {
    let f = fixture(false).await;
    let captured = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    let handle = f
        .overlay
        .open_captured(&captured, Access::Read)
        .await
        .unwrap();
    f.namespace.confirm_file_access(&captured).await.unwrap();
    let namespace = Arc::downgrade(&f.namespace);
    let overlay = Arc::downgrade(&f.overlay);
    drop(captured);
    let Fixture {
        namespace: owner,
        overlay: file,
        ..
    } = f;
    drop(owner);
    assert!(namespace.upgrade().is_none());
    assert_eq!(file.read(handle, 0, 4).await.unwrap(), b"base");
    drop(file);
    assert!(overlay.upgrade().is_none());
}

#[tokio::test]
async fn model_append_chmod_and_responses_use_the_new_handle_route_without_old_fallback() {
    let f = fixture(true).await;
    let model = f.model.as_ref().unwrap();
    f.old.denied.store(true, Ordering::SeqCst);
    let handle = model
        .open(
            f.inode,
            OpenFlags {
                write: true,
                append: true,
                truncate: false,
            },
        )
        .await
        .unwrap();
    model.write(handle, 0, b"!").await.unwrap();
    let attributes = model
        .setattr(f.inode, Some(handle), None, Some(0o755))
        .await
        .unwrap();
    assert_eq!(attributes.size, 5);
    assert_eq!(attributes.mode, 0o755);
    assert_eq!(model.read(handle, 0, 5).await.unwrap(), b"base!");
    f.current.denied.store(true, Ordering::SeqCst);
    assert!(model.write(handle, 0, b"x").await.is_err());
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_open_rechecks_freeze_after_async_policy_without_mutating_metadata() {
    let f = fixture(false).await;
    let captured = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    let before = f.overlay.checkpoint_guard().await.metadata();
    f.current.pause_write.store(true, Ordering::SeqCst);
    let open = f.overlay.open_captured(&captured, Access::Write);
    tokio::pin!(open);
    tokio::select! { _ = f.current.entered.notified() => {}, result = &mut open => panic!("open did not block: {result:?}") }
    let mut freeze = Box::pin(f.namespace.freeze_operation());
    std::future::poll_fn(|cx| {
        assert!(matches!(
            std::future::Future::poll(freeze.as_mut(), cx),
            std::task::Poll::Pending
        ));
        std::task::Poll::Ready(())
    })
    .await;
    f.current.pause_write.store(false, Ordering::SeqCst);
    f.current.resume.notify_one();
    assert_eq!(open.await.err(), Some(Error::Busy));
    let lease = freeze.await.unwrap();
    assert_eq!(f.overlay.checkpoint_guard().await.metadata(), before);
    drop(lease);
}

#[tokio::test]
async fn existing_writer_rechecks_freeze_after_policy_before_appending() {
    let f = fixture(false).await;
    let captured = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    let writer = f
        .overlay
        .open_captured(&captured, Access::Write)
        .await
        .unwrap();
    let before = f.overlay.checkpoint_guard().await.metadata();
    f.current.pause_write.store(true, Ordering::SeqCst);
    let write = f.overlay.write(writer, 0, b"lost");
    tokio::pin!(write);
    tokio::select! { _ = f.current.entered.notified() => {}, result = &mut write => panic!("write did not block: {result:?}") }
    let mut freeze = Box::pin(f.namespace.freeze_operation());
    std::future::poll_fn(|cx| {
        assert!(matches!(
            std::future::Future::poll(freeze.as_mut(), cx),
            std::task::Poll::Pending
        ));
        std::task::Poll::Ready(())
    })
    .await;
    f.current.pause_write.store(false, Ordering::SeqCst);
    f.current.resume.notify_one();
    assert_eq!(write.await.err(), Some(Error::Busy));
    assert!(matches!(freeze.await, Err(NamespaceError::Busy)));
    assert_eq!(f.overlay.checkpoint_guard().await.metadata(), before);
    f.overlay.release(writer).await.unwrap();
}

async fn cleanup_releases_slot(f: &Fixture) {
    let model = f.model.as_ref().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match model.open(f.inode, flags(false)).await {
                Ok(handle) => {
                    model.release(handle).await.unwrap();
                    return;
                }
                Err(crate::linux_working::Error::HandleLimit) => tokio::task::yield_now().await,
                Err(error) => panic!("cleanup left unexpected open error: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(f.overlay.checkpoint_guard().await.metadata().writers, 0);
}

#[tokio::test]
async fn cancellation_after_inner_open_keeps_owned_cleanup_through_pending_confirmation() {
    let f = fixture(true).await;
    let model = f.model.as_ref().unwrap();
    f.current.pause_write.store(true, Ordering::SeqCst);
    let mut open = Box::pin(model.open(f.inode, flags(true)));
    tokio::select! { _ = f.current.entered.notified() => {}, result = &mut open => panic!("open did not block: {result:?}") }
    let locked = f.namespace.state.lock().await;
    f.current.pause_write.store(false, Ordering::SeqCst);
    f.current.resume.notify_one();
    std::future::poll_fn(|cx| {
        assert!(matches!(
            std::future::Future::poll(open.as_mut(), cx),
            std::task::Poll::Pending
        ));
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(f.overlay.checkpoint_guard().await.metadata().writers, 1);
    drop(open);
    drop(locked);
    cleanup_releases_slot(&f).await;
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_confirmation_releases_the_unpublished_inner_handle_and_slot() {
    let f = fixture(true).await;
    let model = f.model.as_ref().unwrap();
    f.current.pause_write.store(true, Ordering::SeqCst);
    let mut open = Box::pin(model.open(f.inode, flags(true)));
    tokio::select! { _ = f.current.entered.notified() => {}, result = &mut open => panic!("open did not block: {result:?}") }
    {
        let mut state = f.namespace.state.lock().await;
        // Private stale-frame fixture, not an installer. An identical backend
        // and policy in a different binding cannot confirm the old capture.
        let stale = Arc::new(NamespaceBinding {
            backend: f.namespace.current_binding().unwrap().backend.clone(),
            gate: f.namespace.current_binding().unwrap().gate.clone(),
            policy: f.namespace.current_binding().unwrap().policy.clone(),
        });
        state
            .nodes
            .get_mut(&1)
            .unwrap()
            .names
            .as_mut()
            .unwrap()
            .clear();
        detach(&mut state, f.inode, &stale);
    }
    f.current.pause_write.store(false, Ordering::SeqCst);
    f.current.resume.notify_one();
    assert!(matches!(
        open.await,
        Err(crate::linux_working::Error::Namespace(
            NamespaceError::Backend(Error::Stale)
        ))
    ));
    cleanup_releases_slot(&f).await;
    model.shutdown().await.unwrap();
}

struct UnderlayReader {
    inner: Arc<Reader>,
    target: NodeKey,
}
impl Backend for UnderlayReader {
    fn underlay_view<'a>(
        &'a self,
        scope: &'a NodeKey,
        head: &'a crate::core::ScopeHead,
    ) -> BackendFuture<'a, NodeKey> {
        Box::pin(async move {
            if *scope != key(0)
                || head.revision != self.target.revision
                || head.root != Some(self.target.node)
            {
                return Err(Error::Stale);
            }
            Ok(self.target)
        })
    }
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        self.inner.lookup(parent, name)
    }
    fn list<'a>(
        &'a self,
        dir: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        self.inner.list(dir, offset, limit)
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        self.inner.read(key, offset, size)
    }
    fn digest<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        self.inner.digest(key)
    }
}
struct TargetPolicy {
    denied: AtomicBool,
    target: NodeKey,
}
impl AccessGate for TargetPolicy {
    fn authorize<'a>(&'a self, key: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if *key == self.target && self.denied.load(Ordering::SeqCst) {
                Err(Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn captured_handle_uses_effective_underlay_gate_not_the_permissive_raw_source_policy() {
    let mut f = fixture(false).await;
    let target = NodeKey {
        revision: lore_base::types::Address::zero_context_hash(lore_base::types::Hash::from_u64(
            99,
        )),
        ..key(0)
    };
    let policy = Arc::new(TargetPolicy {
        denied: AtomicBool::new(true),
        target,
    });
    let backend: Arc<dyn Backend> = Arc::new(UnderlayReader {
        inner: f.current_reader.clone(),
        target,
    });
    let gate: Arc<dyn AccessGate> = Arc::new(BackendGate {
        underlays: BTreeMap::from([(
            (key(0).repository, key(0).exposure),
            (
                key(0),
                crate::core::ScopeHead {
                    revision: target.revision,
                    root: Some(target.node),
                },
            ),
        )]),
        backend: backend.clone(),
        gate: policy.clone(),
    });
    // Only route selection is under test; no view is installed or persisted.
    Arc::get_mut(&mut f.namespace).unwrap().routing =
        routing::Routing::new(Arc::new(NamespaceBinding {
            backend,
            gate,
            policy: policy.clone(),
        }));
    let access = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    assert!(policy.authorize(&key(1), Access::Read).await.is_ok());
    assert_eq!(
        f.overlay.open_captured(&access, Access::Read).await.err(),
        Some(Error::PermissionDenied)
    );
    policy.denied.store(false, Ordering::SeqCst);
    let handle = f
        .overlay
        .open_captured(&access, Access::Read)
        .await
        .unwrap();
    f.namespace.confirm_file_access(&access).await.unwrap();
    policy.denied.store(true, Ordering::SeqCst);
    assert_eq!(
        f.overlay.read(handle, 0, 4).await.err(),
        Some(Error::PermissionDenied)
    );
    f.overlay.release(handle).await.unwrap();
}
