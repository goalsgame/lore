// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use super::*;
use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::NamedNode;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::core::{self};
use crate::namespace::Namespace;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::SparseStore;
use crate::recovery::Recovery;
use crate::storage_budget::StorageBudget;

pub(super) fn root() -> Node {
    Node {
        key: NodeKey {
            repository: [1; 16].into(),
            revision: Default::default(),
            node: 0,
            exposure: 1,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}
pub(super) struct Base {
    pub block: AtomicBool,
    pub started: tokio::sync::Notify,
}
impl Base {
    pub fn new() -> Self {
        Self {
            block: AtomicBool::new(false),
            started: tokio::sync::Notify::new(),
        }
    }
    fn file() -> Node {
        Node {
            key: NodeKey {
                node: 1,
                ..root().key
            },
            kind: NodeKind::File,
            size: 4,
            mode: 0o444,
        }
    }
}
impl Backend for Base {
    fn lookup<'a>(&'a self, _key: &'a NodeKey, _name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(core::Error::Unsupported) })
    }
    fn list<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        _limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            Ok(BackendPage {
                entries: if key.node == 0 && offset == 0 {
                    vec![NamedNode {
                        name: b"base".to_vec(),
                        node: Self::file(),
                    }]
                } else {
                    vec![]
                },
                next_offset: None,
            })
        })
    }
    fn read<'a>(
        &'a self,
        _key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            if self.block.load(Ordering::Acquire) {
                self.started.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(b"base"[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, _key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Ok(Some([42; 32])) })
    }
}
#[derive(Default)]
pub(super) struct Gate {
    pub deny_write: AtomicBool,
    pub deny_read: AtomicBool,
}
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.deny_read.load(Ordering::Acquire)
                || (access == Access::Write && self.deny_write.load(Ordering::Acquire))
            {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
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
#[derive(Default)]
pub(super) struct Stores {
    pub fail: AtomicBool,
    pub count: AtomicU64,
}
impl StoreFactory for Stores {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async move {
            self.count.fetch_add(1, Ordering::AcqRel);
            if self.fail.load(Ordering::Acquire) {
                Err(core::Error::Io)
            } else {
                Ok(Arc::new(Store::default()) as Arc<dyn SparseStore>)
            }
        })
    }
}
pub(super) struct Directory(pub PathBuf);
impl Directory {
    pub fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-working-{label}-{}-{}",
            std::process::id(),
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
pub(super) const WRITE: OpenFlags = OpenFlags {
    write: true,
    append: false,
    truncate: false,
};
pub(super) const READ: OpenFlags = OpenFlags {
    write: false,
    append: false,
    truncate: false,
};
pub(super) async fn fixture(
    directory: &Directory,
    limits: Limits,
    budget: u64,
) -> (Arc<Model>, Arc<Base>, Arc<Gate>, Arc<Stores>) {
    let base = Arc::new(Base::new());
    let gate = Arc::new(Gate::default());
    let stores = Arc::new(Stores::default());
    let namespace =
        Arc::new(Namespace::new(base.clone(), root(), gate.clone(), Default::default()).unwrap());
    let model = Model::new(
        namespace,
        stores.clone(),
        Arc::new(StorageBudget::new(budget)),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        limits,
    )
    .await
    .unwrap();
    (model, base, gate, stores)
}

#[tokio::test]
async fn retirement_checkpoint_drains_writers_and_restores_unfsynced_dirty_bytes() {
    let directory = Directory::new("retirement");
    let (model, base, gate, _stores) = fixture(&directory, Default::default(), 1024).await;
    let (writer, entry) = model.create(1, b"dirty", 0o644, WRITE).await.unwrap();
    model
        .write(writer, 0, b"saved at retirement")
        .await
        .unwrap();
    assert!(matches!(
        model.checkpoint_for_retirement().await,
        Err(Error::Core(core::Error::Busy))
    ));
    assert!(model.checkpoint_owner().test_snapshot().unwrap().is_none());
    model.close_requests();
    model.checkpoint_for_retirement().await.unwrap();
    model.checkpoint_owner().available().unwrap();
    let file = model
        .namespace()
        .materialized_file(entry.entry.inode)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.metadata().await.unwrap().writers, 0);
    drop(file);
    assert!(model.read(writer, 0, 100).await.is_err());
    model.shutdown().await.unwrap();
    drop(model);
    let restored = Model::restore(
        base,
        root(),
        gate,
        Default::default(),
        Arc::new(Stores::default()),
        Arc::new(StorageBudget::new(1024)),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let reader = restored.open(entry.entry.inode, READ).await.unwrap();
    assert_eq!(
        restored.read(reader, 0, 100).await.unwrap(),
        b"saved at retirement"
    );
    assert_eq!(restored.digest(entry.entry.inode).await.unwrap(), None);
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn retirement_publication_failure_never_acknowledges_or_discards_dirty_data() {
    for boundary in [
        crate::recovery::Boundary::Write,
        crate::recovery::Boundary::DirectorySync,
    ] {
        let directory = Directory::new("retirement-failed");
        let (model, _base, _gate, _stores) = fixture(&directory, Default::default(), 1024).await;
        let (writer, entry) = model.create(1, b"dirty", 0o644, WRITE).await.unwrap();
        model
            .write(writer, 0, b"retained after failure")
            .await
            .unwrap();
        let file = model
            .namespace()
            .materialized_file(entry.entry.inode)
            .await
            .unwrap()
            .unwrap();
        model.checkpoint_owner().test_fault(Some(boundary));
        model.close_requests();
        assert!(model.checkpoint_for_retirement().await.is_err());
        assert!(model.checkpoint_owner().is_poisoned());
        let reader = file.open(Access::Read).await.unwrap();
        assert_eq!(
            file.read(reader, 0, 100).await.unwrap(),
            b"retained after failure"
        );
        file.release(reader).await.unwrap();
        drop(model.namespace().freeze_operation().await.unwrap());
        model.shutdown().await.unwrap();
        assert!(model.checkpoint_owner().is_poisoned());
    }
}

#[tokio::test]
async fn retained_handles_and_coherent_checkpoint_restore() {
    let directory = Directory::new("restart");
    let (model, base, gate, stores) = fixture(&directory, Default::default(), 1024).await;
    let (a, aa) = model.create(1, b"a", 0o644, WRITE).await.unwrap();
    model.write(a, 0, b"old").await.unwrap();
    let (b, bb) = model.create(1, b"b", 0o644, WRITE).await.unwrap();
    model.write(b, 0, b"new").await.unwrap();
    model.rename(1, b"a", 1, b"b").await.unwrap();
    assert_eq!(
        model.lookup(1, b"b").await.unwrap().entry.inode,
        aa.entry.inode
    );
    assert_eq!(model.read(b, 0, 10).await.unwrap(), b"new");
    assert!(!model.getattr(bb.entry.inode).await.unwrap().linked);
    model.remove(1, b"b", false).await.unwrap();
    assert_eq!(model.read(a, 0, 10).await.unwrap(), b"old");
    let (c, cc) = model.create(1, b"c", 0o644, WRITE).await.unwrap();
    model.write(c, 0, b"hello").await.unwrap();
    model
        .setattr(cc.entry.inode, Some(c), Some(3), Some(0o755))
        .await
        .unwrap();
    assert_eq!(model.getattr(cc.entry.inode).await.unwrap().mode, 0o755);
    assert!(model.namespace().freeze_operation().await.is_err());
    model.fsync(c, false).await.unwrap();
    assert!(model.budget().reserved_bytes() >= 11);
    model.shutdown().await.unwrap();
    drop(model);
    let restored = Model::restore(
        base,
        root(),
        gate,
        Default::default(),
        stores,
        Arc::new(StorageBudget::new(1024)),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let entry = restored.lookup(1, b"c").await.unwrap();
    assert_eq!(entry.entry.inode, cc.entry.inode);
    assert_eq!(entry.mode, 0o755);
    let handle = restored.open(entry.entry.inode, READ).await.unwrap();
    assert_eq!(restored.read(handle, 0, 10).await.unwrap(), &b"hello"[..3]);
    assert_eq!(restored.budget().reserved_bytes(), 11);
    assert!(restored.lookup(1, b"a").await.is_err());
    assert!(restored.lookup(1, b"b").await.is_err());
    restored.shutdown().await.unwrap();
}
#[tokio::test]
async fn independent_write_gate_and_freeze() {
    let directory = Directory::new("gate");
    let (model, _base, gate, _stores) = fixture(&directory, Default::default(), 1024).await;
    let entry = model.lookup(1, b"base").await.unwrap();
    assert_eq!(entry.mode, 0o644);
    gate.deny_write.store(true, Ordering::Release);
    let read = model.open(entry.entry.inode, READ).await.unwrap();
    assert_eq!(model.read(read, 0, 4).await.unwrap(), b"base");
    assert!(matches!(
        model.open(entry.entry.inode, WRITE).await,
        Err(Error::Core(core::Error::PermissionDenied))
    ));
    gate.deny_write.store(false, Ordering::Release);
    let freeze = model.namespace().freeze_operation().await.unwrap();
    assert!(matches!(
        model.open(entry.entry.inode, WRITE).await,
        Err(Error::Core(core::Error::Busy))
    ));
    drop(freeze);
    let write = model.open(entry.entry.inode, WRITE).await.unwrap();
    gate.deny_write.store(true, Ordering::Release);
    assert!(model.write(write, 0, b"nope").await.is_err());
    gate.deny_read.store(true, Ordering::Release);
    assert!(model.read(read, 0, 4).await.is_err());
    model.release(write).await.unwrap();
    model.release(read).await.unwrap();
    model.shutdown().await.unwrap();
}
#[tokio::test]
async fn global_limits_and_stable_directory_cookies() {
    let directory = Directory::new("bounds");
    let limits = Limits {
        max_handles: 2,
        max_directory_entries: 2,
        max_retained_directory_entries: 4,
        max_stores: 2,
        ..Default::default()
    };
    let (model, _base, _gate, stores) = fixture(&directory, limits, 5).await;
    let dir = model.opendir(1).await.unwrap();
    let first = model.readdir(dir, 0, 2).await.unwrap();
    let cookie = first.entries.last().unwrap().0;
    assert!(matches!(model.opendir(1).await, Err(Error::SnapshotLimit)));
    let (file, _) = model.create(1, b"new", 0o644, WRITE).await.unwrap();
    model.write(file, 0, b"12345").await.unwrap();
    assert!(matches!(
        model.write(file, 0, b"6").await,
        Err(Error::Core(core::Error::ResourceLimit))
    ));
    assert!(matches!(model.open(2, READ).await, Err(Error::HandleLimit)));
    let remaining = model.readdir(dir, cookie, 10).await.unwrap();
    assert_eq!(remaining.entries.len(), 1);
    assert_eq!(remaining.entries[0].1.name, b"base");
    model.release(dir).await.unwrap();
    model.release(file).await.unwrap();
    let dir = model.opendir(1).await.unwrap();
    assert!(model.readdir(dir, cookie, 10).await.is_err());
    model.release(dir).await.unwrap();
    stores.fail.store(true, Ordering::Release);
    assert!(model.open(2, READ).await.is_err());
    assert!(matches!(model.open(2, READ).await, Err(Error::StoreLimit)));
    assert_eq!(stores.count.load(Ordering::Acquire), 2);
    model.shutdown().await.unwrap();
}
#[tokio::test]
async fn teardown_cancels_backend_without_waiting_for_resume() {
    let directory = Directory::new("cancel");
    let (model, base, _gate, _stores) = fixture(&directory, Default::default(), 1024).await;
    let entry = model.lookup(1, b"base").await.unwrap();
    let file = model.open(entry.entry.inode, WRITE).await.unwrap();
    base.block.store(true, Ordering::Release);
    let read = model.read(file, 0, 4);
    tokio::pin!(read);
    tokio::select! {_=&mut read=>panic!("blocked backend returned"),_=base.started.notified()=>{}}
    model.stop();
    assert!(matches!(
        read.await,
        Err(Error::Core(core::Error::Unmounted))
    ));
    tokio::time::timeout(Duration::from_secs(1), model.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        model
            .namespace()
            .materialized_file(entry.entry.inode)
            .await
            .unwrap()
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .writers,
        0
    );
}
#[test]
fn shutdown_drains_cleanup_after_original_runtime_stops() {
    let directory = Directory::new("cleanup");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (model, inode) = runtime.block_on(async {
        let (model, _base, _gate, _stores) = fixture(&directory, Default::default(), 1024).await;
        let (handle, entry) = model.create(1, b"a", 0o644, WRITE).await.unwrap();
        let handle = model.state.lock().unwrap().handles.remove(&handle).unwrap();
        drop(handle);
        (model, entry.entry.inode)
    });
    drop(runtime);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        model.shutdown().await.unwrap();
        assert_eq!(
            model
                .namespace()
                .materialized_file(inode)
                .await
                .unwrap()
                .unwrap()
                .metadata()
                .await
                .unwrap()
                .writers,
            0
        );
        drop(model.namespace().freeze_operation().await.unwrap());
    });
}

#[tokio::test]
async fn invalid_inode_does_not_retain_cache_and_invalid_mode_never_truncates() {
    let directory = Directory::new("validation");
    let (model, _base, _gate, stores) = fixture(&directory, Default::default(), 1024).await;
    for inode in 1000..2000 {
        assert!(model.open(inode, READ).await.is_err());
    }
    assert!(model.files.lock().await.is_empty());
    assert_eq!(stores.count.load(Ordering::Acquire), 0);
    let (handle, entry) = model.create(1, b"file", 0o644, WRITE).await.unwrap();
    model.write(handle, 0, b"preserved").await.unwrap();
    assert!(matches!(
        model
            .setattr(entry.entry.inode, Some(handle), Some(0), Some(0o600))
            .await,
        Err(Error::Core(core::Error::Unsupported))
    ));
    assert_eq!(model.read(handle, 0, 16).await.unwrap(), b"preserved");
    model.shutdown().await.unwrap();
}

#[cfg(target_os = "linux")]
#[path = "mounted.rs"]
mod mounted;

#[tokio::test]
async fn checkpoint_limit_preserves_acknowledged_and_live_edits() {
    let directory = Directory::new("checkpoint-limit");
    let limits = Limits {
        max_checkpoint_data_bytes: 4,
        ..Default::default()
    };
    let (model, base, gate, stores) = fixture(&directory, limits, 1024).await;
    let (handle, entry) = model.create(1, b"file", 0o644, WRITE).await.unwrap();
    model.write(handle, 0, b"safe").await.unwrap();
    model.fsync(handle, false).await.unwrap();
    model.write(handle, 4, b"extra").await.unwrap();
    assert!(matches!(
        model.fsync(handle, false).await,
        Err(Error::Namespace(crate::namespace::NamespaceError::Backend(
            core::Error::ResourceLimit
        )))
    ));
    assert_eq!(model.read(handle, 0, 20).await.unwrap(), b"safeextra");
    model.shutdown().await.unwrap();
    drop(model);
    let restored = Model::restore(
        base,
        root(),
        gate,
        Default::default(),
        stores,
        Arc::new(StorageBudget::new(1024)),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        limits,
    )
    .await
    .unwrap();
    let handle = restored.open(entry.entry.inode, READ).await.unwrap();
    assert_eq!(restored.read(handle, 0, 20).await.unwrap(), b"safe");
    restored.shutdown().await.unwrap();
}
#[tokio::test]
async fn pending_and_corrupt_recovery_are_preserved() {
    let directory = Directory::new("bad-recovery");
    let (model, base, gate, stores) = fixture(&directory, Default::default(), 1024).await;
    let (handle, _entry) = model.create(1, b"file", 0o644, WRITE).await.unwrap();
    model.write(handle, 0, b"durable").await.unwrap();
    model.fsync(handle, false).await.unwrap();
    model.shutdown().await.unwrap();
    drop(model);
    let recovery_path = directory.0.join("recovery");
    let checkpoint = std::fs::read(recovery_path.join("checkpoint")).unwrap();
    std::fs::write(recovery_path.join("pending"), b"do not discard").unwrap();
    let result = Model::restore(
        base.clone(),
        root(),
        gate.clone(),
        Default::default(),
        stores.clone(),
        Arc::new(StorageBudget::new(1024)),
        Recovery::open(&recovery_path, Default::default()).unwrap(),
        Default::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Recovery(crate::recovery::Error::Pending))
    ));
    assert_eq!(
        std::fs::read(recovery_path.join("pending")).unwrap(),
        b"do not discard"
    );
    assert_eq!(
        std::fs::read(recovery_path.join("checkpoint")).unwrap(),
        checkpoint
    );
    std::fs::remove_file(recovery_path.join("pending")).unwrap();
    let mut corrupt = checkpoint;
    let last = corrupt.last_mut().unwrap();
    *last ^= 1;
    std::fs::write(recovery_path.join("checkpoint"), &corrupt).unwrap();
    let result = Model::restore(
        base,
        root(),
        gate,
        Default::default(),
        stores,
        Arc::new(StorageBudget::new(1024)),
        Recovery::open(&recovery_path, Default::default()).unwrap(),
        Default::default(),
    )
    .await;
    assert!(matches!(
        result,
        Err(Error::Recovery(crate::recovery::Error::Corrupt))
    ));
    assert_eq!(
        std::fs::read(recovery_path.join("checkpoint")).unwrap(),
        corrupt
    );
}
#[tokio::test]
async fn concurrent_name_edits_preserve_link_identity() {
    let directory = Directory::new("namespace-race");
    let (model, _base, _gate, _stores) = fixture(&directory, Default::default(), 1 << 20).await;
    for index in 0..30 {
        let a = format!("a{index}");
        let b = format!("b{index}");
        let (handle, _) = model.create(1, a.as_bytes(), 0o644, WRITE).await.unwrap();
        model.release(handle).await.unwrap();
        let (handle, _) = model.create(1, b.as_bytes(), 0o644, WRITE).await.unwrap();
        model.release(handle).await.unwrap();
        let (_renamed, _removed) = tokio::join!(
            model.rename(1, a.as_bytes(), 1, b.as_bytes()),
            model.remove(1, b.as_bytes(), false)
        );
    }
    let snapshot = model.namespace().snapshot(1 << 20).await.unwrap();
    let expected: std::collections::BTreeSet<_> = snapshot
        .nodes
        .iter()
        .filter(|node| !node.attached)
        .map(|node| node.entry.inode)
        .collect();
    assert_eq!(model.state.lock().unwrap().unlinked, expected);
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn pinned_directory_rechecks_revocation_after_open_and_remove() {
    let directory = Directory::new("pinned-directory-policy");
    let (model, _base, gate, _stores) = fixture(&directory, Default::default(), 1024).await;
    let child = model.mkdir(1, b"empty").await.unwrap();
    let handle = model.opendir(child.entry.inode).await.unwrap();
    model.remove(1, b"empty", true).await.unwrap();
    assert!(model.opendir(child.entry.inode).await.is_err());
    let page = model.readdir(handle, 0, 2).await.unwrap();
    assert_eq!(page.entries[0].1.name, b".");
    assert_eq!(page.entries[1].1.name, b"..");
    assert_eq!(page.entries[0].1.entry.inode, child.entry.inode);
    gate.deny_read.store(true, Ordering::Release);
    assert!(matches!(
        model.readdir(handle, 0, 2).await,
        Err(Error::Namespace(namespace::NamespaceError::Backend(
            core::Error::PermissionDenied
        )))
    ));
    gate.deny_read.store(false, Ordering::Release);
    assert!(model.readdir(handle, 0, 2).await.unwrap().eof);
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn pinned_directory_retains_names_and_cookies_after_rename_and_unlink() {
    let directory = Directory::new("pinned-directory-names");
    let (model, _base, _gate, _stores) = fixture(&directory, Default::default(), 1024).await;
    let handle = model.opendir(1).await.unwrap();
    let first = model.readdir(handle, 0, 2).await.unwrap();
    let cookie = first.entries.last().unwrap().0;
    model.rename(1, b"base", 1, b"renamed").await.unwrap();
    model.remove(1, b"renamed", false).await.unwrap();
    assert!(model.lookup(1, b"base").await.is_err());
    assert!(model.lookup(1, b"renamed").await.is_err());
    let saved = model.readdir(handle, cookie, 2).await.unwrap();
    assert_eq!(saved.entries.len(), 1);
    assert_eq!(saved.entries[0].1.name, b"base");
    assert!(saved.eof);
    assert!(matches!(
        model.readdir(handle, cookie, 0).await,
        Err(Error::Core(core::Error::Range))
    ));
    assert!(matches!(
        model.readdir(handle, (handle << 32) | 99, 1).await,
        Err(Error::Core(core::Error::Stale))
    ));
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn pinned_directory_rechecks_each_emitted_child_policy() {
    #[derive(Default)]
    struct ChildPolicy(Mutex<Option<NodeKey>>);
    impl AccessGate for ChildPolicy {
        fn authorize<'a>(&'a self, key: &'a NodeKey, _access: Access) -> BackendFuture<'a, ()> {
            Box::pin(async move {
                if self.0.lock().unwrap().as_ref() == Some(key) {
                    Err(core::Error::PermissionDenied)
                } else {
                    Ok(())
                }
            })
        }
    }
    let directory = Directory::new("pinned-directory-child-policy");
    let policy = Arc::new(ChildPolicy::default());
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Base::new()),
            root(),
            policy.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let model = Model::new(
        namespace,
        Arc::new(Stores::default()),
        Arc::new(StorageBudget::new(1024)),
        Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
        Default::default(),
    )
    .await
    .unwrap();
    let handle = model.opendir(1).await.unwrap();
    let first = model.readdir(handle, 0, 2).await.unwrap();
    let cookie = first.entries.last().unwrap().0;
    *policy.0.lock().unwrap() = Some(Base::file().key);
    assert!(matches!(
        model.readdir(handle, cookie, 1).await,
        Err(Error::Namespace(namespace::NamespaceError::Backend(
            core::Error::PermissionDenied
        )))
    ));
    assert_eq!(model.readdir(handle, 0, 2).await.unwrap().entries.len(), 2);
    *policy.0.lock().unwrap() = None;
    assert_eq!(
        model.readdir(handle, cookie, 1).await.unwrap().entries[0]
            .1
            .name,
        b"base"
    );
    model.release(handle).await.unwrap();
    model.shutdown().await.unwrap();
}
