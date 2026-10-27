// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::types::Address;
use lore_base::types::RepositoryId;

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::NamedNode;

#[path = "detached_index_tests.rs"]
mod detached_index;

fn key(id: u32) -> NodeKey {
    NodeKey {
        repository: RepositoryId::from([1; 16]),
        revision: Address::default(),
        node: id,
        exposure: 1,
    }
}
fn root() -> Node {
    Node {
        key: key(0),
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}
struct Base {
    entries: Vec<(Vec<u8>, Node)>,
    fail: AtomicBool,
}
impl Backend for Base {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { panic!("namespace uses bounded listing") })
    }
    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Io);
            }
            let entries = if directory.node == 0 {
                self.entries
                    .iter()
                    .skip(offset as usize)
                    .take(limit)
                    .map(|(name, node)| NamedNode {
                        name: name.clone(),
                        node: node.clone(),
                    })
                    .collect()
            } else {
                Vec::new()
            };
            Ok(BackendPage {
                entries,
                next_offset: None,
            })
        })
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            assert_eq!(key.node, 1);
            Ok(b"base"[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { panic!("local file digest must not fetch backend") })
    }
}
#[derive(Default)]
struct Gate {
    deny: AtomicBool,
    block: AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.block.load(Ordering::SeqCst) {
                self.started.notify_one();
                self.resume.notified().await;
            }
            if self.deny.load(Ordering::SeqCst) {
                Err(Error::PermissionDenied)
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
            Ok(self.0.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut s = self.0.lock().await;
            s.resize(offset as usize + bytes.len(), 0);
            s[offset as usize..].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}
fn fixture(entries: Vec<(Vec<u8>, Node)>, limits: Limits) -> (Arc<Namespace>, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    (
        Arc::new(
            Namespace::new(
                Arc::new(Base {
                    entries,
                    fail: AtomicBool::new(false),
                }),
                root(),
                gate.clone(),
                limits,
            )
            .unwrap(),
        ),
        gate,
    )
}
async fn create(ns: &Namespace, parent: u64, name: &[u8]) -> Entry {
    ns.create_file(
        parent,
        name,
        0o644,
        Arc::new(Store::default()),
        FileLimits::default(),
    )
    .await
    .unwrap()
}
async fn file(ns: &Namespace, id: u64) -> Arc<OverlayFile> {
    ns.file(id, Arc::new(Store::default()), FileLimits::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn unlink_recreate_and_rename_over_retain_open_files() {
    let (ns, _) = fixture(Vec::new(), Limits::default());
    let a = create(&ns, 1, b"a").await;
    let fa = file(&ns, a.inode).await;
    let ha = fa.open(Access::Write).await.unwrap();
    fa.write(ha, 0, b"old").await.unwrap();
    ns.remove(1, b"a", false).await.unwrap();
    let b = create(&ns, 1, b"a").await;
    assert_ne!(a.inode, b.inode);
    assert_eq!(fa.read(ha, 0, 9).await.unwrap(), b"old");
    let fb = file(&ns, b.inode).await;
    let hb = fb.open(Access::Write).await.unwrap();
    fb.write(hb, 0, b"new").await.unwrap();
    let c = create(&ns, 1, b"c").await;
    let fc = file(&ns, c.inode).await;
    let hc = fc.open(Access::Write).await.unwrap();
    fc.write(hc, 0, b"replaced").await.unwrap();
    ns.rename(1, b"a", 1, b"c").await.unwrap();
    assert_eq!(ns.lookup(1, b"c").await.unwrap().inode, b.inode);
    assert_eq!(fc.read(hc, 0, 99).await.unwrap(), b"replaced");
    assert_eq!(fb.digest().await.unwrap(), None);
    assert_eq!(fb.read(hb, 0, 9).await.unwrap(), b"new");
}
#[tokio::test]
async fn merge_base_case_matching_and_unicode_collisions() {
    let base = Node {
        key: key(1),
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    };
    let (ns, _) = fixture(vec![(b"Base".to_vec(), base)], Limits::default());
    let entry = ns.lookup(1, b"BASE").await.unwrap();
    let f = file(&ns, entry.inode).await;
    let h = f.open(Access::Read).await.unwrap();
    assert_eq!(f.read(h, 0, 4).await.unwrap(), b"base");
    create(&ns, 1, "Ä".as_bytes()).await;
    assert_eq!(
        ns.lookup(1, "ä".as_bytes()).await.unwrap().kind,
        NodeKind::File
    );
    assert_eq!(
        ns.create_directory(1, "ä".as_bytes()).await,
        Err(NamespaceError::Exists)
    );
    create(&ns, 1, "İ".as_bytes()).await;
    assert_eq!(
        ns.create_directory(1, "i\u{307}".as_bytes()).await,
        Err(NamespaceError::Exists)
    );
    ns.remove(1, b"base", false).await.unwrap();
    assert_eq!(ns.lookup(1, b"BASE").await, Err(Error::NotFound.into()));
    assert_eq!(ns.list(1, None, 10).await.unwrap().entries.len(), 2);
}
#[tokio::test]
async fn directories_cycles_replacement_and_boundaries() {
    let mut child = root();
    child.key = key(9);
    child.key.exposure = 2;
    let (ns, _) = fixture(vec![(b"linked".to_vec(), child)], Limits::default());
    let a = ns.create_directory(1, b"a").await.unwrap();
    let b = ns.create_directory(a.inode, b"b").await.unwrap();
    assert_eq!(
        ns.rename(1, b"a", b.inode, b"cycle").await,
        Err(NamespaceError::Cycle)
    );
    assert_eq!(
        ns.remove(1, b"a", true).await,
        Err(NamespaceError::NotEmpty)
    );
    assert_eq!(
        ns.remove(1, b"linked", true).await,
        Err(NamespaceError::CrossBoundary)
    );
    let linked = ns.lookup(1, b"linked").await.unwrap();
    assert_eq!(
        ns.rename(a.inode, b"b", linked.inode, b"move").await,
        Err(NamespaceError::CrossBoundary)
    );
    let c = ns.create_directory(1, b"c").await.unwrap();
    ns.rename(a.inode, b"b", 1, b"c").await.unwrap();
    assert_eq!(ns.lookup(1, b"c").await.unwrap().inode, b.inode);
    assert_eq!(
        ns.list(c.inode, None, 1).await.err(),
        Some(Error::Stale.into())
    );
}
#[tokio::test]
async fn denial_cancellation_and_cookie_generation_are_atomic() {
    let (ns, gate) = fixture(Vec::new(), Limits::default());
    create(&ns, 1, b"a").await;
    create(&ns, 1, b"b").await;
    let cookie = ns.list(1, None, 1).await.unwrap().next.unwrap();
    gate.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        ns.rename(1, b"a", 1, b"b").await,
        Err(Error::PermissionDenied.into())
    );
    gate.deny.store(false, Ordering::SeqCst);
    gate.block.store(true, Ordering::SeqCst);
    #[allow(clippy::disallowed_methods)]
    let task = {
        let ns = ns.clone();
        tokio::spawn(async move { ns.remove(1, b"a", false).await })
    };
    gate.started.notified().await;
    task.abort();
    let _ = task.await;
    gate.block.store(false, Ordering::SeqCst);
    assert!(ns.lookup(1, b"a").await.is_ok());
    assert!(ns.list(1, Some(cookie), 1).await.is_ok());
    ns.rename(1, b"a", 1, b"A").await.unwrap();
    assert_eq!(
        ns.list(1, Some(cookie), 1).await.err(),
        Some(Error::Stale.into())
    );
}
#[tokio::test]
async fn limits_and_invalid_names() {
    let (ns, _) = fixture(
        Vec::new(),
        Limits {
            max_inodes: 3,
            max_directory_entries: 1,
        },
    );
    create(&ns, 1, b"a").await;
    assert_eq!(
        ns.create_directory(1, b"b").await,
        Err(Error::ResourceLimit.into())
    );
    ns.remove(1, b"a", false).await.unwrap();
    create(&ns, 1, b"b").await;
    ns.remove(1, b"b", false).await.unwrap();
    assert_eq!(
        ns.create_directory(1, b"c").await,
        Err(Error::ResourceLimit.into())
    );
    for invalid in [b".".as_slice(), b"..", b"a/b", b"a\\b", b".lore", b"\xff"] {
        assert_eq!(ns.lookup(1, invalid).await, Err(Error::InvalidInput.into()));
    }
}
#[tokio::test]
async fn snapshot_roundtrip_preserves_dirty_and_removed_files() {
    let (ns, gate) = fixture(Vec::new(), Limits::default());
    let a = create(&ns, 1, b"a").await;
    let f = file(&ns, a.inode).await;
    let h = f.open(Access::Write).await.unwrap();
    f.write(h, 2, b"dirty").await.unwrap();
    ns.remove(1, b"a", false).await.unwrap();
    create(&ns, 1, b"a").await;
    assert_eq!(
        ns.snapshot(4).await.err(),
        Some(Error::ResourceLimit.into())
    );
    let snapshot = ns.snapshot(100).await.unwrap();
    let stores = snapshot
        .files
        .keys()
        .map(|&id| (id, Arc::new(Store::default()) as Arc<dyn SparseStore>))
        .collect();
    let restored = Namespace::restore(
        Arc::new(Base {
            entries: Vec::new(),
            fail: AtomicBool::new(false),
        }),
        root(),
        gate,
        snapshot,
        stores,
        Limits::default(),
        FileLimits::default(),
    )
    .await
    .unwrap();
    assert_ne!(restored.lookup(1, b"a").await.unwrap().inode, a.inode);
    let f = file(&restored, a.inode).await;
    assert_eq!(f.read(h, 0, 8).await, Err(Error::BadHandle));
    let h = f.open(Access::Read).await.unwrap();
    assert_eq!(f.read(h, 0, 8).await.unwrap(), b"\0\0dirty");
}

#[tokio::test]
async fn malformed_backend_and_snapshot_fail_without_publication() {
    let base = Node {
        key: key(1),
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    };
    let (ns, _) = fixture(
        vec![(b"A".to_vec(), base.clone()), (b"a".to_vec(), base)],
        Limits::default(),
    );
    assert_eq!(ns.list(1, None, 1).await.err(), Some(Error::Io.into()));
    assert_eq!(ns.snapshot(0).await.unwrap().nodes.len(), 1);
    let (ns, _) = fixture(vec![(b"loop".to_vec(), root())], Limits::default());
    assert_eq!(ns.lookup(1, b"loop").await, Err(Error::Io.into()));
    let (ns, _) = fixture(Vec::new(), Limits::default());
    let a = ns.create_directory(1, b"a").await.unwrap();
    let b = ns.create_directory(a.inode, b"b").await.unwrap();
    let mut missing = ns.snapshot(0).await.unwrap();
    missing
        .nodes
        .iter_mut()
        .find(|node| node.entry.inode == b.inode)
        .unwrap()
        .entry
        .parent = 999;
    assert_eq!(
        missing.validate(Limits::default(), FileLimits::default()),
        Err(Error::InvalidInput.into())
    );
    let mut cycle = ns.snapshot(0).await.unwrap();
    cycle
        .nodes
        .iter_mut()
        .find(|node| node.entry.inode == a.inode)
        .unwrap()
        .entry
        .parent = b.inode;
    assert_eq!(
        cycle.validate(Limits::default(), FileLimits::default()),
        Err(Error::InvalidInput.into())
    );
}

#[tokio::test]
async fn detached_file_rechecks_authorization_and_paging_is_bounded() {
    let (ns, gate) = fixture(Vec::new(), Limits::default());
    let a = create(&ns, 1, b"a").await;
    create(&ns, 1, b"b").await;
    let f = file(&ns, a.inode).await;
    let h = f.open(Access::Write).await.unwrap();
    f.write(h, 0, b"secret").await.unwrap();
    ns.remove(1, b"a", false).await.unwrap();
    gate.deny.store(true, Ordering::SeqCst);
    assert_eq!(f.read(h, 0, 6).await, Err(Error::PermissionDenied));
    assert_eq!(f.write(h, 0, b"x").await, Err(Error::PermissionDenied));
    f.release(h).await.unwrap();
    gate.deny.store(false, Ordering::SeqCst);
    assert_eq!(
        ns.list(1, None, 0).await.err(),
        Some(Error::InvalidInput.into())
    );
    assert_eq!(
        ns.list(1, None, MAX_DIRECTORY_ENTRIES + 1).await.err(),
        Some(Error::InvalidInput.into())
    );
    let generation = ns.state.lock().await.generation;
    assert_eq!(
        ns.list(
            1,
            Some(Cookie {
                generation,
                offset: 2
            }),
            1
        )
        .await
        .err(),
        Some(Error::InvalidInput.into())
    );
}

#[tokio::test]
async fn base_directory_budget_failure_retains_no_partial_nodes() {
    let entries = (1..=2)
        .map(|id| {
            (
                vec![b'a' + id as u8],
                Node {
                    key: key(id),
                    kind: NodeKind::File,
                    size: 4,
                    mode: 0o644,
                },
            )
        })
        .collect();
    let (ns, _) = fixture(
        entries,
        Limits {
            max_inodes: 10,
            max_directory_entries: 1,
        },
    );
    assert_eq!(ns.lookup(1, b"b").await, Err(Error::ResourceLimit.into()));
    assert_eq!(ns.snapshot(0).await.unwrap().nodes.len(), 1);
}

#[tokio::test]
async fn unsupported_backend_provenance_cannot_be_ignored_on_restore() {
    let (ns, gate) = fixture(Vec::new(), Limits::default());
    let mut snapshot = ns.snapshot(0).await.unwrap();
    snapshot.backend_state = vec![1];
    assert!(matches!(
        Namespace::restore(
            Arc::new(Base {
                entries: Vec::new(),
                fail: AtomicBool::new(false)
            }),
            root(),
            gate,
            snapshot,
            BTreeMap::new(),
            Limits::default(),
            FileLimits::default()
        )
        .await,
        Err(NamespaceError::Backend(Error::Unsupported))
    ));
}

#[tokio::test]
async fn operation_lease_rejects_retained_writers_and_thaws_without_losing_edits() {
    let (ns, _) = fixture(Vec::new(), Limits::default());
    let entry = create(&ns, 1, b"file").await;
    let file = file(&ns, entry.inode).await;
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"kept").await.unwrap();
    ns.remove(1, b"file", false).await.unwrap();
    assert!(matches!(
        ns.freeze_operation().await,
        Err(NamespaceError::Busy)
    ));
    file.write(writer, 0, b"safe").await.unwrap();
    file.release(writer).await.unwrap();
    let reader = file.open(Access::Read).await.unwrap();
    let lease = ns.freeze_operation().await.unwrap();
    let generation = file.metadata().await.unwrap().generation;
    assert!(matches!(
        ns.freeze_operation().await,
        Err(NamespaceError::Busy)
    ));
    assert_eq!(
        ns.create_directory(1, b"blocked").await,
        Err(NamespaceError::Busy)
    );
    assert_eq!(file.open(Access::Write).await, Err(Error::Busy));
    assert_eq!(file.metadata().await.unwrap().generation, generation);
    assert_eq!(file.read(reader, 0, 8).await.unwrap(), b"safe");
    drop(lease);
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"live").await.unwrap();
    file.release(writer).await.unwrap();
    assert_eq!(file.read(reader, 0, 8).await.unwrap(), b"live");
}
#[tokio::test]
async fn cancelling_operation_acquisition_thaws_every_retained_overlay() {
    let (ns, gate) = fixture(Vec::new(), Limits::default());
    let entry = create(&ns, 1, b"file").await;
    let file = file(&ns, entry.inode).await;
    gate.block.store(true, Ordering::SeqCst);
    #[allow(clippy::disallowed_methods)]
    let acquire = {
        let ns = ns.clone();
        tokio::spawn(async move { ns.freeze_operation().await })
    };
    gate.started.notified().await;
    acquire.abort();
    let _ = acquire.await;
    gate.block.store(false, Ordering::SeqCst);
    let writer = file.open(Access::Write).await.unwrap();
    file.release(writer).await.unwrap();
    ns.create_directory(1, b"thawed").await.unwrap();
}

#[tokio::test]
async fn old_lease_cannot_observe_or_thaw_a_later_operation_after_edits() {
    let (ns, gate) = fixture(Vec::new(), Limits::default());
    let entry = create(&ns, 1, b"file").await;
    let file = file(&ns, entry.inode).await;
    let old = ns.freeze_operation().await.unwrap();
    let old_token = old.token();
    gate.block.store(true, Ordering::SeqCst);
    #[allow(clippy::disallowed_methods)]
    let read = {
        let ns = ns.clone();
        tokio::spawn(async move { ns.frozen_file(old_token, entry.inode).await })
    };
    gate.started.notified().await;
    drop(old);
    gate.block.store(false, Ordering::SeqCst);
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"later").await.unwrap();
    file.release(writer).await.unwrap();
    let current = ns.freeze_operation().await.unwrap();
    assert_ne!(old_token, current.token());
    gate.resume.notify_one();
    assert_eq!(read.await.unwrap().err(), Some(Error::Stale.into()));
    ns.operation_freeze.release(old_token);
    assert_eq!(file.open(Access::Write).await, Err(Error::Busy));
    drop(current);
    let writer = file.open(Access::Write).await.unwrap();
    file.release(writer).await.unwrap();
}

#[tokio::test]
async fn directory_handles_can_capture_bounded_names_and_reuse_authorized_overlays() {
    let base = Node {
        key: key(1),
        kind: NodeKind::File,
        size: 4,
        mode: 0o444,
    };
    let (namespace, gate) = fixture(vec![(b"base".to_vec(), base)], Limits::default());
    let entry = namespace.lookup(1, b"base").await.unwrap();
    assert!(
        namespace
            .materialized_file(entry.inode)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        namespace.directory_snapshot_bounded(1, 0).await,
        Err(NamespaceError::Backend(Error::ResourceLimit))
    ));
    let snapshot = namespace.directory_snapshot(1).await.unwrap();
    assert_eq!(snapshot.entry.inode, 1);
    assert_eq!(snapshot.parent.inode, 1);
    assert_eq!(snapshot.entries[0].name, b"base");
    let file = file(&namespace, entry.inode).await;
    assert!(Arc::ptr_eq(
        &file,
        &namespace
            .materialized_file(entry.inode)
            .await
            .unwrap()
            .unwrap()
    ));
    namespace.rename(1, b"base", 1, b"renamed").await.unwrap();
    let later = namespace.directory_snapshot(1).await.unwrap();
    assert!(later.generation > snapshot.generation);
    assert_eq!(snapshot.entries[0].name, b"base");
    assert_eq!(later.entries[0].name, b"renamed");
    gate.deny.store(true, Ordering::SeqCst);
    assert!(matches!(
        namespace.materialized_file(entry.inode).await,
        Err(NamespaceError::Backend(Error::PermissionDenied))
    ));
    assert!(matches!(
        namespace.directory_snapshot(1).await,
        Err(NamespaceError::Backend(Error::PermissionDenied))
    ));
}

struct ScopeBackend {
    base: Base,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl Backend for ScopeBackend {
    fn lookup<'a>(&'a self, key: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        self.base.lookup(key, name)
    }
    fn list<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        self.base.list(key, offset, limit)
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        self.base.read(key, offset, size)
    }
    fn digest<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        self.base.digest(key)
    }
    fn owning_scope<'a>(&'a self, _key: &'a NodeKey) -> BackendFuture<'a, crate::core::ScopeInfo> {
        Box::pin(async {
            Ok(crate::core::ScopeInfo {
                root: root().key,
                initial_revision: root().key.revision,
                source_path: vec![],
                parent: None,
            })
        })
    }
    fn scope_head<'a>(
        &'a self,
        _scope: &'a NodeKey,
        revision: lore_base::types::Address,
    ) -> BackendFuture<'a, crate::core::ScopeHead> {
        Box::pin(async move {
            self.started.notify_one();
            self.resume.notified().await;
            Ok(crate::core::ScopeHead {
                revision,
                root: Some(0),
            })
        })
    }
}
#[tokio::test]
async fn blocked_baseline_proof_does_not_lock_namespace_or_adopt_after_freeze_replacement() {
    let backend = Arc::new(ScopeBackend {
        base: Base {
            entries: vec![],
            fail: AtomicBool::new(false),
        },
        started: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    let namespace = Arc::new(
        Namespace::new(
            backend.clone(),
            root(),
            Arc::new(Gate::default()),
            Limits::default(),
        )
        .unwrap(),
    );
    let old = namespace.freeze_operation().await.unwrap();
    let token = old.token();
    let next = crate::core::HeldBaseline {
        scope: root().key,
        head: crate::core::ScopeHead {
            revision: lore_base::types::Address::zero_context_hash([2; 32].into()),
            root: Some(0),
        },
        sequence: 1,
        parent_projection: None,
    };
    let adoption = namespace.adopt_committed(
        token,
        vec![],
        vec![BaselineUpdate {
            expected: None,
            next,
        }],
        || Ok(()),
    );
    tokio::pin!(adoption);
    tokio::select! {result=&mut adoption=>panic!("proof did not wait: {result:?}"),_=backend.started.notified()=>{}}
    tokio::time::timeout(std::time::Duration::from_secs(1), namespace.getattr(1))
        .await
        .expect("immutable proof IO held the namespace lock")
        .unwrap();
    drop(old);
    let replacement = namespace.freeze_operation().await.unwrap();
    backend.resume.notify_one();
    assert_eq!(
        adoption.await.unwrap_err(),
        NamespaceError::Backend(Error::Stale)
    );
    assert!(namespace.held_baselines().await.unwrap().is_empty());
    assert_eq!(
        namespace.create_directory(1, b"blocked").await.unwrap_err(),
        NamespaceError::Busy
    );
    drop(replacement);
    namespace.create_directory(1, b"allowed").await.unwrap();
}

struct RestoreBackend {
    replacement: Arc<dyn Backend>,
}
impl Backend for RestoreBackend {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { panic!("restore must use the replacement backend") })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async { panic!("restore must use the replacement backend") })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("restore must use the replacement backend") })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { panic!("restore must use the replacement backend") })
    }
    fn prepare_restore<'a>(&'a self, _: &'a [u8]) -> BackendFuture<'a, Option<Arc<dyn Backend>>> {
        Box::pin(async { Ok(Some(self.replacement.clone())) })
    }
}
struct BindingFactory {
    expected: Arc<dyn Backend>,
    gate: Arc<Gate>,
    calls: std::sync::atomic::AtomicUsize,
    reject: bool,
}
impl crate::overlay::AccessGateFactory for BindingFactory {
    fn bind(&self, backend: Arc<dyn Backend>) -> crate::core::Result<Arc<dyn AccessGate>> {
        assert!(Arc::ptr_eq(&backend, &self.expected));
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            Err(Error::PermissionDenied)
        } else {
            Ok(self.gate.clone())
        }
    }
}

#[tokio::test]
async fn factory_binds_final_backend_once_before_restored_private_writes() {
    let (source, _) = fixture(Vec::new(), Limits::default());
    let entry = create(&source, 1, b"private").await;
    let original = file(&source, entry.inode).await;
    let writer = original.open(Access::Write).await.unwrap();
    original.write(writer, 0, b"saved").await.unwrap();
    original.release(writer).await.unwrap();
    let snapshot = source.snapshot(100).await.unwrap();
    let final_backend: Arc<dyn Backend> = Arc::new(Base {
        entries: Vec::new(),
        fail: AtomicBool::new(false),
    });
    let input: Arc<dyn Backend> = Arc::new(RestoreBackend {
        replacement: final_backend.clone(),
    });
    let gate = Arc::new(Gate::default());
    for reject in [true, false] {
        let factory = Arc::new(BindingFactory {
            expected: final_backend.clone(),
            gate: gate.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            reject,
        });
        let store = Arc::new(Store::default());
        let stores = BTreeMap::from([(entry.inode, store.clone() as Arc<dyn SparseStore>)]);
        let restored = Namespace::restore_with_gate_factory(
            input.clone(),
            root(),
            factory.clone(),
            snapshot.clone(),
            stores,
            Limits::default(),
            FileLimits::default(),
        )
        .await;
        assert_eq!(factory.calls.load(Ordering::SeqCst), 1);
        if reject {
            assert!(matches!(
                restored,
                Err(NamespaceError::Backend(Error::PermissionDenied))
            ));
            assert!(store.0.lock().await.is_empty());
        } else {
            let restored = restored.unwrap();
            let expected_factory: Arc<dyn crate::overlay::AccessGateFactory> = factory.clone();
            assert!(Arc::ptr_eq(&restored.gate_factory, &expected_factory));
            let restored_file = file(&restored, entry.inode).await;
            let reader = restored_file.open(Access::Read).await.unwrap();
            assert_eq!(restored_file.read(reader, 0, 5).await.unwrap(), b"saved");
            gate.deny.store(true, Ordering::SeqCst);
            assert!(matches!(
                restored.getattr(entry.inode).await,
                Err(NamespaceError::Backend(Error::PermissionDenied))
            ));
            assert_eq!(
                restored_file.read(reader, 0, 5).await,
                Err(Error::PermissionDenied)
            );
        }
    }
}

#[tokio::test]
async fn fresh_factory_and_injected_wrapper_preserve_exact_policy() {
    let backend: Arc<dyn Backend> = Arc::new(Base {
        entries: Vec::new(),
        fail: AtomicBool::new(false),
    });
    let gate = Arc::new(Gate::default());
    let erased: Arc<dyn AccessGate> = gate.clone();
    let injected = crate::overlay::InjectedGateFactory::new(erased.clone());
    let bound = crate::overlay::AccessGateFactory::bind(&injected, backend.clone()).unwrap();
    assert!(Arc::ptr_eq(&bound, &erased));
    let factory = Arc::new(BindingFactory {
        expected: backend.clone(),
        gate: gate.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        reject: false,
    });
    let namespace =
        Namespace::new_with_gate_factory(backend, root(), factory.clone(), Limits::default())
            .unwrap();
    let expected_factory: Arc<dyn crate::overlay::AccessGateFactory> = factory.clone();
    assert!(Arc::ptr_eq(&namespace.gate_factory, &expected_factory));
    namespace.getattr(1).await.unwrap();
    gate.deny.store(true, Ordering::SeqCst);
    assert!(matches!(
        namespace.getattr(1).await,
        Err(NamespaceError::Backend(Error::PermissionDenied))
    ));
    assert_eq!(factory.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exact_factory_is_retained_without_retaining_namespace() {
    let backend: Arc<dyn Backend> = Arc::new(Base {
        entries: Vec::new(),
        fail: AtomicBool::new(false),
    });
    let gate = Arc::new(Gate::default());
    let factory = Arc::new(BindingFactory {
        expected: backend.clone(),
        gate: gate.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        reject: false,
    });
    let weak_factory = Arc::downgrade(&factory);
    let namespace = Arc::new(
        Namespace::new_with_gate_factory(
            backend.clone(),
            root(),
            factory.clone(),
            Limits::default(),
        )
        .unwrap(),
    );
    let weak_namespace = Arc::downgrade(&namespace);
    drop(factory);
    assert_eq!(
        weak_factory.upgrade().unwrap().calls.load(Ordering::SeqCst),
        1
    );
    drop(namespace);
    assert!(weak_namespace.upgrade().is_none());
    assert!(weak_factory.upgrade().is_none());

    let erased_gate: Arc<dyn AccessGate> = gate.clone();
    let injected = Namespace::new(
        backend.clone(),
        root(),
        erased_gate.clone(),
        Limits::default(),
    )
    .unwrap();
    let retained_gate = injected.gate_factory.bind(backend).unwrap();
    assert!(Arc::ptr_eq(&retained_gate, &erased_gate));
    gate.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        retained_gate.authorize(&key(0), Access::Write).await,
        Err(Error::PermissionDenied)
    );
    assert!(matches!(
        injected.getattr(1).await,
        Err(NamespaceError::Backend(Error::PermissionDenied))
    ));
}

#[path = "fixed_binding_tests.rs"]
mod fixed_binding;
