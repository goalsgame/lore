// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicUsize;

use super::*;

struct Spy {
    directories: BTreeMap<NodeKey, Vec<NamedNode>>,
    file: NodeKey,
    bytes: &'static [u8],
    retired: AtomicBool,
    listings: AtomicUsize,
}
impl Backend for Spy {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { panic!("namespace lookup uses bounded listing") })
    }
    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            self.listings.fetch_add(1, Ordering::SeqCst);
            if self.retired.load(Ordering::SeqCst) {
                return Err(Error::Stale);
            }
            let entries = self.directories.get(directory).ok_or(Error::NotDirectory)?;
            let start = usize::try_from(offset).map_err(|_overflow| Error::ResourceLimit)?;
            Ok(BackendPage {
                entries: entries
                    .iter()
                    .skip(start)
                    .take(limit)
                    .map(|entry| NamedNode {
                        name: entry.name.clone(),
                        node: entry.node.clone(),
                    })
                    .collect(),
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
            assert_eq!(*key, self.file);
            let start = offset as usize;
            Ok(self.bytes[start..start + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async move {
            assert_eq!(*key, self.file);
            Ok(Some([self.bytes[0]; 32]))
        })
    }
}
fn data(key: NodeKey) -> Node {
    Node {
        key,
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    }
}
fn spy(directory: NodeKey, file: NodeKey, bytes: &'static [u8]) -> Arc<Spy> {
    Arc::new(Spy {
        directories: BTreeMap::from([(
            directory,
            vec![NamedNode {
                name: b"file".to_vec(),
                node: data(file),
            }],
        )]),
        file,
        bytes,
        retired: AtomicBool::new(false),
        listings: AtomicUsize::new(0),
    })
}
fn binding(backend: Arc<dyn Backend>, gate: Arc<Gate>, root: Node) -> Arc<NamespaceBinding> {
    Namespace::new(backend, root, gate, Limits::default())
        .unwrap()
        .current_binding()
        .unwrap()
}
fn newer_root() -> Node {
    let mut root = root();
    root.key.revision = Address::zero_context_hash(lore_base::types::Hash::from_u64(42));
    root
}

#[tokio::test]
async fn lazy_detached_file_uses_old_data_and_old_policy_not_the_current_pair() {
    let old_backend = spy(key(0), key(1), b"old!");
    let old_gate = Arc::new(Gate::default());
    let mut namespace = Namespace::new(
        old_backend.clone(),
        root(),
        old_gate.clone(),
        Limits::default(),
    )
    .unwrap();
    let old = namespace.lookup(1, b"file").await.unwrap();
    namespace.remove(1, b"file", false).await.unwrap();
    assert!(
        namespace.state.lock().await.nodes[&old.inode]
            .file
            .is_none()
    );
    let current_root = newer_root();
    let current_key = NodeKey {
        revision: current_root.key.revision,
        ..key(1)
    };
    let current_backend = spy(current_root.key, current_key, b"new!");
    let current_gate = Arc::new(Gate::default());
    // Private fixture branch selection only, not an installed image or an
    // atomic publication path. Inode 1 is the new global root.
    namespace.routing = routing::Routing::new(binding(
        current_backend,
        current_gate.clone(),
        current_root.clone(),
    ));
    {
        let state = namespace.state.get_mut();
        state.backend_ids.remove(&key(0));
        state.backend_ids.insert(current_root.key, 1);
        let root = state.nodes.get_mut(&1).unwrap();
        root.entry.authority = current_root.key;
        root.entry.base = Some(current_root);
        root.names = None;
    }
    let current = namespace.lookup(1, b"file").await.unwrap();
    assert_ne!(current.inode, old.inode);
    old_gate.deny.store(true, Ordering::SeqCst);
    assert!(namespace.getattr(old.inode).await.is_err());
    assert!(
        namespace
            .file(old.inode, Arc::new(Store::default()), FileLimits::default())
            .await
            .is_err()
    );
    let new_file = file(&namespace, current.inode).await;
    let read = new_file.open(Access::Read).await.unwrap();
    assert_eq!(new_file.read(read, 0, 4).await.unwrap(), b"new!");
    new_file.release(read).await.unwrap();
    old_gate.deny.store(false, Ordering::SeqCst);
    old_backend.retired.store(true, Ordering::SeqCst);
    let old_file = file(&namespace, old.inode).await;
    let read = old_file.open(Access::Read).await.unwrap();
    assert_eq!(old_file.read(read, 0, 4).await.unwrap(), b"old!");
    old_file.release(read).await.unwrap();
    current_gate.deny.store(true, Ordering::SeqCst);
    assert!(namespace.getattr(old.inode).await.is_ok());
    assert!(
        namespace
            .materialized_file(old.inode)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(namespace.digest(old.inode).await.unwrap(), Some([b'o'; 32]));
    assert!(namespace.materialized_file(current.inode).await.is_err());
}

#[tokio::test]
async fn attached_cached_overlay_access_checks_current_policy_before_the_pinned_file() {
    let old_backend = spy(key(0), key(1), b"old!");
    let old_gate = Arc::new(Gate::default());
    let mut namespace =
        Namespace::new(old_backend, root(), old_gate.clone(), Limits::default()).unwrap();
    let entry = namespace.lookup(1, b"file").await.unwrap();
    let pinned = file(&namespace, entry.inode).await;
    let handle = pinned.open(Access::Read).await.unwrap();
    let current_gate = Arc::new(Gate::default());
    let current_backend = spy(key(0), key(1), b"new!");
    namespace.routing =
        routing::Routing::new(binding(current_backend, current_gate.clone(), root()));
    current_gate.deny.store(true, Ordering::SeqCst);
    assert!(namespace.materialized_file(entry.inode).await.is_err());
    assert!(namespace.digest(entry.inode).await.is_err());
    assert_eq!(pinned.read(handle, 0, 4).await.unwrap(), b"old!");
    current_gate.deny.store(false, Ordering::SeqCst);
    old_gate.deny.store(true, Ordering::SeqCst);
    assert!(
        namespace
            .materialized_file(entry.inode)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        namespace.digest(entry.inode).await.unwrap(),
        Some([b'n'; 32])
    );
    assert!(pinned.read(handle, 0, 4).await.is_err());
    pinned.release(handle).await.unwrap();
}

#[tokio::test]
async fn attached_cold_directory_uses_current_discovery_after_the_old_allocator_retires() {
    let child = Node {
        key: key(2),
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    };
    let old_backend = Arc::new(Spy {
        directories: BTreeMap::from([(
            key(0),
            vec![NamedNode {
                name: b"child".to_vec(),
                node: child.clone(),
            }],
        )]),
        file: key(1),
        bytes: b"old!",
        retired: AtomicBool::new(false),
        listings: AtomicUsize::new(0),
    });
    let old_gate = Arc::new(Gate::default());
    let mut namespace = Namespace::new(
        old_backend.clone(),
        root(),
        old_gate.clone(),
        Limits::default(),
    )
    .unwrap();
    let directory = namespace.lookup(1, b"child").await.unwrap();
    let nested_key = NodeKey {
        exposure: 2,
        ..key(3)
    };
    let current_backend = Arc::new(Spy {
        directories: BTreeMap::from([(
            child.key,
            vec![NamedNode {
                name: b"nested".to_vec(),
                node: data(nested_key),
            }],
        )]),
        file: nested_key,
        bytes: b"new!",
        retired: AtomicBool::new(false),
        listings: AtomicUsize::new(0),
    });
    namespace.routing = routing::Routing::new(binding(
        current_backend.clone(),
        Arc::new(Gate::default()),
        root(),
    ));
    old_backend.retired.store(true, Ordering::SeqCst);
    old_gate.deny.store(true, Ordering::SeqCst);
    let nested = namespace.lookup(directory.inode, b"nested").await.unwrap();
    assert_eq!(nested.authority, nested_key);
    assert_eq!(old_backend.listings.load(Ordering::SeqCst), 1);
    assert_eq!(current_backend.listings.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn snapshot_has_no_binding_and_restore_authorizes_retired_records_with_fresh_policy() {
    let backend = spy(key(0), key(1), b"base");
    let old_gate = Arc::new(Gate::default());
    let namespace =
        Namespace::new(backend.clone(), root(), old_gate.clone(), Limits::default()).unwrap();
    let entry = namespace.lookup(1, b"file").await.unwrap();
    namespace.remove(1, b"file", false).await.unwrap();
    let old_binding = Arc::downgrade(&namespace.current_binding().unwrap());
    let snapshot = namespace.snapshot(0).await.unwrap();
    drop(namespace);
    assert!(old_binding.upgrade().is_none());
    let fresh_gate = Arc::new(Gate::default());
    let restored = Namespace::restore(
        backend,
        root(),
        fresh_gate.clone(),
        snapshot,
        BTreeMap::new(),
        Limits::default(),
        FileLimits::default(),
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(
        restored.state.lock().await.nodes[&entry.inode]
            .retired_binding
            .as_ref()
            .unwrap(),
        &restored.current_binding().unwrap()
    ));
    old_gate.deny.store(true, Ordering::SeqCst);
    let retained = file(&restored, entry.inode).await;
    let read = retained.open(Access::Read).await.unwrap();
    assert_eq!(retained.read(read, 0, 4).await.unwrap(), b"base");
    retained.release(read).await.unwrap();
    fresh_gate.deny.store(true, Ordering::SeqCst);
    assert!(restored.getattr(entry.inode).await.is_err());
    assert!(restored.materialized_file(entry.inode).await.is_err());
}

#[tokio::test]
async fn retiring_an_already_detached_record_never_replaces_its_binding() {
    let backend = spy(key(0), key(1), b"base");
    let gate = Arc::new(Gate::default());
    let mut namespace = Namespace::new(backend, root(), gate.clone(), Limits::default()).unwrap();
    let entry = namespace.lookup(1, b"file").await.unwrap();
    namespace.remove(1, b"file", false).await.unwrap();
    let original = namespace.current_binding().unwrap().clone();
    let replacement = binding(
        spy(key(0), key(1), b"new!"),
        Arc::new(Gate::default()),
        root(),
    );
    namespace.routing = routing::Routing::new(replacement.clone());
    detach(namespace.state.get_mut(), entry.inode, &replacement);
    assert!(Arc::ptr_eq(
        namespace.state.lock().await.nodes[&entry.inode]
            .retired_binding
            .as_ref()
            .unwrap(),
        &original
    ));
    gate.deny.store(true, Ordering::SeqCst);
    assert!(namespace.getattr(entry.inode).await.is_err());
}
