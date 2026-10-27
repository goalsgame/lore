// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;

struct Base;
impl Backend for Base {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
}
struct Gate;
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
fn root() -> Node {
    Node {
        key: NodeKey {
            repository: [42; 16].into(),
            revision: Default::default(),
            exposure: 1,
            node: 0,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o755,
    }
}
fn fixture() -> (State, Arc<NamespaceBinding>) {
    let namespace =
        Namespace::new(Arc::new(Base), root(), Arc::new(Gate), Limits::default()).unwrap();
    {
        let binding = namespace.current_binding().unwrap();
        (namespace.state.into_inner(), binding)
    }
}
fn leaf(inode: u64, attached: bool, binding: &Arc<NamespaceBinding>) -> Record {
    let base = Node {
        key: NodeKey {
            node: 1,
            ..root().key
        },
        kind: NodeKind::File,
        size: 4,
        mode: 0o644,
    };
    Record {
        receipt: receipt_cell(),
        entry: Entry {
            inode,
            parent: 1,
            kind: base.kind,
            authority: base.key,
            base: Some(base),
        },
        names: None,
        file: None,
        attached,
        retired_binding: (!attached).then(|| binding.clone()),
    }
}
fn budget() -> Budget {
    Budget::new(Default::default()).unwrap()
}
#[test]
fn duplicate_attached_keys_are_rejected_but_retained_history_is_not_indexed() {
    let (mut state, binding) = fixture();
    state.next_inode = 4;
    state.nodes.insert(2, leaf(2, true, &binding));
    state.nodes.insert(3, leaf(3, true, &binding));
    state.nodes.get_mut(&1).unwrap().names = Some(BTreeMap::from([
        (b"a".to_vec(), (b"a".to_vec(), 2)),
        (b"b".to_vec(), (b"b".to_vec(), 3)),
    ]));
    state.backend_ids.clear();
    assert!(validate_state(&mut state, Limits::default(), &root(), &mut budget()).is_err());
    state.nodes.insert(3, leaf(3, false, &binding));
    state
        .nodes
        .get_mut(&1)
        .unwrap()
        .names
        .as_mut()
        .unwrap()
        .remove(b"b".as_slice());
    state.backend_ids.clear();
    validate_state(&mut state, Limits::default(), &root(), &mut budget()).unwrap();
    assert_eq!(state.backend_ids[&state.nodes[&2].entry.authority], 2);
    assert_eq!(
        state.nodes[&3].entry.authority,
        state.nodes[&2].entry.authority
    );
    assert!(Arc::ptr_eq(
        state.nodes[&3].retired_binding.as_ref().unwrap(),
        &binding
    ));
}
#[test]
fn structural_readiness_rejects_cycles_bad_root_names_and_unpinned_history() {
    let (mut state, binding) = fixture();
    state.nodes.get_mut(&1).unwrap().entry.parent = 2;
    assert!(validate_state(&mut state, Limits::default(), &root(), &mut budget()).is_err());
    state.nodes.get_mut(&1).unwrap().entry.parent = 1;
    state.next_inode = 3;
    let mut detached = leaf(2, false, &binding);
    detached.retired_binding = None;
    state.nodes.insert(2, detached);
    assert!(validate_state(&mut state, Limits::default(), &root(), &mut budget()).is_err());
    state.nodes.insert(2, leaf(2, true, &binding));
    state.nodes.get_mut(&1).unwrap().names =
        Some(BTreeMap::from([(b"FILE".to_vec(), (b"FILE".to_vec(), 2))]));
    assert!(validate_state(&mut state, Limits::default(), &root(), &mut budget()).is_err());
    let mut directory = leaf(2, true, &binding);
    directory.entry.kind = NodeKind::Directory;
    directory.entry.base.as_mut().unwrap().kind = NodeKind::Directory;
    directory.entry.parent = 2;
    directory.names = Some(BTreeMap::from([(b"self".to_vec(), (b"self".to_vec(), 2))]));
    state.nodes.insert(2, directory);
    state.nodes.get_mut(&1).unwrap().names = Some(BTreeMap::new());
    state.backend_ids.clear();
    assert!(validate_state(&mut state, Limits::default(), &root(), &mut budget()).is_err());
}

#[test]
fn selected_child_baseline_requires_explicit_matching_parent_projection() {
    let parent = crate::core::ParentScope {
        scope: root().key,
        initial_revision: Default::default(),
        link: NodeKey {
            node: 2,
            ..root().key
        },
    };
    assert!(selected_projection(&Some(parent.clone()), None).is_err());
    let mut baseline = HeldBaseline {
        scope: NodeKey {
            exposure: 2,
            ..root().key
        },
        head: crate::core::ScopeHead {
            revision: Default::default(),
            root: Some(0),
        },
        sequence: 1,
        parent_projection: Some(crate::core::ScopeProjection {
            parent: parent.scope,
            head: Default::default(),
            target: None,
        }),
    };
    assert!(selected_projection(&None, Some(&baseline)).is_err());
    assert_eq!(
        selected_projection(&Some(parent.clone()), Some(&baseline)).unwrap(),
        baseline.parent_projection
    );
    baseline.parent_projection.as_mut().unwrap().parent.exposure += 1;
    assert!(selected_projection(&Some(parent), Some(&baseline)).is_err());
    assert!(selected_projection(&None, None).unwrap().is_none());
}
