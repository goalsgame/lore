// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::namespace::BaselineUpdate;
use crate::namespace::complete_binding::PreparedPolicyBinding;
use crate::overlay::AccessGateFactory;
use crate::transition_prepare::install_layout::FileLayout;

async fn binding(plan: &PreparedTransition) -> PreparedPolicyBinding {
    plan.freeze
        .namespace()
        .bind_complete_backend(plan.prepare_complete_backend().await.unwrap())
        .unwrap()
}

#[tokio::test]
async fn layout_keeps_root_and_sparse_source_identity_without_copying_private_data() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let dirty = dirty(&f, b"edited").await;
            let clean = f.namespace.lookup(1, b"unrelated").await.unwrap();
            let clean_file = f
                .namespace
                .file(clean.inode, Arc::new(Store::default()), Default::default())
                .await
                .unwrap();
            let reader = clean_file.open(Access::Read).await.unwrap();
            clean_file.release(reader).await.unwrap();
            let before = f.namespace.snapshot(100).await.unwrap();
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options {
                    local: LocalPolicy::KeepLocal,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            let bound = binding(&plan).await;
            let count = Arc::strong_count(&f.namespace);
            let layout = plan.prepare_install_layout(bound).await.unwrap();
            assert_eq!(Arc::strong_count(&f.namespace), count);
            assert_eq!(layout.root_inode, 1);
            assert!(layout.parent_patch.is_none());
            assert!(
                layout
                    .fresh
                    .keys()
                    .all(|id| *id == 1 || *id >= before.next_inode)
            );
            assert_eq!(layout.source_next_inode, before.next_inode);
            assert_eq!(layout.next_generation, before.generation + 1);
            let edited = layout
                .fresh
                .values()
                .find(|node| node.path == "edited")
                .unwrap();
            let FileLayout::Private {
                source_inode,
                generation,
                base_end,
                receipt_source,
            } = edited.file.as_ref().unwrap()
            else {
                panic!("dirty data became lazy")
            };
            let image = &plan.snapshot.files[source_inode];
            assert_eq!(*generation, image.metadata.generation);
            assert_eq!(*base_end, image.base_end);
            assert!(receipt_source.is_none());
            assert_eq!(edited.entry.base.as_ref().unwrap().key, image.node.key);
            assert_eq!(edited.entry.authority, image.node.key);
            assert!(std::ptr::eq(
                &layout.source.snapshot.files[source_inode],
                image
            ));
            let clean = layout
                .fresh
                .values()
                .find(|node| node.path == "unrelated")
                .unwrap();
            assert_eq!(clean.file, Some(FileLayout::LazyImmutable));
            assert!(layout.retire.contains(source_inode));
            for node in layout.fresh.values() {
                if let Some(base) = &node.entry.base {
                    assert_eq!(base.key, node.entry.authority);
                }
            }
            let reader = dirty.open(Access::Read).await.unwrap();
            assert_eq!(dirty.read(reader, 0, 8).await.unwrap(), b"local!!!");
            dirty.release(reader).await.unwrap();
            unchanged_metadata(&before, &f.namespace.snapshot(100).await.unwrap());
            assert_eq!(before.files, f.namespace.snapshot(100).await.unwrap().files);
            drop(layout);
            plan.validate_source().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn layout_uses_retained_namespace_limits_and_never_advances_live_high_water() {
    LORE_CONTEXT
        .scope(execution(), async {
            for directory_limit in [false, true] {
                let f = fixture().await;
                let limits = crate::namespace::Limits {
                    max_inodes: if directory_limit { 100 } else { 8 },
                    max_directory_entries: if directory_limit { 2 } else { 100 },
                };
                let namespace = Arc::new(
                    Namespace::new(
                        f.namespace.backend().unwrap(),
                        f.namespace.getattr(1).await.unwrap().base.unwrap(),
                        f.gate,
                        limits,
                    )
                    .unwrap(),
                );
                if !directory_limit {
                    namespace.lookup(1, b"edited").await.unwrap();
                }
                let before = namespace.snapshot(0).await.unwrap();
                let plan = prepare(
                    namespace.clone(),
                    f.scope,
                    f.target,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap();
                if !directory_limit {
                    assert!(plan.candidate.len() <= limits.max_inodes);
                }
                let bound = binding(&plan).await;
                assert!(matches!(
                    plan.prepare_install_layout(bound).await,
                    Err(Error::Backend(core::Error::ResourceLimit))
                ));
                unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
                plan.validate_source().await.unwrap();
            }
            let f = fixture().await;
            let root = f.namespace.getattr(1).await.unwrap().base.unwrap();
            let mut source = f.namespace.snapshot(0).await.unwrap();
            source.next_inode = u64::MAX - 1;
            let namespace = Arc::new(
                Namespace::restore(
                    f.namespace.backend().unwrap(),
                    root,
                    f.gate,
                    source.clone(),
                    BTreeMap::new(),
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let plan = prepare(
                namespace.clone(),
                f.scope,
                f.target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let bound = binding(&plan).await;
            assert!(matches!(
                plan.prepare_install_layout(bound).await,
                Err(Error::Backend(core::Error::ResourceLimit))
            ));
            unchanged_metadata(&source, &namespace.snapshot(0).await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn selected_nonroot_scope_gets_fresh_root_and_only_parent_mapping_bookkeeping() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([233; 16])).await;
            let source = State::new();
            add(&source, &child, 0, "file", false, 1).await;
            let pin = commit(&child, source).await;
            let target = State::new();
            add(&target, &child, 0, "file", false, 2).await;
            let target = Address::zero_context_hash(commit(&child, target).await);
            let state = State::new();
            add(&state, &parent, 0, "outside", false, 3).await;
            link_node(&state, &parent, &child, pin, "mount").await;
            let parent_pin = commit(&parent, state).await;
            let backend = Arc::new(RevisionBackend::new(parent, parent_pin).await.unwrap());
            let gate = Arc::new(Gate::default());
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    backend.root(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let selected = namespace.lookup(1, b"mount").await.unwrap();
            let outside = namespace.lookup(1, b"outside").await.unwrap();
            let before = namespace.snapshot(0).await.unwrap();
            let provenance = backend.checkpoint_state().await.unwrap();
            let plan = prepare(
                namespace.clone(),
                selected.authority,
                target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let bound = binding(&plan).await;
            gate.deny_write.store(true, Ordering::SeqCst);
            let layout = plan.prepare_install_layout(bound).await.unwrap();
            assert!(layout.root_inode >= before.next_inode);
            assert!(!layout.fresh.contains_key(&1));
            assert!(!layout.retire.contains(&outside.inode));
            let patch = layout.parent_patch.as_ref().unwrap();
            assert_eq!(
                (
                    patch.parent,
                    patch.spelling,
                    patch.old_child,
                    patch.new_child
                ),
                (1, b"mount".as_slice(), selected.inode, layout.root_inode)
            );
            assert_eq!(layout.fresh[&layout.root_inode].entry.parent, 1);
            assert_eq!(
                layout.fresh[&layout.root_inode].entry.authority.revision,
                target
            );
            unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
            assert_eq!(backend.checkpoint_state().await.unwrap(), provenance);
        })
        .await;
}

#[tokio::test]
async fn unchanged_children_keep_distinct_inodes_dirty_data_and_logical_heads() {
    LORE_CONTEXT.scope(execution(), async {
        let parent = repository().await;
        let child = parent.to_link_context(RepositoryId::from([234; 16])).await;
        let child_pin = revision_with_bytes(&child, b"linked-original").await;
        let child_head = Address::zero_context_hash(revision_with_bytes(&child, b"child-new-head").await);
        let source = State::new(); add(&source, &parent, 0, "file", false, 1).await;
        for name in ["left", "right"] { link_node(&source, &parent, &child, child_pin, name).await; }
        let pin = commit(&parent, source).await;
        let target = State::new(); add(&target, &parent, 0, "file", false, 2).await;
        add(&target, &parent, 0, "extra", false, 3).await;
        for name in ["left", "right"] { link_node(&target, &parent, &child, child_pin, name).await; }
        let target = Address::zero_context_hash(commit(&parent, target).await);
        let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
        let scope = backend.root().key;
        let namespace = Arc::new(Namespace::new(backend.clone(), backend.root(), Arc::new(Gate::default()), Default::default()).unwrap());
        let left = namespace.lookup(1, b"left").await.unwrap();
        let right = namespace.lookup(1, b"right").await.unwrap();
        assert_ne!(left.authority.exposure, right.authority.exposure);
        let file = namespace.lookup(left.inode, b"file").await.unwrap();
        let overlay = namespace.file(file.inode, Arc::new(Store::default()), Default::default()).await.unwrap();
        let writer = overlay.open(Access::Write).await.unwrap(); overlay.write(writer, 0, b"EDIT").await.unwrap(); overlay.release(writer).await.unwrap();
        let baseline = HeldBaseline { scope: left.authority, head: ScopeHead { revision: child_head, root: Some(0) }, sequence: 1,
            parent_projection: Some(ScopeProjection { parent: scope, head: scope.revision, target: Some(ScopePin { repository: child.id, revision: Address::zero_context_hash(child_pin), node: 0 }) }) };
        let freeze = namespace.freeze_operation().await.unwrap();
        namespace.adopt_committed(freeze.token(), Vec::new(), vec![BaselineUpdate { expected: None, next: baseline.clone() }], || Ok(())).await.unwrap();
        drop(freeze);
        let before = namespace.snapshot(100).await.unwrap();
        let plan = prepare(namespace.clone(), scope, target, Options { local: LocalPolicy::KeepLocal, ..Default::default() }, Default::default()).await.unwrap();
        let bound = binding(&plan).await;
        let layout = plan.prepare_install_layout(bound).await.unwrap();
        assert_eq!(layout.children.len(), 2);
        for boundary in [&left, &right] {
            let child = layout.children.iter().find(|child| child.inode == boundary.inode).unwrap();
            assert_eq!(child.scope, boundary.authority);
            assert_eq!(child.new_parent, 1);
            assert!(!layout.retire.contains(&boundary.inode));
            assert!(!layout.fresh.contains_key(&boundary.inode));
            assert_eq!(child.target_link.revision, target);
        }
        assert!(!layout.retire.contains(&file.inode));
        assert!(!layout.fresh.values().any(|node| matches!(node.file.as_ref(), Some(FileLayout::Private { source_inode, .. }) if *source_inode == file.inode)));
        assert_eq!(layout.projections.len(), 1);
        assert_eq!(layout.projections[0].expected, &baseline);
        assert_eq!(layout.projections[0].next.head, target);
        assert_eq!(layout.projections[0].next.target, baseline.parent_projection.as_ref().unwrap().target);
        assert_eq!(layout.source.snapshot.baselines[&left.authority].head, baseline.head);
        assert_eq!(before.files, namespace.snapshot(100).await.unwrap().files);
        unchanged_metadata(&before, &namespace.snapshot(100).await.unwrap());
        let reader = overlay.open(Access::Read).await.unwrap(); assert_eq!(overlay.read(reader, 0, 4).await.unwrap(), b"EDIT"); overlay.release(reader).await.unwrap();
    }).await;
}

#[tokio::test]
async fn cold_proven_children_stay_lazy_and_cannot_alias_exposures() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([235; 16])).await;
            let child_pin = revision_with_bytes(&child, b"linked-original").await;
            let state = State::new();
            for name in ["left", "right"] {
                link_node(&state, &parent, &child, child_pin, name).await;
            }
            let pin = commit(&parent, state).await;
            let target_state = State::new();
            add(&target_state, &parent, 0, "extra", false, 1).await;
            for name in ["left", "right"] {
                link_node(&target_state, &parent, &child, child_pin, name).await;
            }
            let target = Address::zero_context_hash(commit(&parent, target_state).await);
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let root = backend.root();
            let left = backend.lookup(&root.key, b"left").await.unwrap();
            let right = backend.lookup(&root.key, b"right").await.unwrap();
            let namespace = Arc::new(
                Namespace::new(
                    backend,
                    root.clone(),
                    Arc::new(Gate::default()),
                    Default::default(),
                )
                .unwrap(),
            );
            let plan = prepare(
                namespace,
                root.key,
                target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let bound = binding(&plan).await;
            let layout = plan.prepare_install_layout(bound).await.unwrap();
            assert!(layout.children.is_empty());
            for (path, expected) in [("left", left.key), ("right", right.key)] {
                let node = layout
                    .fresh
                    .values()
                    .find(|node| node.path == path)
                    .unwrap();
                assert_eq!(node.entry.authority, expected);
                assert!(node.names.is_none());
                assert!(node.file.is_none());
                let binding = node.child_binding.as_ref().unwrap();
                assert_eq!(binding.scope, expected);
                assert_eq!(binding.target_link.revision, target);
                assert_eq!(binding.target_link.node, if path == "left" { 2 } else { 3 });
            }
        })
        .await;
}

#[tokio::test]
async fn absent_layout_is_refused_before_any_policy_or_namespace_work() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([236; 16])).await;
            let source = State::new();
            let selected = add(&source, &child, 0, "selected", true, 0).await;
            add(&source, &child, selected, "file", false, 1).await;
            let pin = commit(&child, source).await;
            let absent = Address::zero_context_hash(commit(&child, State::new()).await);
            let source = State::new();
            selected_link(&source, &parent, &child, pin, selected, "mount").await;
            let pin = commit(&parent, source).await;
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let gate = Arc::new(Gate::default());
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    backend.root(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let selected = namespace.lookup(1, b"mount").await.unwrap();
            let plan = prepare(
                namespace.clone(),
                selected.authority,
                absent,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            assert!(plan.intent.target.root.is_none());
            let bound = binding(&plan).await;
            let before = namespace.snapshot(0).await.unwrap();
            let checks = gate.source_checks.load(Ordering::SeqCst);
            assert!(matches!(
                plan.prepare_install_layout(bound).await,
                Err(Error::Unsupported("writable absent underlay layout"))
            ));
            assert_eq!(gate.source_checks.load(Ordering::SeqCst), checks);
            unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
        })
        .await;
}

struct SwitchFactory {
    source: Arc<Gate>,
    future: Arc<Gate>,
    calls: std::sync::atomic::AtomicUsize,
}
impl AccessGateFactory for SwitchFactory {
    fn bind(&self, _: Arc<dyn Backend>) -> crate::core::Result<Arc<dyn AccessGate>> {
        Ok(if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.source.clone()
        } else {
            self.future.clone()
        })
    }
}
#[tokio::test]
async fn future_policy_denial_and_stale_source_cannot_produce_a_layout() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let future = Arc::new(Gate::default());
            future.deny_source.store(true, Ordering::SeqCst);
            let factory = Arc::new(SwitchFactory {
                source: f.gate.clone(),
                future,
                calls: std::sync::atomic::AtomicUsize::new(0),
            });
            let namespace = Arc::new(
                Namespace::new_with_gate_factory(
                    f.namespace.backend().unwrap(),
                    f.namespace.getattr(1).await.unwrap().base.unwrap(),
                    factory,
                    Default::default(),
                )
                .unwrap(),
            );
            let plan = prepare(
                namespace.clone(),
                f.scope,
                f.target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let bound = binding(&plan).await;
            assert!(matches!(
                plan.prepare_install_layout(bound).await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            plan.validate_source().await.unwrap();
            drop(plan);
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let bound = binding(&plan).await;
            f.namespace.lookup(1, b"Dir").await.unwrap();
            assert!(matches!(
                plan.prepare_install_layout(bound).await,
                Err(Error::StaleSource)
            ));
        })
        .await;
}
