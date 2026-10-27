// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::namespace::Entry;
use crate::namespace::SnapshotNode;

#[tokio::test]
async fn mixed_private_base_and_lazy_target_directory_keep_full_version_identity() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let old = State::new();
            add(&old, &repository, 0, "file", false, 11).await;
            let pin = commit(&repository, old).await;
            let target = State::new();
            let cold = add(&target, &repository, 0, "dir", true, 0).await;
            add(&target, &repository, cold, "leaf", false, 22).await;
            add(&target, &repository, 0, "dir-other", false, 33).await;
            let target = Address::zero_context_hash(commit(&repository, target).await);
            let backend = Arc::new(RevisionBackend::new(repository.clone(), pin).await.unwrap());
            let root = backend.root();
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let namespace = Arc::new(
                Namespace::new(backend, root.clone(), gate.clone(), Default::default()).unwrap(),
            );
            let fixture = Fixture {
                repository,
                namespace: namespace.clone(),
                scope: root.key,
                target,
                gate: gate.clone(),
            };
            let _file = dirty(&fixture, b"file").await;
            let plan = prepare(
                namespace.clone(),
                root.key,
                target,
                Options {
                    local: LocalPolicy::KeepLocal,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            let current = complete.backend();
            let mut snapshot = plan.snapshot.clone();
            let private = snapshot.files.clone();
            let file_inode = snapshot
                .nodes
                .iter()
                .find(|node| {
                    node.entry.base.as_ref().is_some_and(|base| {
                        base.key.node == cold && base.key.revision == root.key.revision
                    })
                })
                .unwrap()
                .entry
                .inode;
            let cold_key = NodeKey {
                revision: target,
                node: cold,
                ..root.key
            };
            let cold_node = current.restore_node_metadata(&cold_key).await.unwrap();
            let cold_inode = snapshot.next_inode;
            snapshot.next_inode += 1;
            snapshot.backend_state = complete.provenance().to_vec();
            snapshot.active_underlays.insert(
                root.key,
                ScopeHead {
                    revision: target,
                    root: Some(0),
                },
            );
            let live_root = snapshot
                .nodes
                .iter_mut()
                .find(|node| node.entry.inode == 1)
                .unwrap();
            live_root.entry.base = complete.selected_root().cloned();
            live_root.entry.authority = live_root.entry.base.as_ref().unwrap().key;
            live_root.names = Some(vec![
                (b"file".to_vec(), file_inode),
                (b"DIR".to_vec(), cold_inode),
            ]);
            snapshot.nodes.push(SnapshotNode {
                receipt: None,
                entry: Entry {
                    inode: cold_inode,
                    parent: 1,
                    kind: core::NodeKind::Directory,
                    authority: cold_key,
                    base: Some(cold_node),
                },
                names: None,
                attached: true,
            });
            let mut budget = Budget::new(Limits::default()).unwrap();
            let image = inventory(&snapshot, &current, root.key, 1, gate, &mut budget)
                .await
                .unwrap();
            assert_eq!(image["file"].revision, root.key.revision);
            assert_eq!(image["file"].summary.kind, Kind::File);
            assert!(image["file"].summary.immutable.is_none());
            assert!(image["file"].uncertain);
            assert_eq!(image["file"].local.unwrap().0, file_inode);
            assert_eq!(image["dir"].summary.kind, Kind::Directory);
            assert_eq!(image["dir/leaf"].revision, target);
            assert_eq!(image["dir/leaf"].path, "DIR/leaf");
            assert_eq!(
                image["dir/leaf"].summary.immutable,
                Some(Address::zero_context_hash(Hash::from_u64(22)))
            );
            assert!(!image.contains_key("dir-other"));
            assert_eq!(snapshot.files, private);
            // Public preparation must not enable consumers which still assume a
            // single underlay; this private reader grants no activation rights.
            assert!(
                namespace
                    .backend()
                    .unwrap()
                    .checkpoint_state()
                    .await
                    .is_ok()
            );
        })
        .await;
}

#[tokio::test]
async fn repeated_child_routes_use_the_exact_active_parent_version() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([234; 16])).await;
            let child_state = State::new();
            add(&child_state, &child, 0, "payload-must-not-load", false, 333).await;
            let child_pin = commit(&child, child_state).await;
            let old = State::new();
            for name in ["left", "right"] {
                selected_link(&old, &parent, &child, child_pin, 0, name).await;
            }
            let pin = commit(&parent, old).await;
            let new = State::new();
            add(&new, &parent, 0, "shift", false, 444).await;
            for name in ["left", "right"] {
                selected_link(&new, &parent, &child, child_pin, 0, name).await;
            }
            let target = Address::zero_context_hash(commit(&parent, new).await);
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let root = backend.root();
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    root.clone(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let left = namespace.lookup(1, b"left").await.unwrap();
            let right = namespace.lookup(1, b"right").await.unwrap();
            assert_ne!(left.authority.exposure, right.authority.exposure);
            let plan = prepare(
                namespace,
                root.key,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            let current = complete.backend();
            let mut snapshot = plan.snapshot.clone();
            snapshot.active_underlays.insert(
                root.key,
                ScopeHead {
                    revision: target,
                    root: Some(0),
                },
            );
            let live_root = snapshot
                .nodes
                .iter_mut()
                .find(|node| node.entry.inode == 1)
                .unwrap();
            live_root.entry.base = complete.selected_root().cloned();
            live_root.entry.authority = live_root.entry.base.as_ref().unwrap().key;
            let mut budget = Budget::new(Limits::default()).unwrap();
            let image = inventory(&snapshot, &current, root.key, 1, gate, &mut budget)
                .await
                .unwrap();
            assert_eq!(image["left"].node, 2);
            assert_eq!(image["right"].node, 3);
            assert_eq!(image["left"].revision, target);
            assert_eq!(image["right"].revision, target);
            assert_eq!(
                image["left"].summary.link,
                Some(ScopePin {
                    repository: child.id,
                    revision: Address::zero_context_hash(child_pin),
                    node: 0
                })
            );
            assert!(!image.contains_key("left/payload-must-not-load"));
            assert!(!image.contains_key("right/payload-must-not-load"));
            assert!(matches!(
                backend.inventory_parent_link(root.key, target, left.authority, &mut budget),
                Err(core::Error::Stale)
            ));
        })
        .await;
}

#[tokio::test]
async fn an_existing_but_unregistered_version_is_not_an_inventory_source() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let mut snapshot = f.namespace.snapshot(0).await.unwrap();
            let root = snapshot
                .nodes
                .iter_mut()
                .find(|node| node.entry.inode == 1)
                .unwrap();
            root.entry.authority.revision = f.target;
            root.entry.base.as_mut().unwrap().key.revision = f.target;
            let backend_owner = f.namespace.backend().unwrap();
            let backend = backend_owner
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let before = backend.checkpoint_state().await.unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            assert!(matches!(
                inventory(&snapshot, backend, f.scope, 1, f.gate, &mut budget).await,
                Err(core::Error::Stale)
            ));
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
        })
        .await;
}

#[tokio::test]
async fn registered_inventory_obeys_policy_budget_and_cancellation_without_mutation() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let snapshot = f.namespace.snapshot(0).await.unwrap();
            let backend_owner = f.namespace.backend().unwrap();
            let backend = backend_owner
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let before = backend.checkpoint_state().await.unwrap();
            let mut budget = Budget::new(Limits {
                max_nodes: 1,
                ..Limits::default()
            })
            .unwrap();
            assert!(matches!(
                inventory(&snapshot, backend, f.scope, 1, f.gate.clone(), &mut budget).await,
                Err(core::Error::ResourceLimit)
            ));
            let mut budget = Budget::new(Limits::default()).unwrap();
            let mut reading = Box::pin(inventory(
                &snapshot,
                backend,
                f.scope,
                1,
                f.gate.clone(),
                &mut budget,
            ));
            std::future::poll_fn(|cx| {
                assert!(matches!(
                    std::future::Future::poll(reading.as_mut(), cx),
                    std::task::Poll::Pending
                ));
                std::task::Poll::Ready(())
            })
            .await;
            drop(reading);
            f.gate.deny_source.store(true, Ordering::SeqCst);
            assert!(matches!(
                inventory(&snapshot, backend, f.scope, 1, f.gate.clone(), &mut budget).await,
                Err(core::Error::PermissionDenied)
            ));
            f.gate.deny_source.store(false, Ordering::SeqCst);
            let image = inventory(&snapshot, backend, f.scope, 1, f.gate, &mut budget)
                .await
                .unwrap();
            assert!(!image.is_empty());
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
        })
        .await;
}

struct SelectiveRead {
    inner: Arc<Gate>,
    deny: std::sync::Mutex<Option<NodeKey>>,
    revoke_on_second: Option<(NodeKey, NodeKey)>,
    count: std::sync::atomic::AtomicUsize,
}
impl AccessGate for SelectiveRead {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.inner.authorize(key, access).await?;
            if *self.deny.lock().unwrap() == Some(*key) {
                return Err(core::Error::PermissionDenied);
            }
            if let Some((trigger, revoke)) = self.revoke_on_second
                && *key == trigger
                && self.count.fetch_add(1, Ordering::SeqCst) == 1
            {
                *self.deny.lock().unwrap() = Some(revoke);
            }
            Ok(())
        })
    }
    fn authorize_selection<'a>(
        &'a self,
        selection: &'a crate::overlay::TargetSelection,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a crate::overlay::ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_target_head(proof)
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a crate::overlay::ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.inner.authorize_target_node(proof, key, access).await?;
            if *self.deny.lock().unwrap() == Some(*key) {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn local_anchors_and_earlier_version_keys_are_rechecked_after_other_reads() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let edited = f.namespace.lookup(1, b"edited").await.unwrap();
            let dir = f.namespace.lookup(1, b"Dir").await.unwrap();
            let local = f
                .namespace
                .create_file(
                    dir.inode,
                    b"local",
                    0o644,
                    Arc::new(Store::default()),
                    Default::default(),
                )
                .await
                .unwrap();
            let mut snapshot = f.namespace.snapshot(0).await.unwrap();
            let root = snapshot
                .nodes
                .iter_mut()
                .find(|node| node.entry.inode == 1)
                .unwrap();
            // Stack order: immutable edited file, directory, then entirely local file.
            root.names = Some(vec![
                (b"Dir".to_vec(), dir.inode),
                (b"edited".to_vec(), edited.inode),
            ]);
            snapshot
                .nodes
                .iter_mut()
                .find(|node| node.entry.inode == dir.inode)
                .unwrap()
                .names = Some(vec![(b"local".to_vec(), local.inode)]);
            let backend_owner = f.namespace.backend().unwrap();
            let backend = backend_owner
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let before = backend.checkpoint_state().await.unwrap();
            let policy = Arc::new(SelectiveRead {
                inner: f.gate.clone(),
                deny: std::sync::Mutex::new(Some(local.authority)),
                revoke_on_second: None,
                count: Default::default(),
            });
            let mut budget = Budget::new(Limits::default()).unwrap();
            assert!(matches!(
                inventory(&snapshot, backend, f.scope, 1, policy, &mut budget).await,
                Err(core::Error::PermissionDenied)
            ));
            // Scope/head remain permitted while a later local read revokes an
            // earlier immutable leaf. Returning the collected image must fail.
            let policy = Arc::new(SelectiveRead {
                inner: f.gate.clone(),
                deny: Default::default(),
                revoke_on_second: Some((local.authority, edited.authority)),
                count: Default::default(),
            });
            assert!(matches!(
                inventory(&snapshot, backend, f.scope, 1, policy.clone(), &mut budget).await,
                Err(core::Error::PermissionDenied)
            ));
            assert_eq!(policy.count.load(Ordering::SeqCst), 2);
            assert_eq!(*policy.deny.lock().unwrap(), Some(edited.authority));
            let image = inventory(&snapshot, backend, f.scope, 1, f.gate, &mut budget)
                .await
                .unwrap();
            assert!(image.contains_key("dir/local"));
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
        })
        .await;
}

#[tokio::test]
async fn cold_raw_link_stays_opaque_and_denial_never_resolves_its_missing_target() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([235; 16])).await;
            let state = State::new();
            selected_link(&state, &parent, &child, Hash::from_u64(919), 0, "opaque").await;
            let pin = commit(&parent, state).await;
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let root = backend.root();
            let gate = Arc::new(Gate::default());
            let namespace = Namespace::new(
                backend.clone(),
                root.clone(),
                gate.clone(),
                Default::default(),
            )
            .unwrap();
            let snapshot = namespace.snapshot(0).await.unwrap();
            assert!(snapshot.nodes[0].names.is_none());
            let before = backend.checkpoint_state().await.unwrap();
            let key = NodeKey {
                node: 1,
                ..root.key
            };
            let mut budget = Budget::new(Limits::default()).unwrap();
            let image = inventory(&snapshot, &backend, root.key, 1, gate.clone(), &mut budget)
                .await
                .unwrap();
            assert_eq!(image["opaque"].summary.kind, Kind::Link);
            assert_eq!(
                image["opaque"].summary.link.as_ref().unwrap().repository,
                child.id
            );
            let denied = Arc::new(SelectiveRead {
                inner: gate,
                deny: std::sync::Mutex::new(Some(key)),
                revoke_on_second: None,
                count: Default::default(),
            });
            assert!(matches!(
                inventory(&snapshot, &backend, root.key, 1, denied, &mut budget).await,
                Err(core::Error::PermissionDenied)
            ));
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
        })
        .await;
}
