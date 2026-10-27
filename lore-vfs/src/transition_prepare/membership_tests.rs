// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::revision::OwnerMembershipKind;

#[tokio::test]
async fn registered_versions_with_reused_node_ids_keep_exact_keys_and_original_scope() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([241; 16])).await;
            let child_pin = commit(&child, State::new()).await;
            let original = State::new();
            selected_link(&original, &parent, &child, child_pin, 0, "linked").await;
            let pin = commit(&parent, original).await;
            let changed = State::new();
            let reused = add(&changed, &parent, 0, "new-file", false, 99).await;
            selected_link(&changed, &parent, &child, child_pin, 0, "linked").await;
            let target = Address::zero_context_hash(commit(&parent, changed).await);
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let root = backend.root();
            backend.lookup(&root.key, b"linked").await.unwrap();
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let namespace =
                Arc::new(Namespace::new(backend, root.clone(), gate, Default::default()).unwrap());
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
            let mut budget = Budget::new(Limits::default()).unwrap();
            let old_link = NodeKey {
                node: reused,
                ..root.key
            };
            let new_file = NodeKey {
                revision: target,
                node: reused,
                ..root.key
            };
            let boundary = current
                .prove_owner_membership(&old_link, &mut budget)
                .await
                .unwrap();
            let file = current
                .prove_owner_membership(&new_file, &mut budget)
                .await
                .unwrap();
            let directory = current
                .prove_owner_membership(&complete.selected_root().unwrap().key, &mut budget)
                .await
                .unwrap();
            assert_eq!(boundary.kind(), OwnerMembershipKind::Boundary);
            assert_eq!(boundary.boundary_source_scope(), Some(root.key));
            assert!(boundary.original_scope().is_none());
            assert!(boundary.node().is_none());
            assert_eq!(file.kind(), OwnerMembershipKind::Member);
            assert_eq!(file.key(), new_file);
            assert_eq!(file.node().unwrap().kind, crate::core::NodeKind::File);
            assert_eq!(file.original_scope(), Some(root.key));
            assert!(file.same_scope(&directory, &current));
            assert!(!boundary.same_scope(&file, &current));
            assert!(!boundary.same_scope(&boundary, &current));
            assert!(file.matches_key(&current, &new_file));
            assert!(!file.matches_key(&current, &old_link));
        })
        .await;
}

#[tokio::test]
async fn repeated_exposures_and_independent_issuers_never_share_membership() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([242; 16])).await;
            let state = State::new();
            let selected = add(&state, &child, 0, "selected", true, 0).await;
            let inside = add(&state, &child, selected, "inside", false, 1).await;
            let sibling = add(&state, &child, 0, "outside", false, 2).await;
            let pin = commit(&child, state).await;
            let state = State::new();
            for name in ["left", "right"] {
                selected_link(&state, &parent, &child, pin, selected, name).await;
            }
            let pin = commit(&parent, state).await;
            let backend = Arc::new(RevisionBackend::new(parent.clone(), pin).await.unwrap());
            let root = backend.root();
            let left = backend.lookup(&root.key, b"left").await.unwrap();
            let right = backend.lookup(&root.key, b"right").await.unwrap();
            let left_key = NodeKey {
                node: inside,
                ..left.key
            };
            let right_key = NodeKey {
                node: inside,
                ..right.key
            };
            let mut budget = Budget::new(Limits::default()).unwrap();
            let a = backend
                .prove_owner_membership(&left_key, &mut budget)
                .await
                .unwrap();
            let b = backend
                .prove_owner_membership(&right_key, &mut budget)
                .await
                .unwrap();
            assert_eq!(a.original_scope(), Some(left.key));
            assert_eq!(b.original_scope(), Some(right.key));
            assert!(!a.same_scope(&b, &backend));
            assert!(matches!(
                backend
                    .prove_owner_membership(
                        &NodeKey {
                            node: sibling,
                            ..left.key
                        },
                        &mut budget
                    )
                    .await,
                Err(core::Error::PermissionDenied)
            ));
            assert!(
                backend
                    .prove_owner_membership(
                        &NodeKey {
                            revision: Address::zero_context_hash(Hash::from_u64(u64::MAX)),
                            ..left_key
                        },
                        &mut budget
                    )
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .prove_owner_membership(
                        &NodeKey {
                            repository: parent.id,
                            ..left_key
                        },
                        &mut budget
                    )
                    .await
                    .is_err()
            );
            let foreign = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            // Exposure 1 now represents the other route, despite an identical key tuple.
            let foreign_right = foreign.lookup(&root.key, b"right").await.unwrap();
            let foreign_key = NodeKey {
                node: inside,
                ..foreign_right.key
            };
            assert_eq!(foreign_key, left_key);
            let other = foreign
                .prove_owner_membership(&foreign_key, &mut budget)
                .await
                .unwrap();
            assert_eq!(other.original_scope(), a.original_scope());
            assert!(!a.matches_key(&foreign, &left_key));
            assert!(!a.same_scope(&other, &backend));
            assert!(!a.same_scope(&other, &foreign));
        })
        .await;
}

#[tokio::test]
async fn absent_registered_view_has_no_member_or_ancestor_fact() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([243; 16])).await;
            let state = State::new();
            let selected = add(&state, &child, 0, "selected", true, 0).await;
            let pin = commit(&child, state).await;
            let absent = Address::zero_context_hash(commit(&child, State::new()).await);
            let state = State::new();
            selected_link(&state, &parent, &child, pin, selected, "selected").await;
            let pin = commit(&parent, state).await;
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(absent);
            let namespace = Arc::new(
                Namespace::new(backend.clone(), backend.root(), gate, Default::default()).unwrap(),
            );
            let selected = namespace.lookup(1, b"selected").await.unwrap().authority;
            let plan = prepare(
                namespace,
                selected,
                absent,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            let current = complete.backend();
            let mut budget = Budget::new(Limits::default()).unwrap();
            for node in [0, selected.node] {
                assert!(matches!(
                    current
                        .prove_owner_membership(
                            &NodeKey {
                                revision: absent,
                                node,
                                ..selected
                            },
                            &mut budget
                        )
                        .await,
                    Err(core::Error::Stale)
                ));
            }
            let old = current
                .prove_owner_membership(&selected, &mut budget)
                .await
                .unwrap();
            assert_eq!(old.original_scope(), Some(selected));
        })
        .await;
}

#[tokio::test]
async fn raw_link_boundary_does_not_load_or_allocate_its_missing_target() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let missing = repository
                .to_link_context(RepositoryId::from([244; 16]))
                .await;
            let state = State::new();
            let raw = link_node(&state, &repository, &missing, Hash::from_u64(777), "opaque").await;
            let pin = state
                .serialize(repository.clone(), repository.try_write_token().unwrap())
                .await
                .unwrap();
            let backend = RevisionBackend::new(repository, pin).await.unwrap();
            let root = backend.root();
            let key = NodeKey {
                node: raw,
                ..root.key
            };
            let before = backend.checkpoint_state().await.unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let fact = backend
                .prove_owner_membership(&key, &mut budget)
                .await
                .unwrap();
            assert_eq!(fact.kind(), OwnerMembershipKind::Boundary);
            assert_eq!(fact.boundary_source_scope(), Some(root.key));
            assert!(fact.original_scope().is_none());
            assert!(fact.node().is_none());
            assert!(fact.matches_key(&backend, &key));
            assert!(!fact.same_scope(&fact, &backend));
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
        })
        .await;
}

#[tokio::test]
async fn configured_file_scope_root_is_a_member_but_its_sibling_is_not() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([245; 16])).await;
            let state = State::new();
            let file = add(&state, &child, 0, "file", false, 1).await;
            let sibling = add(&state, &child, 0, "other", false, 2).await;
            let pin = commit(&child, state).await;
            let state = State::new();
            selected_link(&state, &parent, &child, pin, file, "file").await;
            let pin = commit(&parent, state).await;
            let backend = RevisionBackend::new(parent, pin).await.unwrap();
            let file = backend.lookup(&backend.root().key, b"file").await.unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let fact = backend
                .prove_owner_membership(&file.key, &mut budget)
                .await
                .unwrap();
            assert_eq!(fact.node().unwrap(), &file);
            assert_eq!(fact.original_scope(), Some(file.key));
            assert!(fact.same_scope(&fact, &backend));
            assert!(matches!(
                backend
                    .prove_owner_membership(
                        &NodeKey {
                            node: sibling,
                            ..file.key
                        },
                        &mut budget
                    )
                    .await,
                Err(core::Error::PermissionDenied)
            ));
        })
        .await;
}

#[tokio::test]
async fn membership_budget_cancellation_and_captured_context_are_independent_of_authorization() {
    let (namespace, backend, key, gate) = LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let backend = f.namespace.backend().unwrap();
            (f.namespace, backend, f.scope, f.gate)
        })
        .await;
    assert!(lore_revision::runtime::try_execution_context().is_none());
    let backend = backend
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
        backend.prove_owner_membership(&key, &mut budget).await,
        Err(core::Error::ResourceLimit)
    ));
    let mut budget = Budget::new(Limits {
        max_name_bytes: 1,
        max_path_bytes: 1,
        ..Limits::default()
    })
    .unwrap();
    let mut pending = Box::pin(backend.prove_owner_membership(&key, &mut budget));
    std::future::poll_fn(|cx| {
        assert!(matches!(
            std::future::Future::poll(pending.as_mut(), cx),
            std::task::Poll::Pending
        ));
        std::task::Poll::Ready(())
    })
    .await;
    drop(pending);
    assert!(lore_revision::runtime::try_execution_context().is_none());
    let fact = backend
        .prove_owner_membership(&key, &mut budget)
        .await
        .unwrap();
    assert_eq!(fact.original_scope(), Some(key));
    assert_eq!(backend.checkpoint_state().await.unwrap(), before);
    gate.deny_source.store(true, Ordering::SeqCst);
    assert!(fact.same_scope(&fact, backend));
    assert!(matches!(
        namespace.getattr(1).await,
        Err(crate::namespace::NamespaceError::Backend(
            core::Error::PermissionDenied
        ))
    ));
}

#[tokio::test]
async fn membership_ancestry_uses_the_callers_depth_budget() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let state = State::new();
            let first = add(&state, &repository, 0, "one", true, 0).await;
            let second = add(&state, &repository, first, "two", true, 0).await;
            let leaf = add(&state, &repository, second, "leaf", false, 19).await;
            let pin = commit(&repository, state).await;
            let backend = RevisionBackend::new(repository, pin).await.unwrap();
            let key = NodeKey {
                node: leaf,
                ..backend.root().key
            };
            let mut budget = Budget::new(Limits {
                max_depth: 1,
                ..Limits::default()
            })
            .unwrap();
            assert!(matches!(
                backend.prove_owner_membership(&key, &mut budget).await,
                Err(core::Error::ResourceLimit)
            ));
            let mut budget = Budget::new(Limits::default()).unwrap();
            let fact = backend
                .prove_owner_membership(&key, &mut budget)
                .await
                .unwrap();
            assert_eq!(fact.original_scope(), Some(backend.root().key));
        })
        .await;
}

#[tokio::test]
async fn retained_subtree_uses_its_registered_root_not_the_original_node_id() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([246; 16])).await;
            let state = State::new();
            let selected = add(&state, &child, 0, "selected", true, 0).await;
            add(&state, &child, selected, "inside", false, 1).await;
            let pin = commit(&child, state).await;
            let changed = State::new();
            let outside = add(&changed, &child, 0, "outside", false, 2).await;
            assert_eq!(outside, selected);
            let new_root = add(&changed, &child, 0, "selected", true, 0).await;
            let inside = add(&changed, &child, new_root, "inside", false, 3).await;
            let target = Address::zero_context_hash(commit(&child, changed).await);
            let state = State::new();
            selected_link(&state, &parent, &child, pin, selected, "selected").await;
            let pin = commit(&parent, state).await;
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let namespace = Arc::new(
                Namespace::new(backend.clone(), backend.root(), gate, Default::default()).unwrap(),
            );
            let scope = namespace.lookup(1, b"selected").await.unwrap().authority;
            let plan = prepare(
                namespace,
                scope,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            let current = complete.backend();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let member = current
                .prove_owner_membership(
                    &NodeKey {
                        revision: target,
                        node: inside,
                        ..scope
                    },
                    &mut budget,
                )
                .await
                .unwrap();
            let root = current
                .prove_owner_membership(
                    &NodeKey {
                        revision: target,
                        node: new_root,
                        ..scope
                    },
                    &mut budget,
                )
                .await
                .unwrap();
            let original = current
                .prove_owner_membership(&scope, &mut budget)
                .await
                .unwrap();
            assert_eq!(member.original_scope(), Some(scope));
            assert!(member.same_scope(&root, &current));
            assert!(member.same_scope(&original, &current));
            assert!(matches!(
                current
                    .prove_owner_membership(
                        &NodeKey {
                            revision: target,
                            node: outside,
                            ..scope
                        },
                        &mut budget
                    )
                    .await,
                Err(core::Error::PermissionDenied)
            ));
        })
        .await;
}
