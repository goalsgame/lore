// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

#[tokio::test]
async fn candidate_outlives_plan_and_preserves_files_receipts_admission_and_freeze() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(
                Limits {
                    max_requests: 1,
                    ..Default::default()
                },
                4096,
                b"DIRTY123",
                true,
            )
            .await;
            let old = f.model.open(f.entry.inode, READ).await.unwrap();
            let old_lease = f.model.file_handle(old).unwrap();
            let original = f
                .model
                .namespace
                .snapshot(4096)
                .await
                .unwrap()
                .original_root;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let copied_inode = *resources.files.keys().next().unwrap();
            let copied = resources.files[&copied_inode].file.clone();
            let candidate = resources.into_owned_candidate().await.unwrap();
            drop(plan);
            // No borrowed data remain: this is eligible for a later owned worker,
            // but still confers no publication/anchor ownership.
            fn owned_send<T: Send + 'static>(_: &T) {}
            owned_send(&candidate);
            assert_eq!(candidate.state.test_original_root(), &original);
            let entries = candidate.state.test_entries();
            assert!(
                entries
                    .iter()
                    .any(|(entry, attached)| entry.inode == copied_inode && *attached)
            );
            assert!(
                entries
                    .iter()
                    .any(|(entry, attached)| entry.inode == f.entry.inode && !*attached)
            );
            assert_eq!(
                candidate.state.test_index(copied.overlay.key()),
                Some(copied_inode)
            );
            assert_eq!(
                candidate.state.test_baseline(f.scope).head.revision,
                f.target
            );
            assert_eq!(candidate.state.test_baseline(f.scope).sequence, 1);
            assert!(Arc::ptr_eq(
                &candidate.state.test_receipt(copied_inode),
                &copied.overlay.receipt_cell()
            ));
            assert!(Arc::ptr_eq(
                candidate.files[&copied_inode]
                    .lock()
                    .await
                    .as_ref()
                    .unwrap(),
                &copied
            ));
            assert!(matches!(
                f.model.read(old, 0, 8).await,
                Err(crate::linux_working::model::Error::Core(core::Error::Busy))
            ));
            assert_eq!(
                old_lease
                    .file
                    .overlay
                    .read(old_lease.handle().unwrap(), 0, 8)
                    .await
                    .unwrap(),
                b"DIRTY123"
            );
            assert_eq!(f.model.requests.available_permits(), 0);
            assert!(f.model.namespace.freeze_operation().await.is_err());
            assert_eq!(
                f.model.namespace.lookup(1, b"file").await.unwrap().inode,
                f.entry.inode
            );
            drop(candidate);
            assert_eq!(f.model.requests.available_permits(), 1);
            let freeze = f.model.namespace.freeze_operation().await.unwrap();
            drop(freeze);
            f.model.release(old).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn owned_conversion_denial_releases_admission_without_publishing() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let before = f.model.files.lock().await.len();
            f.gate.target_read.store(true, Ordering::Release);
            assert!(resources.into_owned_candidate().await.is_err());
            assert_eq!(f.model.files.lock().await.len(), before);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            f.gate.target_read.store(false, Ordering::Release);
            assert_eq!(
                f.model.namespace.lookup(1, b"file").await.unwrap().inode,
                f.entry.inode
            );
            drop(plan);
            let freeze = f.model.namespace.freeze_operation().await.unwrap();
            drop(freeze);
        })
        .await;
}

#[tokio::test]
async fn conversion_rejects_substituted_binding_and_metadata_budget_before_publication() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let other = Fixture::new(Default::default(), 4096, b"OTHER123", false).await;
            let plan = f.plan().await;
            let other_plan = other.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let other_resources = other
                .model
                .prepare_private_resources(other.layout(&other_plan).await)
                .await
                .unwrap();
            let PreparedPrivateResources {
                layout,
                view,
                files,
                admission,
                mut budget,
            } = resources;
            drop(view);
            drop(files);
            drop(admission);
            let recipe = layout.into_owned(&mut budget).unwrap();
            assert!(
                f.model
                    .namespace
                    .prepare_owned_state(recipe, other_resources.view, BTreeMap::new(), &mut budget)
                    .await
                    .is_err()
            );
            drop(other_plan);
            drop(plan);
            let next = f.plan().await;
            let mut resources = f
                .model
                .prepare_private_resources(f.layout(&next).await)
                .await
                .unwrap();
            let before = f.stores.count.load(Ordering::Acquire);
            resources.budget =
                crate::transition_prepare::Budget::new(crate::transition_prepare::Limits {
                    max_nodes: 1,
                    ..Default::default()
                })
                .unwrap();
            assert!(resources.into_owned_candidate().await.is_err());
            assert_eq!(f.stores.count.load(Ordering::Acquire), before);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            assert_eq!(
                f.model.namespace.lookup(1, b"file").await.unwrap().inode,
                f.entry.inode
            );
        })
        .await;
}

#[tokio::test]
async fn cancelled_or_closed_conversion_releases_same_admission_and_keeps_plan_freeze() {
    LORE_CONTEXT.scope(execution(), async {
        for close in [false, true] {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let resources = f.model.prepare_private_resources(f.layout(&plan).await).await.unwrap();
            let stores = f.stores.count.load(Ordering::Acquire);
            f.gate.pause_read.store(true, Ordering::Release);
            let mut conversion = Box::pin(resources.into_owned_candidate());
            tokio::select! {
                _ = f.gate.read_entered.notified() => {}
                result = &mut conversion => panic!("conversion unexpectedly completed: {}", result.is_ok()),
            }
            if close {
                f.model.close_requests();
                assert!(tokio::time::timeout(Duration::from_secs(2), &mut conversion).await.unwrap().is_err());
            }
            drop(conversion);
            f.gate.pause_read.store(false, Ordering::Release);
            assert_eq!(f.model.requests.available_permits(), f.model.limits.max_requests);
            assert_eq!(f.stores.count.load(Ordering::Acquire), stores);
            assert!(f.model.namespace.freeze_operation().await.is_err());
            assert_eq!(f.model.namespace.lookup(1, b"file").await.unwrap().inode, f.entry.inode);
            drop(plan);
            if !close { drop(f.model.namespace.freeze_operation().await.unwrap()); }
        }
    }).await;
}

#[tokio::test]
async fn same_key_fresh_file_cannot_substitute_backend_gate_or_source_primitives() {
    LORE_CONTEXT
        .scope(execution(), async {
            for wrong in 0..4 {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let plan = f.plan().await;
                let mut resources = f
                    .model
                    .prepare_private_resources(f.layout(&plan).await)
                    .await
                    .unwrap();
                let spec = resources.layout.private_files().next().unwrap().unwrap();
                let inode = spec.new_inode;
                let backend = if wrong == 0 {
                    f.model.namespace.backend().unwrap()
                } else {
                    resources.view.backend()
                };
                let gate: Arc<dyn AccessGate> = if wrong == 1 {
                    f.gate.clone()
                } else {
                    resources.view.gate()
                };
                let copy = OverlayFile::copy_private(
                    spec.image,
                    spec.source_inode,
                    inode,
                    backend,
                    gate,
                    f.model.store().await.unwrap(),
                    f.model.limits.file,
                )
                .await
                .unwrap();
                let copy = if wrong == 2 {
                    copy
                } else {
                    f.model.namespace.attach_prepared_file(copy)
                };
                let copy = if wrong == 3 {
                    copy.with_mutation_gate(Arc::new(
                        crate::namespace::metadata_gate::MutationGate::default(),
                    ))
                } else {
                    copy
                };
                let copy = Arc::new(copy);
                assert_eq!(copy.key(), resources.files[&inode].file.overlay.key());
                resources.files.insert(
                    inode,
                    PreparedFile {
                        receipt: copy.receipt_cell(),
                        file: Arc::new(File {
                            overlay: copy,
                            mutations: AsyncMutex::new(()),
                        }),
                    },
                );
                assert!(
                    resources.into_owned_candidate().await.is_err(),
                    "foreign binding case {wrong}"
                );
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests
                );
                assert_eq!(
                    f.model.namespace.lookup(1, b"file").await.unwrap().inode,
                    f.entry.inode
                );
                drop(plan);
            }
        })
        .await;
}
