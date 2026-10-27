// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

#[tokio::test]
async fn captures_reject_foreign_namespaces_and_old_tokens_without_retaining_freeze() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(Limits::default()).await;
            let namespace = f.provider.namespace();
            let lease = namespace.freeze_operation().await.unwrap();
            let routing = namespace.capture_operation_routing(&lease).await.unwrap();
            assert!(Arc::ptr_eq(
                routing.backend(),
                &namespace.backend().unwrap()
            ));
            let other = Arc::new(
                Namespace::new(
                    namespace.backend().unwrap(),
                    namespace.getattr(1).await.unwrap().base.unwrap(),
                    f.gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            assert!(other.capture_operation_routing(&lease).await.is_err());
            assert!(routing.check(&other, lease.token()).is_err());
            let old = lease.token();
            drop(lease);
            assert!(routing.check(namespace, old).is_err());
            let next = namespace.freeze_operation().await.unwrap();
            assert_ne!(next.token(), old);
            assert!(routing.check(namespace, next.token()).is_err());
            drop(next);
        })
        .await;
}

#[tokio::test]
async fn operation_finish_releases_freeze_while_its_captured_backend_is_retained() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(Limits::default()).await;
            let state = f.provider.operation_state().await.unwrap();
            let namespace = f.provider.namespace();
            assert!(Arc::ptr_eq(
                state.routing.backend(),
                &namespace.backend().unwrap()
            ));
            assert!(namespace.freeze_operation().await.is_err());
            state.finish();
            let next = namespace.freeze_operation().await.unwrap();
            assert!(state.routing.check(namespace, state.token).is_err());
            let ran = AtomicBool::new(false);
            let key = namespace.getattr(1).await.unwrap().authority;
            assert!(
                state
                    .source_metadata(&key, "inactive source", async {
                        ran.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                    .is_err()
            );
            assert!(!ran.load(Ordering::SeqCst));
            drop(next);
        })
        .await;
}

#[tokio::test]
async fn source_policy_denial_and_revocation_surround_metadata_work() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(Limits::default()).await;
            let state = f.provider.operation_state().await.unwrap();
            let key = f.provider.namespace().getattr(1).await.unwrap().authority;
            let ran = AtomicBool::new(false);
            f.gate.deny.store(true, Ordering::SeqCst);
            assert!(
                state
                    .source_metadata(&key, "denied metadata", async {
                        ran.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                    .is_err()
            );
            assert!(!ran.load(Ordering::SeqCst));
            f.gate.deny.store(false, Ordering::SeqCst);
            assert!(
                state
                    .source_metadata(&key, "revoked metadata", async {
                        ran.store(true, Ordering::SeqCst);
                        f.gate.deny.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                    .is_err()
            );
            assert!(ran.load(Ordering::SeqCst));
            f.gate.deny.store(false, Ordering::SeqCst);
            assert_eq!(
                state
                    .source_metadata(&key, "permitted metadata", async { Ok(41) })
                    .await
                    .unwrap(),
                41
            );
            state.finish();
        })
        .await;
}

#[tokio::test]
async fn cancellation_of_a_blocked_source_check_never_keeps_writer_exclusion() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture(Limits::default()).await;
            let state = f.provider.operation_state().await.unwrap();
            let key = f.provider.namespace().getattr(1).await.unwrap().authority;
            f.gate.block.store(true, Ordering::SeqCst);
            {
                let pending =
                    state.request(state.source_metadata(&key, "blocked source", async { Ok(()) }));
                tokio::pin!(pending);
                tokio::select! {
                    _ = f.gate.entered.notified() => {},
                    result = &mut pending => panic!("blocked source completed: {}", result.is_ok()),
                }
            }
            f.gate.block.store(false, Ordering::SeqCst);
            state.finish();
            drop(f.provider.namespace().freeze_operation().await.unwrap());
            f.provider.drain_operations().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn checkpoint_lineage_is_rechecked_before_export_admission() {
    LORE_CONTEXT
        .scope(execution(), async {
            let a = fixture_options(Limits::default(), false, false, 0, true).await;
            let b = fixture_options(Limits::default(), false, false, 0, true).await;
            let mut wrong = (*a.provider).clone();
            wrong.checkpoint = Some(b.model.as_ref().unwrap().checkpoint_owner());
            let directory = a._directory.0.join("exports");
            let before = std::fs::read_dir(&directory).unwrap().count();
            assert!(wrong.operation_state().await.is_err());
            assert_eq!(std::fs::read_dir(&directory).unwrap().count(), before);
            drop(a.provider.namespace().freeze_operation().await.unwrap());
        })
        .await;
}

struct ExactReadGate {
    denied: Mutex<Option<NodeKey>>,
}
impl AccessGate for ExactReadGate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if access != Access::Read || *self.denied.lock().unwrap() == Some(*key) {
                Err(Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn restored_capture_keeps_effective_underlay_policy_and_exact_prepared_backend() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture_inner(Limits::default(), true).await;
            let namespace = f.provider.namespace();
            let original = namespace.backend().unwrap();
            let backend = original
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let root = backend.root();
            let head = crate::core::ScopeHead {
                revision: Address::default(),
                root: Some(0),
            };
            let bytes = backend
                .test_view_provenance(&[(root.key, head.clone())], &[], None)
                .unwrap();
            let prepared = original.prepare_restore(&bytes).await.unwrap().unwrap();
            let prepared_backend = prepared
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let target_key = NodeKey {
                revision: head.revision,
                ..root.key
            };
            let target_root = prepared_backend
                .restore_node_metadata(&target_key)
                .await
                .unwrap();
            let mut snapshot = namespace.snapshot(0).await.unwrap();
            assert_eq!(snapshot.nodes.len(), 1);
            snapshot.backend_state = bytes;
            snapshot.active_underlays.insert(root.key, head);
            snapshot.generation += 1;
            snapshot.nodes[0].entry.authority = target_key;
            snapshot.nodes[0].entry.base = Some(target_root);
            snapshot.nodes[0].names = None;
            let gate = Arc::new(ExactReadGate {
                denied: Mutex::new(None),
            });
            let restored = Arc::new(
                Namespace::restore(
                    original.clone(),
                    root.clone(),
                    gate.clone(),
                    snapshot,
                    BTreeMap::new(),
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let provider = WorkingProvider::from_restored_namespace(
                restored.clone(),
                f._directory.0.join("restored-exports"),
                Limits::default(),
            )
            .await
            .unwrap();
            let state = provider.operation_state().await.unwrap();
            assert!(Arc::ptr_eq(
                state.routing.backend(),
                &restored.backend().unwrap()
            ));
            assert!(!Arc::ptr_eq(state.routing.backend(), &original));
            *gate.denied.lock().unwrap() = Some(target_key);
            assert!(gate.authorize(&root.key, Access::Read).await.is_ok());
            // The captured effective gate also checks the installed underlay. It
            // cannot fall back to the raw source policy which still permits this key.
            let ran = AtomicBool::new(false);
            assert!(
                state
                    .source_metadata(&root.key, "retained source", async {
                        ran.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                    .is_err()
            );
            assert!(!ran.load(Ordering::SeqCst));
            *gate.denied.lock().unwrap() = None;
            state
                .source_metadata(
                    &root.key,
                    "retained source",
                    state.routing.backend().owning_scope(&root.key),
                )
                .await
                .unwrap();
            state.finish();
            drop(restored.freeze_operation().await.unwrap());
        })
        .await;
}

struct SourceAnchorOnly {
    anchor: NodeKey,
    seen: Mutex<Vec<NodeKey>>,
}
impl AccessGate for SourceAnchorOnly {
    fn authorize<'a>(&'a self, key: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(*key);
            if *key == self.anchor {
                Ok(())
            } else {
                Err(Error::PermissionDenied)
            }
        })
    }
}

#[tokio::test]
async fn normal_stage_commit_does_not_ask_generic_policy_to_grant_committed_target_keys() {
    LORE_CONTEXT
        .scope(execution(), async {
            let mut f = fixture(Limits::default()).await;
            let backend = Arc::new(
                RevisionBackend::new(f.repository.clone(), Hash::default())
                    .await
                    .unwrap(),
            );
            let root = backend.root();
            let gate = Arc::new(SourceAnchorOnly {
                anchor: root.key,
                seen: Mutex::new(Vec::new()),
            });
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    root.clone(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let provider = Arc::new(
                WorkingProvider::new(
                    namespace,
                    backend,
                    f._directory.0.join("anchor-only-exports"),
                    Limits::default(),
                )
                .unwrap(),
            );
            f.repository = Arc::new(
                RepositoryContext::new(RepositoryContextCreationArgs {
                    paths: f.repository.paths.clone(),
                    immutable_store: f.repository.immutable_store(),
                    mutable_store: f.repository.try_mutable_store_arc().unwrap(),
                    id: f.repository.id,
                    instance_id: f.repository.instance_id,
                    remote: Err(lore_base::error::NoRemote.into()),
                    filter: f.repository.filter.clone(),
                    filesystem_provider: Some(provider.clone()),
                })
                .with_write_token(f.token.share()),
            );
            f.provider = provider;
            write(&f, "file", b"only the original local authority is admitted").await;
            stage(&f, "file").await;
            let revision = lore_revision::commit::commit_boxed(
                f.repository.clone(),
                &f.token,
                lore_revision::commit::CommitOptions::new("source-authorized proof".into()),
            )
            .await
            .unwrap();
            let receipts = f.provider.receipts().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].committed_revision, revision);
            let target = NodeKey {
                revision: Address::zero_context_hash(revision),
                node: 1,
                ..root.key
            };
            assert_ne!(target, root.key);
            assert!(gate.seen.lock().unwrap().iter().all(|key| *key == root.key));
            assert!(gate.authorize(&target, Access::Read).await.is_err());
        })
        .await;
}
