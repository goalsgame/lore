// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;

struct Factory;
impl crate::linux_working::StoreFactory for Factory {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async { Ok(Arc::new(Store::default()) as Arc<dyn SparseStore>) })
    }
}
#[tokio::test]
async fn publication_slots_are_fresh_for_prepared_and_complete_backend_forks() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = fixture().await;
            let directory = crate::digest_index::tests::Directory::new();
            std::fs::create_dir(&directory.0).unwrap();
            let model = crate::linux_working::Model::new(
                fixture.namespace.clone(),
                Arc::new(Factory),
                Arc::new(crate::storage_budget::StorageBudget::new(1024)),
                crate::recovery::Recovery::open(directory.0.join("recovery"), Default::default())
                    .unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
            let owner = fixture.namespace.backend().unwrap();
            let backend = owner
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let plan = prepare(
                fixture.namespace.clone(),
                fixture.scope,
                fixture.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap().backend();
            let policy = fixture.namespace.transition_policy().unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let origin = backend
                .transition_view(
                    fixture.scope,
                    fixture.scope.revision,
                    policy.clone(),
                    &mut budget,
                )
                .await
                .unwrap();
            let target = backend
                .transition_view(fixture.scope, fixture.target, policy, &mut budget)
                .await
                .unwrap();
            let prepared = backend
                .transition_fork(fixture.scope, &origin, &target, &mut budget)
                .await
                .unwrap()
                .unwrap();
            assert!(!Arc::ptr_eq(
                backend.publication_slot(),
                complete.publication_slot()
            ));
            assert!(!Arc::ptr_eq(
                backend.publication_slot(),
                prepared.publication_slot()
            ));
            assert!(!Arc::ptr_eq(
                complete.publication_slot(),
                prepared.publication_slot()
            ));
            assert!(Arc::ptr_eq(
                &backend.shared_mutation_gate(),
                &complete.shared_mutation_gate()
            ));
            assert!(Arc::ptr_eq(
                &backend.shared_mutation_gate(),
                &prepared.shared_mutation_gate()
            ));
            drop(plan);
            model.shutdown().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn successor_issuer_refuses_independent_source_backend_with_shared_metadata_gate() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let source = f.namespace.backend().unwrap();
            let provenance = source.checkpoint_state().await.unwrap();
            let replay = source.prepare_restore(&provenance).await.unwrap().unwrap();
            let root = f.namespace.getattr(1).await.unwrap().base.unwrap();
            let independent = Arc::new(
                Namespace::new(replay.clone(), root, f.gate.clone(), Default::default()).unwrap(),
            );
            let source = source
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let replay = replay
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            assert!(Arc::ptr_eq(
                &source.shared_mutation_gate(),
                &replay.shared_mutation_gate()
            ));
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let prepared = plan.prepare_complete_backend().await.unwrap();
            assert!(f.namespace.prepare_successor_issuer(&prepared).is_ok());
            assert!(independent.prepare_successor_issuer(&prepared).is_err());
        })
        .await;
}
