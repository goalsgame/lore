// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;

use super::tests::Base;
use super::tests::Directory;
use super::tests::Gate;
use super::tests::Stores;
use super::tests::root;
use super::*;

fn recovery(directory: &Directory, label: &str) -> Recovery {
    Recovery::open(directory.0.join(label), Default::default()).unwrap()
}
async fn build(
    namespace: Arc<Namespace>,
    recovery: Recovery,
    limits: Limits,
) -> Result<Arc<Model>> {
    Model::new(
        namespace,
        Arc::new(Stores::default()),
        Arc::new(StorageBudget::new(1024)),
        recovery,
        limits,
    )
    .await
}
#[tokio::test]
async fn publication_owner_is_unique_and_sticky_after_close_drop_and_old_freeze() {
    let directory = Directory::new("publication-sticky");
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Base::new()),
            root(),
            Arc::new(Gate::default()),
            Default::default(),
        )
        .unwrap(),
    );
    let model = build(
        namespace.clone(),
        recovery(&directory, "first"),
        Default::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        build(
            namespace.clone(),
            recovery(&directory, "duplicate"),
            Default::default()
        )
        .await,
        Err(Error::Core(core::Error::Busy))
    ));
    let freeze = namespace.freeze_operation().await.unwrap();
    let checkpoint = model.checkpoint_owner();
    let weak = Arc::downgrade(&model);
    model.stop();
    drop(model);
    assert!(
        weak.upgrade().is_none(),
        "claim identity must not retain the Model"
    );
    assert!(matches!(
        build(
            namespace.clone(),
            recovery(&directory, "retained"),
            Default::default()
        )
        .await,
        Err(Error::Core(core::Error::Busy))
    ));
    drop(freeze);
    drop(checkpoint);
    assert!(matches!(
        build(
            namespace,
            recovery(&directory, "retired"),
            Default::default()
        )
        .await,
        Err(Error::Core(core::Error::Busy))
    ));
}
#[tokio::test]
async fn publication_owner_validation_failure_and_unclaimed_freeze_do_not_burn_namespace() {
    let directory = Directory::new("publication-failed");
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Base::new()),
            root(),
            Arc::new(Gate::default()),
            Default::default(),
        )
        .unwrap(),
    );
    let freeze = namespace.freeze_operation().await.unwrap();
    assert!(matches!(
        build(
            namespace.clone(),
            recovery(&directory, "frozen"),
            Default::default()
        )
        .await,
        Err(Error::Core(core::Error::Busy))
    ));
    drop(freeze);
    assert!(matches!(
        build(
            namespace.clone(),
            recovery(&directory, "invalid"),
            Limits {
                max_handles: 0,
                ..Default::default()
            }
        )
        .await,
        Err(Error::Core(core::Error::InvalidInput))
    ));
    let model = build(namespace, recovery(&directory, "retry"), Default::default())
        .await
        .unwrap();
    model.shutdown().await.unwrap();
}
#[derive(Default)]
struct PausedGate {
    paused: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl AccessGate for PausedGate {
    fn authorize<'a>(
        &'a self,
        _key: &'a crate::core::NodeKey,
        _access: Access,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.paused.load(Ordering::Acquire) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            Ok(())
        })
    }
}
#[tokio::test]
async fn publication_owner_cancellation_releases_reservation_and_blocks_freeze_race() {
    let directory = Directory::new("publication-cancel");
    let gate = Arc::new(PausedGate::default());
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Base::new()),
            root(),
            gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    gate.paused.store(true, Ordering::Release);
    {
        let startup = build(
            namespace.clone(),
            recovery(&directory, "cancelled"),
            Default::default(),
        );
        tokio::pin!(startup);
        tokio::select! {
            _=gate.entered.notified()=>{},
            result=&mut startup=>panic!("constructor bypassed paused capture: {}", result.is_ok()),
        }
        assert!(matches!(
            build(
                namespace.clone(),
                recovery(&directory, "racing"),
                Default::default()
            )
            .await,
            Err(Error::Core(core::Error::Busy))
        ));
        assert!(matches!(
            namespace.freeze_operation().await,
            Err(NamespaceError::Busy)
        ));
    }
    gate.paused.store(false, Ordering::Release);
    let model = build(namespace, recovery(&directory, "retry"), Default::default())
        .await
        .unwrap();
    model.shutdown().await.unwrap();
}
#[tokio::test]
async fn publication_owner_rejects_unclaimed_and_unrelated_checkpoint_lineage() {
    let directory = Directory::new("publication-checkpoint");
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(Base::new()),
            root(),
            Arc::new(Gate::default()),
            Default::default(),
        )
        .unwrap(),
    );
    let model = build(
        namespace.clone(),
        recovery(&directory, "owned"),
        Default::default(),
    )
    .await
    .unwrap();
    model
        .checkpoint
        .check_publication_owner(&model.ownership)
        .unwrap();
    let unclaimed = CheckpointOwner::new(
        namespace,
        Arc::new(Stores::default()),
        recovery(&directory, "standalone"),
        1024,
    );
    assert!(matches!(
        unclaimed.check_publication_owner(&model.ownership),
        Err(crate::checkpoint_owner::Error::Core(core::Error::Stale))
    ));
    let other = Arc::new(
        Namespace::new(
            Arc::new(Base::new()),
            root(),
            Arc::new(Gate::default()),
            Default::default(),
        )
        .unwrap(),
    );
    let other_model = build(other, recovery(&directory, "other"), Default::default())
        .await
        .unwrap();
    assert!(matches!(
        model
            .checkpoint
            .check_publication_owner(&other_model.ownership),
        Err(crate::checkpoint_owner::Error::Core(core::Error::Stale))
    ));
    model.shutdown().await.unwrap();
    other_model.shutdown().await.unwrap();
}

#[tokio::test]
async fn publication_owner_rejects_backend_alias_but_allows_independent_restore_with_shared_gate() {
    use lore_base::runtime::LORE_CONTEXT;
    use lore_revision::interface::ExecutionContext;
    use lore_revision::interface::LoreGlobalArgs;
    use lore_revision::relay::EventDispatcher;
    use lore_revision::repository::RepositoryContext;

    use crate::revision::RevisionBackend;

    let execution = Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "publication-owner-test".into(),
    ));
    LORE_CONTEXT
        .scope(execution, async {
            let directory = Directory::new("publication-backend");
            let immutable = lore_storage::local::immutable_store::create(
                None::<&str>,
                lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
                false,
                Default::default(),
            )
            .await
            .unwrap();
            let mutable = lore_storage::local::mutable_store::create(
                None::<&str>,
                Default::default(),
                immutable.clone(),
            )
            .await
            .unwrap();
            let repository = Arc::new(RepositoryContext::new_server_context(
                immutable,
                mutable,
                [201; 16].into(),
            ));
            let backend = Arc::new(
                RevisionBackend::new(repository, Default::default())
                    .await
                    .unwrap(),
            );
            let original_root = backend.root();
            let gate = Arc::new(Gate::default());
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    original_root.clone(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let model = build(
                namespace.clone(),
                recovery(&directory, "first"),
                Default::default(),
            )
            .await
            .unwrap();
            let alias = Arc::new(
                Namespace::new(
                    backend.clone(),
                    original_root.clone(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            assert!(matches!(
                build(alias, recovery(&directory, "alias"), Default::default()).await,
                Err(Error::Core(core::Error::Busy))
            ));
            let snapshot = namespace.snapshot(0).await.unwrap();
            let mut committed = recovery(&directory, "restore");
            committed.publish(&snapshot).unwrap();
            let restored = Model::restore(
                backend.clone(),
                original_root,
                gate,
                Default::default(),
                Arc::new(Stores::default()),
                Arc::new(StorageBudget::new(1024)),
                committed,
                Default::default(),
            )
            .await
            .unwrap();
            let replay = restored.namespace.backend().unwrap();
            let replay = replay
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            assert!(Arc::ptr_eq(
                &backend.shared_mutation_gate(),
                &replay.shared_mutation_gate()
            ));
            assert!(!Arc::ptr_eq(
                backend.publication_slot(),
                replay.publication_slot()
            ));
            model.live().unwrap();
            restored.live().unwrap();
            model.shutdown().await.unwrap();
            restored.shutdown().await.unwrap();
        })
        .await;
}
