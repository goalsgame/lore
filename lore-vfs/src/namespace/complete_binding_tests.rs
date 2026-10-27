// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::State as RevisionState;

use super::*;
use crate::core::BackendFuture;
use crate::overlay::AccessGateFactory;
use crate::overlay::ProvenTargetMetadata;
use crate::overlay::TargetSelection;

struct Gate;
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_selection<'a>(&'a self, _: &'a TargetSelection) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_target_head<'a>(&'a self, _: &'a ProvenTargetMetadata) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_target_node<'a>(
        &'a self,
        _: &'a ProvenTargetMetadata,
        _: &'a NodeKey,
        _: Access,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
struct Factory {
    calls: AtomicUsize,
    mutate: AtomicBool,
    source: Arc<MutationGate>,
}
impl AccessGateFactory for Factory {
    fn bind(&self, _: Arc<dyn Backend>) -> crate::core::Result<Arc<dyn AccessGate>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.mutate.load(Ordering::SeqCst) {
            drop(self.source.mutation()?);
        }
        Ok(Arc::new(Gate))
    }
}
fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "complete-binding-test".into(),
    ))
}
async fn commit(repository: &Arc<RepositoryContext>, value: u64) -> Hash {
    use lore_revision::node::NodeFlags;
    let state = RevisionState::new();
    let node = state
        .node_add(
            repository.clone(),
            0,
            lore_revision::node::Node {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string("file"),
                address: Address::zero_context_hash(Hash::from_u64(value)),
                size: 4,
                ..Default::default()
            },
            "file",
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            node,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let branch = [u8::try_from(value).unwrap(); 16].into();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        repository.try_write_token().unwrap(),
        state,
        metadata,
        Hash::default(),
        branch,
    )
    .await
    .unwrap()
}
async fn fixture() -> (
    Arc<Namespace>,
    Arc<Factory>,
    Arc<RepositoryContext>,
    Hash,
    Address,
) {
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
        RepositoryId::from([213; 16]),
    ));
    let source = commit(&repository, 1).await;
    let target = Address::zero_context_hash(commit(&repository, 2).await);
    let backend = Arc::new(
        RevisionBackend::new(repository.clone(), source)
            .await
            .unwrap(),
    );
    let factory = Arc::new(Factory {
        calls: AtomicUsize::new(0),
        mutate: AtomicBool::new(false),
        source: backend.shared_mutation_gate(),
    });
    let namespace = Arc::new(
        Namespace::new_with_gate_factory(
            backend.clone(),
            backend.root(),
            factory.clone(),
            Limits::default(),
        )
        .unwrap(),
    );
    (namespace, factory, repository, source, target)
}

#[tokio::test]
async fn complete_binding_rejects_an_independent_source_before_factory_callback() {
    LORE_CONTEXT
        .scope(execution(), async {
            let (namespace, _, repository, source, target) = fixture().await;
            let scope = namespace.original_root.key;
            let plan = crate::transition_prepare::prepare(
                namespace.clone(),
                scope,
                target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let prepared = plan.prepare_complete_backend().await.unwrap();
            let other = Arc::new(RevisionBackend::new(repository, source).await.unwrap());
            let factory = Arc::new(Factory {
                calls: AtomicUsize::new(0),
                mutate: AtomicBool::new(false),
                source: other.shared_mutation_gate(),
            });
            let wrong = Namespace::new_with_gate_factory(
                other.clone(),
                other.root(),
                factory.clone(),
                Limits::default(),
            )
            .unwrap();
            assert!(matches!(
                wrong.bind_complete_backend(prepared),
                Err(NamespaceError::Backend(Error::InvalidInput))
            ));
            assert_eq!(factory.calls.load(Ordering::SeqCst), 1);
        })
        .await;
}

#[tokio::test]
async fn complete_binding_checks_epoch_before_and_after_factory_callback() {
    LORE_CONTEXT
        .scope(execution(), async {
            for before_callback in [false, true] {
                let (namespace, factory, _, _, target) = fixture().await;
                let plan = crate::transition_prepare::prepare(
                    namespace.clone(),
                    namespace.original_root.key,
                    target,
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap();
                let prepared = plan.prepare_complete_backend().await.unwrap();
                if before_callback {
                    drop(namespace.mutation_gate.mutation().unwrap());
                } else {
                    factory.mutate.store(true, Ordering::SeqCst);
                }
                assert!(matches!(
                    namespace.bind_complete_backend(prepared),
                    Err(NamespaceError::Backend(Error::Stale))
                ));
                assert_eq!(
                    factory.calls.load(Ordering::SeqCst),
                    if before_callback { 1 } else { 2 }
                );
            }
        })
        .await;
}

#[tokio::test]
async fn complete_binding_keeps_backend_and_policy_without_namespace_or_freeze() {
    LORE_CONTEXT
        .scope(execution(), async {
            let (namespace, factory, _, _, target) = fixture().await;
            let scope = namespace.original_root.key;
            let weak = Arc::downgrade(&namespace);
            let plan = crate::transition_prepare::prepare(
                namespace.clone(),
                scope,
                target,
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let prepared = plan.prepare_complete_backend().await.unwrap();
            let binding = namespace.bind_complete_backend(prepared).unwrap();
            assert_eq!(factory.calls.load(Ordering::SeqCst), 2);
            drop(plan);
            drop(namespace.freeze_operation().await.unwrap());
            drop(namespace);
            assert!(weak.upgrade().is_none());
            binding
                .policy()
                .authorize(&scope, Access::Read)
                .await
                .unwrap();
            let backend = binding.backend();
            backend.lookup(&scope, b"file").await.unwrap();
            backend
                .lookup(
                    &NodeKey {
                        revision: target,
                        ..scope
                    },
                    b"file",
                )
                .await
                .unwrap();
        })
        .await;
}
