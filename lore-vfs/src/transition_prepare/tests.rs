// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::node::Node as RevisionNode;
use lore_revision::node::NodeFlags;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::State;

use super::*;
use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::ScopeProjection;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::SparseStore;

#[path = "tests/install_layout.rs"]
mod install_layout;

#[path = "tests/rename_baseline.rs"]
mod rename_baseline;

#[path = "tests/publication_ownership.rs"]
mod publication_ownership;

#[derive(Default)]
struct Gate {
    deny_target: AtomicBool,
    deny_source: AtomicBool,
    target: std::sync::Mutex<Option<Address>>,
    deny_exposure: std::sync::Mutex<Option<u64>>,
    target_root_checks: std::sync::atomic::AtomicUsize,
    pause_final: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    source_checks: std::sync::atomic::AtomicUsize,
    pause_source_at: std::sync::atomic::AtomicUsize,
    block_queries: AtomicBool,
    require_context: AtomicBool,
    target_proof: std::sync::Mutex<Option<crate::overlay::ProvenTargetMetadata>>,
    revoke_after_target_node: AtomicBool,
    allow_candidate: AtomicBool,
    deny_candidate: AtomicBool,
    deny_candidate_parent: AtomicBool,
    pause_candidate: AtomicBool,
    deny_write: AtomicBool,
    deny_write_node: std::sync::Mutex<Option<u32>>,
    revoke_write_during_candidate: AtomicBool,
    candidate_requests: std::sync::Mutex<Vec<crate::overlay::CandidateEdit>>,
    source_writes: std::sync::Mutex<Vec<NodeKey>>,
}
impl AccessGate for Gate {
    fn authorize_candidate_edit<'a>(
        &'a self,
        edit: &'a crate::overlay::CandidateEdit,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if !self.allow_candidate.load(Ordering::SeqCst) {
                return Err(core::Error::Unsupported);
            }
            if self.pause_candidate.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.candidate_requests.lock().unwrap().push(edit.clone());
            if self.deny_candidate.load(Ordering::SeqCst)
                || self.deny_candidate_parent.load(Ordering::SeqCst)
                    && matches!(
                        edit.location(),
                        crate::overlay::CandidateLocation::TargetDirectoryParent { .. }
                    )
            {
                return Err(core::Error::PermissionDenied);
            }
            if self
                .revoke_write_during_candidate
                .swap(false, Ordering::SeqCst)
            {
                self.deny_write.store(true, Ordering::SeqCst);
            }
            Ok(())
        })
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a crate::overlay::ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if *self.target.lock().unwrap() == Some(proof.head().revision) {
                *self.target_proof.lock().unwrap() = Some(proof.clone());
            }
            if let Some(root) = proof.root() {
                self.authorize(&root.key, Access::Read).await
            } else if self.deny_target.load(Ordering::SeqCst)
                && *self.target.lock().unwrap() == Some(proof.head().revision)
                && self
                    .deny_exposure
                    .lock()
                    .unwrap()
                    .is_none_or(|id| id == proof.selection().original_scope().exposure)
            {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
    fn authorize_target_node<'a>(
        &'a self,
        _proof: &'a crate::overlay::ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        if access == Access::Write {
            return Box::pin(async { Err(core::Error::ReadOnly) });
        }
        Box::pin(async move {
            let result = self.authorize(key, access).await;
            if self.revoke_after_target_node.swap(false, Ordering::SeqCst) {
                self.deny_target.store(true, Ordering::SeqCst);
            }
            result
        })
    }
    fn authorize_selection<'a>(
        &'a self,
        selection: &'a crate::overlay::TargetSelection,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.require_context.load(Ordering::SeqCst)
                && lore_revision::runtime::try_execution_context().is_none()
            {
                return Err(core::Error::PermissionDenied);
            }
            let target = *self.target.lock().unwrap();
            if self.deny_target.load(Ordering::SeqCst)
                && target == Some(selection.requested_revision())
                && self
                    .deny_exposure
                    .lock()
                    .unwrap()
                    .is_none_or(|exposure| exposure == selection.original_scope().exposure)
            {
                return Err(core::Error::PermissionDenied);
            }
            Ok(())
        })
    }
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if access == Access::Write {
                self.source_writes.lock().unwrap().push(*key);
                if self.deny_write.load(Ordering::SeqCst)
                    || *self.deny_write_node.lock().unwrap() == Some(key.node)
                {
                    return Err(core::Error::PermissionDenied);
                }
            }
            if self.require_context.load(Ordering::SeqCst)
                && lore_revision::runtime::try_execution_context().is_none()
            {
                return Err(core::Error::PermissionDenied);
            }
            if self.block_queries.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            let target = *self.target.lock().unwrap();
            if target != Some(key.revision) && key.node == 0 {
                let count = self.source_checks.fetch_add(1, Ordering::SeqCst) + 1;
                if count == self.pause_source_at.load(Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.resume.notified().await;
                }
            }
            if target == Some(key.revision) && key.node == 0 {
                self.target_root_checks.fetch_add(1, Ordering::SeqCst);
            } else if self.pause_final.load(Ordering::SeqCst)
                && self.target_root_checks.load(Ordering::SeqCst) >= 3
            {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            if access == Access::Write
                && (self.deny_write.load(Ordering::SeqCst)
                    || *self.deny_write_node.lock().unwrap() == Some(key.node))
                || self.deny_source.load(Ordering::SeqCst) && target != Some(key.revision)
                || self.deny_target.load(Ordering::SeqCst)
                    && target == Some(key.revision)
                    && self
                        .deny_exposure
                        .lock()
                        .unwrap()
                        .is_none_or(|exposure| exposure == key.exposure)
            {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn final_target_authorization_and_cancelled_preparation_release_freeze() {
    LORE_CONTEXT.scope(execution(), async {
        let f=fixture().await;
        f.gate.pause_final.store(true, Ordering::SeqCst);
        let prepare = prepare(f.namespace.clone(),f.scope,f.target,Options::default(),Limits::default());
        tokio::pin!(prepare);
        tokio::select! {
            _ = f.gate.entered.notified()=>{},
            result=&mut prepare=>panic!("preparation bypassed final gate: {}",result.is_ok()),
        }
        f.gate.pause_final.store(false, Ordering::SeqCst);
        f.gate.deny_target.store(true,Ordering::SeqCst);
        f.gate.resume.notify_one();
        assert!(matches!(tokio::time::timeout(std::time::Duration::from_secs(5), prepare).await.unwrap(),Err(Error::Backend(core::Error::PermissionDenied))));
        f.gate.pause_final.store(false,Ordering::SeqCst);
        drop(f.namespace.freeze_operation().await.unwrap());
        f.gate.deny_target.store(false,Ordering::SeqCst);
        f.gate.pause_final.store(true,Ordering::SeqCst);
        f.gate.target_root_checks.store(0,Ordering::SeqCst);
        {
            let prepare=super::prepare(f.namespace.clone(),f.scope,f.target,Options::default(),Limits::default());
            tokio::pin!(prepare);
            tokio::select! {
                _=f.gate.entered.notified()=>{},
                result=&mut prepare=>panic!("preparation bypassed cancellation gate: {}",result.is_ok()),
            }
        }
        f.gate.pause_final.store(false,Ordering::SeqCst);
        drop(f.namespace.freeze_operation().await.unwrap());
    }).await;
}
#[derive(Default)]
struct Store(tokio::sync::Mutex<Vec<u8>>);
impl SparseStore for Store {
    fn read(&self, offset: u64, size: usize) -> BackendFuture<'_, Vec<u8>> {
        Box::pin(async move {
            Ok(self.0.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut data = self.0.lock().await;
            data.resize(offset as usize + bytes.len(), 0);
            data[offset as usize..].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}
fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "transition-test".into(),
    ))
}
struct Fixture {
    repository: Arc<RepositoryContext>,
    namespace: Arc<Namespace>,
    scope: NodeKey,
    target: Address,
    gate: Arc<Gate>,
}
async fn repository() -> Arc<RepositoryContext> {
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
    Arc::new(RepositoryContext::new_server_context(
        immutable,
        mutable,
        RepositoryId::from([211; 16]),
    ))
}
async fn add(
    state: &State,
    repository: &Arc<RepositoryContext>,
    parent: u32,
    name: &str,
    directory: bool,
    value: u64,
) -> u32 {
    let id = state
        .node_add(
            repository.clone(),
            parent,
            RevisionNode {
                flags: if directory { 0 } else { NodeFlags::File.bits() },
                name_hash: lore_storage::hash::hash_string(name),
                // Deliberately absent payload: preparation must only inspect metadata.
                address: if directory {
                    Address::default()
                } else {
                    Address::zero_context_hash(Hash::from_u64(value))
                },
                size: if directory { 0 } else { 8 },
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            id,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    id
}
async fn commit(repository: &Arc<RepositoryContext>, state: Arc<State>) -> Hash {
    let token = repository.try_write_token().unwrap();
    static NEXT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(1);
    let branch = [NEXT.fetch_add(1, Ordering::Relaxed); 16].into();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        token,
        state,
        metadata,
        Hash::default(),
        branch,
    )
    .await
    .unwrap()
}
async fn fixture() -> Fixture {
    fixture_siblings(0).await
}
async fn fixture_siblings(siblings: usize) -> Fixture {
    let repository = repository().await;
    let source = State::new();
    add(&source, &repository, 0, "edited", false, 1).await;
    add(&source, &repository, 0, "unrelated", false, 2).await;
    let dir = add(&source, &repository, 0, "Dir", true, 0).await;
    add(&source, &repository, dir, "File", false, 3).await;
    add(&source, &repository, 0, "dir-other", false, 4).await;
    add(&source, &repository, 0, "dir.other", false, 5).await;
    for index in 0..siblings {
        add(
            &source,
            &repository,
            0,
            &format!("sibling-{index:04}"),
            false,
            100 + index as u64,
        )
        .await;
    }
    let pin = commit(&repository, source).await;
    let target = State::new();
    add(&target, &repository, 0, "edited", false, 10).await;
    add(&target, &repository, 0, "unrelated", false, 2).await;
    let dir = add(&target, &repository, 0, "Dir", true, 0).await;
    add(&target, &repository, dir, "File", false, 3).await;
    add(&target, &repository, 0, "dir-other", false, 4).await;
    add(&target, &repository, 0, "dir.other", false, 5).await;
    for index in 0..siblings {
        add(
            &target,
            &repository,
            0,
            &format!("sibling-{index:04}"),
            false,
            100 + index as u64,
        )
        .await;
    }
    let target = Address::zero_context_hash(commit(&repository, target).await);
    let backend = Arc::new(RevisionBackend::new(repository.clone(), pin).await.unwrap());
    let root = backend.root();
    let scope = root.key;
    let gate = Arc::new(Gate::default());
    *gate.target.lock().unwrap() = Some(target);
    let namespace =
        Arc::new(Namespace::new(backend, root, gate.clone(), Default::default()).unwrap());
    Fixture {
        repository,
        namespace,
        scope,
        target,
        gate,
    }
}

#[tokio::test]
async fn ordinary_content_conflict_with_thousand_siblings_fits_default_budget() {
    LORE_CONTEXT.scope(execution(), async {
        let f=fixture_siblings(1024).await;
        dirty(&f,b"edited").await;
        let result=prepare(f.namespace.clone(),f.scope,f.target,Options::default(),Limits::default()).await;
        assert!(matches!(result,Err(Error::Conflicts(ref conflicts)) if conflicts.len()==1 && conflicts[0].kind==ConflictKind::Content && conflicts[0].path==b"edited"));
        drop(f.namespace.freeze_operation().await.unwrap());
    }).await;
}

async fn revision_with_bytes(repository: &Arc<RepositoryContext>, bytes: &[u8]) -> Hash {
    let state = State::new();
    let address = lore_revision::immutable::write_borrowed(
        repository.clone(),
        [214; 16].into(),
        bytes,
        lore_storage::options::WriteOptions::default().with_fixed_size_chunk(3),
    )
    .await
    .unwrap();
    let id = state
        .node_add(
            repository.clone(),
            0,
            RevisionNode {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string("file"),
                address,
                size: bytes.len() as u64,
                ..Default::default()
            },
            "file",
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            id,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    commit(repository, state).await
}

#[tokio::test]
async fn isolated_target_and_original_keys_use_verified_ranges_and_current_policy() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let old_pin = revision_with_bytes(&repository, b"original bytes").await;
            let target = Address::zero_context_hash(
                revision_with_bytes(&repository, b"verified target bytes").await,
            );
            let backend = Arc::new(RevisionBackend::new(repository, old_pin).await.unwrap());
            let root = backend.root();
            let scope = root.key;
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let ns = Arc::new(
                Namespace::new(backend, root.clone(), gate.clone(), Default::default()).unwrap(),
            );
            let old_state = ns.snapshot(0).await.unwrap().backend_state;
            let view = prepare(
                ns.clone(),
                scope,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap()
            .prepare_target_view()
            .await
            .unwrap();
            let capability = view.backend();
            assert_eq!(
                capability.checkpoint_state().await,
                Err(core::Error::Unsupported)
            );
            assert!(matches!(
                capability.prepare_restore(&[]).await,
                Err(core::Error::Unsupported)
            ));
            let old = core::Core::new(capability.clone(), root).unwrap();
            let old_file = old.lookup(1, b"file").await.unwrap();
            let old_handle = old.open(old_file.inode).unwrap();
            let new = core::Core::new(capability.clone(), view.root().unwrap().clone()).unwrap();
            let target_file = new.lookup(1, b"file").await.unwrap();
            let handle = new.open(target_file.inode).unwrap();
            assert_eq!(new.read(handle, 9, 6).await.unwrap(), b"target");
            assert_eq!(
                old.read(old_handle, 0, 99).await.unwrap(),
                b"original bytes"
            );
            assert_eq!(
                capability
                    .owning_scope(&target_file.node.key)
                    .await
                    .unwrap()
                    .root,
                scope
            );
            assert_eq!(
                capability.digest(&target_file.node.key).await.unwrap(),
                None
            );
            assert!(matches!(
                capability
                    .read(&target_file.node.key, 0, 1024 * 1024 + 1)
                    .await,
                Err(core::Error::InvalidInput)
            ));
            assert!(matches!(
                capability.revalidate(&target_file.node.key, true).await,
                Err(core::Error::ReadOnly)
            ));
            assert_eq!(
                ns.snapshot(0).await.unwrap().backend_state,
                old_state,
                "target keys are not published in v4 provenance"
            );
            gate.deny_source.store(true, Ordering::SeqCst);
            assert_eq!(
                old.read(old_handle, 0, 2).await,
                Err(core::Error::PermissionDenied)
            );
            gate.deny_source.store(false, Ordering::SeqCst);
            gate.source_checks.store(0, Ordering::SeqCst);
            gate.pause_source_at.store(2, Ordering::SeqCst);
            {
            let read = new.read(handle, 0, 99);
            tokio::pin!(read);
            tokio::select! {
                _=gate.entered.notified()=>{},
                result=&mut read=>panic!("read did not reach post-IO authorization: {result:?}"),
            }
            gate.deny_target.store(true, Ordering::SeqCst);
            gate.resume.notify_one();
            assert_eq!(
                read.await,
                Err(core::Error::PermissionDenied),
                "revoked bytes must not be returned"
            );
            }
            gate.deny_target.store(false, Ordering::SeqCst);
            old.release(old_handle).unwrap();
            new.release(handle).unwrap();
            drop(view);
            assert!(
                ns.freeze_operation().await.is_err(),
                "retained backend capabilities own the source freeze"
            );
            drop(new);
            drop(old);
            drop(capability);
            drop(ns.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn target_view_metadata_and_digest_do_not_hydrate_missing_payload() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let view = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap()
            .prepare_target_view()
            .await
            .unwrap();
            let backend = view.backend();
            let root = view.root().unwrap();
            let file = backend.lookup(&root.key, b"edited").await.unwrap();
            assert_eq!(
                backend
                    .content_identity(&file.key)
                    .await
                    .unwrap()
                    .unwrap()
                    .size,
                8
            );
            assert_eq!(backend.digest(&file.key).await.unwrap(), None);
            assert!(
                backend.read(&file.key, 0, 8).await.is_err(),
                "fixture has no payload backing the metadata"
            );
            assert!(
                backend
                    .list(&root.key, 0, 1024)
                    .await
                    .unwrap()
                    .entries
                    .len()
                    > 1
            );
        })
        .await;
}

#[tokio::test]
async fn target_view_rejects_stale_preparation_and_cancellation_releases_ownership() {
    LORE_CONTEXT.scope(execution(),async {
        let f=fixture().await;
        let plan=prepare(f.namespace.clone(),f.scope,f.target,Options::default(),Limits::default()).await.unwrap();
        f.namespace.lookup(1,b"edited").await.unwrap();
        assert!(matches!(plan.prepare_target_view().await,Err(Error::StaleSource)));
        drop(f.namespace.freeze_operation().await.unwrap());
        let plan=prepare(f.namespace.clone(),f.scope,f.target,Options::default(),Limits::default()).await.unwrap();
        f.gate.target_root_checks.store(0,Ordering::SeqCst);
        f.gate.pause_final.store(true,Ordering::SeqCst);
        {
            let create=plan.prepare_target_view();
            tokio::pin!(create);
            tokio::select! {
                _=f.gate.entered.notified()=>{},
                result=&mut create=>panic!("target construction did not reach final source check: {}",result.is_ok()),
            }
        }
        f.gate.pause_final.store(false,Ordering::SeqCst);
        drop(f.namespace.freeze_operation().await.unwrap());
    }).await;
}

#[tokio::test]
#[allow(
    clippy::disallowed_methods,
    reason = "The regression requires tasks without inherited LORE_CONTEXT to verify the backend restores its own exact captured context."
)]
async fn target_view_request_cap_and_cancellation_preserve_captured_context() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let view = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap()
            .prepare_target_view()
            .await
            .unwrap();
            let root = view.root().unwrap().key;
            let backend = view.backend();
            f.gate.require_context.store(true, Ordering::SeqCst);
            f.gate.block_queries.store(true, Ordering::SeqCst);
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..64 {
                let backend = backend.clone();
                tasks.spawn(async move { backend.lookup(&root, b"edited").await });
                tokio::time::timeout(std::time::Duration::from_secs(5), f.gate.entered.notified())
                    .await
                    .unwrap();
            }
            assert_eq!(
                backend.lookup(&root, b"edited").await,
                Err(core::Error::ResourceLimit)
            );
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            f.gate.block_queries.store(false, Ordering::SeqCst);
            // Tokio tasks do not inherit LORE_CONTEXT; the view must restore its
            // exact captured context around the injected policy too.
            let task = tokio::spawn(async move { backend.lookup(&root, b"edited").await });
            assert!(task.await.unwrap().is_ok());
        })
        .await;
}

async fn link_node(
    state: &State,
    parent: &Arc<RepositoryContext>,
    child: &Arc<RepositoryContext>,
    pin: Hash,
    name: &str,
) -> u32 {
    let id = state
        .node_add(
            parent.clone(),
            0,
            RevisionNode {
                flags: NodeFlags::Link.bits(),
                child: 0,
                address: Address {
                    context: child.id.into(),
                    hash: pin,
                },
                name_hash: lore_storage::hash::hash_string(name),
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            parent.clone(),
            id,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    id
}

#[tokio::test]
async fn target_view_reuses_only_exact_proven_child_mappings() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([215; 16])).await;
            let child_pin = revision_with_bytes(&child, b"child bytes").await;
            let source = State::new();
            link_node(&source, &parent, &child, child_pin, "vendor").await;
            let source_pin = commit(&parent, source).await;
            let target = State::new();
            add(&target, &parent, 0, "extra", false, 41).await;
            link_node(&target, &parent, &child, child_pin, "vendor").await;
            let target = Address::zero_context_hash(commit(&parent, target).await);
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), source_pin)
                    .await
                    .unwrap(),
            );
            let root = backend.root();
            let scope = root.key;
            let gate = Arc::new(Gate::default());
            let ns =
                Arc::new(Namespace::new(backend, root, gate.clone(), Default::default()).unwrap());
            let before = ns.snapshot(0).await.unwrap().backend_state;
            let plan = prepare(
                ns.clone(),
                scope,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    plan.prepare_target_view().await,
                    Err(Error::Backend(core::Error::Unsupported))
                ),
                "unresolved original child cannot allocate a private replacement exposure"
            );
            assert_eq!(ns.snapshot(0).await.unwrap().backend_state, before);
            let original_child = ns.lookup(1, b"vendor").await.unwrap();
            let proven = ns.snapshot(0).await.unwrap().backend_state;
            let view = prepare(
                ns.clone(),
                scope,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap()
            .prepare_target_view()
            .await
            .unwrap();
            let capability = view.backend();
            let child_root = capability
                .lookup(&view.root().unwrap().key, b"vendor")
                .await
                .unwrap();
            assert_eq!(child_root.key, original_child.authority);
            let file = capability.lookup(&child_root.key, b"file").await.unwrap();
            assert_eq!(
                capability.read(&file.key, 0, 99).await.unwrap(),
                b"child bytes"
            );
            assert_eq!(
                capability.owning_scope(&file.key).await.unwrap().root,
                original_child.authority
            );
            assert_eq!(
                ns.snapshot(0).await.unwrap().backend_state,
                proven,
                "no target-view exposure allocation"
            );
            drop(capability);
            drop(view);
            let changed_child = revision_with_bytes(&child, b"changed child").await;
            for (name, pin) in [("vendor", changed_child), ("new-vendor", child_pin)] {
                let changed = State::new();
                link_node(&changed, &parent, &child, pin, name).await;
                let changed = Address::zero_context_hash(commit(&parent, changed).await);
                let plan = prepare(
                    ns.clone(),
                    scope,
                    changed,
                    Options::default(),
                    Limits::default(),
                )
                .await
                .unwrap();
                assert!(matches!(
                    plan.prepare_target_view().await,
                    Err(Error::Backend(core::Error::Unsupported))
                ));
                assert_eq!(ns.snapshot(0).await.unwrap().backend_state, proven);
            }
        })
        .await;
}
async fn dirty(fixture: &Fixture, name: &[u8]) -> Arc<crate::overlay::OverlayFile> {
    let entry = fixture.namespace.lookup(1, name).await.unwrap();
    let file = fixture
        .namespace
        .file(entry.inode, Arc::new(Store::default()), Default::default())
        .await
        .unwrap();
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"local!!!").await.unwrap();
    file.release(writer).await.unwrap();
    file
}

#[tokio::test]
async fn absent_payloads_unloaded_subtrees_and_case_rename_are_preserved() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            f.namespace.rename(1, b"Dir", 1, b"DIR").await.unwrap();
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                plan.candidate_node("DIR/File").unwrap().immutable,
                Some(Address::zero_context_hash(Hash::from_u64(3)))
            );
            assert!(
                plan.changes()
                    .iter()
                    .all(|change| change.path != b"DIR/File")
            );
            assert_eq!(
                f.namespace.lookup(1, b"dir").await.unwrap().kind,
                core::NodeKind::Directory
            );
            plan.validate_source().await.unwrap();
            assert!(f.namespace.create_directory(1, b"blocked").await.is_err());
            drop(plan);
            f.namespace.create_directory(1, b"allowed").await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn dirty_content_conflicts_and_explicit_policies_do_not_mutate_source() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let file = dirty(&f, b"edited").await;
            let before = file.metadata().await;
            assert!(matches!(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    f.target,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Conflicts(_))
            ));
            let keep = prepare(
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
            assert_eq!(keep.candidate_node("edited").unwrap().immutable, None);
            assert!(keep.candidate_origin("edited").unwrap().local.is_some());
            drop(keep);
            let overwrite = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options {
                    local: LocalPolicy::OverwriteLocal,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            assert_eq!(
                overwrite.candidate_node("edited").unwrap().immutable,
                Some(Address::zero_context_hash(Hash::from_u64(10)))
            );
            assert_eq!(file.metadata().await, before);
            drop(overwrite);
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 8).await.unwrap(), b"local!!!");
            file.release(reader).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn writer_denial_and_budget_failure_release_freeze() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let file = dirty(&f, b"unrelated").await;
            let writer = file.open(Access::Write).await.unwrap();
            assert!(matches!(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    f.target,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Namespace(NamespaceError::Busy))
            ));
            file.release(writer).await.unwrap();
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert_eq!(plan.candidate_node("unrelated").unwrap().immutable, None);
            assert_eq!(
                plan.candidate_node("edited").unwrap().immutable,
                Some(Address::zero_context_hash(Hash::from_u64(10)))
            );
            drop(plan);
            for limits in [
                Limits {
                    max_nodes: 1,
                    ..Default::default()
                },
                Limits {
                    max_name_bytes: 1,
                    ..Default::default()
                },
                Limits {
                    max_path_bytes: 1,
                    ..Default::default()
                },
                Limits {
                    max_private_data_bytes: 1,
                    ..Default::default()
                },
                Limits {
                    max_comparisons: 0,
                    ..Default::default()
                },
                Limits {
                    max_depth: 1,
                    ..Default::default()
                },
            ] {
                assert!(
                    prepare(
                        f.namespace.clone(),
                        f.scope,
                        f.target,
                        Options::default(),
                        limits
                    )
                    .await
                    .is_err()
                );
                drop(f.namespace.freeze_operation().await.unwrap());
            }
            f.gate.deny_target.store(true, Ordering::SeqCst);
            assert!(matches!(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    f.target,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            drop(f.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn rename_conflicts_require_preservation_or_explicit_refusal() {
    LORE_CONTEXT.scope(execution(), async {
        let f=fixture().await;
        f.namespace.rename(1,b"Dir",1,b"Moved").await.unwrap();
        let target=State::new();
        let dir=add(&target,&f.repository,0,"Dir",true,0).await;
        add(&target,&f.repository,dir,"File",false,31).await;
        let target=Address::zero_context_hash(commit(&f.repository,target).await);
        assert!(matches!(prepare(f.namespace.clone(),f.scope,target,Options::default(),Limits::default()).await,Err(Error::Conflicts(ref conflicts)) if conflicts.iter().any(|conflict|conflict.kind==ConflictKind::Rename)));
        assert!(matches!(prepare(f.namespace.clone(),f.scope,target,Options{local:LocalPolicy::OverwriteLocal,..Default::default()},Limits::default()).await,Err(Error::Unsupported(_))));
        let plan=prepare(f.namespace.clone(),f.scope,target,Options{local:LocalPolicy::KeepLocal,..Default::default()},Limits::default()).await.unwrap();
        assert!(plan.candidate_node("Dir").is_none());
        assert_eq!(plan.candidate_node("Moved/File").unwrap().immutable,Some(Address::zero_context_hash(Hash::from_u64(3))));
    }).await;
}

#[tokio::test]
async fn restored_backend_and_strict_target_errors_use_captured_context() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let snapshot = f.namespace.snapshot(0).await.unwrap();
            let fresh = Arc::new(
                RevisionBackend::new(f.repository.clone(), f.scope.revision.hash)
                    .await
                    .unwrap(),
            );
            let root = fresh.root();
            let restored = Arc::new(
                Namespace::restore(
                    fresh,
                    root,
                    f.gate.clone(),
                    snapshot,
                    BTreeMap::new(),
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let plan = prepare(
                restored.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(plan.candidate_node("edited").is_some());
            drop(plan);
            assert!(matches!(
                prepare(
                    restored.clone(),
                    f.scope,
                    Address::zero_context_hash(Hash::from_u64(99999)),
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Backend(_))
            ));
            drop(restored.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn staged_origin_is_local_state_and_cache_enrichment_invalidates_witness() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let state = State::new();
            add(&state, &repository, 0, "staged", false, 17).await;
            let pin = state
                .serialize(repository.clone(), repository.try_write_token().unwrap())
                .await
                .unwrap();
            let backend = Arc::new(RevisionBackend::new(repository.clone(), pin).await.unwrap());
            let root = backend.root();
            let scope = root.key;
            let ns = Arc::new(
                Namespace::new(
                    backend,
                    root.clone(),
                    Arc::new(Gate::default()),
                    Default::default(),
                )
                .unwrap(),
            );
            let snapshot = ns.snapshot(0).await.unwrap();
            #[cfg(unix)]
            let snapshot = {
                let directory = crate::digest_index::tests::Directory::new();
                std::fs::create_dir(&directory.0).unwrap();
                let mut recovery =
                    crate::recovery::Recovery::open(directory.0.join("staged"), Default::default())
                        .unwrap();
                recovery.publish(&snapshot).unwrap();
                recovery.load().unwrap().unwrap()
            };
            assert!(snapshot.active_underlays.is_empty());
            assert_eq!(snapshot.original_root, root);
            let fresh = Arc::new(RevisionBackend::new(repository, pin).await.unwrap());
            let ns = Arc::new(
                Namespace::restore(
                    fresh,
                    root,
                    Arc::new(Gate::default()),
                    snapshot,
                    BTreeMap::new(),
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let plan = prepare(
                ns.clone(),
                scope,
                Address::default(),
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(
                plan.candidate_node("staged").is_some(),
                "staged origin differs from logical empty head"
            );
            plan.validate_source().await.unwrap();
            ns.lookup(1, b"staged").await.unwrap();
            assert!(matches!(
                plan.validate_source().await,
                Err(Error::StaleSource)
            ));
            drop(plan);
            drop(ns.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn ancestor_type_conflicts_are_conservatively_refused_with_keep_local() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let directory = f.namespace.lookup(1, b"Dir").await.unwrap();
            let entry = f.namespace.lookup(directory.inode, b"File").await.unwrap();
            let file = f
                .namespace
                .file(entry.inode, Arc::new(Store::default()), Default::default())
                .await
                .unwrap();
            let writer = file.open(Access::Write).await.unwrap();
            file.write(writer, 0, b"local!!!").await.unwrap();
            file.release(writer).await.unwrap();
            let target = State::new();
            add(&target, &f.repository, 0, "Dir", false, 22).await;
            let target = Address::zero_context_hash(commit(&f.repository, target).await);
            assert!(matches!(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    target,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Conflicts(_))
            ));
            assert!(matches!(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    target,
                    Options {
                        local: LocalPolicy::KeepLocal,
                        ..Default::default()
                    },
                    Limits::default()
                )
                .await,
                Err(Error::Unsupported(_))
            ));
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 8).await.unwrap(), b"local!!!");
            file.release(reader).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn repeated_link_exposures_use_exact_scopes_and_missing_subtree_is_distinct_from_error() {
    LORE_CONTEXT.scope(execution(), async {
        let parent=repository().await;
        let child=parent.to_link_context(RepositoryId::from([213;16])).await;
        let source=State::new();
        let selected=add(&source,&child,0,"selected",true,0).await;
        add(&source,&child,selected,"file",false,24).await;
        let pin=commit(&child,source).await;
        let target=State::new();
        let target_selected=add(&target,&child,0,"selected",true,0).await;
        add(&target,&child,target_selected,"file",false,25).await;
        let target=Address::zero_context_hash(commit(&child,target).await);
        let absent=Address::zero_context_hash(commit(&child,State::new()).await);
        let parent_state=State::new();
        for name in ["left","right"] {
            parent_state.node_add(parent.clone(),0,RevisionNode{
                flags:NodeFlags::Link.bits(),child:selected,address:Address{context:child.id.into(),hash:pin},
                name_hash:lore_storage::hash::hash_string(name),..Default::default()
            },name).await.unwrap();
        }
        let parent_pin=parent_state.serialize(parent.clone(),parent.try_write_token().unwrap()).await.unwrap();
        let backend=Arc::new(RevisionBackend::new(parent.clone(),parent_pin).await.unwrap());
        let root=backend.root();
        let gate=Arc::new(Gate::default());
        *gate.target.lock().unwrap()=Some(target);
        let ns=Arc::new(Namespace::new(backend,root,gate.clone(),Default::default()).unwrap());
        let left=ns.lookup(1,b"left").await.unwrap();
        let right=ns.lookup(1,b"right").await.unwrap();
        assert_ne!(left.authority.exposure,right.authority.exposure);
        assert_eq!(left.authority.revision,right.authority.revision);
        let before=ns.snapshot(0).await.unwrap();
        let left_plan=prepare(ns.clone(),left.authority,target,Options::default(),Limits::default()).await.unwrap();
        *gate.deny_exposure.lock().unwrap()=Some(left.authority.exposure);
        gate.deny_target.store(true,Ordering::SeqCst);
        assert!(matches!(left_plan.prepare_target_view().await,Err(Error::Backend(core::Error::PermissionDenied))));
        unchanged_metadata(&before,&ns.snapshot(0).await.unwrap());
        assert!(matches!(prepare(ns.clone(),left.authority,target,Options::default(),Limits::default()).await,Err(Error::Backend(core::Error::PermissionDenied))));
        let plan=prepare(ns.clone(),right.authority,target,Options::default(),Limits::default()).await.unwrap();
        assert_eq!(plan.intent().scope(),right.authority);
        assert!(plan.candidate_node("file").is_some());
        drop(plan);
        let plan=prepare(ns.clone(),right.authority,absent,Options::default(),Limits::default()).await.unwrap();
        assert!(plan.intent().target().root.is_none());
        assert_eq!(plan.candidate_len(),0);
        drop(plan);
        gate.deny_target.store(false,Ordering::SeqCst);
        // A successful independent child commit advances its logical head while
        // the staged parent's committed projection remains absent.
        let root=ns.getattr(1).await.unwrap().base.unwrap();
        let mut snapshot=ns.snapshot(0).await.unwrap();
        snapshot.baselines.insert(right.authority,HeldBaseline{
            scope:right.authority,
            head:ScopeHead{revision:target,root:Some(target_selected)},
            sequence:1,
            parent_projection:Some(ScopeProjection{parent:root.key,head:Address::default(),target:None}),
        });
        let restored=Arc::new(Namespace::restore(ns.backend().unwrap(),root.clone(),gate.clone(),snapshot,BTreeMap::new(),Default::default(),Default::default()).await.unwrap());
        let plan=prepare(restored.clone(),root.key,Address::default(),Options::default(),Limits::default()).await.unwrap();
        assert_eq!(plan.intent().protected_children().len(),1);
        assert_eq!(plan.candidate_node("right").unwrap().link.as_ref().unwrap().revision,target);
        drop(plan);
        let next_parent=State::new();
        let id=next_parent.node_add(parent.clone(),0,RevisionNode{
            flags:NodeFlags::Link.bits(),child:selected,address:Address{context:child.id.into(),hash:pin},
            name_hash:lore_storage::hash::hash_string("right"),..Default::default()
        },"right").await.unwrap();
        next_parent.node_mark_staged(parent.clone(),id,NodeFlags::StagedAdd,NodeFlags::DirtyAdd).await.unwrap();
        let next=Address::zero_context_hash(commit(&parent,next_parent).await);
        let result=prepare(restored.clone(),root.key,next,Options::default(),Limits::default()).await;
        assert!(matches!(result,Err(Error::Conflicts(ref conflicts)) if conflicts.iter().any(|conflict| conflict.kind==ConflictKind::ChildProjection)));
    }).await;
}

#[cfg(unix)]
#[tokio::test]
async fn v5_restores_original_root_active_target_and_retained_dirty_extents_separately() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let old_pin = revision_with_bytes(&repository, b"original bytes").await;
            let target =
                Address::zero_context_hash(revision_with_bytes(&repository, b"target bytes").await);
            let backend = Arc::new(
                RevisionBackend::new(repository.clone(), old_pin)
                    .await
                    .unwrap(),
            );
            let original = backend.root();
            let scope = original.key;
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    original.clone(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let fixture = Fixture {
                repository: repository.clone(),
                namespace: namespace.clone(),
                scope,
                target,
                gate: gate.clone(),
            };
            let dirty_file = dirty(&fixture, b"file").await;
            let old_entry = namespace.lookup(1, b"file").await.unwrap();
            let source = namespace.snapshot(1024).await.unwrap();
            let head = ScopeHead {
                revision: target,
                root: Some(0),
            };
            let bytes = backend
                .test_view_provenance(&[(scope, head.clone())], &[], Some(100))
                .unwrap();
            assert!(bytes.starts_with(&source.backend_state));
            let mut image = source.clone();
            image.generation += 1;
            image.backend_state = bytes.clone();
            image.active_underlays.insert(scope, head.clone());
            image
                .nodes
                .iter_mut()
                .find(|record| record.entry.inode == old_entry.inode)
                .unwrap()
                .attached = false;
            let current_root = NodeKey {
                revision: target,
                ..scope
            };
            let root = image
                .nodes
                .iter_mut()
                .find(|record| record.entry.inode == 1)
                .unwrap();
            root.entry.authority = current_root;
            root.entry.base.as_mut().unwrap().key = current_root;
            root.names = None;
            let directory = crate::digest_index::tests::Directory::new();
            std::fs::create_dir(&directory.0).unwrap();
            let mut recovery =
                crate::recovery::Recovery::open(directory.0.join("checkpoint"), Default::default())
                    .unwrap();
            recovery.publish(&source).unwrap();
            recovery.publish(&image).unwrap();
            assert_eq!(recovery.original_root().unwrap(), Some(original.clone()));
            let image = recovery.load().unwrap().unwrap();
            let fresh = Arc::new(
                RevisionBackend::new(repository.clone(), old_pin)
                    .await
                    .unwrap(),
            );
            let mut missing_view = image.clone();
            missing_view.backend_state = source.backend_state.clone();
            assert!(
                Namespace::restore(
                    fresh.clone(),
                    original.clone(),
                    gate.clone(),
                    missing_view,
                    BTreeMap::from([(
                        old_entry.inode,
                        Arc::new(Store::default()) as Arc<dyn SparseStore>
                    )]),
                    Default::default(),
                    Default::default()
                )
                .await
                .is_err()
            );
            let restored = Arc::new(
                Namespace::restore(
                    fresh.clone(),
                    original.clone(),
                    gate.clone(),
                    image.clone(),
                    BTreeMap::from([(
                        old_entry.inode,
                        Arc::new(Store::default()) as Arc<dyn SparseStore>,
                    )]),
                    Default::default(),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            assert_eq!(restored.getattr(1).await.unwrap().authority, current_root);
            assert!(
                restored.held_baselines().await.unwrap().is_empty(),
                "active underlay does not advance logical head"
            );
            assert_eq!(
                restored
                    .backend()
                    .unwrap()
                    .owning_scope(&current_root)
                    .await
                    .unwrap()
                    .root,
                scope
            );
            assert_eq!(
                restored
                    .backend()
                    .unwrap()
                    .owning_scope(&current_root)
                    .await
                    .unwrap()
                    .initial_revision,
                scope.revision
            );
            let retained = restored
                .materialized_file(old_entry.inode)
                .await
                .unwrap()
                .unwrap();
            let read = retained.open(Access::Read).await.unwrap();
            assert_eq!(retained.read(read, 0, 99).await.unwrap(), b"local!!! bytes");
            retained.release(read).await.unwrap();
            let current = restored.lookup(1, b"file").await.unwrap();
            assert_ne!(current.inode, old_entry.inode);
            let file = restored
                .file(
                    current.inode,
                    Arc::new(Store::default()),
                    Default::default(),
                )
                .await
                .unwrap();
            let read = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(read, 0, 99).await.unwrap(), b"target bytes");
            file.release(read).await.unwrap();
            let old_node = restored
                .backend()
                .unwrap()
                .lookup(&scope, b"file")
                .await
                .unwrap();
            assert_eq!(
                restored
                    .backend()
                    .unwrap()
                    .read(&old_node.key, 0, 99)
                    .await
                    .unwrap(),
                b"original bytes"
            );
            assert_eq!(
                restored
                    .backend()
                    .unwrap()
                    .checkpoint_state()
                    .await
                    .unwrap(),
                bytes
            );
            assert_eq!(
                dirty_file.metadata().await.unwrap(),
                source.files[&old_entry.inode].metadata
            );
            for change in 0..6 {
                let mut corrupt = image.clone();
                if change < 2 {
                    let entry = corrupt
                        .nodes
                        .iter_mut()
                        .find(|node| node.entry.inode == old_entry.inode)
                        .unwrap();
                    let base = entry.entry.base.as_mut().unwrap();
                    if change == 0 {
                        base.size += 1;
                    } else {
                        base.mode ^= 0o111;
                    }
                    corrupt.files.get_mut(&old_entry.inode).unwrap().node = base.clone();
                } else if change < 4 {
                    let base = corrupt
                        .nodes
                        .iter_mut()
                        .find(|node| node.entry.inode == 1)
                        .unwrap()
                        .entry
                        .base
                        .as_mut()
                        .unwrap();
                    if change == 2 {
                        base.size += 1;
                    } else {
                        base.mode ^= 0o111;
                    }
                } else if change == 4 {
                    corrupt.files.remove(&old_entry.inode);
                    let entry = corrupt
                        .nodes
                        .iter_mut()
                        .find(|node| node.entry.inode == old_entry.inode)
                        .unwrap();
                    entry.entry.kind = core::NodeKind::Directory;
                    entry.entry.base.as_mut().unwrap().kind = core::NodeKind::Directory;
                    entry.names = Some(Vec::new());
                } else {
                    let authority = corrupt
                        .nodes
                        .iter()
                        .find(|node| node.entry.inode == old_entry.inode)
                        .unwrap()
                        .entry
                        .authority;
                    let inode = corrupt.next_inode;
                    corrupt.next_inode += 1;
                    corrupt.nodes.push(crate::namespace::SnapshotNode {
                        entry: crate::namespace::Entry {
                            inode,
                            parent: 1,
                            kind: core::NodeKind::Directory,
                            authority,
                            base: None,
                        },
                        receipt: None,
                        attached: true,
                        names: Some(Vec::new()),
                    });
                    corrupt
                        .nodes
                        .iter_mut()
                        .find(|node| node.entry.inode == 1)
                        .unwrap()
                        .names = Some(vec![(b"forged-directory".to_vec(), inode)]);
                }
                corrupt
                    .validate(Default::default(), Default::default())
                    .unwrap();
                let probe = Arc::new(Store::default());
                let stores = if corrupt.files.is_empty() {
                    BTreeMap::new()
                } else {
                    BTreeMap::from([(old_entry.inode, probe.clone() as Arc<dyn SparseStore>)])
                };
                assert!(
                    Namespace::restore(
                        fresh.clone(),
                        original.clone(),
                        gate.clone(),
                        corrupt,
                        stores,
                        Default::default(),
                        Default::default()
                    )
                    .await
                    .is_err()
                );
                assert!(
                    probe.0.lock().await.is_empty(),
                    "metadata verification precedes private extent reconstruction"
                );
            }
            assert_eq!(recovery.load().unwrap().unwrap().files, image.files);
            gate.deny_target.store(true, Ordering::SeqCst);
            assert!(restored.getattr(1).await.is_err());
            assert!(
                Namespace::restore(
                    fresh.clone(),
                    original.clone(),
                    gate.clone(),
                    image.clone(),
                    BTreeMap::from([(
                        old_entry.inode,
                        Arc::new(Store::default()) as Arc<dyn SparseStore>
                    )]),
                    Default::default(),
                    Default::default()
                )
                .await
                .is_err()
            );
            gate.deny_target.store(false, Ordering::SeqCst);
            for head in [
                ScopeHead {
                    revision: target,
                    root: Some(999),
                },
                ScopeHead {
                    revision: Address::zero_context_hash(Hash::from_u64(999999)),
                    root: Some(0),
                },
            ] {
                let invalid = backend
                    .test_view_provenance(&[(scope, head)], &[], None)
                    .unwrap();
                assert!(fresh.prepare_restore(&invalid).await.is_err());
            }
            assert_eq!(recovery.original_root().unwrap(), Some(original));
        })
        .await;
}

async fn selected_link(
    state: &State,
    parent: &Arc<RepositoryContext>,
    child: &Arc<RepositoryContext>,
    pin: Hash,
    selected: u32,
    name: &str,
) {
    let id = state
        .node_add(
            parent.clone(),
            0,
            RevisionNode {
                flags: NodeFlags::Link.bits(),
                child: selected,
                address: Address {
                    context: child.id.into(),
                    hash: pin,
                },
                name_hash: lore_storage::hash::hash_string(name),
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            parent.clone(),
            id,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn v2_retained_views_keep_repeated_exposures_and_allocator_floor_distinct() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([218; 16])).await;
            let source = State::new();
            let selected = add(&source, &child, 0, "selected", true, 0).await;
            add(&source, &child, selected, "file", false, 60).await;
            let source_pin = commit(&child, source).await;
            let target = State::new();
            let target_root = add(&target, &child, 0, "selected", true, 0).await;
            add(&target, &child, target_root, "file", false, 61).await;
            let target_pin = Address::zero_context_hash(commit(&child, target).await);
            let source = State::new();
            for name in ["left", "right", "later"] {
                selected_link(&source, &parent, &child, source_pin, selected, name).await;
            }
            let parent_pin = commit(&parent, source).await;
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), parent_pin)
                    .await
                    .unwrap(),
            );
            let root = backend.root();
            let left = backend.lookup(&root.key, b"left").await.unwrap();
            let right = backend.lookup(&root.key, b"right").await.unwrap();
            let head = ScopeHead {
                revision: target_pin,
                root: Some(target_root),
            };
            let bytes = backend
                .test_view_provenance(
                    &[(left.key, head.clone()), (right.key, head.clone())],
                    &[],
                    Some(100),
                )
                .unwrap();
            let fresh = RevisionBackend::new(parent, parent_pin).await.unwrap();
            let unchanged = fresh.checkpoint_state().await.unwrap();
            let restored = fresh.prepare_restore(&bytes).await.unwrap().unwrap();
            assert_eq!(fresh.checkpoint_state().await.unwrap(), unchanged);
            for key in [left.key, right.key] {
                let target = restored.underlay_view(&key, &head).await.unwrap();
                assert_eq!(restored.owning_scope(&target).await.unwrap().root, key);
                assert_eq!(
                    restored
                        .lookup(&target, b"file")
                        .await
                        .unwrap()
                        .key
                        .exposure,
                    key.exposure
                );
                assert_eq!(
                    restored.lookup(&key, b"file").await.unwrap().key.revision,
                    key.revision
                );
            }
            let later = restored.lookup(&root.key, b"later").await.unwrap();
            assert_eq!(later.key.exposure, 100);
            let extended = restored.checkpoint_state().await.unwrap();
            assert!(extended.starts_with(&bytes));
            let again = fresh.prepare_restore(&extended).await.unwrap().unwrap();
            assert_eq!(
                again.lookup(&root.key, b"later").await.unwrap().key,
                later.key
            );
        })
        .await;
}

#[tokio::test]
async fn absent_underlay_rechecks_target_policy_for_local_children_and_writes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([219; 16])).await;
            let source = State::new();
            let selected = add(&source, &child, 0, "selected", true, 0).await;
            add(&source, &child, selected, "file", false, 70).await;
            let source_pin = commit(&child, source).await;
            let missing = Address::zero_context_hash(commit(&child, State::new()).await);
            let source = State::new();
            for name in ["left", "right"] {
                selected_link(&source, &parent, &child, source_pin, selected, name).await;
            }
            let parent_pin = commit(&parent, source).await;
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), parent_pin)
                    .await
                    .unwrap(),
            );
            let original = backend.root();
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(missing);
            let ns = Namespace::new(
                backend.clone(),
                original.clone(),
                gate.clone(),
                Default::default(),
            )
            .unwrap();
            let left = ns.lookup(1, b"left").await.unwrap();
            let right = ns.lookup(1, b"right").await.unwrap();
            assert_ne!(left.authority.exposure, right.authority.exposure);
            let head = ScopeHead {
                revision: missing,
                root: None,
            };
            let mut image = ns.snapshot(0).await.unwrap();
            image.backend_state = backend
                .test_view_provenance(&[(left.authority, head.clone())], &[], None)
                .unwrap();
            image.active_underlays.insert(left.authority, head.clone());
            image.generation += 1;
            let node = image
                .nodes
                .iter_mut()
                .find(|node| node.entry.inode == left.inode)
                .unwrap();
            node.entry.base = None;
            node.names = Some(Vec::new());
            let fresh = Arc::new(RevisionBackend::new(parent, parent_pin).await.unwrap());
            let restored = Namespace::restore(
                fresh.clone(),
                original.clone(),
                gate.clone(),
                image.clone(),
                BTreeMap::new(),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let anchor = restored
                .backend()
                .unwrap()
                .underlay_view(&left.authority, &head)
                .await
                .unwrap();
            assert_eq!(anchor.node, 0);
            assert!(
                restored
                    .backend()
                    .unwrap()
                    .revalidate(&anchor, false)
                    .await
                    .is_err(),
                "absence policy anchor is not a readable ancestor"
            );
            let local = restored
                .create_file(
                    left.inode,
                    b"local",
                    0o644,
                    Arc::new(Store::default()),
                    Default::default(),
                )
                .await
                .unwrap();
            let file = restored
                .materialized_file(local.inode)
                .await
                .unwrap()
                .unwrap();
            let writer = file.open(Access::Write).await.unwrap();
            file.write(writer, 0, b"private").await.unwrap();
            *gate.deny_exposure.lock().unwrap() = Some(left.authority.exposure);
            gate.deny_target.store(true, Ordering::SeqCst);
            assert!(restored.getattr(left.inode).await.is_err());
            assert!(
                restored.getattr(right.inode).await.is_ok(),
                "same repo/pin sibling exposure remains independently allowed"
            );
            assert_eq!(
                file.write(writer, 0, b"destroy").await,
                Err(core::Error::PermissionDenied)
            );
            assert!(
                restored
                    .create_directory(left.inode, b"denied")
                    .await
                    .is_err()
            );
            assert!(
                restored
                    .create_file(
                        left.inode,
                        b"denied",
                        0o644,
                        Arc::new(Store::default()),
                        Default::default()
                    )
                    .await
                    .is_err()
            );
            assert!(
                Namespace::restore(
                    fresh,
                    original,
                    gate.clone(),
                    image,
                    BTreeMap::new(),
                    Default::default(),
                    Default::default()
                )
                .await
                .is_err()
            );
            gate.deny_target.store(false, Ordering::SeqCst);
            file.release(writer).await.unwrap();
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 99).await.unwrap(), b"private");
            file.release(reader).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn v2_view_bindings_require_original_path_pin_and_exact_repeated_exposure() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([220; 16])).await;
            let child_pin = revision_with_bytes(&child, b"linked payload").await;
            let source = State::new();
            link_node(&source, &parent, &child, child_pin, "vendor").await;
            link_node(&source, &parent, &child, child_pin, "mirror").await;
            let source_pin = commit(&parent, source).await;
            let target = State::new();
            let file = add(&target, &parent, 0, "extra", false, 80).await;
            let vendor_link = link_node(&target, &parent, &child, child_pin, "vendor").await;
            let mirror_link = link_node(&target, &parent, &child, child_pin, "mirror").await;
            let target_pin = Address::zero_context_hash(commit(&parent, target).await);
            let backend = RevisionBackend::new(parent.clone(), source_pin)
                .await
                .unwrap();
            let root = backend.root();
            let vendor = backend.lookup(&root.key, b"vendor").await.unwrap();
            let mirror = backend.lookup(&root.key, b"mirror").await.unwrap();
            assert_ne!(vendor.key.exposure, mirror.key.exposure);
            let vendor_source = NodeKey {
                revision: target_pin,
                node: vendor_link,
                ..root.key
            };
            let mirror_source = NodeKey {
                node: mirror_link,
                ..vendor_source
            };
            let views = [(
                root.key,
                ScopeHead {
                    revision: target_pin,
                    root: Some(0),
                },
            )];
            let bindings = [(vendor_source, vendor.key), (mirror_source, mirror.key)];
            let bytes = backend
                .test_view_provenance(&views, &bindings, None)
                .unwrap();
            let fresh = RevisionBackend::new(parent.clone(), source_pin)
                .await
                .unwrap();
            let restored = fresh.prepare_restore(&bytes).await.unwrap().unwrap();
            let current = NodeKey {
                revision: target_pin,
                ..root.key
            };
            assert_eq!(
                restored.lookup(&current, b"vendor").await.unwrap().key,
                vendor.key
            );
            assert_eq!(
                restored.lookup(&current, b"mirror").await.unwrap().key,
                mirror.key
            );
            let file_node = restored.lookup(&vendor.key, b"file").await.unwrap();
            assert_eq!(
                restored.read(&file_node.key, 0, 99).await.unwrap(),
                b"linked payload"
            );
            assert_eq!(restored.checkpoint_state().await.unwrap(), bytes);
            for invalid in [
                vec![],
                vec![(vendor_source, mirror.key), (mirror_source, mirror.key)],
                vec![
                    (
                        NodeKey {
                            node: file,
                            ..vendor_source
                        },
                        vendor.key,
                    ),
                    (mirror_source, mirror.key),
                ],
            ] {
                let bytes = backend
                    .test_view_provenance(&views, &invalid, None)
                    .unwrap();
                assert!(fresh.prepare_restore(&bytes).await.is_err());
            }
            let changed = State::new();
            let changed_link = link_node(&changed, &parent, &child, child_pin, "new-name").await;
            let changed_pin = Address::zero_context_hash(commit(&parent, changed).await);
            let changed = backend
                .test_view_provenance(
                    &[(
                        root.key,
                        ScopeHead {
                            revision: changed_pin,
                            root: Some(0),
                        },
                    )],
                    &[(
                        NodeKey {
                            revision: changed_pin,
                            node: changed_link,
                            ..root.key
                        },
                        vendor.key,
                    )],
                    None,
                )
                .unwrap();
            assert!(matches!(
                fresh.prepare_restore(&changed).await,
                Err(core::Error::Unsupported)
            ));
        })
        .await;
}

#[derive(Default)]
struct PausedStore {
    inner: Store,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl SparseStore for PausedStore {
    fn read(&self, offset: u64, size: usize) -> BackendFuture<'_, Vec<u8>> {
        Box::pin(async move {
            if self.pause.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.inner.read(offset, size).await
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        self.inner.write(offset, bytes)
    }
}

#[tokio::test]
async fn metadata_seal_does_not_wait_for_cached_policy_or_backing_store_reads() {
    LORE_CONTEXT.scope(execution(),async {
        let f=fixture().await;
        let entry=f.namespace.lookup(1,b"edited").await.unwrap();
        let store=Arc::new(PausedStore::default());
        let file=f.namespace.file(entry.inode,store.clone(),Default::default()).await.unwrap();
        let writer=file.open(Access::Write).await.unwrap();file.write(writer,0,b"local!!!").await.unwrap();file.release(writer).await.unwrap();
        let handle=file.open(Access::Read).await.unwrap();
        let plan=prepare(f.namespace.clone(),f.scope,f.target,Options{local:LocalPolicy::KeepLocal,..Default::default()},Limits::default()).await.unwrap();
        f.gate.block_queries.store(true,Ordering::SeqCst);
        let metadata=f.namespace.getattr(entry.inode);tokio::pin!(metadata);
        tokio::select!{_=f.gate.entered.notified()=>{},result=&mut metadata=>panic!("cached metadata did not block: {result:?}")}
        f.gate.deny_source.store(true,Ordering::SeqCst);
        let sealed=tokio::time::timeout(std::time::Duration::from_secs(2),plan.seal_metadata()).await.unwrap().unwrap();
        assert!(matches!(f.namespace.snapshot(1024).await,Err(NamespaceError::Backend(core::Error::Busy))));
        f.gate.deny_source.store(true,Ordering::SeqCst);f.gate.block_queries.store(false,Ordering::SeqCst);f.gate.resume.notify_waiters();
        assert_eq!(metadata.await,Err(NamespaceError::Backend(core::Error::PermissionDenied)));
        f.gate.deny_source.store(false,Ordering::SeqCst);
        let plan=sealed.reopen();plan.validate_source().await.unwrap();
        store.pause.store(true,Ordering::SeqCst);
        let read=file.read(handle,0,8);tokio::pin!(read);
        tokio::select!{_=store.entered.notified()=>{},result=&mut read=>panic!("backing store did not block: {result:?}")}
        let sealed=tokio::time::timeout(std::time::Duration::from_secs(2),plan.seal_metadata()).await.unwrap().unwrap();
        assert!(f.namespace.lookup(1,b"edited").await.is_ok(),"cached lookups remain readable");
        let dir=f.namespace.lookup(1,b"Dir").await.unwrap();
        assert!(matches!(f.namespace.directory_snapshot(dir.inode).await,Err(NamespaceError::Backend(core::Error::Busy))));
        let unmaterialized=f.namespace.lookup(1,b"unrelated").await.unwrap();
        assert!(matches!(f.namespace.materialized_file(unmaterialized.inode).await,Err(NamespaceError::Backend(core::Error::Busy))));
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2),f.namespace.snapshot(1024)).await.unwrap().is_err());
        f.gate.deny_source.store(true,Ordering::SeqCst);store.pause.store(false,Ordering::SeqCst);store.resume.notify_waiters();
        assert_eq!(read.await,Err(core::Error::PermissionDenied),"post-read policy denies revoked bytes");
        f.gate.deny_source.store(false,Ordering::SeqCst);drop(sealed);
        f.namespace.create_directory(1,b"after-cancel").await.unwrap();
        assert_eq!(file.read(handle,0,8).await.unwrap(),b"local!!!");file.release(handle).await.unwrap();
    }).await;
}

#[tokio::test]
async fn metadata_seal_fences_exposure_allocation_in_every_backend_clone() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([221; 16])).await;
            let child_pin = revision_with_bytes(&child, b"child").await;
            let source = State::new();
            link_node(&source, &parent, &child, child_pin, "vendor").await;
            let pin = commit(&parent, source).await;
            let backend = Arc::new(RevisionBackend::new(parent, pin).await.unwrap());
            let root = backend.root();
            let ns = Arc::new(
                Namespace::new(
                    backend.clone(),
                    root.clone(),
                    Arc::new(Gate::default()),
                    Default::default(),
                )
                .unwrap(),
            );
            let plan = prepare(
                ns.clone(),
                root.key,
                root.key.revision,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let before = backend.checkpoint_state().await.unwrap();
            let clone = backend.prepare_restore(&before).await.unwrap().unwrap();
            let sealed = plan.seal_metadata().await.unwrap();
            assert_eq!(
                backend.lookup(&root.key, b"vendor").await,
                Err(core::Error::Busy)
            );
            assert_eq!(
                clone.lookup(&root.key, b"vendor").await,
                Err(core::Error::Busy)
            );
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
            assert_eq!(clone.checkpoint_state().await.unwrap(), before);
            drop(sealed);
            assert!(clone.lookup(&root.key, b"vendor").await.is_ok());
            assert!(backend.lookup(&root.key, b"vendor").await.is_ok());
            let plan = prepare(
                ns.clone(),
                root.key,
                root.key.revision,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            ns.lookup(1, b"vendor").await.unwrap();
            assert!(matches!(
                plan.seal_metadata().await,
                Err(Error::StaleSource)
            ));
            ns.create_directory(1, b"unsealed").await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn cancelling_a_seal_queued_behind_hydration_releases_exact_freeze() {
    LORE_CONTEXT.scope(execution(),async {
        let f=fixture().await;
        let plan=prepare(f.namespace.clone(),f.scope,f.target,Options::default(),Limits::default()).await.unwrap();
        f.gate.block_queries.store(true,Ordering::SeqCst);
        let hydrate=f.namespace.lookup(1,b"Dir");tokio::pin!(hydrate);
        tokio::select!{_=f.gate.entered.notified()=>{},result=&mut hydrate=>panic!("hydration did not block: {result:?}")}
        {
            let seal=plan.seal_metadata();tokio::pin!(seal);
            assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut seal).await.is_err());
        }
        f.gate.block_queries.store(false,Ordering::SeqCst);f.gate.resume.notify_waiters();hydrate.await.unwrap();
        f.namespace.create_directory(1,b"after-cancel").await.unwrap();
    }).await;
}

fn unchanged_metadata(before: &NamespaceSnapshot, after: &NamespaceSnapshot) {
    assert_eq!(before.original_root, after.original_root);
    assert_eq!(before.active_underlays, after.active_underlays);
    assert_eq!(before.backend_state, after.backend_state);
    assert_eq!(before.baselines, after.baselines);
    assert_eq!(before.files, after.files);
    assert_eq!(before.next_inode, after.next_inode);
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.nodes.len(), after.nodes.len());
    for (left, right) in before.nodes.iter().zip(&after.nodes) {
        assert_eq!(left.entry, right.entry);
        assert_eq!(left.names, right.names);
        assert_eq!(left.receipt, right.receipt);
        assert_eq!(left.attached, right.attached);
    }
}
#[tokio::test]
async fn source_and_target_revocation_after_preparation_preserve_all_state() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            dirty(&f, b"edited").await;
            let before = f.namespace.snapshot(1024).await.unwrap();
            let options = Options {
                local: LocalPolicy::KeepLocal,
                ..Default::default()
            };
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                options,
                Limits::default(),
            )
            .await
            .unwrap();
            f.gate.deny_source.store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.validate_source().await,
                Err(Error::Backend(core::Error::PermissionDenied)
                    | Error::Namespace(NamespaceError::Backend(core::Error::PermissionDenied)))
            ));
            f.gate.deny_source.store(false, Ordering::SeqCst);
            drop(plan);
            unchanged_metadata(&before, &f.namespace.snapshot(1024).await.unwrap());
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                options,
                Limits::default(),
            )
            .await
            .unwrap();
            f.gate.deny_target.store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.prepare_target_view().await,
                Err(Error::Backend(core::Error::PermissionDenied)
                    | Error::Namespace(NamespaceError::Backend(core::Error::PermissionDenied)))
            ));
            f.gate.deny_target.store(false, Ordering::SeqCst);
            unchanged_metadata(&before, &f.namespace.snapshot(1024).await.unwrap());
            drop(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    f.target,
                    options,
                    Limits::default(),
                )
                .await
                .unwrap(),
            );
            drop(f.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

struct SelectionProofGate {
    source: Arc<dyn Backend>,
    other: Arc<RevisionBackend>,
    denied: Address,
    denied_calls: std::sync::atomic::AtomicUsize,
}
impl AccessGate for SelectionProofGate {
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a crate::overlay::ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.source
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap()
                .revalidate_target_head(proof)
                .await
        })
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a crate::overlay::ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.source
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap()
                .revalidate_target_node(proof, key, access == Access::Write)
                .await
        })
    }
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_selection<'a>(
        &'a self,
        selection: &'a crate::overlay::TargetSelection,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let source = self
                .source
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            assert!(selection.belongs_to(source));
            assert!(!selection.belongs_to(&self.other));
            assert!(Arc::ptr_eq(
                &selection.captured_execution(),
                &source.captured_execution()
            ));
            source.revalidate_selection_source(selection).await?;
            assert_eq!(
                self.other.revalidate_selection_source(selection).await,
                Err(core::Error::PermissionDenied)
            );
            let mut forged = crate::overlay::TargetSelection {
                source: selection.source().clone(),
                requested: selection.requested_revision(),
                backend_identity: selection.backend_identity.clone(),
                execution: selection.captured_execution(),
            };
            forged.source.source_path.extend_from_slice(b"/fabricated");
            assert_eq!(
                source.revalidate_selection_source(&forged).await,
                Err(core::Error::PermissionDenied)
            );
            if selection.requested_revision() == self.denied {
                self.denied_calls.fetch_add(1, Ordering::SeqCst);
                return Err(core::Error::PermissionDenied);
            }
            Ok(())
        })
    }
}
struct NodeOnlyGate;
impl AccessGate for NodeOnlyGate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn selection_preflight_denies_before_missing_target_metadata_and_binds_exact_source() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let source = f.namespace.backend().unwrap();
            let other = Arc::new(
                RevisionBackend::new(f.repository.clone(), f.scope.revision.hash)
                    .await
                    .unwrap(),
            );
            let root = other.root();
            let missing = Address::zero_context_hash(Hash::from_u64(u64::MAX));
            let policy = Arc::new(SelectionProofGate {
                source: source.clone(),
                other,
                denied: missing,
                denied_calls: std::sync::atomic::AtomicUsize::new(0),
            });
            let namespace =
                Arc::new(Namespace::new(source, root, policy.clone(), Default::default()).unwrap());
            let before = namespace.snapshot(0).await.unwrap();
            assert!(matches!(
                prepare(
                    namespace.clone(),
                    f.scope,
                    missing,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            assert_eq!(policy.denied_calls.load(Ordering::SeqCst), 1);
            unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
            // Failure releases the exact freeze so a valid target remains usable.
            drop(
                prepare(
                    namespace.clone(),
                    f.scope,
                    f.target,
                    Options::default(),
                    Limits::default(),
                )
                .await
                .unwrap(),
            );
            let mut wrong_context = missing;
            wrong_context.context = [1; 16].into();
            assert!(matches!(
                prepare(
                    namespace.clone(),
                    f.scope,
                    wrong_context,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Backend(core::Error::InvalidInput))
            ));
            assert_eq!(policy.denied_calls.load(Ordering::SeqCst), 1);
        })
        .await;
}

#[tokio::test]
async fn selection_preflight_requires_explicit_policy_support() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let backend = f.namespace.backend().unwrap();
            let root = f.namespace.getattr(1).await.unwrap().base.unwrap();
            let namespace = Arc::new(
                Namespace::new(backend, root, Arc::new(NodeOnlyGate), Default::default()).unwrap(),
            );
            let before = namespace.snapshot(0).await.unwrap();
            assert!(matches!(
                prepare(
                    namespace.clone(),
                    f.scope,
                    f.target,
                    Options::default(),
                    Limits::default()
                )
                .await,
                Err(Error::Backend(core::Error::Unsupported))
            ));
            unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
            drop(namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn proven_target_metadata_is_exact_read_only_and_absence_grants_no_ancestor() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([221; 16])).await;
            let original = State::new();
            let selected = add(&original, &child, 0, "selected", true, 0).await;
            let pin = commit(&child, original).await;
            let target_state = State::new();
            let outside = add(&target_state, &child, 0, "outside", false, 91).await;
            let target_root = add(&target_state, &child, 0, "selected", true, 0).await;
            let inside = add(&target_state, &child, target_root, "inside", false, 92).await;
            let target = Address::zero_context_hash(commit(&child, target_state).await);
            let absent = Address::zero_context_hash(commit(&child, State::new()).await);
            let parent_state = State::new();
            selected_link(&parent_state, &parent, &child, pin, selected, "left").await;
            selected_link(&parent_state, &parent, &child, pin, selected, "right").await;
            let parent_pin = commit(&parent, parent_state).await;
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), parent_pin)
                    .await
                    .unwrap(),
            );
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    backend.root(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let left = namespace.lookup(1, b"left").await.unwrap().authority;
            let right = namespace.lookup(1, b"right").await.unwrap().authority;
            let before = namespace.snapshot(0).await.unwrap();
            let plan = prepare(
                namespace.clone(),
                left,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let proof = gate.target_proof.lock().unwrap().clone().unwrap();
            assert_eq!(proof.root().unwrap().key.node, target_root);
            assert_eq!(proof.selection().source_path(), b"selected");
            let inside_key = NodeKey {
                node: inside,
                ..proof.root().unwrap().key
            };
            backend.revalidate_target_head(&proof).await.unwrap();
            backend
                .revalidate_target_node(&proof, &inside_key, false)
                .await
                .unwrap();
            assert_eq!(
                backend
                    .revalidate_target_node(&proof, &inside_key, true)
                    .await,
                Err(core::Error::ReadOnly)
            );
            for key in [
                NodeKey {
                    node: outside,
                    ..inside_key
                },
                NodeKey {
                    node: 0,
                    ..inside_key
                },
                NodeKey {
                    exposure: right.exposure,
                    ..inside_key
                },
                NodeKey {
                    repository: parent.id,
                    ..inside_key
                },
                NodeKey {
                    revision: left.revision,
                    ..inside_key
                },
            ] {
                assert_eq!(
                    backend.revalidate_target_node(&proof, &key, false).await,
                    Err(core::Error::PermissionDenied)
                );
            }
            drop(plan);
            *gate.target.lock().unwrap() = Some(absent);
            let plan = prepare(
                namespace.clone(),
                left,
                absent,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let absent_proof = gate.target_proof.lock().unwrap().clone().unwrap();
            assert!(absent_proof.root().is_none());
            assert_eq!(absent_proof.head().root, None);
            backend.revalidate_target_head(&absent_proof).await.unwrap();
            assert_eq!(
                backend
                    .revalidate_target_node(
                        &absent_proof,
                        &NodeKey {
                            revision: absent,
                            node: 0,
                            ..left
                        },
                        false
                    )
                    .await,
                Err(core::Error::PermissionDenied)
            );
            drop(plan);
            unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn proven_target_node_ancestry_has_a_fixed_metadata_bound() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let target_state = State::new();
            let mut deepest = 0;
            for _ in 0..5 {
                deepest = add(&target_state, &f.repository, deepest, "n", true, 0).await;
            }
            let target = Address::zero_context_hash(commit(&f.repository, target_state).await);
            *f.gate.target.lock().unwrap() = Some(target);
            assert!(matches!(
                prepare(
                    f.namespace.clone(),
                    f.scope,
                    target,
                    Options::default(),
                    Limits {
                        max_depth: 2,
                        ..Limits::default()
                    }
                )
                .await,
                Err(Error::Backend(core::Error::ResourceLimit))
            ));
            let proof = f.gate.target_proof.lock().unwrap().clone().unwrap();
            let backend = f.namespace.backend().unwrap();
            let concrete = backend
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            assert_eq!(
                concrete
                    .revalidate_target_node(
                        &proof,
                        &NodeKey {
                            node: deepest,
                            ..proof.root().unwrap().key
                        },
                        false
                    )
                    .await,
                Err(core::Error::ResourceLimit)
            );
            drop(f.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

struct SelectionOnlyGate;
impl AccessGate for SelectionOnlyGate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_selection<'a>(
        &'a self,
        _: &'a crate::overlay::TargetSelection,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
struct TargetHeadOnlyGate;
impl AccessGate for TargetHeadOnlyGate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_selection<'a>(
        &'a self,
        _: &'a crate::overlay::TargetSelection,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_target_head<'a>(
        &'a self,
        _: &'a crate::overlay::ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
async fn resolved_target_head_and_node_policy_require_explicit_support() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            for policy in [
                Arc::new(SelectionOnlyGate) as Arc<dyn AccessGate>,
                Arc::new(TargetHeadOnlyGate),
            ] {
                let root = f.namespace.getattr(1).await.unwrap().base.unwrap();
                let namespace = Arc::new(
                    Namespace::new(
                        f.namespace.backend().unwrap(),
                        root,
                        policy,
                        Default::default(),
                    )
                    .unwrap(),
                );
                let before = namespace.snapshot(0).await.unwrap();
                assert!(matches!(
                    prepare(
                        namespace.clone(),
                        f.scope,
                        f.target,
                        Options::default(),
                        Limits::default()
                    )
                    .await,
                    Err(Error::Backend(core::Error::Unsupported))
                ));
                unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
                drop(namespace.freeze_operation().await.unwrap());
            }
        })
        .await;
}

#[tokio::test]
async fn target_view_rechecks_policy_after_selected_backend_validation() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let before = f.namespace.snapshot(0).await.unwrap();
            let view = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap()
            .prepare_target_view()
            .await
            .unwrap();
            let backend = view.backend();
            f.gate
                .revoke_after_target_node
                .store(true, Ordering::SeqCst);
            assert_eq!(
                backend.revalidate(&view.root().unwrap().key, false).await,
                Err(core::Error::PermissionDenied)
            );
            f.gate.deny_target.store(false, Ordering::SeqCst);
            drop(backend);
            drop(view);
            unchanged_metadata(&before, &f.namespace.snapshot(0).await.unwrap());
            drop(f.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn candidate_edit_stage_requires_explicit_head_permission_even_for_empty_diff() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.scope.revision,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(plan.changes().is_empty());
            let before = f.namespace.snapshot(0).await.unwrap();
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::Unsupported))
            ));
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            plan.authorize_candidate_edits().await.unwrap();
            let requests = f.gate.candidate_requests.lock().unwrap().clone();
            assert!(!requests.is_empty());
            assert!(requests.iter().all(|edit| edit.action()
                == crate::overlay::CandidateEditAction::AdvanceSelectedHead
                && matches!(
                    edit.location(),
                    crate::overlay::CandidateLocation::SelectedHead
                )
                && edit.source_before().is_none()
                && edit.target_before().is_none()
                && edit.candidate_after().is_none()));
            unchanged_metadata(&before, &f.namespace.snapshot(0).await.unwrap());
            drop(plan);
            drop(f.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn candidate_edit_union_keeps_source_and_target_identities_and_real_parent_tails() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let source_file = dirty(&f, b"edited").await;
            let directory = f.namespace.create_directory(1, b"local-dir").await.unwrap();
            let local = f
                .namespace
                .create_file(
                    directory.inode,
                    b"new",
                    0o644,
                    Arc::new(Store::default()),
                    Default::default(),
                )
                .await
                .unwrap();
            let local_file = f
                .namespace
                .file(local.inode, Arc::new(Store::default()), Default::default())
                .await
                .unwrap();
            let writer = local_file.open(Access::Write).await.unwrap();
            local_file.write(writer, 0, b"private").await.unwrap();
            local_file.release(writer).await.unwrap();
            let target = State::new();
            add(&target, &f.repository, 0, "new-sibling", false, 100).await;
            let target_id = add(&target, &f.repository, 0, "edited", false, 10).await;
            add(&target, &f.repository, 0, "unrelated", false, 2).await;
            let dir = add(&target, &f.repository, 0, "Dir", true, 0).await;
            add(&target, &f.repository, dir, "File", false, 3).await;
            add(&target, &f.repository, 0, "dir-other", false, 4).await;
            add(&target, &f.repository, 0, "dir.other", false, 5).await;
            let target = Address::zero_context_hash(commit(&f.repository, target).await);
            *f.gate.target.lock().unwrap() = Some(target);
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                target,
                Options {
                    local: LocalPolicy::KeepLocal,
                    ..Default::default()
                },
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(
                !plan
                    .changes()
                    .iter()
                    .any(|change| change.path == b"edited" || change.path == b"local-dir/new")
            );
            let before = f.namespace.snapshot(100).await.unwrap();
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            f.gate.source_writes.lock().unwrap().clear();
            plan.authorize_candidate_edits().await.unwrap();
            let requests = f.gate.candidate_requests.lock().unwrap().clone();
            let edited = requests
                .iter()
                .find(|edit| edit.path() == b"edited")
                .unwrap();
            assert!(edited.change().is_none());
            assert_eq!(edited.source_before(), edited.candidate_after());
            assert_ne!(edited.target_before(), edited.candidate_after());
            match edited.location() {
                crate::overlay::CandidateLocation::TargetNode(node) => {
                    assert_eq!(node.key.node, target_id);
                    assert_ne!(node.key.node, 1);
                    assert_eq!(node.key.revision, target);
                }
                _ => panic!("carried file must use the actual target node"),
            }
            let new = requests
                .iter()
                .find(|edit| edit.path() == b"local-dir/new")
                .unwrap();
            assert_eq!(new.source_before(), new.candidate_after());
            assert!(new.target_before().is_none());
            match new.location() {
                crate::overlay::CandidateLocation::TargetDirectoryParent { parent, tail } => {
                    assert_eq!(parent.key.node, 0);
                    assert_eq!(parent.key.revision, target);
                    assert_eq!(tail, b"local-dir/new");
                }
                _ => panic!("local-only file requires an actual target directory"),
            }
            let writes = f.gate.source_writes.lock().unwrap().clone();
            assert!(writes.iter().any(|key| key.node == 1));
            assert!(writes.iter().all(|key| key.revision == f.scope.revision));
            unchanged_metadata(&before, &f.namespace.snapshot(100).await.unwrap());
            drop(plan);
            for (file, bytes) in [
                (source_file, b"local!!!".as_slice()),
                (local_file, b"private".as_slice()),
            ] {
                let reader = file.open(Access::Read).await.unwrap();
                assert_eq!(file.read(reader, 0, bytes.len()).await.unwrap(), bytes);
                file.release(reader).await.unwrap();
            }
        })
        .await;
}

#[tokio::test]
async fn candidate_edit_denials_and_source_write_revocation_preserve_private_data() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let file = dirty(&f, b"edited").await;
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
            let before = f.namespace.snapshot(100).await.unwrap();
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            f.gate.deny_candidate.store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            f.gate.deny_candidate.store(false, Ordering::SeqCst);
            f.gate
                .revoke_write_during_candidate
                .store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            f.gate.deny_write.store(false, Ordering::SeqCst);
            *f.gate.deny_write_node.lock().unwrap() = Some(1);
            f.gate.candidate_requests.lock().unwrap().clear();
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            assert!(f.gate.candidate_requests.lock().unwrap().is_empty());
            *f.gate.deny_write_node.lock().unwrap() = None;
            plan.authorize_candidate_edits().await.unwrap();
            unchanged_metadata(&before, &f.namespace.snapshot(100).await.unwrap());
            drop(plan);
            let reader = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(reader, 0, 8).await.unwrap(), b"local!!!");
            file.release(reader).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn candidate_actual_edge_deletion_checks_the_original_directory_write_policy() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let directory = f.namespace.lookup(1, b"Dir").await.unwrap();
            let file = f.namespace.lookup(directory.inode, b"File").await.unwrap();
            let target = State::new();
            add(&target, &f.repository, 0, "edited", false, 1).await;
            add(&target, &f.repository, 0, "unrelated", false, 2).await;
            add(&target, &f.repository, 0, "Dir", true, 0).await;
            add(&target, &f.repository, 0, "dir-other", false, 4).await;
            add(&target, &f.repository, 0, "dir.other", false, 5).await;
            let target = Address::zero_context_hash(commit(&f.repository, target).await);
            *f.gate.target.lock().unwrap() = Some(target);
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            *f.gate.deny_write_node.lock().unwrap() = Some(directory.authority.node);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            assert!(f.gate.candidate_requests.lock().unwrap().is_empty());
            *f.gate.deny_write_node.lock().unwrap() = None;
            f.gate.source_writes.lock().unwrap().clear();
            plan.authorize_candidate_edits().await.unwrap();
            let writes = f.gate.source_writes.lock().unwrap().clone();
            assert!(writes.contains(&file.authority));
            assert!(writes.contains(&directory.authority));
            let requests = f.gate.candidate_requests.lock().unwrap().clone();
            let removed = requests
                .iter()
                .find(|edit| edit.path() == b"Dir/File")
                .unwrap();
            assert!(removed.source_before().is_some());
            assert!(removed.target_before().is_none());
            assert!(removed.candidate_after().is_none());
            match removed.location() {
                crate::overlay::CandidateLocation::TargetDirectoryParent { parent, tail } => {
                    assert_eq!(parent.kind, crate::core::NodeKind::Directory);
                    assert_eq!(tail, b"File");
                }
                _ => panic!("removed target path requires its surviving directory"),
            }
        })
        .await;
}

#[tokio::test]
async fn candidate_missing_path_witness_rejects_existing_children_and_proof_tampering() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            f.namespace
                .create_file(
                    1,
                    b"local-only",
                    0o644,
                    Arc::new(Store::default()),
                    Default::default(),
                )
                .await
                .unwrap();
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            plan.authorize_candidate_edits().await.unwrap();
            let edit = f
                .gate
                .candidate_requests
                .lock()
                .unwrap()
                .iter()
                .find(|edit| edit.path() == b"local-only")
                .unwrap()
                .clone();
            let original = f.namespace.backend().unwrap();
            let backend = original
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            backend.revalidate_candidate_edit(&edit).await.unwrap();
            let mut forged = edit.clone();
            forged.path = b"edited".to_vec();
            if let crate::overlay::candidate_edit::Location::Parent { tail, .. } =
                &mut forged.location
            {
                *tail = b"edited".to_vec();
            } else {
                panic!("expected missing-path authority");
            }
            assert_eq!(
                backend.revalidate_candidate_edit(&forged).await,
                Err(core::Error::PermissionDenied)
            );
            let mut forged = edit.clone();
            forged.proof = Arc::new(edit.proof().clone());
            assert_eq!(
                backend.revalidate_candidate_edit(&forged).await,
                Err(core::Error::PermissionDenied)
            );
            let mut forged = edit.clone();
            if let crate::overlay::candidate_edit::Location::Parent { tail, .. } =
                &mut forged.location
            {
                *tail = b"../edited".to_vec();
            }
            assert_eq!(
                backend.revalidate_candidate_edit(&forged).await,
                Err(core::Error::InvalidInput)
            );
            let mut budget = Budget::new(Limits::default()).unwrap();
            let mut view = backend
                .transition_view(f.scope, f.target, f.gate.clone(), &mut budget)
                .await
                .unwrap();
            let root = view.proof.root().unwrap().clone();
            for name in [b"edited".as_slice(), b"EDITED".as_slice()] {
                assert!(matches!(
                    backend
                        .candidate_missing_path(&view, name, &root, name)
                        .await,
                    Err(core::Error::InvalidInput)
                ));
            }
            assert!(matches!(
                backend
                    .candidate_missing_path(&view, b"edited/child", &root, b"edited/child")
                    .await,
                Err(core::Error::Unsupported)
            ));
            assert!(matches!(
                backend
                    .candidate_missing_path(&view, b"Dir/missing", &root, b"Dir/missing")
                    .await,
                Err(core::Error::PermissionDenied)
            ));
            view.proof = Arc::new(view.proof.as_ref().clone());
            assert!(matches!(
                backend
                    .candidate_missing_path(&view, b"missing", &root, b"missing")
                    .await,
                Err(core::Error::PermissionDenied)
            ));
        })
        .await;
}

#[tokio::test]
async fn candidate_stage_is_single_admission_cancel_safe_and_uses_captured_execution() {
    let (f, plan) = LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            f.gate.require_context.store(true, Ordering::SeqCst);
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            (f, plan)
        })
        .await;
    assert!(lore_revision::runtime::try_execution_context().is_none());
    f.gate.pause_candidate.store(true, Ordering::SeqCst);
    let mut first = Box::pin(plan.authorize_candidate_edits());
    tokio::select! { _ = f.gate.entered.notified() => {}, result = &mut first => panic!("candidate stage did not wait: {result:?}") }
    assert!(matches!(
        plan.authorize_candidate_edits().await,
        Err(Error::Backend(core::Error::Busy))
    ));
    drop(first);
    f.gate.pause_candidate.store(false, Ordering::SeqCst);
    plan.authorize_candidate_edits().await.unwrap();
    f.gate.require_context.store(false, Ordering::SeqCst);
    drop(plan);
    drop(f.namespace.freeze_operation().await.unwrap());
}

#[tokio::test]
async fn candidate_carried_case_and_deletion_recheck_source_and_target_directory_authorities() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let directory = f.namespace.lookup(1, b"Dir").await.unwrap();
            let file = f.namespace.lookup(directory.inode, b"File").await.unwrap();
            f.namespace
                .rename(directory.inode, b"File", directory.inode, b"file")
                .await
                .unwrap();
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.scope.revision,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(plan.changes().is_empty());
            f.gate.allow_candidate.store(true, Ordering::SeqCst);
            *f.gate.deny_write_node.lock().unwrap() = Some(directory.authority.node);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            *f.gate.deny_write_node.lock().unwrap() = None;
            f.gate.deny_candidate_parent.store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            f.gate.deny_candidate_parent.store(false, Ordering::SeqCst);
            plan.authorize_candidate_edits().await.unwrap();
            drop(plan);
            f.namespace
                .remove(directory.inode, b"file", false)
                .await
                .unwrap();
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.scope.revision,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            assert!(plan.changes().is_empty());
            *f.gate.deny_write_node.lock().unwrap() = Some(file.authority.node);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            *f.gate.deny_write_node.lock().unwrap() = Some(directory.authority.node);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            *f.gate.deny_write_node.lock().unwrap() = None;
            f.gate.deny_candidate_parent.store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.authorize_candidate_edits().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            f.gate.deny_candidate_parent.store(false, Ordering::SeqCst);
            plan.authorize_candidate_edits().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn complete_backend_routes_old_and_target_ranges_without_retaining_the_plan_freeze() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let pin = revision_with_bytes(&repository, b"old-complete").await;
            let target =
                Address::zero_context_hash(revision_with_bytes(&repository, b"new-complete").await);
            let backend = Arc::new(RevisionBackend::new(repository.clone(), pin).await.unwrap());
            let root = backend.root();
            let old_file = backend.lookup(&root.key, b"file").await.unwrap();
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            // Read-only preparation does not acquire candidate-edit Write permission.
            gate.deny_write.store(true, Ordering::SeqCst);
            let namespace = Arc::new(
                Namespace::new(
                    backend.clone(),
                    root.clone(),
                    gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let before = namespace.snapshot(0).await.unwrap();
            let prefix = backend.checkpoint_state().await.unwrap();
            let plan = prepare(
                namespace.clone(),
                root.key,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            assert!(complete.matches_source(&backend));
            assert_eq!(complete.original_root(), &root);
            assert_eq!(complete.scope(), root.key);
            assert_eq!(complete.head().revision, target);
            assert!(complete.provenance().starts_with(&prefix));
            assert_eq!(
                complete.backend().checkpoint_state().await.unwrap(),
                complete.provenance()
            );
            assert_eq!(backend.checkpoint_state().await.unwrap(), prefix);
            assert_eq!(complete.backend().root(), root);
            let target_root = complete.selected_root().unwrap().clone();
            let target_file = complete
                .backend()
                .lookup(&target_root.key, b"file")
                .await
                .unwrap();
            assert_eq!(
                complete.backend().read(&old_file.key, 0, 12).await.unwrap(),
                b"old-complete"
            );
            assert_eq!(
                complete
                    .backend()
                    .read(&target_file.key, 0, 12)
                    .await
                    .unwrap(),
                b"new-complete"
            );
            let fresh = Arc::new(RevisionBackend::new(repository, pin).await.unwrap());
            assert!(!complete.matches_source(&fresh));
            let reopened = fresh
                .prepare_restore(complete.provenance())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                reopened.checkpoint_state().await.unwrap(),
                complete.provenance()
            );
            assert_eq!(
                reopened.read(&old_file.key, 0, 12).await.unwrap(),
                b"old-complete"
            );
            assert_eq!(
                reopened.read(&target_file.key, 0, 12).await.unwrap(),
                b"new-complete"
            );
            unchanged_metadata(&before, &namespace.snapshot(0).await.unwrap());
            drop(plan);
            drop(namespace.freeze_operation().await.unwrap());
            // Keeping complete routing alive neither freezes the source nor changes it.
            assert_eq!(
                complete
                    .backend()
                    .read(&target_file.key, 0, 12)
                    .await
                    .unwrap(),
                b"new-complete"
            );
        })
        .await;
}

#[tokio::test]
async fn complete_backend_preserves_repeated_child_origins_and_exact_target_bindings() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([232; 16])).await;
            let child_pin = revision_with_bytes(&child, b"linked-original").await;
            let original = State::new();
            add(&original, &parent, 0, "parent", false, 1).await;
            for name in ["left", "right"] {
                selected_link(&original, &parent, &child, child_pin, 0, name).await;
            }
            let pin = commit(&parent, original).await;
            let target_state = State::new();
            add(&target_state, &parent, 0, "parent", false, 2).await;
            add(&target_state, &parent, 0, "extra", false, 3).await;
            for name in ["left", "right"] {
                selected_link(&target_state, &parent, &child, child_pin, 0, name).await;
            }
            let target = Address::zero_context_hash(commit(&parent, target_state).await);
            let backend = Arc::new(RevisionBackend::new(parent.clone(), pin).await.unwrap());
            let root = backend.root();
            let left = backend.lookup(&root.key, b"left").await.unwrap();
            let right = backend.lookup(&root.key, b"right").await.unwrap();
            assert_ne!(left.key.exposure, right.key.exposure);
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
            let prefix = backend.checkpoint_state().await.unwrap();
            let plan = prepare(
                namespace.clone(),
                root.key,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            let target_root = complete.selected_root().unwrap().clone();
            let target_left = complete
                .backend()
                .lookup(&target_root.key, b"left")
                .await
                .unwrap();
            let target_right = complete
                .backend()
                .lookup(&target_root.key, b"right")
                .await
                .unwrap();
            assert_eq!(target_left, left);
            assert_eq!(target_right, right);
            for directory in [left, right] {
                let file = complete
                    .backend()
                    .lookup(&directory.key, b"file")
                    .await
                    .unwrap();
                assert_eq!(
                    complete.backend().read(&file.key, 0, 15).await.unwrap(),
                    b"linked-original"
                );
            }
            assert!(complete.provenance().starts_with(&prefix));
            let bytes = complete.provenance().to_vec();
            drop(plan);
            let next_namespace = Arc::new(
                Namespace::new(complete.backend(), root.clone(), gate, Default::default()).unwrap(),
            );
            let next = prepare(
                next_namespace,
                root.key,
                target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let repeated = next.prepare_complete_backend().await.unwrap();
            assert_eq!(repeated.provenance(), bytes);
            let fresh = RevisionBackend::new(parent, pin).await.unwrap();
            let replayed = fresh.prepare_restore(&bytes).await.unwrap().unwrap();
            assert_eq!(
                replayed.lookup(&target_root.key, b"left").await.unwrap(),
                target_left
            );
            assert_eq!(
                replayed.lookup(&target_root.key, b"right").await.unwrap(),
                target_right
            );
        })
        .await;
}

#[tokio::test]
async fn complete_backend_disables_unproven_allocation_and_preserves_the_exposure_floor() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([233; 16])).await;
            let pin = revision_with_bytes(&child, b"old-linked").await;
            let target =
                Address::zero_context_hash(revision_with_bytes(&child, b"new-linked").await);
            let state = State::new();
            for name in ["left", "right", "late"] {
                selected_link(&state, &parent, &child, pin, 0, name).await;
            }
            let parent_pin = commit(&parent, state).await;
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), parent_pin)
                    .await
                    .unwrap(),
            );
            let root = backend.root();
            let left = backend.lookup(&root.key, b"left").await.unwrap();
            let right = backend.lookup(&root.key, b"right").await.unwrap();
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(target);
            let mut budget = Budget::new(Limits::default()).unwrap();
            let origin = backend
                .transition_view(left.key, left.key.revision, gate.clone(), &mut budget)
                .await
                .unwrap();
            let target_view = backend
                .transition_view(left.key, target, gate, &mut budget)
                .await
                .unwrap();
            let epoch = backend.shared_mutation_gate().epoch().unwrap();
            let complete = backend
                .transition_complete_fork(left.key, &origin, &target_view, epoch, &mut budget)
                .await
                .unwrap();
            assert_eq!(
                complete.backend().lookup(&root.key, b"late").await,
                Err(core::Error::Unsupported)
            );
            let left_target = complete.selected_root().unwrap().clone();
            let file = complete
                .backend()
                .lookup(&left_target.key, b"file")
                .await
                .unwrap();
            assert_eq!(
                complete.backend().read(&file.key, 0, 10).await.unwrap(),
                b"new-linked"
            );
            assert!(matches!(
                complete
                    .backend()
                    .revalidate(
                        &NodeKey {
                            revision: target,
                            ..right.key
                        },
                        false
                    )
                    .await,
                Err(core::Error::Stale)
            ));
            let fresh = RevisionBackend::new(parent, parent_pin).await.unwrap();
            let replayed = fresh
                .prepare_restore(complete.provenance())
                .await
                .unwrap()
                .unwrap();
            let late = replayed.lookup(&root.key, b"late").await.unwrap();
            assert_eq!(late.key.exposure, right.key.exposure + 1);
            assert_eq!(
                complete.backend().checkpoint_state().await.unwrap(),
                complete.provenance()
            );
        })
        .await;
}

#[tokio::test]
async fn complete_backend_absence_has_no_readable_ancestor_and_keeps_other_exposures() {
    LORE_CONTEXT
        .scope(execution(), async {
            let parent = repository().await;
            let child = parent.to_link_context(RepositoryId::from([234; 16])).await;
            let state = State::new();
            let selected = add(&state, &child, 0, "selected", true, 0).await;
            add(&state, &child, selected, "file", false, 21).await;
            let pin = commit(&child, state).await;
            let absent = Address::zero_context_hash(commit(&child, State::new()).await);
            let parent_state = State::new();
            for name in ["left", "right"] {
                selected_link(&parent_state, &parent, &child, pin, selected, name).await;
            }
            let parent_pin = commit(&parent, parent_state).await;
            let backend = Arc::new(RevisionBackend::new(parent, parent_pin).await.unwrap());
            let gate = Arc::new(Gate::default());
            *gate.target.lock().unwrap() = Some(absent);
            let namespace = Arc::new(
                Namespace::new(backend.clone(), backend.root(), gate, Default::default()).unwrap(),
            );
            let left = namespace.lookup(1, b"left").await.unwrap();
            let right = namespace.lookup(1, b"right").await.unwrap();
            let plan = prepare(
                namespace,
                left.authority,
                absent,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let complete = plan.prepare_complete_backend().await.unwrap();
            assert!(complete.selected_root().is_none());
            assert_eq!(complete.head().root, None);
            assert_eq!(
                complete
                    .backend()
                    .revalidate(
                        &NodeKey {
                            revision: absent,
                            node: 0,
                            ..left.authority
                        },
                        false
                    )
                    .await,
                Err(core::Error::Stale)
            );
            complete
                .backend()
                .revalidate(&right.authority, false)
                .await
                .unwrap();
            assert!(matches!(
                complete
                    .backend()
                    .underlay_view(&right.authority, complete.head())
                    .await,
                Err(core::Error::Stale)
            ));
            assert_eq!(
                complete.backend().checkpoint_state().await.unwrap(),
                complete.provenance()
            );
        })
        .await;
}

#[tokio::test]
async fn complete_backend_aggregate_replay_budget_and_epoch_changes_are_rejected() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let original = f.namespace.backend().unwrap();
            let backend = original
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let mut reader_budget = Budget::new(Limits::default()).unwrap();
            let origin = backend
                .transition_view(
                    f.scope,
                    f.scope.revision,
                    f.gate.clone(),
                    &mut reader_budget,
                )
                .await
                .unwrap();
            let target = backend
                .transition_view(f.scope, f.target, f.gate.clone(), &mut reader_budget)
                .await
                .unwrap();
            let epoch = backend.shared_mutation_gate().epoch().unwrap();
            let before = backend.checkpoint_state().await.unwrap();
            let mut small = Budget::new(Limits {
                max_nodes: 20,
                ..Limits::default()
            })
            .unwrap();
            assert!(matches!(
                backend
                    .transition_complete_fork(f.scope, &origin, &target, epoch, &mut small)
                    .await,
                Err(core::Error::ResourceLimit)
            ));
            let mut small = Budget::new(Limits {
                max_path_bytes: 1,
                ..Limits::default()
            })
            .unwrap();
            assert!(matches!(
                backend
                    .transition_complete_fork(f.scope, &origin, &target, epoch, &mut small)
                    .await,
                Err(core::Error::ResourceLimit)
            ));
            let mut budget = Budget::new(Limits::default()).unwrap();
            let mut in_replay = Box::pin(backend.transition_complete_fork(
                f.scope,
                &origin,
                &target,
                epoch,
                &mut budget,
            ));
            // All metadata is cached; strict replay's explicit record yield is the
            // first suspension, after its source epoch and provenance capture.
            std::future::poll_fn(|cx| {
                assert!(matches!(
                    std::future::Future::poll(in_replay.as_mut(), cx),
                    std::task::Poll::Pending
                ));
                std::task::Poll::Ready(())
            })
            .await;
            f.namespace.lookup(1, b"unrelated").await.unwrap();
            assert!(matches!(in_replay.await, Err(core::Error::Stale)));
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
            let epoch = backend.shared_mutation_gate().epoch().unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let complete = backend
                .transition_complete_fork(f.scope, &origin, &target, epoch, &mut budget)
                .await
                .unwrap();
            assert_eq!(complete.source_epoch(), epoch);
            assert_eq!(
                complete.backend().checkpoint_state().await.unwrap(),
                complete.provenance()
            );
        })
        .await;
}

#[tokio::test]
async fn complete_backend_permission_denial_and_stale_plan_leave_the_original_untouched() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let plan = prepare(
                f.namespace.clone(),
                f.scope,
                f.target,
                Options::default(),
                Limits::default(),
            )
            .await
            .unwrap();
            let before = f.namespace.snapshot(0).await.unwrap();
            f.gate.deny_target.store(true, Ordering::SeqCst);
            assert!(matches!(
                plan.prepare_complete_backend().await,
                Err(Error::Backend(core::Error::PermissionDenied))
            ));
            f.gate.deny_target.store(false, Ordering::SeqCst);
            let complete = plan.prepare_complete_backend().await.unwrap();
            unchanged_metadata(&before, &f.namespace.snapshot(0).await.unwrap());
            f.namespace.lookup(1, b"edited").await.unwrap();
            assert!(matches!(
                plan.prepare_complete_backend().await,
                Err(Error::StaleSource)
            ));
            assert_ne!(
                complete.source_epoch(),
                f.namespace.mutation_epoch().unwrap()
            );
            drop(plan);
            drop(f.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn complete_backend_original_staged_underlay_does_not_append_a_committed_view() {
    LORE_CONTEXT
        .scope(execution(), async {
            let repository = repository().await;
            let state = State::new();
            add(&state, &repository, 0, "staged", false, 44).await;
            let stage = state
                .serialize(repository.clone(), repository.try_write_token().unwrap())
                .await
                .unwrap();
            let backend = Arc::new(
                RevisionBackend::new(repository.clone(), stage)
                    .await
                    .unwrap(),
            );
            let root = backend.root();
            let gate = Arc::new(Gate::default());
            let mut budget = Budget::new(Limits::default()).unwrap();
            let origin = backend
                .transition_view(root.key, root.key.revision, gate, &mut budget)
                .await
                .unwrap();
            assert!(!origin.committed);
            let prefix = backend.checkpoint_state().await.unwrap();
            let epoch = backend.shared_mutation_gate().epoch().unwrap();
            let complete = backend
                .transition_complete_fork(root.key, &origin, &origin, epoch, &mut budget)
                .await
                .unwrap();
            assert_eq!(complete.provenance(), prefix);
            assert_eq!(complete.original_root(), &root);
            assert_eq!(complete.selected_root(), Some(&root));
            let staged = complete
                .backend()
                .lookup(&root.key, b"staged")
                .await
                .unwrap();
            assert_eq!(staged.key.revision, root.key.revision);
            assert_eq!(complete.backend().checkpoint_state().await.unwrap(), prefix);
            let fresh = RevisionBackend::new(repository, stage).await.unwrap();
            let replayed = fresh
                .prepare_restore(complete.provenance())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(replayed.lookup(&root.key, b"staged").await.unwrap(), staged);
        })
        .await;
}

#[tokio::test]
async fn complete_backend_rejects_foreign_source_and_incomplete_or_conflicting_view_proofs() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = fixture().await;
            let original = f.namespace.backend().unwrap();
            let backend = original
                .as_any()
                .unwrap()
                .downcast_ref::<RevisionBackend>()
                .unwrap();
            let mut budget = Budget::new(Limits::default()).unwrap();
            let origin = backend
                .transition_view(f.scope, f.scope.revision, f.gate.clone(), &mut budget)
                .await
                .unwrap();
            let mut target = backend
                .transition_view(f.scope, f.target, f.gate.clone(), &mut budget)
                .await
                .unwrap();
            let epoch = backend.shared_mutation_gate().epoch().unwrap();
            let before = backend.checkpoint_state().await.unwrap();
            let foreign = RevisionBackend::new(f.repository.clone(), f.scope.revision.hash)
                .await
                .unwrap();
            assert!(matches!(
                foreign
                    .transition_complete_fork(f.scope, &origin, &target, epoch, &mut budget)
                    .await,
                Err(core::Error::PermissionDenied)
            ));
            target.head.root = Some(u32::MAX);
            assert!(matches!(
                backend
                    .transition_complete_fork(f.scope, &origin, &target, epoch, &mut budget)
                    .await,
                Err(core::Error::InvalidInput)
            ));
            target.head = target.proof.head();
            let wrong_image = backend
                .transition_view(f.scope, f.scope.revision, f.gate.clone(), &mut budget)
                .await
                .unwrap();
            target.entries = wrong_image.entries;
            assert!(matches!(
                backend
                    .transition_complete_fork(f.scope, &origin, &target, epoch, &mut budget)
                    .await,
                Err(core::Error::InvalidInput)
            ));
            assert_eq!(backend.checkpoint_state().await.unwrap(), before);
        })
        .await;
}

#[path = "membership_tests.rs"]
mod membership;

#[path = "inventory_tests.rs"]
mod registered_inventory;

#[path = "identity_tests.rs"]
mod immutable_identity;
