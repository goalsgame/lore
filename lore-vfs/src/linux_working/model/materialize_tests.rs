// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::State as RevisionState;

use super::super::tests::Directory;
use super::super::tests::READ;
use super::super::tests::WRITE;
use super::*;
use crate::overlay::CandidateEdit;
use crate::overlay::CommittedProof;
use crate::overlay::ProvenTargetMetadata;
use crate::overlay::Receipt;
use crate::overlay::TargetSelection;
use crate::revision::RevisionBackend;
use crate::transition_prepare::LocalPolicy;
use crate::transition_prepare::Options;
use crate::transition_prepare::PreparedTransition;
use crate::transition_prepare::{self};

#[derive(Clone)]
struct CellPause {
    owner: std::sync::Weak<Model>,
    inode: u64,
}
#[derive(Default)]
struct Gate {
    target: Address,
    source_read: AtomicBool,
    source_write: AtomicBool,
    target_read: AtomicBool,
    target_write: AtomicBool,
    candidate: AtomicBool,
    pause_read: AtomicBool,
    pause_after_cell: std::sync::Mutex<Option<CellPause>>,
    read_entered: tokio::sync::Notify,
    read_resume: tokio::sync::Notify,
}
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, key: &'a core::NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let pause = self.pause_after_cell.lock().unwrap().clone();
            let after_cell = if let Some(pause) = pause {
                if let Some(owner) = pause.owner.upgrade() {
                    owner.files.lock().await.contains_key(&pause.inode)
                } else {
                    false
                }
            } else {
                false
            };
            if access == Access::Read && (self.pause_read.load(Ordering::Acquire) || after_cell) {
                self.read_entered.notify_one();
                self.read_resume.notified().await;
            }
            let denied = if key.revision == self.target {
                self.target_read.load(Ordering::Acquire)
                    || access == Access::Write && self.target_write.load(Ordering::Acquire)
            } else {
                self.source_read.load(Ordering::Acquire)
                    || access == Access::Write && self.source_write.load(Ordering::Acquire)
            };
            if denied {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if selection.requested_revision() == self.target
                && self.target_read.load(Ordering::Acquire)
            {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if proof.head().revision == self.target && self.target_read.load(Ordering::Acquire) {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
    fn authorize_target_node<'a>(
        &'a self,
        _: &'a ProvenTargetMetadata,
        key: &'a core::NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        if access == Access::Write {
            Box::pin(async { Err(core::Error::ReadOnly) })
        } else {
            self.authorize(key, access)
        }
    }
    fn authorize_candidate_edit<'a>(&'a self, _: &'a CandidateEdit) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.candidate.load(Ordering::Acquire) {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}
#[derive(Default)]
struct Control {
    block: AtomicBool,
    short: AtomicUsize,
    writes: AtomicU64,
    reads: AtomicU64,
    read_block: AtomicBool,
    read_entered: tokio::sync::Notify,
    read_resume: tokio::sync::Notify,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
struct Store {
    data: AsyncMutex<Vec<u8>>,
    control: Arc<Control>,
}
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.control.reads.fetch_add(1, Ordering::AcqRel);
            if self.control.read_block.load(Ordering::Acquire) {
                self.control.read_entered.notify_one();
                self.control.read_resume.notified().await;
            }
            let start = usize::try_from(offset).map_err(|_offset| core::Error::InvalidInput)?;
            let end = start.checked_add(size).ok_or(core::Error::InvalidInput)?;
            self.data
                .lock()
                .await
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or(core::Error::Io)
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            self.control.writes.fetch_add(1, Ordering::AcqRel);
            if self.control.block.load(Ordering::Acquire) {
                self.control.entered.notify_one();
                self.control.resume.notified().await;
            }
            let start = usize::try_from(offset).map_err(|_offset| core::Error::InvalidInput)?;
            let end = start
                .checked_add(bytes.len())
                .ok_or(core::Error::InvalidInput)?;
            let mut data = self.data.lock().await;
            if data.len() < end {
                data.resize(end, 0);
            }
            data[start..end].copy_from_slice(bytes);
            let short = self.control.short.load(Ordering::Acquire);
            Ok(if short == 0 {
                bytes.len()
            } else {
                short.min(bytes.len())
            })
        })
    }
}
#[derive(Default)]
struct Stores {
    count: AtomicU64,
    fail: AtomicBool,
    control: Arc<Control>,
}
impl StoreFactory for Stores {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async move {
            self.count.fetch_add(1, Ordering::AcqRel);
            if self.fail.load(Ordering::Acquire) {
                return Err(core::Error::Io);
            }
            Ok(Arc::new(Store {
                data: AsyncMutex::new(Vec::new()),
                control: self.control.clone(),
            }) as Arc<dyn SparseStore>)
        })
    }
}
fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "materialize-test".into(),
    ))
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
        RepositoryId::from([202; 16]),
    ))
}
async fn commit_file(
    repository: &Arc<RepositoryContext>,
    data: &[u8],
    seed: u8,
) -> (Hash, Address) {
    use lore_revision::node::NodeFlags;
    let state = RevisionState::new();
    let address = lore_revision::immutable::write_borrowed(
        repository.clone(),
        [203; 16].into(),
        data,
        Default::default(),
    )
    .await
    .unwrap();
    let file = state
        .node_add(
            repository.clone(),
            0,
            lore_revision::node::Node {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string("file"),
                address,
                size: data.len() as u64,
                ..Default::default()
            },
            "file",
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            file,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let directory = state
        .node_add(
            repository.clone(),
            0,
            lore_revision::node::Node {
                name_hash: lore_storage::hash::hash_string("cold"),
                ..Default::default()
            },
            "cold",
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            directory,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let leaf = state
        .node_add(
            repository.clone(),
            directory,
            lore_revision::node::Node {
                flags: NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string("leaf"),
                address,
                size: data.len() as u64,
                ..Default::default()
            },
            "leaf",
        )
        .await
        .unwrap();
    state
        .node_mark_staged(
            repository.clone(),
            leaf,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .unwrap();
    let branch = [seed; 16].into();
    let mut metadata = lore_revision::metadata::Metadata::new();
    metadata.set_branch(branch).unwrap();
    let hash = lore_revision::commit::commit_in_memory_revision(
        repository.clone(),
        repository.try_write_token().unwrap(),
        state,
        metadata,
        Hash::default(),
        branch,
    )
    .await
    .unwrap();
    (hash, address)
}
struct Fixture {
    model: Arc<Model>,
    stores: Arc<Stores>,
    gate: Arc<Gate>,
    scope: core::NodeKey,
    target: Address,
    entry: Entry,
    _directory: Directory,
}
impl Fixture {
    async fn new(limits: Limits, budget: u64, data: &[u8], receipt: bool) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = Directory::new(&format!(
            "materialize-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let repository = repository().await;
        let (source, _) = commit_file(&repository, b"abcdefgh", 1).await;
        let (target, _) = commit_file(&repository, b"target!!", 2).await;
        let target = Address::zero_context_hash(target);
        let backend = Arc::new(
            RevisionBackend::new(repository.clone(), source)
                .await
                .unwrap(),
        );
        let scope = backend.root().key;
        let gate = Arc::new(Gate {
            target,
            ..Default::default()
        });
        let namespace = Arc::new(
            Namespace::new(
                backend.clone(),
                backend.root(),
                gate.clone(),
                Default::default(),
            )
            .unwrap(),
        );
        let stores = Arc::new(Stores::default());
        let model = Model::new(
            namespace,
            stores.clone(),
            Arc::new(StorageBudget::new(budget)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            limits,
        )
        .await
        .unwrap();
        let entry = model.lookup(1, b"file").await.unwrap().entry;
        let writer = model.open(entry.inode, WRITE).await.unwrap();
        model.write(writer, 0, data).await.unwrap();
        model.release(writer).await.unwrap();
        if receipt {
            let (hash, address) = commit_file(&repository, data, 3).await;
            let image = model
                .namespace
                .materialized_file(entry.inode)
                .await
                .unwrap()
                .unwrap();
            let metadata = image.metadata().await.unwrap();
            let receipt = Receipt {
                inode: entry.inode,
                authority: entry.authority,
                generation: metadata.generation,
                mode: metadata.mode,
                proof: CommittedProof {
                    node: Node {
                        key: core::NodeKey {
                            revision: Address::zero_context_hash(hash),
                            node: 1,
                            ..entry.authority
                        },
                        kind: NodeKind::File,
                        size: data.len() as u64,
                        mode: 0o444,
                    },
                    address,
                },
                sha256: None,
            };
            let freeze = model.namespace.freeze_operation().await.unwrap();
            model
                .namespace
                .adopt_receipts(freeze.token(), vec![receipt], || Ok(()))
                .await
                .unwrap();
            drop(freeze);
        }
        Self {
            model,
            stores,
            gate,
            scope,
            target,
            entry,
            _directory: directory,
        }
    }
    async fn plan(&self) -> PreparedTransition {
        transition_prepare::prepare(
            self.model.namespace.clone(),
            self.scope,
            self.target,
            Options {
                local: LocalPolicy::KeepLocal,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap()
    }
    async fn layout<'plan>(
        &self,
        plan: &'plan PreparedTransition,
    ) -> PreparedNamespaceLayout<'plan> {
        let prepared = plan.prepare_complete_backend().await.unwrap();
        let binding = self
            .model
            .namespace
            .bind_complete_backend(prepared)
            .unwrap();
        plan.prepare_install_layout(binding).await.unwrap()
    }
}

#[tokio::test]
async fn copied_files_keep_effective_underlay_gate_receipt_and_private_identity() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", true).await;
            let reader = f.model.open(f.entry.inode, READ).await.unwrap();
            let old = f.model.file_handle(reader).unwrap();
            let before = f.model.namespace.snapshot(100).await.unwrap();
            let plan = f.plan().await;
            let layout = f.layout(&plan).await;
            let resources = f.model.prepare_private_resources(layout).await.unwrap();
            assert_eq!(resources.files.len(), 1);
            let (&inode, copied) = resources.files.iter().next().unwrap();
            assert_ne!(inode, f.entry.inode);
            assert!(Arc::ptr_eq(
                copied.file.overlay.test_effective_gate(),
                &resources.view.gate()
            ));
            assert!(Arc::ptr_eq(
                &copied.receipt,
                &copied.file.overlay.receipt_cell()
            ));
            assert_eq!(
                crate::overlay::receipt_lock(&copied.receipt)
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .inode,
                inode
            );
            assert_eq!(
                crate::overlay::receipt_lock(&old.file.overlay.receipt_cell())
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .inode,
                f.entry.inode
            );
            assert!(matches!(
                copied.file.overlay.open(Access::Write).await,
                Err(core::Error::Busy)
            ));
            let copy_reader = copied.file.overlay.open(Access::Read).await.unwrap();
            assert_eq!(
                copied.file.overlay.read(copy_reader, 0, 8).await.unwrap(),
                b"DIRTY123"
            );
            f.gate.target_read.store(true, Ordering::Release);
            assert!(
                f.gate
                    .authorize(&f.entry.authority, Access::Read)
                    .await
                    .is_ok()
            );
            assert_eq!(
                copied.file.overlay.read(copy_reader, 0, 8).await,
                Err(core::Error::PermissionDenied)
            );
            assert_eq!(
                old.file
                    .overlay
                    .read(old.handle().unwrap(), 0, 8)
                    .await
                    .unwrap(),
                b"DIRTY123"
            );
            f.gate.target_read.store(false, Ordering::Release);
            f.gate.source_read.store(true, Ordering::Release);
            assert_eq!(
                copied.file.overlay.read(copy_reader, 0, 8).await,
                Err(core::Error::PermissionDenied)
            );
            f.gate.source_read.store(false, Ordering::Release);
            let fresh = copied.file.overlay.clone();
            let receipt = copied.receipt.clone();
            assert!(!f.model.files.lock().await.contains_key(&inode));
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            assert_eq!(
                before.files,
                f.model.namespace.snapshot(100).await.unwrap().files
            );
            drop(resources);
            drop(plan);
            let writer = fresh.open(Access::Write).await.unwrap();
            fresh.write(writer, 0, b"NEW").await.unwrap();
            fresh.release(writer).await.unwrap();
            assert!(crate::overlay::receipt_lock(&receipt).unwrap().is_none());
            assert_eq!(
                old.file
                    .overlay
                    .read(old.handle().unwrap(), 0, 8)
                    .await
                    .unwrap(),
                b"DIRTY123"
            );
            f.model.release(reader).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn retained_resource_admission_bounds_bundles_and_shutdown_waits_for_drop() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(
                Limits {
                    max_requests: 1,
                    ..Default::default()
                },
                4096,
                b"DIRTY123",
                false,
            )
            .await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            assert_eq!(f.model.requests.available_permits(), 0);
            assert!(matches!(
                f.model.getattr(1).await,
                Err(super::super::Error::Core(core::Error::Busy))
            ));
            let mut shutdown = Box::pin(f.model.shutdown());
            assert!(
                tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
                    .await
                    .is_err()
            );
            drop(resources);
            // Request admission and writer exclusion are separate lifetimes.
            assert_eq!(f.model.requests.available_permits(), 1);
            assert!(matches!(
                f.model.namespace.freeze_operation().await,
                Err(NamespaceError::Busy)
            ));
            drop(plan);
            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .unwrap()
                .unwrap();
        })
        .await;
}

#[tokio::test]
async fn resources_retain_receiver_model_and_factory_without_claiming_exclusive_owner() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let namespace = f.model.namespace.clone();
            let weak_model = Arc::downgrade(&f.model);
            let weak_stores = Arc::downgrade(&f.stores);
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            drop(f.model);
            drop(f.stores);
            assert!(weak_model.upgrade().is_some());
            assert!(weak_stores.upgrade().is_some());
            drop(resources);
            assert!(weak_model.upgrade().is_none());
            assert!(weak_stores.upgrade().is_none());
            assert!(matches!(
                namespace.freeze_operation().await,
                Err(NamespaceError::Busy)
            ));
            drop(plan);
            drop(namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn known_quotas_and_independent_write_denials_precede_store_creation() {
    LORE_CONTEXT
        .scope(execution(), async {
            for case in 0..6 {
                let limits = Limits {
                    max_stores: if case == 0 { 1 } else { 100 },
                    max_checkpoint_data_bytes: if case == 2 { 8 } else { 4096 },
                    ..Default::default()
                };
                let f = Fixture::new(
                    limits,
                    if case == 1 { 15 } else { 4096 },
                    b"DIRTY123",
                    false,
                )
                .await;
                let plan = f.plan().await;
                let layout = f.layout(&plan).await;
                match case {
                    3 => f.gate.source_write.store(true, Ordering::Release),
                    4 => f.gate.candidate.store(true, Ordering::Release),
                    5 => {
                        f.gate.target_write.store(true, Ordering::Release);
                        assert!(
                            f.gate
                                .authorize(&f.entry.authority, Access::Write)
                                .await
                                .is_ok()
                        );
                    }
                    _ => {}
                }
                let before = f.model.namespace.snapshot(100).await.unwrap();
                assert!(
                    f.model.prepare_private_resources(layout).await.is_err(),
                    "case {case}"
                );
                assert_eq!(f.stores.count.load(Ordering::Acquire), 1, "case {case}");
                assert_eq!(f.model.counters.stores.load(Ordering::Acquire), 1);
                assert_eq!(f.model.budget.reserved_bytes(), 8);
                assert_eq!(f.model.requests.available_permits(), limits.max_requests);
                assert_eq!(
                    f.model.namespace.snapshot(100).await.unwrap().files,
                    before.files
                );
                assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            }
        })
        .await;
}

#[tokio::test]
async fn different_namespace_and_stale_source_refuse_without_store_attempts() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let layout = f.layout(&plan).await;
            let root = f.model.namespace.getattr(1).await.unwrap().base.unwrap();
            let source_backend = f.model.namespace.backend().unwrap();
            let provenance = source_backend.checkpoint_state().await.unwrap();
            let independent_backend = source_backend
                .prepare_restore(&provenance)
                .await
                .unwrap()
                .unwrap();
            let namespace = Arc::new(
                Namespace::new(
                    independent_backend,
                    root,
                    f.gate.clone(),
                    Default::default(),
                )
                .unwrap(),
            );
            let stores = Arc::new(Stores::default());
            let other = Model::new(
                namespace,
                stores.clone(),
                Arc::new(StorageBudget::new(4096)),
                Recovery::open(f._directory.0.join("other"), Default::default()).unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
            assert!(matches!(
                other.prepare_private_resources(layout).await,
                Err(PreparationError::Transition(
                    transition_prepare::Error::InvalidScope
                ))
            ));
            assert_eq!(stores.count.load(Ordering::Acquire), 0);
            assert_eq!(
                other.requests.available_permits(),
                other.limits.max_requests
            );
            drop(plan);
            let plan = f.plan().await;
            let layout = f.layout(&plan).await;
            let cold = f.model.lookup(1, b"cold").await.unwrap();
            f.model.lookup(cold.entry.inode, b"leaf").await.unwrap();
            assert!(matches!(
                f.model.prepare_private_resources(layout).await,
                Err(PreparationError::Transition(
                    transition_prepare::Error::StaleSource
                ))
            ));
            assert_eq!(f.stores.count.load(Ordering::Acquire), 1);
            assert_eq!(f.model.budget.reserved_bytes(), 8);
        })
        .await;
}

#[tokio::test]
async fn cancellation_target_revocation_and_close_keep_debt_but_release_admission() {
    LORE_CONTEXT.scope(execution(), async {
        for case in 0..3 {
            let f = Fixture::new(Limits { max_requests: 1, ..Default::default() }, 4096, b"DIRTY123", false).await;
            let before = f.model.namespace.snapshot(100).await.unwrap();
            let plan = f.plan().await; let layout = f.layout(&plan).await;
            f.stores.control.block.store(true, Ordering::Release);
            let mut pending = Box::pin(f.model.prepare_private_resources(layout));
            tokio::select! {
                _=f.stores.control.entered.notified()=>{},
                result=&mut pending=>panic!("private IO pause bypassed: {}", result.is_ok()),
                _=tokio::time::sleep(Duration::from_secs(5))=>panic!("private IO never started"),
            }
            assert_eq!(f.model.budget.reserved_bytes(), 16);
            assert_eq!(f.model.requests.available_permits(), 0);
            match case {
                0 => drop(pending),
                1 => {
                    f.gate.target_write.store(true, Ordering::Release);
                    f.stores.control.resume.notify_one();
                    assert!(tokio::time::timeout(Duration::from_secs(2), pending).await.unwrap().is_err());
                    f.gate.target_write.store(false, Ordering::Release);
                }
                _ => {
                    f.model.stop();
                    assert!(matches!(tokio::time::timeout(Duration::from_secs(2), pending).await.unwrap(), Err(PreparationError::Model(super::super::Error::Core(core::Error::Unmounted)))));
                }
            }
            assert_eq!(f.model.requests.available_permits(), 1);
            assert_eq!(f.stores.count.load(Ordering::Acquire), 2);
            assert_eq!(f.model.counters.stores.load(Ordering::Acquire), 2);
            assert_eq!(f.model.budget.reserved_bytes(), 16);
            assert_eq!(f.model.namespace.snapshot(100).await.unwrap().files, before.files);
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            assert!(matches!(f.model.namespace.freeze_operation().await, Err(NamespaceError::Busy)));
            drop(plan);
        }
    }).await;
}

#[tokio::test]
async fn repeated_preparations_retain_successful_and_failed_store_attempts() {
    LORE_CONTEXT
        .scope(execution(), async {
            for fail in [false, true] {
                let f = Fixture::new(
                    Limits {
                        max_stores: 2,
                        max_requests: 1,
                        ..Default::default()
                    },
                    4096,
                    b"DIRTY123",
                    false,
                )
                .await;
                let plan = f.plan().await;
                let layout = f.layout(&plan).await;
                f.stores.fail.store(fail, Ordering::Release);
                let result = f.model.prepare_private_resources(layout).await;
                assert_eq!(result.is_err(), fail);
                drop(result);
                assert_eq!(f.model.counters.stores.load(Ordering::Acquire), 2);
                assert_eq!(f.model.budget.reserved_bytes(), if fail { 8 } else { 16 });
                f.stores.fail.store(false, Ordering::Release);
                let next = f.layout(&plan).await;
                assert!(matches!(
                    f.model.prepare_private_resources(next).await,
                    Err(PreparationError::Model(super::super::Error::StoreLimit))
                ));
                assert_eq!(f.stores.count.load(Ordering::Acquire), 2);
                assert_eq!(f.model.requests.available_permits(), 1);
            }
        })
        .await;
}

#[tokio::test]
async fn split_copy_extent_memory_is_bounded_before_allocation() {
    LORE_CONTEXT
        .scope(execution(), async {
            let data = vec![b'x'; 1024];
            let f = Fixture::new(Default::default(), 4 * 1024 * 1024, &data, false).await;
            let plan = transition_prepare::prepare(
                f.model.namespace.clone(),
                f.scope,
                f.target,
                Options {
                    local: LocalPolicy::KeepLocal,
                    ..Default::default()
                },
                transition_prepare::Limits {
                    max_nodes: 256,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let layout = f.layout(&plan).await;
            f.stores.control.short.store(1, Ordering::Release);
            assert!(matches!(
                f.model.prepare_private_resources(layout).await,
                Err(PreparationError::Model(super::super::Error::Core(
                    core::Error::ResourceLimit
                )))
            ));
            assert!(f.stores.control.writes.load(Ordering::Acquire) < 1024);
            assert!(f.model.budget.reserved_bytes() > 1024);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            assert_eq!(
                f.model
                    .namespace
                    .materialized_file(f.entry.inode)
                    .await
                    .unwrap()
                    .unwrap()
                    .metadata()
                    .await
                    .unwrap()
                    .reserved_storage_bytes,
                1024
            );
        })
        .await;
}

#[path = "materialize/owned_tests.rs"]
mod owned_tests;

#[path = "materialize/owned/capture_tests.rs"]
mod capture_tests;

#[path = "materialize/successor_tests.rs"]
mod successor_tests;

#[path = "file_cache_tests.rs"]
mod file_cache_tests;

#[path = "materialize/owned/metadata_tests.rs"]
mod metadata_tests;

#[path = "materialize/owned/capture/source_preparation_tests.rs"]
mod source_preparation_tests;

#[path = "materialize/owned/capture/source_publication_tests.rs"]
mod source_publication_tests;

#[path = "materialize/install_tests.rs"]
mod install_tests;
