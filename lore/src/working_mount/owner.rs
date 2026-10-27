// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::types::Address;
use lore_vfs::core::Backend;
use lore_vfs::linux_working::Model;
use lore_vfs::namespace::Namespace;
use lore_vfs::overlay::Access;
use lore_vfs::overlay::AccessGate;
use lore_vfs::overlay::AccessGateFactory;
use lore_vfs::provider::WorkingProvider;
use lore_vfs::revision::RevisionBackend;
use lore_vfs::storage_budget::StorageBudget;
use lore_vfs::working_storage::Session;

use super::Error;
use super::OwnerLimits;
use super::Result;
use super::StartupReservation;

mod backfill;

pub(super) struct Owner {
    pub model: Arc<Model>,
    pub provider: Arc<WorkingProvider>,
    _session: Arc<Session>,
    pub(super) _target: super::mountpoint::Target,
    retired: AtomicBool,
    checkpointed: AtomicBool,
    runtime_draining: AtomicBool,
    retirement: tokio::sync::Mutex<Option<Error>>,
    _runtime: RuntimeOwner,
    _registration: Arc<lore_revision::fs::provider_registry::Registration>,
}
impl Owner {
    #[cfg(any(target_os = "linux", feature = "test-util"))]
    pub(super) fn runtime_handle(&self) -> Result<tokio::runtime::Handle> {
        self._runtime
            .pending
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .as_ref()
            .map(|job| job.runtime.handle().clone())
            .ok_or(Error::Closed)
    }
    pub async fn retire(&self) -> Result<()> {
        let mut retirement = self.retirement.lock().await;
        if let Some(error) = retirement.as_ref() {
            return Err(error.clone());
        }
        let result = self.retire_inner().await;
        if let Err(error) = &result {
            *retirement = Some(error.clone());
        }
        result
    }
    async fn retire_inner(&self) -> Result<()> {
        if self.retired.load(Ordering::Acquire) {
            return Ok(());
        }
        if !self.runtime_draining.load(Ordering::Acquire) {
            self.provider.close_admission();
            self.provider.cancel_operations();
            self.model.close_requests();
            self._registration
                .retire()
                .map_err(|error| Error::Registry(error.to_string()))?;
            self.model
                .checkpoint_owner()
                .retain_retirement_guard(self._registration.clone())
                .map_err(problem)?;
            self.provider.drain_operations().await.map_err(problem)?;
            self.provider.drain_exports().await.map_err(problem)?;
            if !self.checkpointed.load(Ordering::Acquire) {
                // Keep the successful checkpoint stage across cancellation; the
                // subsequent shutdown closes publication permanently.
                self.model
                    .checkpoint_for_retirement()
                    .await
                    .map_err(problem)?;
                self.checkpointed.store(true, Ordering::Release);
            }
            self.model.shutdown().await.map_err(problem)?;
            self.runtime_draining.store(true, Ordering::Release);
        }
        self._runtime.drain().await?;
        self.retired.store(true, Ordering::Release);
        Ok(())
    }
}
fn problem(error: impl std::fmt::Debug) -> Error {
    Error::Owner(format!("{error:?}"))
}

/// Runs on the owning setup thread. Its ticket stays outside the private runtime,
/// so cancellation/runtime teardown cannot release it before actual blocking jobs.
pub(super) fn build(
    ticket: &StartupReservation,
    gate_factory: Arc<dyn AccessGateFactory>,
    limits: OwnerLimits,
    options: super::Options,
) -> Result<Arc<Owner>> {
    build_with_backfill(ticket, gate_factory, limits, options, None)
}

pub(super) fn build_with_backfill(
    ticket: &StartupReservation,
    gate_factory: Arc<dyn AccessGateFactory>,
    limits: OwnerLimits,
    options: super::Options,
    backfill_policy: Option<super::local_policy::LocalCheckoutFactory>,
) -> Result<Arc<Owner>> {
    ticket.check_live()?;
    if limits.max_backing_bytes == 0 {
        return Err(Error::InvalidLimits);
    }
    let input = ticket.record.input()?;
    let (recovery, original) = input.take_recovery(limits.recovery)?;
    let target = super::mountpoint::Target::capture(&ticket.record)?;
    let index = super::cache::prepare(ticket, options.digest_cache)?;
    let session = input.storage.begin_session().map_err(problem)?;
    ticket.check_live()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .max_blocking_threads(1)
        .build()
        .map_err(|error| Error::Owner(error.to_string()))?;
    let execution = input.prepared.execution.clone();
    let gate_factory: Arc<dyn AccessGateFactory> = Arc::new(OwnerGateFactory {
        factory: gate_factory,
        execution: execution.clone(),
    });
    let mut cancelled = ticket.record.cancel.subscribe();
    let result = runtime.block_on(lore_base::runtime::LORE_CONTEXT.scope(execution, async {
        tokio::select! {
            biased;
            _ = async { while !*cancelled.borrow() { if cancelled.changed().await.is_err() { break; } } } => Err(Error::Closed),
            result = async {
                let revision = original.as_ref().map_or(input.prepared.selection.revision, |root| root.key.revision.hash);
                let backend=RevisionBackend::new(input.prepared.repository.clone(),revision).await.map_err(problem)?;
                let backend=Arc::new(if let Some(index)=index.as_ref() {backend.with_digest_index(index.clone())} else {backend});
                if let Some(policy) = backfill_policy.as_ref() {
                    Box::pin(backfill::populate(backend.clone(), policy, limits)).await?;
                    ticket.check_live()?;
                }
                let root = backend.root();
                if original.as_ref().is_some_and(|stored| stored != &root) { return Err(Error::InvalidBinding); }
                let erased: Arc<dyn Backend> = backend;
                let budget = Arc::new(StorageBudget::new(limits.max_backing_bytes));
                let factory = session.factory();
                let model = if original.is_some() {
                    Model::restore_with_gate_factory(erased.clone(), root.clone(), gate_factory, limits.namespace, factory, budget, recovery, limits.kernel).await.map_err(problem)?
                } else {
                    let namespace = Arc::new(Namespace::new_with_gate_factory(erased.clone(), root.clone(), gate_factory, limits.namespace).map_err(problem)?);
                    namespace.getattr(1).await.map_err(problem)?;
                    Model::new(namespace, factory, budget, recovery, limits.kernel).await.map_err(problem)?
                };
                if original.is_some() {
                    let scope = erased.owning_scope(&root.key).await.map_err(problem)?;
                    let baselines = model.namespace().held_baselines().await.map_err(problem)?;
                    let held = baselines.iter().find(|baseline| baseline.scope == scope.root).map_or(scope.initial_revision, |baseline| baseline.head.revision);
                    let held = lore_revision::state::State::deserialize(input.prepared.repository.clone(), held.hash).await.map_err(problem)?.committed_revision();
                    let selected = lore_revision::state::State::deserialize(input.prepared.repository.clone(), input.prepared.selection.revision).await.map_err(problem)?.committed_revision();
                    if Address::zero_context_hash(held) != Address::zero_context_hash(selected) { return Err(Error::AnchorMismatch); }
                }
                let provider = WorkingProvider::from_restored_namespace(model.namespace().clone(), session.export_directory().to_path_buf(), limits.provider).await.map_err(problem)?
                    .with_checkpoint_owner(model.checkpoint_owner()).map_err(problem)?
                    .with_model(&model).map_err(problem)?;
                let provider=if let Some(index)=index.as_ref() {provider.with_digest_index(index.clone())} else {provider};
                Ok(Arc::new(Owner { model, provider:Arc::new(provider), _session:session.clone(), _target:target, retired:AtomicBool::new(false), checkpointed:AtomicBool::new(false), runtime_draining:AtomicBool::new(false), retirement:tokio::sync::Mutex::new(None), _runtime:RuntimeOwner::empty(), _registration:input.prepared.registration.clone() }))
            } => result,
        }
    }));
    match result {
        Ok(mut owner) => {
            Arc::get_mut(&mut owner)
                .ok_or(Error::InvalidBinding)?
                ._runtime =
                RuntimeOwner::new(runtime, session, input.prepared.registration.clone());
            Ok(owner)
        }
        Err(error) => {
            drop(runtime);
            Err(error)
        }
    }
}

// Runtime teardown owns the session and registration until all its actual jobs
// finish, even when the caller awaiting retirement is cancelled.
struct RuntimeOwner {
    pending: std::sync::Mutex<Option<RuntimeJob>>,
    done: tokio::sync::watch::Sender<bool>,
}
struct RuntimeJob {
    runtime: tokio::runtime::Runtime,
    _session: Arc<Session>,
    _registration: Arc<lore_revision::fs::provider_registry::Registration>,
}
impl RuntimeOwner {
    fn empty() -> Self {
        Self {
            pending: std::sync::Mutex::new(None),
            done: tokio::sync::watch::channel(true).0,
        }
    }
    fn new(
        runtime: tokio::runtime::Runtime,
        session: Arc<Session>,
        registration: Arc<lore_revision::fs::provider_registry::Registration>,
    ) -> Self {
        Self {
            pending: std::sync::Mutex::new(Some(RuntimeJob {
                runtime,
                _session: session,
                _registration: registration,
            })),
            done: tokio::sync::watch::channel(false).0,
        }
    }
    fn start(&self) -> Result<()> {
        let Some(job) = self
            .pending
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .take()
        else {
            return Ok(());
        };
        let retained = Arc::new(std::sync::Mutex::new(Some(job)));
        let worker = retained.clone();
        let done = self.done.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("lore-working-runtime".into())
            .spawn(move || {
                let job = worker
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
                if let Some(RuntimeJob {
                    runtime,
                    _session,
                    _registration,
                }) = job
                {
                    drop(runtime);
                    drop(_session);
                    drop(_registration);
                }
                done.send_replace(true);
            })
        {
            // A failed thread admission retains claims and never reports completion.
            #[allow(clippy::mem_forget)]
            std::mem::forget(retained);
            return Err(Error::Owner(error.to_string()));
        }
        Ok(())
    }
    async fn drain(&self) -> Result<()> {
        let mut done = self.done.subscribe();
        self.start()?;
        while !*done.borrow_and_update() {
            done.changed().await.map_err(|_closed| Error::Closed)?;
        }
        Ok(())
    }
}
impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        let _retained = self.start();
    }
}

struct OwnerGateFactory {
    factory: Arc<dyn AccessGateFactory>,
    execution: Arc<lore_revision::interface::ExecutionContext>,
}
impl AccessGateFactory for OwnerGateFactory {
    fn bind(&self, backend: Arc<dyn Backend>) -> lore_vfs::core::Result<Arc<dyn AccessGate>> {
        Ok(Arc::new(OwnerGate {
            gate: self.factory.bind(backend)?,
            execution: self.execution.clone(),
        }))
    }
}

struct OwnerGate {
    gate: Arc<dyn AccessGate>,
    execution: Arc<lore_revision::interface::ExecutionContext>,
}
impl AccessGate for OwnerGate {
    fn authorize_candidate_edit<'a>(
        &'a self,
        edit: &'a lore_vfs::overlay::CandidateEdit,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(self.execution.clone(), async move {
                self.gate.authorize_candidate_edit(edit).await
            }),
        )
    }

    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a lore_vfs::overlay::ProvenTargetMetadata,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(self.execution.clone(), async move {
                self.gate.authorize_target_head(proof).await
            }),
        )
    }

    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a lore_vfs::overlay::ProvenTargetMetadata,
        key: &'a lore_vfs::core::NodeKey,
        access: Access,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(self.execution.clone(), async move {
                self.gate.authorize_target_node(proof, key, access).await
            }),
        )
    }

    fn authorize_selection<'a>(
        &'a self,
        selection: &'a lore_vfs::overlay::TargetSelection,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(self.execution.clone(), async move {
                self.gate.authorize_selection(selection).await
            }),
        )
    }

    fn authorize<'a>(
        &'a self,
        key: &'a lore_vfs::core::NodeKey,
        access: Access,
    ) -> lore_vfs::core::BackendFuture<'a, ()> {
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(self.execution.clone(), async move {
                self.gate.authorize(key, access).await
            }),
        )
    }
}
