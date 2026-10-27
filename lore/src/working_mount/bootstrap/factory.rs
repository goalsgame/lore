// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private local/offline bootstrap over one existing owned metadata directory.
//! Creation initializes its contents; it does not create that directory.

use std::path::PathBuf;
use std::sync::Arc;

use lore_io::lease::IoLease;
use lore_revision::fs::provider_registry::Registration;
use lore_revision::repository::RepositoryConfig;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::repository::working_bootstrap::MetadataInitializer;
use lore_revision::repository::working_bootstrap::Mode;
use lore_revision::repository::working_bootstrap::PendingPublication;
use lore_revision::repository::working_bootstrap::Selection;
use lore_revision::repository::working_bootstrap::StoreInitializer;
use lore_revision::repository::working_bootstrap::StoreMode;
use lore_vfs::overlay::AccessGateFactory;

use super::binding::Binding;
use super::job::Job;
use crate::working_mount::*;

#[lore_macro::test_pub]
pub(crate) enum BootstrapMode {
    CreateNew {
        backing: PathBuf,
        config: RepositoryConfig,
        selection: Selection,
    },
    Import {
        backing: PathBuf,
        config: RepositoryConfig,
        selection: Selection,
        seed: lore_revision::repository::working_bootstrap::ImmutableImport,
    },
    Reuse,
    ReuseWithPolicy {
        policy: lore_revision::repository::working_bootstrap::ImportReadPolicy,
    },
    RemoteCreate {
        backing: PathBuf,
        config: RepositoryConfig,
        selection: Selection,
        contents: lore_revision::repository::working_bootstrap::RemoteContents,
    },
    ReuseRemote {
        contents: lore_revision::repository::working_bootstrap::RemoteContents,
    },
}
#[lore_macro::test_pub]
pub(crate) struct BootstrapRequest {
    pub registration: Arc<Registration>,
    pub mode: BootstrapMode,
}
#[lore_macro::test_pub]
pub(crate) enum BootstrapPolicy {
    Injected(Arc<dyn AccessGateFactory>),
    Local(crate::service::state::WorkingAdmission),
}
#[derive(Debug)]
#[lore_macro::test_pub]
pub(crate) struct BootstrapError {
    pub handle: Option<InstanceHandle>,
    pub error: Error,
}
#[cfg(feature = "test-util")]
pub struct BootstrapPause {
    pub prepared: tokio::sync::oneshot::Sender<Arc<lore_vfs::linux_working::Model>>,
    pub resume: std::sync::mpsc::Receiver<()>,
}
enum Finish {
    #[cfg(target_os = "linux")]
    Mount,
    #[cfg(feature = "test-util")]
    Prepare(BootstrapPause),
}
struct Prepared {
    ticket: StartupReservation,
    publication: PendingPublication,
}
impl Supervisor {
    #[cfg(target_os = "linux")]
    #[lore_macro::test_pub]
    pub(crate) async fn bootstrap_mount(
        self: &Arc<Self>,
        request: BootstrapRequest,
        policy: BootstrapPolicy,
        limits: OwnerLimits,
        options: Options,
    ) -> std::result::Result<InstanceHandle, BootstrapError> {
        self.bootstrap_start(request, policy, limits, options, Finish::Mount)
            .await
    }
    #[cfg(feature = "test-util")]
    pub async fn test_bootstrap_prepare(
        self: &Arc<Self>,
        request: BootstrapRequest,
        policy: BootstrapPolicy,
        limits: OwnerLimits,
        options: Options,
        pause: BootstrapPause,
    ) -> std::result::Result<InstanceHandle, BootstrapError> {
        self.bootstrap_start(request, policy, limits, options, Finish::Prepare(pause))
            .await
    }
    async fn bootstrap_start(
        self: &Arc<Self>,
        request: BootstrapRequest,
        policy: BootstrapPolicy,
        limits: OwnerLimits,
        options: Options,
        finish: Finish,
    ) -> std::result::Result<InstanceHandle, BootstrapError> {
        let reject = |error| BootstrapError {
            handle: None,
            error,
        };
        let caller = lore_revision::runtime::try_execution_context()
            .ok_or_else(|| reject(Error::WrongOwner))?;
        if !caller.is_client()
            || caller.globals().dry_run()
            || caller.globals().in_memory()
            || limits.max_backing_bytes == 0
        {
            return Err(reject(Error::InvalidBinding));
        }
        if !request.registration.belongs_to(&self.registry) {
            return Err(reject(Error::WrongOwner));
        }
        let location = request
            .registration
            .bootstrap_location()
            .map_err(|error| reject(Error::Registry(error.to_string())))?;
        let backing = match &request.mode {
            BootstrapMode::CreateNew {
                backing, config, ..
            }
            | BootstrapMode::Import {
                backing, config, ..
            } => {
                if config
                    .shared_store_to_use
                    .as_ref()
                    .is_some_and(|shared| shared.use_shared_store == Some(true))
                    || (!caller.globals().offline()
                        && config
                            .remote_url
                            .as_deref()
                            .is_some_and(|url| !url.is_empty()))
                {
                    return Err(reject(Error::InvalidBinding));
                }
                backing.clone()
            }
            BootstrapMode::RemoteCreate {
                backing,
                config,
                contents,
                ..
            } => {
                contents
                    .validate(location.repository(), config, &caller)
                    .map_err(problem)
                    .map_err(reject)?;
                if config
                    .shared_store_to_use
                    .as_ref()
                    .is_some_and(|shared| shared.use_shared_store == Some(true))
                {
                    return Err(reject(Error::InvalidBinding));
                }
                backing.clone()
            }
            BootstrapMode::Reuse
            | BootstrapMode::ReuseWithPolicy { .. }
            | BootstrapMode::ReuseRemote { .. } => Binding::read(
                location.metadata().path(),
                location.repository(),
                location.instance(),
            )
            .map_err(|_binding| reject(Error::InvalidBinding))?
            .root()
            .to_path_buf(),
        };
        if let BootstrapMode::Import {
            selection, seed, ..
        } = &request.mode
            && (seed.repository_id() != location.repository()
                || seed.revision() != selection.revision)
        {
            return Err(reject(Error::InvalidBinding));
        }
        if let BootstrapMode::ReuseWithPolicy { policy } = &request.mode
            && policy.repository_id() != location.repository()
        {
            return Err(reject(Error::InvalidBinding));
        }
        if let BootstrapMode::ReuseRemote { contents } = &request.mode
            && (contents.repository_id() != location.repository()
                || caller.globals().offline()
                || caller.globals().trust_remote())
        {
            return Err(reject(Error::InvalidBinding));
        }
        // This entry point is not called by a command already owning this root.
        // Acquire before startup accounting; no token-family alias is created.
        let token = RepositoryWriteToken::acquire(location.target().path()).await;
        let reservation = self
            .reserve_bootstrap(request.registration.clone(), &backing)
            .map_err(reject)?;
        let handle = reservation.handle();
        let record = reservation.record();
        let job = Job::new(reservation, token, request.registration.clone(), location);
        let execution = Arc::new(caller.for_background_owner().await);
        let worker = Arc::new(worker::ThreadOwner::new());
        *record
            .preparation_thread
            .lock()
            .map_err(|_poison| BootstrapError {
                handle: Some(handle.clone()),
                error: Error::Poisoned,
            })? = Some(worker.clone());
        let mut cancel = CancelPreparation {
            supervisor: self.clone(),
            handle: handle.clone(),
            armed: true,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let failure = handle.clone();
        let thread = std::thread::Builder::new()
            .name("lore-bootstrap".into())
            .spawn(move || {
                let result = prepare(job, request, backing, policy, execution, limits, options);
                match result {
                    Ok(prepared) => run_prepared(record, prepared, finish, sender),
                    Err(error) => {
                        let _retired = retire(&record, &error.to_string());
                        let _closed = sender.send(Err(error));
                    }
                }
            });
        match thread {
            Ok(thread) => worker.publish(Some(thread)),
            Err(error) => {
                worker.publish(None);
                return Err(BootstrapError {
                    handle: Some(failure),
                    error: Error::Owner(error.to_string()),
                });
            }
        }
        let result = receiver
            .await
            .unwrap_or_else(|_closed| Err(Error::Owner("bootstrap worker terminated".into())));
        #[cfg(feature = "test-util")]
        if matches!(result, Ok(false)) {
            worker.drain().await.map_err(|error| BootstrapError {
                handle: Some(handle.clone()),
                error,
            })?;
        }
        match result {
            Ok(_published) => {
                cancel.armed = false;
                Ok(handle)
            }
            Err(error) => Err(BootstrapError {
                handle: Some(handle),
                error,
            }),
        }
    }
}
fn prepare(
    job: Arc<Job>,
    request: BootstrapRequest,
    backing: PathBuf,
    policy: BootstrapPolicy,
    execution: Arc<lore_revision::interface::ExecutionContext>,
    limits: OwnerLimits,
    options: Options,
) -> Result<Prepared> {
    let fresh = matches!(
        &request.mode,
        BootstrapMode::CreateNew { .. }
            | BootstrapMode::Import { .. }
            | BootstrapMode::RemoteCreate { .. }
    );
    let backfill_requested =
        matches!(&request.mode, BootstrapMode::Import { .. }) && options.digest_cache.is_some();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .map_err(problem)?;
    let scope = IoLease::new(job.clone());
    let (payload, recovery, storage) = runtime.block_on(lore_base::runtime::LORE_CONTEXT.scope(
        execution,
        scope.scope(async {
            let admitted = job.clone();
            let root = backing.clone();
            lore_base::lore_spawn_blocking_nocontext!(move || admitted.admit(&root, fresh, limits))
                .await
                .map_err(problem)??;
            job.check_live()?;
            let (mode, stores) = match request.mode {
                BootstrapMode::CreateNew {
                    config, selection, ..
                } => (Mode::CreateNew(config), StoreMode::CreateNew(selection)),
                BootstrapMode::Import {
                    config,
                    selection,
                    seed,
                    ..
                } => (Mode::CreateNew(config), StoreMode::Import(selection, seed)),
                BootstrapMode::RemoteCreate {
                    config,
                    selection,
                    contents,
                    ..
                } => (
                    Mode::CreateNew(config),
                    StoreMode::RemoteCreate(selection, contents),
                ),
                BootstrapMode::ReuseRemote { contents } => {
                    (Mode::Reuse, StoreMode::ReuseRemote(contents))
                }
                BootstrapMode::Reuse => (Mode::Reuse, StoreMode::Reuse),
                BootstrapMode::ReuseWithPolicy { policy } => {
                    (Mode::Reuse, StoreMode::ReuseWithPolicy(policy))
                }
            };
            let metadata = MetadataInitializer::new(1)
                .map_err(problem)?
                .prepare_pending(request.registration, job.take_token()?, mode)
                .await
                .map_err(problem)?;
            let binding = job.clone();
            let metadata =
                lore_base::lore_spawn_blocking_nocontext!(move || binding.bind(metadata, fresh))
                    .await
                    .map_err(problem)??;
            let root = job.root()?;
            let working = StoreInitializer::new(1)
                .map_err(problem)?
                .prepare_pending_bound(metadata, stores, root)
                .await
                .map_err(problem)?;
            job.verify_backing()?;
            let mut payload = working.into_owner_build().map_err(|_busy| Error::Busy)?;
            let (recovery, storage) = job.finish_backing(&mut payload, limits).await?;
            Ok::<_, Error>((payload, recovery, storage))
        }),
    ))?;
    drop(scope);
    drop(runtime);
    let (context, publication) = payload.into_trusted_owner_parts();
    let mut backfill = None;
    let factory: Arc<dyn AccessGateFactory> = match policy {
        BootstrapPolicy::Injected(factory) => factory,
        BootstrapPolicy::Local(admission) => {
            if backfill_requested {
                backfill = Some(
                    super::super::local_policy::LocalCheckoutFactory::new(
                        admission.clone(),
                        context.execution.clone(),
                    )
                    .map_err(problem)?,
                );
            }
            Arc::new(
                super::super::local_policy::LocalCheckoutFactory::new(
                    admission,
                    context.execution.clone(),
                )
                .map_err(problem)?,
            )
        }
    };
    let input = StartupInput::from_early_admission(context, storage, recovery, limits.recovery)?;
    let ticket = job
        .take_ticket()?
        .promote(input)
        .map_err(|error| error.error)?;
    let setup = publication.io_lease().map_err(problem)?;
    let owner = lore_io::lease::sync_scope(Some(setup), || {
        owner::build_with_backfill(&ticket, factory, limits, options, backfill)
    })?;
    *ticket
        .record
        .owner
        .lock()
        .map_err(|_poison| Error::Poisoned)? = Some(owner);
    // Only the permit retains the emptied Job across publication; no long-lived
    // session thread captures its caller accounting independently.
    drop(job);
    Ok(Prepared {
        ticket,
        publication,
    })
}
fn run_prepared(
    record: Arc<Record>,
    prepared: Prepared,
    finish: Finish,
    ready: tokio::sync::oneshot::Sender<Result<bool>>,
) {
    let Prepared {
        ticket,
        mut publication,
    } = prepared;
    let ticket = Some(ticket);
    #[cfg(target_os = "linux")]
    let mut ticket = ticket;
    let mut ready = Some(ready);
    let result: Result<()> = match finish {
        #[cfg(feature = "test-util")]
        Finish::Prepare(pause) => {
            let owner = record
                .owner
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
                .unwrap();
            if pause.prepared.send(owner.model.clone()).is_ok() {
                pause.resume.recv().map_err(|_closed| Error::Closed)
            } else {
                Err(Error::Closed)
            }
        }
        #[cfg(target_os = "linux")]
        Finish::Mount => {
            // SAFETY: effective process identity is not caller supplied.
            let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
            linux::run_bootstrap(&record, &mut ticket, uid, gid, &mut ready, &mut publication)
        }
    };
    let _retired = retire(&record, "bootstrap worker stopping");
    let _quarantined = publication.quarantine();
    drop(ticket);
    let drained = drain_owned(&record);
    if let Err(error) = drained {
        if let Some(sender) = ready.take() {
            let _closed = sender.send(Err(error.clone()));
        }
        record
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .error = Some(error.to_string());
        // Retain uncertain resources on the already-owned worker. It never
        // reports an actual exit or successful retirement for this failure.
        let _retained = (record, publication, ready);
        loop {
            std::thread::park();
        }
    }
    if let Err(error) = &result {
        record
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .error = Some(error.to_string());
    }
    drop(publication);
    if let Some(sender) = ready.take() {
        let _closed = sender.send(result.map(|()| false));
    }
}
fn drain_owned(record: &Arc<Record>) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(problem)?;
    let owner = record
        .owner
        .lock()
        .map_err(|_poison| Error::Poisoned)?
        .clone();
    runtime.block_on(async {
        if let Some(owner) = owner {
            owner.retire().await?;
        }
        if let Some(input) = record.input.get() {
            input.storage.close_and_drain_io().await.map_err(problem)?;
        }
        Ok(())
    })
}
fn problem(error: impl std::fmt::Debug) -> Error {
    Error::Owner(format!("{error:?}"))
}
