// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Ownership and reservations for service-owned working mounts. Reserving startup
//! does not publish a provider or start a kernel session. Failed/cancelled startup
//! retains its registration and backing storage until explicitly drained.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use futures::StreamExt;
use lore_revision::fs::provider_registry::ProviderRegistry;
use lore_revision::fs::provider_registry::ProviderResolver;
use lore_revision::repository::working_bootstrap::PreparedWorkingContext;
use lore_vfs::working_storage::WorkingStorage;

#[cfg(not(feature = "test-util"))]
pub(crate) mod bootstrap;
#[cfg(feature = "test-util")]
pub mod bootstrap;
mod cache;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(feature = "test-util"))]
pub(crate) mod local_policy;
#[cfg(feature = "test-util")]
pub mod local_policy;
mod mountpoint;
mod owner;
#[cfg(not(feature = "test-util"))]
mod reservation;
#[cfg(feature = "test-util")]
pub mod reservation;
use reservation::ReservedPaths;
#[cfg(not(feature = "test-util"))]
pub(crate) mod worker;
#[cfg(feature = "test-util")]
pub mod worker;

#[derive(Clone, Debug, Default)]
pub struct Options {
    /// Optional verified digest index. No cache is created or scanned by default.
    pub digest_cache: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
pub struct OwnerLimits {
    pub max_backing_bytes: u64,
    pub namespace: lore_vfs::namespace::Limits,
    pub kernel: lore_vfs::linux_working::Limits,
    pub provider: lore_vfs::provider::Limits,
    pub recovery: lore_vfs::recovery::Limits,
}
impl Default for OwnerLimits {
    fn default() -> Self {
        Self {
            max_backing_bytes: 64 * 1024 * 1024,
            namespace: Default::default(),
            kernel: Default::default(),
            provider: Default::default(),
            recovery: lore_vfs::recovery::Limits {
                max_checkpoint_bytes: 128 * 1024 * 1024,
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_instances: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_instances: 16 }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Ready,
    Retiring,
}
#[derive(Clone, Debug)]
pub struct InstanceHandle {
    id: u64,
    owner: Arc<()>,
}
impl InstanceHandle {
    pub fn id(&self) -> u64 {
        self.id
    }
}
#[derive(Clone, Debug)]
pub struct Status {
    pub id: u64,
    pub mountpoint: PathBuf,
    pub phase: Phase,
    pub error: Option<String>,
}
#[derive(Clone, Debug)]
pub enum Error {
    InvalidLimits,
    InvalidBinding,
    Capacity,
    Overlap,
    IdentifierExhausted,
    WrongOwner,
    Missing,
    Busy,
    Closed,
    Registry(String),
    Storage(String),
    Poisoned,
    Owner(String),
    AnchorMismatch,
    Cache(String),
    Mountpoint(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits => formatter.write_str("invalid working instance limits"),
            Self::InvalidBinding => {
                formatter.write_str("working instance resources have different owners or paths")
            }
            Self::Capacity => formatter.write_str("working instance capacity is exhausted"),
            Self::Overlap => formatter.write_str("working instance paths overlap another owner"),
            Self::IdentifierExhausted => {
                formatter.write_str("working instance identity space is exhausted")
            }
            Self::WrongOwner => {
                formatter.write_str("working instance handle belongs to another supervisor")
            }
            Self::Missing => formatter.write_str("working instance handle is stale"),
            Self::Busy => formatter.write_str("working instance startup is still active"),
            Self::Closed => formatter.write_str("working instance is retiring"),
            Self::Registry(error) => write!(formatter, "working registration: {error}"),
            Self::Storage(error) => write!(formatter, "working backing storage: {error}"),
            Self::Poisoned => formatter.write_str("working supervisor state is poisoned"),
            Self::Mountpoint(error) => write!(formatter, "working mountpoint: {error}"),
            Self::Cache(error) => write!(formatter, "working digest cache: {error}"),
            Self::Owner(error) => write!(formatter, "working instance: {error}"),
            Self::AnchorMismatch => formatter
                .write_str("normal root anchor is inconsistent with the checkpoint logical head"),
        }
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

pub struct StartupInput {
    #[cfg(feature = "test-util")]
    pub prepared: PreparedWorkingContext,
    #[cfg(not(feature = "test-util"))]
    pub(crate) prepared: PreparedWorkingContext,
    #[cfg(feature = "test-util")]
    pub storage: Arc<WorkingStorage>,
    #[cfg(not(feature = "test-util"))]
    pub(crate) storage: Arc<WorkingStorage>,
    admission: Mutex<Option<bootstrap::admission::RecoveryAdmission>>,
}
/// Rejected admission returns ownership to its caller; it does not release an
/// unrelated registration or silently discard prepared metadata.
pub struct RejectedStartup {
    pub error: Error,
    pub input: Box<StartupInput>,
}
struct RecordState {
    phase: Phase,
    startup_active: bool,
    failure: Option<Error>,
    error: Option<String>,
}
struct Record {
    id: u64,
    paths: ReservedPaths,
    input: std::sync::OnceLock<StartupInput>,
    state: Mutex<RecordState>,
    owner: Mutex<Option<Arc<owner::Owner>>>,
    preparation_thread: Mutex<Option<Arc<worker::ThreadOwner>>>,
    cancel: tokio::sync::watch::Sender<bool>,
    startup_settled: tokio::sync::watch::Sender<bool>,
    #[cfg(target_os = "linux")]
    mount: Mutex<Option<Arc<linux::MountWorker>>>,
    #[cfg(all(target_os = "linux", feature = "test-util"))]
    publication_pause: Mutex<Option<linux::PublicationPause>>,
    digest_cache: Mutex<Option<PathBuf>>,
    // Drop the reservation after every record-owned model/session/storage field.
    _registration: Arc<lore_revision::fs::provider_registry::Registration>,
}
impl Record {
    fn input(&self) -> Result<&StartupInput> {
        self.input.get().ok_or(Error::Busy)
    }
}
struct RejectedRecord {
    error: Error,
    input: Option<Box<StartupInput>>,
}
struct State {
    closed: bool,
    next: u64,
    records: BTreeMap<u64, Arc<Record>>,
}
pub struct Supervisor {
    registry: Arc<ProviderRegistry>,
    identity: Arc<()>,
    limits: Limits,
    state: Mutex<State>,
}
impl Supervisor {
    pub fn new(registry: Arc<ProviderRegistry>, limits: Limits) -> Result<Arc<Self>> {
        if limits.max_instances == 0 || limits.max_instances > 4096 {
            return Err(Error::InvalidLimits);
        }
        Ok(Arc::new(Self {
            registry,
            identity: Arc::new(()),
            limits,
            state: Mutex::new(State {
                closed: false,
                next: 1,
                records: BTreeMap::new(),
            }),
        }))
    }
    /// Lists the bounded retained records, including cancelled startup whose
    /// caller never received a handle. No registry/provider callbacks run here.
    pub fn instances(&self) -> Result<Vec<(InstanceHandle, Status)>> {
        let records: Vec<_> = self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .records
            .values()
            .cloned()
            .collect();
        records
            .into_iter()
            .map(|record| {
                let state = record.state.lock().map_err(|_poison| Error::Poisoned)?;
                Ok((
                    InstanceHandle {
                        id: record.id,
                        owner: self.identity.clone(),
                    },
                    Status {
                        id: record.id,
                        mountpoint: record.paths.target().to_path_buf(),
                        phase: state.phase,
                        error: state.error.clone(),
                    },
                ))
            })
            .collect()
    }
    pub fn reserve_startup(
        self: &Arc<Self>,
        input: StartupInput,
    ) -> std::result::Result<StartupReservation, RejectedStartup> {
        let admission = self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)
            .and_then(|state| {
                if state.closed {
                    Err(Error::Closed)
                } else {
                    Ok(())
                }
            })
            .and_then(|()| self.validate_input(&input));
        if let Err(error) = admission {
            return Err(RejectedStartup {
                error,
                input: Box::new(input),
            });
        }
        let registration = input.prepared.registration.clone();
        let paths = ReservedPaths::from_input(&input);
        self.reserve_record(registration, paths, Some(input))
            .map_err(|rejected| RejectedStartup {
                error: rejected.error,
                input: rejected
                    .input
                    .expect("prebuilt admission returns its input"),
            })
    }
    fn reserve_record(
        self: &Arc<Self>,
        registration: Arc<lore_revision::fs::provider_registry::Registration>,
        paths: ReservedPaths,
        mut input: Option<StartupInput>,
    ) -> std::result::Result<StartupReservation, RejectedRecord> {
        let admitted = (|| {
            let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
            if state.closed {
                return Err(Error::Closed);
            }
            if state.records.len() >= self.limits.max_instances {
                return Err(Error::Capacity);
            }
            for record in state.records.values() {
                let cache = record
                    .digest_cache
                    .lock()
                    .map_err(|_poison| Error::Poisoned)?;
                if record
                    .paths
                    .paths()
                    .iter()
                    .any(|old| paths.paths().iter().any(|new| overlaps(old, new)))
                    || cache
                        .as_ref()
                        .is_some_and(|cache| paths.paths().iter().any(|new| overlaps(cache, new)))
                {
                    return Err(Error::Overlap);
                }
            }
            let id = state.next;
            let next = id.checked_add(1).ok_or(Error::IdentifierExhausted)?;
            let input = input
                .take()
                .map_or_else(std::sync::OnceLock::new, std::sync::OnceLock::from);
            let record = Arc::new(Record {
                id,
                paths,
                input,
                state: Mutex::new(RecordState {
                    phase: Phase::Starting,
                    startup_active: true,
                    failure: None,
                    error: None,
                }),
                owner: Mutex::new(None),
                preparation_thread: Mutex::new(None),
                cancel: tokio::sync::watch::channel(false).0,
                startup_settled: tokio::sync::watch::channel(false).0,
                #[cfg(target_os = "linux")]
                mount: Mutex::new(None),
                #[cfg(all(target_os = "linux", feature = "test-util"))]
                publication_pause: Mutex::new(None),
                digest_cache: Mutex::new(None),
                _registration: registration,
            });
            state.next = next;
            state.records.insert(id, record.clone());
            Ok(record)
        })();
        match admitted {
            Ok(record) => Ok(StartupReservation {
                supervisor: self.clone(),
                record,
                active: true,
            }),
            Err(error) => Err(RejectedRecord {
                error,
                input: input.map(Box::new),
            }),
        }
    }
    fn validate_input(&self, input: &StartupInput) -> Result<()> {
        let location = &input.prepared.location;
        let context = &input.prepared.repository;
        if !input.prepared.registration.belongs_to(&self.registry)
            || input.prepared.registration.target() != location.target()
            || context.id != location.repository()
            || context.instance_id != location.instance()
            || input.storage.repository() != location.repository()
            || input.storage.instance() != location.instance()
            || context.try_write_token().is_some()
            || context.path().ok_or(Error::InvalidBinding)? != location.target().path()
            || context
                .dot_dir_path()
                .map_err(|_missing| Error::InvalidBinding)?
                != location.metadata().path()
            || overlaps(input.storage.root(), location.target().path())
            || overlaps(input.storage.root(), location.metadata().path())
        {
            return Err(Error::InvalidBinding);
        }
        match self.registry.resolve_location(location) {
            Err(lore_revision::fs::provider_registry::Error::NotReady) => {}
            Err(lore_revision::fs::provider_registry::Error::Retired) => return Err(Error::Closed),
            _ => return Err(Error::InvalidBinding),
        }
        Ok(())
    }
    fn record(&self, handle: &InstanceHandle) -> Result<Arc<Record>> {
        if !Arc::ptr_eq(&self.identity, &handle.owner) {
            return Err(Error::WrongOwner);
        }
        self.state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .records
            .get(&handle.id)
            .cloned()
            .ok_or(Error::Missing)
    }
    #[cfg(feature = "test-util")]
    pub fn test_instance_model(
        &self,
        handle: &InstanceHandle,
    ) -> Result<Arc<lore_vfs::linux_working::Model>> {
        let record = self.record(handle)?;
        let owner = record.owner.lock().map_err(|_poison| Error::Poisoned)?;
        Ok(owner.as_ref().ok_or(Error::Busy)?.model.clone())
    }
    pub fn status(&self, handle: &InstanceHandle) -> Result<Status> {
        let record = self.record(handle)?;
        let state = record.state.lock().map_err(|_poison| Error::Poisoned)?;
        Ok(Status {
            id: record.id,
            mountpoint: record.paths.target().to_path_buf(),
            phase: state.phase,
            error: state.error.clone(),
        })
    }
    /// Terminally reject new startup, serialized with record admission. Existing
    /// startup remains owned until explicitly retired or successfully published.
    pub fn close_startup_admission(&self) -> Result<()> {
        self.state.lock().map_err(|_poison| Error::Poisoned)?.closed = true;
        Ok(())
    }
    /// Close startup admission and synchronously fence every retained owner
    /// before returning or performing any drain. A failed fence retains its record.
    pub fn request_retirement_all(&self) -> Result<Vec<RetirementFailure>> {
        let records = {
            let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
            state.closed = true;
            state.records.values().cloned().collect::<Vec<_>>()
        };
        let mut failures = Vec::new();
        for record in records {
            if let Err(error) = retire(&record, "supervisor shutdown requested") {
                failures.push(RetirementFailure {
                    handle: InstanceHandle {
                        id: record.id,
                        owner: self.identity.clone(),
                    },
                    error,
                });
            }
        }
        Ok(failures)
    }
    /// Drain independent owners concurrently after fencing all admission.
    /// Cancellation preserves closed admission and all unfinished reservations.
    /// Service callers must separately fence and drain normal command invocations
    /// before final resource removal; provider-phase drain alone cannot do that.
    pub async fn close_and_drain(&self) -> Result<RetirementReport> {
        let _fence_failures = self.request_retirement_all()?;
        let records = self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .records
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut pending = futures::stream::FuturesUnordered::new();
        for record in records {
            pending.push(Box::pin(async move {
                let handle = InstanceHandle {
                    id: record.id,
                    owner: self.identity.clone(),
                };
                let mut settled = record.startup_settled.subscribe();
                let result = async {
                    while !*settled.borrow_and_update() {
                        settled.changed().await.map_err(|_closed| Error::Closed)?;
                    }
                    match self.retire_instance(&handle).await {
                        Err(Error::Missing) => Ok(()),
                        result => result,
                    }
                }
                .await;
                (handle, result)
            }));
        }
        let mut report = RetirementReport::default();
        while let Some((handle, result)) = pending.next().await {
            match result {
                Ok(()) => report.completed.push(handle),
                Err(error) => report.failures.push(RetirementFailure { handle, error }),
            }
        }
        Ok(report)
    }
    /// Fence a startup without releasing its reservation. Its active startup
    /// ticket must settle before resources can be removed.
    pub fn cancel_startup(&self, handle: &InstanceHandle) -> Result<()> {
        let record = self.record(handle)?;
        retire(&record, "startup cancelled")
    }
    /// Withdraw publication and close model/provider admission synchronously.
    /// Resources remain reserved until explicit retirement completes.
    pub fn request_retirement(&self, handle: &InstanceHandle) -> Result<()> {
        let record = self.record(handle)?;
        retire(&record, "retirement requested")
    }
    /// Wait for kernel termination, durable final checkpoint and actual backing
    /// IO/runtime teardown. Failures retain the record and its reservation.
    /// Independent normal command invocations must already be drained by the caller.
    pub async fn retire_instance(&self, handle: &InstanceHandle) -> Result<()> {
        self.retire_unstarted(handle).await
    }
    /// Retire an owner after its active startup ticket has settled. A failed
    /// or cancelled drain leaves the record in Retiring and may be retried.
    pub async fn retire_unstarted(&self, handle: &InstanceHandle) -> Result<()> {
        let record = self.record(handle)?;
        {
            let state = record.state.lock().map_err(|_poison| Error::Poisoned)?;
            if state.startup_active {
                return Err(Error::Busy);
            }
        }
        retire(&record, "startup retired")?;
        let preparation = record
            .preparation_thread
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .clone();
        if let Some(preparation) = preparation {
            preparation.drain().await?;
        }
        #[cfg(target_os = "linux")]
        {
            let mount = record
                .mount
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .clone();
            if let Some(mount) = mount {
                mount.drain().await?;
            }
        }
        let owner = record
            .owner
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .clone();
        if let Some(owner) = owner
            && let Err(error) = owner.retire().await
        {
            record
                .state
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .error = Some(error.to_string());
            return Err(error);
        }
        if let Some(input) = record.input.get()
            && let Err(error) = input.storage.close_and_drain_io().await
        {
            record
                .state
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .error = Some(error.to_string());
            return Err(Error::Storage(error.to_string()));
        }
        let removed = {
            let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
            if state
                .records
                .get(&record.id)
                .is_none_or(|current| !Arc::ptr_eq(current, &record))
            {
                return Err(Error::Missing);
            }
            state.records.remove(&record.id)
        };
        drop(removed);
        Ok(())
    }
}
fn overlaps(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}
fn retire(record: &Record, reason: &str) -> Result<()> {
    let poisoned = {
        let (mut state, poisoned) = match record.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        state.phase = Phase::Retiring;
        if state.error.is_none() {
            state.error = Some(reason.into());
        }
        poisoned
    };
    let withdrawn = record
        ._registration
        .retire()
        .map_err(|error| Error::Registry(error.to_string()));
    record.cancel.send_replace(true);
    let owner = record
        .owner
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    if let Some(owner) = owner {
        owner.provider.close_admission();
        owner.provider.cancel_operations();
        owner.model.close_requests();
    }
    let mut state = record
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let result = if poisoned {
        Err(Error::Poisoned)
    } else {
        withdrawn
    };
    if let Err(error) = result {
        state.failure.get_or_insert(error);
    }
    if let Some(error) = state.failure.clone() {
        state.error = Some(error.to_string());
        return Err(error);
    }
    Ok(())
}
/// Retain this ticket through startup construction. Dropping it fences future
/// publication and leaves all record-owned resources available for cleanup.
pub struct StartupReservation {
    supervisor: Arc<Supervisor>,
    record: Arc<Record>,
    active: bool,
}
impl StartupReservation {
    pub fn handle(&self) -> InstanceHandle {
        InstanceHandle {
            id: self.record.id,
            owner: self.supervisor.identity.clone(),
        }
    }
    pub fn check_live(&self) -> Result<()> {
        let state = self
            .record
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?;
        if state.phase != Phase::Starting {
            return Err(Error::Closed);
        }
        Ok(())
    }
}
impl Drop for StartupReservation {
    fn drop(&mut self) {
        if self.active {
            let _retired = retire(&self.record, "startup ownership dropped");
            self.record
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .startup_active = false;
            self.record.startup_settled.send_replace(true);
        }
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        let records = std::mem::take(
            &mut self
                .state
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .records,
        );
        // Fence every provider before any potentially stuck kernel/runtime drain.
        let fenced = records
            .into_values()
            .map(|record| {
                let result = retire(&record, "supervisor ownership dropped");
                (record, result)
            })
            .collect::<Vec<_>>();
        for (record, fence) in fenced {
            let retained = record.clone();
            let spawned = std::thread::Builder::new()
                .name("lore-working-retire".into())
                .spawn(move || {
                    let result = fence.and_then(|()| {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .max_blocking_threads(1)
                            .build()
                            .map_err(|error| Error::Owner(error.to_string()))?;
                        let preparation = record
                            .preparation_thread
                            .lock()
                            .map_err(|_poison| Error::Poisoned)?
                            .clone();
                        if let Some(preparation) = preparation {
                            runtime.block_on(preparation.drain())?;
                        }
                        #[cfg(target_os = "linux")]
                        {
                            let mount = record
                                .mount
                                .lock()
                                .map_err(|_poison| Error::Poisoned)?
                                .clone();
                            if let Some(mount) = mount {
                                runtime.block_on(mount.drain())?;
                            }
                        }
                        let owner = record
                            .owner
                            .lock()
                            .map_err(|_poison| Error::Poisoned)?
                            .clone();
                        if let Some(owner) = owner {
                            let result =
                                runtime.block_on(lore_base::runtime::LORE_CONTEXT.scope(
                                    record.input()?.prepared.execution.clone(),
                                    owner.retire(),
                                ));
                            result?;
                        }
                        if let Some(input) = record.input.get() {
                            runtime
                                .block_on(input.storage.close_and_drain_io())
                                .map_err(|error| Error::Storage(error.to_string()))?;
                        }
                        Ok(())
                    });
                    if let Err(error) = result {
                        record
                            .state
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .error = Some(error.to_string());
                        loop {
                            std::thread::park();
                        }
                    }
                });
            if spawned.is_err() {
                // Bounded failed thread admission must retain uncertain claims.
                #[allow(clippy::mem_forget)]
                std::mem::forget(retained);
            }
        }
    }
}
impl std::fmt::Debug for RejectedStartup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RejectedStartup")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct RetirementFailure {
    pub handle: InstanceHandle,
    pub error: Error,
}
#[derive(Debug, Default)]
pub struct RetirementReport {
    pub completed: Vec<InstanceHandle>,
    pub failures: Vec<RetirementFailure>,
}

#[derive(Debug)]
pub enum PreparationError {
    Rejected(RejectedStartup),
    Retiring {
        handle: InstanceHandle,
        error: Error,
    },
}
/// Constructed model/provider with an active startup ticket. It is not published
/// or mounted; dropping it preserves a Retiring record for explicit cleanup.
pub struct PreparedInstance {
    startup: StartupReservation,
}
impl PreparedInstance {
    pub fn handle(&self) -> InstanceHandle {
        self.startup.handle()
    }
    #[cfg(all(target_os = "linux", feature = "test-util"))]
    pub fn test_pause_publication(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *self.startup.record.publication_pause.lock().unwrap() = Some(linux::PublicationPause {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }
    #[cfg(feature = "test-util")]
    pub fn test_runtime(&self) -> tokio::runtime::Handle {
        self.startup
            .record
            .owner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .runtime_handle()
            .unwrap()
    }
    #[cfg(feature = "test-util")]
    pub fn test_model(&self) -> Arc<lore_vfs::linux_working::Model> {
        self.startup
            .record
            .owner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .model
            .clone()
    }
}
struct CancelPreparation {
    supervisor: Arc<Supervisor>,
    handle: InstanceHandle,
    armed: bool,
}
impl Drop for CancelPreparation {
    fn drop(&mut self) {
        if self.armed {
            let _cancelled = self.supervisor.cancel_startup(&self.handle);
        }
    }
}
impl Supervisor {
    /// Build a real namespace, recovery owner and provider on an owned setup
    /// thread. The supplied gate is explicit; this selects no default edit policy.
    pub async fn prepare_instance(
        self: &Arc<Self>,
        input: StartupInput,
        gate: Arc<dyn lore_vfs::overlay::AccessGate>,
        limits: OwnerLimits,
    ) -> std::result::Result<PreparedInstance, PreparationError> {
        self.prepare_instance_with_options(input, gate, limits, Options::default())
            .await
    }
    /// Configure an optional private verified hash index for both immutable
    /// backend lookups and post-success provider cache publication.
    pub async fn prepare_instance_with_options(
        self: &Arc<Self>,
        input: StartupInput,
        gate: Arc<dyn lore_vfs::overlay::AccessGate>,
        limits: OwnerLimits,
        options: Options,
    ) -> std::result::Result<PreparedInstance, PreparationError> {
        self.prepare_instance_with_gate_factory(
            input,
            Arc::new(lore_vfs::overlay::InjectedGateFactory::new(gate)),
            limits,
            options,
        )
        .await
    }
    /// Binds an explicit policy to the final fresh or recovered backend before
    /// any namespace provider can be published. Startup ownership is unchanged.
    #[lore_macro::test_pub]
    pub(crate) async fn prepare_instance_with_gate_factory(
        self: &Arc<Self>,
        input: StartupInput,
        gate_factory: Arc<dyn lore_vfs::overlay::AccessGateFactory>,
        limits: OwnerLimits,
        options: Options,
    ) -> std::result::Result<PreparedInstance, PreparationError> {
        let ticket = self
            .reserve_startup(input)
            .map_err(PreparationError::Rejected)?;
        let handle = ticket.handle();
        let failure_handle = handle.clone();
        let mut cancel = CancelPreparation {
            supervisor: self.clone(),
            handle: handle.clone(),
            armed: true,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let worker = Arc::new(worker::ThreadOwner::new());
        *ticket.record.preparation_thread.lock().map_err(|_poison| {
            PreparationError::Retiring {
                handle: failure_handle.clone(),
                error: Error::Poisoned,
            }
        })? = Some(worker.clone());
        let spawned = std::thread::Builder::new()
            .name("lore-working-prepare".into())
            .spawn(move || {
                let built = owner::build(&ticket, gate_factory, limits, options);
                let result = match built {
                    Ok(owner) => {
                        *ticket
                            .record
                            .owner
                            .lock()
                            .unwrap_or_else(|error| error.into_inner()) = Some(owner);
                        match ticket.check_live() {
                            Ok(()) => Ok(PreparedInstance { startup: ticket }),
                            Err(error) => {
                                drop(ticket);
                                Err(PreparationError::Retiring { handle, error })
                            }
                        }
                    }
                    Err(error) => {
                        let _retained = retire(&ticket.record, &error.to_string());
                        drop(ticket);
                        Err(PreparationError::Retiring { handle, error })
                    }
                };
                let _received = sender.send(result);
            });
        match spawned {
            Ok(thread) => worker.publish(Some(thread)),
            Err(error) => {
                worker.publish(None);
                return Err(PreparationError::Retiring {
                    handle: failure_handle,
                    error: Error::Owner(error.to_string()),
                });
            }
        }
        let result = receiver.await.unwrap_or_else(|_closed| {
            Err(PreparationError::Retiring {
                handle: failure_handle.clone(),
                error: Error::Owner("owner setup thread terminated".into()),
            })
        });
        worker
            .drain()
            .await
            .map_err(|error| PreparationError::Retiring {
                handle: failure_handle,
                error,
            })?;
        cancel.armed = false;
        result
    }
}

#[cfg(all(target_os = "linux", feature = "test-util"))]
pub fn test_mount_table_decision(bytes: &[u8], target: &Path, owned: u64) -> Result<bool> {
    linux::test_decision(bytes, target, owned)
}
