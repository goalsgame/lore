// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::watch;

use crate::checkpoint_owner::CheckpointOwner;
use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::MAX_READ_SIZE;
use crate::core::Node;
use crate::core::NodeKind;
use crate::core::{self};
use crate::namespace::Entry;
use crate::namespace::Namespace;
use crate::namespace::NamespaceError;
use crate::namespace::pinned_directory::PinnedDirectorySnapshot;
use crate::namespace::{self};
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::Limits as FileLimits;
use crate::overlay::OverlayFile;
use crate::overlay::SparseStore;
use crate::recovery::Recovery;
use crate::recovery::{self};
use crate::storage_budget::BudgetedStore;
use crate::storage_budget::StorageBudget;

#[derive(Debug)]
pub enum Error {
    Core(core::Error),
    Namespace(NamespaceError),
    Recovery(recovery::Error),
    HandleLimit,
    StoreLimit,
    SnapshotLimit,
    FileTooLarge,
    Poisoned,
    ExistingNamespace,
    RestoreRequired,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<core::Error> for Error {
    fn from(error: core::Error) -> Self {
        Self::Core(error)
    }
}
impl From<NamespaceError> for Error {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}
impl From<recovery::Error> for Error {
    fn from(error: recovery::Error) -> Self {
        Self::Recovery(error)
    }
}

impl From<crate::checkpoint_owner::Error> for Error {
    fn from(error: crate::checkpoint_owner::Error) -> Self {
        match error {
            crate::checkpoint_owner::Error::Core(error) => Self::Core(error),
            crate::checkpoint_owner::Error::Namespace(error) => Self::Namespace(error),
            crate::checkpoint_owner::Error::Recovery(error) => Self::Recovery(error),
            crate::checkpoint_owner::Error::Closed => Self::Core(core::Error::Unmounted),
            crate::checkpoint_owner::Error::Poisoned => Self::Poisoned,
            crate::checkpoint_owner::Error::WrongNamespace => Self::Core(core::Error::InvalidInput),
            crate::checkpoint_owner::Error::Spawn(_error) => Self::Core(core::Error::Io),
        }
    }
}

/// Every returned store must be fresh/private and must remain available until
/// the namespace and all retained handles are dropped. Failed/cancelled creation
/// attempts retain their store-count reservation; the owner controls cleanup.
pub trait StoreFactory: Send + Sync {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>>;
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_handles: usize,
    pub max_requests: usize,
    pub max_stores: u64,
    pub max_directory_entries: usize,
    pub max_retained_directory_entries: u64,
    /// Logical retained names+entry metadata; excludes allocator granularity.
    pub max_retained_directory_bytes: u64,
    pub max_checkpoint_data_bytes: usize,
    pub file: FileLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_handles: 4096,
            max_requests: 64,
            max_stores: 65_536,
            max_directory_entries: 4096,
            max_retained_directory_entries: 65_536,
            max_retained_directory_bytes: 32 * 1024 * 1024,
            max_checkpoint_data_bytes: 64 * 1024 * 1024,
            file: FileLimits::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Attributes {
    pub entry: Entry,
    pub size: u64,
    pub mode: u16,
    pub generation: u64,
    pub linked: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenFlags {
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
}
#[derive(Clone, Debug)]
pub struct DirectoryPage {
    pub entries: Vec<(u64, namespace::NamedEntry)>,
    pub eof: bool,
    pub parent: Entry,
}

struct Counters {
    handles: AtomicU64,
    stores: AtomicU64,
    dir_entries: AtomicU64,
    dir_bytes: AtomicU64,
}
fn reserve(counter: &AtomicU64, count: u64, limit: u64) -> std::result::Result<(), ()> {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(count).filter(|next| *next <= limit)
        })
        .map(|_| ())
        .map_err(|_full| ())
}
struct HandleSlot(Arc<Counters>, u64);
impl Drop for HandleSlot {
    fn drop(&mut self) {
        self.0.handles.fetch_sub(1, Ordering::AcqRel);
    }
}
struct DirectoryCharge {
    counters: Arc<Counters>,
    entries: u64,
    bytes: u64,
}
impl DirectoryCharge {
    fn reduce(&mut self, entries: u64, bytes: u64) {
        self.counters
            .dir_entries
            .fetch_sub(self.entries - entries, Ordering::AcqRel);
        self.counters
            .dir_bytes
            .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        self.entries = entries;
        self.bytes = bytes;
    }
}
impl Drop for DirectoryCharge {
    fn drop(&mut self) {
        self.counters
            .dir_entries
            .fetch_sub(self.entries, Ordering::AcqRel);
        self.counters
            .dir_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
type FileCell = Arc<AsyncMutex<Option<Arc<File>>>>;
struct File {
    overlay: Arc<OverlayFile>,
    mutations: AsyncMutex<()>,
}
// Jobs remain owned independently of their spawned task: explicit shutdown can
// finish cleanup even if a runtime dropped an unpolled cleanup future.
struct Cleanup {
    jobs: Mutex<BTreeMap<u64, Arc<CleanupJob>>>,
    runtime: tokio::runtime::Handle,
}
struct CleanupResources {
    file: Arc<File>,
    inner: u64,
    _slot: Arc<HandleSlot>,
}
struct CleanupState {
    resources: Option<CleanupResources>,
    result: Option<core::Result<()>>,
}
struct CleanupJob {
    state: AsyncMutex<CleanupState>,
}
impl CleanupJob {
    async fn run(&self) -> core::Result<()> {
        let mut state = self.state.lock().await;
        if let Some(result) = state.result {
            return result;
        }
        let resources = state.resources.as_ref().ok_or(core::Error::Io)?;
        // Keep resources in the job across this await: cancelling a caller must
        // leave another retained runner able to release the same inner handle.
        let completed = resources.file.overlay.release(resources.inner).await;
        // Completed task clones retain only metadata, never backing storage.
        // No await or registry lock separates resource destruction and result.
        drop(state.resources.take());
        state.result = Some(completed);
        completed
    }
}

impl Cleanup {
    #[allow(clippy::disallowed_methods)] // Retained context-free cleanup, drained explicitly by shutdown.
    fn enqueue(
        self: &Arc<Self>,
        file: Arc<File>,
        inner: u64,
        slot: Arc<HandleSlot>,
    ) -> Arc<CleanupJob> {
        // Slot IDs are reserved before any open and capped at i32::MAX.
        // Each lease enqueues once, including cancelled unpublished opens.
        let id = slot.1;
        let job = Arc::new(CleanupJob {
            state: AsyncMutex::new(CleanupState {
                resources: Some(CleanupResources {
                    file,
                    inner,
                    _slot: slot,
                }),
                result: None,
            }),
        });
        self.jobs
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(id, job.clone());
        let owner = self.clone();
        let task = job.clone();
        self.runtime.spawn(async move {
            let _ = task.run().await;
            owner
                .jobs
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&id);
        });
        job
    }
    async fn drain(&self) -> Result<()> {
        let mut failure = None;
        loop {
            let jobs: Vec<_> = self
                .jobs
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .iter()
                .map(|(id, job)| (*id, job.clone()))
                .collect();
            if jobs.is_empty() {
                return failure.map_or(Ok(()), Err);
            }
            for (id, job) in jobs {
                if let Err(error) = job.run().await {
                    failure = Some(Error::Core(error));
                }
                self.jobs
                    .lock()
                    .map_err(|_poison| Error::Poisoned)?
                    .remove(&id);
            }
        }
    }
}
struct FileLease {
    file: Arc<File>,
    inner: Mutex<Option<u64>>,
    slot: Arc<HandleSlot>,
    cleanup: Arc<Cleanup>,
    flags: OpenFlags,
    inode: u64,
    // Identity for metadata lookup; IO authorization belongs to the inner handle.
    access: crate::namespace::file_access::FileAccess,
}
impl FileLease {
    fn handle(&self) -> Result<u64> {
        self.inner
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .ok_or(core::Error::BadHandle.into())
    }
    async fn release(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .take()
            .ok_or(core::Error::BadHandle)?;
        self.cleanup
            .enqueue(self.file.clone(), inner, self.slot.clone())
            .run()
            .await
            .map_err(Into::into)
    }
}
impl Drop for FileLease {
    fn drop(&mut self) {
        if let Ok(inner) = self.inner.get_mut()
            && let Some(inner) = inner.take()
        {
            self.cleanup
                .enqueue(self.file.clone(), inner, self.slot.clone());
        }
    }
}
struct Directory {
    snapshot: PinnedDirectorySnapshot,
    _charge: DirectoryCharge,
    _slot: Arc<HandleSlot>,
}
enum Handle {
    File(Arc<FileLease>),
    Directory(Arc<Directory>),
}
impl Clone for Handle {
    fn clone(&self) -> Self {
        match self {
            Self::File(file) => Self::File(file.clone()),
            Self::Directory(dir) => Self::Directory(dir.clone()),
        }
    }
}
struct State {
    handles: BTreeMap<u64, Handle>,
    next: u64,
    unlinked: BTreeSet<u64>,
}
pub struct Model {
    namespace: Arc<Namespace>,
    ownership: crate::publication_ownership::OwnerClaim,
    factory: Arc<dyn StoreFactory>,
    budget: Arc<StorageBudget>,
    limits: Limits,
    state: Mutex<State>,
    files: AsyncMutex<BTreeMap<u64, FileCell>>,
    counters: Arc<Counters>,
    requests: Arc<Semaphore>,
    closed: watch::Sender<bool>,
    #[cfg(target_os = "linux")]
    pub(super) mounted: std::sync::atomic::AtomicBool,
    checkpoint: Arc<CheckpointOwner>,
    cleanup: Arc<Cleanup>,
    directory_generation: AtomicU64,
    activity: tokio::sync::Notify,
    names: AsyncMutex<()>,
}

impl Model {
    /// The namespace must have no materialized files and must not subsequently
    /// be materialized or mutated outside this model. Provider freeze/capture
    /// and receipt adoption may use the same namespace. Successful construction
    /// permanently binds this namespace and exact revision backend to one owner;
    /// reuse requires a fresh namespace and backend, or restore. Failed construction releases
    /// its uncommitted reservation.
    pub async fn new(
        namespace: Arc<Namespace>,
        factory: Arc<dyn StoreFactory>,
        budget: Arc<StorageBudget>,
        recovery: Recovery,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        let reservation = namespace.reserve_working_owner()?;
        if recovery.original_root()?.is_some() {
            return Err(Error::RestoreRequired);
        }
        let snapshot = namespace.snapshot(0).await?;
        if !snapshot.files.is_empty() || budget.reserved_bytes() != 0 {
            return Err(Error::ExistingNamespace);
        }
        Self::build(
            namespace,
            factory,
            budget,
            recovery,
            limits,
            0,
            snapshot.generation,
            BTreeSet::new(),
            reservation,
        )
    }
    /// Construct a fresh namespace with policy bound to its exact backend.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_gate_factory(
        backend: Arc<dyn Backend>,
        root: Node,
        gate_factory: Arc<dyn crate::overlay::AccessGateFactory>,
        namespace_limits: namespace::Limits,
        factory: Arc<dyn StoreFactory>,
        budget: Arc<StorageBudget>,
        recovery: Recovery,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        let namespace = Arc::new(Namespace::new_with_gate_factory(
            backend,
            root,
            gate_factory,
            namespace_limits,
        )?);
        Self::new(namespace, factory, budget, recovery, limits).await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn restore(
        backend: Arc<dyn Backend>,
        root: Node,
        gate: Arc<dyn AccessGate>,
        namespace_limits: namespace::Limits,
        factory: Arc<dyn StoreFactory>,
        budget: Arc<StorageBudget>,
        recovery: Recovery,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        Self::restore_with_gate_factory(
            backend,
            root,
            Arc::new(crate::overlay::InjectedGateFactory::new(gate)),
            namespace_limits,
            factory,
            budget,
            recovery,
            limits,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn restore_with_gate_factory(
        backend: Arc<dyn Backend>,
        root: Node,
        gate_factory: Arc<dyn crate::overlay::AccessGateFactory>,
        namespace_limits: namespace::Limits,
        factory: Arc<dyn StoreFactory>,
        budget: Arc<StorageBudget>,
        recovery: Recovery,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        let original = recovery.original_root()?.ok_or(Error::RestoreRequired)?;
        if original != root {
            return Err(core::Error::InvalidInput.into());
        }
        let snapshot = recovery.load()?.ok_or(Error::RestoreRequired)?;
        if budget.reserved_bytes() != 0 {
            return Err(Error::ExistingNamespace);
        }
        let count = u64::try_from(snapshot.files.len()).map_err(|_overflow| Error::StoreLimit)?;
        if count > limits.max_stores {
            return Err(Error::StoreLimit);
        }
        let debt = snapshot
            .files
            .values()
            .try_fold(0u64, |sum, file| {
                sum.checked_add(file.metadata.reserved_storage_bytes)
            })
            .ok_or(Error::StoreLimit)?;
        if debt > budget.limit() {
            return Err(core::Error::ResourceLimit.into());
        }
        let mut stores = BTreeMap::new();
        for inode in snapshot.files.keys() {
            stores.insert(
                *inode,
                Arc::new(BudgetedStore::new(factory.create().await?, budget.clone()))
                    as Arc<dyn SparseStore>,
            );
        }
        let generation = snapshot.generation;
        let unlinked = snapshot
            .nodes
            .iter()
            .filter(|node| !node.attached)
            .map(|node| node.entry.inode)
            .collect();
        let namespace = Arc::new(
            Namespace::restore_with_gate_factory(
                backend,
                root,
                gate_factory,
                snapshot,
                stores,
                namespace_limits,
                limits.file,
            )
            .await?,
        );
        let reservation = namespace.reserve_working_owner()?;
        budget.account_existing(debt)?;
        Self::build(
            namespace,
            factory,
            budget,
            recovery,
            limits,
            count,
            generation,
            unlinked,
            reservation,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn build(
        namespace: Arc<Namespace>,
        factory: Arc<dyn StoreFactory>,
        budget: Arc<StorageBudget>,
        recovery: Recovery,
        limits: Limits,
        stores: u64,
        generation: u64,
        unlinked: BTreeSet<u64>,
        reservation: crate::publication_ownership::Reservation,
    ) -> Result<Arc<Self>> {
        if limits.max_handles == 0
            || limits.max_requests == 0
            || limits.max_requests > Semaphore::MAX_PERMITS
            || limits.max_handles > i32::MAX as usize
            || limits.max_directory_entries > u32::MAX as usize - 2
        {
            return Err(core::Error::InvalidInput.into());
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_runtime| core::Error::InvalidInput)?;
        let cleanup = Arc::new(Cleanup {
            jobs: Mutex::new(BTreeMap::new()),
            runtime: runtime.clone(),
        });
        // No fallible validation or await follows commitment of the exact owner.
        let ownership = reservation.commit()?;
        let checkpoint = CheckpointOwner::new_owned(
            namespace.clone(),
            factory.clone(),
            recovery,
            limits.max_checkpoint_data_bytes,
            ownership.lineage(),
        );
        Ok(Arc::new(Self {
            namespace,
            ownership,
            factory,
            budget,
            limits,
            state: Mutex::new(State {
                handles: BTreeMap::new(),
                next: 1,
                unlinked,
            }),
            files: AsyncMutex::new(BTreeMap::new()),
            counters: Arc::new(Counters {
                handles: AtomicU64::new(0),
                stores: AtomicU64::new(stores),
                dir_entries: AtomicU64::new(0),
                dir_bytes: AtomicU64::new(0),
            }),
            requests: Arc::new(Semaphore::new(limits.max_requests)),
            closed: watch::channel(false).0,
            #[cfg(target_os = "linux")]
            mounted: std::sync::atomic::AtomicBool::new(false),
            checkpoint,
            cleanup,
            directory_generation: AtomicU64::new(generation),
            activity: tokio::sync::Notify::new(),
            names: AsyncMutex::new(()),
        }))
    }
    pub fn namespace(&self) -> &Arc<Namespace> {
        &self.namespace
    }
    pub fn checkpoint_owner(&self) -> Arc<CheckpointOwner> {
        self.checkpoint.clone()
    }
    pub fn budget(&self) -> &Arc<StorageBudget> {
        &self.budget
    }
    pub(super) fn live(&self) -> Result<()> {
        if *self.closed.borrow() {
            Err(core::Error::Unmounted.into())
        } else {
            let routing = self.namespace.check_working_owner(&self.ownership)?;
            self.checkpoint.check_publication_owner(&self.ownership)?;
            self.checkpoint.available()?;
            self.namespace
                .confirm_working_owner_ticket(&routing)
                .map_err(Into::into)
        }
    }
    async fn request<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.live()?;
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_full| core::Error::Busy)?;
        let _request = Request {
            permit: Some(permit),
            activity: &self.activity,
        };
        let mut closed = self.closed.subscribe();
        self.live()?;
        tokio::select! {biased; _=closed.changed()=>Err(core::Error::Unmounted.into()),result=operation=>{self.live()?;result}}
    }
    fn slot(&self) -> Result<Arc<HandleSlot>> {
        reserve(&self.counters.handles, 1, self.limits.max_handles as u64)
            .map_err(|_full| Error::HandleLimit)?;
        let mut slot = HandleSlot(self.counters.clone(), 0);
        let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        self.live()?;
        if state.next > i32::MAX as u64 {
            return Err(Error::HandleLimit);
        }
        slot.1 = state.next;
        state.next += 1;
        Ok(Arc::new(slot))
    }
    fn insert(&self, handle: Handle) -> Result<u64> {
        let id = match &handle {
            Handle::File(file) => file.slot.1,
            Handle::Directory(directory) => directory._slot.1,
        };
        let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
        self.live()?;
        state.handles.insert(id, handle);
        Ok(id)
    }
    fn file_handle(&self, id: u64) -> Result<Arc<FileLease>> {
        match self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .handles
            .get(&id)
        {
            Some(Handle::File(file)) => Ok(file.clone()),
            _ => Err(core::Error::BadHandle.into()),
        }
    }
    fn directory_handle(&self, id: u64) -> Result<Arc<Directory>> {
        match self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .handles
            .get(&id)
        {
            Some(Handle::Directory(dir)) => Ok(dir.clone()),
            _ => Err(core::Error::BadHandle.into()),
        }
    }
    async fn store(&self) -> Result<Arc<dyn SparseStore>> {
        reserve(&self.counters.stores, 1, self.limits.max_stores)
            .map_err(|_full| Error::StoreLimit)?;
        Ok(Arc::new(BudgetedStore::new(
            self.factory.create().await?,
            self.budget.clone(),
        )))
    }
    async fn file(&self, inode: u64) -> Result<Arc<File>> {
        if self.namespace.getattr(inode).await?.kind != NodeKind::File {
            return Err(core::Error::IsDirectory.into());
        }
        let cell = self.cache_cell(inode).await?;
        let mut file = cell.lock().await;
        if let Some(file) = file.as_ref() {
            return Ok(file.clone());
        }
        let overlay = match self.namespace.materialized_file(inode).await? {
            Some(file) => file,
            None => {
                self.namespace
                    .file(inode, self.store().await?, self.limits.file)
                    .await?
            }
        };
        let created = Arc::new(File {
            overlay,
            mutations: AsyncMutex::new(()),
        });
        *file = Some(created.clone());
        Ok(created)
    }
    async fn attributes(&self, mut entry: Entry) -> Result<Attributes> {
        let (size, mode, generation) = if entry.kind == NodeKind::File {
            let (checked, metadata) = self.namespace.file_metadata(entry.inode).await?;
            entry = checked;
            (metadata.size, metadata.mode | 0o200, metadata.generation)
        } else {
            (0, 0o755, self.directory_generation.load(Ordering::Acquire))
        };
        self.attributes_parts(entry, size, mode, generation)
    }
    fn attributes_parts(
        &self,
        entry: Entry,
        size: u64,
        mode: u16,
        generation: u64,
    ) -> Result<Attributes> {
        if generation > i64::MAX as u64 {
            return Err(core::Error::ResourceLimit.into());
        }
        let linked = !self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .unlinked
            .contains(&entry.inode);
        Ok(Attributes {
            entry,
            size,
            mode,
            generation,
            linked,
        })
    }
    async fn attributes_for_lease(&self, lease: &FileLease) -> Result<Attributes> {
        let entry = self.namespace.entry_for_file_lease(&lease.access).await?;
        let metadata = lease
            .file
            .overlay
            .metadata_for_handle(lease.handle()?)
            .await?;
        self.attributes_parts(
            entry,
            metadata.size,
            metadata.mode | 0o200,
            metadata.generation,
        )
    }
    pub async fn getattr(&self, inode: u64) -> Result<Attributes> {
        self.getattr_with_handle(inode, None).await
    }
    /// A supplied handle selects its retained metadata and policy. Unknown or
    /// mismatched handles fail without falling back to current inode access.
    pub async fn getattr_with_handle(&self, inode: u64, handle: Option<u64>) -> Result<Attributes> {
        self.request(async {
            let Some(handle) = handle else {
                return self.attributes(self.namespace.getattr(inode).await?).await;
            };
            let retained = self
                .state
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .handles
                .get(&handle)
                .cloned()
                .ok_or(core::Error::BadHandle)?;
            match retained {
                Handle::File(file) if file.inode == inode => self.attributes_for_lease(&file).await,
                Handle::Directory(directory)
                    if directory.snapshot.snapshot().entry.inode == inode =>
                {
                    directory.snapshot.authorize_directory().await?;
                    let snapshot = directory.snapshot.snapshot();
                    self.attributes_parts(snapshot.entry.clone(), 0, 0o755, snapshot.generation)
                }
                _ => Err(core::Error::BadHandle.into()),
            }
        })
        .await
    }
    pub async fn lookup(&self, parent: u64, name: &[u8]) -> Result<Attributes> {
        self.request(async {
            let entry = match name {
                b"." => self.namespace.getattr(parent).await?,
                b".." => {
                    let parent = self.namespace.getattr(parent).await?.parent;
                    self.namespace.getattr(parent).await?
                }
                _ => self.namespace.lookup(parent, name).await?,
            };
            self.attributes(entry).await
        })
        .await
    }
    async fn open_inner(&self, inode: u64, flags: OpenFlags) -> Result<u64> {
        if (flags.append || flags.truncate) && !flags.write {
            return Err(core::Error::InvalidInput.into());
        }
        self.open_with_slot(inode, flags, self.slot()?).await
    }
    async fn open_with_slot(
        &self,
        inode: u64,
        flags: OpenFlags,
        slot: Arc<HandleSlot>,
    ) -> Result<u64> {
        let file = self.file(inode).await?;
        let captured = self
            .namespace
            .capture_file_access(inode, &file.overlay)
            .await?;
        let inner = file
            .overlay
            .open_captured(
                &captured,
                if flags.write {
                    Access::Write
                } else {
                    Access::Read
                },
            )
            .await?;
        let lease = Arc::new(FileLease {
            file: file.clone(),
            inner: Mutex::new(Some(inner)),
            slot,
            cleanup: self.cleanup.clone(),
            flags,
            inode,
            access: captured,
        });
        self.namespace.confirm_file_access(&lease.access).await?;
        if flags.truncate {
            let _mutation = file.mutations.lock().await;
            file.overlay.truncate(inner, 0).await?;
        }
        self.insert(Handle::File(lease))
    }
    pub async fn open(&self, inode: u64, flags: OpenFlags) -> Result<u64> {
        self.request(self.open_inner(inode, flags)).await
    }
    // Directory attributes must fit the adapter's signed generation range.
    // Every caller holds `names` through the successful namespace mutation and
    // assignment, so no other name mutation can consume the precomputed value.
    fn next_directory_generation(&self) -> Result<u64> {
        self.directory_generation
            .load(Ordering::Acquire)
            .checked_add(1)
            .filter(|generation| i64::try_from(*generation).is_ok())
            .ok_or(core::Error::ResourceLimit.into())
    }
    pub async fn create(
        &self,
        parent: u64,
        name: &[u8],
        mode: u16,
        flags: OpenFlags,
    ) -> Result<(u64, Attributes)> {
        self.request(async {
            if mode & !0o777 != 0 || ((flags.append || flags.truncate) && !flags.write) {
                return Err(core::Error::InvalidInput.into());
            }
            let _names = self.names.lock().await;
            let generation = self.next_directory_generation()?;
            let slot = self.slot()?;
            let store = self.store().await?;
            let entry = self
                .namespace
                .create_file(parent, name, mode, store, self.limits.file)
                .await?;
            self.directory_generation
                .store(generation, Ordering::Release);
            let attributes = self.attributes(entry.clone()).await?;
            let handle = self.open_with_slot(entry.inode, flags, slot).await?;
            Ok((handle, attributes))
        })
        .await
    }
    pub async fn mkdir(&self, parent: u64, name: &[u8]) -> Result<Attributes> {
        self.request(async {
            let _names = self.names.lock().await;
            let generation = self.next_directory_generation()?;
            let entry = self.namespace.create_directory(parent, name).await?;
            self.directory_generation
                .store(generation, Ordering::Release);
            self.attributes(entry).await
        })
        .await
    }
    pub async fn read(&self, handle: u64, offset: u64, size: usize) -> Result<Vec<u8>> {
        self.request(async {
            if size > MAX_READ_SIZE {
                return Err(core::Error::Range.into());
            }
            let file = self.file_handle(handle)?;
            Ok(file.file.overlay.read(file.handle()?, offset, size).await?)
        })
        .await
    }
    pub async fn write(&self, handle: u64, offset: u64, bytes: &[u8]) -> Result<usize> {
        self.write_with_append(handle, offset, bytes, None).await
    }
    pub(super) async fn write_with_append(
        &self,
        handle: u64,
        offset: u64,
        bytes: &[u8],
        append: Option<bool>,
    ) -> Result<usize> {
        self.request(async {
            if bytes.len() > MAX_READ_SIZE {
                return Err(core::Error::Range.into());
            }
            let file = self.file_handle(handle)?;
            if !file.flags.write {
                return Err(core::Error::PermissionDenied.into());
            }
            let _mutation = file.file.mutations.lock().await;
            let offset = if append.unwrap_or(file.flags.append) {
                file.file
                    .overlay
                    .metadata_for_handle(file.handle()?)
                    .await?
                    .size
            } else {
                offset
            };
            if offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|end| end > self.limits.file.max_file_size)
            {
                return Err(Error::FileTooLarge);
            }
            Ok(file
                .file
                .overlay
                .write(file.handle()?, offset, bytes)
                .await?)
        })
        .await
    }
    pub async fn setattr(
        &self,
        inode: u64,
        handle: Option<u64>,
        size: Option<u64>,
        mode: Option<u16>,
    ) -> Result<Attributes> {
        self.request(async {
            let lease = if let Some(handle) = handle {
                let file = self.file_handle(handle)?;
                if file.inode != inode || !file.flags.write {
                    return Err(core::Error::BadHandle.into());
                }
                file
            } else {
                let handle = self
                    .open_inner(
                        inode,
                        OpenFlags {
                            write: true,
                            append: false,
                            truncate: false,
                        },
                    )
                    .await?;
                match self
                    .state
                    .lock()
                    .map_err(|_poison| Error::Poisoned)?
                    .handles
                    .remove(&handle)
                {
                    Some(Handle::File(file)) => file,
                    _ => return Err(core::Error::BadHandle.into()),
                }
            };
            let _mutation = lease.file.mutations.lock().await;
            if size.is_some_and(|size| size > self.limits.file.max_file_size) {
                return Err(Error::FileTooLarge);
            }
            if let Some(mode) = mode {
                let current = lease
                    .file
                    .overlay
                    .metadata_for_handle(lease.handle()?)
                    .await?
                    .mode
                    | 0o200;
                if mode & !0o111 != current & !0o111 || !matches!(mode & 0o111, 0 | 0o111) {
                    return Err(core::Error::Unsupported.into());
                }
            }
            if let Some(size) = size {
                lease.file.overlay.truncate(lease.handle()?, size).await?;
            }
            if let Some(mode) = mode {
                lease
                    .file
                    .overlay
                    .set_executable(lease.handle()?, mode & 0o111 != 0)
                    .await?;
            }
            self.attributes_for_lease(&lease).await
        })
        .await
    }
    pub async fn remove(&self, parent: u64, name: &[u8], directory: bool) -> Result<()> {
        self.request(async {
            let _names = self.names.lock().await;
            let generation = self.next_directory_generation()?;
            let entry = self.namespace.lookup(parent, name).await?;
            self.namespace.remove(parent, name, directory).await?;
            self.state
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .unlinked
                .insert(entry.inode);
            self.directory_generation
                .store(generation, Ordering::Release);
            Ok(())
        })
        .await
    }
    pub async fn rename(
        &self,
        parent: u64,
        name: &[u8],
        destination: u64,
        newname: &[u8],
    ) -> Result<()> {
        self.request(async {
            let _names = self.names.lock().await;
            let generation = self.next_directory_generation()?;
            let previous = match self.namespace.lookup(destination, newname).await {
                Ok(entry) => Some(entry.inode),
                Err(NamespaceError::Backend(core::Error::NotFound)) => None,
                Err(error) => return Err(error.into()),
            };
            let source = self.namespace.lookup(parent, name).await?.inode;
            self.namespace
                .rename(parent, name, destination, newname)
                .await?;
            if let Some(previous) = previous
                && previous != source
            {
                self.state
                    .lock()
                    .map_err(|_poison| Error::Poisoned)?
                    .unlinked
                    .insert(previous);
            }
            self.directory_generation
                .store(generation, Ordering::Release);
            Ok(())
        })
        .await
    }
    pub async fn digest(&self, inode: u64) -> Result<Option<[u8; 32]>> {
        self.request(async { Ok(self.namespace.digest(inode).await?) })
            .await
    }
    pub async fn release(&self, handle: u64) -> Result<()> {
        let handle = self
            .state
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .handles
            .remove(&handle)
            .ok_or(core::Error::BadHandle)?;
        match handle {
            Handle::File(file) => file.release().await,
            Handle::Directory(_directory) => Ok(()),
        }
    }
    pub async fn flush(&self, handle: u64) -> Result<()> {
        self.request(async {
            self.file_handle(handle)?.handle()?;
            Ok(())
        })
        .await
    }
    pub async fn opendir(&self, inode: u64) -> Result<u64> {
        self.request(async {
            let slot = self.slot()?;
            let entries = self.limits.max_directory_entries as u64 + 2;
            let bytes = entries
                .checked_mul((std::mem::size_of::<namespace::NamedEntry>() + 255) as u64)
                .ok_or(Error::SnapshotLimit)?;
            reserve(
                &self.counters.dir_entries,
                entries,
                self.limits.max_retained_directory_entries,
            )
            .map_err(|_full| Error::SnapshotLimit)?;
            if reserve(
                &self.counters.dir_bytes,
                bytes,
                self.limits.max_retained_directory_bytes,
            )
            .is_err()
            {
                self.counters
                    .dir_entries
                    .fetch_sub(entries, Ordering::AcqRel);
                return Err(Error::SnapshotLimit);
            }
            let mut charge = DirectoryCharge {
                counters: self.counters.clone(),
                entries,
                bytes,
            };
            let snapshot = self
                .namespace
                .pinned_directory_snapshot_bounded(inode, self.limits.max_directory_entries)
                .await?;
            let actual = snapshot
                .snapshot()
                .entries
                .iter()
                .try_fold(0u64, |sum, entry| {
                    sum.checked_add(
                        (std::mem::size_of::<namespace::NamedEntry>() + entry.name.len()) as u64,
                    )
                })
                .ok_or(Error::SnapshotLimit)?;
            charge.reduce(
                snapshot.snapshot().entries.len() as u64 + 2,
                actual + 2 * (std::mem::size_of::<namespace::NamedEntry>() + 2) as u64,
            );
            self.insert(Handle::Directory(Arc::new(Directory {
                snapshot,
                _charge: charge,
                _slot: slot,
            })))
        })
        .await
    }
    pub async fn readdir(&self, handle: u64, cookie: u64, limit: usize) -> Result<DirectoryPage> {
        self.request(async {
            if limit == 0 || limit > 1024 {
                return Err(core::Error::Range.into());
            }
            let directory = self.directory_handle(handle)?;
            directory.snapshot.authorize_directory().await?;
            let snapshot = directory.snapshot.snapshot();
            let offset = if cookie == 0 {
                0
            } else {
                if cookie >> 32 != handle {
                    return Err(core::Error::Stale.into());
                }
                (cookie & u32::MAX as u64) as usize
            };
            let count = snapshot.entries.len() + 2;
            if offset > count {
                return Err(core::Error::Stale.into());
            }
            let mut entries = Vec::with_capacity(limit.min(count - offset));
            for position in offset..count.min(offset + limit) {
                let entry = match position {
                    0 => namespace::NamedEntry {
                        name: b".".to_vec(),
                        entry: snapshot.entry.clone(),
                    },
                    1 => namespace::NamedEntry {
                        name: b"..".to_vec(),
                        entry: snapshot.parent.clone(),
                    },
                    _ => snapshot.entries[position - 2].clone(),
                };
                directory.snapshot.authorize_position(position).await?;
                entries.push(((handle << 32) | (position as u64 + 1), entry));
            }
            let eof = offset + entries.len() >= count;
            Ok(DirectoryPage {
                entries,
                eof,
                parent: snapshot.parent.clone(),
            })
        })
        .await
    }
    /// All mutations preceding the coherent snapshot are durable only after
    /// successful publication. Cancellation cannot interrupt an admitted publish.
    pub async fn fsync(&self, handle: u64, directory: bool) -> Result<()> {
        self.request(async {
            if directory {
                self.directory_handle(handle)?;
            } else {
                self.file_handle(handle)?.handle()?;
            }
            self.checkpoint.publish().await.map_err(Into::into)
        })
        .await
    }
    /// Close kernel request admission without closing provider publication.
    /// A service owner drains its admitted provider operations before terminal
    /// `shutdown`, which closes the shared checkpoint owner.
    pub fn close_requests(&self) {
        self.closed.send_replace(true);
        self.activity.notify_waiters();
    }
    pub fn stop(&self) {
        self.close_requests();
        self.checkpoint.close();
    }
    /// Cancel/drain kernel requests and handle cleanup, preserving the shared
    /// checkpoint owner for admitted provider handoffs.
    pub async fn drain_requests(&self) -> Result<()> {
        self.close_requests();
        loop {
            let changed = self.activity.notified();
            if self.requests.available_permits() == self.limits.max_requests {
                break;
            }
            changed.await;
        }
        let handles = std::mem::take(
            &mut self
                .state
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .handles,
        );
        drop(handles);
        self.cleanup.drain().await
    }
    /// Save the final namespace after request admission closes. The caller must
    /// first close/drain provider operations and exports. This drains kernel
    /// handles and prior publishers, then freezes and durably publishes through
    /// the existing owner. It leaves that owner open until terminal shutdown.
    pub async fn checkpoint_for_retirement(&self) -> Result<()> {
        if !*self.closed.borrow() {
            return Err(core::Error::Busy.into());
        }
        let requests = self.drain_requests().await;
        self.checkpoint
            .drain_async()
            .await
            .map_err(Error::from)
            .and(requests)?;
        // A quarantined, actually joined transition already has a durable
        // checkpoint or a recovery obligation. Publishing this poisoned view
        // would overwrite the source/candidate selection needed at restart.
        #[cfg(unix)]
        if self
            .checkpoint
            .transition_retirement_obligation()?
            .is_some()
        {
            return Ok(());
        }
        let lease = self.namespace.freeze_operation().await?;
        self.checkpoint
            .publish_frozen(lease)
            .await
            .map_err(Into::into)
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.stop();
        let cleanup = self.drain_requests().await;
        self.checkpoint
            .drain_async()
            .await
            .map_err(Into::into)
            .and(cleanup)
    }
}
struct Request<'a> {
    permit: Option<OwnedSemaphorePermit>,
    activity: &'a tokio::sync::Notify,
}
impl Drop for Request<'_> {
    fn drop(&mut self) {
        self.permit.take();
        self.activity.notify_waiters();
    }
}

mod file_cache;
mod materialize;
pub(crate) use materialize::CapturedCheckpointImage;
pub(crate) use materialize::OwnedSourcePreparation;
pub(crate) use materialize::PreparationError as SourcePreparationError;
pub(crate) use materialize::SourceContinuation;

#[cfg(all(test, unix))]
#[path = "tests.rs"]
mod tests;

#[cfg(all(test, unix))]
#[path = "model/publication_tests.rs"]
mod publication_tests;

#[cfg(all(test, unix))]
#[path = "model/handle_route_tests.rs"]
mod handle_route_tests;

#[cfg(all(test, unix))]
#[path = "model/directory_generation_tests.rs"]
mod directory_generation_tests;

#[cfg(all(test, unix))]
#[path = "model/cleanup_tests.rs"]
mod cleanup_tests;

#[cfg(unix)]
#[path = "model/transaction.rs"]
mod transaction;
