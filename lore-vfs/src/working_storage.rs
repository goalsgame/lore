// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private backing files for one experimental working instance. Identity and
//! directory ownership are checked before reuse. Each session uses fresh files;
//! stale sessions and failed allocations are retained and count toward bounds.
//! Only the recovery owner acknowledges durability. This module never deletes
//! recovery, session or export data, and does not implement service shutdown.

use std::fs::File;
#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::fs::{self};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore_base::types::RepositoryId;
use lore_revision::instance::InstanceId;
#[cfg(unix)]
use ring::digest::SHA256;
#[cfg(unix)]
use ring::digest::digest;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::core::BackendFuture;
use crate::core::{self};
use crate::linux_working::StoreFactory;
use crate::overlay::SparseStore;

#[cfg(unix)]
const MAGIC: &[u8; 8] = b"LOREST01";
#[cfg(unix)]
const IDENTITY_SIZE: usize = 8 + 16 + 16 + 32;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_retained_sessions: usize,
    pub max_store_files_per_session: u64,
    /// Queued/running blocking filesystem jobs, including cancelled callers.
    pub max_io_jobs: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_retained_sessions: 8,
            max_store_files_per_session: 65_536,
            max_io_jobs: 64,
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Busy,
    Unsupported,
    InvalidLimits,
    InvalidIdentity,
    IdentityMismatch,
    Incompatible,
    Corrupt,
    Incomplete,
    UnsafePath,
    Overlap,
    UnknownEntry,
    SessionLimit,
    StoreLimit,
    IdentifierExhausted,
    Poisoned,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "working storage: {error}"),
            Self::Busy => formatter.write_str("working storage has a live owner"),
            Self::Unsupported => {
                formatter.write_str("private working storage requires Unix ownership")
            }
            Self::InvalidLimits => {
                formatter.write_str("working storage bounds must be nonzero and finite")
            }
            Self::InvalidIdentity => {
                formatter.write_str("working storage requires nonzero repository and instance IDs")
            }
            Self::IdentityMismatch => {
                formatter.write_str("working storage belongs to another repository or instance")
            }
            Self::Incompatible => {
                formatter.write_str("unsupported working storage identity format")
            }
            Self::Corrupt => formatter.write_str("working storage identity is corrupt"),
            Self::Incomplete => formatter.write_str(
                "working storage initialization is incomplete; retained files require inspection",
            ),
            Self::UnsafePath => formatter
                .write_str("working storage requires private owned regular directories and files"),
            Self::Overlap => {
                formatter.write_str("working storage and mountpoint must not contain one another")
            }
            Self::UnknownEntry => {
                formatter.write_str("working storage contains an unrecognized entry")
            }
            Self::SessionLimit => formatter
                .write_str("retained working session limit reached; no sessions were removed"),
            Self::StoreLimit => formatter.write_str("working session store-file limit reached"),
            Self::IdentifierExhausted => {
                formatter.write_str("working storage identity counter exhausted")
            }
            Self::Poisoned => formatter.write_str("working storage state poisoned"),
        }
    }
}
impl std::error::Error for Error {}

struct SessionCounter {
    retained: usize,
    next: u64,
}
#[derive(Default)]
struct IoState {
    closed: bool,
    active: usize,
}
#[derive(Default)]
struct IoJobs {
    state: Mutex<IoState>,
    changed: Notify,
}
struct IoJob(Arc<IoJobs>);
impl Drop for IoJob {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.active -= 1;
        self.0.changed.notify_waiters();
    }
}
/// Exclusive storage lease. Sessions, factories and returned stores retain this
/// lease. Callers must separately retain their session for provider/export work.
pub struct WorkingStorage {
    root: PathBuf,
    recovery: PathBuf,
    sessions: PathBuf,
    repository: RepositoryId,
    instance: InstanceId,
    limits: Limits,
    io: Arc<Semaphore>,
    jobs: Arc<IoJobs>,
    _lock: File,
    #[cfg(unix)]
    directory_file: File,
    counter: Mutex<SessionCounter>,
}
#[cfg(unix)]
enum OpenMode {
    OpenOrCreate,
    CreateNew,
    ExistingOnly,
}
impl WorkingStorage {
    /// Blocking setup. A new root's existing parent must be a directory. Existing
    /// roots must have this module's exact identity and layout; they are never
    /// adopted, repaired or cleaned automatically.
    #[cfg(unix)]
    pub fn open(
        root: &Path,
        mountpoint: &Path,
        repository: RepositoryId,
        instance: InstanceId,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        Self::open_mode(
            root,
            mountpoint,
            repository,
            instance,
            limits,
            OpenMode::OpenOrCreate,
        )
    }
    /// Creates a new backing root. Existing directories, including empty ones,
    /// are refused without creating ownership or identity files in them.
    #[cfg(unix)]
    pub fn create_new(
        root: &Path,
        mountpoint: &Path,
        repository: RepositoryId,
        instance: InstanceId,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        Self::open_mode(
            root,
            mountpoint,
            repository,
            instance,
            limits,
            OpenMode::CreateNew,
        )
    }
    /// Opens an existing exact-identity backing root. Missing roots and parents
    /// are errors; this path never creates a replacement root or owner lock.
    #[cfg(unix)]
    pub fn open_existing(
        root: &Path,
        mountpoint: &Path,
        repository: RepositoryId,
        instance: InstanceId,
        limits: Limits,
    ) -> Result<Arc<Self>> {
        Self::open_mode(
            root,
            mountpoint,
            repository,
            instance,
            limits,
            OpenMode::ExistingOnly,
        )
    }
    #[cfg(unix)]
    fn open_mode(
        root: &Path,
        mountpoint: &Path,
        repository: RepositoryId,
        instance: InstanceId,
        limits: Limits,
        mode: OpenMode,
    ) -> Result<Arc<Self>> {
        use std::os::unix::fs::DirBuilderExt;
        if limits.max_retained_sessions == 0
            || limits.max_retained_sessions == usize::MAX
            || limits.max_store_files_per_session == 0
            || limits.max_store_files_per_session == u64::MAX
            || limits.max_io_jobs == 0
            || limits.max_io_jobs > Semaphore::MAX_PERMITS
        {
            return Err(Error::InvalidLimits);
        }
        if repository.is_zero() || instance.is_zero() {
            return Err(Error::InvalidIdentity);
        }
        if !root.is_absolute() || !mountpoint.is_absolute() {
            return Err(Error::UnsafePath);
        }
        let parent = root.parent().ok_or(Error::UnsafePath)?.canonicalize()?;
        let name = root
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or(Error::UnsafePath)?;
        let root = parent.join(name);
        let mountpoint = mountpoint.canonicalize()?;
        if !mountpoint.is_dir() {
            return Err(Error::UnsafePath);
        }
        if root.starts_with(&mountpoint) || mountpoint.starts_with(&root) {
            return Err(Error::Overlap);
        }
        let created = match mode {
            OpenMode::ExistingOnly => false,
            OpenMode::CreateNew => {
                fs::DirBuilder::new().mode(0o700).create(&root)?;
                true
            }
            OpenMode::OpenOrCreate => match fs::DirBuilder::new().mode(0o700).create(&root) {
                Ok(()) => true,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
                Err(error) => return Err(error.into()),
            },
        };
        private(&root, true)?;
        let directory_file = owner_validation::open_directory(&root)?;
        let lock = if created {
            private_options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(root.join("owner.lock"))?
        } else {
            match private_options()
                .read(true)
                .write(true)
                .open(root.join("owner.lock"))
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::Incomplete);
                }
                Err(error) => return Err(error.into()),
            }
        };
        private_metadata(&lock.metadata()?, false)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Busy),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        let recovery = root.join("recovery");
        let sessions = root.join("sessions");
        if created {
            let bytes = identity(repository, instance);
            let mut marker = private_options()
                .write(true)
                .create_new(true)
                .open(root.join("identity"))?;
            marker.write_all(&bytes)?;
            marker.sync_all()?;
            fs::DirBuilder::new().mode(0o700).create(&recovery)?;
            fs::DirBuilder::new().mode(0o700).create(&sessions)?;
            File::open(&root)?.sync_all()?;
            File::open(&parent)?.sync_all()?;
        } else {
            verify_identity(&root.join("identity"), repository, instance)?;
        }
        validate_root(&root)?;
        private(&recovery, true)?;
        private(&sessions, true)?;
        let (retained, highest) = inventory(&sessions, limits)?;
        let next = highest.checked_add(1).ok_or(Error::IdentifierExhausted)?;
        Ok(Arc::new(Self {
            root,
            recovery,
            sessions,
            repository,
            instance,
            limits,
            io: Arc::new(Semaphore::new(limits.max_io_jobs)),
            jobs: Arc::default(),
            directory_file,
            _lock: lock,
            counter: Mutex::new(SessionCounter { retained, next }),
        }))
    }
    #[cfg(not(unix))]
    pub fn open(
        _root: &Path,
        _mountpoint: &Path,
        _repository: RepositoryId,
        _instance: InstanceId,
        _limits: Limits,
    ) -> Result<Arc<Self>> {
        Err(Error::Unsupported)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn recovery_directory(&self) -> &Path {
        &self.recovery
    }
    pub fn repository(&self) -> RepositoryId {
        self.repository
    }
    pub fn instance(&self) -> InstanceId {
        self.instance
    }
    pub fn retained_sessions(&self) -> Result<usize> {
        Ok(self
            .counter
            .lock()
            .map_err(|_poisoned| Error::Poisoned)?
            .retained)
    }
    /// Blocking allocation. Charges are reserved before filesystem changes and
    /// never refunded, including failed creation. Recovery/export owners validate
    /// the contents of their respective directories.
    pub fn begin_session(self: &Arc<Self>) -> Result<Arc<Session>> {
        #[cfg(unix)]
        {
            private(&self.root, true)?;
            private(&self.sessions, true)?;
            if self.root.canonicalize()? != self.root
                || self.sessions.canonicalize()? != self.sessions
            {
                return Err(Error::UnsafePath);
            }
            verify_identity(&self.root.join("identity"), self.repository, self.instance)?;
        }
        let id = {
            let mut counter = self.counter.lock().map_err(|_poisoned| Error::Poisoned)?;
            let retained = counter
                .retained
                .checked_add(1)
                .filter(|count| *count <= self.limits.max_retained_sessions)
                .ok_or(Error::SessionLimit)?;
            let next = counter
                .next
                .checked_add(1)
                .ok_or(Error::IdentifierExhausted)?;
            let id = counter.next;
            counter.next = next;
            counter.retained = retained;
            id
        };
        let directory = self.sessions.join(format!("run-{id:016x}"));
        create_directory(&directory)?;
        let stores = directory.join("stores");
        let exports = directory.join("exports");
        create_directory(&stores)?;
        create_directory(&exports)?;
        File::open(&directory)?.sync_all()?;
        File::open(&self.sessions)?.sync_all()?;
        Ok(Arc::new(Session {
            inner: Arc::new(SessionInner {
                storage: self.clone(),
                directory,
                stores,
                exports,
                attempted: AtomicU64::new(0),
            }),
        }))
    }

    /// Permanently rejects new backing IO and wakes callers waiting for capacity.
    /// Close provider/kernel admission and finish checkpoint capture first: this
    /// fence deliberately makes subsequent store reads and writes fail.
    pub fn close_io(&self) -> Result<()> {
        let mut state = self
            .jobs
            .state
            .lock()
            .map_err(|_poisoned| Error::Poisoned)?;
        state.closed = true;
        self.io.close();
        Ok(())
    }

    /// Fences backing IO and waits for actual queued/running OS jobs, including
    /// jobs whose callers were cancelled. This does not flush or drain exports,
    /// checkpoint publishers, or synchronous session-directory creation.
    /// Cancellation of this wait leaves admission closed and may be retried.
    pub async fn close_and_drain_io(&self) -> Result<()> {
        self.close_io()?;
        loop {
            let changed = self.jobs.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .jobs
                .state
                .lock()
                .map_err(|_poisoned| Error::Poisoned)?
                .active
                == 0
            {
                return Ok(());
            }
            changed.await;
        }
    }
}

struct SessionInner {
    storage: Arc<WorkingStorage>,
    directory: PathBuf,
    stores: PathBuf,
    exports: PathBuf,
    attempted: AtomicU64,
}
pub struct Session {
    inner: Arc<SessionInner>,
}
impl Session {
    pub fn directory(&self) -> &Path {
        &self.inner.directory
    }
    pub fn store_directory(&self) -> &Path {
        &self.inner.stores
    }
    pub fn export_directory(&self) -> &Path {
        &self.inner.exports
    }
    pub fn attempted_stores(&self) -> u64 {
        self.inner.attempted.load(Ordering::Acquire)
    }
    pub fn factory(self: &Arc<Self>) -> Arc<dyn StoreFactory> {
        self.clone()
    }
}
struct SessionStore {
    file: Arc<Mutex<File>>,
    session: Arc<SessionInner>,
}
async fn io_permit(session: &Arc<SessionInner>) -> core::Result<OwnedSemaphorePermit> {
    session
        .storage
        .io
        .clone()
        .acquire_owned()
        .await
        .map_err(|_closed| core::Error::Io)
}
// Private backing IO has no Lore request context. Use its owning runtime's
// blocking pool, bounded by the retained permit and independent of caller cancellation.
#[allow(clippy::disallowed_methods)]
async fn execute_io<T, F>(
    session: Arc<SessionInner>,
    permit: OwnedSemaphorePermit,
    operation: F,
) -> core::Result<T>
where
    T: Send + 'static,
    F: FnOnce(&Arc<SessionInner>) -> core::Result<T> + Send + 'static,
{
    let runtime = tokio::runtime::Handle::try_current().map_err(|_no_runtime| core::Error::Io)?;
    let job = {
        let mut state = session
            .storage
            .jobs
            .state
            .lock()
            .map_err(|_poisoned| core::Error::Io)?;
        if state.closed {
            return Err(core::Error::Io);
        }
        state.active += 1;
        IoJob(session.storage.jobs.clone())
    };
    // The actual queued/running OS job owns both the storage lease and its IO
    // permit. Cancelling the awaiting future cannot release either early.
    runtime
        .spawn_blocking(move || {
            let _job = job;
            let _permit = permit;
            operation(&session)
        })
        .await
        .map_err(|_failed_job| core::Error::Io)?
}
impl SparseStore for SessionStore {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        let session = self.session.clone();
        let file = self.file.clone();
        Box::pin(async move {
            if size > core::MAX_READ_SIZE || offset.checked_add(size as u64).is_none() {
                return Err(core::Error::InvalidInput);
            }
            let permit = io_permit(&session).await?;
            execute_io(session, permit, move |_lease| {
                let mut file = file.lock().map_err(|_poisoned| core::Error::Io)?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|_seek| core::Error::Io)?;
                let mut bytes = vec![0; size];
                file.read_exact(&mut bytes)
                    .map_err(|_read| core::Error::Io)?;
                Ok(bytes)
            })
            .await
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        let session = self.session.clone();
        let file = self.file.clone();
        Box::pin(async move {
            if bytes.len() > core::MAX_READ_SIZE || offset.checked_add(bytes.len() as u64).is_none()
            {
                return Err(core::Error::InvalidInput);
            }
            let permit = io_permit(&session).await?;
            // Bound the owned copy even when cancelled calls leave IO queued.
            let bytes = bytes.to_vec();
            execute_io(session, permit, move |_lease| {
                let mut file = file.lock().map_err(|_poisoned| core::Error::Io)?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|_seek| core::Error::Io)?;
                file.write(&bytes).map_err(|_write| core::Error::Io)
            })
            .await
        })
    }
}
#[cfg(unix)]
fn open_store(path: &Path) -> core::Result<File> {
    private_options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_create| core::Error::Io)
}
#[cfg(not(unix))]
fn open_store(_path: &Path) -> core::Result<File> {
    Err(core::Error::Unsupported)
}
impl StoreFactory for Session {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        let session = self.inner.clone();
        Box::pin(async move {
            let used = session
                .attempted
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_add(1)
                        .filter(|next| *next <= session.storage.limits.max_store_files_per_session)
                })
                .map_err(|_exhausted| core::Error::ResourceLimit)?;
            let permit = io_permit(&session).await?;
            execute_io(session, permit, move |session| {
                private(&session.stores, true).map_err(|_unsafe_directory| core::Error::Io)?;
                if session
                    .stores
                    .canonicalize()
                    .map_err(|_unavailable| core::Error::Io)?
                    != session.stores
                {
                    return Err(core::Error::Io);
                }
                let path = session.stores.join(format!("store-{:016x}", used + 1));
                let file = open_store(&path)?;
                Ok(Arc::new(SessionStore {
                    file: Arc::new(Mutex::new(file)),
                    session: session.clone(),
                }) as Arc<dyn SparseStore>)
            })
            .await
        })
    }
}

#[cfg(unix)]
fn identity(repository: RepositoryId, instance: InstanceId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(IDENTITY_SIZE);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(repository.data());
    bytes.extend_from_slice(instance.data());
    let hash = digest(&SHA256, &bytes);
    bytes.extend_from_slice(hash.as_ref());
    bytes
}
#[cfg(unix)]
fn private_options() -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    options
}
#[cfg(unix)]
fn private_metadata(metadata: &fs::Metadata, directory: bool) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    // SAFETY: geteuid reads process identity and has no preconditions.
    let uid = unsafe { libc::geteuid() };
    let expected = if directory { 0o700 } else { 0o600 };
    if metadata.uid() != uid
        || metadata.mode() & 0o777 != expected
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(Error::UnsafePath);
    }
    Ok(())
}
#[cfg(unix)]
fn private(path: &Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(Error::UnsafePath);
    }
    private_metadata(&metadata, directory)
}
#[cfg(not(unix))]
fn private(_path: &Path, _directory: bool) -> Result<()> {
    Err(Error::Unsupported)
}
#[cfg(unix)]
fn create_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().mode(0o700).create(path)?;
    private(path, true)
}
#[cfg(not(unix))]
fn create_directory(_path: &Path) -> Result<()> {
    Err(Error::Unsupported)
}
#[cfg(unix)]
fn verify_identity(path: &Path, repository: RepositoryId, instance: InstanceId) -> Result<()> {
    let file = private_options().read(true).open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::Incomplete
        } else {
            Error::Io(error)
        }
    })?;
    private_metadata(&file.metadata()?, false)?;
    let mut bytes = Vec::new();
    file.take(IDENTITY_SIZE as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() != IDENTITY_SIZE {
        return Err(Error::Corrupt);
    }
    if &bytes[..8] != MAGIC {
        return Err(Error::Incompatible);
    }
    if digest(&SHA256, &bytes[..40]).as_ref() != &bytes[40..] {
        return Err(Error::Corrupt);
    }
    if bytes != identity(repository, instance) {
        return Err(Error::IdentityMismatch);
    }
    Ok(())
}
#[cfg(unix)]
fn validate_root(root: &Path) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for entry in fs::read_dir(root)? {
        let name = entry?.file_name();
        let name = name.to_str().ok_or(Error::UnknownEntry)?;
        if !matches!(name, "owner.lock" | "identity" | "recovery" | "sessions")
            || !seen.insert(name.to_owned())
        {
            return Err(Error::UnknownEntry);
        }
    }
    if seen.len() != 4 {
        return Err(Error::Incomplete);
    }
    Ok(())
}
#[cfg(unix)]
fn numbered(name: &std::ffi::OsStr, prefix: &str) -> Result<u64> {
    let name = name.to_str().ok_or(Error::UnknownEntry)?;
    let digits = name
        .strip_prefix(prefix)
        .filter(|digits| digits.len() == 16)
        .ok_or(Error::UnknownEntry)?;
    let id = u64::from_str_radix(digits, 16).map_err(|_invalid_name| Error::UnknownEntry)?;
    if id == 0 || name != format!("{prefix}{id:016x}") {
        return Err(Error::UnknownEntry);
    }
    Ok(id)
}
#[cfg(unix)]
fn inventory(path: &Path, limits: Limits) -> Result<(usize, u64)> {
    let mut count = 0usize;
    let mut highest = 0;
    for entry in fs::read_dir(path)? {
        count = count
            .checked_add(1)
            .filter(|count| *count <= limits.max_retained_sessions)
            .ok_or(Error::SessionLimit)?;
        let entry = entry?;
        let id = numbered(&entry.file_name(), "run-")?;
        private(&entry.path(), true)?;
        highest = highest.max(id);
        let mut children = 0;
        for child in fs::read_dir(entry.path())? {
            children += 1;
            let child = child?;
            if children > 2 || (child.file_name() != "stores" && child.file_name() != "exports") {
                return Err(Error::UnknownEntry);
            }
            private(&child.path(), true)?;
            if child.file_name() == "stores" {
                let mut files = 0u64;
                for file in fs::read_dir(child.path())? {
                    files = files
                        .checked_add(1)
                        .filter(|count| *count <= limits.max_store_files_per_session)
                        .ok_or(Error::StoreLimit)?;
                    let file = file?;
                    if numbered(&file.file_name(), "store-")? > limits.max_store_files_per_session {
                        return Err(Error::StoreLimit);
                    }
                    private(&file.path(), false)?;
                }
            }
        }
    }
    Ok((count, highest))
}

#[cfg(all(test, unix))]
#[path = "working_storage/tests.rs"]
mod tests;

#[cfg(unix)]
#[path = "working_storage/owner_validation.rs"]
mod owner_validation;
