// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit bounded full checkpoints for the experimental overlay namespace.
//!
//! Only a successful `publish` acknowledges durability. Ordinary overlay writes
//! remain volatile. Checkpoints contain their dirty data, then metadata, and are
//! synced before atomic publication and directory sync. This is blocking I/O;
//! callers must use a blocking thread. The experimental Linux working adapter
//! publishes these checkpoints for file and directory fsync.
//!
//! The directory must be private and exclusively managed by this owner. Advisory
//! locking coordinates cooperating processes, not hostile same-user writers.
//! Failed publications retain `pending` for explicit inspection; recovery never
//! truncates corrupt committed state or deletes unacknowledged data. A failure
//! after rename has an uncertain outcome and requires reopening the owner.

use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use crate::namespace::NamespaceSnapshot;

pub(crate) mod budgeted;
mod codec;
pub(crate) mod inspection;
pub(crate) mod intent;
#[cfg(unix)]
pub(crate) mod intent_writer;
#[cfg(unix)]
mod startup;
#[cfg(unix)]
pub(crate) mod transition_completion;
#[cfg(unix)]
pub use startup::RecoveryReconciliation;
#[cfg(unix)]
pub use startup::StartupRecovery;
mod loaded;
mod prepared_encoding;
pub(crate) mod published;
mod transition_admission;
pub(crate) use loaded::LoadedCheckpoint;
pub(crate) use prepared_encoding::encode_candidate;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Includes headers, identities, namespace metadata and all dirty extent data.
    pub max_checkpoint_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_checkpoint_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Busy,
    PrivateStorageRequired,
    Corrupt,
    Incompatible,
    ResourceLimit,
    Pending,
    /// Transition artifacts require explicit reconciliation before ordinary setup or publication.
    TransitionPending,
    /// An older or unrelated capture cannot replace acknowledged edits.
    StaleSnapshot,
    /// Publication may have reached disk. Do not treat this as rollback.
    Uncertain,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Retains a process-scoped exclusive advisory lock until dropped. Lock-file
/// presence does not indicate a live owner; the OS releases ownership on exit.
pub struct Recovery {
    directory: PathBuf,
    _lock: File,
    directory_file: File,
    limits: Limits,
    uncertain: bool,
    publication_pending: bool,
    #[cfg(test)]
    fault: Option<Boundary>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Boundary {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum OpenMode {
    OpenOrCreate,
    ExistingOnly,
}
#[cfg(unix)]
mod owner_validation;

impl Recovery {
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn byte_limit(&self) -> usize {
        self.limits.max_checkpoint_bytes
    }
    pub(crate) fn capture_byte_limit(&self) -> usize {
        self.limits.max_checkpoint_bytes
    }

    /// Creates a new private directory or opens an existing private directory.
    /// Existing symlinks, broad permissions and foreign owners are rejected.
    /// A transition container blocks ordinary admission without inspecting,
    /// repairing or removing any of its facts.
    #[cfg(unix)]
    pub fn open(directory: impl Into<PathBuf>, limits: Limits) -> Result<Self> {
        Self::open_mode(directory.into(), limits, OpenMode::OpenOrCreate)
    }

    /// Acquires ordinary ownership of an existing private recovery directory.
    /// Missing directories/locks are errors; no file is created, synced or repaired.
    /// Transition presence remains forbidden before and after lock acquisition.
    #[cfg(unix)]
    pub fn open_existing(directory: impl Into<PathBuf>, limits: Limits) -> Result<Self> {
        Self::open_mode(directory.into(), limits, OpenMode::ExistingOnly)
    }

    #[cfg(unix)]
    fn open_mode(directory: PathBuf, limits: Limits, mode: OpenMode) -> Result<Self> {
        use std::os::unix::fs::DirBuilderExt;
        if limits.max_checkpoint_bytes < codec::MIN_SIZE
            || limits.max_checkpoint_bytes == usize::MAX
        {
            return Err(Error::ResourceLimit);
        }
        // Existing transition facts must be excluded before creating a lock
        // file or syncing directories. Recheck after ownership is acquired too.
        match fs::symlink_metadata(&directory) {
            Ok(_) => {
                private(&directory, true)?;
                transition_admission::check(&directory)?;
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && mode == OpenMode::OpenOrCreate => {}
            Err(error) => return Err(error.into()),
        }
        if mode == OpenMode::OpenOrCreate {
            match fs::DirBuilder::new().mode(0o700).create(&directory) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            };
        }
        private(&directory, true)?;
        let directory_file = if mode == OpenMode::ExistingOnly {
            use std::os::unix::fs::OpenOptionsExt;
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&directory)?;
            private_metadata(&file.metadata()?, true)?;
            file
        } else {
            File::open(&directory)?
        };
        if mode == OpenMode::OpenOrCreate {
            // Also covers a directory retained by an earlier failed creation.
            let parent = directory
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)?.sync_all()?;
        }
        let lock_path = directory.join("owner.lock");
        let lock = if mode == OpenMode::ExistingOnly {
            use std::os::unix::fs::OpenOptionsExt;
            private(&lock_path, false)?;
            let file = private_options()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&lock_path)?;
            private_metadata(&file.metadata()?, false)?;
            file
        } else {
            match private_options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(file) => {
                    directory_file.sync_all()?;
                    file
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    private(&lock_path, false)?;
                    private_options().read(true).write(true).open(&lock_path)?
                }
                Err(error) => return Err(error.into()),
            }
        };
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Busy),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        transition_admission::check(&directory)?;
        let recovery = Self {
            directory,
            _lock: lock,
            directory_file,
            limits,
            uncertain: false,
            publication_pending: false,
            #[cfg(test)]
            fault: None,
        };
        if mode == OpenMode::ExistingOnly {
            recovery.verify_owner()?;
        }
        Ok(recovery)
    }

    /// Unsupported platforms fail closed rather than run without ownership.
    #[cfg(not(unix))]
    pub fn open(_directory: impl Into<PathBuf>, _limits: Limits) -> Result<Self> {
        Err(Error::PrivateStorageRequired)
    }

    /// The validated original pinned root, for authorized backend setup. This
    /// metadata grants no access; restoration must recheck the full identity.
    pub fn original_root(&self) -> Result<Option<crate::core::Node>> {
        if self.uncertain {
            return Err(Error::Uncertain);
        }
        transition_admission::check(&self.directory)?;
        if self.publication_pending || self.has_pending()? {
            return Err(Error::Pending);
        }
        self.load()?
            .map(|snapshot| Ok(snapshot.original_root))
            .transpose()
    }
    pub(crate) fn requires_reopen(&self) -> bool {
        self.uncertain || self.publication_pending
    }
    pub fn has_pending(&self) -> Result<bool> {
        match fs::symlink_metadata(self.directory.join("pending")) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Loads only the committed checkpoint. A leftover `pending` is retained and
    /// reported by `has_pending`; it is never replayed as acknowledged work.
    /// These read-only facts do not authorize ordinary setup or publication
    /// while a transition container is present.
    pub fn load(&self) -> Result<Option<NamespaceSnapshot>> {
        Ok(self.load_exact()?.map(LoadedCheckpoint::into_snapshot))
    }

    /// Retain original checked bytes with their snapshot. This does not claim
    /// ownership, authorize restoration or account for combined decoded memory.
    pub(crate) fn load_exact(&self) -> Result<Option<LoadedCheckpoint>> {
        self.committed_file()?
            .map(|file| LoadedCheckpoint::read(file, self.limits.max_checkpoint_bytes))
            .transpose()
    }
    /// Charge exact encoded/decoded capacities and validator scratch to the
    /// caller's existing aggregate ledger. This grants no currentness or publish
    /// authority, and never reencodes the previous checkpoint.
    #[allow(
        dead_code,
        reason = "Budgeted source acknowledgment is not integrated."
    )]
    pub(crate) fn load_exact_budgeted(
        &self,
        memory: &mut crate::capture_memory::CaptureMemory,
    ) -> Result<Option<LoadedCheckpoint>> {
        self.committed_file()?
            .map(|file| {
                LoadedCheckpoint::read_budgeted(file, self.limits.max_checkpoint_bytes, memory)
            })
            .transpose()
    }
    fn committed_file(&self) -> Result<Option<File>> {
        let path = self.directory.join("checkpoint");
        match fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        private(&path, false)?;
        File::open(path).map(Some).map_err(Into::into)
    }

    /// Publishes one coherently captured namespace and file checkpoint. All data
    /// is embedded in the synced file, so its durability never depends on the
    /// volatile source stores. Any error after rename requires reopen.
    pub fn publish(&mut self, snapshot: &NamespaceSnapshot) -> Result<()> {
        if self.uncertain {
            return Err(Error::Uncertain);
        }
        transition_admission::check(&self.directory)?;
        // Refuse to replace corrupt committed edits, including after reopening.
        let previous = self.load()?;
        validate(snapshot, self.limits.max_checkpoint_bytes)?;
        if let Some(previous) = previous {
            successor(&previous, snapshot)?;
        }
        let bytes = codec::encode(snapshot, self.limits.max_checkpoint_bytes)?;
        self.write_checkpoint_bytes(&bytes)
    }
    // Only the validated ordinary and retained publication paths call this
    // engine. No prepared image or raw-byte publication capability is exposed.
    fn write_checkpoint_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let pending = self.directory.join("pending");
        let mut file = match private_options()
            .write(true)
            .create_new(true)
            .open(&pending)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(Error::Pending);
            }
            Err(error) => return Err(error.into()),
        };
        self.publication_pending = true;
        #[cfg(test)]
        if self.fault == Some(Boundary::Write) {
            file.write_all(&bytes[..bytes.len() / 2])?;
            return Err(std::io::Error::other("injected checkpoint write failure").into());
        }
        file.write_all(bytes)?;
        #[cfg(test)]
        self.inject(Boundary::FileSync)?;
        file.sync_all()?;
        drop(file);
        #[cfg(test)]
        self.inject(Boundary::Rename)?;
        fs::rename(&pending, self.directory.join("checkpoint"))?;
        self.publication_pending = false;
        self.uncertain = true;
        #[cfg(test)]
        self.inject(Boundary::DirectorySync)
            .map_err(|_sync_error| Error::Uncertain)?;
        self.directory_file
            .sync_all()
            .map_err(|_sync_error| Error::Uncertain)?;
        self.uncertain = false;
        Ok(())
    }

    #[cfg(all(test, unix))]
    pub(crate) fn test_fault(&mut self, boundary: Option<Boundary>) {
        self.fault = boundary;
    }
    #[cfg(test)]
    fn inject(&self, boundary: Boundary) -> Result<()> {
        if self.fault == Some(boundary) {
            Err(std::io::Error::other("injected checkpoint boundary failure").into())
        } else {
            Ok(())
        }
    }
}

fn private_options() -> OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = OpenOptions::new();
        options.mode(0o600);
        options
    }
    #[cfg(not(unix))]
    OpenOptions::new()
}

#[cfg(unix)]
fn private(path: &Path, directory: bool) -> Result<()> {
    private_metadata(&fs::symlink_metadata(path)?, directory)
}
#[cfg(unix)]
fn private_metadata(metadata: &std::fs::Metadata, directory: bool) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    // SAFETY: getuid has no arguments or memory-safety preconditions.
    let uid = unsafe { libc::getuid() };
    if metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || if directory {
            !metadata.is_dir()
        } else {
            !metadata.is_file() || metadata.nlink() != 1
        }
    {
        return Err(Error::PrivateStorageRequired);
    }
    Ok(())
}
#[cfg(not(unix))]
fn private(_path: &Path, _directory: bool) -> Result<()> {
    Err(Error::PrivateStorageRequired)
}

#[cfg(all(test, unix))]
mod tests;

fn validate(snapshot: &NamespaceSnapshot, byte_limit: usize) -> Result<()> {
    snapshot
        .validate(
            crate::namespace::Limits {
                max_inodes: byte_limit,
                max_directory_entries: byte_limit,
            },
            crate::overlay::Limits {
                max_file_size: u64::MAX,
                max_storage_bytes: u64::MAX,
                max_extents: byte_limit,
                max_handles: 0,
            },
        )
        .map_err(|_decode_error| Error::Corrupt)
}
fn effective_receipts(
    snapshot: &NamespaceSnapshot,
) -> std::collections::BTreeMap<u64, &crate::overlay::Receipt> {
    let mut receipts = std::collections::BTreeMap::new();
    for node in &snapshot.nodes {
        if let Some(receipt) = &node.receipt {
            receipts.insert(node.entry.inode, receipt);
        }
    }
    for (&inode, file) in &snapshot.files {
        if let Some(receipt) = &file.receipt {
            receipts.insert(inode, receipt);
        }
    }
    receipts
}
fn receipt_successor(
    old: Option<&crate::overlay::Receipt>,
    new: Option<&crate::overlay::Receipt>,
) -> bool {
    match (old, new) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(old), Some(new)) => {
            let mut identity = old.clone();
            identity.sha256 = new.sha256;
            identity == *new && (old.sha256.is_none() || old.sha256 == new.sha256)
        }
    }
}

fn successor(previous: &NamespaceSnapshot, next: &NamespaceSnapshot) -> Result<()> {
    use std::collections::BTreeMap;
    if next.generation < previous.generation
        || next.original_root != previous.original_root
        || next.next_inode < previous.next_inode
        || !next.backend_state.starts_with(&previous.backend_state)
    {
        return Err(Error::StaleSnapshot);
    }
    for (scope, old) in &previous.active_underlays {
        let Some(new) = next.active_underlays.get(scope) else {
            return Err(Error::StaleSnapshot);
        };
        if next.generation == previous.generation && new != old {
            return Err(Error::StaleSnapshot);
        }
    }
    if next.generation == previous.generation && next.active_underlays != previous.active_underlays
    {
        return Err(Error::StaleSnapshot);
    }
    for (scope, old) in &previous.baselines {
        let Some(new) = next.baselines.get(scope) else {
            return Err(Error::StaleSnapshot);
        };
        if new.sequence < old.sequence
            || (new.sequence == old.sequence && new != old)
            || (next.generation == previous.generation && new != old)
        {
            return Err(Error::StaleSnapshot);
        }
    }
    if next.generation == previous.generation && next.baselines.len() != previous.baselines.len() {
        return Err(Error::StaleSnapshot);
    }
    let mut nodes = BTreeMap::new();
    for node in &next.nodes {
        nodes.insert(node.entry.inode, node);
    }
    let previous_receipts = effective_receipts(previous);
    let next_receipts = effective_receipts(next);
    for old in &previous.nodes {
        let Some(new) = nodes.get(&old.entry.inode) else {
            return Err(Error::StaleSnapshot);
        };
        let inode = old.entry.inode;
        let old_generation = previous
            .files
            .get(&inode)
            .map_or(0, |file| file.metadata.generation);
        let new_generation = next
            .files
            .get(&inode)
            .map_or(0, |file| file.metadata.generation);
        if new_generation < old_generation
            || (new_generation == old_generation
                && !receipt_successor(
                    previous_receipts.get(&inode).copied(),
                    next_receipts.get(&inode).copied(),
                ))
        {
            return Err(Error::StaleSnapshot);
        }
        let root_transition = inode == 1
            && next.generation > previous.generation
            && next.active_underlays != previous.active_underlays;
        if old.entry.kind != new.entry.kind
            || (!root_transition
                && (old.entry.authority != new.entry.authority || old.entry.base != new.entry.base))
            || (next.generation == previous.generation
                && (old.entry != new.entry
                    || old
                        .names
                        .as_ref()
                        .is_some_and(|names| Some(names) != new.names.as_ref())
                    || old.attached != new.attached))
        {
            return Err(Error::StaleSnapshot);
        }
    }
    for (id, old) in &previous.files {
        let Some(new) = next.files.get(id) else {
            return Err(Error::StaleSnapshot);
        };
        if old.node != new.node
            || new.metadata.generation < old.metadata.generation
            || (new.metadata.generation == old.metadata.generation
                && (new.metadata != old.metadata
                    || new.base_end != old.base_end
                    || new.extents != old.extents))
        {
            return Err(Error::StaleSnapshot);
        }
    }
    Ok(())
}
