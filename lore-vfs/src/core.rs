// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! A bounded, read-only view of one pinned revision, including pinned repository links.
//!
//! A backend owns authorization and immutable read contexts for every exposure. An
//! exposure distinguishes a linked subtree from other appearances of the same node.
//! Inodes remain stable across explicitly controlled read-only view transitions.
//! This initial implementation retains discovered inode metadata until shutdown;
//! at the configured cap it returns `ResourceLimit`. It does not implement kernel
//! lookup-reference reclamation. Handles are bounded separately and are never reused.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use lore_base::types::Address;
use lore_base::types::RepositoryId;

pub const ROOT_INODE: u64 = 1;
pub const MAX_DIRECTORY_ENTRIES: usize = 1024;
/// Hard bound for opaque, credential-free backend checkpoint provenance.
pub const MAX_BACKEND_STATE_SIZE: usize = 8 * 1024 * 1024;
pub const MAX_READ_SIZE: usize = 1024 * 1024;
const MAX_HANDLES: usize = 65_536;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    PermissionDenied,
    NotDirectory,
    IsDirectory,
    InvalidInput,
    BadHandle,
    Stale,
    ReadOnly,
    Unsupported,
    NoAttribute,
    Range,
    Io,
    Unmounted,
    ResourceLimit,
    Busy,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeKey {
    pub repository: RepositoryId,
    pub revision: Address,
    pub node: u32,
    pub exposure: u64,
}

pub const MAX_OWNING_SCOPES: usize = 65_536;

/// An immutable selected subtree, without granting access to its owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopePin {
    pub repository: RepositoryId,
    pub revision: Address,
    pub node: u32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentScope {
    pub scope: NodeKey,
    pub initial_revision: Address,
    pub link: NodeKey,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeInfo {
    pub root: NodeKey,
    /// A pinned staged tree can carry a different logical committed base.
    pub initial_revision: Address,
    pub source_path: Vec<u8>,
    pub parent: Option<ParentScope>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeHead {
    pub revision: Address,
    /// None means the independently checked subtree is absent.
    pub root: Option<u32>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeProjection {
    pub parent: NodeKey,
    pub head: Address,
    pub target: Option<ScopePin>,
}
/// Logical held state for one original owning exposure. Its immutable content
/// origin and its parent's committed projection remain separate identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldBaseline {
    pub scope: NodeKey,
    pub head: ScopeHead,
    pub sequence: u64,
    pub parent_projection: Option<ScopeProjection>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub key: NodeKey,
    pub kind: NodeKind,
    pub size: u64,
    pub mode: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub inode: u64,
    pub parent: u64,
    pub node: Node,
}

pub struct NamedNode {
    pub name: Vec<u8>,
    pub node: Node,
}

pub struct BackendPage {
    pub entries: Vec<NamedNode>,
    /// The next ordinal, exactly `offset + entries.len()`, or `None` at EOF.
    pub next_offset: Option<u64>,
}

pub struct DirectoryEntry {
    pub name: Vec<u8>,
    pub entry: Entry,
    pub cookie: u64,
}

pub struct DirectoryPage {
    pub entries: Vec<DirectoryEntry>,
    pub eof: bool,
}

/// Backends must authorize each owning repository independently and retain its
/// pinned revision and caller credentials. Keys received here originate from the
/// backend; a mount must never accept caller-supplied keys as authority.
///
/// Listing uses stable ordinal offsets in an immutable directory, excludes dots,
/// and returns at most `limit` entries. Lookup/list metadata must agree for a key.
/// Reads use Lore's verified content path and return exactly the requested range
/// unless EOF is reached. Digest queries only inspect trusted cache records: they
/// never hydrate content or start hashing. Dropping a future cancels its result.
pub trait Backend: Send + Sync {
    /// Verify an explicitly retained underlay, returning its independently
    /// derived policy anchor. An absence anchor never grants readable ancestry.
    fn underlay_view<'a>(
        &'a self,
        scope: &'a NodeKey,
        head: &'a ScopeHead,
    ) -> BackendFuture<'a, NodeKey> {
        Box::pin(async move {
            if head.revision != scope.revision || head.root != Some(scope.node) {
                return Err(Error::Unsupported);
            }
            self.revalidate(scope, false).await?;
            Ok(*scope)
        })
    }
    /// Optional concrete capabilities. Absence never enables a fallback reader.
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }
    fn owning_scope<'a>(&'a self, _key: &'a NodeKey) -> BackendFuture<'a, ScopeInfo> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn scope_head<'a>(
        &'a self,
        _scope: &'a NodeKey,
        _revision: Address,
    ) -> BackendFuture<'a, ScopeHead> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn scope_projection<'a>(
        &'a self,
        _scope: &'a NodeKey,
        _parent_revision: Address,
    ) -> BackendFuture<'a, Option<ScopeProjection>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    /// Canonical append-only provenance: existing bytes must remain a prefix of
    /// later checkpoints. Credentials and mutable content must never be included.
    fn checkpoint_state(&self) -> BackendFuture<'_, Vec<u8>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    /// Prepare an independent backend without rebinding this shared instance.
    /// Empty state may reuse the original backend. Unsupported provenance fails.
    fn prepare_restore<'a>(
        &'a self,
        state: &'a [u8],
    ) -> BackendFuture<'a, Option<Arc<dyn Backend>>> {
        Box::pin(async move {
            if state.is_empty() {
                Ok(None)
            } else {
                Err(Error::Unsupported)
            }
        })
    }
    /// Recheck cached overlay authority, including local files anchored to a
    /// directory. The caller's mandatory overlay gate still checks write policy.
    /// Backends without provenance rely on that gate for all authorization.
    fn revalidate<'a>(&'a self, _key: &'a NodeKey, _write: bool) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Optional immutable metadata identity, without reading payload bytes.
    fn content_identity<'a>(
        &'a self,
        _key: &'a NodeKey,
    ) -> BackendFuture<'a, Option<crate::digest::ContentIdentity>> {
        Box::pin(async { Ok(None) })
    }
    /// Verify a committed node through the original authorized exposure route.
    /// Backends that cannot validate persisted proofs must fail closed.
    fn validate_committed<'a>(
        &'a self,
        _anchor: &'a NodeKey,
        _proof: &'a crate::overlay::CommittedProof,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node>;
    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage>;
    fn read<'a>(
        &'a self,
        file: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>>;
    fn digest<'a>(&'a self, file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>>;
    fn readlink<'a>(&'a self, _link: &'a NodeKey) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
}

#[derive(Clone)]
struct Handle {
    entry: Entry,
    generation: u64,
    directory: bool,
    high_water: u64,
}
struct View {
    backend: Arc<dyn Backend>,
    root: Node,
}
struct Inode {
    entry: Entry,
    generation: u64,
}
struct State {
    mounted: bool,
    entries: BTreeMap<u64, Inode>,
    inodes: BTreeMap<(u64, NodeKey), u64>,
    views: BTreeMap<u64, View>,
    current: u64,
    next_generation: u64,
    root_names: BTreeSet<Vec<u8>>,
    transition_poisoned: bool,
    limits: transition::Limits,
    handles: BTreeMap<u64, Handle>,
    next_inode: u64,
    next_handle: u64,
}
impl State {
    fn live(&self) -> Result<()> {
        if self.mounted {
            Ok(())
        } else {
            Err(Error::Unmounted)
        }
    }
    fn entry(&self, inode: u64) -> Result<(Entry, u64)> {
        self.live()?;
        if inode == ROOT_INODE {
            return Ok((
                Entry {
                    inode,
                    parent: inode,
                    node: self.views.get(&self.current).ok_or(Error::Io)?.root.clone(),
                },
                self.current,
            ));
        }
        self.entries
            .get(&inode)
            .map(|record| (record.entry.clone(), record.generation))
            .ok_or(Error::NotFound)
    }
    fn backend(&self, generation: u64) -> Result<Arc<dyn Backend>> {
        self.live()?;
        Ok(self
            .views
            .get(&generation)
            .ok_or(Error::Stale)?
            .backend
            .clone())
    }
    fn intern(&mut self, generation: u64, parent: u64, node: Node) -> Result<Entry> {
        self.live()?;
        if self.views.get(&generation).ok_or(Error::Stale)?.root.key == node.key {
            return Err(Error::Io);
        }
        if let Some(inode) = self.inodes.get(&(generation, node.key)) {
            let entry = &self.entries[inode].entry;
            return if entry.node == node && entry.parent == parent {
                Ok(entry.clone())
            } else {
                Err(Error::Io)
            };
        }
        if self.entries.len().saturating_add(self.views.len()) >= self.limits.max_inodes {
            return Err(Error::ResourceLimit);
        }
        let inode = self.next_inode;
        self.next_inode = inode.checked_add(1).ok_or(Error::ResourceLimit)?;
        let entry = Entry {
            inode,
            parent,
            node,
        };
        self.inodes.insert((generation, entry.node.key), inode);
        self.entries.insert(
            inode,
            Inode {
                entry: entry.clone(),
                generation,
            },
        );
        Ok(entry)
    }
    fn handle(&self, handle: u64, directory: bool) -> Result<&Handle> {
        self.live()?;
        let value = self.handles.get(&handle).ok_or(Error::BadHandle)?;
        if value.directory != directory {
            return Err(Error::BadHandle);
        }
        Ok(value)
    }
}

/// Read-only generations retain every issued inode and open-handle snapshot.
/// Transport callbacks must hold `callback_guard` through sending their reply.
/// Direct asynchronous operations select their immutable view before backend I/O.
pub struct Core {
    state: Mutex<State>,
    callbacks: RwLock<()>,
    transition_lock: Mutex<()>,
    shutdown_signal: tokio::sync::watch::Sender<bool>,
}
impl Core {
    pub fn new(backend: Arc<dyn Backend>, root: Node) -> Result<Self> {
        Self::with_limits(backend, root, transition::Limits::default())
    }
    pub fn with_limits(
        backend: Arc<dyn Backend>,
        root: Node,
        limits: transition::Limits,
    ) -> Result<Self> {
        if root.kind != NodeKind::Directory {
            return Err(Error::NotDirectory);
        }
        if limits.max_generations == 0 || limits.max_inodes == 0 || limits.max_root_names == 0 {
            return Err(Error::InvalidInput);
        }
        Ok(Self {
            state: Mutex::new(State {
                mounted: true,
                entries: BTreeMap::new(),
                inodes: BTreeMap::new(),
                views: BTreeMap::from([(1, View { backend, root })]),
                current: 1,
                next_generation: 2,
                root_names: BTreeSet::new(),
                transition_poisoned: false,
                limits,
                handles: BTreeMap::new(),
                next_inode: 2,
                next_handle: 1,
            }),
            callbacks: RwLock::new(()),
            transition_lock: Mutex::new(()),
            shutdown_signal: tokio::sync::watch::channel(false).0,
        })
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|_poisoned| Error::Io)
    }
    async fn backend_call<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let mut shutdown = self.shutdown_signal.subscribe();
        if *shutdown.borrow() {
            return Err(Error::Unmounted);
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => Err(Error::Unmounted),
            result = future => result,
        }
    }
    /// Linux holds this guard until the kernel reply has been sent. A transition
    /// drains these replies only for publication, never during notifications.
    pub fn callback_guard(&self) -> Result<std::sync::RwLockReadGuard<'_, ()>> {
        self.callbacks.read().map_err(|_poisoned| Error::Io)
    }
    pub fn getattr(&self, inode: u64) -> Result<Entry> {
        self.state()?.entry(inode).map(|(entry, _)| entry)
    }
    /// Recheck the retained inode's authority before granting a new OS open or
    /// cache lease. Previously served bytes in an existing OS cache are outside
    /// this callback's control. No core lock is held across authorization.
    /// Transports supporting transitions must retain `callback_guard` through
    /// revalidation and the open reply so the root cannot change between them.
    pub async fn revalidate(&self, inode: u64) -> Result<()> {
        let (entry, backend) = {
            let state = self.state()?;
            let (entry, generation) = state.entry(inode)?;
            (entry, state.backend(generation)?)
        };
        self.backend_call(backend.revalidate(&entry.node.key, false))
            .await?;
        self.state()?.live()
    }
    pub async fn lookup(&self, parent: u64, name: &[u8]) -> Result<Entry> {
        validate_name(name)?;
        let (entry, generation, backend) = {
            let mut state = self.state()?;
            let (entry, generation) = state.entry(parent)?;
            if entry.node.kind != NodeKind::Directory {
                return Err(Error::NotDirectory);
            }
            if parent == ROOT_INODE && !state.root_names.contains(name) {
                if state.root_names.len() >= state.limits.max_root_names {
                    return Err(Error::ResourceLimit);
                }
                state.root_names.insert(name.to_vec());
            }
            (entry, generation, state.backend(generation)?)
        };
        let node = self
            .backend_call(backend.lookup(&entry.node.key, name))
            .await?;
        self.state()?.intern(generation, parent, node)
    }
    pub fn open(&self, inode: u64) -> Result<u64> {
        self.open_kind(inode, false)
    }
    pub fn opendir(&self, inode: u64) -> Result<u64> {
        self.open_kind(inode, true)
    }
    fn open_kind(&self, inode: u64, directory: bool) -> Result<u64> {
        let mut state = self.state()?;
        let (entry, generation) = state.entry(inode)?;
        match (directory, entry.node.kind) {
            (true, NodeKind::Directory) | (false, NodeKind::File) => {}
            (true, _) => return Err(Error::NotDirectory),
            (false, NodeKind::Directory) => return Err(Error::IsDirectory),
            (false, NodeKind::Symlink) => return Err(Error::InvalidInput),
        }
        if state.handles.len() >= MAX_HANDLES || state.next_handle >= i32::MAX as u64 {
            return Err(Error::ResourceLimit);
        }
        let handle = state.next_handle;
        state.next_handle += 1;
        state.handles.insert(
            handle,
            Handle {
                entry,
                generation,
                directory,
                high_water: 0,
            },
        );
        Ok(handle)
    }
    pub fn release(&self, handle: u64) -> Result<()> {
        let mut state = self.state()?;
        state.live()?;
        state
            .handles
            .remove(&handle)
            .map(|_| ())
            .ok_or(Error::BadHandle)
    }
    pub async fn read(&self, handle: u64, offset: u64, size: usize) -> Result<Vec<u8>> {
        if size > MAX_READ_SIZE {
            return Err(Error::InvalidInput);
        }
        let (entry, backend) = {
            let state = self.state()?;
            let handle = state.handle(handle, false)?;
            (handle.entry.clone(), state.backend(handle.generation)?)
        };
        let size = size.min(
            entry
                .node
                .size
                .saturating_sub(offset)
                .min(usize::MAX as u64) as usize,
        );
        let bytes = if size == 0 {
            Vec::new()
        } else {
            self.backend_call(backend.read(&entry.node.key, offset, size))
                .await?
        };
        self.state()?.handle(handle, false)?;
        if bytes.len() != size {
            return Err(Error::Io);
        }
        Ok(bytes)
    }
    pub async fn readdir(&self, handle: u64, cookie: u64, limit: usize) -> Result<DirectoryPage> {
        if limit == 0 || limit > MAX_DIRECTORY_ENTRIES {
            return Err(Error::InvalidInput);
        }
        let (entry, generation, high_water, backend) = {
            let state = self.state()?;
            let value = state.handle(handle, true)?;
            (
                value.entry.clone(),
                value.generation,
                value.high_water,
                state.backend(value.generation)?,
            )
        };
        let offset = cookie & u32::MAX as u64;
        if cookie != 0 && (cookie >> 32 != handle || offset == 0 || offset > high_water) {
            return Err(Error::Stale);
        }
        let page = self
            .backend_call(backend.list(&entry.node.key, offset, limit))
            .await?;
        let end = offset
            .checked_add(page.entries.len() as u64)
            .ok_or(Error::Io)?;
        if page.entries.len() > limit
            || end > u32::MAX as u64
            || page
                .next_offset
                .is_some_and(|next| next != end || next == offset)
        {
            return Err(Error::Io);
        }
        for child in &page.entries {
            validate_name(&child.name).map_err(|_invalid_name| Error::Io)?;
        }
        let mut state = self.state()?;
        state.handle(handle, true)?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for (index, child) in page.entries.into_iter().enumerate() {
            entries.push(DirectoryEntry {
                name: child.name,
                entry: state.intern(generation, entry.inode, child.node)?,
                cookie: (handle << 32) | (offset + index as u64 + 1),
            });
        }
        let directory = state.handles.get_mut(&handle).ok_or(Error::BadHandle)?;
        directory.high_water = directory.high_water.max(end);
        Ok(DirectoryPage {
            entries,
            eof: page.next_offset.is_none(),
        })
    }
    pub async fn digest(&self, inode: u64) -> Result<Option<[u8; 32]>> {
        let (entry, backend) = {
            let state = self.state()?;
            let (entry, generation) = state.entry(inode)?;
            (entry, state.backend(generation)?)
        };
        if entry.node.kind != NodeKind::File {
            return Ok(None);
        }
        let digest = self.backend_call(backend.digest(&entry.node.key)).await?;
        self.state()?.live()?;
        Ok(digest)
    }
    pub async fn readlink(&self, inode: u64) -> Result<Vec<u8>> {
        let (entry, backend) = {
            let state = self.state()?;
            let (entry, generation) = state.entry(inode)?;
            (entry, state.backend(generation)?)
        };
        if entry.node.kind != NodeKind::Symlink {
            return Err(Error::InvalidInput);
        }
        let target = self.backend_call(backend.readlink(&entry.node.key)).await?;
        self.state()?.live()?;
        if target.contains(&0) || target.len() > MAX_READ_SIZE {
            return Err(Error::Io);
        }
        Ok(target)
    }
    pub fn shutdown(&self) -> Result<()> {
        // Wake blocked storage calls even if a poisoned state prevents cleanup.
        self.shutdown_signal.send_replace(true);
        let mut state = self.state()?;
        state.mounted = false;
        state.handles.clear();
        state.entries.clear();
        state.inodes.clear();
        state.views.clear();
        Ok(())
    }
}
#[path = "transition.rs"]
pub mod transition;

fn validate_name(name: &[u8]) -> Result<()> {
    if name.is_empty()
        || name.len() > 1000
        || name == b"."
        || name == b".."
        || name.contains(&0)
        || name.contains(&b'/')
    {
        Err(Error::InvalidInput)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use tokio::sync::Notify;

    use super::*;

    #[derive(Default)]
    struct Fake {
        reads: AtomicUsize,
        lists: AtomicUsize,
        block_read: Option<(Notify, Notify)>,
        read_error: Option<Error>,
        short_read: bool,
        invalid_page: bool,
        digest_data: Option<[u8; 32]>,
    }

    fn node(number: u32, exposure: u64) -> Node {
        Node {
            key: NodeKey {
                repository: RepositoryId::default(),
                revision: Address::default(),
                node: number,
                exposure,
            },
            kind: if number == 0 {
                NodeKind::Directory
            } else {
                NodeKind::File
            },
            size: if number == 0 { 0 } else { 3 },
            mode: 0o555,
        }
    }

    impl Backend for Fake {
        fn lookup<'a>(&'a self, _: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
            Box::pin(async move {
                match name {
                    b"first" => Ok(node(1, 1)),
                    b"alias" => Ok(node(1, 2)),
                    b"directory" => Ok(node(0, 2)),
                    b"other-repository" => {
                        let mut value = node(1, 1);
                        value.key.repository = [1; 16].into();
                        Ok(value)
                    }
                    b"other-revision" => {
                        let mut value = node(1, 1);
                        value.key.revision.hash = [1; 32].into();
                        Ok(value)
                    }
                    b"denied" => Err(Error::PermissionDenied),
                    b"failure" => Err(Error::Io),
                    _ => Err(Error::NotFound),
                }
            })
        }

        fn list<'a>(
            &'a self,
            _: &'a NodeKey,
            offset: u64,
            limit: usize,
        ) -> BackendFuture<'a, BackendPage> {
            Box::pin(async move {
                self.lists.fetch_add(1, Ordering::Relaxed);
                if self.invalid_page {
                    return Ok(BackendPage {
                        entries: Vec::new(),
                        next_offset: Some(offset),
                    });
                }
                let names: [&[u8]; 2] = [b"first", b"alias"];
                let mut entries = Vec::new();
                for (index, name) in names.iter().enumerate().skip(offset as usize).take(limit) {
                    entries.push(NamedNode {
                        name: name.to_vec(),
                        node: node(1, index as u64 + 1),
                    });
                }
                let end = offset + entries.len() as u64;
                Ok(BackendPage {
                    entries,
                    next_offset: (end < 2).then_some(end),
                })
            })
        }

        fn read<'a>(
            &'a self,
            _: &'a NodeKey,
            offset: u64,
            size: usize,
        ) -> BackendFuture<'a, Vec<u8>> {
            Box::pin(async move {
                self.reads.fetch_add(1, Ordering::Relaxed);
                if let Some(error) = self.read_error {
                    return Err(error);
                }
                if self.short_read {
                    return Ok(Vec::new());
                }
                if let Some((started, resume)) = &self.block_read {
                    started.notify_one();
                    resume.notified().await;
                }
                Ok(b"abc"[offset as usize..offset as usize + size].to_vec())
            })
        }

        fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
            Box::pin(async move { Ok(self.digest_data) })
        }
    }

    fn mount(fake: Arc<Fake>) -> Core {
        Core::new(fake, node(0, 1)).unwrap()
    }

    #[tokio::test]
    async fn identities_include_link_exposure_repository_and_revision() {
        let core = mount(Arc::new(Fake::default()));
        let first = core.lookup(ROOT_INODE, b"first").await.unwrap();
        assert_eq!(first, core.lookup(ROOT_INODE, b"first").await.unwrap());
        for name in [b"alias".as_slice(), b"other-repository", b"other-revision"] {
            let other = core.lookup(ROOT_INODE, name).await.unwrap();
            assert_ne!(first.inode, other.inode);
            assert_eq!(other.parent, ROOT_INODE);
        }
        assert_eq!(
            core.lookup(ROOT_INODE, b"denied").await,
            Err(Error::PermissionDenied)
        );
        assert_eq!(core.lookup(ROOT_INODE, b"failure").await, Err(Error::Io));
        assert_eq!(
            core.lookup(ROOT_INODE, b"missing").await,
            Err(Error::NotFound)
        );
    }

    #[tokio::test]
    async fn backend_must_distinguish_exposures_with_different_parents() {
        let core = mount(Arc::new(Fake::default()));
        let first = core.lookup(ROOT_INODE, b"first").await.unwrap();
        let directory = core.lookup(ROOT_INODE, b"directory").await.unwrap();
        // This fake incorrectly returns the root exposure's key in another directory.
        assert_eq!(core.lookup(directory.inode, b"first").await, Err(Error::Io));
        // A separately exposed alias has its own correct parent relationship.
        let alias = core.lookup(directory.inode, b"alias").await.unwrap();
        assert_ne!(first.inode, alias.inode);
        assert_eq!(alias.parent, directory.inode);
    }

    #[tokio::test]
    async fn reads_are_bounded_and_eof_does_not_call_backend() {
        let fake = Arc::new(Fake::default());
        let core = mount(fake.clone());
        let entry = core.lookup(ROOT_INODE, b"first").await.unwrap();
        let handle = core.open(entry.inode).unwrap();
        assert_eq!(core.read(handle, 1, 100).await.unwrap(), b"bc");
        assert!(core.read(handle, u64::MAX, 10).await.unwrap().is_empty());
        assert!(core.read(handle, 0, 0).await.unwrap().is_empty());
        assert_eq!(fake.reads.load(Ordering::Relaxed), 1);
        assert_eq!(
            core.read(handle, 0, MAX_READ_SIZE + 1).await,
            Err(Error::InvalidInput)
        );
        assert_eq!(core.digest(entry.inode).await.unwrap(), None);
        assert_eq!(fake.reads.load(Ordering::Relaxed), 1);
        core.release(handle).unwrap();
        assert_eq!(core.read(handle, 0, 1).await, Err(Error::BadHandle));
        assert_eq!(core.release(handle), Err(Error::BadHandle));
        assert_ne!(handle, core.open(entry.inode).unwrap());
    }

    #[tokio::test]
    async fn cached_digest_hit_does_not_read_content() {
        let fake = Arc::new(Fake {
            digest_data: Some([0xab; 32]),
            ..Fake::default()
        });
        let core = mount(fake.clone());
        let file = core.lookup(ROOT_INODE, b"first").await.unwrap();
        assert_eq!(core.digest(file.inode).await.unwrap(), Some([0xab; 32]));
        assert_eq!(fake.reads.load(Ordering::Relaxed), 0);
        assert_eq!(core.digest(ROOT_INODE).await.unwrap(), None);
    }

    #[tokio::test]
    async fn directory_pages_have_handle_scoped_cookies() {
        let fake = Arc::new(Fake::default());
        let core = mount(fake.clone());
        let handle = core.opendir(ROOT_INODE).unwrap();
        let first = core.readdir(handle, 0, 1).await.unwrap();
        assert!(!first.eof);
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].name, b"first");
        let cookie = first.entries[0].cookie;
        assert!(matches!(
            core.readdir(handle, cookie + 1, 1).await,
            Err(Error::Stale)
        ));
        assert!(matches!(
            core.readdir(handle, handle << 32, 1).await,
            Err(Error::Stale)
        ));
        let second = core.readdir(handle, cookie, 1).await.unwrap();
        assert!(second.eof);
        assert_eq!(second.entries[0].name, b"alias");
        assert_ne!(first.entries[0].entry.inode, second.entries[0].entry.inode);
        assert!(
            core.readdir(handle, second.entries[0].cookie, 1)
                .await
                .unwrap()
                .entries
                .is_empty()
        );
        let other = core.opendir(ROOT_INODE).unwrap();
        assert!(matches!(
            core.readdir(other, cookie, 1).await,
            Err(Error::Stale)
        ));
        core.release(handle).unwrap();
        assert!(matches!(
            core.readdir(handle, cookie, 1).await,
            Err(Error::BadHandle)
        ));
        assert!(matches!(
            core.readdir(other, 0, MAX_DIRECTORY_ENTRIES + 1).await,
            Err(Error::InvalidInput)
        ));
        assert_eq!(fake.lists.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn backend_failures_and_malformed_results_are_rejected() {
        for (fake, expected) in [
            (
                Fake {
                    read_error: Some(Error::PermissionDenied),
                    ..Fake::default()
                },
                Error::PermissionDenied,
            ),
            (
                Fake {
                    read_error: Some(Error::Io),
                    ..Fake::default()
                },
                Error::Io,
            ),
            (
                Fake {
                    short_read: true,
                    ..Fake::default()
                },
                Error::Io,
            ),
        ] {
            let core = mount(Arc::new(fake));
            let file = core.lookup(ROOT_INODE, b"first").await.unwrap();
            let handle = core.open(file.inode).unwrap();
            assert_eq!(core.read(handle, 0, 3).await, Err(expected));
        }
        let core = mount(Arc::new(Fake {
            invalid_page: true,
            ..Fake::default()
        }));
        let handle = core.opendir(ROOT_INODE).unwrap();
        assert!(matches!(core.readdir(handle, 0, 1).await, Err(Error::Io)));
    }

    #[tokio::test]
    async fn type_and_name_errors_do_not_reach_backend() {
        let core = mount(Arc::new(Fake::default()));
        assert_eq!(core.open(ROOT_INODE), Err(Error::IsDirectory));
        for name in [b"".as_slice(), b".", b"..", b"a/b", b"a\0b"] {
            assert_eq!(
                core.lookup(ROOT_INODE, name).await,
                Err(Error::InvalidInput)
            );
        }
        let file = core.lookup(ROOT_INODE, b"first").await.unwrap();
        assert_eq!(core.opendir(file.inode), Err(Error::NotDirectory));
        assert_eq!(
            core.lookup(file.inode, b"child").await,
            Err(Error::NotDirectory)
        );
        let handle = core.opendir(ROOT_INODE).unwrap();
        assert_eq!(core.read(handle, 0, 1).await, Err(Error::BadHandle));
    }

    #[tokio::test]
    async fn teardown_discards_in_flight_results_and_closes_admission() {
        let fake = Arc::new(Fake {
            block_read: Some((Notify::new(), Notify::new())),
            ..Fake::default()
        });
        let core = Arc::new(mount(fake.clone()));
        let file = core.lookup(ROOT_INODE, b"first").await.unwrap();
        let handle = core.open(file.inode).unwrap();
        // The fake backend has no Lore task context; keep this race on the test runtime.
        #[allow(clippy::disallowed_methods)]
        let reader = {
            let core = core.clone();
            tokio::spawn(async move { core.read(handle, 0, 3).await })
        };
        fake.block_read.as_ref().unwrap().0.notified().await;
        core.shutdown().unwrap();
        assert_eq!(core.getattr(ROOT_INODE), Err(Error::Unmounted));
        assert_eq!(core.open(file.inode), Err(Error::Unmounted));
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), reader)
                .await
                .expect("shutdown must cancel storage without waiting for its completion")
                .unwrap(),
            Err(Error::Unmounted)
        );
        core.shutdown().unwrap();
    }

    #[tokio::test]
    async fn release_discards_in_flight_reads() {
        let fake = Arc::new(Fake {
            block_read: Some((Notify::new(), Notify::new())),
            ..Fake::default()
        });
        let core = Arc::new(mount(fake.clone()));
        let file = core.lookup(ROOT_INODE, b"first").await.unwrap();
        let handle = core.open(file.inode).unwrap();
        // The fake backend has no Lore task context; keep this race on the test runtime.
        #[allow(clippy::disallowed_methods)]
        let reader = {
            let core = core.clone();
            tokio::spawn(async move { core.read(handle, 0, 3).await })
        };
        fake.block_read.as_ref().unwrap().0.notified().await;
        core.release(handle).unwrap();
        fake.block_read.as_ref().unwrap().1.notify_one();
        assert_eq!(reader.await.unwrap(), Err(Error::BadHandle));
    }
}
