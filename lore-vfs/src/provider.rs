// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Frozen namespace reads for normal Lore stage/commit operations. Writable
//! admission is refused while an operation holds its lease. Requested file content
//! is exported lazily into private bounded storage for Lore's ingestion interface.
//! Verified per-file commit receipts never discard overlay bytes or unstaged edits.
//! The experimental Linux adapter shares this namespace for mounted stage/commit
//! operations. Binding its checkpoint owner makes successful commit handoffs
//! durable; unbound providers retain volatile adoption. State-changing hold/sync
//! and operation mutators fail explicitly.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use bytes::Bytes;
use lore_base::types::Address;
use lore_base::types::Fragment;
use lore_base::types::Hash;
use lore_revision::fs::filesystem_provider::*;
use lore_revision::node::Node as RevisionNode;
use lore_revision::node::NodeFileMode;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::ChangeStream;
use lore_revision::state::FilesystemDiffStats;
use lore_revision::state::NodeComparison;
use lore_revision::state::State;
use lore_revision::util::path::RelativePath;

use crate::checkpoint_owner::CheckpointOwner;
use crate::core::Backend;
use crate::core::MAX_READ_SIZE;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::digest_index::DigestIndex;
use crate::namespace::Entry;
use crate::namespace::FrozenFile;
use crate::namespace::FrozenNamespace;
use crate::namespace::Namespace;
use crate::namespace::NamespaceError;
use crate::revision::RevisionBackend;

mod control;
mod exports;
mod routing;
#[cfg(unix)]
mod transactional_hold;
use crate::namespace::operation_routing::OperationRouting;
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_export_files: usize,
    pub max_export_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_export_files: 1024,
            max_export_bytes: 1024 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedFileReceipt {
    pub inode: u64,
    pub generation: u64,
    pub authority: NodeKey,
    pub committed_revision: Hash,
    pub address: Address,
    pub size: u64,
    pub mode: u16,
}
struct Inner {
    namespace: Arc<Namespace>,
    exports: Arc<exports::Owner>,
    limits: Limits,
    adoption: tokio::sync::Mutex<()>,
    control: Arc<control::Control>,
}
#[derive(Clone)]
pub struct WorkingProvider {
    inner: Arc<Inner>,
    digest_index: Option<DigestIndex>,
    checkpoint: Option<Arc<CheckpointOwner>>,
    #[cfg(unix)]
    transition_model: Option<std::sync::Weak<crate::linux_working::Model>>,
}
impl WorkingProvider {
    /// Reject new/preparing operations and cancel metadata reads. Already
    /// admitted operations may finish their durable handoffs.
    pub fn close_admission(&self) {
        self.inner.control.close();
    }
    /// Close admission and invalidate active phases. Blocked async requests are
    /// cancelled; admitted local checkpoint publishers retain their exact pins.
    pub fn cancel_operations(&self) {
        self.inner.control.cancel();
    }
    /// Close admission and wait for all phase and request tickets. Retained
    /// content sources and actual export IO have a separate drain.
    pub async fn drain_operations(&self) -> Result<(), FsError> {
        self.inner.control.drain().await
    }
    /// Close/drain operations, then wait for issued content sources, cloned
    /// handles, actual IO and cleanup. Retained source handles keep this pending.
    pub async fn drain_exports(&self) -> Result<(), FsError> {
        let operations = self.drain_operations().await;
        self.inner.exports.drain().await.and(operations)
    }
    /// `namespace` must use this exact backend instance. `export_directory` is a
    /// dedicated private directory, with no retained data from an older operation.
    /// Construction performs blocking ownership/lock checks.
    pub fn new(
        namespace: Arc<Namespace>,
        backend: Arc<RevisionBackend>,
        export_directory: PathBuf,
        limits: Limits,
    ) -> Result<Self, FsError> {
        let erased: Arc<dyn Backend> = backend.clone();
        if !namespace
            .uses_backend(&erased)
            .map_err(|error| namespace_error(error.into()))?
        {
            return Err(FsError::internal("namespace/backend ownership mismatch"));
        }
        let exports = exports::Owner::open(export_directory, limits)?;
        Ok(Self {
            digest_index: None,
            checkpoint: None,
            #[cfg(unix)]
            transition_model: None,
            inner: Arc::new(Inner {
                namespace,
                exports,
                limits,
                adoption: tokio::sync::Mutex::new(()),
                control: control::Control::new(),
            }),
        })
    }
    /// Bind actual mount durability. A provider without this owner is volatile.
    pub fn with_checkpoint_owner(mut self, owner: Arc<CheckpointOwner>) -> Result<Self, FsError> {
        if !Arc::ptr_eq(owner.namespace(), &self.inner.namespace) {
            return Err(FsError::internal(
                "checkpoint owner belongs to another namespace",
            ));
        }
        owner.available().map_err(checkpoint_error)?;
        self.checkpoint = Some(owner);
        Ok(self)
    }
    /// Publish ingestion hashes to this optional immutable disk index after the
    /// entire operation and finalizer succeed. Live receipt hashing is always enabled.
    pub fn with_digest_index(mut self, index: DigestIndex) -> Self {
        self.digest_index = Some(index);
        self
    }
    pub fn namespace(&self) -> &Arc<Namespace> {
        &self.inner.namespace
    }
    /// Receipts describe durable immutable bytes for one exact local generation;
    /// they do not mean the whole working tree or its parent link is clean.
    pub async fn receipts(&self) -> Result<Vec<CommittedFileReceipt>, FsError> {
        let _ticket = self.inner.control.prepare()?;
        self.inner
            .control
            .preparing(async {
                Ok(self
                    .inner
                    .namespace
                    .all_receipts()
                    .await
                    .map_err(namespace_error)?
                    .into_iter()
                    .map(|receipt| CommittedFileReceipt {
                        inode: receipt.inode,
                        generation: receipt.generation,
                        authority: receipt.authority,
                        committed_revision: receipt.proof.node.key.revision.hash,
                        address: receipt.proof.address,
                        size: receipt.proof.node.size,
                        mode: if receipt.mode & 0o111 != 0 {
                            NodeFileMode::Executable.bits()
                        } else {
                            0
                        },
                    })
                    .collect())
            })
            .await
    }
    /// Use the independently prepared backend owned by a restored namespace.
    pub async fn from_restored_namespace(
        namespace: Arc<Namespace>,
        export_directory: PathBuf,
        limits: Limits,
    ) -> Result<Self, FsError> {
        namespace.getattr(1).await.map_err(namespace_error)?;
        let exports = lore_base::lore_spawn_blocking_nocontext!(move || exports::Owner::open(
            export_directory,
            limits
        ))
        .await
        .map_err(|error| FsError::internal(format!("export owner task: {error}")))??;
        Ok(Self {
            digest_index: None,
            checkpoint: None,
            #[cfg(unix)]
            transition_model: None,
            inner: Arc::new(Inner {
                namespace,
                exports,
                limits,
                adoption: tokio::sync::Mutex::new(()),
                control: control::Control::new(),
            }),
        })
    }
}
#[async_trait]
impl FilesystemProvider for WorkingProvider {
    fn supports_transactional_root_hold(&self) -> bool {
        #[cfg(unix)]
        {
            self.transition_model.is_some()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
    async fn transactional_root_hold(
        &self,
        request: lore_revision::fs::filesystem_provider::RootHoldRequest,
    ) -> Result<lore_revision::fs::filesystem_provider::RootHoldOutcome, FsError> {
        #[cfg(unix)]
        {
            self.hold_root(request).await
        }
        #[cfg(not(unix))]
        {
            let _request = request;
            Err(unsupported())
        }
    }

    fn supports_revision_replacement(&self) -> bool {
        false
    }
    async fn read_metadata_file(
        &self,
        path: &RelativePath,
        maximum: usize,
    ) -> Result<Vec<u8>, FsError> {
        let _ticket = self.inner.control.prepare()?;
        self.inner
            .control
            .preparing(async {
                if maximum > lore_revision::repository::MAX_FILTER_FILE_BYTES
                    || path.is_empty()
                    || path
                        .as_str()
                        .split('/')
                        .any(|part| part.is_empty() || part == "." || part == "..")
                {
                    return Err(FsError::internal("invalid metadata read path or bound"));
                }
                let (lease, _routing) = self.capture_routing().await?;
                let namespace = &self.inner.namespace;
                let mut entry = namespace.getattr(1).await.map_err(namespace_error)?;
                for component in path.as_str().split('/') {
                    entry = namespace
                        .lookup(entry.inode, component.as_bytes())
                        .await
                        .map_err(|error| {
                            if error == NamespaceError::Backend(crate::core::Error::NotFound) {
                                not_found(path)
                            } else {
                                namespace_error(error)
                            }
                        })?;
                }
                let file = namespace
                    .frozen_file(lease.token(), entry.inode)
                    .await
                    .map_err(namespace_error)?;
                if file.size > maximum as u64 {
                    return Err(FsError::internal("metadata file exceeds read bound"));
                }
                let mut bytes = Vec::with_capacity(file.size as usize);
                while bytes.len() < file.size as usize {
                    let count = (file.size as usize - bytes.len()).min(MAX_READ_SIZE);
                    let chunk = namespace
                        .read_frozen(lease.token(), entry.inode, bytes.len() as u64, count)
                        .await
                        .map_err(namespace_error)?;
                    if chunk.len() != count {
                        return Err(FsError::internal("metadata file returned a short read"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Ok(bytes)
            })
            .await
    }
    fn holds_state(&self) -> bool {
        true
    }
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        Ok(Arc::new(InstanceOperationImpl::external(
            Operation {
                shared: self.operation_state().await?,
            },
            true,
        )))
    }
}
impl WorkingProvider {
    async fn operation_state(&self) -> Result<Arc<OperationState>, FsError> {
        let ticket = self.inner.control.prepare()?;
        let (lease, routing, exports) = self
            .inner
            .control
            .preparing(async {
                let (lease, routing) = self.capture_routing().await?;
                let exports = self
                    .inner
                    .exports
                    .operation(self.checkpoint.clone())
                    .await?;
                Ok((lease, routing, exports))
            })
            .await?;
        let token = lease.token();
        let shared = Arc::new(OperationState {
            digest_index: self.digest_index.clone(),
            checkpoint: self.checkpoint.clone(),
            cancelled: tokio::sync::watch::channel(false).0,
            token,
            routing,
            invalid_receipt: AtomicBool::new(false),
            adoption_failed: AtomicBool::new(false),
            exported_paths: Mutex::new(BTreeMap::new()),
            owner: self.inner.clone(),
            phase: Mutex::new(Phase {
                lease: Some(lease),
                active: true,
                ticket: Some(ticket),
                exports: Some(exports),
                files: Some(Arc::new(tokio::sync::Mutex::new(BTreeMap::new()))),
            }),
            pending: Mutex::new(BTreeMap::new()),
        });
        let weak = Arc::downgrade(&shared);
        let admitted = {
            let phase = lock(&shared.phase)?;
            self.inner.control.admit(
                phase
                    .ticket
                    .as_ref()
                    .ok_or_else(|| FsError::internal("working operation ticket missing"))?,
                Arc::new(move || {
                    if let Some(shared) = weak.upgrade() {
                        shared.finish();
                    }
                }),
            )
        };
        if let Err(error) = admitted {
            shared.finish();
            return Err(error);
        }
        Ok(shared)
    }
}

struct Phase {
    active: bool,
    lease: Option<FrozenNamespace>,
    ticket: Option<control::Ticket>,
    exports: Option<Arc<exports::OperationExports>>,
    files: Option<ExportCache>,
}
type ExportCache = Arc<tokio::sync::Mutex<BTreeMap<u64, Exported>>>;
struct Resolved {
    entry: Entry,
    source_root: u32,
    relative: String,
}
#[derive(Clone)]
struct Exported {
    sha256: [u8; 32],
    file: FrozenFile,
    path: PathBuf,
    source_root: u32,
    relative: String,
}
struct OperationState {
    digest_index: Option<DigestIndex>,
    cancelled: tokio::sync::watch::Sender<bool>,
    token: u64,
    routing: OperationRouting,
    invalid_receipt: AtomicBool,
    adoption_failed: AtomicBool,
    exported_paths: Mutex<BTreeMap<String, Exported>>,
    owner: Arc<Inner>,
    phase: Mutex<Phase>,
    pending: Mutex<BTreeMap<String, (Exported, Address, u64, u16)>>,
    // Drop terminal reservation after the namespace, exports and operation state.
    checkpoint: Option<Arc<CheckpointOwner>>,
}
fn lock<T>(mutex: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, FsError> {
    mutex
        .lock()
        .map_err(|_poisoned| FsError::internal("working provider state poisoned"))
}
fn checkpoint_error(error: crate::checkpoint_owner::Error) -> FsError {
    FsError::internal(format!("working checkpoint: {error:?}"))
}
fn namespace_error(error: NamespaceError) -> FsError {
    FsError::internal(format!("working namespace: {error:?}"))
}
fn unsupported() -> FsError {
    FsError::internal("this working provider does not apply state-changing operation mutations")
}
fn not_found(path: &RelativePath) -> FsError {
    lore_base::error::FileNotFound {
        resource: path.to_string(),
    }
    .into()
}
fn same_owner(a: NodeKey, b: NodeKey) -> bool {
    a.repository == b.repository && a.revision == b.revision && a.exposure == b.exposure
}
fn baseline_update(
    previous: Option<&crate::core::HeldBaseline>,
    scope: NodeKey,
    head: crate::core::ScopeHead,
    parent_projection: Option<crate::core::ScopeProjection>,
) -> Result<crate::namespace::BaselineUpdate, FsError> {
    let sequence = if let Some(previous) = previous {
        if previous.head == head && previous.parent_projection == parent_projection {
            previous.sequence
        } else {
            previous
                .sequence
                .checked_add(1)
                .ok_or_else(|| FsError::internal("held baseline sequence exhausted"))?
        }
    } else {
        1
    };
    Ok(crate::namespace::BaselineUpdate {
        expected: previous.cloned(),
        next: crate::core::HeldBaseline {
            scope,
            head,
            sequence,
            parent_projection,
        },
    })
}
impl OperationState {
    fn request<'a, T: Send + 'a>(
        &'a self,
        future: impl std::future::Future<Output = Result<T, FsError>> + Send + 'a,
    ) -> impl std::future::Future<Output = Result<T, FsError>> + Send + 'a {
        let future = Box::pin(future);
        async move {
            let mut cancelled = self.cancelled.subscribe();
            self.active()?;
            if *cancelled.borrow() {
                return Err(FsError::internal("working operation is no longer active"));
            }
            self.owner.control.request(async {
            tokio::select! { biased; _ = cancelled.changed() => Err(FsError::internal("working operation is no longer active")), result = future => result }
        }).await
        }
    }
    fn active(&self) -> Result<(), FsError> {
        if lock(&self.phase)?.active {
            Ok(())
        } else {
            Err(FsError::internal("working operation is no longer active"))
        }
    }
    fn finish(&self) {
        let (resources, poisoned) = {
            let (mut phase, poisoned) = match self.phase.lock() {
                Ok(phase) => (phase, false),
                Err(error) => (error.into_inner(), true),
            };
            phase.active = false;
            self.cancelled.send_replace(true);
            self.exported_paths
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clear();
            self.pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clear();
            (
                (
                    phase.lease.take(),
                    phase.ticket.take(),
                    phase.exports.take(),
                    phase.files.take(),
                ),
                poisoned,
            )
        };
        drop(resources);
        if poisoned {
            self.owner.control.fail();
        }
    }
    fn export_lease(&self) -> Result<Arc<exports::OperationExports>, FsError> {
        let phase = lock(&self.phase)?;
        if !phase.active {
            return Err(FsError::internal("working operation is no longer active"));
        }
        phase
            .exports
            .clone()
            .ok_or_else(|| FsError::internal("working export ownership missing"))
    }
    async fn resolve(&self, path: &RelativePath) -> Result<Resolved, FsError> {
        self.request(self.resolve_active(path)).await
    }
    async fn resolve_active(&self, path: &RelativePath) -> Result<Resolved, FsError> {
        self.active()?;
        let mut entry = self
            .owner
            .namespace
            .getattr(1)
            .await
            .map_err(namespace_error)?;
        let mut source_root = entry.authority.node;
        let mut relative = String::new();
        for component in path
            .as_str()
            .split('/')
            .filter(|component| !component.is_empty())
        {
            let parent = entry.authority;
            entry = self
                .owner
                .namespace
                .lookup(entry.inode, component.as_bytes())
                .await
                .map_err(|error| {
                    if error == NamespaceError::Backend(crate::core::Error::NotFound) {
                        not_found(path)
                    } else {
                        namespace_error(error)
                    }
                })?;
            if !same_owner(parent, entry.authority) {
                source_root = entry.authority.node;
                relative.clear();
            } else {
                if !relative.is_empty() {
                    relative.push('/');
                }
                relative.push_str(component);
            }
        }
        self.active()?;
        Ok(Resolved {
            entry,
            source_root,
            relative,
        })
    }
    async fn info(&self, entry: &Entry) -> Result<FileInfo, FsError> {
        self.request(self.info_active(entry)).await
    }
    async fn info_active(&self, entry: &Entry) -> Result<FileInfo, FsError> {
        if entry.kind == NodeKind::Directory {
            return Ok(FileInfo::Directory);
        }
        if entry.kind != NodeKind::File {
            return Err(unsupported());
        }
        let file = self
            .owner
            .namespace
            .frozen_file(self.token, entry.inode)
            .await
            .map_err(namespace_error)?;
        self.active()?;
        // A generation changes before writable admission. Stable synthetic times
        // therefore cannot mask a same-size edit in Lore's mtime comparison cache.
        Ok(FileInfo::File {
            executable: Some(file.mode & 0o111 != 0),
            size: file.size,
            mtime: file.generation,
        })
    }
    async fn export(&self, path: &RelativePath) -> Result<Exported, FsError> {
        self.request(self.export_active(path)).await
    }
    async fn export_active(&self, path: &RelativePath) -> Result<Exported, FsError> {
        let resolved = self.resolve(path).await?;
        let file = self
            .owner
            .namespace
            .frozen_file(self.token, resolved.entry.inode)
            .await
            .map_err(namespace_error)?;
        let cache = lock(&self.phase)?
            .files
            .clone()
            .ok_or_else(|| FsError::internal("working export cache missing"))?;
        let mut exports = cache.lock().await;
        self.active()?;
        if let Some(previous) = exports.get(&file.entry.inode) {
            if previous.file.generation != file.generation {
                return Err(FsError::internal("frozen file changed generation"));
            }
            let phase = lock(&self.phase)?;
            if !phase.active {
                return Err(FsError::internal("working operation is no longer active"));
            }
            let mut paths = lock(&self.exported_paths)?;
            if paths.len() >= self.owner.limits.max_export_files
                && !paths.contains_key(path.as_str())
            {
                return Err(FsError::internal("export path limit"));
            }
            paths.insert(path.to_string(), previous.clone());
            return Ok(previous.clone());
        }
        let lease = self.export_lease()?;
        let output = lease.reserve(file.size)?;
        let destination = lease.create(output.clone()).await?;
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        let mut offset = 0;
        while offset < file.size {
            self.active()?;
            let count = (file.size - offset).min(MAX_READ_SIZE as u64) as usize;
            let bytes = self
                .owner
                .namespace
                .read_frozen(self.token, file.entry.inode, offset, count)
                .await
                .map_err(namespace_error)?;
            if bytes.len() != count {
                return Err(FsError::internal("frozen file returned a short read"));
            }
            digest.update(&bytes);
            destination.write(offset, bytes).await?;
            offset += count as u64;
        }
        drop(destination);
        let checked = self
            .owner
            .namespace
            .frozen_file(self.token, file.entry.inode)
            .await
            .map_err(namespace_error)?;
        if checked.generation != file.generation
            || checked.size != file.size
            || checked.mode != file.mode
        {
            return Err(FsError::internal("export identity changed"));
        }
        self.active()?;
        let exported = Exported {
            sha256: digest.finish().as_ref().try_into().expect("SHA256 length"),
            file,
            path: output,
            source_root: resolved.source_root,
            relative: resolved.relative,
        };
        let phase = lock(&self.phase)?;
        if !phase.active {
            return Err(FsError::internal("export completed after abort"));
        }
        let mut paths = lock(&self.exported_paths)?;
        if paths.len() >= self.owner.limits.max_export_files && !paths.contains_key(path.as_str()) {
            return Err(FsError::internal("export path limit"));
        }
        paths.insert(path.to_string(), exported.clone());
        exports.insert(exported.file.entry.inode, exported.clone());
        Ok(exported)
    }
    async fn committed(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        digests: CommittedContentDigests,
    ) -> Result<(), FsError> {
        self.request(self.committed_active(repository, committed, scope, digests))
            .await
    }
    async fn committed_active(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        digests: CommittedContentDigests,
    ) -> Result<(), FsError> {
        let _adoption = self.owner.adoption.lock().await;
        if let Some(owner) = &self.checkpoint {
            owner.available().map_err(checkpoint_error)?;
        }
        self.active()?;
        if digests.repository() != repository.id || digests.revision() != committed.revision() {
            return Err(FsError::internal("commit digest scope mismatch"));
        }
        if self.invalid_receipt.load(Ordering::Acquire) {
            return Err(FsError::internal(
                "commit supplied an unmatched content receipt",
            ));
        }
        let revision = committed.revision();
        // Deserialize the immutable revision; commit may later reuse/mutate the
        // State object handed to this callback as its dirty staged anchor.
        let state = State::deserialize(repository.clone(), revision)
            .await
            .map_err(|error| FsError::internal(format!("committed revision: {error}")))?;
        let resolved = self.resolve(scope.mount_path()).await?;
        let info = self
            .source_metadata(
                &resolved.entry.authority,
                "committed scope",
                self.routing
                    .backend()
                    .owning_scope(&resolved.entry.authority),
            )
            .await?;
        if info.root.repository != repository.id
            || resolved.entry.authority != info.root
            || info.source_path != scope.source_path().as_bytes()
        {
            return Err(FsError::internal(
                "commit notification does not name the exposed source root",
            ));
        }
        let source_revision = Address::zero_context_hash(scope.source_revision());

        let source = self
            .source_metadata(
                &info.root,
                "commit source head",
                self.routing
                    .backend()
                    .scope_head(&info.root, source_revision),
            )
            .await?;
        if source.root != Some(scope.source_root()) {
            return Err(FsError::internal(
                "commit source root differs from immutable scope",
            ));
        }
        let current = self
            .owner
            .namespace
            .held_baselines()
            .await
            .map_err(namespace_error)?
            .into_iter()
            .map(|baseline| (baseline.scope, baseline))
            .collect::<BTreeMap<_, _>>();
        let new_revision = Address::zero_context_hash(revision);
        let expected_revision = current
            .get(&info.root)
            .map_or(info.root.revision, |baseline| baseline.head.revision);
        let initial_source =
            !current.contains_key(&info.root) && source_revision == info.initial_revision;
        if !initial_source
            && source_revision != expected_revision
            && new_revision != expected_revision
        {
            return Err(FsError::internal(
                "committed source is stale for the held scope",
            ));
        }
        let head = self
            .source_metadata(
                &info.root,
                "committed logical head",
                self.routing.backend().scope_head(&info.root, new_revision),
            )
            .await?;
        let projection = if let Some(parent) = &info.parent {
            let revision = current
                .get(&parent.scope)
                .map_or(parent.initial_revision, |baseline| baseline.head.revision);
            self.source_metadata(
                &info.root,
                "committed parent projection",
                self.routing
                    .backend()
                    .scope_projection(&info.root, revision),
            )
            .await?
        } else {
            None
        };
        let committed_root = head.root;
        let mut baselines = Vec::new();
        baselines.push(baseline_update(
            current.get(&info.root),
            info.root,
            head,
            projection,
        )?);
        for anchor in self
            .owner
            .namespace
            .frozen_scope_anchors(self.token)
            .await
            .map_err(namespace_error)?
        {
            let child = self
                .source_metadata(
                    &anchor,
                    "child owning scope",
                    self.routing.backend().owning_scope(&anchor),
                )
                .await?;
            if child.root == info.root
                || baselines
                    .iter()
                    .any(|update| update.next.scope == child.root)
            {
                continue;
            }
            let previous = current.get(&child.root);
            let Some(parent) = child.parent.as_ref() else {
                continue;
            };
            let related = parent.scope == info.root;
            if !related && !(previous.is_none() && info.parent.is_none()) {
                continue;
            }
            let head = match previous {
                Some(baseline) => baseline.head.clone(),
                None => {
                    self.source_metadata(
                        &child.root,
                        "initial child logical head",
                        self.routing
                            .backend()
                            .scope_head(&child.root, child.initial_revision),
                    )
                    .await?
                }
            };
            let parent_revision = if related {
                new_revision
            } else {
                current
                    .get(&parent.scope)
                    .map_or(parent.initial_revision, |baseline| baseline.head.revision)
            };
            let projection = self
                .source_metadata(
                    &child.root,
                    "parent committed projection",
                    self.routing
                        .backend()
                        .scope_projection(&child.root, parent_revision),
                )
                .await?;
            baselines.push(baseline_update(previous, child.root, head, projection)?);
        }
        let pending = lock(&self.pending)?
            .values()
            .filter(|(file, _, _, _)| same_owner(file.file.entry.authority, info.root))
            .cloned()
            .collect::<Vec<_>>();
        let mut receipts = Vec::new();
        for (export, address, size, mode) in pending {
            self.active()?;
            if export.source_root != info.root.node {
                return Err(FsError::internal(
                    "exported source root differs from committed exposure",
                ));
            }
            let link = state
                .find_relative_node_link(
                    repository.clone(),
                    committed_root
                        .ok_or_else(|| FsError::internal("committed source subtree is absent"))?,
                    &export.relative,
                )
                .await
                .map_err(|error| FsError::internal(format!("committed file identity: {error}")))?;
            if link.repository != repository.id || link.revision != revision {
                return Err(FsError::internal("committed file crossed its owning scope"));
            }
            let node = state
                .node(repository.clone(), link.node)
                .await
                .map_err(|error| FsError::internal(format!("committed file metadata: {error}")))?;
            let current = self
                .owner
                .namespace
                .frozen_file(self.token, export.file.entry.inode)
                .await
                .map_err(namespace_error)?;
            if !node.is_file()
                || current.entry.authority != export.file.entry.authority
                || node.address != address
                || node.size != size
                || node.mode != mode
                || current.generation != export.file.generation
                || current.size != size
                || (current.mode & 0o111 != 0) != (mode & NodeFileMode::Executable.bits() != 0)
            {
                return Err(FsError::internal(
                    "committed receipt does not describe the frozen file",
                ));
            }
            let sha256 = digests
                .content()
                .iter()
                .find(|content| content.address() == address && content.size() == size)
                .map(|content| content.sha256());
            if sha256.is_some_and(|sha256| sha256 != export.sha256) {
                return Err(FsError::internal(
                    "committed ingestion differs from authorized export",
                ));
            }
            let proof = crate::overlay::CommittedProof {
                address,
                node: crate::core::Node {
                    key: NodeKey {
                        repository: repository.id,
                        revision: Address::zero_context_hash(revision),
                        node: link.node,
                        exposure: current.entry.authority.exposure,
                    },
                    kind: NodeKind::File,
                    size,
                    mode: if current.mode & 0o111 != 0 {
                        0o555
                    } else {
                        0o444
                    },
                },
            };
            self.source_metadata(
                &current.entry.authority,
                "committed proof",
                self.routing
                    .backend()
                    .validate_committed(&current.entry.authority, &proof),
            )
            .await?;
            receipts.push(crate::overlay::Receipt {
                inode: current.entry.inode,
                generation: current.generation,
                authority: current.entry.authority,
                mode: current.mode,
                proof,
                sha256,
            });
        }
        self.owner
            .namespace
            .adopt_committed(self.token, receipts, baselines, || {
                let phase = self
                    .phase
                    .lock()
                    .map_err(|_poisoned| crate::core::Error::Io)?;
                if !phase.active {
                    return Err(crate::core::Error::Stale);
                }
                Ok(phase)
            })
            .await
            .map_err(namespace_error)?;
        if let Some(owner) = &self.checkpoint {
            let lease = {
                let phase = lock(&self.phase)?;
                if !phase.active {
                    return Err(FsError::internal("working operation is no longer active"));
                }
                phase
                    .lease
                    .as_ref()
                    .ok_or_else(|| FsError::internal("working freeze lease missing"))?
                    .pin()
            };
            owner
                .publish_frozen(lease)
                .await
                .map_err(checkpoint_error)?;
            self.active()?;
        }
        Ok(())
    }
}
struct Operation {
    shared: Arc<OperationState>,
}
#[async_trait]
impl ExternalOperation for Operation {
    fn requires_durable_commit_handoff(&self) -> bool {
        self.shared.checkpoint.is_some()
    }
    fn wants_content_sha256(&self) -> bool {
        true
    }
    async fn committed_digests(&self, digests: CommittedContentDigests) -> Result<(), FsError> {
        if let Some(index) = self.shared.digest_index.clone() {
            // Optional cache delivery follows finalization. It has its own
            // admission so a closed/drained provider cannot start late IO.
            let _ticket = self.shared.owner.control.prepare()?;
            self.shared
                .owner
                .control
                .preparing(self.shared.owner.exports.cache_job(
                    self.shared.checkpoint.clone(),
                    move || {
                        index
                            .publish_committed(&digests)
                            .map_err(|error| FsError::internal(format!("digest cache: {error}")))
                    },
                ))
                .await?;
        }
        Ok(())
    }
    async fn finalize(&self, changes: bool, outcome: OperationOutcome) -> Result<(), FsError> {
        self.shared
            .owner
            .control
            .request(async {
                let _adoption = self.shared.owner.adoption.lock().await;
                let (lease, ticket, exports, files) = {
                    let mut phase = lock(&self.shared.phase)?;
                    phase.active = false;
                    self.shared.cancelled.send_replace(true);
                    lock(&self.shared.exported_paths)?.clear();
                    lock(&self.shared.pending)?.clear();
                    (
                        phase.lease.take(),
                        phase.ticket.take(),
                        phase.exports.take(),
                        phase.files.take(),
                    )
                };
                drop(ticket);
                drop(exports);
                drop(files);
                let publication = if changes {
                    if let (Some(owner), Some(lease)) = (&self.shared.checkpoint, lease) {
                        owner.publish_frozen(lease).await.map_err(checkpoint_error)
                    } else {
                        Ok(())
                    }
                } else {
                    drop(lease);
                    Ok(())
                };
                publication?;
                if matches!(outcome, OperationOutcome::Succeeded)
                    && self.shared.adoption_failed.load(Ordering::Acquire)
                {
                    return Err(FsError::internal("durable commit receipt adoption failed"));
                }
                Ok(())
            })
            .await
    }
    fn abort(&self) {
        self.shared.finish();
    }
    async fn hold_state(
        &self,
        _repository: Arc<RepositoryContext>,
        _target: Arc<State>,
        _options: HoldOptions,
    ) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    fn stored(&self, path: &RelativePath, address: Address, size: u64, mode: u16) {
        let Ok(phase) = self.shared.phase.lock() else {
            return;
        };
        if !phase.active {
            return;
        }
        let export = lock(&self.shared.exported_paths)
            .ok()
            .and_then(|paths| paths.get(path.as_str()).cloned());
        if let Some(export) = export
            && export.file.size == size
            && (export.file.mode & 0o111 != 0) == (mode & NodeFileMode::Executable.bits() != 0)
            && let Ok(mut pending) = self.shared.pending.lock()
        {
            pending.insert(path.to_string(), (export, address, size, mode));
            return;
        }
        self.shared.invalid_receipt.store(true, Ordering::Release);
    }
    async fn committed(
        &self,
        repository: Arc<RepositoryContext>,
        committed: Arc<State>,
        scope: CommittedScope,
        digests: CommittedContentDigests,
    ) -> Result<(), FsError> {
        let result = self
            .shared
            .committed(repository, committed, scope, digests)
            .await;

        if result.is_err() {
            self.shared.adoption_failed.store(true, Ordering::Release);
        }
        result
    }
}

struct Listing {
    operation: Arc<OperationState>,
    directory: u64,
    cookie: Option<crate::namespace::Cookie>,
    pending: VecDeque<crate::namespace::NamedEntry>,
    eof: bool,
}
#[async_trait]
impl ExternalDirectoryListing for Listing {
    async fn next(&mut self) -> Option<Result<DirectoryEntry, FsError>> {
        let operation = self.operation.clone();
        operation
            .request(async { Ok(self.next_active().await) })
            .await
            .unwrap_or_else(|error| Some(Err(error)))
    }
}
impl Listing {
    async fn next_active(&mut self) -> Option<Result<DirectoryEntry, FsError>> {
        if let Err(error) = self.operation.active() {
            return Some(Err(error));
        }
        if self.pending.is_empty() && !self.eof {
            match self
                .operation
                .owner
                .namespace
                .list(
                    self.directory,
                    self.cookie,
                    crate::core::MAX_DIRECTORY_ENTRIES,
                )
                .await
            {
                Ok(page) => {
                    self.cookie = page.next;
                    self.eof = self.cookie.is_none();
                    self.pending = page.entries.into();
                }
                Err(error) => {
                    self.eof = true;
                    return Some(Err(namespace_error(error)));
                }
            }
        }
        let entry = self.pending.pop_front()?;
        let name = match String::from_utf8(entry.name) {
            Ok(name) => name,
            Err(_invalid_name) => {
                return Some(Err(FsError::internal("namespace name is not UTF-8")));
            }
        };
        let info = match self.operation.info(&entry.entry).await {
            Ok(info) => info,
            Err(error) => return Some(Err(error)),
        };
        if let Err(error) = self.operation.active() {
            return Some(Err(error));
        }
        Some(Ok(DirectoryEntry {
            name_hash: lore_storage::hash::hash_string(&name),
            name,
            info,
        }))
    }
}
impl InstanceOperation for Operation {
    fn changes_from_filesystem_to_state(
        &self,
        diff: FilesystemDiffContext,
    ) -> ChangeStream<FilesystemDiffStats> {
        lore_revision::state::diff_provider_filesystem(diff)
    }
    async fn file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        match self.shared.resolve(path).await {
            Ok(resolved) => self.shared.info(&resolved.entry).await,
            Err(FsError::FileNotFound(_)) => Ok(FileInfo::NotExist),
            Err(error) => Err(error),
        }
    }
    async fn untracked_file_info(&self, path: &RelativePath) -> Result<FileInfo, FsError> {
        self.file_info(path).await
    }
    async fn holds_name_exactly(&self, path: &RelativePath) -> Option<bool> {
        let parent = path.parent().unwrap_or_default();
        let parent = parent.parse::<RelativePath>().ok()?;
        let mut listing = self.read_directory(&parent).await.ok()?;
        while let Some(entry) = listing.next().await {
            let entry = entry.ok()?;
            if entry.name.to_lowercase() == path.name().to_lowercase() {
                return Some(entry.name == path.name());
            }
        }
        Some(false)
    }
    async fn read_directory(&self, path: &RelativePath) -> Result<DirectoryListing, FsError> {
        let resolved = self.shared.resolve(path).await?;
        if resolved.entry.kind != NodeKind::Directory {
            return Err(FsError::internal("provider path is not a directory"));
        }
        Ok(DirectoryListing::new(
            StaticDispatchDirectoryListing::External(Box::new(Listing {
                operation: self.shared.clone(),
                directory: resolved.entry.inode,
                cookie: None,
                pending: VecDeque::new(),
                eof: false,
            })),
        ))
    }
    async fn content_source(
        &self,
        path: &RelativePath,
    ) -> Result<lore_storage::ContentSource<'static>, FsError> {
        let exported = self.shared.export(path).await?;
        Ok(lore_storage::ContentSource::leased_file(
            exported.path,
            self.shared.export_lease()?,
        ))
    }
    async fn file_holds_content(
        &self,
        repository: Arc<RepositoryContext>,
        path: &RelativePath,
        previous: Address,
        previous_size: u64,
        established: &lore_storage::ContentHashes,
    ) -> Result<NodeComparison, FsError> {
        self.shared
            .request(async {
                let resolved = self.shared.resolve(path).await?;
                let file = self
                    .shared
                    .owner
                    .namespace
                    .frozen_file(self.shared.token, resolved.entry.inode)
                    .await
                    .map_err(namespace_error)?;
                if repository.id != file.entry.authority.repository {
                    return Err(FsError::internal(
                        "comparison repository does not own this exposure",
                    ));
                }
                if file.size != previous_size {
                    return Ok(NodeComparison::Differs);
                }
                let receipt = self
                    .shared
                    .owner
                    .namespace
                    .frozen_receipt(self.shared.token, file.entry.inode)
                    .await
                    .map_err(namespace_error)?;
                let source = if let Some(receipt) = receipt {
                    if receipt.generation != file.generation
                        || receipt.authority != file.entry.authority
                    {
                        return Err(FsError::internal("receipt generation mismatch"));
                    }
                    lore_storage::ContentSource::virtual_immutable(
                        receipt.proof.address,
                        receipt.proof.node.size,
                    )
                } else if !file.mutable && file.entry.base.is_some() {
                    match self
                        .shared
                        .source_metadata(
                            &file.entry.authority,
                            "immutable file identity",
                            self.shared
                                .routing
                                .backend()
                                .content_identity(&file.entry.authority),
                        )
                        .await?
                    {
                        Some(identity) => lore_storage::ContentSource::virtual_immutable(
                            identity.address,
                            identity.size,
                        ),
                        None => self.content_source(path).await?,
                    }
                } else {
                    self.content_source(path).await?
                };
                let previous_size = usize::try_from(previous_size)
                    .map_err(|_overflow| FsError::internal("comparison file is too large"))?;
                let comparison = lore_revision::immutable::file_matches(
                    repository,
                    previous,
                    Some(previous_size),
                    &source,
                    established,
                )
                .await
                .map_err(|error| {
                    FsError::internal(format!("frozen content comparison: {error}"))
                })?;
                self.shared
                    .owner
                    .namespace
                    .frozen_file(self.shared.token, file.entry.inode)
                    .await
                    .map_err(namespace_error)?;
                self.shared.active()?;
                Ok(lore_revision::state::node_comparison(comparison))
            })
            .await
    }
    async fn make_executable(
        &self,
        _path: &RelativePath,
        _executable: bool,
    ) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn create_dir_all(&self, _path: &RelativePath) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn write_file(&self, _path: &RelativePath, _contents: Bytes) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn rename(&self, _from: &RelativePath, _to: &RelativePath) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn remove(&self, _path: &RelativePath) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn remove_recursive(&self, _path: &RelativePath) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn write_node(
        &self,
        _repository: Arc<RepositoryContext>,
        _node: &RevisionNode,
        _path: &RelativePath,
    ) -> Result<FileInfo, FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn set_file_to_immutable_store_contents(
        &self,
        _repository: Arc<RepositoryContext>,
        _address: Address,
        _path: &RelativePath,
    ) -> Result<(Fragment, Option<FileInfo>), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
    async fn copy_file(
        &self,
        _source: &RelativePath,
        _destination: &RelativePath,
    ) -> Result<(), FsError> {
        self.shared.active()?;
        Err(unsupported())
    }
}
#[cfg(all(test, unix))]
#[path = "provider/tests.rs"]
mod tests;
