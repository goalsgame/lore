// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Bounded, volatile file overlays over a pinned immutable backend.
//!
//! This module does not implement a namespace, inode allocation, revision changes,
//! mappings, commit, or recovery. An adapter must retain one shared `OverlayFile`
//! for every alias within the same authorized `NodeKey` exposure, including
//! outstanding handles after namespace removal/replacement. Distinct exposures
//! must not be merged: cross-exposure mutable identity is not implemented.
//! It must obtain the node from an authorized
//! backend, never from untrusted caller-supplied keys. Data/metadata access rechecks the
//! full owning key through the required access gate, including reads served wholly
//! from local data. No path is treated as authorization.
//!
//! Writes append externally stored extents and publish only their successful
//! prefix. Failed/cancelled appends consume budget but cannot overwrite visible
//! bytes. Truncation never resurrects pinned bytes. There is no durable success
//! guarantee; the caller owns private storage lifetime and cleanup.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

use crate::core::Backend;
use crate::core::BackendFuture;
use crate::core::Error;
use crate::core::MAX_READ_SIZE;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::core::Result;

#[path = "candidate_edit.rs"]
pub(crate) mod candidate_edit;
pub use candidate_edit::CandidateEdit;
pub use candidate_edit::CandidateEditAction;
pub use candidate_edit::CandidateLocation;

#[path = "overlay_read.rs"]
mod contextual_read;
#[path = "overlay_handle.rs"]
mod handle_routes;
use handle_routes::IoRoute;
use handle_routes::OpenHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Read,
    /// A read/write handle. Mappings are unsupported.
    Write,
}

/// Must authorize the owning repository, pinned revision and exposure using the
/// mount's retained credentials. There is deliberately no permissive default.
pub trait AccessGate: Send + Sync {
    /// Explicit permission for a prepared local candidate. Read-only target
    /// proofs and successful checks alone grant no publication authority.
    fn authorize_candidate_edit<'a>(&'a self, _edit: &'a CandidateEdit) -> BackendFuture<'a, ()> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    /// Authorize inspection of a requested revision through a proven source
    /// selection. This grants no access to a resolved target node or payload.
    /// Authorize a proven immutable selection head, including explicit absence.
    /// This read-only proof does not authorize future candidate edits.
    fn authorize_target_head<'a>(
        &'a self,
        _proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    /// Authorize a node within the proven selected subtree. Absence grants no
    /// node access; writes require a separate future publication policy.
    fn authorize_target_node<'a>(
        &'a self,
        _proof: &'a ProvenTargetMetadata,
        _key: &'a NodeKey,
        _access: Access,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn authorize_selection<'a>(&'a self, _selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()>;
}

/// A backend-minted source route and requested revision, not a readable node.
/// Its in-memory identity is neither serialized nor reusable with a new backend.
#[derive(Clone)]
pub struct TargetSelection {
    pub(crate) source: crate::core::ScopeInfo,
    pub(crate) requested: lore_base::types::Address,
    pub(crate) backend_identity: Arc<()>,
    pub(crate) execution: Arc<lore_revision::interface::ExecutionContext>,
}
impl TargetSelection {
    pub fn source(&self) -> &crate::core::ScopeInfo {
        &self.source
    }
    pub fn original_scope(&self) -> NodeKey {
        self.source.root
    }
    pub fn requested_revision(&self) -> lore_base::types::Address {
        self.requested
    }
    pub fn source_path(&self) -> &[u8] {
        &self.source.source_path
    }
    pub fn captured_execution(&self) -> Arc<lore_revision::interface::ExecutionContext> {
        self.execution.clone()
    }
    pub fn belongs_to(&self, backend: &crate::revision::RevisionBackend) -> bool {
        Arc::ptr_eq(&self.backend_identity, &backend.selection_identity)
            && Arc::ptr_eq(&self.execution, &backend.captured_execution())
    }
}

/// Verified immutable metadata for one requested selection. It contains no
/// file payload, install permission, or readable ancestor for an absent subtree.
#[derive(Clone)]
pub struct ProvenTargetMetadata {
    pub(crate) selection: TargetSelection,
    pub(crate) state: Arc<lore_revision::state::State>,
    pub(crate) root: Option<Node>,
    pub(crate) max_depth: usize,
}
impl ProvenTargetMetadata {
    pub fn selection(&self) -> &TargetSelection {
        &self.selection
    }
    pub fn root(&self) -> Option<&Node> {
        self.root.as_ref()
    }
    pub fn head(&self) -> crate::core::ScopeHead {
        crate::core::ScopeHead {
            revision: self.selection.requested_revision(),
            root: self.root.as_ref().map(|node| node.key.node),
        }
    }
}

/// Bind an immutable policy to the final backend selected by restoration.
/// Persisted metadata and backend identity do not supply write authorization.
pub trait AccessGateFactory: Send + Sync {
    fn bind(&self, backend: Arc<dyn Backend>) -> Result<Arc<dyn AccessGate>>;
}
/// Preserve an independently injected gate without rebinding or replacing it.
pub struct InjectedGateFactory {
    gate: Arc<dyn AccessGate>,
}
impl InjectedGateFactory {
    pub fn new(gate: Arc<dyn AccessGate>) -> Self {
        Self { gate }
    }
}
impl AccessGateFactory for InjectedGateFactory {
    fn bind(&self, _backend: Arc<dyn Backend>) -> Result<Arc<dyn AccessGate>> {
        Ok(self.gate.clone())
    }
}

/// Private, exclusively owned append storage. Reads must return exactly `size`
/// bytes. Writes may return a successful short prefix; errors may have written
/// arbitrary bytes within the supplied range. Implementations must never alter
/// other ranges, and cancellation must not permit a later append to overlap it.
/// Neither operation implies fsync or crash recovery.
pub trait SparseStore: Send + Sync {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>>;
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize>;
}

/// An exclusively created external data file. The owner removes it after all
/// overlays and callbacks are dropped; no recovery metadata is written here.
pub struct FileStore(Mutex<tokio::fs::File>);

impl FileStore {
    pub async fn create(path: &Path) -> Result<Self> {
        let mut options = tokio::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        options
            .open(path)
            .await
            .map(|file| Self(Mutex::new(file)))
            .map_err(|_storage_error| Error::Io)
    }
}

impl SparseStore for FileStore {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            if size > MAX_READ_SIZE || offset.checked_add(size as u64).is_none() {
                return Err(Error::InvalidInput);
            }
            let mut file = self.0.lock().await;
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(|_storage_error| Error::Io)?;
            let mut bytes = vec![0; size];
            file.read_exact(&mut bytes)
                .await
                .map_err(|_storage_error| Error::Io)?;
            Ok(bytes)
        })
    }

    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            if bytes.len() > MAX_READ_SIZE || offset.checked_add(bytes.len() as u64).is_none() {
                return Err(Error::InvalidInput);
            }
            let mut file = self.0.lock().await;
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(|_storage_error| Error::Io)?;
            let count = file
                .write(bytes)
                .await
                .map_err(|_storage_error| Error::Io)?;
            // Drain Tokio's pending file write before permitting a later seek.
            file.flush().await.map_err(|_storage_error| Error::Io)?;
            Ok(count)
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_file_size: u64,
    /// Includes abandoned, overwritten and truncated append ranges.
    pub max_storage_bytes: u64,
    pub max_extents: usize,
    pub max_handles: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_file_size: 1 << 40,
            max_storage_bytes: 1 << 30,
            max_extents: 65_536,
            max_handles: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub size: u64,
    pub mode: u16,
    pub generation: u64,
    pub writers: usize,
    pub mutable: bool,
    pub reserved_storage_bytes: u64,
}

#[derive(Clone, Copy)]
struct Extent {
    end: u64,
    stored: u64,
}

struct State {
    metadata: Metadata,
    base_end: u64,
    extents: BTreeMap<u64, Extent>,
    handles: BTreeMap<u64, OpenHandle>,
    next_handle: u64,
}

/// Immutable metadata used to validate a committed file in its owning scope.
/// It is evidence to verify, never authorization supplied by its caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedProof {
    pub node: Node,
    pub address: lore_base::types::Address,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Receipt {
    pub inode: u64,
    pub authority: NodeKey,
    pub generation: u64,
    pub mode: u16,
    pub proof: CommittedProof,
    pub sha256: Option<[u8; 32]>,
}
pub(crate) type ReceiptCell = Arc<std::sync::Mutex<Option<Receipt>>>;
pub(crate) fn receipt_cell() -> ReceiptCell {
    Arc::new(std::sync::Mutex::new(None))
}
pub(crate) fn receipt_lock(
    cell: &ReceiptCell,
) -> Result<std::sync::MutexGuard<'_, Option<Receipt>>> {
    cell.lock().map_err(|_poisoned| Error::Io)
}
impl Receipt {
    pub(crate) fn validate(
        &self,
        inode: u64,
        authority: NodeKey,
        generation: u64,
        size: u64,
        mode: u16,
    ) -> Result<()> {
        if self.inode == 0
            || self.inode != inode
            || self.authority != authority
            || self.generation != generation
            || self.mode != mode
            || self.proof.node.kind != NodeKind::File
            || self.proof.node.size != size
            || self.proof.node.key.repository != authority.repository
            || self.proof.node.key.exposure != authority.exposure
            || !self.proof.node.key.revision.context.is_zero()
            || self.proof.node.mode != if mode & 0o111 != 0 { 0o555 } else { 0o444 }
        {
            return Err(Error::InvalidInput);
        }
        if size == 0
            && self.sha256.is_some_and(|sha| {
                sha.as_slice() != ring::digest::digest(&ring::digest::SHA256, &[]).as_ref()
            })
        {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}
/// A bounded copy of a file's visible overlay state. Handles are never restored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSnapshot {
    pub(crate) receipt: Option<Receipt>,
    pub(crate) node: Node,
    pub(crate) metadata: Metadata,
    pub(crate) base_end: u64,
    pub(crate) extents: Vec<SnapshotExtent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotExtent {
    pub(crate) start: u64,
    pub(crate) stored: u64,
    pub(crate) data: Vec<u8>,
}

#[path = "overlay_capture.rs"]
mod capture;
#[path = "overlay_live_capture.rs"]
mod live_capture;

/// Retains serialization while a mount-wide checkpoint locks its other files.
/// Acquire guards in stable file identity order under the namespace mutation lock.
pub struct CheckpointGuard<'a> {
    file: &'a OverlayFile,
    state: MutexGuard<'a, State>,
}

impl CheckpointGuard<'_> {
    pub(crate) fn extent_count(&self) -> usize {
        self.state.extents.len()
    }
    pub(crate) fn metadata(&self) -> Metadata {
        self.state.metadata
    }
    pub async fn capture(&self, max_bytes: usize) -> Result<FileSnapshot> {
        self.file
            .gate
            .authorize(&self.file.node.key, Access::Read)
            .await?;
        let mut remaining = max_bytes;
        let mut extents = Vec::new();
        for (&start, extent) in &self.state.extents {
            let length = usize::try_from(extent.end - start)
                .map_err(|_length_error| Error::ResourceLimit)?;
            remaining = remaining.checked_sub(length).ok_or(Error::ResourceLimit)?;
            let mut data = Vec::with_capacity(length);
            while data.len() < length {
                let count = (length - data.len()).min(MAX_READ_SIZE);
                let bytes = self
                    .file
                    .store
                    .read(extent.stored + data.len() as u64, count)
                    .await?;
                if bytes.len() != count {
                    return Err(Error::Io);
                }
                data.extend_from_slice(&bytes);
            }
            extents.push(SnapshotExtent {
                start,
                stored: extent.stored,
                data,
            });
        }
        let mut metadata = self.state.metadata;
        metadata.writers = 0;
        Ok(FileSnapshot {
            receipt: receipt_lock(&self.file.receipt)?.clone(),
            node: self.file.node.clone(),
            metadata,
            base_end: self.state.base_end,
            extents,
        })
    }
}

#[derive(Default)]
pub(crate) struct OperationFreeze {
    active: AtomicU64,
    next: AtomicU64,
}
impl OperationFreeze {
    pub(crate) fn acquire(&self) -> Result<Option<u64>> {
        if self.frozen() {
            return Ok(None);
        }
        let token = self
            .next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_overflow| Error::ResourceLimit)?
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        Ok(self
            .active
            .compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| token))
    }
    pub(crate) fn frozen(&self) -> bool {
        self.active.load(Ordering::Acquire) != 0
    }
    pub(crate) fn matches(&self, token: u64) -> bool {
        token != 0 && self.active.load(Ordering::Acquire) == token
    }
    pub(crate) fn release(&self, token: u64) {
        let _ = self
            .active
            .compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}
pub struct OverlayFile {
    mutation_gate: Arc<crate::namespace::metadata_gate::MutationGate>,
    receipt: ReceiptCell,
    operation_freeze: Arc<OperationFreeze>,
    node: Node,
    backend: Arc<dyn Backend>,
    gate: Arc<dyn AccessGate>,
    store: Arc<dyn SparseStore>,
    limits: Limits,
    // Per-file serialization spans I/O. Unrelated files never share this lock.
    state: Mutex<State>,
}

impl OverlayFile {
    /// `store` must be fresh, private storage for this file alone. Sharing it
    /// with another overlay or mutating it externally violates the range contract.
    pub fn new(
        node: Node,
        backend: Arc<dyn Backend>,
        gate: Arc<dyn AccessGate>,
        store: Arc<dyn SparseStore>,
        limits: Limits,
    ) -> Result<Self> {
        if node.kind != NodeKind::File {
            return Err(Error::InvalidInput);
        }
        if node.size > limits.max_file_size {
            return Err(Error::ResourceLimit);
        }
        let metadata = Metadata {
            size: node.size,
            mode: node.mode,
            generation: 0,
            writers: 0,
            mutable: false,
            reserved_storage_bytes: 0,
        };
        let base_end = node.size;
        Ok(Self {
            mutation_gate: crate::namespace::metadata_gate::for_backend(&backend),
            receipt: receipt_cell(),
            operation_freeze: Arc::new(OperationFreeze::default()),
            node,
            backend,
            gate,
            store,
            limits,
            state: Mutex::new(State {
                metadata,
                base_end,
                extents: BTreeMap::new(),
                handles: BTreeMap::new(),
                next_handle: 1,
            }),
        })
    }

    /// Creates an empty local file. The anchor is authorization context, not file
    /// identity; namespace inode allocation distinguishes siblings using this key.
    pub fn empty(
        anchor: NodeKey,
        mode: u16,
        backend: Arc<dyn Backend>,
        gate: Arc<dyn AccessGate>,
        store: Arc<dyn SparseStore>,
        limits: Limits,
    ) -> Result<Self> {
        let mut file = Self::new(
            Node {
                key: anchor,
                kind: NodeKind::File,
                size: 0,
                mode,
            },
            backend,
            gate,
            store,
            limits,
        )?;
        let state = file.state.get_mut();
        make_mutable(state, &file.receipt, &file.mutation_gate)?;
        Ok(file)
    }

    pub async fn checkpoint_guard(&self) -> CheckpointGuard<'_> {
        CheckpointGuard {
            file: self,
            state: self.state.lock().await,
        }
    }

    /// Restores into fresh, exclusively owned storage. Persisted identity is not
    /// authority: the retained credential gate must authorize it again.
    pub async fn restore(
        snapshot: FileSnapshot,
        backend: Arc<dyn Backend>,
        gate: Arc<dyn AccessGate>,
        store: Arc<dyn SparseStore>,
        limits: Limits,
    ) -> Result<Self> {
        snapshot.validate(limits)?;
        gate.authorize(
            &snapshot.node.key,
            if snapshot.metadata.mutable {
                Access::Write
            } else {
                Access::Read
            },
        )
        .await?;
        if let Some(receipt) = &snapshot.receipt {
            backend
                .validate_committed(&snapshot.node.key, &receipt.proof)
                .await?;
        }
        let mut extents = BTreeMap::new();
        for extent in snapshot.extents {
            let mut written = 0;
            while written < extent.data.len() {
                let end = (written + MAX_READ_SIZE).min(extent.data.len());
                let count = store
                    .write(extent.stored + written as u64, &extent.data[written..end])
                    .await?;
                if count == 0 || count > end - written {
                    return Err(Error::Io);
                }
                written += count;
            }
            extents.insert(
                extent.start,
                Extent {
                    end: extent.start + extent.data.len() as u64,
                    stored: extent.stored,
                },
            );
        }
        Ok(Self {
            mutation_gate: crate::namespace::metadata_gate::for_backend(&backend),
            receipt: Arc::new(std::sync::Mutex::new(snapshot.receipt)),
            operation_freeze: Arc::new(OperationFreeze::default()),
            node: snapshot.node,
            backend,
            gate,
            store,
            limits,
            state: Mutex::new(State {
                metadata: snapshot.metadata,
                base_end: snapshot.base_end,
                extents,
                handles: BTreeMap::new(),
                next_handle: 1,
            }),
        })
    }

    pub(crate) fn with_receipt_cell(mut self, cell: ReceiptCell) -> Self {
        self.receipt = cell;
        self
    }
    pub(crate) fn receipt_cell(&self) -> ReceiptCell {
        self.receipt.clone()
    }
    /// Fresh unpublished files must retain the exact prepared route and source
    /// mutation/freeze primitives, rather than a same-key policy substitute.
    pub(crate) fn has_prepared_bindings(
        &self,
        backend: &Arc<dyn Backend>,
        gate: &Arc<dyn AccessGate>,
        mutation: &Arc<crate::namespace::metadata_gate::MutationGate>,
        freeze: &Arc<OperationFreeze>,
    ) -> bool {
        Arc::ptr_eq(&self.backend, backend)
            && Arc::ptr_eq(&self.gate, gate)
            && Arc::ptr_eq(&self.mutation_gate, mutation)
            && Arc::ptr_eq(&self.operation_freeze, freeze)
    }

    pub(crate) fn with_operation_freeze(mut self, freeze: Arc<OperationFreeze>) -> Self {
        self.operation_freeze = freeze;
        self
    }
    pub(crate) fn with_mutation_gate(
        mut self,
        gate: Arc<crate::namespace::metadata_gate::MutationGate>,
    ) -> Self {
        self.mutation_gate = gate;
        self
    }
    pub fn key(&self) -> NodeKey {
        self.node.key
    }

    pub async fn metadata(&self) -> Result<Metadata> {
        let state = self.state.lock().await;
        self.gate.authorize(&self.node.key, Access::Read).await?;
        Ok(state.metadata)
    }

    pub async fn open(&self, access: Access) -> Result<u64> {
        self.open_default_route(access).await
    }

    /// Cleanup remains permitted after authorization revocation. This waits for
    /// earlier serialized operations; later uses of the handle are rejected.
    pub async fn release(&self, handle: u64) -> Result<()> {
        let released = {
            let mut state = self.state.lock().await;
            let released = state.handles.remove(&handle).ok_or(Error::BadHandle)?;
            if released.access == Access::Write {
                state.metadata.writers -= 1;
            }
            released
        };
        drop(released);
        Ok(())
    }

    pub async fn digest(&self) -> Result<Option<[u8; 32]>> {
        let state = self.state.lock().await;
        self.gate.authorize(&self.node.key, Access::Read).await?;
        if let Some(receipt) = receipt_lock(&self.receipt)?.as_ref() {
            receipt.validate(
                receipt.inode,
                self.node.key,
                state.metadata.generation,
                state.metadata.size,
                state.metadata.mode,
            )?;
            return Ok(receipt.sha256);
        }
        if state.metadata.mutable {
            return Ok(None);
        }
        self.backend.digest(&self.node.key).await
    }

    pub async fn read(&self, handle: u64, offset: u64, size: usize) -> Result<Vec<u8>> {
        if size > MAX_READ_SIZE {
            return Err(Error::InvalidInput);
        }
        let state = self.state.lock().await;
        let opened = state.handles.get(&handle).ok_or(Error::BadHandle)?;
        self.read_locked(&state, &opened.route, offset, size).await
    }
    async fn read_locked(
        &self,
        state: &State,
        route: &IoRoute,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        route.gate.authorize(&route.key, Access::Read).await?;
        let size = size.min(
            state
                .metadata
                .size
                .saturating_sub(offset)
                .min(MAX_READ_SIZE as u64) as usize,
        );
        let mut bytes = vec![0; size];
        let end = offset + size as u64;
        let mut cursor = offset;
        // Each range is served exactly once, avoiding base I/O under dirty extents.
        while cursor < end {
            if let Some((&start, extent)) = state
                .extents
                .range(..=cursor)
                .next_back()
                .filter(|(_, e)| e.end > cursor)
            {
                let length = (extent.end.min(end) - cursor) as usize;
                let data = self
                    .store
                    .read(extent.stored + cursor - start, length)
                    .await?;
                if data.len() != length {
                    return Err(Error::Io);
                }
                let dest = (cursor - offset) as usize;
                bytes[dest..dest + length].copy_from_slice(&data);
                cursor += length as u64;
            } else {
                let next = state
                    .extents
                    .range(cursor..)
                    .next()
                    .map_or(end, |(&start, _)| start.min(end));
                let base_end = next.min(state.base_end);
                if cursor < base_end {
                    let length = (base_end - cursor) as usize;
                    let data = route.backend.read(&route.key, cursor, length).await?;
                    if data.len() != length {
                        return Err(Error::Io);
                    }
                    let dest = (cursor - offset) as usize;
                    bytes[dest..dest + length].copy_from_slice(&data);
                }
                cursor = next;
            }
        }
        route.gate.authorize(&route.key, Access::Read).await?;
        Ok(bytes)
    }

    pub async fn write(&self, handle: u64, offset: u64, bytes: &[u8]) -> Result<usize> {
        if bytes.len() > MAX_READ_SIZE {
            return Err(Error::InvalidInput);
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::InvalidInput)?;
        let mut state = self.state.lock().await;
        self.writer(&state, handle).await?;
        if end > self.limits.max_file_size {
            return Err(Error::ResourceLimit);
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        let stored = state.metadata.reserved_storage_bytes;
        let reserved = stored
            .checked_add(bytes.len() as u64)
            .filter(|&n| n <= self.limits.max_storage_bytes)
            .ok_or(Error::ResourceLimit)?;
        // The shortest successful prefix has the largest possible extent count:
        // advancing its end can only consume existing extents or their tails.
        if prefix_extent_bound(&state.extents, offset) > self.limits.max_extents {
            return Err(Error::ResourceLimit);
        }
        make_mutable(&mut state, &self.receipt, &self.mutation_gate)?;
        state.metadata.reserved_storage_bytes = reserved;
        let count = self.store.write(stored, bytes).await?;
        if count > bytes.len() {
            return Err(Error::Io);
        }
        if count == 0 {
            return Err(Error::Io);
        }
        replace_range(&mut state.extents, offset, offset + count as u64, stored);
        state.metadata.size = state.metadata.size.max(offset + count as u64);
        Ok(count)
    }

    pub async fn truncate(&self, handle: u64, size: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        self.writer(&state, handle).await?;
        if size > self.limits.max_file_size {
            return Err(Error::ResourceLimit);
        }
        make_mutable(&mut state, &self.receipt, &self.mutation_gate)?;
        state.metadata.size = size;
        state.base_end = state.base_end.min(size);
        state.extents.retain(|&start, extent| {
            extent.end = extent.end.min(size);
            start < size
        });
        Ok(())
    }

    pub async fn set_executable(&self, handle: u64, executable: bool) -> Result<()> {
        let mut state = self.state.lock().await;
        self.writer(&state, handle).await?;
        make_mutable(&mut state, &self.receipt, &self.mutation_gate)?;
        if executable {
            state.metadata.mode |= 0o111;
        } else {
            state.metadata.mode &= !0o111;
        }
        Ok(())
    }

    async fn writer(&self, state: &State, handle: u64) -> Result<()> {
        if self.operation_freeze.frozen() {
            return Err(Error::Busy);
        }
        let opened = state.handles.get(&handle).ok_or(Error::BadHandle)?;
        if opened.access != Access::Write {
            return Err(Error::PermissionDenied);
        }
        opened
            .route
            .gate
            .authorize(&opened.route.key, Access::Write)
            .await?;
        if self.operation_freeze.frozen() {
            return Err(Error::Busy);
        }
        Ok(())
    }
}

impl FileSnapshot {
    pub fn key(&self) -> NodeKey {
        self.node.key
    }

    pub fn stored_bytes(&self) -> usize {
        self.extents.iter().map(|extent| extent.data.len()).sum()
    }

    pub(crate) fn validate(&self, limits: Limits) -> Result<()> {
        if let Some(receipt) = &self.receipt {
            receipt.validate(
                receipt.inode,
                self.node.key,
                self.metadata.generation,
                self.metadata.size,
                self.metadata.mode,
            )?;
        }
        if self.node.kind != NodeKind::File
            || self.metadata.writers != 0
            || self.base_end > self.node.size
            || self.base_end > self.metadata.size
            || self.node.size > limits.max_file_size
            || self.metadata.size > limits.max_file_size
            || self.metadata.reserved_storage_bytes > limits.max_storage_bytes
            || self.extents.len() > limits.max_extents
            || (self.metadata.mutable && self.metadata.generation == 0)
            || (!self.metadata.mutable
                && (self.metadata.generation != 0
                    || !self.extents.is_empty()
                    || self.metadata.reserved_storage_bytes != 0
                    || self.base_end != self.node.size
                    || self.metadata.size != self.node.size
                    || self.metadata.mode != self.node.mode))
        {
            return Err(Error::InvalidInput);
        }
        let mut previous = 0;
        let mut stored = Vec::with_capacity(self.extents.len());
        for extent in &self.extents {
            let end = extent
                .start
                .checked_add(extent.data.len() as u64)
                .ok_or(Error::InvalidInput)?;
            let stored_end = extent
                .stored
                .checked_add(extent.data.len() as u64)
                .ok_or(Error::InvalidInput)?;
            if extent.data.is_empty()
                || extent.start < previous
                || end > self.metadata.size
                || stored_end > self.metadata.reserved_storage_bytes
            {
                return Err(Error::InvalidInput);
            }
            previous = end;
            stored.push((extent.stored, stored_end));
        }
        stored.sort_unstable();
        if stored.windows(2).any(|ranges| ranges[0].1 > ranges[1].0) {
            return Err(Error::InvalidInput);
        }
        Ok(())
    }
}

fn make_mutable(
    state: &mut State,
    receipt: &ReceiptCell,
    gate: &crate::namespace::metadata_gate::MutationGate,
) -> Result<()> {
    let generation = state
        .metadata
        .generation
        .checked_add(1)
        .ok_or(Error::ResourceLimit)?;
    let mut cell = receipt_lock(receipt)?;
    let _mutation = gate.mutation()?;
    cell.take();
    state.metadata.generation = generation;
    state.metadata.mutable = true;
    Ok(())
}

fn prefix_extent_bound(extents: &BTreeMap<u64, Extent>, start: u64) -> usize {
    let additional = match extents.range(..=start).next_back() {
        Some((&old_start, old)) if old.end > start => {
            usize::from(old_start < start) + usize::from(old.end > start + 1)
        }
        _ => 1,
    };
    extents.len().saturating_add(additional)
}

fn replace_range(extents: &mut BTreeMap<u64, Extent>, start: u64, end: u64, stored: u64) {
    let overlaps: Vec<_> = extents
        .range(..end)
        .filter(|(_, e)| e.end > start)
        .map(|(&s, &e)| (s, e))
        .collect();
    for (old_start, old) in overlaps {
        extents.remove(&old_start);
        if old_start < start {
            extents.insert(
                old_start,
                Extent {
                    end: start,
                    stored: old.stored,
                },
            );
        }
        if old.end > end {
            extents.insert(
                end,
                Extent {
                    end: old.end,
                    stored: old.stored + end - old_start,
                },
            );
        }
    }
    extents.insert(start, Extent { end, stored });
}

#[path = "overlay_copy.rs"]
mod copy;
pub(crate) use copy::CopyLimits;

#[cfg(test)]
mod tests;
