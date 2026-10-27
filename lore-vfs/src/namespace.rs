// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Bounded mutable namespace over a pinned backend, shared by working providers,
//! mounted adapters and explicit recovery checkpoints.
//!
//! Local inode identity is separate from backend identity and authorization. New
//! nodes inherit their parent's authorization anchor; no invented backend node is
//! fetched. Entries and removed files remain retained within explicit limits.
//! Namespace writes and bounded cache loading serialize. Cached reads capture
//! metadata before asynchronous authorization without holding the namespace lock.
//! Cancellation before publication leaves names unchanged. File I/O uses
//! independent `OverlayFile` locks. UTF-8 names use Lore Unicode lowercase matching with preserved spelling.
//! No Unicode normalization is performed. Folded collisions fail explicitly.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

#[path = "metadata_gate.rs"]
pub(crate) mod metadata_gate;
use metadata_gate::MutationGate;
pub(crate) mod pinned_directory;
use pinned_directory::PinnedDirectorySnapshot;
use tokio::sync::Mutex;

use crate::core::Backend;
use crate::core::Error;
use crate::core::HeldBaseline;
use crate::core::MAX_DIRECTORY_ENTRIES;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::FileSnapshot;
use crate::overlay::Limits as FileLimits;
use crate::overlay::OperationFreeze;
use crate::overlay::OverlayFile;
use crate::overlay::Receipt;
use crate::overlay::ReceiptCell;
use crate::overlay::SparseStore;
use crate::overlay::receipt_cell;
use crate::overlay::receipt_lock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceError {
    Busy,
    Backend(Error),
    Exists,
    NotEmpty,
    CrossBoundary,
    Cycle,
}
impl From<Error> for NamespaceError {
    fn from(error: Error) -> Self {
        Self::Backend(error)
    }
}
pub type Result<T> = std::result::Result<T, NamespaceError>;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_inodes: usize,
    pub max_directory_entries: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_inodes: 65_536,
            max_directory_entries: 4096,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub inode: u64,
    pub parent: u64,
    pub kind: NodeKind,
    /// Full owning key for access rechecks, never a local file identifier.
    pub authority: NodeKey,
    pub base: Option<Node>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedEntry {
    pub name: Vec<u8>,
    pub entry: Entry,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cookie {
    pub generation: u64,
    pub offset: usize,
}
pub struct Page {
    pub entries: Vec<NamedEntry>,
    pub next: Option<Cookie>,
}

/// One bounded directory view captured under the namespace lock. Names and inode
/// identities remain stable in this value even when later mutations change paths.
#[derive(Clone, Debug)]
pub struct DirectorySnapshot {
    pub entry: Entry,
    pub parent: Entry,
    pub generation: u64,
    pub entries: Vec<NamedEntry>,
}

/// Coherent bounded namespace and dirty data checkpoint; no open handles.
#[derive(Clone, Debug)]
pub struct NamespaceSnapshot {
    pub(crate) original_root: Node,
    /// Missing entries retain the original pinned underlay, including staged
    /// origins. They never select a logical committed head implicitly.
    pub(crate) active_underlays: BTreeMap<NodeKey, crate::core::ScopeHead>,
    pub(crate) nodes: Vec<SnapshotNode>,
    pub(crate) next_inode: u64,
    pub(crate) generation: u64,
    pub(crate) files: BTreeMap<u64, FileSnapshot>,
    pub(crate) backend_state: Vec<u8>,
    pub(crate) baselines: BTreeMap<NodeKey, HeldBaseline>,
}
pub(crate) struct BaselineUpdate {
    pub expected: Option<HeldBaseline>,
    pub next: HeldBaseline,
}
#[derive(Clone, Debug)]
pub(crate) struct SnapshotNode {
    pub(crate) receipt: Option<Receipt>,
    pub(crate) entry: Entry,
    pub(crate) names: Option<Vec<(Vec<u8>, u64)>>,
    pub(crate) attached: bool,
}
impl NamespaceSnapshot {
    pub(crate) fn validate(&self, limits: Limits, file_limits: FileLimits) -> Result<()> {
        if self.backend_state.len() > crate::core::MAX_BACKEND_STATE_SIZE {
            return Err(Error::ResourceLimit.into());
        }
        if self.nodes.is_empty() || self.nodes.len() > limits.max_inodes || self.generation == 0 {
            return Err(Error::InvalidInput.into());
        }
        let mut nodes = BTreeMap::new();
        for node in &self.nodes {
            nodes.insert(node.entry.inode, node);
        }
        if nodes.len() != self.nodes.len()
            || nodes.keys().any(|&id| id == 0 || id >= self.next_inode)
        {
            return Err(Error::InvalidInput.into());
        }
        if self.original_root.kind != NodeKind::Directory {
            return Err(Error::InvalidInput.into());
        }
        if self.active_underlays.len() > limits.max_inodes.min(crate::core::MAX_OWNING_SCOPES) {
            return Err(Error::ResourceLimit.into());
        }
        let identity = |key: NodeKey| (key.repository, key.exposure, key.revision.context);
        let mut exposures = BTreeSet::new();
        let mut exposure_roots = BTreeMap::<_, Vec<_>>::new();
        for node in &self.nodes {
            let scope = identity(node.entry.authority);
            exposures.insert(scope);
            let parent = nodes.get(&node.entry.parent).ok_or(Error::InvalidInput)?;
            if node.attached
                && (node.entry.inode == 1
                    || !same_exposure(node.entry.authority, parent.entry.authority))
            {
                exposure_roots.entry(scope).or_default().push(node);
            }
        }
        let mut underlay_scopes = BTreeSet::new();
        for (scope, head) in &self.active_underlays {
            if !underlay_scopes.insert((scope.repository, scope.exposure)) {
                return Err(Error::InvalidInput.into());
            }
            if scope.revision.context != head.revision.context
                || !exposures.contains(&identity(*scope))
            {
                return Err(Error::InvalidInput.into());
            }
            for node in exposure_roots.get(&identity(*scope)).into_iter().flatten() {
                match head.root {
                    Some(id)
                        if node.entry.base.as_ref().is_none_or(|base| {
                            base.key
                                != NodeKey {
                                    revision: head.revision,
                                    node: id,
                                    ..*scope
                                }
                        }) =>
                    {
                        return Err(Error::InvalidInput.into());
                    }
                    None if node.entry.base.is_some()
                        || node.entry.authority != *scope
                        || node.entry.kind != NodeKind::Directory =>
                    {
                        return Err(Error::InvalidInput.into());
                    }
                    _ => {}
                }
            }
        }
        if self.baselines.len() > limits.max_inodes.min(crate::core::MAX_OWNING_SCOPES) {
            return Err(Error::ResourceLimit.into());
        }
        for (scope, baseline) in &self.baselines {
            if *scope != baseline.scope
                || baseline.sequence == 0
                || baseline.head.revision.context != scope.revision.context
                || !exposures.contains(&identity(*scope))
            {
                return Err(Error::InvalidInput.into());
            }
            if let Some(projection) = &baseline.parent_projection
                && (projection.parent.revision.context != projection.head.context
                    || !exposures.contains(&identity(projection.parent)))
            {
                return Err(Error::InvalidInput.into());
            }
        }
        let root = nodes.get(&1).ok_or(Error::InvalidInput)?;
        if root.entry.parent != 1 || root.entry.kind != NodeKind::Directory || !root.attached {
            return Err(Error::InvalidInput.into());
        }
        match self.active_underlays.get(&self.original_root.key) {
            None if root.entry.base.as_ref() != Some(&self.original_root) => {
                return Err(Error::InvalidInput.into());
            }
            Some(head) => match head.root {
                Some(id)
                    if root.entry.base.as_ref().is_none_or(|base| {
                        base.key
                            != NodeKey {
                                revision: head.revision,
                                node: id,
                                ..self.original_root.key
                            }
                    }) =>
                {
                    return Err(Error::InvalidInput.into());
                }
                None if root.entry.base.is_some()
                    || root.entry.authority != self.original_root.key =>
                {
                    return Err(Error::InvalidInput.into());
                }
                _ => {}
            },
            _ => {}
        }
        let mut referenced = BTreeSet::new();
        let mut base_keys = BTreeMap::new();
        for record in &self.nodes {
            let entry = &record.entry;
            if let Some(receipt) = &record.receipt {
                if entry.kind != NodeKind::File || self.files.contains_key(&entry.inode) {
                    return Err(Error::InvalidInput.into());
                }
                let base = entry.base.as_ref().ok_or(Error::InvalidInput)?;
                receipt.validate(entry.inode, entry.authority, 0, base.size, base.mode)?;
            }
            let parent = nodes.get(&entry.parent).ok_or(Error::InvalidInput)?;
            if entry.kind == NodeKind::Symlink {
                return Err(Error::Unsupported.into());
            }
            if parent.entry.kind != NodeKind::Directory {
                return Err(Error::InvalidInput.into());
            }
            if let Some(base) = &entry.base {
                if base.key != entry.authority
                    || base.kind != entry.kind
                    || (record.attached && base_keys.insert(base.key, entry.inode).is_some())
                {
                    return Err(Error::InvalidInput.into());
                }
            } else if !same_exposure(entry.authority, parent.entry.authority)
                && !self
                    .active_underlays
                    .get(&entry.authority)
                    .is_some_and(|head| head.root.is_none() && entry.kind == NodeKind::Directory)
            {
                return Err(Error::InvalidInput.into());
            }
            if entry.kind != NodeKind::Directory && record.names.is_some() {
                return Err(Error::InvalidInput.into());
            }
            if entry.kind == NodeKind::Directory && entry.base.is_none() && record.names.is_none() {
                return Err(Error::InvalidInput.into());
            }
            if let Some(names) = &record.names {
                if names.len() > limits.max_directory_entries {
                    return Err(Error::ResourceLimit.into());
                }
                let mut folded = BTreeSet::new();
                for (component, id) in names {
                    let child = nodes.get(id).ok_or(Error::InvalidInput)?;
                    if *id == 1
                        || child.entry.parent != entry.inode
                        || !child.attached
                        || !record.attached
                        || !folded.insert(name(component)?)
                        || !referenced.insert(*id)
                    {
                        return Err(Error::InvalidInput.into());
                    }
                }
            }
            if entry.kind == NodeKind::File
                && entry.base.is_none()
                && !self.files.contains_key(&entry.inode)
            {
                return Err(Error::InvalidInput.into());
            }
        }
        let mut rooted = BTreeSet::new();
        rooted.insert(1);
        for record in &self.nodes {
            let mut path = BTreeSet::new();
            let mut ancestor = record.entry.inode;
            while !rooted.contains(&ancestor) {
                if !path.insert(ancestor) {
                    return Err(Error::InvalidInput.into());
                }
                ancestor = nodes
                    .get(&ancestor)
                    .ok_or(Error::InvalidInput)?
                    .entry
                    .parent;
            }
            rooted.extend(path);
        }
        for node in &self.nodes {
            if node.entry.inode != 1 && node.attached && !referenced.contains(&node.entry.inode) {
                return Err(Error::InvalidInput.into());
            }
        }
        for (&id, file) in &self.files {
            let entry = &nodes.get(&id).ok_or(Error::InvalidInput)?.entry;
            if entry.kind != NodeKind::File
                || file.node.key != entry.authority
                || entry.base.as_ref().is_some_and(|base| base != &file.node)
                || (entry.base.is_none() && (file.base_end != 0 || !file.metadata.mutable))
            {
                return Err(Error::InvalidInput.into());
            }
            file.validate(file_limits)?;
            if file
                .receipt
                .as_ref()
                .is_some_and(|receipt| receipt.inode != id)
            {
                return Err(Error::InvalidInput.into());
            }
        }
        Ok(())
    }
}

type Names = BTreeMap<Vec<u8>, (Vec<u8>, u64)>;

struct Record {
    receipt: ReceiptCell,
    entry: Entry,
    names: Option<Names>,
    file: Option<Arc<OverlayFile>>,
    attached: bool,
    retired_binding: Option<Arc<NamespaceBinding>>,
}
struct State {
    active_underlays: BTreeMap<NodeKey, crate::core::ScopeHead>,
    nodes: BTreeMap<u64, Record>,
    backend_ids: BTreeMap<NodeKey, u64>,
    next_inode: u64,
    generation: u64,
    baselines: BTreeMap<NodeKey, HeldBaseline>,
}
struct BackendGate {
    underlays: BTreeMap<(lore_base::types::RepositoryId, u64), (NodeKey, crate::core::ScopeHead)>,
    backend: Arc<dyn Backend>,
    gate: Arc<dyn AccessGate>,
}
impl AccessGate for BackendGate {
    fn authorize<'a>(
        &'a self,
        key: &'a NodeKey,
        access: Access,
    ) -> crate::core::BackendFuture<'a, ()> {
        Box::pin(async move {
            if let Some((scope, head)) = self.underlays.get(&(key.repository, key.exposure)) {
                self.gate.authorize(scope, access).await?;
                let anchor = self.backend.underlay_view(scope, head).await?;
                self.gate.authorize(&anchor, access).await?;
            }
            self.gate.authorize(key, access).await?;
            self.backend
                .revalidate(key, access == Access::Write)
                .await?;
            if let Some((scope, head)) = self.underlays.get(&(key.repository, key.exposure)) {
                self.gate.authorize(scope, access).await?;
                let anchor = self.backend.underlay_view(scope, head).await?;
                self.gate.authorize(&anchor, access).await?;
            }
            self.gate.authorize(key, access).await
        })
    }
}
pub struct Namespace {
    mutation_gate: Arc<MutationGate>,
    original_root: Node,
    operation_freeze: Arc<OperationFreeze>,
    publication: Arc<crate::publication_ownership::Slot>,
    routing: routing::Routing,
    #[allow(
        dead_code,
        reason = "Retained for immutable policy binding to a prepared replacement backend."
    )]
    gate_factory: Arc<dyn crate::overlay::AccessGateFactory>,
    limits: Limits,
    state: Mutex<State>,
}
mod binding;
pub(crate) mod file_access;
mod file_read;
pub(crate) mod install;
mod publication;
mod routing;
mod source_snapshot;
mod successor;
use binding::NamespaceBinding;
pub(crate) mod complete_binding;
pub(crate) mod operation_routing;
pub(crate) mod prepared_view;
pub(crate) mod source_authorities;
pub(crate) mod unpublished;
pub(crate) struct MetadataSeal {
    // Reopen metadata before releasing the exact writer freeze.
    _seal: metadata_gate::Seal,
    _freeze: FrozenNamespace,
}
pub(crate) struct MetadataWitness {
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
    pub(crate) next_inode: u64,
}
impl Namespace {
    #[allow(
        dead_code,
        reason = "Metadata sealing has no production transition caller before durable intent integration."
    )]
    pub(crate) async fn seal_metadata(
        self: &Arc<Self>,
        freeze: FrozenNamespace,
        witness: MetadataWitness,
    ) -> Result<MetadataSeal> {
        if !Arc::ptr_eq(self, freeze.namespace()) {
            return Err(Error::Stale.into());
        }
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if !self.operation_freeze.matches(freeze.token())
            || state.generation != witness.generation
            || state.next_inode != witness.next_inode
        {
            return Err(Error::Stale.into());
        }
        let backend = binding
            .backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
            .ok_or(Error::Unsupported)?;
        let seal = backend.seal_metadata(witness.epoch)?;
        Ok(MetadataSeal {
            _seal: seal,
            _freeze: freeze,
        })
    }
    /// Bind once to this fresh backend. Authorization remains asynchronous in
    /// namespace operations; callers may authorize inode 1 before publication.
    pub fn new_with_gate_factory(
        backend: Arc<dyn Backend>,
        root: Node,
        factory: Arc<dyn crate::overlay::AccessGateFactory>,
        limits: Limits,
    ) -> Result<Self> {
        let gate = factory.bind(backend.clone())?;
        if root.kind != NodeKind::Directory {
            return Err(Error::NotDirectory.into());
        }
        if limits.max_inodes == 0 || limits.max_directory_entries == 0 {
            return Err(Error::InvalidInput.into());
        }
        let original_root = root.clone();
        let mutation_gate = backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
            .map_or_else(
                || Arc::new(MutationGate::default()),
                |backend| backend.shared_mutation_gate(),
            );
        let entry = Entry {
            inode: 1,
            parent: 1,
            kind: root.kind,
            authority: root.key,
            base: Some(root),
        };
        let policy = gate.clone();
        let gate: Arc<dyn AccessGate> = Arc::new(BackendGate {
            underlays: BTreeMap::new(),
            backend: backend.clone(),
            gate,
        });
        Ok(Self {
            mutation_gate,
            original_root,
            operation_freeze: Arc::new(OperationFreeze::default()),
            publication: Arc::new(crate::publication_ownership::Slot::default()),
            routing: routing::Routing::new(Arc::new(NamespaceBinding {
                backend,
                gate,
                policy,
            })),
            gate_factory: factory,
            limits,
            state: Mutex::new(State {
                active_underlays: BTreeMap::new(),
                backend_ids: BTreeMap::from([(entry.authority, 1)]),
                nodes: BTreeMap::from([(
                    1,
                    Record {
                        receipt: receipt_cell(),
                        entry,
                        names: None,
                        file: None,
                        attached: true,
                        retired_binding: None,
                    },
                )]),
                next_inode: 2,
                generation: 1,
                baselines: BTreeMap::new(),
            }),
        })
    }
    pub fn new(
        backend: Arc<dyn Backend>,
        root: Node,
        gate: Arc<dyn AccessGate>,
        limits: Limits,
    ) -> Result<Self> {
        Self::new_with_gate_factory(
            backend,
            root,
            Arc::new(crate::overlay::InjectedGateFactory::new(gate)),
            limits,
        )
    }
    /// Reject existing writers, then freeze mutations and every retained overlay's
    /// writable admission. Read handles remain usable. Dropping the lease thaws
    /// synchronously, including when the acquiring future is cancelled.
    pub async fn freeze_operation(self: &Arc<Self>) -> Result<FrozenNamespace> {
        let (token, lineage) = self.acquire_owned_freeze()?;
        let lease = FrozenNamespace {
            lease: Arc::new(FreezeLease {
                namespace: self.clone(),
                token,
                _lineage: lineage,
            }),
        };
        self.scan_frozen_writers(token).await?;
        Ok(lease)
    }
    pub(crate) async fn frozen_file(&self, token: u64, inode: u64) -> Result<FrozenFile> {
        let read = self.capture_record_read(inode, Some(token)).await?;
        let metadata = read.metadata().await?;
        self.confirm_record_read(&read, Some(token)).await?;
        Ok(FrozenFile {
            entry: read.entry,
            size: metadata.size,
            mode: metadata.mode,
            generation: metadata.generation,
            mutable: metadata.mutable,
        })
    }
    pub(crate) async fn read_frozen(
        &self,
        token: u64,
        inode: u64,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        if size > crate::core::MAX_READ_SIZE {
            return Err(Error::InvalidInput.into());
        }
        let read = self.capture_record_read(inode, Some(token)).await?;
        if read.entry.kind != NodeKind::File {
            return Err(Error::IsDirectory.into());
        }
        read.authorize().await?;
        let bytes = if let (Some(file), Some(access)) = (&read.file, &read.access) {
            file.read_frozen_bound(token, access, offset, size).await?
        } else {
            let base = read.entry.base.as_ref().ok_or(Error::Io)?;
            let count = size.min(
                base.size
                    .saturating_sub(offset)
                    .min(crate::core::MAX_READ_SIZE as u64) as usize,
            );
            let bytes = if count == 0 {
                Vec::new()
            } else {
                read.binding.backend.read(&base.key, offset, count).await?
            };
            if bytes.len() != count {
                return Err(Error::Io.into());
            }
            bytes
        };
        read.authorize().await?;
        self.confirm_record_read(&read, Some(token)).await?;
        Ok(bytes)
    }
    pub(crate) fn uses_backend(&self, backend: &Arc<dyn Backend>) -> crate::core::Result<bool> {
        Ok(Arc::ptr_eq(&self.current_binding()?.backend, backend))
    }
    pub(crate) async fn frozen_receipt(&self, token: u64, inode: u64) -> Result<Option<Receipt>> {
        let read = self.capture_record_read(inode, Some(token)).await?;
        if read.entry.kind != NodeKind::File {
            return Err(Error::IsDirectory.into());
        }
        read.authorize().await?;
        let receipt = receipt_lock(&read.cell)?.clone();
        read.authorize().await?;
        self.confirm_record_read(&read, Some(token)).await?;
        Ok(receipt)
    }
    pub(crate) async fn all_receipts(&self) -> Result<Vec<Receipt>> {
        let cells = {
            let state = self.state.lock().await;
            let binding = self.current_binding_locked(&state)?;
            state
                .nodes
                .values()
                .map(|record| {
                    Ok((
                        record.entry.authority,
                        record.receipt.clone(),
                        Self::record_binding(record, &binding)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let mut receipts = Vec::new();
        for (key, cell, binding) in cells {
            binding.gate.authorize(&key, Access::Read).await?;
            if let Some(receipt) = receipt_lock(&cell)?.clone() {
                receipts.push(receipt);
            }
        }
        Ok(receipts)
    }
    pub(crate) fn backend(&self) -> crate::core::Result<Arc<dyn Backend>> {
        Ok(self.current_binding()?.backend.clone())
    }
    /// Computed digest for the current local inode. A receipt with unknown hash
    /// stays absent; adopted mutable content never falls back to the old base.
    pub async fn digest(&self, inode: u64) -> Result<Option<[u8; 32]>> {
        let read = self.capture_record_read(inode, None).await?;
        read.authorize().await?;
        let digest = if let (Some(file), Some(access)) = (&read.file, &read.access) {
            file.digest_bound(access).await?
        } else if read.entry.kind != NodeKind::File {
            None
        } else {
            let base = read.entry.base.as_ref().ok_or(Error::Io)?;
            let receipt = receipt_lock(&read.cell)?.clone();
            if let Some(receipt) = receipt {
                receipt.validate(inode, read.entry.authority, 0, base.size, base.mode)?;
                receipt.sha256
            } else {
                read.binding.backend.digest(&base.key).await?
            }
        };
        read.authorize().await?;
        self.confirm_record_read(&read, None).await?;
        Ok(digest)
    }
    pub(crate) async fn frozen_scope_anchors(&self, token: u64) -> Result<Vec<NodeKey>> {
        let state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale.into());
        }
        let mut scopes = BTreeMap::new();
        for record in state.nodes.values() {
            let key = record.entry.authority;
            scopes
                .entry((key.repository, key.revision, key.exposure))
                .or_insert((key, Self::record_binding(record, &binding)?));
        }
        drop(state);
        for (key, binding) in scopes.values() {
            binding.gate.authorize(key, Access::Read).await?;
        }
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale.into());
        }
        Ok(scopes.into_values().map(|(key, _binding)| key).collect())
    }
    pub async fn held_baselines(&self) -> Result<Vec<HeldBaseline>> {
        let (baselines, binding) = {
            let state = self.state.lock().await;
            let binding = self.current_binding_locked(&state)?;
            (
                state.baselines.values().cloned().collect::<Vec<_>>(),
                binding,
            )
        };
        for baseline in &baselines {
            binding
                .gate
                .authorize(&baseline.scope, Access::Read)
                .await?;
        }
        Ok(baselines)
    }
    async fn validate_baseline(
        &self,
        baseline: &HeldBaseline,
        binding: &Arc<NamespaceBinding>,
    ) -> Result<()> {
        binding
            .gate
            .authorize(&baseline.scope, Access::Read)
            .await?;
        let scope = binding.backend.owning_scope(&baseline.scope).await?;
        if scope.root != baseline.scope
            || binding
                .backend
                .scope_head(&baseline.scope, baseline.head.revision)
                .await?
                != baseline.head
        {
            return Err(Error::InvalidInput.into());
        }
        match (&scope.parent, &baseline.parent_projection) {
            (None, None) => {}
            (Some(parent), Some(projection)) => {
                binding.gate.authorize(&parent.scope, Access::Read).await?;
                if projection.parent != parent.scope
                    || binding
                        .backend
                        .scope_projection(&baseline.scope, projection.head)
                        .await?
                        .as_ref()
                        != Some(projection)
                {
                    return Err(Error::InvalidInput.into());
                }
            }
            _ => return Err(Error::InvalidInput.into()),
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) async fn adopt_receipts<Permit, F>(
        &self,
        token: u64,
        receipts: Vec<Receipt>,
        permit: F,
    ) -> Result<Vec<Receipt>>
    where
        F: FnOnce() -> crate::core::Result<Permit>,
    {
        self.adopt_committed(token, receipts, Vec::new(), permit)
            .await
    }
    pub(crate) async fn adopt_committed<Permit, F>(
        &self,
        token: u64,
        mut receipts: Vec<Receipt>,
        mut baselines: Vec<BaselineUpdate>,
        permit: F,
    ) -> Result<Vec<Receipt>>
    where
        F: FnOnce() -> crate::core::Result<Permit>,
    {
        receipts.sort_by_key(|receipt| receipt.inode);
        if receipts
            .windows(2)
            .any(|pair| pair[0].inode == pair[1].inode)
        {
            return Err(Error::InvalidInput.into());
        }
        baselines.sort_by_key(|update| update.next.scope);
        if baselines
            .windows(2)
            .any(|pair| pair[0].next.scope == pair[1].next.scope)
        {
            return Err(Error::InvalidInput.into());
        }
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale.into());
        }
        let binding = {
            let state = self.state.lock().await;
            if !self.operation_freeze.matches(token) {
                return Err(Error::Stale.into());
            }
            self.current_binding_locked(&state)?
        };
        // Immutable proof IO precedes the namespace lock. Publication still
        // requires the same freeze token and exact baseline CAS below.
        for update in &baselines {
            self.validate_baseline(&update.next, &binding).await?;
        }
        let mut state = self.state.lock().await;
        self.confirms_binding(&binding)?;
        self.mutation_gate.open()?;
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale.into());
        }
        let mut added = 0usize;
        let mut changed = false;
        for update in &baselines {
            let old = state.baselines.get(&update.next.scope);
            if old != update.expected.as_ref() {
                return Err(Error::Stale.into());
            }
            if old != Some(&update.next) {
                let sequence = old
                    .map_or(Some(1), |old| old.sequence.checked_add(1))
                    .ok_or(Error::ResourceLimit)?;
                if update.next.sequence != sequence {
                    return Err(Error::InvalidInput.into());
                }
                changed = true;
                if old.is_none() {
                    added += 1;
                }
            }
        }
        if state
            .baselines
            .len()
            .checked_add(added)
            .is_none_or(|count| count > self.limits.max_inodes.min(crate::core::MAX_OWNING_SCOPES))
        {
            return Err(Error::ResourceLimit.into());
        }
        let generation = if changed {
            state
                .generation
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?
        } else {
            state.generation
        };
        let mut retained_files = BTreeMap::new();
        for receipt in &receipts {
            let record = node(&state, receipt.inode)?;
            if let Some(file) = &record.file {
                retained_files.insert(receipt.inode, file.clone());
            }
        }
        let mut guards = BTreeMap::new();
        for (&inode, file) in &retained_files {
            guards.insert(inode, file.checkpoint_guard().await);
        }
        for receipt in &receipts {
            let record = node(&state, receipt.inode)?;
            if record.entry.kind != NodeKind::File {
                return Err(Error::InvalidInput.into());
            }
            let (generation, size, mode, writers) = if let Some(guard) = guards.get(&receipt.inode)
            {
                let metadata = guard.metadata();
                (
                    metadata.generation,
                    metadata.size,
                    metadata.mode,
                    metadata.writers,
                )
            } else {
                let base = record.entry.base.as_ref().ok_or(Error::Io)?;
                (0, base.size, base.mode, 0)
            };
            if writers != 0 {
                return Err(NamespaceError::Busy);
            }
            receipt.validate(
                receipt.inode,
                record.entry.authority,
                generation,
                size,
                mode,
            )?;
        }
        let retained_cells = receipts
            .iter()
            .map(|receipt| Ok(node(&state, receipt.inode)?.receipt.clone()))
            .collect::<Result<Vec<_>>>()?;
        let mut cells = retained_cells
            .iter()
            .map(receipt_lock)
            .collect::<crate::core::Result<Vec<_>>>()?;
        // No await below: abort takes only the final phase lock and never waits
        // for namespace, file, or cell locks in the opposite order.
        let _permit = permit()?;
        if !self.operation_freeze.matches(token) {
            return Err(Error::Stale.into());
        }
        for (cell, receipt) in cells.iter().zip(receipts.iter_mut()) {
            if let Some(previous) = cell.as_ref()
                && previous.generation == receipt.generation
            {
                if previous.sha256.is_some()
                    && receipt.sha256.is_some()
                    && previous.sha256 != receipt.sha256
                {
                    return Err(Error::InvalidInput.into());
                }
                // Keep the first immutable proof for unchanged bytes, even if
                // a later metadata-only commit establishes another revision.
                let mut retained = previous.clone();
                let mut equal = retained.clone();
                equal.sha256 = receipt.sha256;
                if equal == *receipt && retained.sha256.is_none() {
                    retained.sha256 = receipt.sha256;
                }
                *receipt = retained;
            }
        }
        let _mutation = self.mutation_gate.mutation()?;
        for (cell, receipt) in cells.iter_mut().zip(&receipts) {
            **cell = Some(receipt.clone());
        }
        for update in baselines {
            state.baselines.insert(update.next.scope, update.next);
        }
        state.generation = generation;
        drop(_mutation);
        drop(cells);
        drop(guards);
        Ok(receipts)
    }
    /// Acquires ordered file guards under the namespace lock, then releases the
    /// namespace before policy and private-storage reads. Acquisition may wait
    /// for existing file IO. While file guards are held, gate/store callbacks
    /// must not reenter this namespace or a locked file: another namespace
    /// holder can be waiting for one of those file guards.
    pub async fn snapshot(&self, max_data_bytes: usize) -> Result<NamespaceSnapshot> {
        self.capture_source_snapshot(max_data_bytes, None, None)
            .await
            .map(|(snapshot, _)| snapshot)
    }
    #[cfg(test)]
    pub(crate) fn transition_policy(&self) -> crate::core::Result<Arc<dyn AccessGate>> {
        Ok(self.current_binding()?.policy.clone())
    }
    pub(crate) fn file_cache_mutation(&self) -> crate::core::Result<metadata_gate::Mutation<'_>> {
        self.mutation_gate.mutation()
    }
    pub(crate) fn retained_limits(&self) -> Limits {
        self.limits
    }
    pub(crate) async fn transition_snapshot(
        &self,
        token: u64,
        budget: &mut crate::transition_prepare::Budget,
    ) -> Result<NamespaceSnapshot> {
        self.transition_snapshot_epoch(token, budget)
            .await
            .map(|(snapshot, _)| snapshot)
    }
    pub(crate) fn mutation_epoch(&self) -> Result<u64> {
        self.mutation_gate.epoch().map_err(Into::into)
    }
    pub(crate) async fn transition_snapshot_epoch(
        &self,
        token: u64,
        budget: &mut crate::transition_prepare::Budget,
    ) -> Result<(NamespaceSnapshot, u64)> {
        self.capture_source_snapshot(
            budget.limits.max_private_data_bytes,
            Some(token),
            Some(budget),
        )
        .await
    }
    /// Restore requires the same explicit pinned root and fresh per-file stores.
    /// Backend provenance prepares an independent instance before authorization;
    /// a failed or cancelled restore cannot rebind the caller's shared backend.
    /// Checksums and persisted keys provide no authorization. Store cleanup belongs
    /// to the caller, including partial writes from failed/cancelled restore.
    pub async fn restore(
        backend: Arc<dyn Backend>,
        root: Node,
        gate: Arc<dyn AccessGate>,
        snapshot: NamespaceSnapshot,
        stores: BTreeMap<u64, Arc<dyn SparseStore>>,
        limits: Limits,
        file_limits: FileLimits,
    ) -> Result<Self> {
        Self::restore_with_gate_factory(
            backend,
            root,
            Arc::new(crate::overlay::InjectedGateFactory::new(gate)),
            snapshot,
            stores,
            limits,
            file_limits,
        )
        .await
    }
    /// Bind the factory exactly once after provenance replay selects the final
    /// backend, before checking retained authority or reconstructing file data.
    pub async fn restore_with_gate_factory(
        backend: Arc<dyn Backend>,
        root: Node,
        factory: Arc<dyn crate::overlay::AccessGateFactory>,
        snapshot: NamespaceSnapshot,
        mut stores: BTreeMap<u64, Arc<dyn SparseStore>>,
        limits: Limits,
        file_limits: FileLimits,
    ) -> Result<Self> {
        snapshot.validate(limits, file_limits)?;
        if snapshot.original_root != root
            || stores.len() != snapshot.files.len()
            || snapshot.files.keys().any(|id| !stores.contains_key(id))
        {
            return Err(Error::InvalidInput.into());
        }
        let backend = backend
            .prepare_restore(&snapshot.backend_state)
            .await?
            .unwrap_or(backend);
        let gate = factory.bind(backend.clone())?;
        let mutation_gate = backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
            .map_or_else(
                || Arc::new(MutationGate::default()),
                |backend| backend.shared_mutation_gate(),
            );
        let policy = gate.clone();
        let gate: Arc<dyn AccessGate> = Arc::new(BackendGate {
            underlays: snapshot
                .active_underlays
                .iter()
                .map(|(scope, head)| ((scope.repository, scope.exposure), (*scope, head.clone())))
                .collect(),
            backend: backend.clone(),
            gate,
        });
        gate.authorize(&root.key, Access::Read).await?;
        for (scope, head) in &snapshot.active_underlays {
            gate.authorize(scope, Access::Read).await?;
            if backend.owning_scope(scope).await?.root != *scope {
                return Err(Error::InvalidInput.into());
            }
            let target = backend.underlay_view(scope, head).await?;
            policy.authorize(&target, Access::Read).await?;
        }
        for record in &snapshot.nodes {
            gate.authorize(&record.entry.authority, Access::Read)
                .await?;
            if let Some(revision) = backend
                .as_any()
                .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
            {
                let actual = revision
                    .restore_node_metadata(&record.entry.authority)
                    .await?;
                if match &record.entry.base {
                    Some(base) => actual != *base,
                    None => actual.kind != NodeKind::Directory,
                } {
                    return Err(Error::InvalidInput.into());
                }
            }
            if let Some(receipt) = &record.receipt {
                backend
                    .validate_committed(&record.entry.authority, &receipt.proof)
                    .await?;
            }
        }
        for baseline in snapshot.baselines.values() {
            gate.authorize(&baseline.scope, Access::Read).await?;
            let scope = backend.owning_scope(&baseline.scope).await?;
            if scope.root != baseline.scope
                || backend
                    .scope_head(&baseline.scope, baseline.head.revision)
                    .await?
                    != baseline.head
            {
                return Err(Error::InvalidInput.into());
            }
            match (&scope.parent, &baseline.parent_projection) {
                (None, None) => {}
                (Some(parent), Some(projection)) => {
                    gate.authorize(&parent.scope, Access::Read).await?;
                    if projection.parent != parent.scope
                        || backend
                            .scope_projection(&baseline.scope, projection.head)
                            .await?
                            .as_ref()
                            != Some(projection)
                    {
                        return Err(Error::InvalidInput.into());
                    }
                }
                _ => return Err(Error::InvalidInput.into()),
            }
        }
        let binding = Arc::new(NamespaceBinding {
            backend: backend.clone(),
            gate: gate.clone(),
            policy: policy.clone(),
        });
        let operation_freeze = Arc::new(OperationFreeze::default());
        let mut files = BTreeMap::new();
        for (id, file) in snapshot.files {
            files.insert(
                id,
                Arc::new(
                    OverlayFile::restore(
                        file,
                        backend.clone(),
                        gate.clone(),
                        stores.remove(&id).ok_or(Error::InvalidInput)?,
                        file_limits,
                    )
                    .await?
                    .with_mutation_gate(mutation_gate.clone())
                    .with_operation_freeze(operation_freeze.clone()),
                ),
            );
        }
        let mut nodes = BTreeMap::new();
        let mut backend_ids = BTreeMap::new();
        for record in snapshot.nodes {
            if let Some(base) = &record.entry.base
                && record.attached
            {
                backend_ids.insert(base.key, record.entry.inode);
            }
            let names = record
                .names
                .map(|names| {
                    names
                        .into_iter()
                        .map(|(component, id)| Ok((name(&component)?, (component, id))))
                        .collect::<Result<BTreeMap<_, _>>>()
                })
                .transpose()?;
            let file = files.remove(&record.entry.inode);
            let receipt = file.as_ref().map_or_else(
                || Arc::new(std::sync::Mutex::new(record.receipt)),
                |file| file.receipt_cell(),
            );
            nodes.insert(
                record.entry.inode,
                Record {
                    receipt,
                    file,
                    entry: record.entry,
                    names,
                    attached: record.attached,
                    retired_binding: (!record.attached).then(|| binding.clone()),
                },
            );
        }
        Ok(Self {
            mutation_gate,
            original_root: root,
            operation_freeze,
            publication: Arc::new(crate::publication_ownership::Slot::default()),
            routing: routing::Routing::new(binding),
            gate_factory: factory,
            limits,
            state: Mutex::new(State {
                active_underlays: snapshot.active_underlays,
                nodes,
                backend_ids,
                next_inode: snapshot.next_inode,
                generation: snapshot.generation,
                baselines: snapshot.baselines,
            }),
        })
    }
    pub async fn getattr(&self, inode: u64) -> Result<Entry> {
        let (entry, binding) = {
            let state = self.state.lock().await;
            let binding = self.current_binding_locked(&state)?;
            let record = node(&state, inode)?;
            (
                record.entry.clone(),
                Self::record_binding(record, &binding)?,
            )
        };
        binding
            .gate
            .authorize(&entry.authority, Access::Read)
            .await?;
        Ok(entry)
    }
    async fn readonly_directory(&self, inode: u64) -> Result<(Entry, u64, Arc<NamespaceBinding>)> {
        let (entry, generation, binding) = {
            let mut state = self.state.lock().await;
            let binding = self.current_binding_locked(&state)?;
            let record = node(&state, inode)?;
            if record.entry.kind != NodeKind::Directory {
                return Err(Error::NotDirectory.into());
            }
            if !record.attached {
                return Err(Error::Stale.into());
            }
            if record.names.is_none() {
                self.mutation_gate.open()?;
                self.directory(&mut state, inode, Access::Read).await?;
            }
            (state.nodes[&inode].entry.clone(), state.generation, binding)
        };
        binding
            .gate
            .authorize(&entry.authority, Access::Read)
            .await?;
        Ok((entry, generation, binding))
    }
    async fn directory(&self, state: &mut State, inode: u64, access: Access) -> Result<()> {
        let binding = self.current_binding_locked(state)?;
        let record = node(state, inode)?;
        if record.entry.kind != NodeKind::Directory {
            return Err(Error::NotDirectory.into());
        }
        if !record.attached {
            return Err(Error::Stale.into());
        }
        binding
            .gate
            .authorize(&record.entry.authority, access)
            .await?;
        if record.names.is_some() {
            return Ok(());
        }
        self.mutation_gate.open()?;
        let key = record.entry.authority;
        let mut entries = Vec::new();
        let mut offset = 0;
        loop {
            let page = binding
                .backend
                .list(&key, offset, MAX_DIRECTORY_ENTRIES)
                .await?;
            if page.entries.len() > MAX_DIRECTORY_ENTRIES {
                return Err(Error::Io.into());
            }
            let next = offset
                .checked_add(page.entries.len() as u64)
                .ok_or(Error::Io)?;
            if page
                .next_offset
                .is_some_and(|value| value != next || value == offset)
            {
                return Err(Error::Io.into());
            }
            entries.extend(page.entries);
            if entries.len() > self.limits.max_directory_entries {
                return Err(Error::ResourceLimit.into());
            }
            if page.next_offset.is_none() {
                break;
            }
            offset = next;
        }
        let mut names = BTreeSet::new();
        let mut new_keys = BTreeSet::new();
        let mut listed_keys = BTreeSet::new();
        for named in &entries {
            if named.node.kind == NodeKind::Symlink {
                return Err(Error::Unsupported.into());
            }
            if !listed_keys.insert(named.node.key) || named.node.key == key {
                return Err(Error::Io.into());
            }
            let folded = name(&named.name)?;
            if !names.insert(folded) {
                return Err(Error::Io.into());
            }
            if let Some(&id) = state.backend_ids.get(&named.node.key) {
                let record = &state.nodes[&id];
                let old = &record.entry;
                if !record.attached || old.parent != inode || old.base.as_ref() != Some(&named.node)
                {
                    return Err(Error::Io.into());
                }
            } else if !new_keys.insert(named.node.key) {
                return Err(Error::Io.into());
            }
        }
        if state.nodes.len().saturating_add(new_keys.len()) > self.limits.max_inodes
            || state
                .next_inode
                .checked_add(new_keys.len() as u64)
                .is_none()
        {
            return Err(Error::ResourceLimit.into());
        }
        let _mutation = self.mutation_gate.mutation()?;
        let mut names = BTreeMap::new();
        for named in entries {
            let id = if let Some(&id) = state.backend_ids.get(&named.node.key) {
                id
            } else {
                let id = state.next_inode;
                state.next_inode += 1;
                state.backend_ids.insert(named.node.key, id);
                state.nodes.insert(
                    id,
                    Record {
                        receipt: receipt_cell(),
                        entry: Entry {
                            inode: id,
                            parent: inode,
                            kind: named.node.kind,
                            authority: named.node.key,
                            base: Some(named.node),
                        },
                        names: None,
                        file: None,
                        attached: true,
                        retired_binding: None,
                    },
                );
                id
            };
            names.insert(name(&named.name)?, (named.name, id));
        }
        state.nodes.get_mut(&inode).unwrap().names = Some(names);
        Ok(())
    }
    pub async fn lookup(&self, parent: u64, component: &[u8]) -> Result<Entry> {
        let folded = name(component)?;
        let (directory, generation, binding) = self.readonly_directory(parent).await?;
        let entry = {
            let state = self.state.lock().await;
            self.confirms_binding(&binding)?;
            let record = node(&state, parent)?;
            if state.generation != generation || record.entry != directory {
                return Err(Error::Stale.into());
            }
            let id = record
                .names
                .as_ref()
                .ok_or(Error::Busy)?
                .get(&folded)
                .ok_or(Error::NotFound)?
                .1;
            node(&state, id)?.entry.clone()
        };
        binding
            .gate
            .authorize(&entry.authority, Access::Read)
            .await?;
        Ok(entry)
    }
    pub async fn list(&self, directory: u64, cookie: Option<Cookie>, limit: usize) -> Result<Page> {
        if limit == 0 || limit > MAX_DIRECTORY_ENTRIES {
            return Err(Error::InvalidInput.into());
        }
        let (entry, generation, binding) = self.readonly_directory(directory).await?;
        let state = self.state.lock().await;
        self.confirms_binding(&binding)?;
        let record = node(&state, directory)?;
        if state.generation != generation || record.entry != entry {
            return Err(Error::Stale.into());
        }
        let offset = match cookie {
            Some(cookie) if cookie.generation == generation => cookie.offset,
            Some(_) => return Err(Error::Stale.into()),
            None => 0,
        };
        let names = record.names.as_ref().ok_or(Error::Busy)?;
        if offset > names.len() {
            return Err(Error::InvalidInput.into());
        }
        let entries = names
            .values()
            .skip(offset)
            .take(limit)
            .map(|(name, id)| NamedEntry {
                name: name.clone(),
                entry: state.nodes[id].entry.clone(),
            })
            .collect::<Vec<_>>();
        let end = offset + entries.len();
        Ok(Page {
            entries,
            next: (end < names.len()).then_some(Cookie {
                generation,
                offset: end,
            }),
        })
    }
    pub async fn directory_snapshot(&self, inode: u64) -> Result<DirectorySnapshot> {
        self.directory_snapshot_bounded(inode, self.limits.max_directory_entries)
            .await
    }
    /// Clone a bounded stable directory before authorizing its retained entries;
    /// no namespace lock is held across policy or backend work.
    pub async fn directory_snapshot_bounded(
        &self,
        inode: u64,
        max_entries: usize,
    ) -> Result<DirectorySnapshot> {
        Ok(self
            .pinned_directory_snapshot_bounded(inode, max_entries)
            .await?
            .into_snapshot())
    }
    /// Retain the effective read gate with the same bounded metadata capture.
    /// Existing handles reauthorize saved keys without consulting live inodes.
    pub(crate) async fn pinned_directory_snapshot_bounded(
        &self,
        inode: u64,
        max_entries: usize,
    ) -> Result<PinnedDirectorySnapshot> {
        let (entry, generation, binding) = self.readonly_directory(inode).await?;
        let pinned = {
            let state = self.state.lock().await;
            self.confirms_binding(&binding)?;
            let record = node(&state, inode)?;
            if state.generation != generation || record.entry != entry {
                return Err(Error::Stale.into());
            }
            let names = record.names.as_ref().ok_or(Error::Busy)?;
            if names.len() > self.limits.max_directory_entries || names.len() > max_entries {
                return Err(Error::ResourceLimit.into());
            }
            let parent = node(&state, entry.parent)?.entry.clone();
            let entries = names
                .values()
                .map(|(name, id)| {
                    Ok(NamedEntry {
                        name: name.clone(),
                        entry: node(&state, *id)?.entry.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            PinnedDirectorySnapshot::new(
                DirectorySnapshot {
                    entry,
                    parent,
                    generation,
                    entries,
                },
                binding.gate.clone(),
            )
        };
        pinned.authorize_position(1).await?;
        for position in 0..pinned.snapshot().entries.len() {
            pinned.authorize_position(position + 2).await?;
        }
        Ok(pinned)
    }
    /// Reuse a retained overlay. A sealed cache miss is refused before external
    /// store allocation; existing handles and cached overlays remain readable.
    pub async fn materialized_file(&self, inode: u64) -> Result<Option<Arc<OverlayFile>>> {
        let (entry, file, binding) = {
            let state = self.state.lock().await;
            let binding = self.current_binding_locked(&state)?;
            let record = node(&state, inode)?;
            if record.entry.kind != NodeKind::File {
                return Err(Error::IsDirectory.into());
            }
            if record.file.is_none() {
                self.mutation_gate.open()?;
            }
            (
                record.entry.clone(),
                record.file.clone(),
                Self::record_binding(record, &binding)?,
            )
        };
        binding
            .gate
            .authorize(&entry.authority, Access::Read)
            .await?;
        Ok(file)
    }
    pub async fn create_directory(&self, parent: u64, component: &[u8]) -> Result<Entry> {
        self.create(parent, component, None).await
    }
    /// `store` is fresh exclusive storage; all returned handles share this overlay.
    pub async fn create_file(
        &self,
        parent: u64,
        component: &[u8],
        mode: u16,
        store: Arc<dyn SparseStore>,
        limits: FileLimits,
    ) -> Result<Entry> {
        self.create(parent, component, Some((mode, store, limits)))
            .await
    }
    async fn create(
        &self,
        parent: u64,
        component: &[u8],
        file: Option<(u16, Arc<dyn SparseStore>, FileLimits)>,
    ) -> Result<Entry> {
        let folded = name(component)?;
        let mut state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if self.operation_freeze.frozen() {
            return Err(NamespaceError::Busy);
        }
        self.directory(&mut state, parent, Access::Write).await?;
        let names = state.nodes[&parent].names.as_ref().unwrap();
        if names.contains_key(&folded) {
            return Err(NamespaceError::Exists);
        }
        if names.len() >= self.limits.max_directory_entries
            || state.nodes.len() >= self.limits.max_inodes
        {
            return Err(Error::ResourceLimit.into());
        }
        let next = state
            .next_inode
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let authority = state.nodes[&parent].entry.authority;
        let receipt = receipt_cell();
        let file = file
            .map(|(mode, store, limits)| {
                OverlayFile::empty(
                    authority,
                    mode,
                    binding.backend.clone(),
                    binding.gate.clone(),
                    store,
                    limits,
                )
                .map(|file| {
                    Arc::new(
                        file.with_mutation_gate(self.mutation_gate.clone())
                            .with_operation_freeze(self.operation_freeze.clone())
                            .with_receipt_cell(receipt.clone()),
                    )
                })
            })
            .transpose()?;
        let entry = Entry {
            inode: state.next_inode,
            parent,
            authority,
            kind: if file.is_some() {
                NodeKind::File
            } else {
                NodeKind::Directory
            },
            base: None,
        };
        let _mutation = self.mutation_gate.mutation()?;
        state
            .nodes
            .get_mut(&parent)
            .unwrap()
            .names
            .as_mut()
            .unwrap()
            .insert(folded, (component.to_vec(), entry.inode));
        state.nodes.insert(
            entry.inode,
            Record {
                receipt,
                entry: entry.clone(),
                names: (file.is_none()).then(BTreeMap::new),
                file,
                attached: true,
                retired_binding: None,
            },
        );
        state.next_inode = next;
        state.generation = generation;
        Ok(entry)
    }
    /// Retained overlays survive unlink/replacement. Handle checks and ACL rechecks
    /// are performed by `OverlayFile`; this method never opens a handle itself.
    /// Lazy detached inodes retain their original reader and effective policy.
    pub async fn file(
        &self,
        inode: u64,
        store: Arc<dyn SparseStore>,
        limits: FileLimits,
    ) -> Result<Arc<OverlayFile>> {
        if let Some(file) = self.materialized_file(inode).await? {
            return Ok(file);
        }
        let mut state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        self.mutation_gate.open()?;
        let record = node(&state, inode)?;
        if record.entry.kind != NodeKind::File {
            return Err(Error::IsDirectory.into());
        }
        let binding = Self::record_binding(record, &binding)?;
        binding
            .gate
            .authorize(&record.entry.authority, Access::Read)
            .await?;
        if let Some(file) = &record.file {
            return Ok(file.clone());
        }
        let file = Arc::new(
            OverlayFile::new(
                record.entry.base.clone().ok_or(Error::Io)?,
                binding.backend.clone(),
                binding.gate.clone(),
                store,
                limits,
            )?
            .with_mutation_gate(self.mutation_gate.clone())
            .with_operation_freeze(self.operation_freeze.clone())
            .with_receipt_cell(record.receipt.clone()),
        );
        let _mutation = self.mutation_gate.mutation()?;
        state.nodes.get_mut(&inode).unwrap().file = Some(file.clone());
        Ok(file)
    }
    pub async fn remove(&self, parent: u64, component: &[u8], directory: bool) -> Result<()> {
        let folded = name(component)?;
        let mut state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if self.operation_freeze.frozen() {
            return Err(NamespaceError::Busy);
        }
        self.directory(&mut state, parent, Access::Write).await?;
        let id = state.nodes[&parent]
            .names
            .as_ref()
            .unwrap()
            .get(&folded)
            .ok_or(Error::NotFound)?
            .1;
        self.removable(&mut state, parent, id, directory).await?;
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let _mutation = self.mutation_gate.mutation()?;
        state
            .nodes
            .get_mut(&parent)
            .unwrap()
            .names
            .as_mut()
            .unwrap()
            .remove(&folded);
        detach(&mut state, id, &binding);
        state.generation = generation;
        Ok(())
    }
    async fn removable(
        &self,
        state: &mut State,
        parent: u64,
        id: u64,
        directory: bool,
    ) -> Result<()> {
        let binding = self.current_binding_locked(state)?;
        let entry = &state.nodes[&id].entry;
        if !same_owner(state.nodes[&parent].entry.authority, entry.authority) {
            return Err(NamespaceError::CrossBoundary);
        }
        binding
            .gate
            .authorize(&entry.authority, Access::Write)
            .await?;
        if directory != (entry.kind == NodeKind::Directory) {
            return Err(if directory {
                Error::NotDirectory
            } else {
                Error::IsDirectory
            }
            .into());
        }
        if directory {
            self.directory(state, id, Access::Write).await?;
            if !state.nodes[&id].names.as_ref().unwrap().is_empty() {
                return Err(NamespaceError::NotEmpty);
            }
        }
        Ok(())
    }
    pub async fn rename(
        &self,
        parent: u64,
        component: &[u8],
        destination: u64,
        new_component: &[u8],
    ) -> Result<()> {
        let source_name = name(component)?;
        let target_name = name(new_component)?;
        let mut state = self.state.lock().await;
        let binding = self.current_binding_locked(&state)?;
        if self.operation_freeze.frozen() {
            return Err(NamespaceError::Busy);
        }
        self.directory(&mut state, parent, Access::Write).await?;
        self.directory(&mut state, destination, Access::Write)
            .await?;
        let id = state.nodes[&parent]
            .names
            .as_ref()
            .unwrap()
            .get(&source_name)
            .ok_or(Error::NotFound)?
            .1;
        let entry = state.nodes[&id].entry.clone();
        if !same_owner(entry.authority, state.nodes[&parent].entry.authority)
            || !same_owner(entry.authority, state.nodes[&destination].entry.authority)
        {
            return Err(NamespaceError::CrossBoundary);
        }
        binding
            .gate
            .authorize(&entry.authority, Access::Write)
            .await?;
        let mut ancestor = destination;
        for _ in 0..state.nodes.len() {
            if ancestor == id {
                return Err(NamespaceError::Cycle);
            }
            let next = state.nodes[&ancestor].entry.parent;
            if ancestor == next {
                break;
            }
            ancestor = next;
        }
        let target = state.nodes[&destination]
            .names
            .as_ref()
            .unwrap()
            .get(&target_name)
            .map(|(_, id)| *id);
        if let Some(target) = target.filter(|&target| target != id) {
            self.removable(
                &mut state,
                destination,
                target,
                entry.kind == NodeKind::Directory,
            )
            .await?;
        }
        if target.is_none()
            && parent != destination
            && state.nodes[&destination].names.as_ref().unwrap().len()
                >= self.limits.max_directory_entries
        {
            return Err(Error::ResourceLimit.into());
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let _mutation = self.mutation_gate.mutation()?;
        state
            .nodes
            .get_mut(&parent)
            .unwrap()
            .names
            .as_mut()
            .unwrap()
            .remove(&source_name);
        state
            .nodes
            .get_mut(&destination)
            .unwrap()
            .names
            .as_mut()
            .unwrap()
            .insert(target_name, (new_component.to_vec(), id));
        if let Some(target) = target.filter(|&target| target != id) {
            detach(&mut state, target, &binding);
        }
        state.nodes.get_mut(&id).unwrap().entry.parent = destination;
        state.generation = generation;
        Ok(())
    }
}
#[derive(Clone)]
pub(crate) struct FrozenFile {
    pub mutable: bool,
    pub entry: Entry,
    pub size: u64,
    pub mode: u16,
    pub generation: u64,
}
/// Exclusive mount-wide operation lease. No credentials or open handles are
/// captured; every read continues to recheck its full owning exposure.
pub struct FrozenNamespace {
    lease: Arc<FreezeLease>,
}
struct FreezeLease {
    namespace: Arc<Namespace>,
    token: u64,
    // Read-only ownership identity survives every pin without retaining a Model.
    _lineage: Option<crate::publication_ownership::Lineage>,
}
impl FrozenNamespace {
    pub(crate) fn same_acquisition(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.lease, &other.lease)
    }
    pub(crate) fn token(&self) -> u64 {
        self.lease.token
    }
    pub fn namespace(&self) -> &Arc<Namespace> {
        &self.lease.namespace
    }
    pub(crate) fn pin(&self) -> Self {
        Self {
            lease: self.lease.clone(),
        }
    }
    pub(crate) fn valid(&self) -> bool {
        self.lease
            .namespace
            .operation_freeze
            .matches(self.lease.token)
    }
}
impl Drop for FreezeLease {
    fn drop(&mut self) {
        self.namespace.operation_freeze.release(self.token);
    }
}

fn node(state: &State, id: u64) -> Result<&Record> {
    state.nodes.get(&id).ok_or(Error::NotFound.into())
}
fn detach(state: &mut State, inode: u64, binding: &Arc<NamespaceBinding>) {
    let record = state.nodes.get_mut(&inode).unwrap();
    if record.retired_binding.is_none() {
        record.retired_binding = Some(binding.clone());
    }
    record.attached = false;
    if let Some(base) = &record.entry.base
        && state.backend_ids.get(&base.key) == Some(&inode)
    {
        state.backend_ids.remove(&base.key);
    }
}
fn same_exposure(a: NodeKey, b: NodeKey) -> bool {
    a.repository == b.repository
        && a.exposure == b.exposure
        && a.revision.context == b.revision.context
}
fn same_owner(a: NodeKey, b: NodeKey) -> bool {
    a.repository == b.repository && a.revision == b.revision && a.exposure == b.exposure
}
fn name(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty()
        || bytes.len() > 255
        || bytes == b"."
        || bytes == b".."
        || bytes.iter().any(|b| matches!(b, b'/' | b'\\' | 0))
    {
        return Err(Error::InvalidInput.into());
    }
    let text = std::str::from_utf8(bytes).map_err(|_utf8_error| Error::InvalidInput)?;
    if lore_revision::repository::is_reserved_node_name(text) {
        return Err(Error::InvalidInput.into());
    }
    Ok(text.to_lowercase().into_bytes())
}
#[cfg(test)]
mod tests;
