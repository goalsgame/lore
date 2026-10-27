// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Bounded, non-publishing preparation of one explicitly selected owning scope.
//! Candidates retain their source freeze and private dirty-byte snapshot. They
//! neither change a live namespace nor write instance anchors or recovery intent.
//! `KeepLocal` preserves edits only when the resulting parent/type structure is
//! valid; ambiguous ancestor replacements and forced renames are refused.

use std::collections::BTreeMap;
use std::sync::Arc;

use lore_base::types::Address;

use crate::core;
use crate::core::HeldBaseline;
use crate::core::NodeKey;
use crate::core::ScopeHead;
use crate::core::ScopeInfo;
use crate::core::ScopePin;
use crate::namespace::FrozenNamespace;
use crate::namespace::Namespace;
use crate::namespace::NamespaceError;
use crate::namespace::NamespaceSnapshot;
use crate::revision::RevisionBackend;

mod candidate_edits;
mod capture_footprint;
mod installed_source;
mod inventory;
mod owned_source;
pub(crate) use owned_source::OwnedSourceImage;
pub(crate) use owned_source::SourceFacts;
pub(crate) use owned_source::SourceSelection;
mod rename_origins;
use inventory::inventory;
#[allow(
    dead_code,
    reason = "Complete routing preparation does not install a namespace."
)]
mod complete_backend;
mod install_layout;
pub(crate) use install_layout::PreparedNamespaceLayout;
pub(crate) use install_layout::owned::OwnedLayout;
mod target_view;
pub use target_view::PreparedTargetView;
mod seal;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Aggregate metadata visits and retained records, including source snapshots.
    pub max_nodes: usize,
    /// Aggregate spelling buffers, reserving Unicode folding expansion.
    pub max_name_bytes: usize,
    /// Aggregate paths, copies and encoded exposure provenance buffers.
    pub max_path_bytes: usize,
    pub max_depth: usize,
    pub max_private_data_bytes: usize,
    pub max_comparisons: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_nodes: 262_144,
            max_name_bytes: 16 * 1024 * 1024,
            max_path_bytes: 64 * 1024 * 1024,
            max_depth: 128,
            max_private_data_bytes: 64 * 1024 * 1024,
            max_comparisons: 262_144,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LocalPolicy {
    #[default]
    RejectConflicts,
    KeepLocal,
    OverwriteLocal,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    pub local: LocalPolicy,
    pub dry_run: bool,
    pub reset: bool,
    pub view_changed: bool,
    pub dependency_restricted: bool,
    pub layers: bool,
}
#[derive(Debug)]
pub enum Error {
    Backend(core::Error),
    Namespace(NamespaceError),
    Unsupported(&'static str),
    InvalidScope,
    StaleSource,
    Conflicts(Vec<Conflict>),
}

impl From<core::Error> for Error {
    fn from(error: core::Error) -> Self {
        Self::Backend(error)
    }
}
impl From<NamespaceError> for Error {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictKind {
    Content,
    Structure,
    Rename,
    ChildProjection,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    pub path: Vec<u8>,
    pub kind: ConflictKind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Directory,
    File,
    Link,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeSummary {
    pub kind: Kind,
    pub size: u64,
    pub executable: bool,
    /// Known immutable identity, never inferred by hashing dirty bytes.
    pub immutable: Option<Address>,
    pub link: Option<ScopePin>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    TakeTarget,
    KeepLocal,
}
#[derive(Clone, Debug)]
pub struct Change {
    pub path: Vec<u8>,
    pub before: Option<NodeSummary>,
    pub after: Option<NodeSummary>,
    pub disposition: Disposition,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateOrigin {
    pub revision: Address,
    pub node: u32,
    pub local: Option<(u64, u64)>,
}

/// An unpublished specification. Future publication requires a separate normal
/// instance/staged-anchor protocol and durable recovery intent before any swap.
pub struct IntentSpec {
    dry_run: bool,
    original_root: NodeKey,
    scope: NodeKey,
    expected: Option<HeldBaseline>,
    target: ScopeHead,
    namespace_generation: u64,
    next_inode: u64,
    file_generations: Vec<(u64, u64)>,
    protected_children: Vec<HeldBaseline>,
}
impl IntentSpec {
    pub fn dry_run(&self) -> bool {
        self.dry_run
    }
    pub fn original_root(&self) -> NodeKey {
        self.original_root
    }
    pub fn scope(&self) -> NodeKey {
        self.scope
    }
    pub fn expected_baseline(&self) -> Option<&HeldBaseline> {
        self.expected.as_ref()
    }
    pub fn target(&self) -> &ScopeHead {
        &self.target
    }
    pub fn namespace_generation(&self) -> u64 {
        self.namespace_generation
    }
    pub fn next_inode(&self) -> u64 {
        self.next_inode
    }
    pub fn file_generations(&self) -> &[(u64, u64)] {
        &self.file_generations
    }
    pub fn protected_children(&self) -> &[HeldBaseline] {
        &self.protected_children
    }
}

pub struct PreparedTransition {
    candidate_active: std::sync::atomic::AtomicBool,
    source_epoch: u64,
    source_routing: crate::namespace::operation_routing::OperationRouting,
    freeze: FrozenNamespace,
    snapshot: NamespaceSnapshot,
    limits: Limits,
    intent: IntentSpec,
    changes: Vec<Change>,
    conflicts: Vec<Conflict>,
    candidate: BTreeMap<String, Metadata>,
}
impl PreparedTransition {
    pub fn intent(&self) -> &IntentSpec {
        &self.intent
    }
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }
    pub fn conflicts(&self) -> &[Conflict] {
        &self.conflicts
    }
    pub fn candidate_len(&self) -> usize {
        self.candidate.len()
    }
    pub fn candidate_node(&self, path: &str) -> Option<&NodeSummary> {
        if path.len()
            > self
                .limits
                .max_path_bytes
                .min(self.limits.max_depth.saturating_mul(1001))
        {
            return None;
        }
        self.candidate
            .get(&path.to_lowercase())
            .map(|entry| &entry.summary)
    }
    pub fn candidate_origin(&self, path: &str) -> Option<CandidateOrigin> {
        if path.len()
            > self
                .limits
                .max_path_bytes
                .min(self.limits.max_depth.saturating_mul(1001))
        {
            return None;
        }
        self.candidate
            .get(&path.to_lowercase())
            .map(|entry| CandidateOrigin {
                revision: entry.revision,
                node: entry.node,
                local: entry.local,
            })
    }
    /// Recheck the exact frozen source, including cache enrichment and current
    /// authorization. This is not a publication capability or durable intent.
    pub async fn validate_source(&self) -> Result<()> {
        self.source_routing
            .check(self.freeze.namespace(), self.freeze.token())?;
        if !self.freeze.valid() {
            return Err(Error::StaleSource);
        }
        if self.freeze.namespace().mutation_epoch()? != self.source_epoch {
            return Err(Error::StaleSource);
        }
        let mut budget = Budget::new(self.limits)?;
        let now = self
            .freeze
            .namespace()
            .transition_snapshot(self.freeze.token(), &mut budget)
            .await?;
        let expected = &self.snapshot;
        if now.generation != expected.generation
            || self.freeze.namespace().mutation_epoch()? != self.source_epoch
            || now.original_root != expected.original_root
            || now.active_underlays != expected.active_underlays
            || now.next_inode != expected.next_inode
            || now.files != expected.files
            || now.baselines != expected.baselines
            || now.backend_state != expected.backend_state
            || now.nodes.len() != expected.nodes.len()
            || now.nodes.iter().zip(&expected.nodes).any(|(left, right)| {
                left.entry != right.entry
                    || left.names != right.names
                    || left.attached != right.attached
                    || left.receipt != right.receipt
            })
        {
            return Err(Error::StaleSource);
        }
        self.source_routing
            .check(self.freeze.namespace(), self.freeze.token())?;
        Ok(())
    }
}

pub(crate) struct Budget {
    pub(crate) limits: Limits,
    nodes: usize,
    names: usize,
    paths: usize,
    comparisons: usize,
}
impl Budget {
    pub(crate) fn new(limits: Limits) -> core::Result<Self> {
        if limits.max_nodes == 0
            || limits.max_name_bytes == 0
            || limits.max_path_bytes == 0
            || limits.max_depth == 0
            || limits.max_depth > 1024
        {
            return Err(core::Error::InvalidInput);
        }
        Ok(Self {
            limits,
            nodes: 0,
            names: 0,
            paths: 0,
            comparisons: 0,
        })
    }
    pub(crate) fn node(&mut self) -> core::Result<()> {
        charge(&mut self.nodes, 1, self.limits.max_nodes)
    }
    pub(crate) fn remaining_nodes(&self) -> usize {
        self.limits.max_nodes.saturating_sub(self.nodes)
    }
    pub(crate) fn names(&mut self, bytes: usize) -> core::Result<()> {
        charge(&mut self.names, bytes, self.limits.max_name_bytes)
    }
    pub(crate) fn paths(&mut self, bytes: usize) -> core::Result<()> {
        charge(&mut self.paths, bytes, self.limits.max_path_bytes)
    }
    pub(crate) fn depth_limit(&self) -> usize {
        self.limits.max_depth
    }
    pub(crate) fn depth(&self, depth: usize) -> core::Result<()> {
        if depth > self.limits.max_depth {
            Err(core::Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
    fn compare(&mut self) -> core::Result<()> {
        charge(&mut self.comparisons, 1, self.limits.max_comparisons)
    }
}
fn charge(counter: &mut usize, bytes: usize, maximum: usize) -> core::Result<()> {
    *counter = counter
        .checked_add(bytes)
        .filter(|total| *total <= maximum)
        .ok_or(core::Error::ResourceLimit)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    pub(crate) path: String,
    pub(crate) summary: NodeSummary,
    /// Exact immutable base identity. Local-only access anchors are not node
    /// identities; this field grants neither membership nor authorization.
    pub(crate) identity: Option<NodeKey>,
    pub(crate) node: u32,
    pub(crate) flags: u16,
    pub(crate) local: Option<(u64, u64)>,
    pub(crate) revision: Address,
    pub(crate) uncertain: bool,
}
pub(crate) type Image = BTreeMap<String, Metadata>;
pub(crate) struct ScopeView {
    pub(crate) state: Arc<lore_revision::state::State>,
    pub(crate) proof: Arc<crate::overlay::ProvenTargetMetadata>,
    pub(crate) info: ScopeInfo,
    pub(crate) head: ScopeHead,
    pub(crate) entries: crate::revision::VerifiedImage,
    pub(crate) committed: bool,
}

fn within(parent: &str, child: &str) -> bool {
    parent.is_empty()
        || parent == child
        || child
            .strip_prefix(parent)
            .is_some_and(|tail| tail.starts_with('/'))
}
fn overlaps(left: &str, right: &str) -> bool {
    within(left, right) || within(right, left)
}
fn copy_at(source: &Metadata, path: &str, budget: &mut Budget) -> core::Result<Metadata> {
    budget.node()?;
    budget.paths(
        path.len()
            .checked_mul(8)
            .ok_or(core::Error::ResourceLimit)?,
    )?;
    Ok(Metadata {
        path: path.to_owned(),
        summary: source.summary.clone(),
        identity: source.identity,
        node: source.node,
        flags: source.flags,
        local: source.local,
        revision: source.revision,
        uncertain: source.uncertain,
    })
}
fn insert(image: &mut Image, metadata: Metadata) -> core::Result<()> {
    if image
        .insert(metadata.path.to_lowercase(), metadata)
        .is_some()
    {
        return Err(core::Error::InvalidInput);
    }
    Ok(())
}
fn join(parent: &str, name: &str, budget: &mut Budget) -> core::Result<String> {
    let length = parent
        .len()
        .checked_add(usize::from(!parent.is_empty() && !name.is_empty()))
        .and_then(|length| length.checked_add(name.len()))
        .ok_or(core::Error::ResourceLimit)?;
    budget.paths(length.checked_mul(8).ok_or(core::Error::ResourceLimit)?)?;
    Ok(if parent.is_empty() {
        name.to_owned()
    } else if name.is_empty() {
        parent.to_owned()
    } else {
        format!("{parent}/{name}")
    })
}

fn same(left: &Metadata, right: &Metadata, local: bool) -> bool {
    let flags = lore_revision::node::NodeFlags::from_bits_retain(right.flags);
    left.path == right.path
        && left.summary == right.summary
        && (!local || !right.uncertain)
        && (!local
            || right.summary.kind == Kind::Directory
            || !flags.intersects(
                lore_revision::node::NodeFlags::Dirty | lore_revision::node::NodeFlags::Staged,
            ))
}
fn differences(
    base: &Image,
    changed: &Image,
    local: bool,
    budget: &mut Budget,
) -> core::Result<Vec<String>> {
    let mut keys = std::collections::BTreeSet::new();
    for key in base.keys().chain(changed.keys()) {
        budget.node()?;
        keys.insert(key.as_str());
    }
    let mut result = Vec::new();
    for key in keys {
        if matches!((base.get(key), changed.get(key)), (Some(left), Some(right)) if same(left, right, local))
        {
            continue;
        }
        budget.paths(key.len())?;
        result.push(key.to_owned());
    }
    Ok(result)
}

/// Prepare one owning exposure against its logical held head. No live mutation
/// or journal publication is available from the returned candidate.
pub async fn prepare(
    namespace: Arc<Namespace>,
    scope: NodeKey,
    target: Address,
    options: Options,
    limits: Limits,
) -> Result<PreparedTransition> {
    use crate::core::Backend;
    if options.reset {
        return Err(Error::Unsupported("reset transitions"));
    }
    if options.view_changed {
        return Err(Error::Unsupported("view transitions"));
    }
    if options.dependency_restricted {
        return Err(Error::Unsupported("dependency-restricted transitions"));
    }
    if options.layers {
        return Err(Error::Unsupported("layer transitions"));
    }
    let mut budget = Budget::new(limits)?;
    let freeze = namespace.freeze_operation().await?;
    let source_routing = namespace.capture_operation_routing(&freeze).await?;
    let backend_owner = source_routing.backend().clone();
    let backend = backend_owner
        .as_any()
        .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
        .ok_or(Error::Unsupported("scoped revision metadata reader"))?;
    source_routing
        .authorize_source(&namespace, freeze.token(), &scope)
        .await?;
    let (snapshot, source_epoch) = namespace
        .transition_snapshot_epoch(freeze.token(), &mut budget)
        .await?;
    let root = snapshot.original_root.key;
    let policy = source_routing.policy();
    let inode =
        installed_source::selected_inode(&snapshot, backend, scope, &policy, &mut budget).await?;
    let origin = backend
        .transition_view(scope, scope.revision, policy.clone(), &mut budget)
        .await?;
    let expected = snapshot.baselines.get(&scope).cloned();
    let current_revision = expected
        .as_ref()
        .map_or(origin.info.initial_revision, |baseline| {
            baseline.head.revision
        });
    let current = backend
        .transition_view(scope, current_revision, policy.clone(), &mut budget)
        .await?;
    if expected
        .as_ref()
        .is_some_and(|baseline| baseline.head != current.head)
    {
        return Err(Error::InvalidScope);
    }
    validate_parent(
        backend,
        &snapshot,
        &origin.info,
        expected.as_ref(),
        policy.clone(),
        &mut budget,
    )
    .await?;
    let target = backend
        .transition_view(scope, target, policy.clone(), &mut budget)
        .await?;
    if !target.committed && target.head != current.head {
        return Err(Error::Unsupported("uncommitted transition target"));
    }
    let mut local = inventory(
        &snapshot,
        backend,
        scope,
        inode,
        policy.clone(),
        &mut budget,
    )
    .await?;
    if current.head.root.is_none()
        && local.len() == 1
        && local
            .get("")
            .is_some_and(|root| root.summary.kind == Kind::Directory)
    {
        local.clear();
    }
    let incoming = differences(&current.entries, &target.entries, false, &mut budget)?;
    let mut local_changes = differences(&current.entries, &local, true, &mut budget)?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut protected = Vec::new();
    let mut protected_paths = std::collections::BTreeSet::new();
    for baseline in snapshot.baselines.values() {
        budget.node()?;
        let Some(projection) = &baseline.parent_projection else {
            continue;
        };
        if projection.parent != scope {
            continue;
        }
        let link = backend
            .transition_parent_link(&baseline.scope)?
            .ok_or(Error::InvalidScope)?;
        let origin_link =
            find_key(&origin.entries, &link, &mut budget)?.ok_or(Error::InvalidScope)?;
        let actual = pin_at(&current.entries, &origin_link.path);
        if projection.head != current.head.revision || projection.target != actual {
            return Err(Error::StaleSource);
        }
        let child = baseline.head.root.map(|node| ScopePin {
            repository: baseline.scope.repository,
            revision: baseline.head.revision,
            node,
        });
        if child == projection.target {
            continue;
        }
        let local_link = find_key(&local, &link, &mut budget)?;
        let path = local_link.map_or(origin_link.path.as_str(), |link| link.path.as_str());
        budget.paths(
            path.len()
                .checked_mul(4)
                .ok_or(core::Error::ResourceLimit)?,
        )?;
        protected_paths.insert(path.to_lowercase());
        local_changes.insert(path.to_lowercase());
        protected.push(baseline.clone());
    }
    let mut conflicts = BTreeMap::new();
    let mut overlapping = std::collections::BTreeSet::new();
    let renamed = rename_endpoints(&current.entries, &local, &origin.entries, &mut budget)?;
    let renamed = rename_origins::resolve(
        &snapshot,
        &local,
        rename_origins::Views {
            original: &origin.entries,
            current: &current,
        },
        backend,
        &policy,
        renamed,
        &mut budget,
    )
    .await?;
    for local_key in &local_changes {
        for incoming_key in &incoming {
            budget.compare()?;
            if !overlaps(local_key, incoming_key) {
                continue;
            }
            let metadata = local
                .get(local_key)
                .or_else(|| current.entries.get(local_key))
                .or_else(|| origin.entries.get(local_key))
                .ok_or(Error::InvalidScope)?;
            let kind = if protected_paths.contains(local_key) || metadata.summary.kind == Kind::Link
            {
                ConflictKind::ChildProjection
            } else if renamed.endpoints.contains(local_key) {
                ConflictKind::Rename
            } else if local_key == incoming_key && metadata.summary.kind == Kind::File {
                ConflictKind::Content
            } else {
                ConflictKind::Structure
            };
            budget.paths(
                metadata
                    .path
                    .len()
                    .checked_add(local_key.len())
                    .and_then(|bytes| bytes.checked_mul(8))
                    .ok_or(core::Error::ResourceLimit)?,
            )?;
            conflicts.insert(
                local_key.clone(),
                Conflict {
                    path: metadata.path.as_bytes().to_vec(),
                    kind,
                },
            );
            overlapping.insert(local_key.clone());
        }
    }
    let conflicts = conflicts.into_values().collect::<Vec<_>>();
    if options.local == LocalPolicy::RejectConflicts && !conflicts.is_empty() {
        return Err(Error::Conflicts(conflicts));
    }
    if options.local == LocalPolicy::OverwriteLocal
        && (conflicts
            .iter()
            .any(|conflict| conflict.kind == ConflictKind::Rename)
            || renamed.unproven && !conflicts.is_empty())
    {
        return Err(Error::Unsupported("overwriting conflicted renames"));
    }
    let mut candidate = target.entries.into_image();
    for key in local_changes {
        if options.local == LocalPolicy::OverwriteLocal && overlapping.contains(&key) {
            continue;
        }
        match local.get(&key) {
            None => remove_subtree(&mut candidate, &key, &mut budget)?,
            Some(value) => {
                if value.summary.kind != Kind::Directory {
                    remove_subtree(&mut candidate, &key, &mut budget)?;
                }
                let copy = copy_at(value, &value.path, &mut budget)?;
                candidate.insert(key, copy);
            }
        }
    }
    validate_candidate(&candidate, &mut budget)?;
    let changed = differences(&local, &candidate, false, &mut budget)?;
    let mut changes = Vec::new();
    for key in changed {
        let before = local.get(&key);
        let after = candidate.get(&key);
        let path = after.or(before).ok_or(Error::InvalidScope)?.path.as_bytes();
        budget.paths(path.len())?;
        changes.push(Change {
            path: path.to_vec(),
            before: before.map(|node| node.summary.clone()),
            after: after.map(|node| node.summary.clone()),
            disposition: if after.is_some_and(|node| node.local.is_some()) {
                Disposition::KeepLocal
            } else {
                Disposition::TakeTarget
            },
        });
    }
    // Recheck both original and target policy after preparing the whole plan.
    backend.revalidate(&scope, false).await?;
    policy
        .authorize(&scope, crate::overlay::Access::Read)
        .await?;
    policy.authorize_target_head(&target.proof).await?;
    let intent = IntentSpec {
        dry_run: options.dry_run,
        original_root: root,
        scope,
        expected,
        target: target.head,
        namespace_generation: snapshot.generation,
        next_inode: snapshot.next_inode,
        file_generations: snapshot
            .files
            .iter()
            .map(|(&inode, file)| (inode, file.metadata.generation))
            .collect(),
        protected_children: protected,
    };
    source_routing.check(&namespace, freeze.token())?;
    Ok(PreparedTransition {
        source_routing,
        candidate_active: std::sync::atomic::AtomicBool::new(false),
        source_epoch,
        freeze,
        snapshot,
        limits,
        intent,
        changes,
        conflicts,
        candidate,
    })
}

fn pin_at(image: &Image, path: &str) -> Option<ScopePin> {
    image
        .get(&path.to_lowercase())
        .filter(|entry| entry.summary.kind == Kind::Link)
        .and_then(|entry| entry.summary.link.clone())
}
fn find_key<'a>(
    image: &'a Image,
    key: &NodeKey,
    budget: &mut Budget,
) -> core::Result<Option<&'a Metadata>> {
    let mut found = None;
    for value in image.values() {
        budget.compare()?;
        if value.identity == Some(*key) && found.replace(value).is_some() {
            return Err(core::Error::InvalidInput);
        }
    }
    Ok(found)
}

async fn validate_parent(
    backend: &RevisionBackend,
    snapshot: &NamespaceSnapshot,
    info: &ScopeInfo,
    expected: Option<&HeldBaseline>,
    policy: Arc<dyn crate::overlay::AccessGate>,
    budget: &mut Budget,
) -> Result<()> {
    let Some(parent) = &info.parent else {
        if expected.is_some_and(|baseline| baseline.parent_projection.is_some()) {
            return Err(Error::InvalidScope);
        }
        return Ok(());
    };
    let origin = backend
        .transition_view(parent.scope, parent.scope.revision, policy.clone(), budget)
        .await?;
    let head = snapshot
        .baselines
        .get(&parent.scope)
        .map_or(parent.initial_revision, |baseline| baseline.head.revision);
    let current = backend
        .transition_view(parent.scope, head, policy, budget)
        .await?;
    let link = find_key(&origin.entries, &parent.link, budget)?.ok_or(Error::InvalidScope)?;
    let actual = crate::core::ScopeProjection {
        parent: parent.scope,
        head,
        target: pin_at(&current.entries, &link.path),
    };
    if expected.is_some_and(|baseline| baseline.parent_projection.as_ref() != Some(&actual)) {
        return Err(Error::StaleSource);
    }
    Ok(())
}
fn rename_endpoints(
    base: &Image,
    local: &Image,
    origin: &Image,
    budget: &mut Budget,
) -> core::Result<std::collections::BTreeSet<String>> {
    let mut origins = BTreeMap::new();
    for source in origin.values() {
        budget.node()?;
        let Some(identity) = source.identity else {
            continue;
        };
        if origins.insert(identity, source).is_some() {
            return Err(core::Error::InvalidInput);
        }
    }
    let mut renamed = std::collections::BTreeSet::new();
    for (path, value) in local {
        budget.compare()?;
        if let Some(before) = base.get(path) {
            if before.path != value.path {
                budget.paths(path.len())?;
                renamed.insert(path.clone());
            }
            // Content/metadata changes at an existing spelling cannot be the
            // destination of the displaced-origin rename below.
            continue;
        }
        let Some(identity) = value.identity else {
            continue;
        };
        let Some(source) = origins.get(&identity) else {
            continue;
        };
        budget.paths(
            source
                .path
                .len()
                .checked_mul(4)
                .ok_or(core::Error::ResourceLimit)?,
        )?;
        let old = source.path.to_lowercase();
        if old != *path
            && base.contains_key(&old)
            && !local.contains_key(&old)
            && !base.contains_key(path)
        {
            budget.paths(path.len())?;
            renamed.insert(old);
            renamed.insert(path.clone());
        }
    }
    Ok(renamed)
}

#[cfg(test)]
mod tests;
fn remove_subtree(image: &mut Image, path: &str, budget: &mut Budget) -> core::Result<()> {
    if path.is_empty() {
        image.clear();
        return Ok(());
    }
    image.remove(path);
    budget.paths(
        path.len()
            .checked_add(1)
            .ok_or(core::Error::ResourceLimit)?,
    )?;
    let prefix = format!("{path}/");
    let mut removed = Vec::new();
    for key in image.range(prefix.clone()..).map(|(key, _)| key) {
        budget.compare()?;
        if !key.starts_with(&prefix) {
            break;
        }
        budget.paths(key.len())?;
        removed.push(key.clone());
    }
    for key in removed {
        image.remove(&key);
    }
    Ok(())
}
fn validate_candidate(image: &Image, budget: &mut Budget) -> Result<()> {
    for key in image.keys() {
        budget.compare()?;
        if key.is_empty() {
            continue;
        }
        let parent = key.rsplit_once('/').map_or("", |(parent, _)| parent);
        if !image
            .get(parent)
            .is_some_and(|parent| parent.summary.kind == Kind::Directory)
        {
            return Err(Error::Unsupported("conflicting ancestor/type merge"));
        }
    }
    Ok(())
}
