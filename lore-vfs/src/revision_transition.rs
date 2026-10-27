// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Counted scoped metadata reads. Links remain opaque; no immutable file payload
//! is read and no live namespace directory is materialized.

use std::collections::BTreeSet;

use super::*;
use crate::core::ParentScope;
use crate::core::ScopeHead;
use crate::core::ScopeInfo;
use crate::core::ScopePin;
use crate::overlay::Access as PolicyAccess;
use crate::overlay::AccessGate;
use crate::transition_prepare::Budget;
use crate::transition_prepare::Image;
use crate::transition_prepare::Kind;
use crate::transition_prepare::Metadata;
use crate::transition_prepare::NodeSummary;
use crate::transition_prepare::ScopeView;

/// Only the bounded reader constructs this map. Borrowing never permits
/// mutation; consuming it for a merge also consumes its completeness evidence.
pub(crate) struct VerifiedImage {
    entries: Image,
    proof: Arc<crate::overlay::ProvenTargetMetadata>,
}
impl std::ops::Deref for VerifiedImage {
    type Target = Image;
    fn deref(&self) -> &Image {
        &self.entries
    }
}
impl VerifiedImage {
    pub(super) fn matches(&self, proof: &Arc<crate::overlay::ProvenTargetMetadata>) -> bool {
        Arc::ptr_eq(&self.proof, proof)
    }
    pub(crate) fn into_image(self) -> Image {
        self.entries
    }
}
#[derive(Clone)]
pub(crate) struct MissingTargetPath {
    proof: Arc<crate::overlay::ProvenTargetMetadata>,
    path: Vec<u8>,
    parent: NodeKey,
    tail: Vec<u8>,
}
impl MissingTargetPath {
    pub(super) fn matches(
        &self,
        edit: &crate::overlay::CandidateEdit,
        parent: &NodeKey,
        tail: &[u8],
    ) -> bool {
        Arc::ptr_eq(&self.proof, &edit.proof)
            && self.path == edit.path()
            && self.parent == *parent
            && self.tail == tail
    }
}

impl RevisionBackend {
    /// Revalidate the exact originating source proof without reading the target
    /// revision. A selection preflight does not authorize any target node.
    pub async fn revalidate_selection_source(
        &self,
        selection: &crate::overlay::TargetSelection,
    ) -> Result<(), Error> {
        if !selection.belongs_to(self)
            || selection.requested.context != selection.source.root.revision.context
        {
            return Err(Error::PermissionDenied);
        }
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                self.revalidate(&selection.source.root, false).await?;
                if scopes::info(self, &selection.source.root).await? != selection.source {
                    return Err(Error::PermissionDenied);
                }
                self.revalidate(&selection.source.root, false).await
            })
            .await
    }
    pub(crate) fn seal_metadata(
        &self,
        epoch: u64,
    ) -> Result<crate::namespace::metadata_gate::Seal, Error> {
        // Namespace is locked by the caller. Owner mutations take owners then
        // gate too; no path acquires namespace/owners while holding the gate.
        let _owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        self.mutation_gate.seal(epoch)
    }
    pub(crate) fn shared_mutation_gate(
        &self,
    ) -> Arc<crate::namespace::metadata_gate::MutationGate> {
        self.mutation_gate.clone()
    }
    pub(crate) async fn restore_node_metadata(&self, key: &NodeKey) -> Result<Node, Error> {
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                let owner = self.owner(key)?;
                let node = checkpoint::scoped_node(&owner, key.node).await?;
                let metadata = describe(&owner, key.node, &node);
                self.owner(key)?;
                Ok(metadata)
            })
            .await
    }
    pub(crate) fn transition_execution(&self) -> Arc<lore_revision::interface::ExecutionContext> {
        self.execution.clone()
    }
    pub(crate) fn transition_allows_origin(
        &self,
        scope: NodeKey,
        key: &NodeKey,
    ) -> Result<(), Error> {
        self.owner(&scope)?;
        self.owner(key)?;
        let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        let mut exposure = key.exposure;
        for _ in 0..MAX_LINK_DEPTH {
            if exposure == scope.exposure {
                return Ok(());
            }
            exposure = owners
                .links
                .iter()
                .find_map(|((parent, _), child)| (*child == exposure).then_some(*parent))
                .ok_or(Error::PermissionDenied)?;
        }
        Err(Error::ResourceLimit)
    }

    pub(crate) async fn transition_fork(
        &self,
        scope: NodeKey,
        origin: &ScopeView,
        target: &ScopeView,
        budget: &mut Budget,
    ) -> Result<Option<Arc<RevisionBackend>>, Error> {
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                let Some(root_id) = target.head.root else {
                    return Ok(None);
                };
                self.transition_reserve_provenance(budget)?;
                let mut selected = self.owner(&scope)?;
                selected.state = target.state.clone();
                selected.source_root = root_id;
                let root_node = checkpoint::scoped_node(&selected, root_id).await?;
                let root = describe(&selected, root_id, &root_node);
                let mut by_exposure = BTreeMap::from([(scope.exposure, selected)]);
                let (links, children) = self
                    .transition_link_owners(scope, origin, target, budget)
                    .await?;
                for child in children {
                    by_exposure.insert(child.exposure, child);
                }
                Ok(Some(Arc::new(RevisionBackend {
                    selection_identity: Arc::new(()),
                    publication: Arc::new(crate::publication_ownership::Slot::default()),
                    mutation_gate: self.mutation_gate.clone(),
                    execution: self.execution.clone(),
                    owners: Mutex::new(Owners {
                        views: BTreeMap::new(),
                        bindings: BTreeMap::new(),
                        provenance: Vec::new(),
                        provenance_bytes: 0,
                        by_exposure,
                        links,
                        next: u64::MAX,
                        allocation: AllocationState::Prepared,
                        provenance_complete: false,
                    }),
                    root,
                    digest_index: self.digest_index.clone(),
                })))
            })
            .await
    }

    pub(crate) async fn transition_validate_target_links(
        &self,
        scope: NodeKey,
        origin: &ScopeView,
        target: &ScopeView,
        budget: &mut Budget,
    ) -> Result<(), Error> {
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                self.transition_reserve_provenance(budget)?;
                self.transition_link_owners(scope, origin, target, budget)
                    .await?;
                Ok(())
            })
            .await
    }
    pub(super) async fn transition_link_owners(
        &self,
        scope: NodeKey,
        origin: &ScopeView,
        target: &ScopeView,
        budget: &mut Budget,
    ) -> Result<(BTreeMap<(u64, NodeID), u64>, Vec<Owner>), Error> {
        let mut links = BTreeMap::new();
        let mut children = Vec::new();
        {
            let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
            for (path, node) in target.entries.iter() {
                if node.summary.kind != Kind::Link {
                    continue;
                }
                budget.node()?;
                let previous = origin
                    .entries
                    .get(path)
                    .filter(|previous| {
                        previous.path == node.path && previous.summary == node.summary
                    })
                    .ok_or(Error::Unsupported)?;
                let child = *owners
                    .links
                    .get(&(scope.exposure, previous.node))
                    .ok_or(Error::Unsupported)?;
                let owner = owners.by_exposure.get(&child).ok_or(Error::Stale)?.clone();
                let pin = node.summary.link.as_ref().ok_or(Error::Stale)?;
                if owner.repository.id != pin.repository
                    || Address::zero_context_hash(owner.state.revision()) != pin.revision
                    || owner.source_root != pin.node
                {
                    return Err(Error::Unsupported);
                }
                links.insert((scope.exposure, node.node), child);
                children.push(owner);
            }
        }
        for child in &children {
            budget.node()?;
            if checkpoint::scoped_node(child, child.source_root)
                .await?
                .is_link()
            {
                return Err(Error::Unsupported);
            }
        }
        Ok((links, children))
    }

    pub(crate) async fn candidate_missing_path(
        &self,
        target: &ScopeView,
        path: &[u8],
        parent: &Node,
        tail: &[u8],
    ) -> Result<MissingTargetPath, Error> {
        self.revalidate_target_head(&target.proof).await?;
        if target.proof.root().is_none()
            || !Arc::ptr_eq(&target.entries.proof, &target.proof)
            || !Arc::ptr_eq(&target.state, &target.proof.state)
            || target.head != target.proof.head()
        {
            return Err(Error::PermissionDenied);
        }
        let parts = candidate::path_parts(path, target.proof.max_depth)?;
        let tail_parts = candidate::path_parts(tail, target.proof.max_depth)?;
        if tail_parts.is_empty() {
            return Err(Error::InvalidInput);
        }
        let path_key = std::str::from_utf8(path)
            .map_err(|_encoding| Error::InvalidInput)?
            .to_lowercase();
        if target.entries.contains_key(&path_key) {
            return Err(Error::InvalidInput);
        }
        let mut key = path_key.as_str();
        loop {
            if key.is_empty() {
                return Err(Error::InvalidInput);
            }
            key = key.rsplit_once('/').map_or("", |(parent, _)| parent);
            if let Some(nearest) = target.entries.get(key) {
                if nearest.summary.kind != Kind::Directory {
                    return Err(Error::Unsupported);
                }
                let expected = NodeKey {
                    node: nearest.node,
                    revision: target.head.revision,
                    ..target.proof.selection().original_scope()
                };
                let depth = if key.is_empty() {
                    0
                } else {
                    key.split('/').count()
                };
                if parent.key != expected
                    || parent.kind != NodeKind::Directory
                    || parts.len() != depth + tail_parts.len()
                    || parts[depth..] != tail_parts
                {
                    return Err(Error::PermissionDenied);
                }
                return Ok(MissingTargetPath {
                    proof: target.proof.clone(),
                    path: path.to_vec(),
                    parent: parent.key,
                    tail: tail.to_vec(),
                });
            }
        }
    }

    pub(crate) fn transition_reserve_provenance(&self, budget: &mut Budget) -> Result<(), Error> {
        let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        for _ in owners.by_exposure.values() {
            budget.node()?;
        }
        for _ in 0..owners.views.len() + owners.bindings.len() + owners.provenance.len() {
            budget.node()?;
        }
        // Reserve encoded buffers before checkpoint capture allocates them.
        budget.paths(checkpoint::encoded_size(&owners)?)
    }
    pub(crate) async fn transition_view(
        &self,
        scope: NodeKey,
        revision: Address,
        policy: Arc<dyn AccessGate>,
        budget: &mut Budget,
    ) -> Result<ScopeView, Error> {
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                let owner = self.owner(&scope)?;
                if scope.node != owner.source_root || revision.context != scope.revision.context {
                    return Err(Error::InvalidInput);
                }
                policy.authorize(&scope, PolicyAccess::Read).await?;
                let parts = canonical_path(&owner, budget).await?;
                let source_length =
                    parts.iter().map(String::len).sum::<usize>() + parts.len().saturating_sub(1);
                if source_length > 32 * 1024 {
                    return Err(Error::ResourceLimit);
                }
                // ScopeView and its immutable proof each retain the route spelling.
                budget.paths(source_length.checked_mul(2).ok_or(Error::ResourceLimit)?)?;
                let source_path = parts.join("/").into_bytes();
                let parent = {
                    let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
                    owners
                        .links
                        .iter()
                        .find(|(_, child)| **child == scope.exposure)
                        .map(|((parent, link), _)| {
                            let parent = owners.by_exposure.get(parent).ok_or(Error::Io)?;
                            let parent_scope = NodeKey {
                                repository: parent.repository.id,
                                revision: Address::zero_context_hash(parent.state.revision()),
                                node: parent.source_root,
                                exposure: parent.exposure,
                            };
                            Ok(ParentScope {
                                scope: parent_scope,
                                initial_revision: Address::zero_context_hash(
                                    parent.state.committed_revision(),
                                ),
                                link: NodeKey {
                                    node: *link,
                                    ..parent_scope
                                },
                            })
                        })
                        .transpose()?
                };
                let info = ScopeInfo {
                    root: scope,
                    initial_revision: Address::zero_context_hash(owner.state.committed_revision()),
                    source_path,
                    parent,
                };
                let selection = crate::overlay::TargetSelection {
                    source: info.clone(),
                    requested: revision,
                    backend_identity: self.selection_identity.clone(),
                    execution: self.execution.clone(),
                };
                self.revalidate_selection_source(&selection).await?;
                policy.authorize_selection(&selection).await?;
                self.revalidate_selection_source(&selection).await?;
                let state = if revision == scope.revision {
                    owner.state.clone()
                } else {
                    let state = State::deserialize(owner.repository.clone(), revision.hash)
                        .await
                        .map_err(state_error)?;
                    if revision.hash != owner.state.committed_revision()
                        && state.revision_number() == 0
                    {
                        return Err(Error::InvalidInput);
                    }
                    state
                };
                let (selected, _anchor) =
                    resolve(&owner.repository, &state, &parts, budget).await?;
                let committed = state.revision_number() != 0 || revision.hash.is_zero();
                let selected_root = selected.as_ref().map(|(id, _)| *id);
                if revision == scope.revision && selected_root != Some(scope.node) {
                    return Err(Error::Stale);
                }
                if selected
                    .as_ref()
                    .is_some_and(|(_, node)| node.is_link() || node.is_discarded())
                {
                    return Err(Error::InvalidInput);
                }
                if let Some((_, node)) = &selected {
                    target::validate_metadata(node)?;
                }
                budget.node()?;
                let proof = Arc::new(crate::overlay::ProvenTargetMetadata {
                    root: selected.as_ref().map(|(id, node)| {
                        let mut selected_owner = owner.clone();
                        selected_owner.state = state.clone();
                        describe(&selected_owner, *id, node)
                    }),
                    selection,
                    state: state.clone(),
                    max_depth: budget.depth_limit(),
                });
                self.revalidate_target_head(&proof).await?;
                policy.authorize_target_head(&proof).await?;
                let mut entries = Image::new();
                if let Some((id, node)) = selected {
                    if node.is_link() || node.is_discarded() {
                        return Err(Error::InvalidInput);
                    }
                    let mut pending = vec![(id, node, String::new(), 0usize)];
                    let mut seen = BTreeSet::new();
                    while let Some((id, node, path, depth)) = pending.pop() {
                        budget.node()?;
                        budget.depth(depth)?;
                        if !seen.insert(id) || node.is_discarded() {
                            return Err(Error::Io);
                        }
                        if node.is_file()
                            && (node.size != 0 && node.address.hash.is_zero()
                                || node.mode & !NodeFileMode::Executable.bits() != 0)
                        {
                            return Err(Error::InvalidInput);
                        }
                        self.owner(&scope)?;
                        let key = NodeKey {
                            revision,
                            node: id,
                            ..scope
                        };
                        self.revalidate_target_node(&proof, &key, false).await?;
                        policy
                            .authorize_target_node(&proof, &key, PolicyAccess::Read)
                            .await?;
                        let summary = summary(&node);
                        let key = path.to_lowercase();
                        if entries
                            .insert(
                                key,
                                Metadata {
                                    path: path.clone(),
                                    summary,
                                    identity: Some(NodeKey {
                                        revision,
                                        node: id,
                                        ..scope
                                    }),
                                    node: id,
                                    flags: node.flags,
                                    local: None,
                                    revision,
                                    uncertain: false,
                                },
                            )
                            .is_some()
                        {
                            return Err(Error::Io);
                        }
                        if node.is_directory() {
                            let mut children =
                                lore_revision::state::StateNodeChildrenIterator::from_parent(
                                    state.clone(),
                                    owner.repository.clone(),
                                    id,
                                    &node,
                                )
                                .await
                                .map_err(state_error)?;
                            while let Some((child, record)) =
                                children.next().await.map_err(state_error)?
                            {
                                budget.node()?;
                                if record.parent != id || child == ROOT_NODE {
                                    return Err(Error::Io);
                                }
                                let name =
                                    read_name(&owner.repository, &state, child, &record, budget)
                                        .await?;
                                let length = path
                                    .len()
                                    .checked_add(usize::from(!path.is_empty()))
                                    .and_then(|value| value.checked_add(name.len()))
                                    .ok_or(Error::ResourceLimit)?;
                                budget.paths(length.checked_mul(8).ok_or(Error::ResourceLimit)?)?;
                                budget.depth(depth + 1)?;
                                let child_path = if path.is_empty() {
                                    name
                                } else {
                                    format!("{path}/{name}")
                                };
                                pending.push((child, record, child_path, depth + 1));
                            }
                        }
                    }
                }
                self.owner(&scope)?;
                self.revalidate_target_head(&proof).await?;
                policy.authorize(&scope, PolicyAccess::Read).await?;
                policy.authorize_target_head(&proof).await?;
                budget.node()?;
                let entries = VerifiedImage {
                    entries,
                    proof: proof.clone(),
                };
                Ok(ScopeView {
                    state,
                    proof,
                    info,
                    head: ScopeHead {
                        revision,
                        root: selected_root,
                    },
                    entries,
                    committed,
                })
            })
            .await
    }

    pub(crate) fn transition_owner_root(&self, key: &NodeKey) -> Result<NodeKey, Error> {
        self.owner(key)?;
        let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        Ok(scopes::root(
            owners.by_exposure.get(&key.exposure).ok_or(Error::Stale)?,
        ))
    }
    pub(crate) fn transition_parent_link(&self, key: &NodeKey) -> Result<Option<NodeKey>, Error> {
        let owner = self.owner(key)?;
        let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        owners
            .links
            .iter()
            .find(|(_, child)| **child == owner.exposure)
            .map(|((parent, link), _)| {
                let parent = owners.by_exposure.get(parent).ok_or(Error::Io)?;
                Ok(NodeKey {
                    repository: parent.repository.id,
                    revision: Address::zero_context_hash(parent.state.revision()),
                    node: *link,
                    exposure: parent.exposure,
                })
            })
            .transpose()
    }
}

async fn canonical_path(owner: &Owner, budget: &mut Budget) -> Result<Vec<String>, Error> {
    let mut parts = Vec::new();
    let mut id = owner.source_root;
    let mut seen = BTreeSet::new();
    while id != ROOT_NODE {
        budget.node()?;
        budget.depth(parts.len() + 1)?;
        if !seen.insert(id) {
            return Err(Error::Io);
        }
        let record = owner
            .state
            .node(owner.repository.clone(), id)
            .await
            .map_err(state_error)?;
        if record.is_discarded() || record.is_link() {
            return Err(Error::InvalidInput);
        }
        parts.push(read_name(&owner.repository, &owner.state, id, &record, budget).await?);
        id = record.parent;
    }
    parts.reverse();
    Ok(parts)
}

async fn read_name(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    id: NodeID,
    node: &RevisionNode,
    budget: &mut Budget,
) -> Result<String, Error> {
    if node.name_length == 0 || node.name_length > 1000 {
        return Err(Error::InvalidInput);
    }
    // Reserve raw spelling plus worst-case lowercase expansion before cloning.
    budget.names(
        (node.name_length as usize)
            .checked_mul(8)
            .ok_or(Error::ResourceLimit)?,
    )?;
    let name = state
        .node_name_clone(repository.clone(), id)
        .await
        .map_err(state_error)?;
    if name.is_empty()
        || name.len() > 1000
        || name.contains(['/', '\\', '\0'])
        || name == "."
        || name == ".."
        || is_reserved_node_name(&name)
    {
        return Err(Error::InvalidInput);
    }
    Ok(name)
}

async fn resolve(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    parts: &[String],
    budget: &mut Budget,
) -> Result<(Option<(NodeID, RevisionNode)>, NodeID), Error> {
    budget.node()?;
    let mut id = ROOT_NODE;
    let mut node = state
        .node(repository.clone(), id)
        .await
        .map_err(state_error)?;
    for (depth, part) in parts.iter().enumerate() {
        let part = part.to_lowercase();
        budget.depth(depth + 1)?;
        if !node.is_directory() {
            return Ok((None, id));
        }
        let mut children = lore_revision::state::StateNodeChildrenIterator::from_parent(
            state.clone(),
            repository.clone(),
            id,
            &node,
        )
        .await
        .map_err(state_error)?;
        let mut found = None;
        let mut names = BTreeSet::new();
        while let Some((child, record)) = children.next().await.map_err(state_error)? {
            budget.node()?;
            if record.parent != id || child == ROOT_NODE {
                return Err(Error::Io);
            }
            let name = read_name(repository, state, child, &record, budget).await?;
            let folded = name.to_lowercase();
            if !names.insert(folded.clone()) {
                return Err(Error::Io);
            }
            if folded == part {
                found = Some((child, record));
            }
        }
        let Some(next) = found else {
            return Ok((None, id));
        };
        (id, node) = next;
        if node.is_discarded() || node.is_link() {
            return Err(Error::InvalidInput);
        }
    }
    Ok((Some((id, node)), id))
}

pub(super) fn summary(node: &RevisionNode) -> NodeSummary {
    if node.is_link() {
        let link = node.linked_node();
        NodeSummary {
            kind: Kind::Link,
            size: 0,
            executable: false,
            immutable: None,
            link: Some(ScopePin {
                repository: link.repository,
                revision: Address::zero_context_hash(link.revision),
                node: link.node,
            }),
        }
    } else if node.is_file() {
        NodeSummary {
            kind: Kind::File,
            size: node.size,
            executable: node.mode & NodeFileMode::Executable.bits() != 0,
            immutable: Some(node.address),
            link: None,
        }
    } else {
        NodeSummary {
            kind: Kind::Directory,
            size: 0,
            executable: false,
            immutable: None,
            link: None,
        }
    }
}

pub(super) async fn restore_view(
    origin: &Owner,
    head: &ScopeHead,
    budget: &mut Budget,
) -> Result<RetainedView, Error> {
    checkpoint::scoped_node(origin, origin.source_root).await?;
    if head.revision.context != Address::zero_context_hash(origin.state.revision()).context {
        return Err(Error::InvalidInput);
    }
    let parts = canonical_path(origin, budget).await?;
    let state = State::deserialize(origin.repository.clone(), head.revision.hash)
        .await
        .map_err(state_error)?;
    if state.revision_number() == 0
        && !head.revision.hash.is_zero()
        && head.revision.hash != origin.state.committed_revision()
    {
        return Err(Error::InvalidInput);
    }
    let (selected, anchor) = resolve(&origin.repository, &state, &parts, budget).await?;
    if selected.as_ref().map(|(id, _)| *id) != head.root {
        return Err(Error::Stale);
    }
    checkpoint::scoped_node(origin, origin.source_root).await?;
    Ok(RetainedView {
        state,
        root: head.root,
        authority: NodeKey {
            revision: head.revision,
            node: anchor,
            ..scopes::root(origin)
        },
    })
}

pub(super) async fn view_links(
    origin: &Owner,
    view: &RetainedView,
    budget: &mut Budget,
) -> Result<Vec<NodeID>, Error> {
    let Some(root) = view.root else {
        return Ok(Vec::new());
    };
    let mut pending = vec![(root, 0usize)];
    let mut seen = BTreeSet::new();
    let mut links = Vec::new();
    while let Some((id, depth)) = pending.pop() {
        budget.node()?;
        budget.depth(depth)?;
        if !seen.insert(id) {
            return Err(Error::Io);
        }
        let node = view
            .state
            .node(origin.repository.clone(), id)
            .await
            .map_err(state_error)?;
        if node.is_discarded() {
            return Err(Error::InvalidInput);
        }
        if node.is_link() {
            links.push(id);
            continue;
        }
        if !node.is_directory() {
            continue;
        }
        let mut names = BTreeSet::new();
        let mut children = lore_revision::state::StateNodeChildrenIterator::from_parent(
            view.state.clone(),
            origin.repository.clone(),
            id,
            &node,
        )
        .await
        .map_err(state_error)?;
        while let Some((child, record)) = children.next().await.map_err(state_error)? {
            budget.node()?;
            budget.depth(depth + 1)?;
            if record.parent != id || child == ROOT_NODE {
                return Err(Error::Io);
            }
            let name = read_name(&origin.repository, &view.state, child, &record, budget).await?;
            if !names.insert(name.to_lowercase()) {
                return Err(Error::Io);
            }
            pending.push((child, depth + 1));
        }
    }
    Ok(links)
}

pub(super) async fn verify_binding(
    origin: &Owner,
    view: &RetainedView,
    target: NodeID,
    child: &Owner,
    known: &BTreeMap<(u64, NodeID), u64>,
    budget: &mut Budget,
) -> Result<(), Error> {
    let root = view.root.ok_or(Error::InvalidInput)?;
    let mut target_owner = origin.clone();
    target_owner.state = view.state.clone();
    target_owner.source_root = root;
    let target_node = checkpoint::scoped_node(&target_owner, target).await?;
    if !target_node.is_link() {
        return Err(Error::Stale);
    }
    let mut parts = Vec::new();
    let mut current = target;
    let mut seen = BTreeSet::new();
    while current != root {
        budget.node()?;
        budget.depth(parts.len() + 1)?;
        if !seen.insert(current) {
            return Err(Error::Io);
        }
        let node = view
            .state
            .node(origin.repository.clone(), current)
            .await
            .map_err(state_error)?;
        if node.is_discarded() || (current != target && !node.is_directory()) {
            return Err(Error::InvalidInput);
        }
        parts.push(read_name(&origin.repository, &view.state, current, &node, budget).await?);
        current = node.parent;
    }
    parts.reverse();
    let mut original = origin.source_root;
    for part in &parts {
        budget.node()?;
        let node = origin
            .state
            .node(origin.repository.clone(), original)
            .await
            .map_err(state_error)?;
        if !node.is_directory() {
            return Err(Error::Stale);
        }
        let mut children = lore_revision::state::StateNodeChildrenIterator::from_parent(
            origin.state.clone(),
            origin.repository.clone(),
            original,
            &node,
        )
        .await
        .map_err(state_error)?;
        let mut found = None;
        let mut names = BTreeSet::new();
        while let Some((id, node)) = children.next().await.map_err(state_error)? {
            budget.node()?;
            if node.parent != original || id == ROOT_NODE {
                return Err(Error::Io);
            }
            let name = read_name(&origin.repository, &origin.state, id, &node, budget).await?;
            if !names.insert(name.to_lowercase()) {
                return Err(Error::Io);
            }
            if &name == part {
                found = Some(id);
            }
        }
        original = found.ok_or(Error::Unsupported)?;
    }
    if known.get(&(origin.exposure, original)) != Some(&child.exposure) {
        return Err(Error::Unsupported);
    }
    let original_node = checkpoint::scoped_node(origin, original).await?;
    if !original_node.is_link() {
        return Err(Error::Unsupported);
    }
    let previous = original_node.linked_node();
    let current = target_node.linked_node();
    if (previous.repository, previous.revision, previous.node)
        != (current.repository, current.revision, current.node)
    {
        return Err(Error::Unsupported);
    }
    let pin = target_node.linked_node();
    if pin.repository != child.repository.id
        || pin.revision != child.state.revision()
        || pin.node != child.source_root
        || checkpoint::scoped_node(child, child.source_root)
            .await?
            .is_link()
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}
