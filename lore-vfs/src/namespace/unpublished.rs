// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Owned, unpublished state and capture inputs. No checkpoint bytes or activation.
#![allow(
    dead_code,
    reason = "Unpublished candidates have no installation caller."
)]
mod capture;
mod install;
mod model_metadata;
mod read_access;
mod source_preparation;
mod successor;
pub(crate) use read_access::FileReadAccess;

use super::*;
use crate::transition_prepare::Budget;
use crate::transition_prepare::OwnedLayout;

pub(crate) struct PreparedRecordFile {
    pub(crate) overlay: Arc<OverlayFile>,
    pub(crate) receipt: ReceiptCell,
}
/// All inputs needed to capture a candidate later, without retaining a plan.
/// The state and binding cannot be substituted independently.
pub(crate) struct UnpublishedState {
    issuer: crate::publication_ownership::successor::PreparedIssuer,
    source_footprint: crate::capture_memory::Footprint,
    selection: crate::transition_prepare::SourceSelection,
    state: State,
    binding: Arc<NamespaceBinding>,
    original_root: Node,
    provenance: Vec<u8>,
    source_binding: Arc<NamespaceBinding>,
    freeze: FrozenNamespace,
    epoch: u64,
    generation: u64,
    source_next: u64,
}
impl Namespace {
    pub(crate) async fn prepare_owned_state(
        self: &Arc<Self>,
        layout: OwnedLayout,
        view: prepared_view::PreparedEffectiveView,
        mut files: BTreeMap<u64, PreparedRecordFile>,
        budget: &mut Budget,
    ) -> crate::transition_prepare::Result<UnpublishedState> {
        let layout = layout.into_validated(self, &view)?;
        if !Arc::ptr_eq(self, layout.freeze.namespace()) || !layout.freeze.valid() {
            return Err(crate::transition_prepare::Error::StaleSource);
        }
        let binding = view.into_binding();
        let source = self.state.lock().await;
        let source_binding = self.current_binding_locked(&source)?;
        if source.generation != layout.generation
            || source.next_inode != layout.source_next
            || self.mutation_epoch()? != layout.epoch
            || source.baselines.get(&layout.scope) != layout.expected.as_ref()
        {
            return Err(crate::transition_prepare::Error::StaleSource);
        }
        // Every retained record, name, index and baseline coexists with source
        // metadata until an eventual publication. Reserve before cloning.
        for record in source.nodes.values() {
            budget.node()?;
            if record.attached && record.entry.base.is_some() {
                budget.node()?;
            }
            if let Some(names) = &record.names {
                for (folded, (spelling, _)) in names {
                    budget.node()?;
                    budget.names(
                        folded
                            .len()
                            .checked_add(spelling.len())
                            .ok_or(Error::ResourceLimit)?,
                    )?;
                }
            }
        }
        for _ in source.baselines.values() {
            budget.node()?;
        }
        for _ in source.active_underlays.values() {
            budget.node()?;
        }
        let mut state = State {
            active_underlays: source.active_underlays.clone(),
            nodes: source
                .nodes
                .iter()
                .map(|(&id, record)| {
                    (
                        id,
                        Record {
                            receipt: record.receipt.clone(),
                            entry: record.entry.clone(),
                            names: record.names.clone(),
                            file: record.file.clone(),
                            attached: record.attached,
                            retired_binding: record.retired_binding.clone(),
                        },
                    )
                })
                .collect(),
            backend_ids: BTreeMap::new(),
            next_inode: layout.next,
            generation: layout.next_generation,
            baselines: source.baselines.clone(),
        };
        drop(source);
        for inode in layout.retire {
            let record = state.nodes.get_mut(&inode).ok_or(Error::InvalidInput)?;
            if !record.attached || inode == 1 {
                return Err(Error::InvalidInput.into());
            }
            record.attached = false;
            if let Some(names) = &mut record.names {
                names.clear();
            }
            record
                .retired_binding
                .get_or_insert_with(|| source_binding.clone());
        }
        budget.node()?; // New immutable binding allocation.
        for (inode, node) in layout.fresh {
            let entry = node.entry;
            let names = node.names;
            budget.node()?; // Record allocation while the recipe map still exists.
            if entry.inode != inode
                || inode == 0
                || inode >= layout.next
                || state.nodes.contains_key(&inode) && inode != 1
            {
                return Err(Error::InvalidInput.into());
            }
            let mut normalized = names.as_ref().map(|_| BTreeMap::new());
            if let Some(names) = names {
                for (spelling, child) in names {
                    let folded = name(&spelling)?;
                    budget.node()?;
                    budget.paths(folded.len())?;
                    if normalized
                        .as_mut()
                        .unwrap()
                        .insert(folded, (spelling, child))
                        .is_some()
                    {
                        return Err(Error::InvalidInput.into());
                    }
                }
            }
            let prepared = files.remove(&inode);
            let (file, receipt) = if let Some(file) = prepared {
                if !file.overlay.has_prepared_bindings(
                    &binding.backend,
                    &binding.gate,
                    &self.mutation_gate,
                    &self.operation_freeze,
                ) {
                    return Err(Error::InvalidInput.into());
                }
                (Some(file.overlay), file.receipt)
            } else {
                if entry.kind == NodeKind::File && entry.base.is_none() {
                    return Err(Error::InvalidInput.into());
                }
                (None, receipt_cell())
            };
            state.nodes.insert(
                inode,
                Record {
                    receipt,
                    entry,
                    names: normalized,
                    file,
                    attached: true,
                    retired_binding: None,
                },
            );
        }
        if !files.is_empty() {
            return Err(Error::InvalidInput.into());
        }
        for child in layout.children {
            let record = state
                .nodes
                .get_mut(&child.inode)
                .ok_or(Error::InvalidInput)?;
            if !record.attached || record.entry.parent != child.old_parent {
                return Err(Error::InvalidInput.into());
            }
            record.entry.parent = child.new_parent;
        }
        if let Some((parent, spelling, old, new)) = layout.parent {
            let names = state
                .nodes
                .get_mut(&parent)
                .and_then(|record| record.names.as_mut())
                .ok_or(Error::InvalidInput)?;
            let value = names
                .get_mut(&name(&spelling)?)
                .ok_or(Error::InvalidInput)?;
            if value.0 != spelling || value.1 != old {
                return Err(Error::InvalidInput.into());
            }
            value.1 = new;
        }
        for (expected, projection) in layout.projections {
            let baseline = state
                .baselines
                .get_mut(&expected.scope)
                .ok_or(Error::InvalidInput)?;
            if *baseline != expected {
                return Err(crate::transition_prepare::Error::StaleSource);
            }
            baseline.parent_projection = Some(projection);
        }
        binding.gate.authorize(&layout.scope, Access::Read).await?;
        let scope = binding.backend.owning_scope(&layout.scope).await?;
        let projection = selected_projection(&scope.parent, layout.expected.as_ref())?;
        if let Some(projection) = &projection
            && binding
                .backend
                .scope_projection(&layout.scope, projection.head)
                .await?
                .as_ref()
                != Some(projection)
        {
            return Err(crate::transition_prepare::Error::Unsupported(
                "unproven selected baseline parent projection",
            ));
        }
        budget.node()?;
        state.baselines.insert(
            layout.scope,
            HeldBaseline {
                scope: layout.scope,
                head: layout.target.clone(),
                sequence: layout
                    .expected
                    .as_ref()
                    .map_or(Some(1), |old| old.sequence.checked_add(1))
                    .ok_or(Error::ResourceLimit)?,
                parent_projection: projection,
            },
        );
        if !state.active_underlays.contains_key(&layout.scope) {
            budget.node()?;
        }
        let selection = crate::transition_prepare::SourceSelection {
            scope: layout.scope,
            target: layout.target.clone(),
        };
        state.active_underlays.insert(layout.scope, layout.target);
        validate_state(&mut state, self.limits, &self.original_root, budget)?;
        let concrete = binding
            .backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
            .ok_or(Error::Unsupported)?;
        for record in state.nodes.values() {
            let gate = if record.attached {
                &binding.gate
            } else {
                &record.retired_binding.as_ref().ok_or(Error::Stale)?.gate
            };
            gate.authorize(&record.entry.authority, Access::Read)
                .await?;
            if record.attached {
                let membership = concrete
                    .prove_owner_membership(&record.entry.authority, budget)
                    .await?;
                if !membership.matches_key(concrete, &record.entry.authority)
                    || membership.kind() != crate::revision::OwnerMembershipKind::Member
                    || record
                        .entry
                        .base
                        .as_ref()
                        .is_some_and(|base| membership.node() != Some(base))
                {
                    return Err(Error::InvalidInput.into());
                }
            }
            if let Some(file) = &record.file {
                if file.key() != record.entry.authority
                    || !Arc::ptr_eq(&file.receipt_cell(), &record.receipt)
                {
                    return Err(Error::InvalidInput.into());
                }
                let metadata = file.checkpoint_guard().await.metadata();
                if let Some(receipt) = receipt_lock(&record.receipt)?.as_ref() {
                    receipt.validate(
                        record.entry.inode,
                        record.entry.authority,
                        metadata.generation,
                        metadata.size,
                        metadata.mode,
                    )?;
                }
            } else if let Some(receipt) = receipt_lock(&record.receipt)?.as_ref() {
                if record.entry.kind != NodeKind::File {
                    return Err(Error::InvalidInput.into());
                }
                let base = record.entry.base.as_ref().ok_or(Error::InvalidInput)?;
                receipt.validate(
                    record.entry.inode,
                    record.entry.authority,
                    0,
                    base.size,
                    base.mode,
                )?;
            }
            gate.authorize(&record.entry.authority, Access::Read)
                .await?;
        }
        for baseline in state.baselines.values() {
            binding
                .gate
                .authorize(&baseline.scope, Access::Read)
                .await?;
            let owning = binding.backend.owning_scope(&baseline.scope).await?;
            if owning.root != baseline.scope
                || binding
                    .backend
                    .scope_head(&baseline.scope, baseline.head.revision)
                    .await?
                    != baseline.head
            {
                return Err(Error::InvalidInput.into());
            }
            match (&owning.parent, &baseline.parent_projection) {
                (None, None) => {}
                (Some(parent), Some(projection)) if parent.scope == projection.parent => {
                    binding.gate.authorize(&parent.scope, Access::Read).await?;
                    if binding
                        .backend
                        .scope_projection(&baseline.scope, projection.head)
                        .await?
                        .as_ref()
                        != Some(projection)
                    {
                        return Err(Error::InvalidInput.into());
                    }
                    binding.gate.authorize(&parent.scope, Access::Read).await?;
                }
                _ => return Err(Error::InvalidInput.into()),
            }
            binding
                .gate
                .authorize(&baseline.scope, Access::Read)
                .await?;
        }
        let current = self.state.lock().await;
        if !layout.freeze.valid()
            || current.generation != layout.generation
            || current.next_inode != layout.source_next
            || self.mutation_epoch()? != layout.epoch
        {
            return Err(crate::transition_prepare::Error::StaleSource);
        }
        self.confirms_binding(&source_binding)?;
        drop(current);
        Ok(UnpublishedState {
            selection,
            issuer: layout.issuer,
            source_footprint: layout.source_footprint,
            state,
            binding,
            original_root: self.original_root.clone(),
            provenance: layout.provenance,
            source_binding,
            freeze: layout.freeze,
            epoch: layout.epoch,
            generation: layout.generation,
            source_next: layout.source_next,
        })
    }
}
fn selected_projection(
    parent: &Option<crate::core::ParentScope>,
    expected: Option<&HeldBaseline>,
) -> crate::transition_prepare::Result<Option<crate::core::ScopeProjection>> {
    match (
        parent,
        expected.and_then(|old| old.parent_projection.as_ref()),
    ) {
        (None, None) => Ok(None),
        (Some(parent), Some(projection)) if parent.scope == projection.parent => {
            Ok(Some(projection.clone()))
        }
        _ => Err(crate::transition_prepare::Error::Unsupported(
            "unproven selected baseline parent projection",
        )),
    }
}

fn validate_state(
    state: &mut State,
    limits: Limits,
    original_root: &Node,
    budget: &mut Budget,
) -> Result<()> {
    if state.nodes.len() > limits.max_inodes || state.generation == 0 {
        return Err(Error::ResourceLimit.into());
    }
    let root = state.nodes.get(&1).ok_or(Error::InvalidInput)?;
    if !root.attached || root.entry.parent != 1 || root.entry.kind != NodeKind::Directory {
        return Err(Error::InvalidInput.into());
    }
    match state.active_underlays.get(&original_root.key) {
        None if root.entry.base.as_ref() != Some(original_root) => {
            return Err(Error::InvalidInput.into());
        }
        Some(head)
            if head.root.is_none_or(|id| {
                root.entry.base.as_ref().is_none_or(|base| {
                    base.key
                        != NodeKey {
                            revision: head.revision,
                            node: id,
                            ..original_root.key
                        }
                })
            }) =>
        {
            return Err(Error::InvalidInput.into());
        }
        _ => {}
    }
    let identity = |key: NodeKey| (key.repository, key.exposure, key.revision.context);
    let mut exposures = BTreeSet::new();
    for record in state.nodes.values() {
        budget.node()?;
        exposures.insert(identity(record.entry.authority));
    }
    if state.active_underlays.len() > limits.max_inodes.min(crate::core::MAX_OWNING_SCOPES)
        || state.baselines.len() > limits.max_inodes.min(crate::core::MAX_OWNING_SCOPES)
    {
        return Err(Error::ResourceLimit.into());
    }
    let mut underlay_identities = BTreeMap::new();
    for (scope, head) in &state.active_underlays {
        budget.node()?;
        if scope.revision.context != head.revision.context
            || !exposures.contains(&identity(*scope))
            || underlay_identities
                .insert(identity(*scope), (scope, head))
                .is_some()
        {
            return Err(Error::InvalidInput.into());
        }
    }
    for record in state.nodes.values().filter(|record| record.attached) {
        let parent = state
            .nodes
            .get(&record.entry.parent)
            .ok_or(Error::InvalidInput)?;
        if (record.entry.inode == 1
            || !same_exposure(record.entry.authority, parent.entry.authority))
            && let Some((scope, head)) = underlay_identities.get(&identity(record.entry.authority))
            && head.root.is_none_or(|id| {
                record.entry.base.as_ref().is_none_or(|base| {
                    base.key
                        != NodeKey {
                            revision: head.revision,
                            node: id,
                            ..**scope
                        }
                })
            })
        {
            return Err(Error::InvalidInput.into());
        }
    }
    for (scope, baseline) in &state.baselines {
        if *scope != baseline.scope
            || baseline.sequence == 0
            || baseline.head.revision.context != scope.revision.context
            || !exposures.contains(&identity(*scope))
            || baseline
                .parent_projection
                .as_ref()
                .is_some_and(|projection| {
                    projection.parent.revision.context != projection.head.context
                        || !exposures.contains(&identity(projection.parent))
                })
        {
            return Err(Error::InvalidInput.into());
        }
    }
    let mut references = BTreeMap::<u64, usize>::new();
    for (&inode, record) in &state.nodes {
        if inode == 0
            || inode >= state.next_inode
            || record.entry.inode != inode
            || record
                .entry
                .base
                .as_ref()
                .is_some_and(|base| base.key != record.entry.authority)
            || !record.attached && record.names.as_ref().is_some_and(|names| !names.is_empty())
        {
            return Err(Error::InvalidInput.into());
        }
        let parent = state
            .nodes
            .get(&record.entry.parent)
            .ok_or(Error::InvalidInput)?;
        if parent.entry.kind != NodeKind::Directory
            || record.entry.kind == NodeKind::Symlink
            || record.entry.kind != NodeKind::Directory && record.names.is_some()
            || record.entry.kind == NodeKind::Directory
                && record.entry.base.is_none()
                && record.names.is_none()
            || record
                .entry
                .base
                .as_ref()
                .is_some_and(|base| base.kind != record.entry.kind)
            || record.entry.kind == NodeKind::File
                && record.entry.base.is_none()
                && record.file.is_none()
            || record.entry.base.is_none()
                && !same_exposure(record.entry.authority, parent.entry.authority)
        {
            return Err(Error::InvalidInput.into());
        }
        if !record.attached && record.retired_binding.is_none() {
            return Err(Error::InvalidInput.into());
        }
        if record.attached {
            if record.retired_binding.is_some() {
                return Err(Error::InvalidInput.into());
            }
            if let Some(base) = &record.entry.base {
                budget.node()?;
                if state.backend_ids.insert(base.key, inode).is_some() {
                    return Err(Error::InvalidInput.into());
                }
            }
        }
        if let Some(names) = &record.names {
            if names.len() > limits.max_directory_entries {
                return Err(Error::ResourceLimit.into());
            }
            for (folded, (spelling, child)) in names {
                if name(spelling)? != *folded || !record.attached {
                    return Err(Error::InvalidInput.into());
                }
                let target = state.nodes.get(child).ok_or(Error::InvalidInput)?;
                if !target.attached || target.entry.parent != inode || *child == 1 {
                    return Err(Error::InvalidInput.into());
                }
                budget.node()?;
                *references.entry(*child).or_default() += 1;
            }
        }
    }
    let mut rooted = BTreeSet::from([1]);
    for &inode in state.nodes.keys() {
        let mut path = BTreeSet::new();
        let mut ancestor = inode;
        while !rooted.contains(&ancestor) {
            budget.node()?;
            budget.node()?;
            if !path.insert(ancestor) {
                return Err(Error::InvalidInput.into());
            }
            ancestor = state
                .nodes
                .get(&ancestor)
                .ok_or(Error::InvalidInput)?
                .entry
                .parent;
        }
        rooted.extend(path);
    }
    for (&inode, record) in &state.nodes {
        // Unhydrated directories deliberately have no cached child references.
        if record.attached && inode != 1 && references.get(&inode) != Some(&1) {
            return Err(Error::InvalidInput.into());
        }
    }
    Ok(())
}

#[cfg(test)]
impl UnpublishedState {
    pub(crate) fn test_witness(&self) -> MetadataWitness {
        MetadataWitness {
            epoch: self.epoch,
            generation: self.generation,
            next_inode: self.source_next,
        }
    }
    pub(crate) fn test_freeze(&self) -> FrozenNamespace {
        self.freeze.pin()
    }
    pub(crate) fn test_entries(&self) -> Vec<(Entry, bool)> {
        self.state
            .nodes
            .values()
            .map(|record| (record.entry.clone(), record.attached))
            .collect()
    }
    pub(crate) fn test_receipt(&self, inode: u64) -> ReceiptCell {
        self.state.nodes[&inode].receipt.clone()
    }
    pub(crate) fn test_original_root(&self) -> &Node {
        &self.original_root
    }
    pub(crate) fn test_index(&self, key: NodeKey) -> Option<u64> {
        self.state.backend_ids.get(&key).copied()
    }
    pub(crate) fn test_baseline(&self, scope: NodeKey) -> &HeldBaseline {
        &self.state.baselines[&scope]
    }
}

#[cfg(test)]
#[path = "unpublished_tests.rs"]
mod tests;
