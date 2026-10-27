// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit candidate authorization. Requests never install state or write stores.

use std::collections::BTreeSet;

use super::*;
use crate::core::Backend;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::overlay::CandidateEdit;
use crate::overlay::CandidateEditAction;
use crate::overlay::candidate_edit::Location;

struct Admission<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}
impl PreparedTransition {
    /// Check source write policy and explicit candidate decisions. Target reads
    /// remain metadata-only; witness checks may read bounded private source data.
    /// Success is not an installation capability and does not stop revocation.
    pub async fn authorize_candidate_edits(&self) -> Result<()> {
        self.candidate_active
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|_active| core::Error::Busy)?;
        let _admission = Admission(&self.candidate_active);
        let original = self.source_routing.backend().clone();
        let backend = original
            .as_any()
            .and_then(|value| value.downcast_ref::<RevisionBackend>())
            .ok_or(Error::Unsupported(
                "scoped revision candidate authorization",
            ))?;
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(backend.transition_execution(), async {
                self.validate_source().await?;
                let policy = self.source_routing.policy();
                let scope = self.intent.scope;
                authorize_sources(backend, &policy, &BTreeSet::from([scope])).await?;
                let mut budget = Budget::new(self.limits)?;
                reserve_retained(self, &mut budget)?;
                let origin = backend
                    .transition_view(scope, scope.revision, policy.clone(), &mut budget)
                    .await?;
                let target = backend
                    .transition_view(
                        scope,
                        self.intent.target.revision,
                        policy.clone(),
                        &mut budget,
                    )
                    .await?;
                if target.head != self.intent.target {
                    return Err(Error::StaleSource);
                }
                backend
                    .transition_validate_target_links(scope, &origin, &target, &mut budget)
                    .await?;
                let inode = super::installed_source::selected_inode(
                    &self.snapshot,
                    backend,
                    scope,
                    &policy,
                    &mut budget,
                )
                .await?;
                let mut local = inventory(
                    &self.snapshot,
                    backend,
                    scope,
                    inode,
                    policy.clone(),
                    &mut budget,
                )
                .await?;
                if self
                    .intent
                    .expected
                    .as_ref()
                    .is_some_and(|baseline| baseline.head.root.is_none())
                    && local.len() == 1
                    && local
                        .get("")
                        .is_some_and(|node| node.summary.kind == Kind::Directory)
                {
                    local.clear();
                }
                let mut actual = BTreeMap::new();
                let mut keys = BTreeSet::new();
                for change in &self.changes {
                    budget.node()?;
                    budget.paths(
                        change
                            .path
                            .len()
                            .checked_mul(8)
                            .ok_or(core::Error::ResourceLimit)?,
                    )?;
                    let key = std::str::from_utf8(&change.path)
                        .map_err(|_encoding| core::Error::InvalidInput)?
                        .to_lowercase();
                    if actual.insert(key.clone(), change).is_some() {
                        return Err(Error::InvalidScope);
                    }
                    keys.insert(key);
                }
                for key in differences(&target.entries, &self.candidate, false, &mut budget)? {
                    budget.node()?;
                    keys.insert(key);
                }
                let mut records = BTreeMap::new();
                for record in &self.snapshot.nodes {
                    budget.node()?;
                    records.insert(record.entry.inode, &record.entry);
                }
                let mut bases: BTreeMap<NodeKey, Vec<&crate::namespace::SnapshotNode>> =
                    BTreeMap::new();
                for record in &self.snapshot.nodes {
                    if let Some(base) = &record.entry.base {
                        budget.node()?;
                        bases.entry(base.key).or_default().push(record);
                    }
                }
                let mut sources = BTreeSet::from([scope]);
                for key in &keys {
                    budget.compare()?;
                    let before = local.get(key);
                    let target_before = target.entries.get(key);
                    let after = self.candidate.get(key);
                    if [before, target_before, after]
                        .into_iter()
                        .flatten()
                        .any(|node| node.summary.kind == Kind::Link || node.summary.link.is_some())
                    {
                        return Err(Error::Unsupported("candidate link edits"));
                    }
                    let source = match before {
                        Some(node) => source_authority(node, scope, &records)?,
                        None => nearest_directory(&local, key, &mut budget)?
                            .map(|node| source_authority(node, scope, &records))
                            .transpose()?
                            .unwrap_or(scope),
                    };
                    insert_source(source, scope, &mut sources, &mut budget)?;
                    let source_edge = actual.contains_key(key) && edge_changed(before, after);
                    let target_edge = edge_changed(target_before, after);
                    if (source_edge || target_edge) && !key.is_empty() {
                        let parent = nearest_directory(&local, key, &mut budget)?
                            .map(|node| source_authority(node, scope, &records))
                            .transpose()?
                            .unwrap_or(scope);
                        insert_source(parent, scope, &mut sources, &mut budget)?;
                    }
                    if target_edge {
                        if let Some(original_node) = origin.entries.get(key) {
                            if original_node.summary.kind == Kind::Link {
                                return Err(Error::Unsupported("candidate link edits"));
                            }
                            let original_key = source_authority(original_node, scope, &records)?;
                            insert_source(original_key, scope, &mut sources, &mut budget)?;
                            if let Some(parent) =
                                nearest_directory(&origin.entries, key, &mut budget)?
                            {
                                insert_source(
                                    source_authority(parent, scope, &records)?,
                                    scope,
                                    &mut sources,
                                    &mut budget,
                                )?;
                            }
                            if let Some(versions) = bases.get(&original_key) {
                                let mut attached = None;
                                for record in versions {
                                    budget.compare()?;
                                    if record.attached
                                        && attached.replace(record.entry.inode).is_some()
                                    {
                                        return Err(Error::InvalidScope);
                                    }
                                }
                                for record in versions {
                                    if attached.is_some_and(|id| id != record.entry.inode)
                                        || record.entry.inode == inode
                                    {
                                        continue;
                                    }
                                    let parent = records
                                        .get(&record.entry.parent)
                                        .ok_or(Error::InvalidScope)?
                                        .authority;
                                    insert_source(parent, scope, &mut sources, &mut budget)?;
                                }
                            }
                        } else if before.is_none() && after.is_none() {
                            return Err(Error::Unsupported(
                                "carried deletion source identity unavailable",
                            ));
                        }
                    }
                }
                authorize_sources(backend, &policy, &sources).await?;
                budget.node()?;
                let head = CandidateEdit {
                    proof: target.proof.clone(),
                    action: CandidateEditAction::AdvanceSelectedHead,
                    path: Vec::new(),
                    source_before: None,
                    target_before: None,
                    candidate_after: None,
                    change: None,
                    location: Location::Head,
                };
                backend.revalidate_candidate_edit(&head).await?;
                policy.authorize_candidate_edit(&head).await?;
                for key in keys {
                    budget.node()?;
                    let before = local.get(&key);
                    let target_before = target.entries.get(&key);
                    let after = self.candidate.get(&key);
                    let path = after
                        .or(before)
                        .or(target_before)
                        .ok_or(Error::InvalidScope)?
                        .path
                        .as_bytes();
                    budget.paths(
                        path.len()
                            .checked_mul(4)
                            .ok_or(core::Error::ResourceLimit)?,
                    )?;
                    let location = if target.proof.root().is_none() {
                        Location::Absent
                    } else if let Some(node) = target_before {
                        let key = NodeKey {
                            node: node.node,
                            revision: target.head.revision,
                            ..scope
                        };
                        let (actual, _) =
                            backend.candidate_node_metadata(&target.proof, &key).await?;
                        budget.paths(node.path.len())?;
                        Location::Node {
                            node: actual,
                            path: node.path.as_bytes().to_vec(),
                        }
                    } else {
                        let parent = nearest_directory(&target.entries, &key, &mut budget)?
                            .ok_or(Error::InvalidScope)?;
                        let key = NodeKey {
                            node: parent.node,
                            revision: target.head.revision,
                            ..scope
                        };
                        let (actual, _) =
                            backend.candidate_node_metadata(&target.proof, &key).await?;
                        let depth = if parent.path.is_empty() {
                            0
                        } else {
                            parent.path.split('/').count()
                        };
                        let path = std::str::from_utf8(path)
                            .map_err(|_encoding| core::Error::InvalidInput)?;
                        let tail = path
                            .split('/')
                            .skip(depth)
                            .collect::<Vec<_>>()
                            .join("/")
                            .into_bytes();
                        budget.paths(
                            parent
                                .path
                                .len()
                                .checked_add(tail.len())
                                .ok_or(core::Error::ResourceLimit)?,
                        )?;
                        let missing = backend
                            .candidate_missing_path(&target, path.as_bytes(), &actual, &tail)
                            .await?;
                        Location::Parent {
                            node: actual,
                            path: parent.path.as_bytes().to_vec(),
                            tail,
                            entry: None,
                            missing: Some(Box::new(missing)),
                        }
                    };
                    let edit = CandidateEdit {
                        proof: target.proof.clone(),
                        action: CandidateEditAction::EntryChange,
                        path: path.to_vec(),
                        source_before: before.map(|node| node.summary.clone()),
                        target_before: target_before.map(|node| node.summary.clone()),
                        candidate_after: after.map(|node| node.summary.clone()),
                        change: actual.get(&key).map(|change| (*change).clone()),
                        location,
                    };
                    backend.revalidate_candidate_edit(&edit).await?;
                    policy.authorize_candidate_edit(&edit).await?;
                    let source_edge = actual.contains_key(&key) && edge_changed(before, after);
                    let target_edge = edge_changed(target_before, after);
                    if (source_edge || target_edge)
                        && !key.is_empty()
                        && let Location::Node { node: entry, .. } = &edit.location
                    {
                        let parent = nearest_directory(&target.entries, &key, &mut budget)?
                            .ok_or(Error::InvalidScope)?;
                        let parent_key = NodeKey {
                            node: parent.node,
                            revision: target.head.revision,
                            ..scope
                        };
                        let (node, _) = backend
                            .candidate_node_metadata(&target.proof, &parent_key)
                            .await?;
                        let depth = if parent.path.is_empty() {
                            0
                        } else {
                            parent.path.split('/').count()
                        };
                        let tail = std::str::from_utf8(edit.path())
                            .map_err(|_encoding| core::Error::InvalidInput)?
                            .split('/')
                            .skip(depth)
                            .collect::<Vec<_>>()
                            .join("/")
                            .into_bytes();
                        budget.node()?;
                        budget.paths(
                            edit.path
                                .len()
                                .checked_mul(3)
                                .and_then(|bytes| bytes.checked_add(parent.path.len()))
                                .and_then(|bytes| bytes.checked_add(tail.len()))
                                .ok_or(core::Error::ResourceLimit)?,
                        )?;
                        let parent_edit = CandidateEdit {
                            proof: target.proof.clone(),
                            action: CandidateEditAction::EntryChange,
                            path: edit.path.clone(),
                            source_before: edit.source_before.clone(),
                            target_before: edit.target_before.clone(),
                            candidate_after: edit.candidate_after.clone(),
                            change: edit.change.clone(),
                            location: Location::Parent {
                                node,
                                path: parent.path.as_bytes().to_vec(),
                                tail,
                                entry: Some(entry.clone()),
                                missing: None,
                            },
                        };
                        backend.revalidate_candidate_edit(&parent_edit).await?;
                        policy.authorize_candidate_edit(&parent_edit).await?;
                    }
                }
                self.validate_source().await?;
                policy.authorize_candidate_edit(&head).await?;
                authorize_sources(backend, &policy, &sources).await
            }),
        )
        .await
    }
}

fn edge_changed(before: Option<&Metadata>, after: Option<&Metadata>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => {
            before.path != after.path || before.summary.kind != after.summary.kind
        }
        (None, None) => false,
        _ => true,
    }
}
fn insert_source(
    key: NodeKey,
    scope: NodeKey,
    sources: &mut BTreeSet<NodeKey>,
    budget: &mut Budget,
) -> Result<()> {
    if key.repository != scope.repository || key.exposure != scope.exposure {
        return Err(Error::InvalidScope);
    }
    budget.node()?;
    sources.insert(key);
    Ok(())
}

fn source_authority(
    metadata: &Metadata,
    scope: NodeKey,
    records: &BTreeMap<u64, &crate::namespace::Entry>,
) -> Result<NodeKey> {
    let key = match metadata.local {
        Some((inode, _generation)) => records.get(&inode).ok_or(Error::InvalidScope)?.authority,
        None => metadata.identity.ok_or(Error::InvalidScope)?,
    };
    if key.repository != scope.repository || key.exposure != scope.exposure {
        return Err(Error::InvalidScope);
    }
    Ok(key)
}
async fn authorize_sources(
    backend: &RevisionBackend,
    policy: &Arc<dyn AccessGate>,
    sources: &BTreeSet<NodeKey>,
) -> Result<()> {
    for key in sources {
        backend.revalidate(key, true).await?;
        policy.authorize(key, Access::Write).await?;
    }
    Ok(())
}
fn nearest_directory<'a>(
    image: &'a Image,
    key: &str,
    budget: &mut Budget,
) -> Result<Option<&'a Metadata>> {
    let mut key = key;
    loop {
        budget.compare()?;
        if key.is_empty() {
            return Ok(None);
        }
        key = key.rsplit_once('/').map_or("", |(parent, _name)| parent);
        if let Some(parent) = image.get(key) {
            if parent.summary.kind != Kind::Directory {
                return Err(Error::Unsupported(
                    "candidate authority crosses a non-directory",
                ));
            }
            return Ok(Some(parent));
        }
    }
}
fn reserve_retained(plan: &PreparedTransition, budget: &mut Budget) -> Result<()> {
    for record in &plan.snapshot.nodes {
        budget.node()?;
        if let Some(names) = &record.names {
            for (name, _) in names {
                budget.node()?;
                budget.names(name.len())?;
            }
        }
    }
    for metadata in plan.candidate.values() {
        budget.node()?;
        budget.paths(metadata.path.len())?;
    }
    for change in &plan.changes {
        budget.node()?;
        budget.paths(change.path.len())?;
    }
    for conflict in &plan.conflicts {
        budget.node()?;
        budget.paths(conflict.path.len())?;
    }
    let mut private_bytes = 0usize;
    for file in plan.snapshot.files.values() {
        budget.node()?;
        for extent in &file.extents {
            budget.node()?;
            private_bytes = private_bytes
                .checked_add(extent.data.len())
                .filter(|bytes| *bytes <= plan.limits.max_private_data_bytes)
                .ok_or(core::Error::ResourceLimit)?;
        }
    }
    budget.paths(plan.snapshot.backend_state.len())?;
    for _ in plan.snapshot.baselines.values() {
        budget.node()?;
    }
    for _ in plan.snapshot.active_underlays.values() {
        budget.node()?;
    }
    for _baseline in &plan.intent.protected_children {
        budget.node()?;
    }
    Ok(())
}
