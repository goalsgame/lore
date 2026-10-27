// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Bounded namespace bookkeeping only. No stores, writes or installation rights.
#![allow(
    dead_code,
    reason = "Layout preparation has no activation or materialization caller."
)]

mod materialize;
pub(crate) mod owned;

use super::*;
use crate::core::Backend;
use crate::core::Node;
use crate::core::NodeKind;
use crate::core::ScopeProjection;
use crate::namespace::Entry;
use crate::namespace::complete_binding::PreparedPolicyBinding;
use crate::overlay::Access;

struct Admission<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub(crate) struct PreparedNamespaceLayout<'plan> {
    pub(super) source: &'plan PreparedTransition,
    pub(super) binding: PreparedPolicyBinding,
    pub(super) source_generation: u64,
    pub(super) source_epoch: u64,
    pub(super) source_next_inode: u64,
    pub(super) next_inode: u64,
    pub(super) next_generation: u64,
    pub(super) root_inode: u64,
    pub(super) fresh: BTreeMap<u64, LayoutNode<'plan>>,
    // Retirement clears cached names on old directories. Existing Model
    // directory handles retain their independent enumeration snapshots.
    pub(super) retire: Vec<u64>,
    pub(super) children: Vec<ChildReparent>,
    pub(super) parent_patch: Option<ParentPatch<'plan>>,
    pub(super) projections: Vec<ProjectionRebase<'plan>>,
}
pub(super) struct LayoutNode<'plan> {
    pub(super) path: &'plan str,
    pub(super) entry: Entry,
    pub(super) names: Option<Vec<(Vec<u8>, u64)>>,
    pub(super) file: Option<FileLayout>,
    pub(super) child_binding: Option<Box<ChildBinding>>,
}
pub(super) struct ChildBinding {
    pub(super) scope: NodeKey,
    pub(super) target_link: NodeKey,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FileLayout {
    LazyImmutable,
    Private {
        source_inode: u64,
        generation: u64,
        base_end: u64,
        // Materialization must validate and reissue this receipt for its NEW
        // inode, or explicitly discard it. The old receipt/cell is never reused.
        receipt_source: Option<u64>,
    },
}
pub(crate) struct ChildReparent {
    pub(crate) scope: NodeKey,
    pub(crate) inode: u64,
    pub(crate) old_parent: u64,
    pub(crate) new_parent: u64,
    pub(crate) target_link: NodeKey,
}
pub(super) struct ParentPatch<'plan> {
    pub(super) parent: u64,
    pub(super) spelling: &'plan [u8],
    pub(super) old_child: u64,
    pub(super) new_child: u64,
}
pub(super) struct ProjectionRebase<'plan> {
    pub(super) expected: &'plan HeldBaseline,
    pub(super) next: ScopeProjection,
}
enum Classified {
    Fresh {
        authority: NodeKey,
        base: Option<Node>,
        kind: NodeKind,
        file: Option<FileLayout>,
        child_binding: Option<Box<ChildBinding>>,
    },
    Child {
        scope: NodeKey,
        inode: u64,
        old_parent: u64,
        target_link: NodeKey,
    },
}

impl PreparedTransition {
    pub(crate) async fn prepare_install_layout<'plan>(
        &'plan self,
        binding: PreparedPolicyBinding,
    ) -> Result<PreparedNamespaceLayout<'plan>> {
        if self.intent.target.root.is_none() {
            return Err(Error::Unsupported("writable absent underlay layout"));
        }
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
            .ok_or(Error::Unsupported("revision namespace layout"))?;
        lore_base::runtime::LORE_CONTEXT
            .scope(backend.transition_execution(), async {
                self.validate_source().await?;
                let prepared = binding.prepared();
                if !prepared.matches_source(backend)
                    || prepared.original_root() != &self.snapshot.original_root
                    || prepared.scope() != self.intent.scope
                    || prepared.head() != &self.intent.target
                    || prepared.source_epoch() != self.source_epoch
                {
                    return Err(Error::InvalidScope);
                }
                let selected = prepared.selected_root().ok_or(Error::InvalidScope)?.clone();
                if selected.kind != NodeKind::Directory {
                    return Err(Error::Unsupported("non-directory owning scope"));
                }
                let complete = prepared.backend();
                let policy = self.source_routing.policy();
                let future_policy = binding.policy();
                let limits = self.freeze.namespace().retained_limits();
                let mut budget = Budget::new(self.limits)?;
                reserve_source(self, prepared.provenance().len(), &mut budget)?;
                let scope = self.intent.scope;
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
                let current = if let Some(expected) = &self.intent.expected {
                    if expected.head.revision != scope.revision {
                        let view = backend
                            .transition_view(
                                scope,
                                expected.head.revision,
                                policy.clone(),
                                &mut budget,
                            )
                            .await?;
                        if view.head != expected.head {
                            return Err(Error::StaleSource);
                        }
                        Some(view)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let current = current.as_ref().unwrap_or(&origin);
                let mut records = BTreeMap::new();
                let mut owners = BTreeMap::new();
                let mut retained_scopes = std::collections::BTreeSet::new();
                for record in &self.snapshot.nodes {
                    budget.node()?;
                    records.insert(record.entry.inode, record);
                    let owner = backend.transition_owner_root(&record.entry.authority)?;
                    owners.insert(record.entry.inode, owner);
                    retained_scopes.insert(owner);
                }
                let old_inode = super::installed_source::selected_inode(
                    &self.snapshot,
                    backend,
                    scope,
                    &policy,
                    &mut budget,
                )
                .await?;
                let old_root = records
                    .get(&old_inode)
                    .copied()
                    .ok_or(Error::InvalidScope)?;
                if self
                    .candidate
                    .get("")
                    .is_none_or(|entry| entry.summary.kind != Kind::Directory)
                {
                    return Err(Error::Unsupported("non-directory candidate root"));
                }
                let mut direct_children = BTreeMap::new();
                for record in &self.snapshot.nodes {
                    budget.node()?;
                    if record.attached
                        && owners[&record.entry.inode] != scope
                        && owners.get(&record.entry.parent) == Some(&scope)
                        && direct_children
                            .insert(owners[&record.entry.inode], record)
                            .is_some()
                    {
                        return Err(Error::Unsupported("ambiguous child boundary"));
                    }
                }
                let mut classes = BTreeMap::new();
                let mut used_children = std::collections::BTreeSet::new();
                let mut projections = Vec::new();
                for (path, metadata) in &self.candidate {
                    budget.node()?;
                    budget.depth(path.split('/').filter(|part| !part.is_empty()).count())?;
                    let class = if metadata.summary.kind == Kind::Link {
                        let previous = origin
                            .entries
                            .get(path)
                            .ok_or(Error::Unsupported("new child boundary"))?;
                        let incoming = target
                            .entries
                            .get(path)
                            .ok_or(Error::Unsupported("removed child boundary"))?;
                        if previous.path != incoming.path
                            || incoming.path != metadata.path
                            || previous.summary != incoming.summary
                            || incoming.summary.kind != Kind::Link
                        {
                            return Err(Error::Unsupported("changed child binding"));
                        }
                        let pin = incoming.summary.link.as_ref().ok_or(Error::InvalidScope)?;
                        let (_, spelling) = split(&incoming.path);
                        let parent = target
                            .entries
                            .get(split(path).0)
                            .ok_or(Error::InvalidScope)?;
                        let parent_key = NodeKey {
                            node: parent.node,
                            revision: target.head.revision,
                            ..scope
                        };
                        let link = NodeKey {
                            node: incoming.node,
                            revision: target.head.revision,
                            ..scope
                        };
                        future_policy.authorize(&parent_key, Access::Read).await?;
                        future_policy.authorize(&link, Access::Read).await?;
                        policy
                            .authorize_target_node(&target.proof, &link, Access::Read)
                            .await?;
                        let resolved = complete.lookup(&parent_key, spelling.as_bytes()).await?;
                        future_policy.authorize(&link, Access::Read).await?;
                        let child = complete.transition_owner_root(&resolved.key)?;
                        if child.repository != pin.repository
                            || child.revision != pin.revision
                            || child.node != pin.node
                            || !used_children.insert(child)
                        {
                            return Err(Error::Unsupported("ambiguous child mapping"));
                        }
                        let original_link = NodeKey {
                            node: previous.node,
                            revision: scope.revision,
                            ..scope
                        };
                        if let Some(record) = direct_children.get(&child) {
                            if backend.transition_parent_link(&record.entry.authority)?
                                != Some(original_link)
                                || complete.transition_owner_root(&record.entry.authority)? != child
                            {
                                return Err(Error::Unsupported("unproven child parent"));
                            }
                            policy
                                .authorize(&record.entry.authority, Access::Read)
                                .await?;
                            future_policy
                                .authorize(&record.entry.authority, Access::Read)
                                .await?;
                            if let Some(baseline) = self.snapshot.baselines.get(&child)
                                && let Some(projection) = &baseline.parent_projection
                            {
                                let actual = current
                                    .entries
                                    .get(path)
                                    .and_then(|entry| entry.summary.link.clone());
                                if projection.parent != scope
                                    || projection.head != current.head.revision
                                    || projection.target != actual
                                {
                                    return Err(Error::StaleSource);
                                }
                                budget.node()?;
                                projections.push(ProjectionRebase {
                                    expected: baseline,
                                    next: ScopeProjection {
                                        parent: scope,
                                        head: target.head.revision,
                                        target: Some(pin.clone()),
                                    },
                                });
                            }
                            Classified::Child {
                                scope: child,
                                inode: record.entry.inode,
                                old_parent: record.entry.parent,
                                target_link: link,
                            }
                        } else {
                            if retained_scopes.contains(&child)
                                || self.snapshot.baselines.contains_key(&child)
                                || self.snapshot.active_underlays.contains_key(&child)
                            {
                                return Err(Error::Unsupported(
                                    "retained child without attached boundary",
                                ));
                            }
                            future_policy.authorize(&resolved.key, Access::Read).await?;
                            policy.authorize(&resolved.key, Access::Read).await?;
                            budget.node()?;
                            Classified::Fresh {
                                authority: resolved.key,
                                kind: resolved.kind,
                                file: (resolved.kind == NodeKind::File)
                                    .then_some(FileLayout::LazyImmutable),
                                base: Some(resolved),
                                child_binding: Some(Box::new(ChildBinding {
                                    scope: child,
                                    target_link: link,
                                })),
                            }
                        }
                    } else if path.is_empty() {
                        future_policy.authorize(&selected.key, Access::Read).await?;
                        Classified::Fresh {
                            authority: selected.key,
                            base: Some(selected.clone()),
                            kind: NodeKind::Directory,
                            file: None,
                            child_binding: None,
                        }
                    } else if let Some((inode, generation)) = metadata.local {
                        let record = records.get(&inode).ok_or(Error::InvalidScope)?;
                        if owners[&inode] != scope
                            || complete.transition_owner_root(&record.entry.authority)? != scope
                        {
                            return Err(Error::InvalidScope);
                        }
                        let expected = if metadata.summary.kind == Kind::Directory {
                            NodeKind::Directory
                        } else {
                            NodeKind::File
                        };
                        if record.entry.kind != expected {
                            return Err(Error::InvalidScope);
                        }
                        future_policy
                            .authorize(&record.entry.authority, Access::Read)
                            .await?;
                        let file = if record.entry.kind == NodeKind::File {
                            if let Some(image) = self.snapshot.files.get(&inode) {
                                if image.metadata.generation != generation {
                                    return Err(Error::StaleSource);
                                }
                                if image.metadata.mutable {
                                    Some(FileLayout::Private {
                                        source_inode: inode,
                                        generation,
                                        base_end: image.base_end,
                                        receipt_source: image
                                            .receipt
                                            .as_ref()
                                            .map(|receipt| receipt.inode),
                                    })
                                } else {
                                    Some(FileLayout::LazyImmutable)
                                }
                            } else {
                                if record.receipt.is_some()
                                    && origin
                                        .entries
                                        .get(path)
                                        .is_none_or(|entry| entry.summary != metadata.summary)
                                {
                                    return Err(Error::Unsupported(
                                        "unmaterialized private receipt",
                                    ));
                                }
                                Some(FileLayout::LazyImmutable)
                            }
                        } else {
                            None
                        };
                        Classified::Fresh {
                            authority: record.entry.authority,
                            base: record.entry.base.clone(),
                            kind: record.entry.kind,
                            file,
                            child_binding: None,
                        }
                    } else {
                        let key = NodeKey {
                            node: metadata.node,
                            revision: metadata.revision,
                            ..scope
                        };
                        future_policy.authorize(&key, Access::Read).await?;
                        let node = complete.restore_node_metadata(&key).await?;
                        future_policy.authorize(&key, Access::Read).await?;
                        let expected = if metadata.summary.kind == Kind::Directory {
                            NodeKind::Directory
                        } else {
                            NodeKind::File
                        };
                        if node.kind != expected
                            || node.kind == NodeKind::File
                                && (node.size != metadata.summary.size
                                    || (node.mode & 0o111 != 0) != metadata.summary.executable)
                        {
                            return Err(Error::InvalidScope);
                        }
                        Classified::Fresh {
                            authority: key,
                            file: (node.kind == NodeKind::File)
                                .then_some(FileLayout::LazyImmutable),
                            kind: node.kind,
                            base: Some(node),
                            child_binding: None,
                        }
                    };
                    classes.insert(path.as_str(), class);
                }
                if direct_children
                    .keys()
                    .any(|scope| !used_children.contains(scope))
                {
                    return Err(Error::Unsupported("removal of retained child region"));
                }
                let fresh_count = classes
                    .values()
                    .filter(|class| matches!(class, Classified::Fresh { .. }))
                    .count();
                let stable_root = old_root.entry.inode == 1;
                let additional = fresh_count
                    .checked_sub(usize::from(stable_root))
                    .ok_or(Error::InvalidScope)?;
                if self
                    .snapshot
                    .nodes
                    .len()
                    .checked_add(additional)
                    .is_none_or(|count| count > limits.max_inodes)
                {
                    return Err(core::Error::ResourceLimit.into());
                }
                let additional =
                    u64::try_from(additional).map_err(|_count| core::Error::ResourceLimit)?;
                let next_inode = self
                    .snapshot
                    .next_inode
                    .checked_add(additional)
                    .ok_or(core::Error::ResourceLimit)?;
                let mut next = self.snapshot.next_inode;
                let mut ids = BTreeMap::new();
                for (path, class) in &classes {
                    budget.node()?;
                    let id = match class {
                        Classified::Child { inode, .. } => *inode,
                        Classified::Fresh { .. } if path.is_empty() && stable_root => 1,
                        Classified::Fresh { .. } => {
                            let id = next;
                            next += 1;
                            id
                        }
                    };
                    ids.insert(*path, id);
                }
                if next != next_inode {
                    return Err(Error::InvalidScope);
                }
                let root_inode = *ids.get("").ok_or(Error::InvalidScope)?;
                let mut fresh = BTreeMap::new();
                let mut children = Vec::new();
                for (path, class) in classes {
                    budget.node()?;
                    let inode = ids[path];
                    let parent_path = split(path).0;
                    let parent = if path.is_empty() {
                        if stable_root {
                            1
                        } else {
                            old_root.entry.parent
                        }
                    } else {
                        *ids.get(parent_path).ok_or(Error::InvalidScope)?
                    };
                    match class {
                        Classified::Fresh {
                            authority,
                            base,
                            kind,
                            file,
                            child_binding,
                        } => {
                            if base.as_ref().is_some_and(|base| base.key != authority) {
                                return Err(Error::InvalidScope);
                            }
                            fresh.insert(
                                inode,
                                LayoutNode {
                                    path: self.candidate[path].path.as_str(),
                                    entry: Entry {
                                        inode,
                                        parent,
                                        kind,
                                        authority,
                                        base,
                                    },
                                    names: (kind == NodeKind::Directory && child_binding.is_none())
                                        .then(Vec::new),
                                    file,
                                    child_binding,
                                },
                            );
                        }
                        Classified::Child {
                            scope,
                            inode,
                            old_parent,
                            target_link,
                        } => {
                            children.push(ChildReparent {
                                scope,
                                inode,
                                old_parent,
                                new_parent: parent,
                                target_link,
                            });
                        }
                    }
                }
                for (path, metadata) in &self.candidate {
                    if path.is_empty() {
                        continue;
                    }
                    budget.node()?;
                    let parent_path = split(path).0;
                    let parent = fresh
                        .get_mut(ids.get(parent_path).ok_or(Error::InvalidScope)?)
                        .ok_or(Error::Unsupported("candidate crosses child boundary"))?;
                    let names = parent.names.as_mut().ok_or(Error::InvalidScope)?;
                    if names.len() == limits.max_directory_entries {
                        return Err(core::Error::ResourceLimit.into());
                    }
                    let spelling = split(&metadata.path).1.as_bytes();
                    budget.names(spelling.len())?;
                    names.push((spelling.to_vec(), ids[path.as_str()]));
                }
                let parent_patch = if stable_root {
                    None
                } else {
                    let parent = records
                        .get(&old_root.entry.parent)
                        .ok_or(Error::InvalidScope)?;
                    let matches: Vec<_> = parent
                        .names
                        .as_ref()
                        .ok_or(Error::InvalidScope)?
                        .iter()
                        .filter(|(_, inode)| *inode == old_root.entry.inode)
                        .collect();
                    if matches.len() != 1 {
                        return Err(Error::InvalidScope);
                    }
                    Some(ParentPatch {
                        parent: parent.entry.inode,
                        spelling: &matches[0].0,
                        old_child: old_root.entry.inode,
                        new_child: root_inode,
                    })
                };
                let mut retire = Vec::new();
                for record in &self.snapshot.nodes {
                    budget.node()?;
                    if record.attached
                        && record.entry.inode != 1
                        && owners[&record.entry.inode] == scope
                    {
                        retire.push(record.entry.inode);
                    }
                }
                self.validate_source().await?;
                policy.authorize_target_head(&target.proof).await?;
                if self.freeze.namespace().mutation_epoch()? != self.source_epoch {
                    return Err(Error::StaleSource);
                }
                Ok(PreparedNamespaceLayout {
                    source: self,
                    binding,
                    source_generation: self.snapshot.generation,
                    source_epoch: self.source_epoch,
                    source_next_inode: self.snapshot.next_inode,
                    next_inode,
                    next_generation: self
                        .snapshot
                        .generation
                        .checked_add(1)
                        .ok_or(core::Error::ResourceLimit)?,
                    root_inode,
                    fresh,
                    retire,
                    children,
                    parent_patch,
                    projections,
                })
            })
            .await
    }
}

fn split(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}
fn reserve_source(plan: &PreparedTransition, provenance: usize, budget: &mut Budget) -> Result<()> {
    budget.paths(provenance)?;
    budget.paths(plan.snapshot.backend_state.len())?;
    for record in &plan.snapshot.nodes {
        budget.node()?;
        if let Some(names) = &record.names {
            for (name, _) in names {
                budget.node()?;
                budget.names(name.len())?;
            }
        }
    }
    for file in plan.snapshot.files.values() {
        budget.node()?;
        for _ in &file.extents {
            budget.node()?;
        }
    }
    for metadata in plan.candidate.values() {
        budget.node()?;
        budget.paths(metadata.path.len())?;
    }
    Ok(())
}
