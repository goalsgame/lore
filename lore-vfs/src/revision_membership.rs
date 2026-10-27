// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Issuer-bound owning-scope identity facts. They authorize neither reads nor
//! writes and are not a namespace binding or mutation capability.

use std::collections::BTreeSet;

use super::*;
use crate::transition_prepare::Budget;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerMembershipKind {
    Member,
    /// The raw link belongs to its source tree; no target was resolved.
    Boundary,
}
#[derive(Clone)]
pub(crate) struct OwnerMembership {
    issuer: Arc<()>,
    execution: Arc<ExecutionContext>,
    key: NodeKey,
    scope: NodeKey,
    kind: OwnerMembershipKind,
    node: Node,
}
impl OwnerMembership {
    pub(crate) fn kind(&self) -> OwnerMembershipKind {
        self.kind
    }
    pub(crate) fn key(&self) -> NodeKey {
        self.key
    }
    /// Only ordinary members provide an owning scope for equality decisions.
    pub(crate) fn original_scope(&self) -> Option<NodeKey> {
        (self.kind == OwnerMembershipKind::Member).then_some(self.scope)
    }
    /// Source-side identity only, never the link target's scope or authority.
    pub(crate) fn boundary_source_scope(&self) -> Option<NodeKey> {
        (self.kind == OwnerMembershipKind::Boundary).then_some(self.scope)
    }
    pub(crate) fn node(&self) -> Option<&Node> {
        (self.kind == OwnerMembershipKind::Member).then_some(&self.node)
    }
    pub(crate) fn matches_key(&self, backend: &RevisionBackend, key: &NodeKey) -> bool {
        self.key == *key
            && Arc::ptr_eq(&self.issuer, &backend.selection_identity)
            && Arc::ptr_eq(&self.execution, &backend.execution)
    }
    pub(crate) fn same_scope(&self, other: &Self, current: &RevisionBackend) -> bool {
        self.kind == OwnerMembershipKind::Member
            && other.kind == OwnerMembershipKind::Member
            && self.matches_key(current, &self.key)
            && other.matches_key(current, &other.key)
            && self.scope == other.scope
    }
}
impl RevisionBackend {
    /// Check a full key against a registered original/retained selected subtree.
    /// The caller supplies one cumulative metadata budget. Current access policy
    /// must be checked separately, including after this metadata work.
    pub(crate) async fn prove_owner_membership(
        &self,
        key: &NodeKey,
        budget: &mut Budget,
    ) -> Result<OwnerMembership, Error> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async {
            let owner = self.owner(key)?;
            let scope = {
                let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
                let original = owners.by_exposure.get(&key.exposure).ok_or(Error::Stale)?;
                if original.repository.id != key.repository {
                    return Err(Error::PermissionDenied);
                }
                scopes::root(original)
            };
            budget.node()?; // Reserve the returned fact before allocating traversal state.
            let mut seen = BTreeSet::new();
            let mut id = key.node;
            let mut depth = 0usize;
            let mut member = None;
            loop {
                budget.depth(depth)?;
                if seen.contains(&id) {
                    return Err(Error::Stale);
                }
                budget.node()?; // Count the visited metadata node.
                budget.node()?; // Bound the owned cycle-detection record.
                seen.insert(id);
                if depth.is_multiple_of(32) {
                    tokio::task::yield_now().await;
                }
                let node = owner
                    .state
                    .node(owner.repository.clone(), id)
                    .await
                    .map_err(state_error)?;
                target::validate_metadata(&node)?;
                if depth != 0 && (node.is_link() || !node.is_directory()) {
                    return Err(Error::PermissionDenied);
                }
                if depth == 0 {
                    member = Some(node);
                }
                if id == owner.source_root {
                    break;
                }
                if id == ROOT_NODE {
                    return Err(Error::PermissionDenied);
                }
                id = node.parent;
                depth = depth.checked_add(1).ok_or(Error::ResourceLimit)?;
            }
            // Owner versions are append-only; still recheck registration and the
            // current repository route after all potentially suspended reads.
            let current = self.owner(key)?;
            if current.source_root != owner.source_root
                || !Arc::ptr_eq(&current.state, &owner.state)
            {
                return Err(Error::Stale);
            }
            let member = member.ok_or(Error::InvalidInput)?;
            Ok(OwnerMembership {
                issuer: self.selection_identity.clone(),
                execution: self.execution.clone(),
                key: *key,
                scope,
                kind: if member.is_link() {
                    OwnerMembershipKind::Boundary
                } else {
                    OwnerMembershipKind::Member
                },
                node: describe(&owner, key.node, &member),
            })
        }))
        .await
    }
}
