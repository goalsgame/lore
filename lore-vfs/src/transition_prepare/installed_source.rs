// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Identify the live scope root through exact registered underlay membership.
//! Logical held heads remain independent from each record's immutable base.
use super::*;
use crate::core::Backend;
use crate::overlay::Access;
use crate::overlay::AccessGate;
use crate::revision::OwnerMembershipKind;

async fn registered_root(
    snapshot: &NamespaceSnapshot,
    backend: &RevisionBackend,
    scope: NodeKey,
    policy: &Arc<dyn AccessGate>,
    budget: &mut Budget,
) -> Result<crate::core::Node> {
    let original = ScopeHead {
        revision: scope.revision,
        root: Some(scope.node),
    };
    let head = snapshot.active_underlays.get(&scope).unwrap_or(&original);
    if head != &original && !snapshot.baselines.contains_key(&scope) {
        return Err(Error::InvalidScope);
    }
    let node = head
        .root
        .ok_or(Error::Unsupported("writable absent source underlay"))?;
    let expected = NodeKey {
        revision: head.revision,
        node,
        ..scope
    };
    policy.authorize(&scope, Access::Read).await?;
    let root = backend.underlay_view(&scope, head).await?;
    if root != expected {
        return Err(Error::InvalidScope);
    }
    policy.authorize(&root, Access::Read).await?;
    let fact = backend.prove_owner_membership(&root, budget).await?;
    if !fact.matches_key(backend, &root)
        || fact.kind() != OwnerMembershipKind::Member
        || fact.original_scope() != Some(scope)
    {
        return Err(Error::InvalidScope);
    }
    let root_node = fact.node().ok_or(Error::InvalidScope)?;
    if root_node.key != root {
        return Err(Error::InvalidScope);
    }
    policy.authorize(&root, Access::Read).await?;
    policy.authorize(&scope, Access::Read).await?;
    Ok(root_node.clone())
}

pub(super) async fn selected_inode(
    snapshot: &NamespaceSnapshot,
    backend: &RevisionBackend,
    scope: NodeKey,
    policy: &Arc<dyn AccessGate>,
    budget: &mut Budget,
) -> Result<u64> {
    // Every installed scope must still be an exact registered version. This
    // neither substitutes its logical committed head nor grants target access.
    for installed in snapshot.active_underlays.keys() {
        budget.node()?;
        registered_root(snapshot, backend, *installed, policy, budget).await?;
    }
    let root = registered_root(snapshot, backend, scope, policy, budget).await?;
    let mut found = None;
    for record in &snapshot.nodes {
        budget.node()?;
        if record.attached
            && record.entry.authority == root.key
            && record.entry.kind == root.kind
            && record.entry.base.as_ref() == Some(&root)
            && found.replace(record.entry.inode).is_some()
        {
            return Err(Error::InvalidScope);
        }
    }
    found.ok_or(Error::InvalidScope)
}
