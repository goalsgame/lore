// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Discover exact pinned child states with the ordinary node/link resolvers.
//! Permission-denied mounts remain opaque; their policy travels with the import.

use std::collections::BTreeSet;
use std::sync::Arc;

use lore_base::types::Address;
use lore_base::types::Hash;
use lore_base::types::RepositoryId;

use super::ImportLimits;
use super::ImportScope;
use super::failure;
use crate::node::Node;
use crate::node::NodeID;
use crate::node::ROOT_NODE;
use crate::repository::RepositoryContext;
use crate::repository::working_bootstrap::Error;
use crate::repository::working_bootstrap::Result;
// The ordinary revision-tree link resolver uses this same depth bound.
use crate::state::MAX_LINK_DEPTH;
use crate::state::State;
use crate::state::StateNodeChildrenIterator;

type ScopeKey = (RepositoryId, Address, NodeID);
struct Pending {
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    root: NodeID,
    ancestors: Vec<ScopeKey>,
}
struct Proof {
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    revision: Hash,
    node: NodeID,
    pin: Address,
    root: NodeID,
    target: RepositoryId,
    allowed: bool,
}
pub(super) struct LinkedScopes {
    pub(super) scopes: Vec<ImportScope>,
    proofs: Vec<Proof>,
}
impl LinkedScopes {
    pub(super) async fn revalidate(&self) -> Result<()> {
        for scope in &self.scopes {
            if scope.state.revision() != scope.revision {
                return Err(Error::InvalidSelection);
            }
        }
        for proof in &self.proofs {
            if proof.repository.can_read_link(proof.target) != proof.allowed
                || proof.state.revision() != proof.revision
            {
                return Err(Error::Preparation(
                    "linked import authorization changed".into(),
                ));
            }
            let node = proof
                .state
                .node(proof.repository.clone(), proof.node)
                .await
                .map_err(failure)?;
            if !node.is_link() || node.address != proof.pin || node.linked_node().node != proof.root
            {
                return Err(Error::InvalidSelection);
            }
        }
        Ok(())
    }
}
fn key(repository: &RepositoryContext, state: &State, root: NodeID) -> ScopeKey {
    // States are addressed by a zero-context signature within their repository
    // partition. The original full link address is retained separately in Proof.
    (
        repository.id,
        Address::zero_context_hash(state.revision()),
        root,
    )
}
fn push<T>(values: &mut Vec<T>, value: T) -> Result<()> {
    values
        .try_reserve(1)
        .map_err(|_capacity| Error::InvalidLimit)?;
    values.push(value);
    Ok(())
}

pub(super) async fn collect(
    source: Arc<RepositoryContext>,
    revision: Hash,
    limits: ImportLimits,
) -> Result<LinkedScopes> {
    if source.id.is_zero() || limits.max_scopes == 0 || limits.max_nodes == 0 {
        return Err(Error::InvalidLimit);
    }
    let state = State::deserialize(source.clone(), revision)
        .await
        .map_err(failure)?;
    let mut scheduled = BTreeSet::new();
    scheduled.insert(key(&source, &state, ROOT_NODE));
    let mut pending = Vec::new();
    push(
        &mut pending,
        Pending {
            repository: source,
            state,
            root: ROOT_NODE,
            ancestors: Vec::new(),
        },
    )?;
    let mut visited = BTreeSet::new();
    let mut copied_states = BTreeSet::new();
    let mut scopes = Vec::new();
    let mut proofs = Vec::new();
    let mut nodes = 0usize;
    while let Some(mut scope) = pending.pop() {
        let current = key(&scope.repository, &scope.state, scope.root);
        if scope.ancestors.contains(&current) {
            return Err(Error::InvalidSelection);
        }
        if !visited.insert(current) {
            continue;
        }
        if visited.len() > limits.max_scopes || scope.ancestors.len() > MAX_LINK_DEPTH {
            return Err(Error::InvalidLimit);
        }
        scope.ancestors.push(current);
        if copied_states.insert((scope.repository.id, scope.state.revision())) {
            push(
                &mut scopes,
                ImportScope {
                    repository: scope.repository.clone(),
                    state: scope.state.clone(),
                    revision: scope.state.revision(),
                },
            )?;
        }
        let mut walk: Vec<(NodeID, Node)> = Vec::new();
        let root = scope
            .state
            .node(scope.repository.clone(), scope.root)
            .await
            .map_err(failure)?;
        push(&mut walk, (scope.root, root))?;
        while let Some((id, node)) = walk.pop() {
            nodes = nodes
                .checked_add(1)
                .filter(|count| *count <= limits.max_nodes)
                .ok_or(Error::InvalidLimit)?;
            if node.is_discarded() || node.is_staged_delete() {
                continue;
            }
            if node.is_link() {
                let link = node.linked_node();
                if link.repository.is_zero() || !link.is_valid_or_root() {
                    return Err(Error::InvalidSelection);
                }
                let allowed = scope.repository.can_read_link(link.repository);
                push(
                    &mut proofs,
                    Proof {
                        repository: scope.repository.clone(),
                        state: scope.state.clone(),
                        revision: scope.state.revision(),
                        node: id,
                        pin: node.address,
                        root: link.node,
                        target: link.repository,
                        allowed,
                    },
                )?;
                if !allowed {
                    continue;
                }
                if scope.ancestors.len() > MAX_LINK_DEPTH {
                    return Err(Error::InvalidLimit);
                }
                let child = (
                    link.repository,
                    Address::zero_context_hash(link.revision),
                    link.node,
                );
                if scope.ancestors.contains(&child) {
                    return Err(Error::InvalidSelection);
                }
                if scheduled.contains(&child) {
                    continue;
                }
                let minimum_nodes = nodes
                    .checked_add(walk.len())
                    .and_then(|count| count.checked_add(pending.len()))
                    .and_then(|count| count.checked_add(1));
                if scheduled.len() >= limits.max_scopes
                    || minimum_nodes.is_none_or(|count| count > limits.max_nodes)
                {
                    return Err(Error::InvalidLimit);
                }
                // This is the existing pinned link resolver, not a branch lookup.
                let (repository, state) = link.open(&scope.repository).await.map_err(failure)?;
                if !scope.repository.can_read_link(link.repository)
                    || repository.id != link.repository
                    || state.revision() != link.revision
                {
                    return Err(Error::InvalidSelection);
                }
                scheduled.insert(child);
                push(
                    &mut pending,
                    Pending {
                        repository,
                        state,
                        root: link.node,
                        ancestors: scope.ancestors.clone(),
                    },
                )?;
            } else if node.is_directory() {
                let mut children = StateNodeChildrenIterator::new(
                    scope.state.clone(),
                    scope.repository.clone(),
                    id,
                )
                .await
                .map_err(failure)?;
                while let Some(child) = children.next().await.map_err(failure)? {
                    if nodes
                        .checked_add(walk.len())
                        .is_none_or(|count| count >= limits.max_nodes)
                    {
                        return Err(Error::InvalidLimit);
                    }
                    push(&mut walk, child)?;
                }
            }
        }
    }
    let result = LinkedScopes { scopes, proofs };
    result.revalidate().await?;
    Ok(result)
}

#[cfg(test)]
mod tests;
