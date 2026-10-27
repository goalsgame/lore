// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use super::*;
use crate::core::ParentScope;
use crate::core::ScopeHead;
use crate::core::ScopeInfo;
use crate::core::ScopePin;
use crate::core::ScopeProjection;
const MAX_SCOPE_PATH: usize = 32 * 1024;
pub(super) fn root(owner: &Owner) -> NodeKey {
    NodeKey {
        repository: owner.repository.id,
        revision: Address::zero_context_hash(owner.state.revision()),
        node: owner.source_root,
        exposure: owner.exposure,
    }
}
async fn path(owner: &Owner, node: NodeID) -> Result<String, Error> {
    let path = owner
        .state
        .node_path(owner.repository.clone(), node)
        .await
        .map_err(state_error)?;
    if path.len() > MAX_SCOPE_PATH {
        return Err(Error::ResourceLimit);
    }
    Ok(path)
}
pub(super) async fn info(backend: &RevisionBackend, key: &NodeKey) -> Result<ScopeInfo, Error> {
    let (current, _) = backend.node(key).await?;
    let owner = backend
        .owners
        .lock()
        .map_err(|_poison| Error::Io)?
        .by_exposure
        .get(&current.exposure)
        .ok_or(Error::Stale)?
        .clone();
    let source_path = path(&owner, owner.source_root).await?.into_bytes();
    let parent = {
        let owners = backend.owners.lock().map_err(|_poison| Error::Io)?;
        owners
            .links
            .iter()
            .find(|(_route, exposure)| **exposure == owner.exposure)
            .map(|((parent, link), _)| {
                let parent = owners.by_exposure.get(parent).ok_or(Error::Io)?;
                Ok(ParentScope {
                    scope: root(parent),
                    initial_revision: Address::zero_context_hash(parent.state.committed_revision()),
                    link: NodeKey {
                        node: *link,
                        ..root(parent)
                    },
                })
            })
            .transpose()?
    };
    Ok(ScopeInfo {
        root: root(&owner),
        initial_revision: Address::zero_context_hash(owner.state.committed_revision()),
        source_path,
        parent,
    })
}
fn owner(backend: &RevisionBackend, scope: &NodeKey) -> Result<Owner, Error> {
    backend.owner(scope)?;
    let owner = backend
        .owners
        .lock()
        .map_err(|_poison| Error::Io)?
        .by_exposure
        .get(&scope.exposure)
        .ok_or(Error::Stale)?
        .clone();
    if root(&owner) != *scope {
        return Err(Error::InvalidInput);
    }
    Ok(owner)
}
pub(super) async fn head(
    backend: &RevisionBackend,
    scope: &NodeKey,
    revision: Address,
) -> Result<ScopeHead, Error> {
    let owner = owner(backend, scope)?;
    if revision.context != scope.revision.context {
        return Err(Error::InvalidInput);
    }
    let source = path(&owner, owner.source_root).await?;
    let state = State::deserialize(owner.repository.clone(), revision.hash)
        .await
        .map_err(state_error)?;
    if revision != scope.revision
        && revision.hash != owner.state.committed_revision()
        && state.revision_number() == 0
    {
        return Err(Error::InvalidInput);
    }
    let root = match state
        .find_node_link(owner.repository.clone(), &source)
        .await
    {
        Ok(node) => {
            if node.repository != scope.repository || node.revision != revision.hash {
                return Err(Error::PermissionDenied);
            }
            let record = state
                .node(owner.repository.clone(), node.node)
                .await
                .map_err(state_error)?;
            if record.is_discarded() || record.is_link() {
                return Err(Error::InvalidInput);
            }
            Some(node.node)
        }
        Err(error) => match state_error(error) {
            Error::NotFound => None,
            error => return Err(error),
        },
    };
    Ok(ScopeHead { revision, root })
}
pub(super) async fn projection(
    backend: &RevisionBackend,
    scope: &NodeKey,
    revision: Address,
) -> Result<Option<ScopeProjection>, Error> {
    let Some(parent) = info(backend, scope).await?.parent else {
        return Ok(None);
    };
    let owner = owner(backend, &parent.scope)?;
    if revision.context != parent.scope.revision.context {
        return Err(Error::InvalidInput);
    }
    let path = path(&owner, parent.link.node).await?;
    let state = State::deserialize(owner.repository.clone(), revision.hash)
        .await
        .map_err(state_error)?;
    if revision != parent.scope.revision
        && revision.hash != owner.state.committed_revision()
        && state.revision_number() == 0
    {
        return Err(Error::InvalidInput);
    }
    let target = match state.find_node_link(owner.repository.clone(), &path).await {
        Ok(node) => {
            if node.repository != parent.scope.repository || node.revision != revision.hash {
                return Err(Error::PermissionDenied);
            }
            let node = state
                .node(owner.repository.clone(), node.node)
                .await
                .map_err(state_error)?;
            if node.is_discarded() {
                None
            } else if node.is_link() {
                let link = node.linked_node();
                Some(ScopePin {
                    repository: link.repository,
                    revision: Address::zero_context_hash(link.revision),
                    node: link.node,
                })
            } else {
                None
            }
        }
        Err(error) => match state_error(error) {
            Error::NotFound => None,
            error => return Err(error),
        },
    };
    Ok(Some(ScopeProjection {
        parent: parent.scope,
        head: revision,
        target,
    }))
}
