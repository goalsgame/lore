// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Registered-version metadata only; this reader never allocates a link route.

use super::*;
use crate::overlay::AccessGate;
use crate::transition_prepare::Budget;
use crate::transition_prepare::ScopeView;

impl RevisionBackend {
    pub(crate) async fn inventory_registered_view(
        &self,
        scope: NodeKey,
        key: NodeKey,
        policy: Arc<dyn AccessGate>,
        budget: &mut Budget,
    ) -> Result<ScopeView, Error> {
        let fact = self.prove_owner_membership(&key, budget).await?;
        if !fact.matches_key(self, &key)
            || fact.original_scope().or(fact.boundary_source_scope()) != Some(scope)
        {
            return Err(Error::PermissionDenied);
        }
        let registered = self.owner(&key)?;
        let view = self
            .transition_view(scope, key.revision, policy, budget)
            .await?;
        if view.head.root != Some(registered.source_root) || view.head.revision != key.revision {
            return Err(Error::Stale);
        }
        let current = self.owner(&key)?;
        if current.source_root != registered.source_root
            || !Arc::ptr_eq(&current.state, &registered.state)
        {
            return Err(Error::Stale);
        }
        Ok(view)
    }

    /// Find a child route only in the exact registered parent version. A pin or
    /// raw node ID shared by another version never substitutes for a binding.
    pub(crate) fn inventory_parent_link(
        &self,
        scope: NodeKey,
        revision: Address,
        child: NodeKey,
        budget: &mut Budget,
    ) -> Result<NodeKey, Error> {
        self.owner(&scope)?;
        self.owner(&child)?;
        let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
        let parent = owners
            .by_exposure
            .get(&scope.exposure)
            .ok_or(Error::Stale)?;
        let original = owners
            .by_exposure
            .get(&child.exposure)
            .ok_or(Error::Stale)?;
        if scopes::root(parent) != scope || scopes::root(original) != child {
            return Err(Error::PermissionDenied);
        }
        if revision != scope.revision && !owners.views.contains_key(&(scope.exposure, revision)) {
            return Err(Error::Stale);
        }
        let mut found = None;
        if revision == scope.revision {
            for ((exposure, node), target) in &owners.links {
                budget.node()?;
                if *exposure == scope.exposure
                    && *target == child.exposure
                    && found.replace(*node).is_some()
                {
                    return Err(Error::Unsupported);
                }
            }
        } else {
            for ((exposure, version, node), target) in &owners.bindings {
                budget.node()?;
                if *exposure == scope.exposure
                    && *version == revision
                    && *target == child.exposure
                    && found.replace(*node).is_some()
                {
                    return Err(Error::Unsupported);
                }
            }
        }
        Ok(NodeKey {
            revision,
            node: found.ok_or(Error::Unsupported)?,
            ..scope
        })
    }
}
