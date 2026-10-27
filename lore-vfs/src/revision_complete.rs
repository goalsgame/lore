// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Complete, independent immutable routing preparation. No namespace, policy,
//! allocation activation, installation permission, or writer freeze is retained.

use super::*;
use crate::core::ScopeHead;
use crate::transition_prepare::Budget;
use crate::transition_prepare::ScopeView;

pub(crate) struct PreparedCompleteBackend {
    backend: Arc<RevisionBackend>,
    provenance: Vec<u8>,
    original_root: Node,
    scope: NodeKey,
    head: ScopeHead,
    selected_root: Option<Node>,
    source_epoch: u64,
    source_brand: Arc<()>,
    execution: Arc<ExecutionContext>,
}
impl PreparedCompleteBackend {
    pub(crate) fn backend(&self) -> Arc<RevisionBackend> {
        self.backend.clone()
    }
    pub(crate) fn provenance(&self) -> &[u8] {
        &self.provenance
    }
    pub(crate) fn original_root(&self) -> &Node {
        &self.original_root
    }
    pub(crate) fn scope(&self) -> NodeKey {
        self.scope
    }
    pub(crate) fn head(&self) -> &ScopeHead {
        &self.head
    }
    pub(crate) fn selected_root(&self) -> Option<&Node> {
        self.selected_root.as_ref()
    }
    pub(crate) fn source_epoch(&self) -> u64 {
        self.source_epoch
    }
    pub(crate) fn matches_source(&self, backend: &RevisionBackend) -> bool {
        Arc::ptr_eq(&self.source_brand, &backend.selection_identity)
            && Arc::ptr_eq(&self.execution, &backend.execution)
            && self.original_root == backend.root
    }
}
impl RevisionBackend {
    pub(crate) async fn transition_complete_fork(
        &self,
        scope: NodeKey,
        origin: &ScopeView,
        target: &ScopeView,
        expected_epoch: u64,
        budget: &mut Budget,
    ) -> Result<PreparedCompleteBackend, Error> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async {
            for view in [origin, target] {
                if !view.entries.matches(&view.proof)
                    || !Arc::ptr_eq(&view.state, &view.proof.state)
                    || view.head != view.proof.head()
                    || view.proof.selection().original_scope() != scope
                    || view.info != origin.info
                {
                    return Err(Error::InvalidInput);
                }
                self.revalidate_target_head(&view.proof).await?;
            }
            let origin_head = ScopeHead {
                revision: scope.revision,
                root: Some(scope.node),
            };
            if origin.head != origin_head || target.head != origin_head && !target.committed {
                return Err(Error::InvalidInput);
            }
            let (mut records, existing_view, existing_bindings, floor) = {
                let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
                let owner = owners
                    .by_exposure
                    .get(&scope.exposure)
                    .ok_or(Error::Stale)?;
                if !owners.provenance_complete
                    || scopes::root(owner) != scope
                    || !Arc::ptr_eq(&owner.state, &origin.state)
                {
                    return Err(Error::InvalidInput);
                }
                if self.mutation_gate.epoch()? != expected_epoch {
                    return Err(Error::Stale);
                }
                for _ in owners.by_exposure.values() {
                    budget.node()?;
                }
                for _ in owners.views.values() {
                    budget.node()?;
                }
                for _ in owners.links.values() {
                    budget.node()?;
                }
                for _ in owners.bindings.values() {
                    budget.node()?;
                }
                for _record in &owners.provenance {
                    budget.node()?;
                }
                let size = checkpoint::encoded_size(&owners)?;
                if size > crate::core::MAX_BACKEND_STATE_SIZE {
                    return Err(Error::ResourceLimit);
                }
                budget.paths(size)?;
                (
                    owners.provenance.clone(),
                    owners
                        .views
                        .get(&(scope.exposure, target.head.revision))
                        .map(|view| view.root),
                    owners.bindings.clone(),
                    owners.next,
                )
            };
            if let Some(root) = existing_view
                && root != target.head.root
            {
                return Err(Error::Stale);
            }
            let (links, children) = self
                .transition_link_owners(scope, origin, target, budget)
                .await?;
            let mut child_scopes = BTreeMap::new();
            for child in children {
                budget.node()?;
                child_scopes.insert(child.exposure, scopes::root(&child));
            }
            if self.mutation_gate.epoch()? != expected_epoch {
                return Err(Error::Stale);
            }
            if target.head != origin_head && existing_view.is_none() {
                budget.node()?;
                records.push(checkpoint::Record::View {
                    scope,
                    head: target.head.clone(),
                });
            }
            if target.head != origin_head {
                for ((exposure, node), child) in links {
                    let source = NodeKey {
                        revision: target.head.revision,
                        node,
                        ..scope
                    };
                    if exposure != scope.exposure {
                        return Err(Error::InvalidInput);
                    }
                    if let Some(existing) =
                        existing_bindings.get(&(scope.exposure, target.head.revision, node))
                    {
                        if *existing != child {
                            return Err(Error::Stale);
                        }
                        continue;
                    }
                    budget.node()?;
                    records.push(checkpoint::Record::Binding {
                        source,
                        child: *child_scopes.get(&child).ok_or(Error::Stale)?,
                    });
                }
            }
            // Charge the actual encoded allocation before the codec reserves it.
            let size = records.iter().try_fold(8usize + 76, |sum, record| {
                sum.checked_add(record.size()).ok_or(Error::ResourceLimit)
            })?;
            if size > crate::core::MAX_BACKEND_STATE_SIZE {
                return Err(Error::ResourceLimit);
            }
            budget.paths(size)?;
            let provenance = checkpoint::encode(self.root.key, &records)?;
            let backend =
                checkpoint::prepare_bounded(self, &provenance, budget, AllocationState::Prepared)
                    .await?
                    .ok_or(Error::InvalidInput)?;
            if backend.root != self.root
                || backend.owners.lock().map_err(|_poison| Error::Io)?.next != floor
            {
                return Err(Error::Stale);
            }
            if self.mutation_gate.epoch()? != expected_epoch {
                return Err(Error::Stale);
            }
            self.revalidate_target_head(&target.proof).await?;
            if self.mutation_gate.epoch()? != expected_epoch {
                return Err(Error::Stale);
            }
            Ok(PreparedCompleteBackend {
                backend,
                provenance,
                original_root: self.root.clone(),
                scope,
                head: target.head.clone(),
                selected_root: target.proof.root().cloned(),
                source_epoch: expected_epoch,
                source_brand: self.selection_identity.clone(),
                execution: self.execution.clone(),
            })
        }))
        .await
    }
}
