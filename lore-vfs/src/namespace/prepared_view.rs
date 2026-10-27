// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! An unpublished effective gate derived only from an exact prepared layout.
#![allow(
    dead_code,
    reason = "Prepared views have no namespace installation caller."
)]

use super::*;
use crate::transition_prepare::Budget;
use crate::transition_prepare::PreparedNamespaceLayout;
use crate::transition_prepare::{self};

pub(crate) struct PreparedEffectiveView {
    backend: Arc<dyn Backend>,
    policy: Arc<dyn AccessGate>,
    gate: Arc<BackendGate>,
}
impl PreparedEffectiveView {
    pub(crate) fn matches_binding(
        &self,
        binding: &complete_binding::PreparedPolicyBinding,
    ) -> bool {
        let backend: Arc<dyn Backend> = binding.prepared().backend();
        Arc::ptr_eq(&self.backend, &backend) && Arc::ptr_eq(&self.policy, &binding.policy())
    }
    pub(super) fn into_binding(self) -> Arc<NamespaceBinding> {
        Arc::new(NamespaceBinding {
            backend: self.backend,
            gate: self.gate,
            policy: self.policy,
        })
    }
    pub(crate) fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }
    pub(crate) fn gate(&self) -> Arc<dyn AccessGate> {
        self.gate.clone()
    }
}
impl Namespace {
    pub(crate) async fn prepare_layout_view(
        self: &Arc<Self>,
        layout: &PreparedNamespaceLayout<'_>,
        budget: &mut Budget,
    ) -> transition_prepare::Result<PreparedEffectiveView> {
        layout.check_namespace(self)?;
        layout.validate_source().await?;
        let binding = layout.prepared_binding();
        let prepared = binding.prepared();
        self.check_complete_source(prepared)?;
        let backend = prepared.backend();
        let policy = binding.policy();
        let scope = prepared.scope();
        let head = prepared.head();
        let source = layout.source_underlays();
        let count = source
            .len()
            .checked_add(usize::from(!source.contains_key(&scope)))
            .filter(|count| *count <= self.limits.max_inodes.min(crate::core::MAX_OWNING_SCOPES))
            .ok_or(Error::ResourceLimit)?;
        let mut underlays = BTreeMap::new();
        for (source_scope, source_head) in source {
            budget.node()?;
            budget.paths(std::mem::size_of::<(NodeKey, crate::core::ScopeHead)>())?;
            if source_head.root.is_none() {
                return Err(transition_prepare::Error::Unsupported(
                    "writable absent underlay view",
                ));
            }
            if underlays
                .insert(
                    (source_scope.repository, source_scope.exposure),
                    (*source_scope, source_head.clone()),
                )
                .is_some()
            {
                return Err(transition_prepare::Error::InvalidScope);
            }
        }
        if head.root.is_none() {
            return Err(transition_prepare::Error::Unsupported(
                "writable absent underlay view",
            ));
        }
        let identity = (scope.repository, scope.exposure);
        if underlays
            .get(&identity)
            .is_some_and(|(existing, _)| *existing != scope)
        {
            return Err(transition_prepare::Error::InvalidScope);
        }
        if !underlays.contains_key(&identity) {
            budget.node()?;
            budget.paths(std::mem::size_of::<(NodeKey, crate::core::ScopeHead)>())?;
        }
        underlays.insert(identity, (scope, head.clone()));
        if underlays.len() != count {
            return Err(transition_prepare::Error::InvalidScope);
        }
        for (scope, head) in underlays.values() {
            policy.authorize(scope, Access::Read).await?;
            if backend.owning_scope(scope).await?.root != *scope {
                return Err(transition_prepare::Error::InvalidScope);
            }
            let anchor = backend.underlay_view(scope, head).await?;
            policy.authorize(&anchor, Access::Read).await?;
            backend.revalidate(&anchor, false).await?;
            policy.authorize(scope, Access::Read).await?;
            policy.authorize(&anchor, Access::Read).await?;
        }
        layout.validate_source().await?;
        self.check_complete_source(prepared)?;
        budget.node()?;
        let gate = Arc::new(BackendGate {
            underlays,
            backend: backend.clone(),
            gate: policy.clone(),
        });
        Ok(PreparedEffectiveView {
            backend,
            policy,
            gate,
        })
    }

    pub(crate) fn attach_prepared_file(&self, file: OverlayFile) -> OverlayFile {
        file.with_mutation_gate(self.mutation_gate.clone())
            .with_operation_freeze(self.operation_freeze.clone())
    }
}
