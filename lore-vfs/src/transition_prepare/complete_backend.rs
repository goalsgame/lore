// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Independent read-only routing preparation; publication needs a separate
//! candidate authorization, metadata seal and durable instance-anchor protocol.

use super::*;
use crate::overlay::Access;
use crate::revision::PreparedCompleteBackend;

struct Admission<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}
impl PreparedTransition {
    pub(crate) async fn prepare_complete_backend(&self) -> Result<PreparedCompleteBackend> {
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
            .ok_or(Error::Unsupported("complete revision routing"))?;
        Box::pin(
            lore_base::runtime::LORE_CONTEXT.scope(backend.transition_execution(), async {
                self.validate_source().await?;
                let policy = self.source_routing.policy();
                let scope = self.intent.scope;
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
                if target.head != self.intent.target
                    || target.proof.selection().original_scope() != scope
                {
                    return Err(Error::StaleSource);
                }
                let complete = backend
                    .transition_complete_fork(
                        scope,
                        &origin,
                        &target,
                        self.source_epoch,
                        &mut budget,
                    )
                    .await?;
                self.validate_source().await?;
                policy.authorize(&scope, Access::Read).await?;
                policy.authorize_target_head(&target.proof).await?;
                if self.freeze.namespace().mutation_epoch()? != self.source_epoch {
                    return Err(Error::StaleSource);
                }
                Ok(complete)
            }),
        )
        .await
    }
}
fn reserve_retained(plan: &PreparedTransition, budget: &mut Budget) -> Result<()> {
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
        for _extent in &file.extents {
            budget.node()?;
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
    for _ in plan.snapshot.baselines.values() {
        budget.node()?;
    }
    for _ in plan.snapshot.active_underlays.values() {
        budget.node()?;
    }
    Ok(())
}
