// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Weak running-Model association; request ownership lives in the external worker.

use lore_revision::fs::filesystem_provider::RootHoldOutcome;
use lore_revision::fs::filesystem_provider::RootHoldRequest;

use super::*;

impl WorkingProvider {
    pub fn with_model(mut self, model: &Arc<crate::linux_working::Model>) -> Result<Self, FsError> {
        if !Arc::ptr_eq(model.namespace(), &self.inner.namespace)
            || self
                .checkpoint
                .as_ref()
                .is_none_or(|owner| !Arc::ptr_eq(owner, &model.checkpoint_owner()))
        {
            return Err(FsError::internal(
                "Transactional provider requires its exact running Model",
            ));
        }
        self.transition_model = Some(Arc::downgrade(model));
        Ok(self)
    }
    pub(super) async fn hold_root(
        &self,
        request: RootHoldRequest,
    ) -> Result<RootHoldOutcome, FsError> {
        let model = self
            .transition_model
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| FsError::internal("Working root publication owner is unavailable"))?;
        model.transactional_root_hold(request).await
    }
}
