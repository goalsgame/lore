// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact immutable policy binding; neither authorization nor installation.
#![cfg_attr(
    not(any(test, feature = "test-util")),
    allow(
        dead_code,
        reason = "Complete backend binding has no live installation caller."
    )
)]

use super::*;
use crate::revision::PreparedCompleteBackend;
use crate::revision::RevisionBackend;

pub(crate) struct PreparedPolicyBinding {
    prepared: PreparedCompleteBackend,
    policy: Arc<dyn AccessGate>,
}
impl PreparedPolicyBinding {
    pub(crate) fn prepared(&self) -> &PreparedCompleteBackend {
        &self.prepared
    }
    pub(crate) fn backend(&self) -> Arc<dyn Backend> {
        self.prepared.backend()
    }
    pub(crate) fn policy(&self) -> Arc<dyn AccessGate> {
        self.policy.clone()
    }
}

impl Namespace {
    pub(super) fn check_complete_source(&self, prepared: &PreparedCompleteBackend) -> Result<()> {
        let binding = self.current_binding()?;
        let source = binding
            .backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(Error::Unsupported)?;
        if !prepared.matches_source(source)
            || prepared.original_root() != &self.original_root
            || !Arc::ptr_eq(&self.mutation_gate, &source.shared_mutation_gate())
            || !Arc::ptr_eq(
                &self.mutation_gate,
                &prepared.backend().shared_mutation_gate(),
            )
        {
            return Err(Error::InvalidInput.into());
        }
        if self.mutation_epoch()? != prepared.source_epoch() {
            return Err(Error::Stale.into());
        }
        Ok(())
    }

    /// Bind only a proven complete derivative of this exact source. The result
    /// grants no candidate edits, namespace installation or anchor publication.
    pub(crate) fn bind_complete_backend(
        &self,
        prepared: PreparedCompleteBackend,
    ) -> Result<PreparedPolicyBinding> {
        self.check_complete_source(&prepared)?;
        let policy = self.gate_factory.bind(prepared.backend())?;
        // A custom synchronous factory may invoke metadata callbacks. Never
        // return a binding after the source changed during that callback.
        self.check_complete_source(&prepared)?;
        Ok(PreparedPolicyBinding { prepared, policy })
    }
}

#[cfg(test)]
#[path = "complete_binding_tests.rs"]
mod tests;
