// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Publication ownership and reservation retention have separate lifetimes.
//! Retention pins cannot publish a provider or regain retired admission.

use std::sync::Arc;
use std::sync::Weak;

use bootstrap_gate::Gate;

use super::*;

pub(super) struct RegistrationClaim {
    pub(super) registry: Arc<ProviderRegistry>,
    pub(super) target: CanonicalTarget,
    pub(super) token: u64,
    pub(super) gate: Arc<Gate>,
}
#[derive(Clone)]
pub(super) struct ClaimPin {
    _claim: Arc<RegistrationClaim>,
}
impl ClaimPin {
    pub(super) fn registration(&self) -> &Arc<RegistrationClaim> {
        &self._claim
    }
}
impl RegistrationClaim {
    pub(super) fn new(
        registry: Arc<ProviderRegistry>,
        target: CanonicalTarget,
        token: u64,
        gate: Arc<Gate>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            target,
            token,
            gate,
        })
    }
    pub(super) fn pin(self: &Arc<Self>) -> ClaimPin {
        ClaimPin {
            _claim: self.clone(),
        }
    }
    pub(super) fn matches(self: &Arc<Self>, entry: &Entry) -> bool {
        entry.token == self.token
            && Weak::ptr_eq(&entry.claim, &Arc::downgrade(self))
            && Arc::ptr_eq(&entry.gate, &self.gate)
    }
    pub(super) fn retire(self: &Arc<Self>) -> Result<()> {
        let (removed, result) = {
            let mut state = self.registry.state.lock();
            let entry = state
                .entries
                .get_mut(self.target.path())
                .filter(|entry| self.matches(entry))
                .ok_or(Error::Stale)?;
            entry.retired = true;
            let removed = entry.provider.take();
            // Registry precedes gate. Even a poisoned gate must not leave READY
            // published, and provider destruction never runs under either lock.
            let result = entry.gate.retire().map_err(|_poison| Error::Poisoned);
            (removed, result)
        };
        drop(removed);
        result
    }
}
impl Drop for RegistrationClaim {
    fn drop(&mut self) {
        self.registry.remove(&self.target, self.token);
    }
}

#[cfg(test)]
mod tests;
