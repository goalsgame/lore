// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Final metadata epoch admission. Acquire after namespace, routing and owners.
use super::*;

pub(crate) struct GateInstallGuard<'a> {
    state: MutexGuard<'a, State>,
    gate: &'a Arc<MutationGate>,
    next: u64,
}
impl Seal {
    /// This holds the final lock; no callback, allocation, I/O or await follows.
    pub(crate) fn prepare_install(&self, expected: u64) -> Result<GateInstallGuard<'_>> {
        let state = self.gate.state.lock().map_err(|_poison| Error::Io)?;
        if state.sealed != Some(self.id) || state.epoch != expected {
            return Err(Error::Stale);
        }
        let next = expected.checked_add(1).ok_or(Error::ResourceLimit)?;
        Ok(GateInstallGuard {
            state,
            gate: &self.gate,
            next,
        })
    }
    pub(crate) fn matches_gate(&self, gate: &Arc<MutationGate>) -> bool {
        Arc::ptr_eq(&self.gate, gate)
    }
}
impl GateInstallGuard<'_> {
    pub(crate) fn matches_gate(&self, gate: &Arc<MutationGate>) -> bool {
        Arc::ptr_eq(self.gate, gate)
    }
    pub(crate) fn matches_epoch(&self, expected: u64) -> bool {
        self.state.epoch == expected
    }
    pub(crate) fn commit(&mut self) {
        self.state.epoch = self.next;
    }
}

#[cfg(test)]
#[path = "install/tests.rs"]
mod tests;
