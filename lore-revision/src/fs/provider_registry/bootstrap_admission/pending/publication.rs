// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Qualified publication preflights outside locks and commits pointer/flag changes.

use super::*;

impl PendingQualification {
    fn check_registration(&self, registration: &Registration) -> Result<()> {
        if Arc::ptr_eq(self._claim.registration(), &registration.claim) {
            Ok(())
        } else {
            Err(Error::Stale)
        }
    }
    pub(crate) fn preflight_publication(
        &self,
        registration: &Registration,
        location: &WorkingLocation,
        provider: &Arc<dyn FilesystemProvider>,
    ) -> Result<()> {
        self.check_registration(registration)?;
        {
            let state = registration.claim.registry.state.lock();
            let entry = current_location(&state, location)?;
            if !registration.claim.matches(entry) {
                return Err(Error::Stale);
            }
            if entry.provider.is_some() {
                return Err(Error::AlreadyPublished);
            }
            let _gate = entry
                .gate
                .publication_commit(&self._exclusive, &self._issuer)
                .map_err(admission_error)?;
        }
        if !provider.holds_state() {
            return Err(Error::InvalidProvider);
        }
        metadata::verify(location)?;
        registration.check_pending_location(location)?;
        Ok(())
    }
    /// The caller retains all resources across this method and until AFTER its
    /// outer Record arbitration guard is released. This performs no IO/callback.
    pub(crate) fn commit_publication(
        &mut self,
        registration: &Registration,
        location: &WorkingLocation,
        provider: &mut Option<Arc<dyn FilesystemProvider>>,
        retire_armed: &mut bool,
    ) -> Result<()> {
        self.check_registration(registration)?;
        if provider.is_none() || !*retire_armed {
            return Err(Error::Stale);
        }
        let mut state = registration.claim.registry.state.lock();
        current_location(&state, location)?;
        let entry = state
            .entries
            .get_mut(registration.claim.target.path())
            .filter(|entry| registration.claim.matches(entry))
            .ok_or(Error::Stale)?;
        if entry.provider.is_some() {
            return Err(Error::AlreadyPublished);
        }
        let mut gate = entry
            .gate
            .publication_commit(&self._exclusive, &self._issuer)
            .map_err(admission_error)?;
        // Every check has completed. These preallocated pointer/flag writes are
        // infallible; no provider, claim, lease, or parent drops under the guards.
        entry.provider = provider.take();
        *retire_armed = false;
        gate.open_ready();
        Ok(())
    }
}
