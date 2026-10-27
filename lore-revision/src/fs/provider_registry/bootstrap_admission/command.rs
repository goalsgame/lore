// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Pre-intent exclusion of other command loads and actual finite store work.

use super::*;

pub(crate) struct CommandExclusion {
    exclusive: bootstrap_gate::ExclusiveReservation,
    load: BoundLoad,
}
impl BoundLoad {
    pub(crate) fn exclude_command(&self) -> Result<CommandExclusion> {
        metadata::verify(&self.location)?;
        let claim = self.claim.registration();
        let mut exclusive = None;
        let result = {
            let state = claim.registry.state.lock();
            (|| {
                let entry = current_location(&state, &self.location)?;
                if !claim.matches(entry) {
                    return Err(Error::Stale);
                }
                if entry.provider.is_none() {
                    return Err(Error::NotReady);
                }
                exclusive = Some(
                    entry
                        .gate
                        .try_exclusive(&self.principal)
                        .map_err(admission_error)?,
                );
                Ok(())
            })()
        };
        result?;
        Ok(CommandExclusion {
            exclusive: exclusive.ok_or(Error::Stale)?,
            load: self.pin(),
        })
    }
}
impl CommandExclusion {
    pub(crate) fn quarantine(&self) -> Result<()> {
        let claim = self.load.claim.registration();
        // Gate poisoning does not prevent exact entry withdrawal. Confirm the
        // terminal registry state separately, with no provider destructor held.
        let _retirement = claim.retire();
        let state = claim.registry.state.lock();
        let entry = state
            .entries
            .get(self.load.location.target.path())
            .ok_or(Error::Stale)?;
        if !claim.matches(entry) || !entry.retired || entry.provider.is_some() {
            return Err(Error::Stale);
        }
        Ok(())
    }

    pub(crate) fn check(&self) -> Result<()> {
        // Bounded metadata IO precedes both guards. This checks the registered
        // canonical location, repository, instance and routing configuration;
        // it is not an atomicity guarantee against arbitrary OS-level edits.
        metadata::verify(&self.load.location)?;
        let claim = self.load.claim.registration();
        let state = claim.registry.state.lock();
        let entry = current_location(&state, &self.load.location)?;
        if !claim.matches(entry) {
            return Err(Error::Stale);
        }
        if entry.provider.is_none() {
            return Err(Error::NotReady);
        }
        // This checks only the exact nonce/issuer. It never opens READY or drops
        // resources while the paired registry and gate guards are held.
        let _gate = entry
            .gate
            .publication_commit(&self.exclusive, &self.load.principal)
            .map_err(admission_error)?;
        Ok(())
    }
}
