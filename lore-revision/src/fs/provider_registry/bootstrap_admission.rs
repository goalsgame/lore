// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Admission bound to one ready registry claim. These values confer neither a
//! Client token nor exclusive store, recovery or revision-publication authority.

use super::claim::ClaimPin;
use super::*;

mod command;
#[cfg(unix)]
pub(super) mod pending;
mod work;
pub(crate) use command::CommandExclusion;
pub use work::BoundWork;

/// A provider and load principal selected by one exact ready-entry decision.
pub struct ResolvedBootstrap {
    provider: Arc<dyn FilesystemProvider>,
    load: BoundLoad,
}
impl ResolvedBootstrap {
    /// The caller must retain the load alongside its command context. Retention
    /// alone does not wrap or authorize provider or store operations; those
    /// operations require their own admission and lifetime coverage.
    pub fn into_parts(self) -> (Arc<dyn FilesystemProvider>, BoundLoad) {
        (self.provider, self.load)
    }
}

/// Retains one admitted load and its exact reservation. An inherited pin shares
/// that principal; another explicit resolution creates a distinct principal.
/// This does not admit individual store operations or an exclusive handoff.
pub struct BoundLoad {
    principal: bootstrap_gate::LoadLease,
    location: WorkingLocation,
    claim: ClaimPin,
}
impl BoundLoad {
    pub fn location(&self) -> &WorkingLocation {
        &self.location
    }
    pub fn pin(&self) -> Self {
        Self {
            principal: self.principal.pin(),
            location: self.location.clone(),
            claim: self.claim.clone(),
        }
    }
}

pub(super) fn resolve(
    registry: &ProviderRegistry,
    location: &WorkingLocation,
) -> Result<ResolvedBootstrap> {
    {
        let state = registry.state.lock();
        if current_location(&state, location)?.provider.is_none() {
            return Err(Error::NotReady);
        }
    }
    metadata::verify(location)?;
    admit_verified(registry, location)
}

fn admit_verified(
    registry: &ProviderRegistry,
    location: &WorkingLocation,
) -> Result<ResolvedBootstrap> {
    // These outer slots must outlive the registry guard on errors and unwinding.
    // The upgraded claim can become the last owner concurrently; its destructor
    // reacquires the registry lock. Provider destruction must also occur outside.
    let mut claim = None;
    let mut principal = None;
    let mut provider = None;
    let result = {
        let state = registry.state.lock();
        (|| {
            let entry = current_location(&state, location)?;
            if entry.provider.is_none() {
                return Err(Error::NotReady);
            }
            claim = Some(entry.claim.upgrade().ok_or(Error::Stale)?);
            let retained = claim.as_ref().ok_or(Error::Stale)?;
            if !retained.matches(entry) {
                return Err(Error::Stale);
            }
            principal = Some(entry.gate.admit().map_err(admission_error)?);
            provider = entry.provider.clone();
            Ok(())
        })()
    };
    result?;
    let claim = claim.ok_or(Error::Stale)?;
    Ok(ResolvedBootstrap {
        provider: provider.ok_or(Error::NotReady)?,
        load: BoundLoad {
            principal: principal.ok_or(Error::Stale)?,
            location: location.clone(),
            claim: claim.pin(),
        },
    })
}
pub(super) fn admission_error(error: bootstrap_gate::Error) -> Error {
    match error {
        bootstrap_gate::Error::Busy => Error::AdmissionBusy,
        bootstrap_gate::Error::Retired => Error::Retired,
        bootstrap_gate::Error::Limit => Error::Capacity,
        bootstrap_gate::Error::Poisoned => Error::Poisoned,
        bootstrap_gate::Error::WrongGate | bootstrap_gate::Error::InvalidLimit => Error::Stale,
    }
}

#[cfg(test)]
mod tests;
