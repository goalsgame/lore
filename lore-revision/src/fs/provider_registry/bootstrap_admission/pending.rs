// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Internal pending exclusion only; no public permit or store authority escapes.
mod publication;

use super::*;

pub(crate) struct PendingQualification {
    _exclusive: bootstrap_gate::ExclusiveReservation,
    _issuer: bootstrap_gate::LoadLease,
    _claim: ClaimPin,
}
pub(crate) fn qualify(
    registration: &Arc<Registration>,
    location: &WorkingLocation,
) -> Result<PendingQualification> {
    let claim = registration.claim.pin();
    let mut issuer = None;
    let mut exclusive = None;
    let result = {
        let state = registration.claim.registry.state.lock();
        (|| {
            let entry = current_location(&state, location)?;
            if !registration.claim.matches(entry) {
                return Err(Error::Stale);
            }
            if entry.provider.is_some() {
                return Err(Error::AlreadyPublished);
            }
            issuer = Some(entry.gate.admit().map_err(admission_error)?);
            exclusive = Some(
                entry
                    .gate
                    .try_exclusive(issuer.as_ref().ok_or(Error::Stale)?)
                    .map_err(admission_error)?,
            );
            Ok(())
        })()
    };
    // Claim/principal/exclusive destruction never runs under the registry lock.
    result?;
    Ok(PendingQualification {
        _exclusive: exclusive.ok_or(Error::Stale)?,
        _issuer: issuer.ok_or(Error::Stale)?,
        _claim: claim,
    })
}
