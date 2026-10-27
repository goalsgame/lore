// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact allocator admission for one privately reserved complete successor.
//! Locks source then target Owners after publication slots, before metadata gate.
use std::sync::MutexGuard;

use super::*;
use crate::publication_ownership::successor::SuccessorCommitGuard;

pub(crate) struct RevisionInstallGuard<'a> {
    source: MutexGuard<'a, Owners>,
    target: MutexGuard<'a, Owners>,
}
impl RevisionBackend {
    pub(crate) fn prepare_install_owners<'a>(
        &'a self,
        target: &'a RevisionBackend,
        ownership: &SuccessorCommitGuard<'_>,
    ) -> Result<RevisionInstallGuard<'a>, Error> {
        if std::ptr::eq(self, target)
            || !Arc::ptr_eq(&self.mutation_gate, &target.mutation_gate)
            || !Arc::ptr_eq(&self.execution, &target.execution)
            || self.root != target.root
        {
            return Err(Error::Stale);
        }
        ownership.check_backends(self, target)?;
        let source = self.owners.lock().map_err(|_poison| Error::Io)?;
        let successor = target.owners.lock().map_err(|_poison| Error::Io)?;
        if source.allocation != AllocationState::Active
            || successor.allocation != AllocationState::Prepared
            || !source.provenance_complete
            || !successor.provenance_complete
            || successor.next < source.next
            || successor.by_exposure.len() < source.by_exposure.len()
        {
            return Err(Error::Stale);
        }
        Ok(RevisionInstallGuard {
            source,
            target: successor,
        })
    }
}
impl RevisionInstallGuard<'_> {
    /// All validation and lock acquisition has completed before namespace swap.
    pub(crate) fn commit(&mut self) {
        self.source.allocation = AllocationState::Retired;
        self.target.allocation = AllocationState::Active;
    }
}
