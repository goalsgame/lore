// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact active association commit, retained across the paired namespace swap.

use std::sync::MutexGuard;

use super::*;

pub(crate) struct SuccessorCommitGuard<'a> {
    namespace: MutexGuard<'a, State>,
    _source: MutexGuard<'a, State>,
    target: MutexGuard<'a, State>,
    reservation: &'a SuccessorReservation,
}
impl SuccessorReservation {
    pub(crate) fn prepare_commit<'a>(
        &'a self,
        owner: &OwnerClaim,
        issuer: &PreparedIssuer,
    ) -> Result<SuccessorCommitGuard<'a>> {
        if !Arc::ptr_eq(&self.namespace, &owner.namespace)
            || !Arc::ptr_eq(&self.owner, &owner.lineage.identity)
        {
            return Err(Error::Stale);
        }
        let namespace = self.namespace.state.lock().map_err(|_poison| Error::Io)?;
        if !committed(&namespace, &self.owner)
            || !same_backend(active_backend(&namespace), Some(&self.source))
        {
            return Err(Error::Stale);
        }
        let source = self.source.state.lock().map_err(|_poison| Error::Io)?;
        if !committed(&source, &self.owner) {
            return Err(Error::Stale);
        }
        let target = self.target.state.lock().map_err(|_poison| Error::Io)?;
        if !reserved(&target, &self.nonce) {
            return Err(Error::Stale);
        }
        let guard = SuccessorCommitGuard {
            namespace,
            _source: source,
            target,
            reservation: self,
        };
        guard.check_issuer(issuer)?;
        Ok(guard)
    }
}
impl SuccessorCommitGuard<'_> {
    pub(crate) fn check_issuer(&self, issuer: &PreparedIssuer) -> Result<()> {
        let reservation = self.reservation;
        if !Arc::ptr_eq(&reservation.namespace, &issuer.namespace)
            || !Arc::ptr_eq(&reservation.source, &issuer.source)
            || !Arc::ptr_eq(&reservation.target, &issuer.target)
            || !Arc::ptr_eq(&reservation.source_brand, &issuer.source_brand)
            || !Arc::ptr_eq(&reservation.target_brand, &issuer.target_brand)
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(crate) fn check_backends(
        &self,
        source: &RevisionBackend,
        target: &RevisionBackend,
    ) -> Result<()> {
        let reservation = self.reservation;
        if !Arc::ptr_eq(&reservation.source, source.publication_slot())
            || !Arc::ptr_eq(&reservation.target, target.publication_slot())
            || !Arc::ptr_eq(&reservation.source_brand, &source.selection_identity)
            || !Arc::ptr_eq(&reservation.target_brand, &target.selection_identity)
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    /// All fallible checks precede the first namespace/Model replacement.
    /// This changes only exact identity Arcs; displaced image resources are
    /// retained by the namespace installer until every guard has been released.
    pub(crate) fn commit(&mut self) {
        *self.target = State::Committed {
            owner: self.reservation.owner.clone(),
            active: None,
        };
        *self.namespace = State::Committed {
            owner: self.reservation.owner.clone(),
            active: Some(self.reservation.target.clone()),
        };
    }
}

#[cfg(test)]
mod tests;
