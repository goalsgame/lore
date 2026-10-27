// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Provisional ownership of one exact prepared target's runtime slot.
//! Source ownership remains unchanged. Reservations neither choose an active
//! successor nor authorize intent, anchor writes or installation. A coordinator
//! must select and revalidate a reservation through separate publication admission.
#![allow(
    dead_code,
    reason = "Successor reservations have no publication driver."
)]

mod commit;
pub(crate) use commit::SuccessorCommitGuard;

use super::*;
use crate::revision::PreparedCompleteBackend;
use crate::revision::RevisionBackend;

/// Only an opaque, validated complete backend can supply this identity capsule.
/// No backend, policy, Model or checkpoint resource is retained here.
pub(crate) struct PreparedIssuer {
    namespace: Arc<Slot>,
    source: Arc<Slot>,
    target: Arc<Slot>,
    source_brand: Arc<()>,
    target_brand: Arc<()>,
}
impl PreparedIssuer {
    pub(crate) fn from_complete(
        namespace: Arc<Slot>,
        source: &RevisionBackend,
        prepared: &PreparedCompleteBackend,
    ) -> Result<Self> {
        if !prepared.matches_source(source) {
            return Err(Error::Stale);
        }
        let target = prepared.backend();
        let source_slot = source.publication_slot();
        let target_slot = target.publication_slot();
        if Arc::ptr_eq(&namespace, source_slot)
            || Arc::ptr_eq(&namespace, target_slot)
            || Arc::ptr_eq(source_slot, target_slot)
        {
            return Err(Error::InvalidInput);
        }
        Ok(Self {
            namespace,
            source: source_slot.clone(),
            target: target_slot.clone(),
            source_brand: source.selection_identity.clone(),
            target_brand: target.selection_identity.clone(),
        })
    }
    pub(crate) fn check_backends(
        &self,
        namespace: &Arc<Slot>,
        source: &RevisionBackend,
        target: &RevisionBackend,
    ) -> Result<()> {
        if !Arc::ptr_eq(namespace, &self.namespace)
            || !Arc::ptr_eq(source.publication_slot(), &self.source)
            || !Arc::ptr_eq(target.publication_slot(), &self.target)
            || !Arc::ptr_eq(&source.selection_identity, &self.source_brand)
            || !Arc::ptr_eq(&target.selection_identity, &self.target_brand)
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
}

pub(crate) struct SuccessorReservation {
    namespace: Arc<Slot>,
    source: Arc<Slot>,
    target: Arc<Slot>,
    owner: Arc<Identity>,
    nonce: Arc<Identity>,
    source_brand: Arc<()>,
    target_brand: Arc<()>,
}
impl OwnerClaim {
    pub(crate) fn reserve_successor(
        &self,
        issuer: &PreparedIssuer,
    ) -> Result<SuccessorReservation> {
        if self.backend.is_none() {
            return Err(Error::Unsupported);
        }
        let source = &issuer.source;
        if !Arc::ptr_eq(&self.namespace, &issuer.namespace) {
            return Err(Error::Stale);
        }
        if Arc::ptr_eq(&issuer.namespace, &issuer.target) || Arc::ptr_eq(source, &issuer.target) {
            return Err(Error::InvalidInput);
        }
        let nonce = Arc::new(Identity);
        {
            let namespace = self.namespace.state.lock().map_err(|_poison| Error::Io)?;
            if !committed(&namespace, &self.lineage.identity)
                || !same_backend(active_backend(&namespace), Some(source))
            {
                return Err(Error::Stale);
            }
            // The namespace guard stabilizes source ownership. Do not hold two
            // backend slot guards, even for invalid cross-owner target attempts.
            {
                let source = source.state.lock().map_err(|_poison| Error::Io)?;
                if !committed(&source, &self.lineage.identity) {
                    return Err(Error::Stale);
                }
            }
            let mut target = issuer.target.state.lock().map_err(|_poison| Error::Io)?;
            if !matches!(*target, State::Vacant) {
                return Err(Error::Busy);
            }
            *target = State::Reserved(nonce.clone());
        }
        Ok(SuccessorReservation {
            namespace: self.namespace.clone(),
            source: source.clone(),
            target: issuer.target.clone(),
            owner: self.lineage.identity.clone(),
            nonce,
            source_brand: issuer.source_brand.clone(),
            target_brand: issuer.target_brand.clone(),
        })
    }
}
impl SuccessorReservation {
    /// Confirms these provisional facts only, not Model liveness or publication
    /// admission. The actual receiver must recheck its owned candidate and source.
    pub(crate) fn check(&self, owner: &OwnerClaim, issuer: &PreparedIssuer) -> Result<()> {
        if !Arc::ptr_eq(&self.namespace, &owner.namespace)
            || !Arc::ptr_eq(&self.owner, &owner.lineage.identity)
            || !Arc::ptr_eq(&self.namespace, &issuer.namespace)
            || !Arc::ptr_eq(&self.source, &issuer.source)
            || !Arc::ptr_eq(&self.target, &issuer.target)
            || !Arc::ptr_eq(&self.source_brand, &issuer.source_brand)
            || !Arc::ptr_eq(&self.target_brand, &issuer.target_brand)
        {
            return Err(Error::Stale);
        }
        let namespace = self.namespace.state.lock().map_err(|_poison| Error::Io)?;
        if !committed(&namespace, &self.owner)
            || !same_backend(active_backend(&namespace), Some(&self.source))
        {
            return Err(Error::Stale);
        }
        {
            let source = self.source.state.lock().map_err(|_poison| Error::Io)?;
            if !committed(&source, &self.owner) {
                return Err(Error::Stale);
            }
        }
        let target = self.target.state.lock().map_err(|_poison| Error::Io)?;
        if !reserved(&target, &self.nonce) {
            return Err(Error::Stale);
        }
        Ok(())
    }
}
impl Drop for SuccessorReservation {
    fn drop(&mut self) {
        // No source or namespace state changes, no callback and no repair.
        if let Ok(mut target) = self.target.state.lock()
            && reserved(&target, &self.nonce)
        {
            *target = State::Vacant;
        }
    }
}

#[cfg(test)]
mod tests;
