// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Final in-memory publication. The externally retained acknowledgment owns the
//! candidate and displaced resources through cancellation and panic quarantine.
use super::*;
use crate::namespace::MetadataSeal;
use crate::publication_ownership::successor::SuccessorReservation;
use crate::revision::RevisionBackend;

impl UnpublishedCandidate {
    pub(crate) async fn install_in_place(
        &mut self,
        seal: &MetadataSeal,
        reservation: &SuccessorReservation,
    ) -> PreparedResult<()> {
        let owner = self.admission.owner.clone();
        owner.live()?;
        let names = owner.names.lock().await;
        owner.live()?;
        self.metadata.check_source(&owner)?;
        self.state.validate_source().await?;
        let mut files = owner.files.lock().await;
        let mut namespace = owner.namespace.prepare_install(&mut self.state).await?;
        namespace.check_seal(seal)?;
        let (source, target) = namespace.backend_pair();
        let source_backend = source
            .as_any()
            .and_then(|value| value.downcast_ref::<RevisionBackend>())
            .ok_or(core::Error::Unsupported)?;
        let target_backend = target
            .as_any()
            .and_then(|value| value.downcast_ref::<RevisionBackend>())
            .ok_or(core::Error::Unsupported)?;
        let mut model = owner.state.lock().map_err(|_poison| Error::Poisoned)?;
        self.metadata.check_locked(&owner)?;
        if *owner.closed.borrow() {
            return Err(core::Error::Unmounted.into());
        }
        let mut routing = namespace.lock_routing()?;
        let mut ownership = reservation.prepare_commit(&owner.ownership, namespace.issuer())?;
        let mut allocators = source_backend.prepare_install_owners(target_backend, &ownership)?;
        let mut epoch = seal.prepare_install(namespace.epoch())?;
        // No fallible operation, callback, allocation, I/O or await may follow
        // the first successful namespace swap.
        namespace.commit(&mut routing, &ownership, &mut epoch)?;
        ownership.commit();
        allocators.commit();
        std::mem::swap(&mut *files, &mut self.files);
        self.metadata.commit_locked(&owner, &mut model.unlinked);
        // Displaced state/files stay in self, which the outer worker retains.
        drop(epoch);
        drop(allocators);
        drop(ownership);
        drop(routing);
        drop(model);
        drop(namespace);
        drop(files);
        drop(names);
        Ok(())
    }
}
