// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Runtime Model ownership and freeze admission; no installation capability.

use super::*;
use crate::publication_ownership::Lineage;
use crate::publication_ownership::OwnerClaim;
use crate::publication_ownership::Reservation;
use crate::publication_ownership::Slot;

pub(crate) struct OwnerRoutingTicket {
    binding: Arc<NamespaceBinding>,
}
fn publication_backend(binding: &NamespaceBinding) -> Option<Arc<Slot>> {
    binding
        .backend
        .as_any()
        .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
        .map(|backend| backend.publication_slot().clone())
}
impl Namespace {
    pub(crate) fn reserve_working_owner(&self) -> crate::core::Result<Reservation> {
        let binding = self.current_binding()?;
        let reservation = Reservation::reserve(
            self.publication.clone(),
            publication_backend(&binding),
            || !self.operation_freeze.frozen(),
        )?;
        self.confirms_binding(&binding)?;
        Ok(reservation)
    }
    pub(crate) fn check_working_owner(
        &self,
        owner: &OwnerClaim,
    ) -> crate::core::Result<OwnerRoutingTicket> {
        let binding = self.current_binding()?;
        owner.check(&self.publication, publication_backend(&binding).as_ref())?;
        self.confirms_binding(&binding)?;
        Ok(OwnerRoutingTicket { binding })
    }
    pub(crate) fn confirm_working_owner_ticket(
        &self,
        ticket: &OwnerRoutingTicket,
    ) -> crate::core::Result<()> {
        self.confirms_binding(&ticket.binding)
    }
    pub(super) fn acquire_owned_freeze(&self) -> Result<(u64, Option<Lineage>)> {
        self.publication
            .acquire_freeze(|| self.operation_freeze.acquire())
            .map_err(|error| match error {
                Error::Busy => NamespaceError::Busy,
                error => error.into(),
            })
    }
}
