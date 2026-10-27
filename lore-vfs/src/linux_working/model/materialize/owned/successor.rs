// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Provisional target reservation through the actual candidate's Model.

use super::*;
use crate::publication_ownership::successor::SuccessorReservation;

impl UnpublishedCandidate {
    pub(crate) async fn reserve_successor(
        &self,
        owner: &Arc<Model>,
    ) -> PreparedResult<SuccessorReservation> {
        if !Arc::ptr_eq(&self.admission.owner, owner) {
            return Err(core::Error::Stale.into());
        }
        owner.live()?;
        self.state.validate_source().await?;
        let reservation = owner
            .ownership
            .reserve_successor(self.state.successor_issuer()?)?;
        // No slot guard spans source validation. Cancellation or any error drops
        // this reservation and releases only its exact target nonce.
        self.state.validate_source().await?;
        owner.live()?;
        reservation.check(&owner.ownership, self.state.successor_issuer()?)?;
        Ok(reservation)
    }
}
