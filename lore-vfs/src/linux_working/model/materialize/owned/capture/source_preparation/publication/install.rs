// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::publication_ownership::successor::SuccessorReservation;

impl SourceContinuation {
    pub(crate) async fn install_in_place(
        &mut self,
        reservation: &SuccessorReservation,
    ) -> PreparedResult<()> {
        self.captured
            .candidate
            .install_in_place(&self._seal, reservation)
            .await
    }
}
