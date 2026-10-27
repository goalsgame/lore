// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Identity capsule for an exact complete candidate; no target reservation.

use super::*;
use crate::publication_ownership::successor::PreparedIssuer;
use crate::revision::PreparedCompleteBackend;
use crate::revision::RevisionBackend;

impl Namespace {
    pub(crate) fn prepare_successor_issuer(
        &self,
        prepared: &PreparedCompleteBackend,
    ) -> crate::transition_prepare::Result<PreparedIssuer> {
        self.check_complete_source(prepared)?;
        let backend = self.backend()?;
        let source = backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(crate::transition_prepare::Error::InvalidScope)?;
        PreparedIssuer::from_complete(self.publication.clone(), source, prepared)
            .map_err(Into::into)
    }
}
