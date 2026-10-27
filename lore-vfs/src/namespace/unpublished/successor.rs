// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact issuer identity retained by a privately validated candidate.

use super::*;
use crate::publication_ownership::successor::PreparedIssuer;
use crate::revision::RevisionBackend;

impl UnpublishedState {
    pub(crate) fn successor_issuer(&self) -> crate::core::Result<&PreparedIssuer> {
        let namespace = self.freeze.namespace();
        namespace.confirms_binding(&self.source_binding)?;
        if !self.freeze.valid() {
            return Err(Error::Stale);
        }
        let source = self
            .source_binding
            .backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(Error::Unsupported)?;
        let target = self
            .binding
            .backend
            .as_any()
            .and_then(|backend| backend.downcast_ref::<RevisionBackend>())
            .ok_or(Error::Unsupported)?;
        self.issuer
            .check_backends(&namespace.publication, source, target)?;
        Ok(&self.issuer)
    }
}
