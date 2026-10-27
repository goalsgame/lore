// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Pure equalities between independently checked artifact facts. Success does
//! not establish scope membership, provenance compatibility, authorization,
//! anchor selection, admission, freshness or reconciliation permission.

use super::Artifact;
use super::TransitionFacts;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImageRelationError {
    Incomplete,
    SourceIdentity,
    CandidateIdentity,
    OriginalRootKey,
    OriginalRootDescriptor,
}
impl TransitionFacts {
    /// Compare only the immutable identities and original-root descriptors in
    /// this one load's facts. This performs no IO or allocation and grants no
    /// capability. A coordinator still needs its retained inspection claim and
    /// fresh facts before making any reconciliation decision.
    pub(crate) fn check_image_relation(&self) -> Result<(), ImageRelationError> {
        let inventory = self.inventory();
        if inventory.observed_known() != 0b1011
            || inventory.unknown_or_duplicate()
            || inventory.excess()
            || !matches!(self.pending_intent(), Artifact::Missing)
        {
            return Err(ImageRelationError::Incomplete);
        }
        let (Artifact::Checked(source), Artifact::Checked(candidate), Artifact::Checked(intent)) =
            (self.source(), self.candidate(), self.intent())
        else {
            return Err(ImageRelationError::Incomplete);
        };
        let intent = intent.parsed();
        if source.identity() != intent.source {
            return Err(ImageRelationError::SourceIdentity);
        }
        if candidate.identity() != intent.candidate {
            return Err(ImageRelationError::CandidateIdentity);
        }
        let source_root = &source.snapshot().original_root;
        let candidate_root = &candidate.snapshot().original_root;
        if source_root.key != intent.original_root || candidate_root.key != intent.original_root {
            return Err(ImageRelationError::OriginalRootKey);
        }
        if source_root != candidate_root {
            return Err(ImageRelationError::OriginalRootDescriptor);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
