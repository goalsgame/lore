// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact source pairing without a source-image recapture.
use super::*;
use crate::transition_prepare::OwnedSourceImage;
use crate::transition_prepare::SourceFacts;

impl UnpublishedState {
    pub(crate) fn confirms_source_image(
        &self,
        source: &OwnedSourceImage,
    ) -> crate::transition_prepare::Result<()> {
        self.confirms_source_facts(source.facts())?;
        if &source.snapshot().original_root != source.original_root() {
            return Err(crate::transition_prepare::Error::InvalidScope);
        }
        Ok(())
    }
    pub(crate) fn confirms_source_facts(
        &self,
        source: &SourceFacts,
    ) -> crate::transition_prepare::Result<()> {
        let witness = source.witness();
        if !self.freeze.same_acquisition(source.freeze())
            || !self.freeze.valid()
            || self.epoch != witness.epoch
            || self.generation != witness.generation
            || self.source_next != witness.next_inode
            || source.original_root() != &self.original_root
            || source.selection() != &self.selection
            || source.footprint() != self.source_footprint
        {
            return Err(crate::transition_prepare::Error::InvalidScope);
        }
        source
            .routing()
            .check(self.freeze.namespace(), self.freeze.token())?;
        self.freeze
            .namespace()
            .confirms_binding(&self.source_binding)?;
        Ok(())
    }
}
