// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Conservative reservation while a caller may still retain this preparation.
use super::*;
use crate::capture_memory::Footprint;

impl PreparedTransition {
    pub(crate) fn capture_footprint(&self) -> core::Result<Footprint> {
        let mut size = Footprint::snapshot(&self.snapshot)?;
        size.vector(&self.intent.file_generations)?;
        size.vector(&self.intent.protected_children)?;
        size.nodes(self.intent.file_generations.len())?;
        size.nodes(self.intent.protected_children.len())?;
        size.vector(&self.changes)?;
        for change in &self.changes {
            size.nodes(1)?;
            size.vector(&change.path)?;
            size.name(change.path.len())?;
        }
        size.vector(&self.conflicts)?;
        for conflict in &self.conflicts {
            size.nodes(1)?;
            size.vector(&conflict.path)?;
            size.name(conflict.path.len())?;
        }
        for (path, metadata) in &self.candidate {
            size.nodes(1)?;
            size.buffer(path.capacity())?;
            size.buffer(metadata.path.capacity())?;
            size.name(path.len())?;
            size.name(metadata.path.len())?;
        }
        Ok(size)
    }
}
