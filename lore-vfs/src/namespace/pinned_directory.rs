// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Retained directory names and their exact read authorization binding.
//! The effective gate owns its backend route and rechecks policy on each read.
//! This value retains no namespace freeze and grants no write capability.

use super::*;

pub(crate) struct PinnedDirectorySnapshot {
    snapshot: DirectorySnapshot,
    gate: Arc<dyn AccessGate>,
}
impl PinnedDirectorySnapshot {
    pub(super) fn new(snapshot: DirectorySnapshot, gate: Arc<dyn AccessGate>) -> Self {
        Self { snapshot, gate }
    }
    pub(crate) fn snapshot(&self) -> &DirectorySnapshot {
        &self.snapshot
    }
    pub(super) fn into_snapshot(self) -> DirectorySnapshot {
        self.snapshot
    }
    pub(crate) async fn authorize_directory(&self) -> Result<()> {
        self.gate
            .authorize(&self.snapshot.entry.authority, Access::Read)
            .await
            .map_err(Into::into)
    }
    /// Positions follow the retained listing: directory, parent, then children.
    pub(crate) async fn authorize_position(&self, position: usize) -> Result<()> {
        let entry = match position {
            0 => &self.snapshot.entry,
            1 => &self.snapshot.parent,
            _ => {
                &self
                    .snapshot
                    .entries
                    .get(position - 2)
                    .ok_or(Error::Range)?
                    .entry
            }
        };
        self.gate
            .authorize(&entry.authority, Access::Read)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
