// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Ordinary owner path checks; these facts are not a recovery completion permit.
use std::os::unix::fs::MetadataExt;

use super::*;

impl Recovery {
    /// Checks that current paths still name the held directory and owner lock,
    /// then applies the ordinary transition-presence fence. No files are changed.
    /// Cooperating-owner checks do not provide hostile-writer path atomicity.
    pub fn verify_owner(&self) -> Result<()> {
        private(&self.directory, true)?;
        let held = self.directory_file.metadata()?;
        let current = fs::symlink_metadata(&self.directory)?;
        if held.dev() != current.dev() || held.ino() != current.ino() {
            return Err(Error::PrivateStorageRequired);
        }
        let lock = self.directory.join("owner.lock");
        private(&lock, false)?;
        let held = self._lock.metadata()?;
        let current = fs::symlink_metadata(lock)?;
        if held.dev() != current.dev() || held.ino() != current.ino() {
            return Err(Error::PrivateStorageRequired);
        }
        transition_admission::check(&self.directory)
    }
}
