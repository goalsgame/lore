// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Read-only comparison against this owner's retained directory and lock.
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;

use super::*;

pub(super) fn open_directory(path: &Path) -> Result<File> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    private_metadata(&directory.metadata()?, true)?;
    Ok(directory)
}
impl WorkingStorage {
    /// Verifies that current paths still name the held directory/owner lock and
    /// exact storage identity. Performs no creation, repair or session inventory.
    /// Checks coordinate cooperating owners; they do not make path lookups atomic.
    pub fn verify_owner(&self) -> Result<()> {
        private(&self.root, true)?;
        let held = self.directory_file.metadata()?;
        let current = fs::symlink_metadata(&self.root)?;
        if held.dev() != current.dev()
            || held.ino() != current.ino()
            || self.root.canonicalize()? != self.root
        {
            return Err(Error::UnsafePath);
        }
        let lock_path = self.root.join("owner.lock");
        private(&lock_path, false)?;
        private_metadata(&self._lock.metadata()?, false)?;
        let held = self._lock.metadata()?;
        let current = fs::symlink_metadata(lock_path)?;
        if held.dev() != current.dev() || held.ino() != current.ino() {
            return Err(Error::UnsafePath);
        }
        verify_identity(&self.root.join("identity"), self.repository, self.instance)?;
        validate_root(&self.root)?;
        for directory in [&self.recovery, &self.sessions] {
            private(directory, true)?;
            if directory.canonicalize()? != *directory {
                return Err(Error::UnsafePath);
            }
        }
        Ok(())
    }
}
