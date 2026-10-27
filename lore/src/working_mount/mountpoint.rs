// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;

use super::Error;
use super::Record;
use super::Result;

/// Pins the prepared underlay inode so replacement cannot reuse its identity.
pub(super) struct Target {
    _directory: File,
}
impl Target {
    pub(super) fn capture(record: &Record) -> Result<Self> {
        let target = record.paths.target();
        let checked = crate::mount::validate_mountpoint(target, Some(record.paths.metadata()))
            .map_err(problem)?;
        if checked != target {
            return Err(Error::Mountpoint(
                "target canonical location changed".into(),
            ));
        }
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(target)
            .map_err(problem)?;
        let observed = directory.metadata().map_err(problem)?;
        let current = std::fs::symlink_metadata(target).map_err(problem)?;
        // SAFETY: geteuid reads process identity and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        if current.uid() != uid
            || !current.is_dir()
            || current.file_type().is_symlink()
            || current.dev() != observed.dev()
            || current.ino() != observed.ino()
        {
            return Err(Error::Mountpoint(
                "target changed while opening its directory".into(),
            ));
        }
        Ok(Self {
            _directory: directory,
        })
    }
    #[cfg(target_os = "linux")]
    pub(super) fn revalidate(&self, record: &Record) -> Result<()> {
        let current = Self::capture(record)?;
        let pinned = self._directory.metadata().map_err(problem)?;
        let observed = current._directory.metadata().map_err(problem)?;
        if pinned.dev() != observed.dev() || pinned.ino() != observed.ino() {
            return Err(Error::Mountpoint(
                "target directory identity changed after preparation".into(),
            ));
        }
        Ok(())
    }
}
fn problem(error: std::io::Error) -> Error {
    Error::Mountpoint(error.to_string())
}
