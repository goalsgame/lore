// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Existing-only read facts under the actual recovery owner lock. Inspection
//! never creates, syncs, repairs or publishes files and grants no reconciliation
//! permission. Returned image facts do not retain the owner lock or currentness.
#![allow(
    dead_code,
    reason = "Inspection awaits typed reconciliation coordination."
)]

#[cfg(unix)]
mod artifacts;
#[cfg(unix)]
mod reconcile;

use std::fs::File;
use std::path::Path;

use super::Error;
use super::Limits;
use super::LoadedCheckpoint;
use super::Result;
use crate::capture_memory::CaptureMemory;

pub(crate) struct RecoveryInspection {
    // Unlock while the pinned directory handle is still retained.
    _lock: File,
    directory: File,
    limit: usize,
}
impl RecoveryInspection {
    #[cfg(unix)]
    pub(crate) fn open_existing(directory: &Path, limits: Limits) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        if limits.max_checkpoint_bytes < super::codec::MIN_SIZE
            || limits.max_checkpoint_bytes == usize::MAX
        {
            return Err(Error::ResourceLimit);
        }
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(directory)?;
        super::private_metadata(&directory.metadata()?, true)?;
        let lock = open_file(&directory, c"owner.lock")?;
        super::private_metadata(&lock.metadata()?, false)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Busy),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        Ok(Self {
            directory,
            limit: limits.max_checkpoint_bytes,
            _lock: lock,
        })
    }
    #[cfg(not(unix))]
    pub(crate) fn open_existing(_directory: &Path, _limits: Limits) -> Result<Self> {
        Err(Error::PrivateStorageRequired)
    }
    pub(crate) fn transition_presence(&self) -> Result<bool> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: the directory fd is retained, the name is a fixed C string,
            // and fstatat initializes the supplied stat storage on success.
            let result = unsafe {
                libc::fstatat(
                    self.directory.as_raw_fd(),
                    c"transition".as_ptr(),
                    metadata.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result == 0 {
                return Ok(true);
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error.into())
            }
        }
        #[cfg(not(unix))]
        {
            Err(Error::PrivateStorageRequired)
        }
    }
    /// Charge to the caller's aggregate image ledger. These are committed-file
    /// facts only; transition contents and anchor selection remain unchecked.
    pub(crate) fn load_committed_budgeted(
        &self,
        memory: &mut CaptureMemory,
    ) -> Result<Option<LoadedCheckpoint>> {
        #[cfg(unix)]
        {
            let file = match open_file(&self.directory, c"checkpoint") {
                Ok(file) => file,
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            super::private_metadata(&file.metadata()?, false)?;
            LoadedCheckpoint::read_budgeted(file, self.limit, memory).map(Some)
        }
        #[cfg(not(unix))]
        {
            let _memory = memory;
            Err(Error::PrivateStorageRequired)
        }
    }
}
#[cfg(unix)]
fn open_file(directory: &File, name: &std::ffi::CStr) -> Result<File> {
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    // SAFETY: the directory fd is retained and the fixed component is a valid
    // C string. O_CREAT/O_TRUNC and writable access are never requested.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: openat returned a new owned fd; exactly this File takes ownership.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(all(test, unix))]
mod tests;
