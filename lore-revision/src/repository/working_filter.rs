// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::path::Path;
use std::sync::Arc;

use lore_error_set::prelude::*;

use super::DOT_LOREIGNORE;
use super::DOT_URCIGNORE;
use super::MAX_FILTER_FILE_BYTES;
use super::RepositoryError;
use super::VIEW_FILTER;
use crate::filter;
use crate::fs::filesystem_provider::FilesystemProvider;
use crate::fs::filesystem_provider::FsError;
use crate::util::path::RelativePath;

pub(super) async fn load(
    provider: &dyn FilesystemProvider,
    root: &Path,
    metadata: &Path,
) -> Result<Arc<filter::Filter>, RepositoryError> {
    let (name, bytes) = match provider
        .read_metadata_file(
            &RelativePath::new_from_clean_parts("", DOT_LOREIGNORE),
            MAX_FILTER_FILE_BYTES,
        )
        .await
    {
        Ok(bytes) => (DOT_LOREIGNORE, bytes),
        Err(FsError::FileNotFound(_)) => {
            let bytes = match provider
                .read_metadata_file(
                    &RelativePath::new_from_clean_parts("", DOT_URCIGNORE),
                    MAX_FILTER_FILE_BYTES,
                )
                .await
            {
                Ok(bytes) => bytes,
                Err(FsError::FileNotFound(_)) => Vec::new(),
                Err(error) => {
                    return Err(error).forward_any("Failed to load legacy working ignore");
                }
            };
            (DOT_URCIGNORE, bytes)
        }
        Err(error) => return Err(error).forward_any("Failed to load working ignore"),
    };
    // The provider contract bounds reads; enforce it again at this trust boundary.
    if bytes.len() > MAX_FILTER_FILE_BYTES {
        return Err(RepositoryError::internal(
            "Working ignore exceeds size limit",
        ));
    }
    let ignore = filter::parse_filter(&bytes, &root.join(name))
        .forward::<RepositoryError>("Failed to parse working ignore")?;
    let view_path = metadata.join(VIEW_FILTER);
    let metadata = metadata.to_owned();
    let view = lore_base::lore_spawn_blocking_nocontext!(move || private_view(&metadata))
        .await
        .map_err(|error| RepositoryError::internal(format!("Working view read task: {error}")))?
        .map_err(|error| {
            RepositoryError::internal(format!("Failed to read private working view: {error}"))
        })?;
    let view = filter::parse_filter(&view, &view_path)
        .forward::<RepositoryError>("Failed to parse working view")?;
    filter::from_instances(ignore, view)
        .map(Arc::new)
        .forward::<RepositoryError>("Failed to assemble working filters")
}

#[cfg(unix)]
fn private_view(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::fd::FromRawFd;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::OpenOptionsExt;

    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "unsafe working view metadata",
        )
    };
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let owner = directory.metadata()?;
    // SAFETY: geteuid reads the process identity without preconditions.
    let uid = unsafe { libc::geteuid() };
    if !owner.is_dir() || owner.uid() != uid || owner.mode() & 0o777 != 0o700 {
        return Err(invalid());
    }
    // SAFETY: directory is a live descriptor and the fixed name is NUL terminated.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            c"view".as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::NotFound {
            Ok(Vec::new())
        } else {
            Err(error)
        };
    }
    // SAFETY: successful openat transferred a fresh owned descriptor.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let info = file.metadata()?;
    if !info.is_file()
        || info.uid() != uid
        || info.mode() & 0o022 != 0
        || info.len() > MAX_FILTER_FILE_BYTES as u64
    {
        return Err(invalid());
    }
    let mut bytes = Vec::with_capacity(info.len() as usize);
    let mut chunk = [0; 8192];
    loop {
        let count = file.read(&mut chunk)?;
        if count == 0 {
            return Ok(bytes);
        }
        if bytes.len() + count > MAX_FILTER_FILE_BYTES {
            return Err(invalid());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
}

#[cfg(not(unix))]
fn private_view(_path: &Path) -> std::io::Result<Vec<u8>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "working filters require Unix private metadata",
    ))
}
