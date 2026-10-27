// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::path::Path;

use super::CanonicalTarget;
use super::Error;
use super::Result;
use super::WorkingLocation;

#[cfg(unix)]
fn owned(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    // SAFETY: geteuid reads process identity and has no preconditions.
    metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o022 == 0
}

#[cfg(unix)]
pub(super) fn target_owner(target: &CanonicalTarget) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(target.path()).map_err(Error::Io)?;
    // SAFETY: geteuid reads process identity and has no preconditions.
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(Error::InvalidTarget);
    }
    Ok(())
}
#[cfg(not(unix))]
pub(super) fn target_owner(_target: &CanonicalTarget) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(unix)]
pub(super) fn directory(path: &Path) -> Result<CanonicalTarget> {
    let endpoint = std::fs::symlink_metadata(path).map_err(Error::Io)?;
    if !endpoint.is_dir() || endpoint.file_type().is_symlink() || !owned(&endpoint) {
        return Err(Error::InvalidMetadata);
    }
    let directory = CanonicalTarget::resolve(path)?;
    let current = std::fs::metadata(directory.path()).map_err(Error::Io)?;
    if !owned(&current) {
        return Err(Error::InvalidMetadata);
    }
    Ok(directory)
}
#[cfg(not(unix))]
pub(super) fn directory(_path: &Path) -> Result<CanonicalTarget> {
    Err(Error::UnsupportedPlatform)
}

#[cfg(unix)]
fn read_regular(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(Error::Io)?;
    let metadata = file.metadata().map_err(Error::Io)?;
    if !metadata.is_file() || !owned(&metadata) || metadata.len() > maximum as u64 {
        return Err(Error::InvalidMetadata);
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(Error::Io)?;
    if bytes.len() > maximum {
        return Err(Error::InvalidMetadata);
    }
    Ok(bytes)
}

#[cfg(unix)]
pub(super) fn verify(location: &WorkingLocation) -> Result<()> {
    use crate::repository::CONFIG;
    use crate::repository::ID;
    use crate::repository::INSTANCE;
    use crate::repository::RepositoryConfig;
    let canonical = directory(location.metadata.path())?;
    if canonical != location.metadata {
        return Err(Error::Stale);
    }
    for marker in [super::INITIALIZING_METADATA, super::INITIALIZING_STORES] {
        match std::fs::symlink_metadata(canonical.path().join(marker)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(Error::InvalidMetadata),
        }
    }
    let repository = read_regular(&canonical.path().join(ID), 16)?;
    let instance = read_regular(&canonical.path().join(INSTANCE), 16)?;
    if repository.as_slice() != location.repository.data()
        || instance.as_slice() != location.instance.data()
    {
        return Err(Error::InvalidMetadata);
    }
    // Configuration is local routing metadata, bounded independently of content.
    let config = read_regular(&canonical.path().join(CONFIG), 64 * 1024)?;
    let config = std::str::from_utf8(&config).map_err(|_invalid| Error::InvalidMetadata)?;
    let config: RepositoryConfig =
        toml::from_str(config).map_err(|_invalid| Error::InvalidMetadata)?;
    if !config.is_experimental_working() {
        return Err(Error::InvalidMetadata);
    }
    Ok(())
}
#[cfg(not(unix))]
pub(super) fn verify(_location: &WorkingLocation) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}
