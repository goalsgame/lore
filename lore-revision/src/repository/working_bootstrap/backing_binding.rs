// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private backing-root identity. Reading a record grants no recovery admission.
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;

use lore_base::types::RepositoryId;

use crate::instance::InstanceId;

pub(super) const NAME: &str = ".working-backing-v1";
const MAGIC: &[u8; 8] = b"LOREWBND";
const VERSION: u32 = 1;
pub(super) const MAX_PATH: usize = 4096;
const HEADER: usize = 8 + 4 + 16 + 16 + 4;
const CHECKSUM: usize = 32;
const MAX_BYTES: usize = HEADER + MAX_PATH + CHECKSUM;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Invalid,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Checked repository, instance and canonical backing-path facts. This retains
/// no filesystem or recovery ownership and grants no admission.
pub struct BackingBinding {
    repository: RepositoryId,
    instance: InstanceId,
    root: PathBuf,
}
impl BackingBinding {
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn new(repository: RepositoryId, instance: InstanceId, root: &Path) -> Result<Self> {
        let bytes = root.as_os_str().as_bytes();
        if repository.is_zero()
            || instance.is_zero()
            || bytes.is_empty()
            || bytes.len() > MAX_PATH
            || bytes.contains(&0)
            || !root.is_absolute()
            || !std::fs::metadata(root)?.is_dir()
            || root.canonicalize()?.as_os_str().as_bytes() != bytes
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            repository,
            instance,
            root: root.to_path_buf(),
        })
    }
    fn encode(&self) -> Result<Vec<u8>> {
        let path = self.root.as_os_str().as_bytes();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(HEADER + path.len() + CHECKSUM)
            .map_err(|_capacity| Error::Invalid)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(self.repository.data());
        bytes.extend_from_slice(self.instance.data());
        bytes.extend_from_slice(
            &u32::try_from(path.len())
                .map_err(|_length| Error::Invalid)?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(path);
        let checksum = lore_storage::hash::hash_slice(&bytes);
        bytes.extend_from_slice(checksum.data());
        Ok(bytes)
    }
    fn decode(bytes: &[u8], repository: RepositoryId, instance: InstanceId) -> Result<Self> {
        if bytes.len() < HEADER + CHECKSUM
            || bytes.len() > MAX_BYTES
            || &bytes[..8] != MAGIC
            || bytes[8..12] != VERSION.to_le_bytes()
            || bytes[12..28] != *repository.data()
            || bytes[28..44] != *instance.data()
        {
            return Err(Error::Invalid);
        }
        let length = u32::from_le_bytes(bytes[44..48].try_into().map_err(|_length| Error::Invalid)?)
            as usize;
        if length > MAX_PATH || bytes.len() != HEADER + length + CHECKSUM {
            return Err(Error::Invalid);
        }
        let checksum = lore_storage::hash::hash_slice(&bytes[..HEADER + length]);
        if bytes[HEADER + length..] != *checksum.data() {
            return Err(Error::Invalid);
        }
        let path = PathBuf::from(std::ffi::OsString::from_vec(
            bytes[HEADER..HEADER + length].to_vec(),
        ));
        Self::new(repository, instance, &path)
    }
    pub fn read(metadata: &Path, repository: RepositoryId, instance: InstanceId) -> Result<Self> {
        let directory = private_directory(metadata)?;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(metadata.join(NAME))?;
        let stat = file.metadata()?;
        if !stat.is_file()
            || stat.uid() != effective_uid()
            || stat.mode() & 0o777 != 0o600
            || stat.nlink() != 1
            || stat.len() > MAX_BYTES as u64
        {
            return Err(Error::Invalid);
        }
        let bytes = read_sized(&mut file, stat.len())?;
        verify_directory(metadata, &directory)?;
        Self::decode(&bytes, repository, instance)
    }
    pub fn write_new(
        metadata: &Path,
        repository: RepositoryId,
        instance: InstanceId,
        root: &Path,
    ) -> Result<Self> {
        let binding = Self::new(repository, instance, root)?;
        let bytes = binding.encode()?;
        let directory = private_directory(metadata)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(metadata.join(NAME))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        directory.sync_all()?;
        verify_directory(metadata, &directory)?;
        Ok(binding)
    }
}
// The descriptor length bounds one allocation; growth is probed on the stack.
fn read_sized(reader: &mut impl Read, expected: u64) -> Result<Vec<u8>> {
    let expected = usize::try_from(expected).map_err(|_length| Error::Invalid)?;
    if expected > MAX_BYTES {
        return Err(Error::Invalid);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(expected)
        .map_err(|_capacity| Error::Invalid)?;
    if bytes.capacity() > MAX_BYTES {
        return Err(Error::Invalid);
    }
    bytes.resize(expected, 0);
    reader.read_exact(&mut bytes)?;
    let mut extra = [0u8; 1];
    loop {
        match reader.read(&mut extra) {
            Ok(0) => return Ok(bytes),
            Ok(_) => return Err(Error::Invalid),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}
fn effective_uid() -> u32 {
    // SAFETY: geteuid reads process identity without pointers.
    unsafe { libc::geteuid() }
}
fn private_directory(path: &Path) -> Result<File> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let stat = directory.metadata()?;
    if !stat.is_dir()
        || stat.uid() != effective_uid()
        || stat.mode() & 0o777 != 0o700
        || path.canonicalize()?.as_os_str() != path.as_os_str()
    {
        return Err(Error::Invalid);
    }
    Ok(directory)
}
fn verify_directory(path: &Path, directory: &File) -> Result<()> {
    let before = directory.metadata()?;
    let after = std::fs::symlink_metadata(path)?;
    if !after.is_dir() || before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(Error::Invalid);
    }
    Ok(())
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "backing binding: {error}"),
            Self::Invalid => f.write_str("invalid backing-root binding"),
        }
    }
}
impl std::error::Error for Error {}

#[cfg(test)]
mod tests;
