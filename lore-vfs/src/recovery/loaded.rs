// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact checked checkpoint bytes and their structurally validated snapshot.
//! These facts grant no authorization or publication ownership. Encoded bytes,
//! decoded allocations and validation scratch need a caller's aggregate budget.
#![allow(
    dead_code,
    reason = "Retained source images await durable transition coordination."
)]

use std::fs::File;
use std::fs::Metadata;
use std::io::ErrorKind;
use std::io::Read;
use std::time::SystemTime;

use super::Error;
use super::Result;
use super::codec;
use super::intent::CheckpointIdentity;
use super::validate;
use crate::capture_memory::CaptureMemory;
use crate::namespace::NamespaceSnapshot;

pub(crate) struct LoadedCheckpoint {
    bytes: Vec<u8>,
    snapshot: NamespaceSnapshot,
    identity: CheckpointIdentity,
}
impl LoadedCheckpoint {
    pub(super) fn read(file: File, limit: usize) -> Result<Self> {
        Self::read_with_memory(file, limit, None)
    }
    pub(super) fn read_budgeted(
        file: File,
        limit: usize,
        memory: &mut CaptureMemory,
    ) -> Result<Self> {
        Self::read_with_memory(file, limit, Some(memory))
    }
    fn read_with_memory(
        mut file: File,
        limit: usize,
        mut memory: Option<&mut CaptureMemory>,
    ) -> Result<Self> {
        let before = Stamp::of(&file.metadata()?);
        let bytes =
            read_bounded_with_memory(&mut file, before.length, limit, memory.as_deref_mut())?;
        // Best-effort consistency only. Private ownership and the recovery lock
        // coordinate cooperating writers; these checks are not hostile-writer IO.
        let after = Stamp::of(&file.metadata()?);
        if before != after {
            return Err(Error::Corrupt);
        }
        let snapshot = if let Some(memory) = &mut memory {
            codec::decode_budgeted(&bytes, memory)?
        } else {
            codec::decode(&bytes)?
        };
        if let Some(memory) = &mut memory {
            super::budgeted::validate(&snapshot, limit, memory)?;
        } else {
            validate(&snapshot, limit)?;
        }
        let identity = CheckpointIdentity::from_bytes(&bytes, limit)
            .map_err(|_identity_limit| Error::ResourceLimit)?;
        Ok(Self {
            bytes,
            snapshot,
            identity,
        })
    }
    pub(crate) fn snapshot(&self) -> &NamespaceSnapshot {
        &self.snapshot
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn encoded_capacity(&self) -> usize {
        self.bytes.capacity()
    }
    pub(crate) fn identity(&self) -> CheckpointIdentity {
        self.identity
    }
    pub(super) fn into_snapshot(self) -> NamespaceSnapshot {
        self.snapshot
    }
}

#[derive(PartialEq, Eq)]
struct Stamp {
    length: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    changed: (i64, i64),
}
impl Stamp {
    fn of(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

fn read_bounded(reader: &mut impl Read, expected: u64, limit: usize) -> Result<Vec<u8>> {
    read_bounded_with_memory(reader, expected, limit, None)
}
fn read_bounded_with_memory(
    reader: &mut impl Read,
    expected: u64,
    limit: usize,
    memory: Option<&mut CaptureMemory>,
) -> Result<Vec<u8>> {
    let expected = usize::try_from(expected).map_err(|_length| Error::ResourceLimit)?;
    if expected > limit {
        return Err(Error::ResourceLimit);
    }
    let mut bytes = if let Some(memory) = memory {
        memory
            .vector(expected)
            .map_err(|_capacity| Error::ResourceLimit)?
    } else {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(expected)
            .map_err(|_allocation| Error::ResourceLimit)?;
        bytes
    };
    if bytes.capacity() > limit {
        return Err(Error::ResourceLimit);
    }
    bytes.resize(expected, 0);
    reader.read_exact(&mut bytes).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            Error::Corrupt
        } else {
            Error::Io(error)
        }
    })?;
    // Probe one byte on the stack; never grow the image buffer beyond metadata.
    let mut extra = [0u8; 1];
    loop {
        match reader.read(&mut extra) {
            Ok(0) => return Ok(bytes),
            Ok(_) if expected == limit => return Err(Error::ResourceLimit),
            Ok(_) => return Err(Error::Corrupt),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(all(test, unix))]
mod tests;
