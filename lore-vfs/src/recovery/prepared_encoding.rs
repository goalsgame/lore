// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! One budgeted checkpoint encoding for a source or candidate image; no
//! publication or source-currentness proof.
use super::*;
use crate::capture_memory::CaptureMemory;

pub(crate) fn encode_candidate(
    snapshot: &NamespaceSnapshot,
    limit: usize,
    memory: &mut CaptureMemory,
) -> Result<Vec<u8>> {
    let size = codec::encoded_size(snapshot, limit)?;
    memory
        .buffer(size)
        .map_err(|_capacity| Error::ResourceLimit)?;
    let bytes = codec::encode(snapshot, size)?;
    if bytes.capacity() != size {
        return Err(Error::ResourceLimit);
    }
    Ok(bytes)
}
