// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Reservations for unchanged structural predicates. The caller's one ledger
//! already retains its other encoded/decoded images. These logical record and
//! owned-buffer bounds do not include allocator implementation overhead.
use super::Error;
use super::Result;
use crate::capture_memory::CaptureMemory;
use crate::namespace::NamespaceSnapshot;

pub(super) fn validate(
    snapshot: &NamespaceSnapshot,
    limit: usize,
    memory: &mut CaptureMemory,
) -> Result<()> {
    memory
        .validator(snapshot)
        .map_err(|_capacity| Error::ResourceLimit)?;
    let ranges = snapshot
        .files
        .values()
        .map(|file| file.extents.len())
        .max()
        .unwrap_or(0);
    memory
        .buffer(
            ranges
                .checked_mul(std::mem::size_of::<(u64, u64)>())
                .ok_or(Error::ResourceLimit)?,
        )
        .map_err(|_capacity| Error::ResourceLimit)?;
    super::validate(snapshot, limit)
}

#[allow(
    dead_code,
    reason = "Budgeted source acknowledgment is not integrated."
)]
pub(crate) fn successor(
    previous: &NamespaceSnapshot,
    next: &NamespaceSnapshot,
    memory: &mut CaptureMemory,
) -> Result<()> {
    // nextnodes and both effective-receipt maps are live together. Valid images
    // contain at most one receipt per inode; count both input locations anyway.
    let records = next
        .nodes
        .len()
        .checked_add(previous.nodes.len())
        .and_then(|count| count.checked_add(previous.files.len()))
        .and_then(|count| count.checked_add(next.nodes.len()))
        .and_then(|count| count.checked_add(next.files.len()))
        .ok_or(Error::ResourceLimit)?;
    memory
        .nodes(records)
        .map_err(|_capacity| Error::ResourceLimit)?;
    super::successor(previous, next)
}
