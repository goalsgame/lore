// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Per-candidate owned-buffer capacities and logical metadata bounds. This is
//! neither a global RAM/RSS limit nor accounting for a source loader's past peak.
//! Backend/store caches, allocator overhead, and issued IO that outlives a
//! cancelled future remain outside this receipt.

use crate::core::Error;
use crate::core::Result;
use crate::namespace::NamespaceSnapshot;

#[derive(Clone, Copy)]
pub(crate) struct CaptureLimits {
    pub(crate) max_buffer_bytes: usize,
    pub(crate) max_metadata_nodes: usize,
    pub(crate) max_name_bytes: usize,
}
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Footprint {
    buffers: usize,
    nodes: usize,
    names: usize,
}
impl Footprint {
    pub(crate) fn snapshot(snapshot: &NamespaceSnapshot) -> Result<Self> {
        let mut size = Self::default();
        size.vector(&snapshot.nodes)?;
        size.vector(&snapshot.backend_state)?;
        size.nodes(snapshot.active_underlays.len())?;
        size.nodes(snapshot.baselines.len())?;
        for record in &snapshot.nodes {
            size.nodes(1)?;
            if let Some(names) = &record.names {
                size.vector(names)?;
                for (name, _) in names {
                    size.vector(name)?;
                    size.names = size
                        .names
                        .checked_add(name.len())
                        .ok_or(Error::ResourceLimit)?;
                    size.nodes(1)?;
                }
            }
        }
        for file in snapshot.files.values() {
            size.nodes(1)?;
            size.vector(&file.extents)?;
            for extent in &file.extents {
                size.nodes(1)?;
                size.vector(&extent.data)?;
            }
        }
        Ok(size)
    }
    pub(crate) fn buffer(&mut self, capacity: usize) -> Result<()> {
        self.buffers = self
            .buffers
            .checked_add(capacity)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn name(&mut self, length: usize) -> Result<()> {
        self.names = self.names.checked_add(length).ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn nodes(&mut self, count: usize) -> Result<()> {
        self.nodes = self.nodes.checked_add(count).ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn vector<T>(&mut self, vector: &Vec<T>) -> Result<()> {
        self.buffers = self
            .buffers
            .checked_add(
                vector
                    .capacity()
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or(Error::ResourceLimit)?,
            )
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
}
/// A conservative retained receipt. Temporary read/validation reservations stay
/// charged until the paired captured artifact drops, even after scratch is freed.
pub(crate) struct CaptureMemory {
    limits: CaptureLimits,
    used: Footprint,
}
impl CaptureMemory {
    pub(crate) fn new(limits: CaptureLimits) -> Result<Self> {
        if limits.max_buffer_bytes == 0
            || limits.max_metadata_nodes == 0
            || limits.max_name_bytes == 0
        {
            return Err(Error::InvalidInput);
        }
        Ok(Self {
            limits,
            used: Footprint::default(),
        })
    }
    pub(crate) fn retain(&mut self, size: Footprint) -> Result<()> {
        self.buffer(size.buffers)?;
        self.nodes(size.nodes)?;
        self.names(size.names)
    }
    pub(crate) fn preflight_buffer(&self, bytes: usize) -> Result<()> {
        self.used
            .buffers
            .checked_add(bytes)
            .filter(|used| *used <= self.limits.max_buffer_bytes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn buffer(&mut self, bytes: usize) -> Result<()> {
        self.used.buffers = self
            .used
            .buffers
            .checked_add(bytes)
            .filter(|used| *used <= self.limits.max_buffer_bytes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn preflight_nodes(&self, count: usize) -> Result<()> {
        self.used
            .nodes
            .checked_add(count)
            .filter(|used| *used <= self.limits.max_metadata_nodes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn nodes(&mut self, count: usize) -> Result<()> {
        self.used.nodes = self
            .used
            .nodes
            .checked_add(count)
            .filter(|used| *used <= self.limits.max_metadata_nodes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn names(&mut self, bytes: usize) -> Result<()> {
        self.used.names = self
            .used
            .names
            .checked_add(bytes)
            .filter(|used| *used <= self.limits.max_name_bytes)
            .ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    /// Reserve a known allocation before requesting it and reject an unexpected
    /// capacity rather than allowing implicit growth during subsequent writes.
    pub(crate) fn vector<T>(&mut self, count: usize) -> Result<Vec<T>> {
        let requested = count
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(Error::ResourceLimit)?;
        self.buffer(requested)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(count)
            .map_err(|_allocation| Error::ResourceLimit)?;
        if output.capacity() != count {
            return Err(Error::ResourceLimit);
        }
        Ok(output)
    }
    pub(crate) fn validator(&mut self, snapshot: &NamespaceSnapshot) -> Result<()> {
        let records = snapshot.nodes.len();
        let names = snapshot.nodes.iter().try_fold(0usize, |sum, node| {
            sum.checked_add(node.names.as_ref().map_or(0, Vec::len))
                .ok_or(Error::ResourceLimit)
        })?;
        let bytes = snapshot
            .nodes
            .iter()
            .filter_map(|node| node.names.as_ref())
            .flatten()
            .try_fold(0usize, |sum, (name, _)| {
                sum.checked_add(name.len()).ok_or(Error::ResourceLimit)
            })?;
        // Logical map/set records, including parent-cycle and normalized-name
        // sets. Allocator implementation overhead is deliberately not estimated.
        self.nodes(
            records
                .checked_mul(10)
                .and_then(|count| count.checked_add(names.checked_mul(2)?))
                .ok_or(Error::ResourceLimit)?,
        )?;
        // UTF-8 lowercase expansion, temporary normalized names, and root-list
        // vectors are conservatively bounded independently of private bytes.
        let normalized = bytes
            .checked_mul(24)
            .and_then(|bytes| bytes.checked_add(names.checked_mul(8)?))
            .ok_or(Error::ResourceLimit)?;
        // Vec<&SnapshotNode> starts at capacity four; doubling keeps the sum
        // across all exposure groups at no more than four entries per root.
        self.buffer(
            normalized
                .checked_add(
                    records
                        .checked_mul(4 * std::mem::size_of::<usize>())
                        .ok_or(Error::ResourceLimit)?,
                )
                .ok_or(Error::ResourceLimit)?,
        )?;
        self.names(normalized)
    }
    #[cfg(test)]
    pub(crate) fn remaining_buffer_bytes(&self) -> usize {
        self.limits.max_buffer_bytes - self.used.buffers
    }
    #[cfg(test)]
    pub(crate) fn reserved_nodes(&self) -> usize {
        self.used.nodes
    }
    #[cfg(test)]
    pub(crate) fn reserved_buffers(&self) -> usize {
        self.used.buffers
    }
}

#[cfg(test)]
#[path = "capture_memory_tests.rs"]
mod tests;
