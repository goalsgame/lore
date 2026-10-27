// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;

use lore_base::types::Address;
use ring::digest::SHA256;
use ring::digest::digest;

use super::Error;
use super::Result;
use crate::capture_memory::CaptureMemory;
use crate::core::HeldBaseline;
use crate::core::Node;
use crate::core::NodeKey;
use crate::core::NodeKind;
use crate::core::ScopeHead;
use crate::core::ScopePin;
use crate::core::ScopeProjection;
use crate::namespace::Entry;
use crate::namespace::NamespaceSnapshot;
use crate::namespace::SnapshotNode;
use crate::overlay::CommittedProof;
use crate::overlay::FileSnapshot;
use crate::overlay::Metadata;
use crate::overlay::Receipt;
use crate::overlay::SnapshotExtent;

const MAGIC: &[u8; 8] = b"LOREOV05";
pub(super) const MIN_SIZE: usize = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 32;

struct Encoder {
    bytes: Option<Vec<u8>>,
    length: usize,
    limit: usize,
}
impl Encoder {
    fn raw(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self
            .length
            .checked_add(bytes.len())
            .ok_or(Error::ResourceLimit)?;
        if end > self.limit {
            return Err(Error::ResourceLimit);
        }
        if let Some(output) = &mut self.bytes {
            // The sizing pass reserved the entire record, including its checksum.
            // Never permit an implicit growth allocation during encoding.
            if end > output.capacity() {
                return Err(Error::ResourceLimit);
            }
            output.extend_from_slice(bytes);
        }
        self.length = end;
        Ok(())
    }
    fn u64(&mut self, value: u64) -> Result<()> {
        self.raw(&value.to_le_bytes())
    }
    fn count(&mut self, value: usize) -> Result<()> {
        self.u64(u64::try_from(value).map_err(|_decode_error| Error::ResourceLimit)?)
    }
    fn flag(&mut self, value: bool) -> Result<()> {
        self.raw(&[u8::from(value)])
    }
    fn kind(&mut self, kind: NodeKind) -> Result<()> {
        self.raw(&[match kind {
            NodeKind::File => 0,
            NodeKind::Directory => 1,
            NodeKind::Symlink => 2,
        }])
    }
    fn key(&mut self, key: NodeKey) -> Result<()> {
        self.raw(key.repository.data())?;
        self.raw(key.revision.context.data())?;
        self.raw(key.revision.hash.data())?;
        self.raw(&key.node.to_le_bytes())?;
        self.u64(key.exposure)
    }
    fn address(&mut self, address: Address) -> Result<()> {
        self.raw(address.context.data())?;
        self.raw(address.hash.data())
    }
    fn baseline(&mut self, baseline: &HeldBaseline) -> Result<()> {
        self.key(baseline.scope)?;
        self.u64(baseline.sequence)?;
        self.address(baseline.head.revision)?;
        self.flag(baseline.head.root.is_some())?;
        if let Some(root) = baseline.head.root {
            self.raw(&root.to_le_bytes())?;
        }
        self.flag(baseline.parent_projection.is_some())?;
        if let Some(projection) = &baseline.parent_projection {
            self.key(projection.parent)?;
            self.address(projection.head)?;
            self.flag(projection.target.is_some())?;
            if let Some(pin) = &projection.target {
                self.raw(pin.repository.data())?;
                self.address(pin.revision)?;
                self.raw(&pin.node.to_le_bytes())?;
            }
        }
        Ok(())
    }
    fn receipt(&mut self, receipt: &Option<Receipt>) -> Result<()> {
        self.flag(receipt.is_some())?;
        if let Some(receipt) = receipt {
            self.u64(receipt.inode)?;
            self.key(receipt.authority)?;
            self.u64(receipt.generation)?;
            self.raw(&receipt.mode.to_le_bytes())?;
            self.node(&receipt.proof.node)?;
            self.raw(receipt.proof.address.context.data())?;
            self.raw(receipt.proof.address.hash.data())?;
            self.flag(receipt.sha256.is_some())?;
            if let Some(hash) = receipt.sha256 {
                self.raw(&hash)?;
            }
        }
        Ok(())
    }
    fn node(&mut self, node: &Node) -> Result<()> {
        self.key(node.key)?;
        self.kind(node.kind)?;
        self.u64(node.size)?;
        self.raw(&node.mode.to_le_bytes())
    }
}

pub(super) fn encoded_size(snapshot: &NamespaceSnapshot, limit: usize) -> Result<usize> {
    let mut sizing = Encoder {
        bytes: None,
        length: 0,
        limit: limit.checked_sub(32).ok_or(Error::ResourceLimit)?,
    };
    encode_payload(snapshot, &mut sizing)?;
    sizing.length.checked_add(32).ok_or(Error::ResourceLimit)
}

pub(super) fn encode(snapshot: &NamespaceSnapshot, limit: usize) -> Result<Vec<u8>> {
    let total = encoded_size(snapshot, limit)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total)
        .map_err(|_allocation| Error::ResourceLimit)?;
    if bytes.capacity() > limit {
        return Err(Error::ResourceLimit);
    }
    let mut out = Encoder {
        bytes: Some(bytes),
        length: 0,
        limit: total - 32,
    };
    encode_payload(snapshot, &mut out)?;
    let length = u64::try_from(out.length).map_err(|_overflow| Error::ResourceLimit)?;
    let mut bytes = out.bytes.ok_or(Error::Corrupt)?;
    bytes[8..16].copy_from_slice(&length.to_le_bytes());
    let checksum = digest(&SHA256, &bytes);
    bytes.extend_from_slice(checksum.as_ref());
    Ok(bytes)
}

// Both passes traverse the same immutable snapshot. The first counts borrowed
// extents without copying them, so oversized records fail before allocation.
fn encode_payload(snapshot: &NamespaceSnapshot, out: &mut Encoder) -> Result<()> {
    out.raw(MAGIC)?;
    out.u64(0)?;
    out.node(&snapshot.original_root)?;
    if snapshot.active_underlays.len() > crate::core::MAX_OWNING_SCOPES {
        return Err(Error::ResourceLimit);
    }
    out.count(snapshot.active_underlays.len())?;
    for (scope, head) in &snapshot.active_underlays {
        out.key(*scope)?;
        out.address(head.revision)?;
        out.flag(head.root.is_some())?;
        if let Some(root) = head.root {
            out.raw(&root.to_le_bytes())?;
        }
    }
    out.count(snapshot.backend_state.len())?;
    out.raw(&snapshot.backend_state)?;
    if snapshot.baselines.len() > crate::core::MAX_OWNING_SCOPES {
        return Err(Error::ResourceLimit);
    }
    out.count(snapshot.baselines.len())?;
    for baseline in snapshot.baselines.values() {
        out.baseline(baseline)?;
    }
    out.count(snapshot.files.len())?;
    // Dirty content precedes the file and namespace metadata it supports. The
    // entire record, including both, is synced before its name is published.
    for (&inode, file) in &snapshot.files {
        out.u64(inode)?;
        out.count(file.extents.len())?;
        for extent in &file.extents {
            out.count(extent.data.len())?;
            out.raw(&extent.data)?;
            out.u64(extent.start)?;
            out.u64(extent.stored)?;
        }
        out.node(&file.node)?;
        out.u64(file.metadata.size)?;
        out.raw(&file.metadata.mode.to_le_bytes())?;
        out.u64(file.metadata.generation)?;
        out.flag(file.metadata.mutable)?;
        out.u64(file.metadata.reserved_storage_bytes)?;
        out.u64(file.base_end)?;
        out.receipt(&file.receipt)?;
    }
    out.u64(snapshot.next_inode)?;
    out.u64(snapshot.generation)?;
    out.count(snapshot.nodes.len())?;
    for node in &snapshot.nodes {
        out.u64(node.entry.inode)?;
        out.u64(node.entry.parent)?;
        out.kind(node.entry.kind)?;
        out.key(node.entry.authority)?;
        out.flag(node.entry.base.is_some())?;
        if let Some(base) = &node.entry.base {
            out.node(base)?;
        }
        out.flag(node.attached)?;
        out.receipt(&node.receipt)?;
        out.flag(node.names.is_some())?;
        if let Some(names) = &node.names {
            out.count(names.len())?;
            for (name, inode) in names {
                out.count(name.len())?;
                out.raw(name)?;
                out.u64(*inode)?;
            }
        }
    }
    Ok(())
}

struct Decoder<'a, 'memory> {
    bytes: &'a [u8],
    memory: Option<&'memory mut CaptureMemory>,
}
impl<'a> Decoder<'a, '_> {
    fn vector<T>(&mut self, count: usize) -> Result<Vec<T>> {
        if let Some(memory) = &mut self.memory {
            memory
                .nodes(count)
                .map_err(|_capacity| Error::ResourceLimit)?;
            memory
                .vector(count)
                .map_err(|_capacity| Error::ResourceLimit)
        } else {
            Ok(Vec::with_capacity(count))
        }
    }
    fn blob(&mut self, count: usize, name: bool) -> Result<Vec<u8>> {
        let bytes = self.raw(count)?;
        if let Some(memory) = &mut self.memory {
            if name {
                memory
                    .names(count)
                    .map_err(|_capacity| Error::ResourceLimit)?;
            }
            let mut output = memory
                .vector(count)
                .map_err(|_capacity| Error::ResourceLimit)?;
            output.extend_from_slice(bytes);
            Ok(output)
        } else {
            Ok(bytes.to_vec())
        }
    }
    fn map_record(&mut self) -> Result<()> {
        if let Some(memory) = &mut self.memory {
            memory.nodes(1).map_err(|_capacity| Error::ResourceLimit)?;
        }
        Ok(())
    }
    fn raw(&mut self, count: usize) -> Result<&'a [u8]> {
        if count > self.bytes.len() {
            return Err(Error::Corrupt);
        }
        let (value, remaining) = self.bytes.split_at(count);
        self.bytes = remaining;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.raw(N)?
            .try_into()
            .map_err(|_decode_error| Error::Corrupt)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn count(&mut self, min_item: usize) -> Result<usize> {
        let count = usize::try_from(self.u64()?).map_err(|_decode_error| Error::ResourceLimit)?;
        if count > self.bytes.len() / min_item {
            return Err(Error::Corrupt);
        }
        Ok(count)
    }
    fn flag(&mut self) -> Result<bool> {
        match self.raw(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Corrupt),
        }
    }
    fn kind(&mut self) -> Result<NodeKind> {
        match self.raw(1)?[0] {
            0 => Ok(NodeKind::File),
            1 => Ok(NodeKind::Directory),
            2 => Ok(NodeKind::Symlink),
            _ => Err(Error::Corrupt),
        }
    }
    fn key(&mut self) -> Result<NodeKey> {
        Ok(NodeKey {
            repository: self.array::<16>()?.into(),
            revision: lore_base::types::Address {
                context: self.array::<16>()?.into(),
                hash: self.array::<32>()?.into(),
            },
            node: u32::from_le_bytes(self.array()?),
            exposure: self.u64()?,
        })
    }
    fn receipt(&mut self) -> Result<Option<Receipt>> {
        if !self.flag()? {
            return Ok(None);
        }
        let inode = self.u64()?;
        let authority = self.key()?;
        let generation = self.u64()?;
        let mode = u16::from_le_bytes(self.array()?);
        let node = self.node()?;
        let address = lore_base::types::Address {
            context: self.array::<16>()?.into(),
            hash: self.array::<32>()?.into(),
        };
        let sha256 = if self.flag()? {
            Some(self.array()?)
        } else {
            None
        };
        Ok(Some(Receipt {
            inode,
            authority,
            generation,
            mode,
            proof: CommittedProof { node, address },
            sha256,
        }))
    }
    fn address(&mut self) -> Result<Address> {
        Ok(Address {
            context: self.array::<16>()?.into(),
            hash: self.array::<32>()?.into(),
        })
    }
    fn baseline(&mut self) -> Result<HeldBaseline> {
        let scope = self.key()?;
        let sequence = self.u64()?;
        let revision = self.address()?;
        let root = if self.flag()? {
            Some(u32::from_le_bytes(self.array()?))
        } else {
            None
        };
        let parent_projection = if self.flag()? {
            let parent = self.key()?;
            let head = self.address()?;
            let target = if self.flag()? {
                Some(ScopePin {
                    repository: self.array::<16>()?.into(),
                    revision: self.address()?,
                    node: u32::from_le_bytes(self.array()?),
                })
            } else {
                None
            };
            Some(ScopeProjection {
                parent,
                head,
                target,
            })
        } else {
            None
        };
        Ok(HeldBaseline {
            scope,
            sequence,
            head: ScopeHead { revision, root },
            parent_projection,
        })
    }
    fn node(&mut self) -> Result<Node> {
        Ok(Node {
            key: self.key()?,
            kind: self.kind()?,
            size: self.u64()?,
            mode: u16::from_le_bytes(self.array()?),
        })
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<NamespaceSnapshot> {
    decode_with_memory(bytes, None)
}
pub(super) fn decode_budgeted(
    bytes: &[u8],
    memory: &mut CaptureMemory,
) -> Result<NamespaceSnapshot> {
    decode_with_memory(bytes, Some(memory))
}
fn decode_with_memory(
    bytes: &[u8],
    memory: Option<&mut CaptureMemory>,
) -> Result<NamespaceSnapshot> {
    if bytes.len() < MIN_SIZE {
        return Err(Error::Corrupt);
    }
    if &bytes[..8] != MAGIC {
        return Err(Error::Incompatible);
    }
    let checksum_start = bytes.len() - 32;
    if digest(&SHA256, &bytes[..checksum_start]).as_ref() != &bytes[checksum_start..] {
        return Err(Error::Corrupt);
    }
    let mut input = Decoder {
        bytes: &bytes[8..checksum_start],
        memory,
    };
    if input.u64()? != checksum_start as u64 {
        return Err(Error::Corrupt);
    }
    let original_root = input.node()?;
    let count = input.count(125)?;
    if count > crate::core::MAX_OWNING_SCOPES {
        return Err(Error::ResourceLimit);
    }
    let mut active_underlays = BTreeMap::new();
    for _ in 0..count {
        let scope = input.key()?;
        let revision = input.address()?;
        let root = if input.flag()? {
            Some(input.u32()?)
        } else {
            None
        };
        input.map_record()?;
        if active_underlays
            .insert(scope, ScopeHead { revision, root })
            .is_some()
        {
            return Err(Error::Corrupt);
        }
    }
    let state_length = input.count(1)?;
    if state_length > crate::core::MAX_BACKEND_STATE_SIZE {
        return Err(Error::ResourceLimit);
    }
    let backend_state = input.blob(state_length, false)?;
    let count = input.count(134)?;
    if count > crate::core::MAX_OWNING_SCOPES {
        return Err(Error::ResourceLimit);
    }
    let mut baselines = BTreeMap::new();
    for _ in 0..count {
        let baseline = input.baseline()?;
        input.map_record()?;
        if baselines.insert(baseline.scope, baseline).is_some() {
            return Err(Error::Corrupt);
        }
    }
    let count = input.count(16)?;
    let mut files = BTreeMap::new();
    for _ in 0..count {
        let inode = input.u64()?;
        let extent_count = input.count(24)?;
        let mut extents = input.vector(extent_count)?;
        for _ in 0..extent_count {
            let length = input.count(1)?;
            let data = input.blob(length, false)?;
            extents.push(SnapshotExtent {
                start: input.u64()?,
                stored: input.u64()?,
                data,
            });
        }
        let node = input.node()?;
        let metadata = Metadata {
            size: input.u64()?,
            mode: u16::from_le_bytes(input.array()?),
            generation: input.u64()?,
            mutable: input.flag()?,
            writers: 0,
            reserved_storage_bytes: input.u64()?,
        };
        let base_end = input.u64()?;
        let receipt = input.receipt()?;
        let file = FileSnapshot {
            receipt,
            node,
            metadata,
            base_end,
            extents,
        };
        input.map_record()?;
        if files.insert(inode, file).is_some() {
            return Err(Error::Corrupt);
        }
    }
    let next_inode = input.u64()?;
    let generation = input.u64()?;
    let count = input.count(96)?;
    let mut nodes = input.vector(count)?;
    for _ in 0..count {
        let inode = input.u64()?;
        let parent = input.u64()?;
        let kind = input.kind()?;
        let authority = input.key()?;
        let base = if input.flag()? {
            Some(input.node()?)
        } else {
            None
        };
        let attached = input.flag()?;
        let receipt = input.receipt()?;
        let names = if input.flag()? {
            let count = input.count(16)?;
            let mut names = input.vector(count)?;
            for _ in 0..count {
                let length = input.count(1)?;
                names.push((input.blob(length, true)?, input.u64()?));
            }
            Some(names)
        } else {
            None
        };
        nodes.push(SnapshotNode {
            receipt,
            entry: Entry {
                inode,
                parent,
                kind,
                authority,
                base,
            },
            names,
            attached,
        });
    }
    if !input.bytes.is_empty() {
        return Err(Error::Corrupt);
    }
    Ok(NamespaceSnapshot {
        original_root,
        active_underlays,
        nodes,
        next_inode,
        generation,
        files,
        backend_state,
        baselines,
    })
}
