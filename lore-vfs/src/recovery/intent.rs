// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Pure pending-intent metadata; no disk publication or installation capability.
//! Checkpoint identities bind borrowed bytes, not validated checkpoint images.
//! A coordinator must validate images, scope provenance, policy and exclusive
//! ownership before using these facts in a durable transition protocol.
#![allow(
    dead_code,
    reason = "Durable transition coordination is not integrated."
)]

use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::Partition;
use lore_revision::instance::strict_anchors::Snapshot;
use ring::digest::SHA256;
use ring::digest::digest;

use crate::core::NodeKey;

const MAGIC: &[u8; 8] = b"LOREIN01";
/// Fixed metadata only; checkpoint payloads are never copied into this record.
const HASH_BYTES: usize = 32;
const ID_BYTES: usize = 16;
const NODE_KEY_BYTES: usize = ID_BYTES + ID_BYTES + HASH_BYTES + 4 + 8;
const CHECKPOINT_ID_BYTES: usize = 8 + HASH_BYTES;
const ANCHOR_BYTES: usize = ID_BYTES + ID_BYTES + HASH_BYTES + ID_BYTES + 1 + HASH_BYTES;
pub(crate) const ENCODED_SIZE: usize = MAGIC.len()
    + HASH_BYTES
    + 2 * NODE_KEY_BYTES
    + 2 * CHECKPOINT_ID_BYTES
    + 2 * ANCHOR_BYTES
    + HASH_BYTES;
const PAYLOAD_SIZE: usize = ENCODED_SIZE - 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    ResourceLimit,
    Corrupt,
    Incompatible,
    InvalidIdentity,
    AmbiguousAnchors,
}
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CheckpointIdentity {
    pub(crate) length: u64,
    pub(crate) sha256: [u8; 32],
}
impl CheckpointIdentity {
    /// Hashes the complete supplied bytes without copying or decoding them.
    pub(crate) fn from_bytes(bytes: &[u8], maximum: usize) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > maximum {
            return Err(Error::ResourceLimit);
        }
        Ok(Self {
            length: u64::try_from(bytes.len()).map_err(|_length_overflow| Error::ResourceLimit)?,
            sha256: digest(&SHA256, bytes).as_ref().try_into().unwrap(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Anchors {
    pub(crate) repository: Partition,
    pub(crate) instance: [u8; 16],
    pub(crate) current: Hash,
    pub(crate) branch: Context,
    pub(crate) staged: Option<Hash>,
}
impl From<Snapshot> for Anchors {
    fn from(value: Snapshot) -> Self {
        Self {
            repository: value.repository,
            instance: *value.instance.data(),
            current: value.current,
            branch: value.branch,
            staged: value.staged,
        }
    }
}
impl Anchors {
    fn validate(self) -> Result<()> {
        if self.repository.is_zero()
            || self.instance == [0; 16]
            || self.branch.is_zero()
            || self.staged.is_some_and(|hash| hash.is_zero())
        {
            return Err(Error::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Intent {
    pub(crate) transaction: [u8; 32],
    pub(crate) original_root: NodeKey,
    /// The selected owning exposure may belong to a linked repository.
    pub(crate) scope: NodeKey,
    pub(crate) source: CheckpointIdentity,
    pub(crate) candidate: CheckpointIdentity,
    pub(crate) old: Anchors,
    pub(crate) new: Anchors,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Selection {
    Source,
    Candidate,
    /// Retain both images; no mounted view can be inferred from these anchors.
    Unresolved,
}
impl Intent {
    fn validate(&self, maximum_checkpoint_bytes: usize) -> Result<()> {
        if self.transaction == [0; 32]
            || self.original_root.repository.is_zero()
            || self.scope.repository.is_zero()
            || self.old.repository != self.scope.repository
            || self.new.repository != self.scope.repository
            || self.old.instance != self.new.instance
        {
            return Err(Error::InvalidIdentity);
        }
        self.old.validate()?;
        self.new.validate()?;
        for checkpoint in [self.source, self.candidate] {
            if checkpoint.length == 0
                || u64::try_from(maximum_checkpoint_bytes)
                    .is_ok_and(|maximum| checkpoint.length > maximum)
            {
                return Err(Error::ResourceLimit);
            }
        }
        // Identical tuples cannot distinguish image selection, even for a no-op.
        if self.old == self.new {
            return Err(Error::AmbiguousAnchors);
        }
        Ok(())
    }
    pub(crate) fn encode(&self, maximum_checkpoint_bytes: usize) -> Result<[u8; ENCODED_SIZE]> {
        self.validate(maximum_checkpoint_bytes)?;
        let mut out = Encoder {
            bytes: [0; ENCODED_SIZE],
            position: 0,
        };
        out.raw(MAGIC);
        out.raw(&self.transaction);
        out.key(self.original_root);
        out.key(self.scope);
        for checkpoint in [self.source, self.candidate] {
            out.raw(&checkpoint.length.to_le_bytes());
            out.raw(&checkpoint.sha256);
        }
        out.anchors(self.old);
        out.anchors(self.new);
        debug_assert_eq!(out.position, PAYLOAD_SIZE);
        let checksum = digest(&SHA256, &out.bytes[..PAYLOAD_SIZE]);
        out.raw(checksum.as_ref());
        Ok(out.bytes)
    }
    pub(crate) fn decode(bytes: &[u8], maximum_checkpoint_bytes: usize) -> Result<Self> {
        if bytes.len() != ENCODED_SIZE {
            return Err(Error::Corrupt);
        }
        if &bytes[..8] != MAGIC {
            return Err(Error::Incompatible);
        }
        if digest(&SHA256, &bytes[..PAYLOAD_SIZE]).as_ref() != &bytes[PAYLOAD_SIZE..] {
            return Err(Error::Corrupt);
        }
        let mut input = Decoder {
            bytes: &bytes[8..PAYLOAD_SIZE],
        };
        let intent = Self {
            transaction: input.array()?,
            original_root: input.key()?,
            scope: input.key()?,
            source: input.checkpoint()?,
            candidate: input.checkpoint()?,
            old: input.anchors()?,
            new: input.anchors()?,
        };
        if !input.bytes.is_empty() {
            return Err(Error::Corrupt);
        }
        intent.validate(maximum_checkpoint_bytes)?;
        Ok(intent)
    }
    /// Classifies a successfully read exact tuple only. This does not authorize
    /// activation, flush anchors, resolve IO errors or complete an intent.
    pub(crate) fn classify(
        &self,
        observed: Snapshot,
        maximum_checkpoint_bytes: usize,
    ) -> Result<Selection> {
        self.validate(maximum_checkpoint_bytes)?;
        let observed = Anchors::from(observed);
        observed.validate()?;
        Ok(if observed == self.old {
            Selection::Source
        } else if observed == self.new {
            Selection::Candidate
        } else {
            Selection::Unresolved
        })
    }
}
struct Encoder {
    bytes: [u8; ENCODED_SIZE],
    position: usize,
}
impl Encoder {
    fn raw(&mut self, bytes: &[u8]) {
        self.bytes[self.position..self.position + bytes.len()].copy_from_slice(bytes);
        self.position += bytes.len();
    }
    fn key(&mut self, key: NodeKey) {
        self.raw(key.repository.data());
        self.raw(key.revision.context.data());
        self.raw(key.revision.hash.data());
        self.raw(&key.node.to_le_bytes());
        self.raw(&key.exposure.to_le_bytes());
    }
    fn anchors(&mut self, anchors: Anchors) {
        self.raw(anchors.repository.data());
        self.raw(&anchors.instance);
        self.raw(anchors.current.data());
        self.raw(anchors.branch.data());
        self.raw(&[u8::from(anchors.staged.is_some())]);
        self.raw(anchors.staged.unwrap_or_default().data());
    }
}
struct Decoder<'a> {
    bytes: &'a [u8],
}
impl Decoder<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        if self.bytes.len() < N {
            return Err(Error::Corrupt);
        }
        let (head, tail) = self.bytes.split_at(N);
        self.bytes = tail;
        Ok(head.try_into().unwrap())
    }
    fn key(&mut self) -> Result<NodeKey> {
        Ok(NodeKey {
            repository: self.array::<16>()?.into(),
            revision: lore_base::types::Address {
                context: self.array::<16>()?.into(),
                hash: self.array::<32>()?.into(),
            },
            node: u32::from_le_bytes(self.array()?),
            exposure: u64::from_le_bytes(self.array()?),
        })
    }
    fn checkpoint(&mut self) -> Result<CheckpointIdentity> {
        Ok(CheckpointIdentity {
            length: u64::from_le_bytes(self.array()?),
            sha256: self.array()?,
        })
    }
    fn anchors(&mut self) -> Result<Anchors> {
        let repository = self.array::<16>()?.into();
        let instance = self.array()?;
        let current = self.array::<32>()?.into();
        let branch = self.array::<16>()?.into();
        let flag = self.array::<1>()?[0];
        let value: Hash = self.array::<32>()?.into();
        let staged = match flag {
            0 if value.is_zero() => None,
            1 if !value.is_zero() => Some(value),
            _ => return Err(Error::Corrupt),
        };
        Ok(Anchors {
            repository,
            instance,
            current,
            branch,
            staged,
        })
    }
}

#[cfg(test)]
mod tests;
