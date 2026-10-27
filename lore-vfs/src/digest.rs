// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Whole-file SHA-256 publication for immutable content generations.
//!
//! Adapters serialize this state with their inode state. Before granting writable
//! access (including writable mappings), call `mark_dirty`. Do not restore an
//! immutable identity until writers and mappings have drained and content has
//! been committed. A missing digest means ENOATTR on Darwin, ENODATA on Linux.

use std::io::Read;
use std::io::{self};
use std::sync::Arc;

use lore_base::types::Address;
use ring::digest::Context;
use ring::digest::SHA256;

pub const XATTR_NAME: &str = "user.sha256";

/// Includes address context and whole-file length; paths are not content identities.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ContentIdentity {
    pub address: Address,
    pub size: u64,
}

/// Digest availability for one inode. The token also distinguishes inode instances.
pub struct DigestState {
    generation: Arc<()>,
    content: Option<ContentIdentity>,
    digest: Option<[u8; 32]>,
}

pub struct DigestJob {
    generation: Arc<()>,
    content: ContentIdentity,
}

pub struct DigestCompletion {
    generation: Arc<()>,
    content: ContentIdentity,
    digest: [u8; 32],
}

impl DigestState {
    pub fn new(content: ContentIdentity) -> Self {
        Self {
            generation: Arc::new(()),
            content: Some(content),
            digest: None,
        }
    }

    /// Returns raw digest bytes, never hexadecimal text. Does not read file content.
    pub fn xattr(&self) -> Option<&[u8; 32]> {
        self.digest.as_ref()
    }

    /// Revoke the digest before mutable bytes can become visible, even at equal size.
    pub fn mark_dirty(&mut self) {
        self.generation = Arc::new(());
        self.content = None;
        self.digest = None;
    }

    /// Publish an immutable version after the adapter has quiesced writable access.
    /// Even returning to the same address creates a new generation.
    pub fn replace(&mut self, content: ContentIdentity) {
        *self = Self::new(content);
    }

    pub fn begin_hash(&self) -> Option<DigestJob> {
        self.content.map(|content| DigestJob {
            generation: Arc::clone(&self.generation),
            content,
        })
    }

    /// Ignore completions from replaced content, dirty inodes, or other inodes.
    pub fn complete(&mut self, completion: DigestCompletion) -> bool {
        if !Arc::ptr_eq(&self.generation, &completion.generation)
            || self.content != Some(completion.content)
        {
            return false;
        }
        self.digest = Some(completion.digest);
        true
    }
}

impl DigestJob {
    pub fn content(&self) -> ContentIdentity {
        self.content
    }

    /// Hash the complete, reconstructed file using bounded memory.
    ///
    /// The caller must supply a reader pinned to `content()` through Lore's verified
    /// content-read path. SHA-256 and length alone cannot validate a Lore address:
    /// fragmented content addresses can identify reference lists rather than file bytes.
    /// No completion is produced on a read error or length mismatch.
    pub fn compute(self, mut verified_reader: impl Read) -> io::Result<DigestCompletion> {
        let mut hash = Context::new(&SHA256);
        let mut remaining = self.content.size;
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let count = match verified_reader.read(&mut buffer) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if count == 0 {
                break;
            }
            remaining = remaining.checked_sub(count as u64).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "content exceeds expected length",
                )
            })?;
            hash.update(&buffer[..count]);
        }
        if remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete content",
            ));
        }
        let mut digest = [0; 32];
        digest.copy_from_slice(hash.finish().as_ref());
        Ok(DigestCompletion {
            generation: self.generation,
            content: self.content,
            digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(size: u64) -> ContentIdentity {
        ContentIdentity {
            address: Address::default(),
            size,
        }
    }

    #[test]
    fn publishes_raw_sha256() {
        let mut state = DigestState::new(identity(3));
        assert!(state.xattr().is_none());
        let result = state.begin_hash().unwrap().compute(&b"abc"[..]).unwrap();
        assert!(state.complete(result));
        assert_eq!(
            state.xattr().unwrap(),
            &[
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
        state.mark_dirty();
        assert!(state.xattr().is_none());
        assert!(state.begin_hash().is_none());
    }

    #[test]
    fn dirty_state_rejects_in_flight_hash() {
        let mut state = DigestState::new(identity(3));
        let job = state.begin_hash().unwrap();
        state.mark_dirty();
        assert!(!state.complete(job.compute(&b"abc"[..]).unwrap()));
        assert!(state.xattr().is_none());
    }

    #[test]
    fn same_size_revision_rejects_old_completion() {
        let mut state = DigestState::new(identity(3));
        let job = state.begin_hash().unwrap();
        let mut changed = identity(3);
        changed.address.hash = [1; 32].into();
        state.replace(changed);
        assert!(!state.complete(job.compute(&b"abc"[..]).unwrap()));
        let result = state.begin_hash().unwrap().compute(&b"ABC"[..]).unwrap();
        assert!(state.complete(result));
    }

    #[test]
    fn returning_to_same_identity_does_not_revive_old_jobs() {
        let mut state = DigestState::new(identity(3));
        let job = state.begin_hash().unwrap();
        state.mark_dirty();
        state.replace(identity(3));
        assert!(!state.complete(job.compute(&b"abc"[..]).unwrap()));
    }

    #[test]
    fn completion_cannot_cross_inode_instances() {
        let first = DigestState::new(identity(3));
        let mut second = DigestState::new(identity(3));
        assert!(!second.complete(first.begin_hash().unwrap().compute(&b"abc"[..]).unwrap()));
    }

    #[test]
    fn rejects_truncated_and_oversized_streams() {
        let state = DigestState::new(identity(3));
        assert!(
            matches!(state.begin_hash().unwrap().compute(&b"ab"[..]), Err(e) if e.kind() == io::ErrorKind::UnexpectedEof)
        );
        assert!(
            matches!(state.begin_hash().unwrap().compute(&b"abcd"[..]), Err(e) if e.kind() == io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn read_errors_do_not_publish() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("verification failed"))
            }
        }
        let state = DigestState::new(identity(3));
        assert!(state.begin_hash().unwrap().compute(Broken).is_err());
        assert!(state.xattr().is_none());
    }

    #[test]
    fn empty_file_has_a_digest() {
        let mut state = DigestState::new(identity(0));
        let result = state.begin_hash().unwrap().compute(io::empty()).unwrap();
        assert!(state.complete(result));
        assert_eq!(&state.xattr().unwrap()[..4], &[0xe3, 0xb0, 0xc4, 0x42]);
    }
}
