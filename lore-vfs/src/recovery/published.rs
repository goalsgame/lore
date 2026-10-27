// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Retained bytes from successful local recovery publication. This durable fact
//! grants no mount ownership, currentness after later writes, or intent authority.
#![allow(
    dead_code,
    reason = "Retained source acknowledgment is not integrated."
)]

use super::*;
use crate::capture_memory::CaptureMemory;
use crate::recovery::intent::CheckpointIdentity;

pub(crate) struct PublishedCheckpointImage {
    snapshot: NamespaceSnapshot,
    bytes: Vec<u8>,
    identity: CheckpointIdentity,
}
impl PublishedCheckpointImage {
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
}
impl Recovery {
    /// The moved snapshot is already charged to this same operation ledger,
    /// alongside its other retained inputs. This method charges new load,
    /// validation, successor and encoding work; it does not double-charge the
    /// supplied image or validate runtime ownership/authorization.
    pub(crate) fn publish_retained(
        &mut self,
        snapshot: NamespaceSnapshot,
        memory: &mut CaptureMemory,
    ) -> Result<PublishedCheckpointImage> {
        if self.uncertain {
            return Err(Error::Uncertain);
        }
        transition_admission::check(&self.directory)?;
        if self.publication_pending || self.has_pending()? {
            return Err(Error::Pending);
        }
        let previous = self.load_exact_budgeted(memory)?;
        budgeted::validate(&snapshot, self.limits.max_checkpoint_bytes, memory)?;
        if let Some(previous) = &previous {
            budgeted::successor(previous.snapshot(), &snapshot, memory)?;
        }
        drop(previous);
        let bytes = prepared_encoding::encode_candidate(
            &snapshot,
            self.limits.max_checkpoint_bytes,
            memory,
        )?;
        let identity = CheckpointIdentity::from_bytes(&bytes, self.limits.max_checkpoint_bytes)
            .map_err(|_identity_limit| Error::ResourceLimit)?;
        // All fallible preparation precedes local publication. After directory
        // sync, constructing the retained result only moves owned values.
        self.write_checkpoint_bytes(&bytes)?;
        Ok(PublishedCheckpointImage {
            snapshot,
            bytes,
            identity,
        })
    }
}
