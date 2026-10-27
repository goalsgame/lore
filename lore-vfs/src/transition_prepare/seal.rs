// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Metadata admission primitive only. It cannot install state or publish intent.
//! The epoch witness is not authorization: policy can revoke without changing
//! metadata. A future preallocation/intent owner must separately recheck source
//! and target policy before irreversible work, outside publication locks.
#![allow(
    dead_code,
    reason = "Production transition dispatch remains disabled until preallocation and durable intent ownership are integrated."
)]
use super::*;

pub(crate) struct MetadataSealedPreparation {
    // Drop admission seal before the preparation's final exact freeze pin.
    seal: crate::namespace::MetadataSeal,
    preparation: PreparedTransition,
}
impl PreparedTransition {
    pub(crate) async fn seal_metadata(self) -> Result<MetadataSealedPreparation> {
        let witness = crate::namespace::MetadataWitness {
            epoch: self.source_epoch,
            generation: self.snapshot.generation,
            next_inode: self.snapshot.next_inode,
        };
        let seal = self
            .freeze
            .namespace()
            .seal_metadata(self.freeze.pin(), witness)
            .await
            .map_err(|error| match error {
                NamespaceError::Backend(core::Error::Stale) => Error::StaleSource,
                error => error.into(),
            })?;
        Ok(MetadataSealedPreparation {
            seal,
            preparation: self,
        })
    }
}
impl MetadataSealedPreparation {
    pub(crate) fn preparation(&self) -> &PreparedTransition {
        &self.preparation
    }
    pub(crate) fn reopen(self) -> PreparedTransition {
        let Self { seal, preparation } = self;
        drop(seal);
        preparation
    }
}
