// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Paired candidate snapshot/encoding and per-candidate accounting. No intent,
//! current-source claim, checkpoint publication or live-map coherence promise.
mod source_preparation;
pub(crate) use source_preparation::OwnedSourcePreparation;
pub(crate) use source_preparation::SourceContinuation;

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::capture_memory::Footprint;
use crate::namespace::NamespaceSnapshot;
use crate::recovery::LoadedCheckpoint;

pub(crate) struct CapturedCandidate {
    source: Option<LoadedCheckpoint>,
    snapshot: NamespaceSnapshot,
    encoded: Vec<u8>,
    memory: CaptureMemory,
    // The external artifact retains the original admission/Model/freeze. It
    // cannot be stored unchanged in CheckpointOwner without making a cycle.
    candidate: UnpublishedCandidate,
}
/// Borrowed together from one immutable capture; no constructor accepts a
/// separately supplied snapshot or encoded buffer.
pub(crate) struct CapturedCheckpointImage<'a> {
    snapshot: &'a NamespaceSnapshot,
    encoded: &'a [u8],
}
impl CapturedCheckpointImage<'_> {
    pub(crate) fn snapshot(&self) -> &NamespaceSnapshot {
        self.snapshot
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        self.encoded
    }
}
impl CapturedCandidate {
    pub(crate) fn artifact_parts(&mut self) -> (CapturedCheckpointImage<'_>, &mut CaptureMemory) {
        (
            CapturedCheckpointImage {
                snapshot: &self.snapshot,
                encoded: &self.encoded,
            },
            &mut self.memory,
        )
    }
}
impl UnpublishedCandidate {
    pub(crate) async fn capture_checkpoint(
        self,
        source: Option<LoadedCheckpoint>,
        limits: CaptureLimits,
    ) -> PreparedResult<CapturedCandidate> {
        let owner = &self.admission.owner;
        owner.live()?;
        let mut closed = owner.closed.subscribe();
        owner.live()?;
        let capture = async {
            let mut memory = CaptureMemory::new(limits)?;
            self.state.retain_capture_inputs(&mut memory)?;
            self.metadata.retain_capture_inputs(&mut memory)?;
            memory.nodes(self.files.len())?;
            if let Some(source) = &source {
                memory.buffer(source.encoded_capacity())?;
                memory.retain(Footprint::snapshot(source.snapshot())?)?;
                // Its earlier decode/validation peak is outside this adoption
                // boundary; retained source capacities are charged from here.
            }
            let snapshot = self
                .state
                .capture_snapshot(
                    &mut memory,
                    owner.limits.max_checkpoint_data_bytes,
                    owner.limits.file,
                )
                .await?;
            let encoded = crate::recovery::encode_candidate(
                &snapshot,
                owner.checkpoint.capture_byte_limit(),
                &mut memory,
            )
            .map_err(super::super::super::Error::from)?;
            self.state.validate_source().await?;
            owner.live()?;
            Ok::<_, PreparationError>((snapshot, encoded, memory))
        };
        let (snapshot, encoded, memory) = tokio::select! { biased;
            _=closed.changed()=>return Err(core::Error::Unmounted.into()),
            result=capture=>result?,
        };
        owner.live()?;
        Ok(CapturedCandidate {
            source,
            snapshot,
            encoded,
            memory,
            candidate: self,
        })
    }
}

#[cfg(test)]
impl CapturedCandidate {
    pub(crate) fn test_exhaust_source_authority_memory(&mut self) {
        let remaining = self.memory.remaining_buffer_bytes();
        self.memory.buffer(remaining).unwrap();
    }
    pub(crate) fn test_read_gate(&self, inode: u64) -> Arc<dyn AccessGate> {
        self.candidate.state.test_read_gate(inode)
    }
    pub(crate) fn into_test_snapshot(self) -> NamespaceSnapshot {
        self.snapshot
    }
    pub(crate) fn test_source_hash(&self) -> Option<[u8; 32]> {
        self.source.as_ref().map(|source| source.identity().sha256)
    }
    pub(crate) fn test_snapshot(&self) -> &NamespaceSnapshot {
        &self.snapshot
    }
    pub(crate) fn test_encoding(&self) -> &Vec<u8> {
        &self.encoded
    }
    pub(crate) fn test_nodes(&self) -> usize {
        self.memory.reserved_nodes()
    }
    pub(crate) fn test_memory(&self) -> usize {
        self.memory.reserved_buffers()
    }
}
