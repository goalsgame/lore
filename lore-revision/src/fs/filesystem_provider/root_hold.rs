// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Transactional root replacement, dispatched before a legacy operation freeze.

use super::*;
use crate::instance::strict_anchors::Snapshot;
use crate::repository::WorkingCommandHandoff;

/// Exact command-owned input. Construction prepares immutable staged metadata
/// and retains root exclusion; it changes no normal or staged anchor.
pub struct RootHoldRequest {
    handoff: Arc<WorkingCommandHandoff>,
    target: Hash,
    old: Snapshot,
    new: Snapshot,
    options: HoldOptions,
}
impl RootHoldRequest {
    pub(crate) async fn prepare(
        repository: &RepositoryContext,
        target: Hash,
        branch: crate::lore::BranchId,
        options: HoldOptions,
    ) -> Result<Self, FsError> {
        let (handoff, old, new) = repository
            .prepare_root_anchors(target, branch, options)
            .await
            .map_err(problem)?;
        Ok(Self {
            handoff,
            target,
            old,
            new,
            options,
        })
    }
    pub fn target(&self) -> Hash {
        self.target
    }
    pub fn old_anchors(&self) -> Snapshot {
        self.old
    }
    pub fn new_anchors(&self) -> Snapshot {
        self.new
    }
    pub fn options(&self) -> HoldOptions {
        self.options
    }
    pub fn handoff(&self) -> &Arc<WorkingCommandHandoff> {
        &self.handoff
    }
}

/// An applied result acknowledges completed durable publication and installation.
/// Preview and unchanged results do not acknowledge a durable transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootHoldOutcome {
    Applied,
    Unchanged,
    Preview,
}

/// Trusted external-provider protocol: implement only for an acknowledged
/// durable intent retaining its images and pending owner. This cross-crate
/// contract is not an unforgeable security boundary for arbitrary Rust callers.
pub trait DurableRootIntent: Send + Sync {
    fn expected_anchors(&self) -> Snapshot;
    fn replacement_anchors(&self) -> Snapshot;
}
fn problem(error: impl std::fmt::Display) -> FsError {
    FsError::internal(format!("Preparing transactional root hold: {error}"))
}
