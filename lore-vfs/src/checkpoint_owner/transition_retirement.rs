// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Retirement permission after exact withdrawal and actual transition joining.

use std::sync::atomic::AtomicU8;

use lore_revision::repository::WorkingCommandHandoff;

use super::*;

#[derive(Clone, Copy)]
pub(crate) enum RetirementObligation {
    PendingIntent,
    DurableCheckpoint,
}

#[derive(Default)]
pub(super) struct RetirementProgress(AtomicU8);
impl RetirementProgress {
    // Called only after propagated resources finish and their runtime drops.
    pub(super) fn resources_finished(&self, completed: bool) {
        self.0
            .store(if completed { 2 } else { 1 }, Ordering::Release);
    }
    fn obligation(&self) -> Option<RetirementObligation> {
        match self.0.load(Ordering::Acquire) {
            1 => Some(RetirementObligation::PendingIntent),
            2 => Some(RetirementObligation::DurableCheckpoint),
            _ => None,
        }
    }
}

impl CheckpointOwner {
    // The caller has joined the actual publisher, including its destructors.
    // Neither a poison flag nor a detached worker can mint this permission.
    pub(super) fn certify_transition_retirement(
        &self,
        progress: &RetirementProgress,
        handoff: &WorkingCommandHandoff,
    ) -> Result<()> {
        if let Some(obligation) = progress.obligation() {
            handoff.quarantine().map_err(|_error| Error::Poisoned)?;
            *self
                .transition_retirement
                .lock()
                .map_err(|_poison| Error::Poisoned)? = Some(obligation);
        }
        Ok(())
    }

    pub(crate) fn transition_retirement_obligation(&self) -> Result<Option<RetirementObligation>> {
        self.transition_retirement
            .lock()
            .map(|obligation| *obligation)
            .map_err(|_poison| Error::Poisoned)
    }
}
