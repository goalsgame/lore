// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Resource completion is separate from publisher joining. Every publisher
//! payload and returned output retains progress after its owned resources.

use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::Ordering;

use crate::checkpoint_owner::CheckpointOwner;
use crate::checkpoint_owner::Publishing;

// Owner resources drop before the final publication count. A caller must retain
// this value last in every task/output wrapper, including error outputs.
pub(in crate::checkpoint_owner) struct Progress {
    pub(super) owner: Arc<CheckpointOwner>,
    _completion: Publishing,
}
impl Progress {
    pub(in crate::checkpoint_owner) fn owner(&self) -> &Arc<CheckpointOwner> {
        &self.owner
    }

    pub(in crate::checkpoint_owner) fn new(
        owner: Arc<CheckpointOwner>,
        completion: Publishing,
    ) -> Arc<Self> {
        Arc::new(Self {
            owner,
            _completion: completion,
        })
    }
}

// Declared before a handoff job's JoinHandle, with its Progress last. An armed
// drop poisons before detaching, even when the publisher still retains progress.
pub(in crate::checkpoint_owner) struct HandoffGuard {
    owner: Weak<CheckpointOwner>,
    armed: bool,
}
impl HandoffGuard {
    pub(in crate::checkpoint_owner) fn new(progress: &Arc<Progress>) -> Self {
        Self {
            owner: Arc::downgrade(&progress.owner),
            armed: true,
        }
    }
    pub(in crate::checkpoint_owner) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for HandoffGuard {
    fn drop(&mut self) {
        if self.armed {
            poison(&self.owner);
        }
    }
}

// This guard owns no Model, checkpoint or storage capability after completion.
// It covers joiner panics outside the handoff job's own armed interval.
pub(in crate::checkpoint_owner) struct JoinerPanicGuard(Weak<CheckpointOwner>);
impl JoinerPanicGuard {
    pub(in crate::checkpoint_owner) fn new(owner: &Arc<CheckpointOwner>) -> Self {
        Self(Arc::downgrade(owner))
    }
}
impl Drop for JoinerPanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            poison(&self.0);
        }
    }
}
fn poison(owner: &Weak<CheckpointOwner>) {
    if let Some(owner) = owner.upgrade() {
        owner.poisoned.store(true, Ordering::Release);
    }
}
