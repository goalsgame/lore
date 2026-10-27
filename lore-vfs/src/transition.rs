// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit read-only view transitions with immutable open file reads and directory
//! enumeration. Root-relative lookups, including openat on an old root descriptor,
//! follow the active view: FUSE LOOKUP carries no directory handle. Relative lookup
//! through a retained nonroot directory inode stays in that inode's old view.
//!
//! Publication drains transport replies briefly, then admits candidate callbacks
//! before invalidation to avoid deadlocking kernel inode locks. Paths may observe
//! either generation until acknowledgment. Failure republishes the prior view and
//! invalidates again. A failed rollback invalidation poisons further switches until
//! explicit cache recovery. Both generations and all issued inodes remain retained.
//! This controller accepts read-only Core views; it cannot switch mutable overlays.

use std::io;

use super::*;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_generations: usize,
    pub max_inodes: usize,
    pub max_root_names: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_generations: 16,
            max_inodes: 1_000_000,
            max_root_names: 65_536,
        }
    }
}
/// Conservative invalidation at the fixed mount root. New nonroot inodes are
/// unique per generation, so old file data and attributes never need rebinding.
pub struct Invalidation {
    pub root_names: Vec<Vec<u8>>,
}
/// Must return only after the kernel has accepted each invalidation, or an error.
/// Notifications run without the callback publication barrier held.
pub trait Invalidator: Send + Sync {
    fn invalidate(&self, plan: &Invalidation) -> io::Result<()>;
}
#[derive(Debug)]
pub enum TransitionError {
    Core(Error),
    /// The previous view is active. A rollback error leaves caches uncertain.
    Invalidation {
        source: io::Error,
        rollback: Option<io::Error>,
    },
    Poisoned,
    Unsupported,
}
impl From<Error> for TransitionError {
    fn from(error: Error) -> Self {
        Self::Core(error)
    }
}
pub type TransitionResult<T> = std::result::Result<T, TransitionError>;

impl Core {
    /// Synchronous control operation: call outside callbacks and async workers.
    /// Success identifies the active generation after kernel invalidation.
    pub fn transition(
        &self,
        backend: Arc<dyn Backend>,
        root: Node,
        invalidator: &dyn Invalidator,
    ) -> TransitionResult<u64> {
        if root.kind != NodeKind::Directory {
            return Err(Error::NotDirectory.into());
        }
        let _switch = self.transition_lock.lock().map_err(|_poisoned| Error::Io)?;
        let (old, candidate, plan) = {
            let _callbacks = self.callbacks.write().map_err(|_poisoned| Error::Io)?;
            let mut state = self.state()?;
            state.live()?;
            if state.transition_poisoned {
                return Err(TransitionError::Poisoned);
            }
            if state.views.len() >= state.limits.max_generations
                || state.entries.len().saturating_add(state.views.len()) >= state.limits.max_inodes
            {
                return Err(Error::ResourceLimit.into());
            }
            let candidate = state.next_generation;
            let next = candidate.checked_add(1).ok_or(Error::ResourceLimit)?;
            let old = state.current;
            state.views.insert(candidate, View { backend, root });
            state.current = candidate;
            state.next_generation = next;
            (
                old,
                candidate,
                Invalidation {
                    root_names: state.root_names.iter().cloned().collect(),
                },
            )
        };
        if let Err(source) = invalidator.invalidate(&plan) {
            let plan = {
                let _callbacks = self.callbacks.write().map_err(|_poisoned| Error::Io)?;
                let mut state = self.state()?;
                state.live()?;
                state.current = old;
                Invalidation {
                    root_names: state.root_names.iter().cloned().collect(),
                }
            };
            let rollback = invalidator.invalidate(&plan).err();
            let mut state = self.state()?;
            state.live()?;
            if rollback.is_some() {
                state.transition_poisoned = true;
            }
            return Err(TransitionError::Invalidation { source, rollback });
        }
        self.state()?.live()?;
        Ok(candidate)
    }
    /// Cancellation of this awaiting future does not abandon publication or rollback.
    pub async fn transition_async(
        self: Arc<Self>,
        backend: Arc<dyn Backend>,
        root: Node,
        invalidator: Arc<dyn Invalidator>,
    ) -> TransitionResult<u64> {
        lore_base::lore_spawn_blocking_nocontext!(move || self.transition(
            backend,
            root,
            invalidator.as_ref()
        ))
        .await
        .map_err(|_join_error| Error::Io)?
    }
    /// Explicitly recover uncertain kernel caches for the currently active view.
    pub fn recover_transition_cache(&self, invalidator: &dyn Invalidator) -> TransitionResult<()> {
        let _switch = self.transition_lock.lock().map_err(|_poisoned| Error::Io)?;
        let plan = {
            let _callbacks = self.callbacks.write().map_err(|_poisoned| Error::Io)?;
            let state = self.state()?;
            state.live()?;
            Invalidation {
                root_names: state.root_names.iter().cloned().collect(),
            }
        };
        invalidator
            .invalidate(&plan)
            .map_err(|source| TransitionError::Invalidation {
                source,
                rollback: None,
            })?;
        let mut state = self.state()?;
        state.live()?;
        state.transition_poisoned = false;
        Ok(())
    }
}
#[cfg(test)]
#[path = "transition/tests.rs"]
mod tests;
