// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Mutex;
use std::thread::JoinHandle;

use super::Error;
use super::Result;

enum State {
    Unpublished,
    Running(JoinHandle<()>),
    Joining,
    Joined(Result<()>),
}

/// Retains the actual worker until its thread has exited, independently of any
/// caller waiting for logical completion or cancellation of that wait.
#[allow(private_interfaces)]
#[lore_macro::test_pub]
pub(super) struct ThreadOwner {
    state: Mutex<State>,
}

impl Default for ThreadOwner {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadOwner {
    #[lore_macro::test_pub]
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State::Unpublished),
        }
    }

    /// Install before awaiting worker results. A failed spawn has no thread to
    /// join, but still publishes that fact to a concurrently retiring record.
    #[lore_macro::test_pub]
    pub(super) fn publish(&self, thread: Option<JoinHandle<()>>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        debug_assert!(matches!(*state, State::Unpublished));
        *state = match thread {
            Some(thread) => State::Running(thread),
            None => State::Joined(Ok(())),
        };
    }

    #[lore_macro::test_pub]
    pub(super) async fn drain(&self) -> Result<()> {
        loop {
            let thread = {
                let mut state = self.state.lock().map_err(|_poison| Error::Poisoned)?;
                match &*state {
                    State::Joined(result) => return result.clone(),
                    State::Running(thread) if thread.is_finished() => {
                        let State::Running(thread) = std::mem::replace(&mut *state, State::Joining)
                        else {
                            unreachable!("finished worker has a thread handle");
                        };
                        Some(thread)
                    }
                    _ => None,
                }
            };
            if let Some(thread) = thread {
                // The thread has already exited. No worker is occupied waiting
                // for user work, and cancellation cannot occur between taking
                // the handle, joining, and publishing the shared result.
                let result = thread
                    .join()
                    .map_err(|_panic| Error::Owner("working owner thread panicked".into()));
                *self
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()) = State::Joined(result.clone());
                return result;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
