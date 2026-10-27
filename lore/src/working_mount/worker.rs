// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use super::Error;
use super::Result;

enum State {
    Unpublished,
    Running(JoinHandle<()>),
    Joining(Option<JoinHandle<()>>),
    Joined(Result<()>),
}

/// Retains the actual worker until its thread has exited, independently of any
/// caller waiting for logical completion or cancellation of that wait.
#[allow(private_interfaces)]
#[lore_macro::test_pub]
pub(crate) struct ThreadOwner {
    state: Arc<Mutex<State>>,
}

impl Default for ThreadOwner {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadOwner {
    #[lore_macro::test_pub]
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::Unpublished)),
        }
    }

    /// Install before awaiting worker results. A failed spawn has no thread to
    /// join, but still publishes that fact to a concurrently retiring record.
    #[lore_macro::test_pub]
    pub(crate) fn publish(&self, thread: Option<JoinHandle<()>>) {
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

    /// Starts a retained native joiner once and observes its recorded result.
    /// A failed joiner spawn leaves the worker available for a later retry.
    pub(crate) fn try_drain(&self) -> Option<Result<()>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Some(Err(Error::Poisoned)),
        };
        match &*state {
            State::Joined(result) => return Some(result.clone()),
            State::Running(_) => {}
            _ => return None,
        }
        let State::Running(thread) = std::mem::replace(&mut *state, State::Joining(None)) else {
            unreachable!("running worker has a thread handle");
        };
        *state = State::Joining(Some(thread));
        let shared = self.state.clone();
        // is_finished can precede TLS destruction. Join on a dedicated thread
        // so even a blocked destructor never occupies an async runtime or its
        // blocking pool. The joiner retains state after every waiter is gone.
        let joiner = std::thread::Builder::new()
            .name("lore-worker-join".into())
            .spawn(move || {
                let thread = {
                    let mut state = shared.lock().unwrap_or_else(|poison| poison.into_inner());
                    let State::Joining(thread) = &mut *state else {
                        unreachable!("joiner owns the worker handle");
                    };
                    thread.take().expect("one joiner owns the worker")
                };
                let result = thread
                    .join()
                    .map_err(|_panic| Error::Owner("owner thread panicked".into()));
                *shared.lock().unwrap_or_else(|poison| poison.into_inner()) = State::Joined(result);
            });
        if let Err(error) = joiner {
            let State::Joining(thread) = &mut *state else {
                unreachable!("failed joiner leaves its worker handle");
            };
            *state = State::Running(thread.take().expect("unstarted joiner"));
            return Some(Err(Error::Owner(format!(
                "starting worker joiner: {error}"
            ))));
        }
        None
    }

    #[lore_macro::test_pub]
    pub(crate) async fn drain(&self) -> Result<()> {
        loop {
            if let Some(result) = self.try_drain() {
                return result;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
