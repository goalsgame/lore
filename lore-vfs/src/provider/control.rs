// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Admission and cancellation for provider requests. Phase tickets are distinct
//! from actual export and checkpoint IO, which have their own drain owners.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use lore_revision::fs::filesystem_provider::FsError;
use tokio::sync::watch;

const MAX_REQUESTS: usize = 64;
type Abort = Arc<dyn Fn() + Send + Sync>;
struct State {
    closed: bool,
    cancelled: bool,
    failed: bool,
    next: u64,
    phases: BTreeMap<u64, Option<Abort>>,
    requests: usize,
}
pub(super) struct Control {
    state: Mutex<State>,
    closed: watch::Sender<bool>,
    cancelled: watch::Sender<bool>,
    changed: tokio::sync::Notify,
}
fn unavailable() -> FsError {
    FsError::internal("working provider admission is closed")
}
fn poisoned() -> FsError {
    FsError::internal("working provider admission is poisoned")
}
impl Control {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                closed: false,
                cancelled: false,
                failed: false,
                next: 1,
                phases: BTreeMap::new(),
                requests: 0,
            }),
            closed: watch::channel(false).0,
            cancelled: watch::channel(false).0,
            changed: tokio::sync::Notify::new(),
        })
    }
    pub(super) fn prepare(self: &Arc<Self>) -> Result<Ticket, FsError> {
        let mut state = self.state.lock().map_err(|_poison| poisoned())?;
        if state.failed {
            return Err(poisoned());
        }
        if state.closed {
            return Err(unavailable());
        }
        if state.phases.len() >= MAX_REQUESTS {
            return Err(FsError::internal("working provider operation limit"));
        }
        let id = state.next;
        state.next = id
            .checked_add(1)
            .ok_or_else(|| FsError::internal("working provider operation identity exhausted"))?;
        state.phases.insert(id, None);
        Ok(Ticket {
            control: self.clone(),
            id,
        })
    }
    pub(super) fn preparing<'a, T: Send + 'a>(
        &'a self,
        future: impl Future<Output = Result<T, FsError>> + Send + 'a,
    ) -> impl Future<Output = Result<T, FsError>> + Send + 'a {
        let future = Box::pin(future);
        async move {
            let mut closed = self.closed.subscribe();
            if *closed.borrow() {
                return Err(unavailable());
            }
            tokio::select! { biased; _ = closed.changed() => Err(unavailable()), result = future => result }
        }
    }
    pub(super) fn admit(&self, ticket: &Ticket, abort: Abort) -> Result<(), FsError> {
        let mut state = self.state.lock().map_err(|_poison| poisoned())?;
        if state.failed {
            return Err(poisoned());
        }
        if state.closed {
            return Err(unavailable());
        }
        let slot = state.phases.get_mut(&ticket.id).ok_or_else(unavailable)?;
        *slot = Some(abort);
        Ok(())
    }
    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        self.closed.send_replace(true);
        self.changed.notify_waiters();
    }
    pub(super) fn cancel(&self) {
        let phases = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            state.cancelled = true;
            self.closed.send_replace(true);
            self.cancelled.send_replace(true);
            state
                .phases
                .values()
                .filter_map(Clone::clone)
                .collect::<Vec<_>>()
        };
        for abort in phases {
            abort();
        }
        self.changed.notify_waiters();
    }
    pub(super) fn fail(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.failed = true;
        state.closed = true;
        state.cancelled = true;
        self.closed.send_replace(true);
        self.cancelled.send_replace(true);
        self.changed.notify_waiters();
    }
    pub(super) fn request<'a, T: Send + 'a>(
        self: &'a Arc<Self>,
        future: impl Future<Output = Result<T, FsError>> + Send + 'a,
    ) -> impl Future<Output = Result<T, FsError>> + Send + 'a {
        let future = Box::pin(future);
        async move {
            let mut cancelled = self.cancelled.subscribe();
            let guard = {
                let mut state = self.state.lock().map_err(|_poison| poisoned())?;
                if state.failed {
                    return Err(poisoned());
                }
                if state.cancelled {
                    return Err(unavailable());
                }
                if state.requests >= MAX_REQUESTS {
                    return Err(FsError::internal("working provider request limit"));
                }
                state.requests += 1;
                Request(self.clone())
            };
            let result = tokio::select! { biased; _ = cancelled.changed() => Err(unavailable()), result = future => result };
            drop(guard);
            result
        }
    }
    pub(super) async fn drain(&self) -> Result<(), FsError> {
        self.close();
        loop {
            let changed = self.changed.notified();
            let (done, failed) = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                (state.phases.is_empty() && state.requests == 0, state.failed)
            };
            if done {
                return if failed || self.state.is_poisoned() {
                    Err(poisoned())
                } else {
                    Ok(())
                };
            }
            changed.await;
        }
    }
}
pub(super) struct Ticket {
    control: Arc<Control>,
    id: u64,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.control
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .phases
            .remove(&self.id);
        self.control.changed.notify_waiters();
    }
}
struct Request(Arc<Control>);
impl Drop for Request {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .requests -= 1;
        self.0.changed.notify_waiters();
    }
}
