// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Optional ownership retained by submitted IO through actual completion. A scope
//! covers polls, the inner future's destructor, and synchronous calls inside it;
//! executors must propagate it when spawning work. Dropping a scope does not
//! cancel or drain submitted operations.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use pin_project::pin_project;
use pin_project::pinned_drop;

thread_local! {
    static CURRENT: RefCell<Option<IoLease>> = const { RefCell::new(None) };
}

/// Opaque ownership; it grants no authorization and contains no credentials.
#[derive(Clone)]
pub struct IoLease(Arc<dyn Send + Sync>);
impl IoLease {
    pub fn new<T: Send + Sync + 'static>(owner: Arc<T>) -> Self {
        Self(owner)
    }
    pub fn current() -> Option<Self> {
        CURRENT
            .try_with(|current| current.borrow().clone())
            .ok()
            .flatten()
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn scope<F: Future>(&self, future: F) -> IoScope<F> {
        scope(Some(self.clone()), future)
    }
    pub fn sync_scope<T>(&self, function: impl FnOnce() -> T) -> T {
        sync_scope(Some(self.clone()), function)
    }
}

struct Reset {
    previous: Option<IoLease>,
    installed: bool,
}
fn enter(lease: Option<IoLease>) -> Reset {
    match CURRENT.try_with(|current| current.replace(lease)) {
        Ok(previous) => Reset {
            previous,
            installed: true,
        },
        Err(_destroyed) => Reset {
            previous: None,
            installed: false,
        },
    }
}
impl Drop for Reset {
    fn drop(&mut self) {
        if !self.installed {
            return;
        }
        let removed = CURRENT.try_with(|current| current.replace(self.previous.take()));
        drop(removed);
    }
}

/// Apply captured ownership, including an explicitly unscoped `None`, and restore
/// the previous scope on return or unwind. Never holds a TLS borrow during work.
pub fn sync_scope<T>(lease: Option<IoLease>, function: impl FnOnce() -> T) -> T {
    let reset = enter(lease);
    let result = function();
    drop(reset);
    result
}

#[pin_project(PinnedDrop)]
pub struct IoScope<F> {
    #[pin]
    future: F,
    // Fields drop in declaration order: retain ownership through inner cleanup.
    lease: Option<IoLease>,
    // Installed by pinned_drop, and restored after the inner destructor runs.
    reset: Option<Reset>,
}
/// Zero-allocation poll scope, independent of any async runtime. `None` clears an
/// unrelated executor caller's scope while this future is being polled.
pub fn scope<F: Future>(lease: Option<IoLease>, future: F) -> IoScope<F> {
    IoScope {
        lease,
        future,
        reset: None,
    }
}
#[pinned_drop]
impl<F> PinnedDrop for IoScope<F> {
    fn drop(self: Pin<&mut Self>) {
        let projected = self.project();
        *projected.reset = Some(enter(projected.lease.clone()));
    }
}
impl<F: Future> Future for IoScope<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        sync_scope(projected.lease.clone(), || projected.future.poll(context))
    }
}
