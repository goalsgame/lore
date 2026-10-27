// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Ordered destruction of an admitted publisher's captures. Completion covers
//! these resources; it does not claim that the OS thread has been joined.
use tokio::sync::OwnedMutexGuard;

use super::*;
use crate::namespace::NamespaceSnapshot;

struct PoisonOnPanic(Arc<CheckpointOwner>);
impl Drop for PoisonOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.poisoned.store(true, Ordering::Release);
        }
    }
}
#[cfg(all(test, unix))]
pub(super) struct Tail {
    pub(super) pause: Option<Arc<PublishPause>>,
    pub(super) panic: bool,
}
#[cfg(all(test, unix))]
impl Drop for Tail {
    fn drop(&mut self) {
        if let Some(pause) = &self.pause {
            pause.enter();
        }
        assert!(!self.panic, "injected publisher capture destruction panic");
    }
}

// Fields drop in declaration order, including when publication or capture
// destruction unwinds. The counter is last and never races ahead of captures.
pub(super) struct PublicationTask {
    snapshot: NamespaceSnapshot,
    #[cfg(all(test, unix))]
    tail: Tail,
    #[cfg(all(test, unix))]
    before_io: Option<Arc<PublishPause>>,
    _poison: PoisonOnPanic,
    _lease: Option<FrozenNamespace>,
    _serial: OwnedMutexGuard<()>,
    owner: Arc<CheckpointOwner>,
    sender: Option<tokio::sync::oneshot::Sender<Result<()>>>,
    _completion: Publishing,
}
impl PublicationTask {
    pub(super) fn new(
        snapshot: NamespaceSnapshot,
        lease: Option<FrozenNamespace>,
        serial: OwnedMutexGuard<()>,
        owner: Arc<CheckpointOwner>,
        completion: Publishing,
        sender: tokio::sync::oneshot::Sender<Result<()>>,
    ) -> Self {
        Self {
            snapshot,
            #[cfg(all(test, unix))]
            tail: Tail {
                pause: None,
                panic: false,
            },
            #[cfg(all(test, unix))]
            before_io: None,
            sender: Some(sender),
            _poison: PoisonOnPanic(owner.clone()),
            _lease: lease,
            _serial: serial,
            owner,
            _completion: completion,
        }
    }
    #[cfg(all(test, unix))]
    pub(super) fn observe(mut self, before_io: Option<Arc<PublishPause>>, tail: Tail) -> Self {
        self.before_io = before_io;
        self.tail = tail;
        self
    }
    pub(super) fn run(mut self) {
        #[cfg(all(test, unix))]
        if let Some(pause) = &self.before_io {
            pause.enter();
        }
        let result = match self.owner.recovery.lock() {
            Ok(mut recovery) => {
                let result = recovery.publish(&self.snapshot);
                if let Err(error) = &result
                    && publication_error_requires_reopen(&recovery, error)
                {
                    self.owner.poisoned.store(true, Ordering::Release);
                }
                result.map_err(Error::Recovery)
            }
            Err(_poison) => {
                self.owner.poisoned.store(true, Ordering::Release);
                Err(Error::Poisoned)
            }
        };
        let sender = self.sender.take();
        drop(self);
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }
}
