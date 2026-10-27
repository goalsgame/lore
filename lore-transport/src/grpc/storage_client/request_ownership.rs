// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Ownership of accepted items, independent of a cached stream's lifetime.
//! Closing a generation rejects new admission and half-closes its request body
//! after accepted queued work drains. It is not a synchronous RPC drain receipt.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use lore_io::lease::IoLease;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::ProtocolError;

pub(super) struct SessionLife {
    closed: Mutex<bool>,
    stopped: CancellationToken,
}
pub(super) struct Admission<'a> {
    // Holding this short guard linearizes cache selection and final enqueue
    // against stop. It must never span await, user callbacks or resource drop.
    _guard: MutexGuard<'a, bool>,
}
impl SessionLife {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: Mutex::new(false),
            stopped: CancellationToken::new(),
        })
    }
    pub(super) fn admit(&self) -> Result<Admission<'_>, ProtocolError> {
        let guard = self.closed.lock().map_err(|_poison| stopped())?;
        if *guard {
            return Err(stopped());
        }
        Ok(Admission { _guard: guard })
    }
    pub(super) fn check_open(&self) -> Result<(), ProtocolError> {
        drop(self.admit()?);
        Ok(())
    }
    pub(super) async fn wait_closed(&self) {
        self.stopped.cancelled().await;
    }
    pub(super) fn close(&self) {
        {
            let mut closed = self
                .closed
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *closed = true;
        }
        // Wake request generators only after releasing admission.
        self.stopped.cancel();
    }
}
pub(super) fn stopped() -> ProtocolError {
    // A stopped generation is terminal; never classify it as Disconnected and
    // let a stream or channel reissue an operation for this closed session.
    ProtocolError::internal("gRPC session stopped")
}

/// One logical call, shared by all of its reissues/replies. Cancellation of a
/// waiter cannot release capacity or IO ownership for an accepted operation.
pub(in crate::grpc) struct Operation {
    // Capacity becomes reusable before inherited actual-work ownership drops.
    _permit: OwnedSemaphorePermit,
    _io: Option<IoLease>,
}
impl Operation {
    pub(super) async fn acquire(
        limiter: &Arc<Semaphore>,
        life: &SessionLife,
    ) -> Result<Arc<Self>, ProtocolError> {
        life.check_open()?;
        let io = IoLease::current();
        let permit = tokio::select! {
            _ = life.wait_closed() => return Err(stopped()),
            permit = limiter.clone().acquire_owned() => permit.map_err(|_closed| {
                ProtocolError::internal("gRPC request capacity closed")
            })?,
        };
        life.check_open()?;
        Ok(Arc::new(Self {
            _permit: permit,
            _io: io,
        }))
    }
}
pub(super) struct Reply<S> {
    sender: oneshot::Sender<Result<S, ProtocolError>>,
    // Dispose response/request capabilities before the last actual-work owner.
    operation: Arc<Operation>,
}
impl<S> Reply<S> {
    pub(super) fn new(
        sender: oneshot::Sender<Result<S, ProtocolError>>,
        operation: Arc<Operation>,
    ) -> Self {
        Self { sender, operation }
    }
    pub(super) fn send(self, value: Result<S, ProtocolError>) {
        let Self { sender, operation } = self;
        // A cancelled receiver returns the owned result; dispose it here before
        // releasing IO ownership rather than returning it to an unowned caller.
        let undelivered = sender.send(value);
        drop(undelivered);
        drop(operation);
    }
}
/// Tuple order disposes a queued payload before its reply's IO owner.
pub(super) type Item<K, S> = (K, Reply<S>);

/// A reader is prepared before admission/cache locking and started after both
/// guards are released. No callback, task spawn or old-handle destruction runs
/// under the session/cache lock pair.
pub(super) struct PreparedStream<K, S> {
    pub(super) handle: super::StreamHandle<K, S>,
    reader: futures::future::BoxFuture<'static, ()>,
}
impl<K, S> PreparedStream<K, S> {
    pub(super) fn new(
        handle: super::StreamHandle<K, S>,
        reader: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        use futures::FutureExt;
        Self {
            handle,
            reader: reader.boxed(),
        }
    }
    pub(super) fn start(self) {
        let Self { handle, reader } = self;
        drop(handle);
        lore_base::lore_spawn_net_nocontext!(reader);
    }
}

/// Runs terminal cleanup even if an unpolled prepared reader is discarded or a
/// running reader unwinds. Construction/start/destruction occur outside guards.
pub(super) struct ReaderCleanup(Option<Box<dyn FnOnce() + Send>>);
impl ReaderCleanup {
    pub(super) fn new(cleanup: impl FnOnce() + Send + 'static) -> Self {
        Self(Some(Box::new(cleanup)))
    }
}
impl Drop for ReaderCleanup {
    fn drop(&mut self) {
        if let Some(cleanup) = self.0.take() {
            cleanup();
        }
    }
}

#[cfg(test)]
mod tests;
