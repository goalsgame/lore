// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Stream failure is separate from graceful session stop. The reader can close
//! and extract buffered work even if tonic never polls its request body again.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::task::Poll;

use futures::task::AtomicWaker;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::request_ownership::Item;
use super::request_ownership::SessionLife;
use crate::ProtocolError;

struct State<K, S> {
    terminal: Option<ProtocolError>,
    receiver: mpsc::Receiver<Item<K, S>>,
}
pub(super) struct StreamLife<K, S> {
    state: Mutex<State<K, S>>,
    wake: AtomicWaker,
    ended: CancellationToken,
}
pub(super) struct StreamAdmission<'a, K, S> {
    // Enqueue and wire-map registration finish while this short guard is held.
    // It never spans an await or a reply/payload/resource destruction.
    _guard: MutexGuard<'a, State<K, S>>,
}
impl<K, S> StreamLife<K, S> {
    pub(super) fn new(receiver: mpsc::Receiver<Item<K, S>>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                terminal: None,
                receiver,
            }),
            wake: AtomicWaker::new(),
            ended: CancellationToken::new(),
        })
    }
    pub(super) fn admit(&self) -> Result<StreamAdmission<'_, K, S>, ProtocolError> {
        let state = self
            .state
            .lock()
            .map_err(|_poison| ProtocolError::internal("gRPC stream poisoned"))?;
        if let Some(error) = state.terminal.as_ref() {
            return Err(error.clone());
        }
        Ok(StreamAdmission { _guard: state })
    }
    pub(super) fn terminal_error(&self) -> Option<ProtocolError> {
        self.state.lock().ok()?.terminal.clone()
    }
    fn close_input(&self) {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.receiver.close();
        }
        self.wake.wake();
    }
    pub(super) async fn next(&self, session: &SessionLife) -> Option<Item<K, S>> {
        let mut closing = false;
        loop {
            let next = std::future::poll_fn(|cx| {
                self.wake.register(cx.waker());
                let Ok(mut state) = self.state.lock() else {
                    return Poll::Ready(None);
                };
                if state.terminal.is_some() {
                    return Poll::Ready(None);
                }
                state.receiver.poll_recv(cx)
            });
            tokio::select! {
                _ = session.wait_closed(), if !closing => {
                    self.close_input();
                    closing = true;
                },
                item = next => return item,
            }
        }
    }
    /// Mark failure before draining queue/maps. `error` remains owned outside
    /// the guard even when an earlier terminal reason already won the race.
    pub(super) async fn wait_terminal(&self) {
        self.ended.cancelled().await;
    }
    pub(super) fn terminate(&self, error: ProtocolError) -> ProtocolError {
        let result = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.terminal.is_none() {
                state.terminal = Some(error.clone());
            }
            state.receiver.close();
            state.terminal.as_ref().unwrap().clone()
        };
        self.ended.cancel();
        self.wake.wake();
        result
    }
    /// The returned payload/reply is disposed by the reader after guard release.
    /// A terminal generation admits no late enqueue or wire-map registration.
    pub(super) fn take_failed(&self) -> Option<Item<K, S>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.terminal.as_ref()?;
        let item = state.receiver.try_recv().ok();
        drop(state);
        item
    }
}

#[cfg(test)]
mod tests;
