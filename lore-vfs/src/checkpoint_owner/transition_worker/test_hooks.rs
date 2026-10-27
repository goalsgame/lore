// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Inactive-by-default phase controls for real owned transition integration tests.
#![cfg(feature = "test-util")]
#![allow(clippy::disallowed_methods)]

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionTestStep {
    AfterIntent,
    AfterAnchors,
    BeforeThreadExit,
    PublisherDestructor,
}
pub struct TransitionTestPause {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl TransitionTestPause {
    pub async fn entered(&self) {
        self.entered.notified().await;
    }
    pub fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn enter(&self) {
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
}
pub(in crate::checkpoint_owner) struct Hook {
    step: TransitionTestStep,
    pause: Arc<TransitionTestPause>,
    fail: bool,
}
impl CheckpointOwner {
    pub fn test_transition_active(&self) -> usize {
        *self.publications.active.lock().unwrap()
    }

    pub fn test_transition_pause(
        &self,
        step: TransitionTestStep,
        fail: bool,
    ) -> Arc<TransitionTestPause> {
        let pause = Arc::new(TransitionTestPause {
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            changed: std::sync::Condvar::new(),
        });
        *self.transition_hook.lock().unwrap() = Some(Arc::new(Hook {
            step,
            pause: pause.clone(),
            fail,
        }));
        pause
    }
    pub(in crate::checkpoint_owner) fn hit_transition_test(
        &self,
        step: TransitionTestStep,
    ) -> Outcome {
        let hook = self
            .transition_hook
            .lock()
            .map_err(|_poison| FsError::internal("Transition test hook poisoned"))?
            .clone();
        if let Some(hook) = hook.filter(|hook| hook.step == step) {
            hook.pause.enter();
            if hook.fail {
                return Err(FsError::internal("Injected owned transition failure"));
            }
        }
        Ok(())
    }
}

pub(super) struct PublisherTail(Arc<CheckpointOwner>);
impl PublisherTail {
    pub(super) fn new(owner: Arc<CheckpointOwner>) -> Self {
        Self(owner)
    }
}
impl Drop for PublisherTail {
    fn drop(&mut self) {
        assert!(
            self.0
                .hit_transition_test(TransitionTestStep::PublisherDestructor)
                .is_ok(),
            "injected publisher destructor panic"
        );
    }
}
