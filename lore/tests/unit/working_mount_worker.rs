// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(unix)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use lore::working_mount::Error;
use lore::working_mount::worker::ThreadOwner;

const DEADLINE: Duration = Duration::from_secs(5);
const STILL_RUNNING: Duration = Duration::from_millis(20);

struct FinalCapture {
    dropped: Arc<AtomicBool>,
    release: mpsc::Receiver<()>,
}
impl Drop for FinalCapture {
    fn drop(&mut self) {
        self.release.recv_timeout(DEADLINE).unwrap();
        self.dropped.store(true, Ordering::Release);
    }
}

fn paused_worker() -> (Arc<ThreadOwner>, mpsc::Sender<()>, Arc<AtomicBool>) {
    let owner = Arc::new(ThreadOwner::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let (logical_tx, logical_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let capture = FinalCapture {
        dropped: dropped.clone(),
        release: release_rx,
    };
    let thread = std::thread::spawn(move || {
        let _capture = capture;
        logical_tx.send(()).unwrap();
    });
    owner.publish(Some(thread));
    logical_rx.recv_timeout(DEADLINE).unwrap();
    (owner, release_tx, dropped)
}

#[tokio::test]
async fn logical_completion_does_not_release_actual_thread_or_its_captures() {
    let (owner, release, dropped) = paused_worker();
    assert!(
        tokio::time::timeout(STILL_RUNNING, owner.drain())
            .await
            .is_err()
    );
    assert!(!dropped.load(Ordering::Acquire));
    release.send(()).unwrap();
    tokio::time::timeout(DEADLINE, owner.drain())
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::Acquire));
}

#[tokio::test]
async fn cancelling_one_waiter_preserves_join_for_concurrent_drain_and_retry() {
    let (owner, release, dropped) = paused_worker();
    let mut cancelled = Box::pin(owner.drain());
    assert!(
        tokio::time::timeout(STILL_RUNNING, &mut cancelled)
            .await
            .is_err()
    );
    drop(cancelled);
    release.send(()).unwrap();
    let (first, second) = tokio::time::timeout(DEADLINE, async {
        tokio::join!(owner.drain(), owner.drain())
    })
    .await
    .unwrap();
    first.unwrap();
    second.unwrap();
    assert!(dropped.load(Ordering::Acquire));
    owner.drain().await.unwrap();
}

#[tokio::test]
async fn finished_thread_must_be_published_before_drain_can_complete() {
    let owner = ThreadOwner::new();
    let thread = std::thread::spawn(|| ());
    assert!(
        tokio::time::timeout(STILL_RUNNING, owner.drain())
            .await
            .is_err()
    );
    owner.publish(Some(thread));
    tokio::time::timeout(DEADLINE, owner.drain())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn worker_panic_is_a_retained_failure_for_every_waiter() {
    let owner = ThreadOwner::new();
    owner.publish(Some(std::thread::spawn(|| panic!("worker failed"))));
    for _ in 0..2 {
        let result = tokio::time::timeout(DEADLINE, owner.drain()).await.unwrap();
        assert!(matches!(result, Err(Error::Owner(_))));
    }
}

#[tokio::test]
async fn failed_spawn_publishes_that_no_thread_needs_joining() {
    let owner = ThreadOwner::new();
    owner.publish(None);
    owner.drain().await.unwrap();
}

struct TlsCapture {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    dropped: Arc<AtomicBool>,
}
impl Drop for TlsCapture {
    fn drop(&mut self) {
        let _entered = self.entered.send(());
        // A broken runtime join must fail the test rather than hang its process.
        let _released = self.release.recv_timeout(DEADLINE);
        self.dropped.store(true, Ordering::Release);
    }
}
thread_local! {
    static TLS_CAPTURE: std::cell::RefCell<Option<TlsCapture>> = const { std::cell::RefCell::new(None) };
}
struct ReleaseOnDrop(mpsc::Sender<()>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let _released = self.0.send(());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_tls_destruction_keeps_timeout_live_and_join_result_after_cancellation() {
    for panic in [false, true] {
        let owner = ThreadOwner::new();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release = ReleaseOnDrop(release_tx);
        let dropped = Arc::new(AtomicBool::new(false));
        let capture = TlsCapture {
            entered: entered_tx,
            release: release_rx,
            dropped: dropped.clone(),
        };
        let thread = std::thread::spawn(move || {
            TLS_CAPTURE.with(|slot| *slot.borrow_mut() = Some(capture));
            assert!(!panic, "worker failed before TLS destruction");
        });
        entered_rx.recv_timeout(DEADLINE).unwrap();
        assert!(thread.is_finished(), "the worker body has completed");
        owner.publish(Some(thread));

        let mut cancelled = Box::pin(owner.drain());
        assert!(
            tokio::time::timeout(STILL_RUNNING, &mut cancelled)
                .await
                .is_err(),
            "a blocked native join must not prevent an async timeout"
        );
        drop(cancelled);
        assert!(!dropped.load(Ordering::Acquire));
        drop(release);

        let (first, second) = tokio::time::timeout(DEADLINE, async {
            tokio::join!(owner.drain(), owner.drain())
        })
        .await
        .unwrap();
        assert_eq!(first.is_err(), panic);
        assert_eq!(second.is_err(), panic);
        assert_eq!(owner.drain().await.is_err(), panic);
        assert!(dropped.load(Ordering::Acquire));
    }
}
