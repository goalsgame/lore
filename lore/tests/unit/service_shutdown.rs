// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(unix)]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use lore::ShutdownError;
use lore::service::shutdown::Coordinator;
use lore::service::shutdown::wait_for_service;

const DEADLINE: Duration = Duration::from_secs(5);

#[test]
fn no_service_tears_down_once_and_permanently_closes_start_admission() {
    let coordinator = Coordinator::new();
    let steps = Mutex::new(Vec::new());
    assert_eq!(
        coordinator.try_shutdown(
            || {
                steps.lock().unwrap().push("service retired");
                Ok(())
            },
            || {
                assert_eq!(
                    coordinator.with_start_admission(|| ()),
                    Err(ShutdownError::AlreadyShutDown)
                );
                steps.lock().unwrap().push("global teardown");
                Ok(())
            },
        ),
        Ok(())
    );
    assert_eq!(
        *steps.lock().unwrap(),
        ["service retired", "global teardown"]
    );
    assert_eq!(
        coordinator.try_shutdown(
            || panic!("preflight repeated"),
            || panic!("teardown repeated")
        ),
        Err(ShutdownError::AlreadyShutDown)
    );
}

#[test]
fn retained_service_skips_destructive_work_and_allows_retry() {
    let coordinator = Coordinator::new();
    let retained = ShutdownError::ServiceRetained("busy mount".into());
    assert_eq!(
        coordinator.try_shutdown(
            || wait_for_service(async { Err("busy mount".into()) }, DEADLINE),
            || panic!("failed preflight must retain global resources"),
        ),
        Err(retained)
    );
    assert_eq!(coordinator.with_start_admission(|| 7), Ok(7));
    assert_eq!(coordinator.try_shutdown(|| Ok(()), || Ok(())), Ok(()));
}

#[test]
fn service_timeout_skips_destructive_work_and_allows_retry() {
    let coordinator = Coordinator::new();
    assert_eq!(
        coordinator.try_shutdown(
            || wait_for_service(std::future::pending(), Duration::from_millis(20)),
            || panic!("timed out preflight must retain global resources"),
        ),
        Err(ShutdownError::ServiceTimedOut)
    );
    assert_eq!(coordinator.with_start_admission(|| ()), Ok(()));
    assert_eq!(coordinator.try_shutdown(|| Ok(()), || Ok(())), Ok(()));
}

#[test]
fn concurrent_shutdown_returns_in_progress_without_waiting_for_actor() {
    let coordinator = Arc::new(Coordinator::new());
    let worker_coordinator = coordinator.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        worker_coordinator.try_shutdown(
            || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(DEADLINE).unwrap();
                Err(ShutdownError::ServiceRetained("retry".into()))
            },
            || panic!("retained actor must prevent teardown"),
        )
    });
    entered_rx.recv_timeout(DEADLINE).unwrap();
    assert_eq!(
        coordinator.try_shutdown(|| panic!("second preflight"), || panic!("second teardown")),
        Err(ShutdownError::InProgress)
    );
    // This also proves the actor wait does not keep the gate mutex locked.
    assert_eq!(
        coordinator.with_start_admission(|| panic!("start admitted during preflight")),
        Err::<(), _>(ShutdownError::InProgress)
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        worker.join().unwrap(),
        Err(ShutdownError::ServiceRetained(_))
    ));
    assert_eq!(coordinator.try_shutdown(|| Ok(()), || Ok(())), Ok(()));
}

#[test]
fn admitted_start_is_published_before_shutdown_can_observe_the_run() {
    let coordinator = Arc::new(Coordinator::new());
    let published = Arc::new(AtomicBool::new(false));
    let retired = Arc::new(AtomicBool::new(false));
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (publish_tx, publish_rx) = mpsc::channel();
    let start_coordinator = coordinator.clone();
    let start_published = published.clone();
    let starter = std::thread::spawn(move || {
        start_coordinator.with_start_admission(|| {
            admitted_tx.send(()).unwrap();
            publish_rx.recv_timeout(DEADLINE).unwrap();
            start_published.store(true, Ordering::SeqCst);
        })
    });
    admitted_rx.recv_timeout(DEADLINE).unwrap();
    // A startup paused before publication cannot be mistaken for no service.
    assert_eq!(
        coordinator.try_shutdown(
            || panic!("preflight cannot inspect an unpublished admitted start"),
            || panic!("an admitted start cannot appear after teardown"),
        ),
        Err(ShutdownError::InProgress)
    );
    publish_tx.send(()).unwrap();
    assert_eq!(starter.join().unwrap(), Ok(()));
    assert_eq!(
        coordinator.try_shutdown(
            || {
                assert!(
                    published.load(Ordering::SeqCst),
                    "an admitted run must be visible"
                );
                retired.store(true, Ordering::SeqCst);
                Ok(())
            },
            || {
                assert!(retired.load(Ordering::SeqCst));
                assert_eq!(
                    coordinator.with_start_admission(|| ()),
                    Err(ShutdownError::AlreadyShutDown)
                );
                Ok(())
            },
        ),
        Ok(())
    );
}

#[test]
fn panicking_preflight_rolls_back_only_its_preparation() {
    let coordinator = Coordinator::new();
    assert!(
        std::panic::catch_unwind(|| {
            coordinator.try_shutdown(|| panic!("preflight aborted"), || Ok(()))
        })
        .is_err()
    );
    assert_eq!(coordinator.with_start_admission(|| ()), Ok(()));
    assert_eq!(coordinator.try_shutdown(|| Ok(()), || Ok(())), Ok(()));
}

#[test]
fn panicking_start_publication_fails_closed_before_retry_inspects_retained_run() {
    let coordinator = Coordinator::new();
    let published = AtomicBool::new(false);
    assert!(
        std::panic::catch_unwind(|| {
            coordinator.with_start_admission(|| {
                published.store(true, Ordering::SeqCst);
                panic!("start publication interrupted");
            })
        })
        .is_err()
    );
    assert!(published.load(Ordering::SeqCst));
    assert_eq!(
        coordinator.with_start_admission(|| panic!("publication fence cannot reopen")),
        Err::<(), _>(ShutdownError::AdmissionUnavailable(
            "service start publication panicked"
        ))
    );
    assert_eq!(
        coordinator.try_shutdown(
            || panic!("interrupted publication cannot enter preflight on the poisoned attempt"),
            || panic!("interrupted publication cannot destroy resources"),
        ),
        Err(ShutdownError::AdmissionUnavailable(
            "service start publication panicked"
        ))
    );
    assert_eq!(
        coordinator.try_shutdown(
            || {
                assert!(published.load(Ordering::SeqCst));
                Ok(())
            },
            || Ok(()),
        ),
        Ok(())
    );
}
