// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Owned service retirement must precede terminal global library teardown.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

mod test_util;

use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

const CHILD: &str = "LORE_SHUTDOWN_PREFLIGHT_CHILD";
const DEADLINE: Duration = Duration::from_secs(45);

#[test]
fn owned_service_timeout_retains_globals_and_retry_completes_in_a_child() {
    let sandbox = test_util::TempDir::new("shutdown-preflight");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "owned_service_child", "--ignored", "--nocapture"])
        .env(CHILD, "1")
        .env("LORE_USE_SERVICE", "0")
        .env(lore_base::env::GLOBAL_PATH_VAR, sandbox.path())
        .env(lore_base::env::AUTH_PATH_VAR, sandbox.path())
        .env(
            "LORE_SERVICE_SOCKET",
            format!("lore-shutdown-preflight-{}", std::process::id()),
        )
        .spawn()
        .unwrap();
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "shutdown child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("shutdown child exceeded {DEADLINE:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
#[ignore = "terminal shutdown runs only in the explicitly isolated child"]
fn owned_service_child() {
    assert_eq!(std::env::var(CHILD).as_deref(), Ok("1"));
    assert!(!lore_base::runtime::runtime_shutdown_started());
    let unpublished_start = lore::remote::service_process::begin_service_start().unwrap();
    assert_eq!(
        lore::try_shutdown(),
        Err(lore::ShutdownError::ServiceRetained(
            "an admitted service start has no tracked lifecycle actor".into()
        ))
    );
    assert!(!lore_base::runtime::runtime_shutdown_started());
    drop(unpublished_start);
    let (finished_tx, finished_rx) = mpsc::channel();
    let service = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(lore::service::run(
            Default::default(),
            lore::service::LoreServiceRunArgs {},
            None,
        ));
        finished_tx.send(result).unwrap();
    });
    let ready_deadline = Instant::now() + Duration::from_secs(10);
    let owner = loop {
        if lore::remote::service_process::service_runs_in_this_process() {
            break lore::remote::service_process::current_service_owner().unwrap();
        }
        if let Ok(status) = finished_rx.try_recv() {
            panic!("service exited before becoming ready: {status}");
        }
        assert!(
            Instant::now() < ready_deadline,
            "service did not become ready"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let admitted_work = owner.state.try_begin_invocation().unwrap();
    assert_eq!(
        lore::try_shutdown(),
        Err(lore::ShutdownError::ServiceTimedOut)
    );
    assert!(!lore_base::runtime::runtime_shutdown_started());
    assert!(matches!(
        finished_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    // The failed attempt must leave runtime admission and allocator-backed work
    // usable, rather than merely postponing a terminal marker.
    let value = lore::runtime().block_on(async {
        tokio::task::yield_now().await;
        lore::interface::LoreString::from("retained library resources")
    });
    assert_eq!(value.as_str(), "retained library resources");
    drop(value);
    drop(admitted_work);
    drop(owner);

    assert_eq!(lore::try_shutdown(), Ok(()));
    assert!(lore_base::runtime::runtime_shutdown_started());
    assert_eq!(finished_rx.recv_timeout(Duration::from_secs(5)).unwrap(), 0);
    service.join().unwrap();
    assert_eq!(
        lore::try_shutdown(),
        Err(lore::ShutdownError::AlreadyShutDown)
    );
    assert_ne!(lore::interface::lore_shutdown(), 0);
}
