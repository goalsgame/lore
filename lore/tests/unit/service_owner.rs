// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use lore::remote::service_process::ServiceStartGuard;
use lore::remote::service_process::StopRequest;
use lore::remote::service_process::begin_service_start_for;
use lore::remote::service_process::owner_for;
use lore::remote::service_process::register_service_process;
use lore::service::owner::RunCleanup;
use lore::service::owner::ServiceOwner;
use lore::service::state::ServiceStateImpl;
use lore::service::state::capture_stop_request;
use lore::service::state::current_service_state;
use lore::service::state::resolve_owner;
use lore::service::state::with_invocation;
use lore::service::state::with_owner;

fn run(request: &Arc<StopRequest>) -> (Arc<ServiceOwner>, ServiceStartGuard, RunCleanup) {
    let state = Arc::new(ServiceStateImpl::new());
    state.initialize();
    let owner = ServiceOwner::for_state(state).unwrap();
    let guard = begin_service_start_for(request.clone()).unwrap();
    assert!(guard.bind_owner(&owner));
    register_service_process(&guard);
    let cleanup = RunCleanup::new(owner.clone());
    (owner, guard, cleanup)
}

#[tokio::test]
async fn owner_cleanup_closes_old_admission_despite_retained_owner_and_ticket() {
    let request = Arc::new(StopRequest::new());
    let (old, guard, cleanup) = run(&request);
    let ticket = old.state.try_begin_invocation().unwrap();
    assert!(old._supervisor.instances().unwrap().is_empty());
    drop(cleanup);
    assert!(old.state.try_begin_invocation().is_none());
    assert!(old.state.try_begin_control_invocation().is_none());
    assert!(ServiceOwner::for_state(old.state.clone()).is_err());
    drop(guard);
    let (new, guard, cleanup) = run(&request);
    assert!(!Arc::ptr_eq(&old.state, &new.state));
    assert!(!Arc::ptr_eq(
        old.state.provider_registry(),
        new.state.provider_registry()
    ));
    assert!(new.state.try_begin_invocation().is_some());
    drop(ticket);
    tokio::time::timeout(
        Duration::from_secs(5),
        old.state.close_and_drain_invocations(),
    )
    .await
    .unwrap();
    assert!(new.state.try_begin_invocation().is_some());
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn stale_admitted_command_after_await_cannot_stop_or_report_successor_run() {
    let request = Arc::new(StopRequest::new());
    let (old, guard, cleanup) = run(&request);
    let ticket = old.state.try_begin_control_invocation().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let command_owner = old.clone();
    let command_request = request.clone();
    let command = tokio::spawn(with_invocation(ticket, async move {
        with_owner(command_owner.clone(), async move {
            let captured = capture_stop_request().unwrap();
            let _entered = entered_tx.send(());
            release_rx.await.unwrap();
            assert!(Arc::ptr_eq(
                &resolve_owner(owner_for(&command_request)).unwrap(),
                &command_owner
            ));
            assert!(!captured.request_stop());
            assert!(!capture_stop_request().unwrap().request_stop());
            assert_eq!(
                lore::service::quit(
                    Default::default(),
                    lore::service::LoreServiceQuitArgs {},
                    None
                )
                .await,
                0
            );
            assert_eq!(
                lore::service::stop(Default::default(), Default::default(), None).await,
                0
            );
            let reports = Arc::new(Mutex::new(Vec::new()));
            let events = reports.clone();
            let callback = Some(Box::new(move |event: &lore::interface::LoreEvent| {
                if let lore::interface::LoreEvent::ServiceStatus(data) = event {
                    events.lock().unwrap().push(data.running);
                }
            })
                as Box<dyn Fn(&lore::interface::LoreEvent) + Send + Sync>);
            assert_eq!(
                lore::service::status(Default::default(), Default::default(), callback).await,
                0
            );
            assert_eq!(*reports.lock().unwrap(), vec![0]);
        })
        .await
        .unwrap();
    }));
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    drop(cleanup);
    drop(guard);
    let (new, guard, cleanup) = run(&request);
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), command)
        .await
        .unwrap()
        .unwrap();
    assert!(!request.is_requested());
    assert!(request.is_running());
    assert_eq!(old.state.invocation_count(), 0);
    assert_eq!(new.state.invocation_count(), 0);
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn owner_scope_rejects_wrong_ticket_and_does_not_leak_to_background_tasks() {
    let request = Arc::new(StopRequest::new());
    let (old, guard, cleanup) = run(&request);
    let another = ServiceOwner::for_state(Arc::new(ServiceStateImpl::new())).unwrap();
    with_invocation(old.state.try_begin_invocation().unwrap(), async {
        assert!(resolve_owner(Some(another.clone())).is_none());
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = polled.clone();
        assert!(
            with_owner(another.clone(), async move {
                probe.store(true, std::sync::atomic::Ordering::Release);
            })
            .await
            .is_err()
        );
        assert!(!polled.load(std::sync::atomic::Ordering::Acquire));
        with_owner(old.clone(), async {
            assert!(Arc::ptr_eq(
                &resolve_owner(Some(another.clone())).unwrap(),
                &old
            ));
            lore_base::lore_spawn!(async {
                assert!(current_service_state().is_none());
                assert!(resolve_owner(None).is_none());
                assert!(capture_stop_request().is_none());
            })
            .await
            .unwrap();
        })
        .await
        .unwrap();
    })
    .await;
    assert!(current_service_state().is_none());
    assert!(resolve_owner(None).is_none());
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn cancelled_owner_scope_releases_ticket_and_weak_run_binding_has_no_cycle() {
    use std::future::Future;
    use std::task::Poll;
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let weak = Arc::downgrade(&owner);
    let mut command = Box::pin(with_invocation(
        owner.state.try_begin_invocation().unwrap(),
        with_owner(owner.clone(), std::future::pending::<()>()),
    ));
    std::future::poll_fn(|context| {
        assert!(command.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(owner.state.invocation_count(), 1);
    drop(command);
    assert_eq!(owner.state.invocation_count(), 0);
    drop(cleanup);
    drop(owner);
    assert!(weak.upgrade().is_none());
    assert!(owner_for(&request).is_none());
    drop(guard);
}

#[test]
fn stale_start_guard_drop_and_registration_cannot_mutate_a_fresh_run() {
    let request = Arc::new(StopRequest::new());
    let (owner, old, cleanup) = run(&request);
    let old_stop = old.stop_request();
    drop(cleanup);
    request.end_service();
    let new = begin_service_start_for(request.clone()).unwrap();
    assert!(!old_stop.is_current());
    assert!(!old_stop.request_stop());
    register_service_process(&old);
    assert!(!request.is_running());
    drop(old);
    assert!(new.stop_request().is_current());
    register_service_process(&new);
    assert!(request.is_running());
    assert!(!request.is_requested());
    drop(owner);
    drop(new);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_old_connection_is_fenced_when_a_fresh_owner_starts() {
    use std::io::Read;

    use lore::remote::message;
    use lore::remote::network;
    let request = Arc::new(StopRequest::new());
    let (old, guard, cleanup) = run(&request);
    let name = format!("owner-stale-{}", std::process::id());
    let listener = network::UdsListener::new(&name).unwrap();
    let mut client =
        std::os::unix::net::UnixStream::connect(network::unix::uds_sock_path(&name)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let stream = listener.accept().unwrap();
    lore::remote::connection::serve_owned_connection(
        old.clone(),
        lore::remote::connection::ConnectionId(0),
        stream,
    );
    assert_eq!(old.state.connection_count(), 1);
    drop(cleanup);
    drop(guard);
    let (new, guard, cleanup) = run(&request);
    let command = message::MessageToServer {
        globals: Default::default(),
        command: lore::remote::command::LoreCommand::ServiceStop(Default::default()),
        environment: Default::default(),
    };
    message::write_payload(&mut client, &message::encode_message(&command)).unwrap();
    let read = tokio::task::spawn_blocking(move || {
        let mut byte = [0];
        client.read(&mut byte)
    });
    match tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .unwrap()
        .unwrap()
    {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        result => panic!("old connection unexpectedly replied or stayed open: {result:?}"),
    }
    assert!(!request.is_requested());
    assert!(new.state.try_begin_invocation().is_some());
    tokio::time::timeout(Duration::from_secs(5), async {
        while old.state.connection_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(listener);
    let mut lock = network::unix::uds_sock_path(&name).into_os_string();
    lock.push(".lock");
    let _removed = std::fs::remove_file(lock);
    drop(cleanup);
    drop(guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_stop_after_control_admission_closes_acknowledges_only_its_retired_run() {
    use std::io::Read;

    use lore::remote::message;
    use lore::remote::network;

    for current in [true, false] {
        let request = Arc::new(StopRequest::new());
        let (owner, guard, cleanup) = run(&request);
        let mut guard = Some(guard);
        let name = format!("owner-retired-stop-{}-{current}", std::process::id());
        let listener = network::UdsListener::new(&name).unwrap();
        let mut client =
            std::os::unix::net::UnixStream::connect(network::unix::uds_sock_path(&name)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        lore::remote::connection::serve_owned_connection(
            owner.clone(),
            lore::remote::connection::ConnectionId(0),
            listener.accept().unwrap(),
        );
        assert_eq!(owner.state.connection_count(), 1);

        control_stop(owner.clone(), false).await.unwrap();
        assert_eq!(owner.status().0, lore::service::owner::Phase::ReadyToExit);
        owner.state.close_and_drain_invocations().await;
        assert!(owner.state.try_begin_control_invocation().is_none());
        let replacement = if current {
            None
        } else {
            drop(guard.take());
            Some(run(&request))
        };
        let command = message::MessageToServer {
            globals: Default::default(),
            command: lore::remote::command::LoreCommand::ServiceStop(Default::default()),
            environment: Default::default(),
        };
        message::write_payload(&mut client, &message::encode_message(&command)).unwrap();
        let replies = tokio::task::spawn_blocking(move || {
            if !current {
                let mut byte = [0];
                match client.read(&mut byte) {
                    Ok(0) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                    result => panic!("a stale stop connection must close: {result:?}"),
                }
                return Vec::new();
            }
            let mut events = Vec::new();
            loop {
                let (reply, _) =
                    message::blocking_read_message::<message::MessageToClient, _>(&mut client)
                        .unwrap()
                        .expect("an already retired stop still receives a result");
                match reply {
                    message::MessageToClient::Event(lore::interface::LoreEvent::Complete(data)) => {
                        assert_eq!(data.status, 0);
                        assert_eq!(data.error.error_code, 0);
                        events.push("complete");
                    }
                    message::MessageToClient::Event(lore::interface::LoreEvent::End(_)) => {
                        events.push("end");
                    }
                    message::MessageToClient::ApiResult(code) => {
                        assert_eq!(code, 0);
                        events.push("result");
                        return events;
                    }
                    message::MessageToClient::Event(_) => panic!("unexpected stop acknowledgement"),
                }
            }
        });
        let events = tokio::time::timeout(Duration::from_secs(10), replies)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            events,
            if current {
                vec!["complete", "end", "result"]
            } else {
                vec![]
            }
        );
        if let Some((new, _, _)) = &replacement {
            assert!(!request.is_requested());
            assert!(new.state.try_begin_invocation().is_some());
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while owner.state.connection_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(listener);
        let mut lock = network::unix::uds_sock_path(&name).into_os_string();
        lock.push(".lock");
        let _removed = std::fs::remove_file(lock);
        drop(replacement);
        drop(cleanup);
        drop(guard);
    }
}

/// Service initialization configures a process-wide storage flush delay, so the
/// actual run/cancel/restart contract runs in its own normal-harness process.
#[test]
fn actual_service_main_cancel_and_restart_isolated_process() {
    isolated_service_child("service_owner::service_main_cancellation_child");
}
fn isolated_service_child(test: &str) {
    use std::process::Command;
    use std::process::Stdio;
    let isolation = lore_base::test_util::TempDir::new("lore-service-owner-child-");
    let global = isolation.join("global");
    let auth = isolation.join("auth");
    std::fs::create_dir(&global).unwrap();
    std::fs::create_dir(&auth).unwrap();
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let socket = format!(
        "owner-main-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--ignored", "--nocapture"])
        .env(lore_base::env::GLOBAL_PATH_VAR, &global)
        .env(lore_base::env::AUTH_PATH_VAR, &auth)
        .env("LORE_SERVICE_SOCKET", socket)
        .env("LORE_OWNER_MAIN_CHILD", "1")
        .env("LORE_BLOCKING_THREADS", "4")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "isolated service lifecycle timed out: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "isolated service lifecycle failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "invoked in an isolated process by actual_service_main_cancel_and_restart_isolated_process"]
fn service_main_cancellation_child() {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use std::io::Read;

        use lore::remote::message;
        use lore::remote::network;
        use lore::remote::service_process::current_service_owner;
        use lore::remote::service_process::service_runs_in_this_process;
        async fn ready(state: &Arc<ServiceStateImpl>) -> Arc<ServiceOwner> {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if service_runs_in_this_process()
                        && let Some(owner) = current_service_owner()
                        && Arc::ptr_eq(&owner.state, state)
                    {
                        break owner;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap()
        }
        let first = Arc::new(ServiceStateImpl::new());
        let globals = lore::interface::LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        };
        let task = tokio::spawn(lore::service::service_main::service_main(
            globals.clone(),
            first.clone(),
        ));
        let retained = ready(&first).await;
        let old_run = lore::service::actor::current_run().unwrap();
        let name = lore::remote::service_socket_name();
        let mut old_client =
            std::os::unix::net::UnixStream::connect(network::unix::uds_sock_path(name)).unwrap();
        old_client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while first.connection_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(first.try_begin_invocation().is_none());
        tokio::time::timeout(Duration::from_secs(10), async {
            while !old_run.has_completed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(first.try_begin_control_invocation().is_none());
        // Cancellation wakes the independently owned accept thread. Wait for
        // its old socket claim to finish before starting another run.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match network::UdsListener::claim(name).unwrap() {
                    Some(claim) => {
                        drop(claim);
                        break;
                    }
                    None => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
        let second = Arc::new(ServiceStateImpl::new());
        let task = tokio::spawn(lore::service::service_main::service_main(
            globals.clone(),
            second.clone(),
        ));
        let current = ready(&second).await;
        // The first restart reaps the cancelled actor without requiring an
        // earlier caller to wait for its native thread join.
        old_run.wait().await.unwrap();
        assert!(!retained.stop.get().unwrap().is_current());
        assert!(!Arc::ptr_eq(&retained.state, &current.state));
        let command = message::MessageToServer {
            globals: globals.clone(),
            command: lore::remote::command::LoreCommand::ServiceStop(Default::default()),
            environment: Default::default(),
        };
        let _closed = message::write_payload(&mut old_client, &message::encode_message(&command));
        let read = tokio::task::spawn_blocking(move || {
            let mut byte = [0];
            old_client.read(&mut byte)
        });
        match tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .unwrap()
            .unwrap()
        {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            result => panic!("old main connection unexpectedly replied: {result:?}"),
        }
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                current.stop.get().unwrap().requested()
            )
            .await
            .is_err()
        );
        assert!(second.try_begin_invocation().is_some());
        assert_eq!(
            lore::service::quit(globals, lore::service::LoreServiceQuitArgs {}, None).await,
            0
        );
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(second.try_begin_invocation().is_none());
        assert!(second.try_begin_control_invocation().is_none());
        assert!(current_service_owner().is_none());
        let mut lock = network::unix::uds_sock_path(name).into_os_string();
        lock.push(".lock");
        let _removed = std::fs::remove_file(lock);
    });
}

async fn control_stop(
    owner: Arc<ServiceOwner>,
    force: bool,
) -> Result<(), lore::service::ServiceStopError> {
    let ticket = owner.state.try_begin_control_invocation().unwrap();
    with_invocation(
        ticket,
        with_owner(
            owner.clone(),
            async move { owner.stop_control(force).await },
        ),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn graceful_stop_refuses_busy_work_without_closing_admission() {
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let work = owner.state.try_begin_invocation().unwrap();
    assert!(control_stop(owner.clone(), false).await.is_err());
    assert_eq!(owner.status().0, lore::service::owner::Phase::Running);
    assert!(owner.state.try_begin_invocation().is_some());
    assert!(!request.is_requested());
    drop(work);
    tokio::time::timeout(Duration::from_secs(5), control_stop(owner.clone(), false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owner.status().0, lore::service::owner::Phase::ReadyToExit);
    assert!(owner.state.try_begin_invocation().is_none());
    assert!(owner.state.try_begin_control_invocation().is_some());
    assert!(request.is_requested());
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn stop_under_work_ticket_rejects_self_drain_without_changing_lifetime() {
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let result = with_invocation(
        owner.state.try_begin_invocation().unwrap(),
        with_owner(owner.clone(), async { owner.stop_control(true).await }),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(owner.state.try_begin_invocation().is_some());
    assert!(!request.is_requested());
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn cancelled_control_wait_keeps_owned_drain_and_status_controls_alive() {
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let work = owner.state.try_begin_invocation().unwrap();
    let mut stopping = Box::pin(control_stop(owner.clone(), true));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut stopping)
            .await
            .is_err()
    );
    assert_eq!(owner.status().0, lore::service::owner::Phase::Stopping);
    assert!(owner.state.try_begin_invocation().is_none());
    assert!(owner.state.try_begin_control_invocation().is_some());
    drop(stopping);
    assert!(!request.is_requested());
    drop(work);
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.status().0 != lore::service::owner::Phase::ReadyToExit {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(request.is_requested());
    assert_eq!(owner.state.invocation_count(), 0);
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn concurrent_control_stops_join_one_attempt_and_do_not_count_themselves_as_work() {
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let work = owner.state.try_begin_invocation().unwrap();
    let mut first = Box::pin(control_stop(owner.clone(), true));
    let mut second = Box::pin(control_stop(owner.clone(), true));
    tokio::select! {
        result=&mut first=>panic!("first stop ignored active work: {result:?}"),
        result=&mut second=>panic!("second stop ignored active work: {result:?}"),
        _=tokio::time::sleep(Duration::from_millis(20))=>{},
    }
    assert_eq!(owner.state.work_invocation_count(), 1);
    assert_eq!(owner.state.invocation_count(), 3);
    drop(work);
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first, second)
    })
    .await
    .unwrap();
    first.unwrap();
    second.unwrap();
    assert_eq!(owner.status().0, lore::service::owner::Phase::ReadyToExit);
    assert_eq!(owner.state.invocation_count(), 0);
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn queued_stop_rechecks_bound_run_after_serialization_wait() {
    let request = Arc::new(StopRequest::new());
    let (old, guard, cleanup) = run(&request);
    let serial = old.stop_serial.clone().lock_owned().await;
    let mut stopping = Box::pin(control_stop(old.clone(), false));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut stopping)
            .await
            .is_err()
    );
    drop(cleanup);
    drop(guard);
    let (new, guard, cleanup) = run(&request);
    drop(serial);
    tokio::time::timeout(Duration::from_secs(5), stopping)
        .await
        .unwrap()
        .unwrap();
    assert!(new.state.try_begin_invocation().is_some());
    assert!(!request.is_requested());
    drop(cleanup);
    drop(guard);
}

#[tokio::test]
async fn failed_owner_retirement_keeps_control_admission_and_never_signals_terminal_stop() {
    lore_base::runtime::LORE_CONTEXT
        .scope(super::working_mount::execution(), async {
            let request = Arc::new(StopRequest::new());
            let (owner, guard, cleanup) = run(&request);
            let root = lore_base::test_util::TempDir::new("lore-service-retained-owner-");
            let input = super::working_mount::input_with_recovery_limits(
                &root,
                owner.state.provider_registry(),
                lore_vfs::recovery::Limits {
                    max_checkpoint_bytes: 1024,
                },
            )
            .await;
            let prepared = owner
                ._supervisor
                .prepare_instance(
                    input,
                    Arc::new(super::working_mount::ExplicitEdits),
                    lore::working_mount::OwnerLimits {
                        recovery: lore_vfs::recovery::Limits {
                            max_checkpoint_bytes: 1024,
                        },
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let model = prepared.test_model();
            let (file, _) = model
                .create(
                    1,
                    b"too-large",
                    0o644,
                    lore_vfs::linux_working::OpenFlags {
                        write: true,
                        append: false,
                        truncate: false,
                    },
                )
                .await
                .unwrap();
            model.write(file, 0, &[5; 2048]).await.unwrap();
            model.release(file).await.unwrap();
            drop(model);
            drop(prepared);
            assert!(
                tokio::time::timeout(Duration::from_secs(10), control_stop(owner.clone(), false))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(owner.status().0, lore::service::owner::Phase::Retained);
            assert!(owner.status().1.unwrap().contains("ResourceLimit"));
            assert!(!request.is_requested());
            assert!(owner.state.try_begin_invocation().is_none());
            assert!(owner.state.try_begin_control_invocation().is_some());
            assert!(
                tokio::time::timeout(Duration::from_secs(10), control_stop(owner.clone(), true))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(!request.is_requested());
            assert_eq!(owner._supervisor.instances().unwrap().len(), 1);
            drop(cleanup);
            drop(guard);
        })
        .await;
}

#[test]
fn actual_retained_listener_keeps_typed_controls_and_rejects_work() {
    isolated_service_child("service_owner::service_main_retained_child");
}

#[test]
#[ignore = "invoked by actual_retained_listener_keeps_typed_controls_and_rejects_work"]
fn service_main_retained_child() {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use lore::remote::command::LoreCommand;
        use lore::remote::message;
        use lore::remote::network;
        fn rpc(command: LoreCommand) -> Option<(i32, Vec<String>, Vec<u8>)> {
            let mut client = std::os::unix::net::UnixStream::connect(network::unix::uds_sock_path(
                lore::remote::service_socket_name(),
            ))
            .unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let command = message::MessageToServer {
                globals: lore::interface::LoreGlobalArgs {
                    offline: 1,
                    ..Default::default()
                },
                command,
                environment: Default::default(),
            };
            message::write_payload(&mut client, &message::encode_message(&command)).unwrap();
            let mut messages = Vec::new();
            let mut running = Vec::new();
            loop {
                let (reply, _payload) =
                    message::blocking_read_message::<message::MessageToClient, _>(&mut client)
                        .unwrap()?;
                match reply {
                    message::MessageToClient::ApiResult(code) => {
                        return Some((code, messages, running));
                    }
                    message::MessageToClient::Event(
                        lore::interface::LoreEvent::ServiceMessage(data),
                    ) => messages.push(data.message.as_str().to_owned()),
                    message::MessageToClient::Event(lore::interface::LoreEvent::ServiceStatus(
                        data,
                    )) => running.push(data.running),
                    message::MessageToClient::Event(_) => {}
                }
            }
        }
        let state = Arc::new(ServiceStateImpl::new());
        let root = lore_base::test_util::TempDir::new("lore-service-retained-listener-");
        // Bootstrap precedes service initialization's process-wide flush policy.
        let input = lore_base::runtime::LORE_CONTEXT
            .scope(
                super::working_mount::execution(),
                super::working_mount::input_with_recovery_limits(
                    &root,
                    state.provider_registry(),
                    lore_vfs::recovery::Limits {
                        max_checkpoint_bytes: 1024,
                    },
                ),
            )
            .await;
        let task = tokio::spawn(lore::service::service_main::service_main(
            lore::interface::LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            },
            state.clone(),
        ));
        let owner = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if lore::remote::service_process::service_runs_in_this_process()
                    && let Some(owner) = lore::remote::service_process::current_service_owner()
                {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let prepared = owner
            ._supervisor
            .prepare_instance(
                input,
                Arc::new(super::working_mount::ExplicitEdits),
                lore::working_mount::OwnerLimits {
                    recovery: lore_vfs::recovery::Limits {
                        max_checkpoint_bytes: 1024,
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let model = prepared.test_model();
        let (file, _) = model
            .create(
                1,
                b"retained",
                0o644,
                lore_vfs::linux_working::OpenFlags {
                    write: true,
                    append: false,
                    truncate: false,
                },
            )
            .await
            .unwrap();
        model.write(file, 0, &[9; 2048]).await.unwrap();
        model.release(file).await.unwrap();
        drop(model);
        drop(prepared);
        state.drain_logs();
        // A raw request may already be signalled before owner coordination.
        // A failed attempt must wait for control retry rather than spin/exit.
        owner.stop.get().unwrap().request_stop();
        tokio::time::timeout(Duration::from_secs(10), async {
            while owner.status().0 != lore::service::owner::Phase::Retained
                || state.invocation_count() != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (logs, dropped) = state.drain_logs();
        assert!(logs.len() <= 1);
        assert_eq!(dropped, 0);
        assert!(!task.is_finished());
        assert!(state.try_begin_invocation().is_none());
        let (code, messages, running) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(|| rpc(LoreCommand::ServiceStatus(Default::default()))),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(code, 0);
        assert_eq!(running, vec![1]);
        assert!(messages.iter().any(|message| message.contains("Retained")));
        let guards = (0..lore_revision::service_state::MAX_CONTROL_INVOCATIONS)
            .map(|_| state.try_begin_control_invocation().unwrap())
            .collect::<Vec<_>>();
        assert_ne!(
            lore::service::status(Default::default(), Default::default(), None).await,
            0
        );
        assert!(
            tokio::time::timeout(
                Duration::from_secs(10),
                tokio::task::spawn_blocking(|| rpc(LoreCommand::ServiceStatus(Default::default())))
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        drop(guards);
        assert!(
            tokio::time::timeout(
                Duration::from_secs(10),
                tokio::task::spawn_blocking(|| rpc(LoreCommand::ServiceStart(Default::default())))
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        let (code, _, _) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(|| rpc(LoreCommand::ServiceStop(Default::default()))),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_ne!(code, 0);
        assert!(!task.is_finished());
        assert_eq!(owner.status().0, lore::service::owner::Phase::Retained);
        // Caller cancellation leaves the failed owner and its control listener
        // retained. No mounted view is exposed by this isolated fixture.
        task.abort();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(state.try_begin_control_invocation().is_some());
        assert!(state.try_begin_invocation().is_none());
        assert!(lore::remote::service_process::begin_service_start().is_none());
        let run = lore::service::actor::current_run().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(10), run.request_and_wait_shutdown())
                .await
                .unwrap()
                .is_err()
        );
        let (code, messages, _) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(|| rpc(LoreCommand::ServiceStatus(Default::default()))),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(code, 0);
        assert!(messages.iter().any(|message| message.contains("Retained")));
    });
}

#[test]
fn actual_service_actor_runtime_drop_isolated_process() {
    isolated_service_child("service_owner::service_actor_runtime_drop_child");
}

#[test]
#[ignore = "invoked in an isolated process by actual_service_actor_runtime_drop_isolated_process"]
fn service_actor_runtime_drop_child() {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    use std::io::Read;

    use lore::remote::service_process::begin_service_start;
    use lore::remote::service_process::current_service_owner;
    use lore::remote::service_process::service_runs_in_this_process;
    use lore::service::actor::current_run;
    let state = Arc::new(ServiceStateImpl::new());
    let work = state.try_begin_invocation().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let globals = lore::interface::LoreGlobalArgs {
        offline: 1,
        ..Default::default()
    };
    let (old, mut idle) = runtime.block_on(async {
        tokio::spawn(lore::service::service_main::service_main(
            globals.clone(),
            state.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !service_runs_in_this_process() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let idle = std::os::unix::net::UnixStream::connect(
            lore::remote::network::unix::uds_sock_path(lore::remote::service_socket_name()),
        )
        .unwrap();
        idle.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while state.connection_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        (current_run().unwrap(), idle)
    });
    drop(runtime);
    assert!(state.try_begin_invocation().is_none());
    assert!(state.try_begin_control_invocation().is_some());
    assert!(begin_service_start().is_none());
    let observer = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    observer.block_on(async {
        assert_eq!(
            lore::service::status(globals.clone(), Default::default(), None).await,
            0
        );
        assert_eq!(
            current_service_owner().unwrap().status().0,
            lore::service::owner::Phase::Stopping
        );
        drop(work);
        tokio::time::timeout(Duration::from_secs(10), old.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(current_run().is_none());
        let mut byte = [0];
        assert_eq!(idle.read(&mut byte).unwrap(), 0);
        let second = Arc::new(ServiceStateImpl::new());
        let task = tokio::spawn(lore::service::service_main::service_main(
            globals,
            second.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !service_runs_in_this_process() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        old.request_shutdown();
        old.request_and_wait_shutdown().await.unwrap();
        assert!(second.try_begin_invocation().is_some());
        let fresh = current_run().unwrap();
        tokio::time::timeout(Duration::from_secs(10), fresh.request_and_wait_shutdown())
            .await
            .unwrap()
            .unwrap();
        task.await.unwrap().unwrap();
    });
}

#[test]
fn actor_before_spawn_unwind_releases_unpublished_run() {
    isolated_service_child("service_owner::service_actor_before_spawn_child");
}
#[test]
fn actor_after_spawn_unwind_keeps_exact_tracked_run() {
    isolated_service_child("service_owner::service_actor_after_spawn_child");
}
#[test]
fn actor_driver_panic_retains_control_runtime_and_refuses_false_stop_success() {
    isolated_service_child("service_owner::service_actor_driver_panic_child");
}
#[test]
#[ignore = "invoked in an isolated process by actor_before_spawn_unwind_releases_unpublished_run"]
fn service_actor_before_spawn_child() {
    actor_fault_child(1);
}
#[test]
#[ignore = "invoked in an isolated process by actor_after_spawn_unwind_keeps_exact_tracked_run"]
fn service_actor_after_spawn_child() {
    actor_fault_child(2);
}
#[test]
#[ignore = "invoked in an isolated process by actor_driver_panic_retains_control_runtime_and_refuses_false_stop_success"]
fn service_actor_driver_panic_child() {
    actor_fault_child(3);
}
fn actor_fault_child(point: u8) {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use lore::remote::service_process::current_service_owner;
        use lore::remote::service_process::current_stop_request;
        use lore::service::actor::current_run;
        use lore::service::actor::set_test_fault;
        set_test_fault(point);
        let state = Arc::new(ServiceStateImpl::new());
        let globals = lore::interface::LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        };
        let task = tokio::spawn(lore::service::service_main::service_main(
            globals.clone(),
            state.clone(),
        ));
        if point == 1 {
            assert!(task.await.unwrap_err().is_panic());
            assert!(current_run().is_none());
            assert!(current_service_owner().is_none());
            assert!(current_stop_request().is_none());
        } else if point == 2 {
            assert!(task.await.unwrap_err().is_panic());
            let run = current_run().expect("spawned actor must remain tracked after caller unwind");
            tokio::time::timeout(Duration::from_secs(10), run.request_and_wait_shutdown())
                .await
                .unwrap()
                .unwrap();
            assert!(current_run().is_none());
            assert!(current_stop_request().is_none());
            assert!(state.try_begin_invocation().is_none());
        } else {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(owner) = current_service_owner()
                        && owner.status().0 == lore::service::owner::Phase::Retained
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(state.try_begin_invocation().is_none());
            assert!(state.try_begin_control_invocation().is_some());
            let owner = current_service_owner().unwrap();
            assert!(control_stop(owner.clone(), true).await.is_err());
            assert_eq!(owner.status().0, lore::service::owner::Phase::Retained);
            assert_eq!(
                lore::service::status(globals, Default::default(), None).await,
                0
            );
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    current_run().unwrap().request_and_wait_shutdown()
                )
                .await
                .unwrap()
                .is_err()
            );
            assert!(current_run().is_some());
            assert!(current_stop_request().unwrap().is_current());
            task.abort();
        }
    });
}

#[test]
fn actor_saturated_initial_reads_preserve_existing_control_progress() {
    isolated_service_child("service_owner::service_actor_saturation_child");
}
#[test]
#[ignore = "invoked in an isolated process by actor_saturated_initial_reads_preserve_existing_control_progress"]
fn service_actor_saturation_child() {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use std::io::Read;

        use lore::remote::message;
        use lore::remote::network;
        use lore::remote::service_process::service_runs_in_this_process;
        let state = Arc::new(ServiceStateImpl::new());
        let globals = lore::interface::LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        };
        let task = tokio::spawn(lore::service::service_main::service_main(
            globals.clone(),
            state.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !service_runs_in_this_process() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let path = network::unix::uds_sock_path(lore::remote::service_socket_name());
        let mut clients = Vec::new();
        for _ in 0..128 {
            clients.push(std::os::unix::net::UnixStream::connect(&path).unwrap());
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while state.connection_count() != 128 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut excess = std::os::unix::net::UnixStream::connect(&path).unwrap();
        excess
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut byte = [0];
        assert_eq!(excess.read(&mut byte).unwrap(), 0);
        let mut control = clients.pop().unwrap();
        control
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let command = message::MessageToServer {
            globals,
            command: lore::remote::command::LoreCommand::ServiceStatus(Default::default()),
            environment: Default::default(),
        };
        message::write_payload(&mut control, &message::encode_message(&command)).unwrap();
        loop {
            let (reply, _) =
                message::blocking_read_message::<message::MessageToClient, _>(&mut control)
                    .unwrap()
                    .unwrap();
            if let message::MessageToClient::ApiResult(code) = reply {
                assert_eq!(code, 0);
                break;
            }
        }
        let run = lore::service::actor::current_run().unwrap();
        // Idle readers retain actual socket leases until shutdown wakes them.
        assert!(state.connection_count() >= 127);
        tokio::time::timeout(Duration::from_secs(10), run.request_and_wait_shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.connection_count(), 0);
        assert!(lore::service::actor::current_run().is_none());
        task.await.unwrap().unwrap();
    });
}

#[test]
fn actor_failed_start_preserves_external_socket_claim() {
    isolated_service_child("service_owner::service_actor_claim_refusal_child");
}
#[test]
#[ignore = "invoked in an isolated process by actor_failed_start_preserves_external_socket_claim"]
fn service_actor_claim_refusal_child() {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use lore::remote::network::UdsListener;
        use lore::remote::service_socket_name;
        let claim = UdsListener::claim(service_socket_name()).unwrap().unwrap();
        let state = Arc::new(ServiceStateImpl::new());
        let task = tokio::spawn(lore::service::service_main::service_main(
            Default::default(),
            state.clone(),
        ));
        let run = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(run) = lore::service::actor::current_run() {
                    break run;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The external claim has not bound a listener. Cancellation must end
        // the refusal wait without initializing any service stores or mounts.
        assert!(
            tokio::time::timeout(Duration::from_secs(10), run.request_and_wait_shutdown())
                .await
                .unwrap()
                .is_err()
        );
        assert!(task.await.unwrap().is_err());
        assert!(lore::service::actor::current_run().is_none());
        assert!(UdsListener::claim(service_socket_name()).unwrap().is_none());
        assert!(state.try_begin_invocation().is_none());
        drop(claim);
        assert!(UdsListener::claim(service_socket_name()).unwrap().is_some());
    });
}

#[test]
fn actor_fatal_error_fences_queued_stop_after_serial_wait() {
    isolated_service_child("service_owner::service_actor_queued_fatal_child");
}
#[test]
fn actor_fatal_error_prevents_inflight_attempt_success_publication() {
    isolated_service_child("service_owner::service_actor_attempt_fatal_child");
}
#[test]
fn actor_request_identity_exhaustion_cannot_reuse_success() {
    isolated_service_child("service_owner::service_actor_exhausted_child");
}
#[test]
#[ignore = "invoked by actor_fatal_error_fences_queued_stop_after_serial_wait"]
fn service_actor_queued_fatal_child() {
    actor_fatal_race_child(0);
}
#[test]
#[ignore = "invoked by actor_fatal_error_prevents_inflight_attempt_success_publication"]
fn service_actor_attempt_fatal_child() {
    actor_fatal_race_child(1);
}
#[test]
#[ignore = "invoked by actor_request_identity_exhaustion_cannot_reuse_success"]
fn service_actor_exhausted_child() {
    actor_fatal_race_child(2);
}
fn actor_fatal_race_child(kind: u8) {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = Arc::new(ServiceStateImpl::new());
        let task = tokio::spawn(lore::service::service_main::service_main(
            lore::interface::LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            },
            state.clone(),
        ));
        let owner = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if lore::remote::service_process::service_runs_in_this_process()
                    && let Some(owner) = lore::remote::service_process::current_service_owner()
                {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let run = lore::service::actor::current_run().unwrap();
        if kind == 0 {
            let serial = owner.stop_serial.clone().lock_owned().await;
            let mut stopping = Box::pin(control_stop(owner.clone(), true));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut stopping)
                    .await
                    .is_err()
            );
            run.inject_retained_failure();
            drop(serial);
            assert!(
                tokio::time::timeout(Duration::from_secs(5), stopping)
                    .await
                    .unwrap()
                    .is_err()
            );
        } else if kind == 1 {
            let work = state.try_begin_invocation().unwrap();
            let mut stopping = Box::pin(control_stop(owner.clone(), true));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut stopping)
                    .await
                    .is_err()
            );
            assert_eq!(owner.status().0, lore::service::owner::Phase::Stopping);
            run.inject_retained_failure();
            drop(work);
            assert!(
                tokio::time::timeout(Duration::from_secs(5), stopping)
                    .await
                    .unwrap()
                    .is_err()
            );
        } else {
            run.exhaust_request_ids();
            let error =
                tokio::time::timeout(Duration::from_secs(5), run.request_and_wait_shutdown())
                    .await
                    .unwrap()
                    .unwrap_err();
            assert!(error.contains("exhausted"));
        }
        assert_eq!(owner.status().0, lore::service::owner::Phase::Retained);
        assert!(owner.status().1.is_some());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                owner.stop.get().unwrap().requested()
            )
            .await
            .is_err()
        );
        assert!(state.try_begin_invocation().is_none());
        assert!(state.try_begin_control_invocation().is_some());
        assert!(control_stop(owner.clone(), true).await.is_err());
        assert_eq!(owner.status().0, lore::service::owner::Phase::Retained);
        assert!(lore::service::actor::current_run().is_some());
        task.abort();
    });
}

#[tokio::test]
async fn local_edit_admission_requires_exact_work_and_survives_retirement_scope() {
    use lore::service::state::working_admission;
    use lore_vfs::core::Error;
    assert!(matches!(working_admission(), Err(Error::PermissionDenied)));
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let control = owner.state.try_begin_control_invocation().unwrap();
    assert!(matches!(
        with_invocation(
            control,
            with_owner(owner.clone(), async { working_admission() })
        )
        .await
        .unwrap(),
        Err(Error::PermissionDenied)
    ));
    let work = owner.state.try_begin_invocation().unwrap();
    assert!(matches!(
        with_invocation(work, async { working_admission() }).await,
        Err(Error::PermissionDenied)
    ));
    let work = owner.state.try_begin_invocation().unwrap();
    let admission = with_invocation(
        work,
        with_owner(owner.clone(), async { working_admission().unwrap() }),
    )
    .await
    .unwrap();
    admission.validate().unwrap();
    let work = owner.state.try_begin_invocation().unwrap();
    let mut stopping = Box::pin(control_stop(owner.clone(), true));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut stopping)
            .await
            .is_err()
    );
    assert_eq!(owner.status().0, lore::service::owner::Phase::Stopping);
    admission.validate().unwrap();
    drop(work);
    stopping.await.unwrap();
    admission.validate().unwrap();
    drop(guard);
    assert_eq!(admission.validate(), Err(Error::PermissionDenied));
    let (fresh, fresh_guard, fresh_cleanup) = run(&request);
    assert_eq!(admission.validate(), Err(Error::PermissionDenied));
    assert!(fresh.state.try_begin_invocation().is_some());
    drop(fresh_cleanup);
    drop(fresh_guard);
    drop(cleanup);
}

#[tokio::test]
async fn local_edit_admission_does_not_retain_service_owner_or_registry() {
    use lore::service::state::working_admission;
    let request = Arc::new(StopRequest::new());
    let (owner, guard, cleanup) = run(&request);
    let weak_owner = Arc::downgrade(&owner);
    let weak_registry = Arc::downgrade(owner.state.provider_registry());
    let work = owner.state.try_begin_invocation().unwrap();
    let admission = with_invocation(
        work,
        with_owner(owner.clone(), async { working_admission().unwrap() }),
    )
    .await
    .unwrap();
    drop(cleanup);
    drop(owner);
    assert!(weak_owner.upgrade().is_none());
    assert!(weak_registry.upgrade().is_none());
    assert_eq!(
        admission.validate(),
        Err(lore_vfs::core::Error::PermissionDenied)
    );
    drop(guard);
}

#[test]
fn lifecycle_precondition_failure_retains_controls_and_retries_without_bypass() {
    isolated_service_child("service_owner::service_actor_precondition_failure_child");
}
#[test]
#[ignore = "invoked by lifecycle_precondition_failure_retains_controls_and_retries_without_bypass"]
fn service_actor_precondition_failure_child() {
    if std::env::var("LORE_OWNER_MAIN_CHILD").ok().as_deref() != Some("1") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use lore::service::actor::current_run;
        use lore::service::actor::set_test_fault;
        let state = Arc::new(ServiceStateImpl::new());
        let globals = lore::interface::LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        };
        let task = tokio::spawn(lore::service::service_main::service_main(
            globals.clone(),
            state.clone(),
        ));
        let owner = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if lore::remote::service_process::service_runs_in_this_process()
                    && let Some(owner) = lore::remote::service_process::current_service_owner()
                {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let run = current_run().unwrap();
        for _ in 0..2 {
            set_test_fault(4);
            let error =
                tokio::time::timeout(Duration::from_secs(10), run.request_and_wait_shutdown())
                    .await
                    .unwrap()
                    .unwrap_err();
            assert!(error.contains("precondition"));
            assert_eq!(owner.status().0, lore::service::owner::Phase::Retained);
            assert!(owner.status().1.unwrap().contains("precondition"));
            assert!(state.try_begin_invocation().is_none());
            assert!(state.try_begin_control_invocation().is_some());
            assert_eq!(
                lore::service::status(globals.clone(), Default::default(), None).await,
                0
            );
        }
        tokio::time::timeout(Duration::from_secs(10), run.request_and_wait_shutdown())
            .await
            .unwrap()
            .unwrap();
        task.await.unwrap().unwrap();
        assert!(current_run().is_none());
    });
}
