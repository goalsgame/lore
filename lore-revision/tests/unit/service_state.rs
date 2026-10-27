// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_revision::service_state::ServiceStateImpl;

#[test]
fn independent_service_states_own_distinct_provider_registries() {
    let first = ServiceStateImpl::new();
    let second = ServiceStateImpl::new();
    assert!(!Arc::ptr_eq(
        first.provider_registry(),
        second.provider_registry()
    ));
    assert!(Arc::ptr_eq(
        first.provider_registry(),
        first.provider_registry()
    ));
}

#[test]
#[allow(clippy::disallowed_methods)] // Exercise cancellation on an owned test runtime.
fn disconnected_invocations_remain_counted_until_their_tasks_end() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = Arc::new(ServiceStateImpl::new());
        let connection = state.increment_connections();
        let invocation = state.try_begin_invocation().unwrap();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _invocation = invocation;
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        drop(connection);
        assert_eq!(state.connection_count(), 0);
        assert_eq!(state.invocation_count(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(state.invocation_count(), 0);

        let invocation = state.try_begin_invocation().unwrap();
        let unpolled = async move {
            let _invocation = invocation;
            std::future::pending::<()>().await;
        };
        assert_eq!(state.invocation_count(), 1);
        drop(unpolled);
        assert_eq!(state.invocation_count(), 0);
    });
}

#[tokio::test]
async fn command_drain_fences_late_arrivals_and_survives_cancelled_waiters() {
    use std::future::Future;
    use std::task::Poll;
    let state = Arc::new(ServiceStateImpl::new());
    let first = state.try_begin_invocation().unwrap();
    let second = state.try_begin_invocation().unwrap();
    let mut drain = Box::pin(state.close_and_drain_invocations());
    std::future::poll_fn(|context| {
        assert!(drain.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(state.try_begin_invocation().is_none());
    drop(drain);
    drop(first);
    assert_eq!(state.invocation_count(), 1);
    let mut retry = Box::pin(state.close_and_drain_invocations());
    let mut other_waiter = Box::pin(state.close_and_drain_invocations());
    std::future::poll_fn(|context| {
        assert!(retry.as_mut().poll(context).is_pending());
        assert!(other_waiter.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(second);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(retry, other_waiter);
    })
    .await
    .unwrap();
    assert_eq!(state.invocation_count(), 0);
    assert!(state.try_begin_invocation().is_none());
}

#[tokio::test]
async fn graceful_stop_busy_refusal_preserves_admission_and_success_is_terminal() {
    let state = Arc::new(ServiceStateImpl::new());
    let stop = state.try_begin_invocation().unwrap();
    let work = state.try_begin_invocation().unwrap();
    assert!(!stop.try_close_admission_if_last());
    let later = state
        .try_begin_invocation()
        .expect("busy stop keeps serving");
    drop(work);
    assert!(!stop.try_close_admission_if_last());
    drop(later);
    assert!(stop.try_close_admission_if_last());
    assert!(state.try_begin_invocation().is_none());
    assert!(!stop.try_close_admission_if_last());
    drop(stop);
    state.close_and_drain_invocations().await;
    assert!(state.try_begin_invocation().is_none());
}

#[test]
fn graceful_stop_and_new_command_cannot_both_win_admission() {
    for _ in 0..64 {
        let state = Arc::new(ServiceStateImpl::new());
        let stop = state.try_begin_invocation().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker_state = state.clone();
        let worker_barrier = barrier.clone();
        let worker = std::thread::spawn(move || {
            worker_barrier.wait();
            worker_state.try_begin_invocation()
        });
        barrier.wait();
        let closed = stop.try_close_admission_if_last();
        let admitted = worker.join().unwrap();
        assert_ne!(closed, admitted.is_some());
        if !closed {
            assert!(state.try_begin_invocation().is_some());
        }
        drop(admitted);
        drop(stop);
        assert_eq!(state.invocation_count(), 0);
    }
}

#[tokio::test]
async fn control_admission_is_bounded_and_survives_work_retirement_only() {
    use lore_revision::service_state::MAX_CONTROL_INVOCATIONS;
    let state = Arc::new(ServiceStateImpl::new());
    let work = state.try_begin_invocation().unwrap();
    assert!(!work.is_control());
    assert!(!work.try_close_work_admission_if_idle());
    let mut controls: Vec<_> = (0..MAX_CONTROL_INVOCATIONS)
        .map(|_| state.try_begin_control_invocation().unwrap())
        .collect();
    assert!(controls.iter().all(|ticket| ticket.is_control()));
    assert!(state.try_begin_control_invocation().is_none());
    assert_eq!(state.invocation_count(), MAX_CONTROL_INVOCATIONS + 1);
    assert_eq!(state.work_invocation_count(), 1);
    assert!(!controls[0].try_close_work_admission_if_idle());
    assert!(state.try_begin_invocation().is_some());
    drop(work);
    assert!(controls[0].try_close_work_admission_if_idle());
    assert!(state.try_begin_invocation().is_none());
    // Retirement can drain work while the stop and status tickets are alive.
    state.close_and_drain_work_invocations().await;
    drop(controls.pop());
    let replacement = state.try_begin_control_invocation().unwrap();
    state.close_invocation_admission();
    state.close_work_admission();
    assert!(state.try_begin_control_invocation().is_none());
    assert!(state.try_begin_invocation().is_none());
    drop(controls);
    drop(replacement);
    state.close_and_drain_invocations().await;
    assert_eq!(state.invocation_count(), 0);
}

#[tokio::test]
async fn cancelled_work_drain_keeps_controls_usable_and_terminal_drain_waits_for_them() {
    use std::future::Future;
    use std::task::Poll;
    let state = Arc::new(ServiceStateImpl::new());
    let work = state.try_begin_invocation().unwrap();
    let control = state.try_begin_control_invocation().unwrap();
    let mut drain = Box::pin(state.close_and_drain_work_invocations());
    std::future::poll_fn(|context| {
        assert!(drain.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(drain);
    assert!(state.try_begin_invocation().is_none());
    assert!(state.try_begin_control_invocation().is_some());
    drop(work);
    state.close_and_drain_work_invocations().await;
    let mut terminal = Box::pin(state.close_and_drain_invocations());
    std::future::poll_fn(|context| {
        assert!(terminal.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(state.try_begin_control_invocation().is_none());
    drop(control);
    terminal.await;
}

#[test]
fn control_graceful_fence_and_normal_command_cannot_both_win() {
    for _ in 0..64 {
        let state = Arc::new(ServiceStateImpl::new());
        let control = state.try_begin_control_invocation().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker_state = state.clone();
        let worker_barrier = barrier.clone();
        let worker = std::thread::spawn(move || {
            worker_barrier.wait();
            worker_state.try_begin_invocation()
        });
        barrier.wait();
        let closed = control.try_close_work_admission_if_idle();
        let admitted = worker.join().unwrap();
        assert_ne!(closed, admitted.is_some());
        assert!(state.try_begin_control_invocation().is_some());
        drop(admitted);
        drop(control);
        assert_eq!(state.invocation_count(), 0);
        assert_eq!(state.work_invocation_count(), 0);
    }
}

#[tokio::test]
#[allow(clippy::disallowed_methods)] // A raw owned task tests ticket cancellation without call context.
async fn cancelled_control_tasks_release_their_capacity() {
    let state = Arc::new(ServiceStateImpl::new());
    let control = state.try_begin_control_invocation().unwrap();
    let task = tokio::spawn(async move {
        let _control = control;
        std::future::pending::<()>().await;
    });
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(state.invocation_count(), 0);
    assert_eq!(state.work_invocation_count(), 0);
    assert!(state.try_begin_control_invocation().is_some());
}
