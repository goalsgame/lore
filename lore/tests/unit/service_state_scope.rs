// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;

use lore::service::state::ServiceStateImpl;
use lore::service::state::current_service_state;
use lore::service::state::try_close_current_admission;
use lore::service::state::with_invocation;

#[tokio::test]
#[allow(clippy::disallowed_methods)] // Verify plain background tasks cannot inherit a ticket.
async fn invocation_scope_isolated_from_nested_owners_and_background_tasks() {
    let outer = Arc::new(ServiceStateImpl::new());
    let inner = Arc::new(ServiceStateImpl::new());
    assert!(current_service_state().is_none());
    assert_eq!(try_close_current_admission(), None);
    with_invocation(outer.try_begin_invocation().unwrap(), async {
        assert!(Arc::ptr_eq(&current_service_state().unwrap(), &outer));
        with_invocation(inner.try_begin_invocation().unwrap(), async {
            assert!(Arc::ptr_eq(&current_service_state().unwrap(), &inner));
            assert_eq!(try_close_current_admission(), Some(true));
        })
        .await;
        assert!(Arc::ptr_eq(&current_service_state().unwrap(), &outer));
        tokio::spawn(async {
            assert!(current_service_state().is_none());
            assert_eq!(try_close_current_admission(), None);
        })
        .await
        .unwrap();
        assert!(outer.try_begin_invocation().is_some());
        assert!(inner.try_begin_invocation().is_none());
    })
    .await;
    assert!(current_service_state().is_none());
    assert_eq!(outer.invocation_count(), 0);
    assert_eq!(inner.invocation_count(), 0);
}

#[tokio::test]
async fn cancelled_scoped_command_releases_ticket_and_clears_context() {
    use std::future::Future;
    use std::task::Poll;
    let state = Arc::new(ServiceStateImpl::new());
    let mut command = Box::pin(with_invocation(
        state.try_begin_invocation().unwrap(),
        async {
            assert!(current_service_state().is_some());
            std::future::pending::<()>().await;
        },
    ));
    std::future::poll_fn(|context| {
        assert!(command.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(current_service_state().is_none());
    assert_eq!(state.invocation_count(), 1);
    drop(command);
    assert!(current_service_state().is_none());
    assert_eq!(state.invocation_count(), 0);
    state.close_and_drain_invocations().await;
}
