// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;

struct NoIo;
impl Backend for NoIo {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { panic!("routing capture performs no lookup") })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async { panic!("routing capture performs no listing") })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { panic!("routing capture performs no payload read") })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { panic!("routing capture performs no digest lookup") })
    }
}
impl AccessGate for NoIo {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: Access) -> BackendFuture<'a, ()> {
        Box::pin(async { panic!("routing capture performs no policy I/O") })
    }
}
fn binding() -> Arc<NamespaceBinding> {
    Arc::new(NamespaceBinding {
        backend: Arc::new(NoIo),
        gate: Arc::new(NoIo),
        policy: Arc::new(NoIo),
    })
}

#[test]
fn capture_preserves_the_exact_pair_and_releases_the_read_guard() {
    let original = binding();
    let routing = Routing::new(original.clone());
    let captured = routing.capture().unwrap();
    assert!(Arc::ptr_eq(&captured, &original));
    routing.confirm(&captured).unwrap();
    assert!(routing.current.try_write().is_ok());
    assert_eq!(routing.confirm(&binding()), Err(Error::Stale));
}

#[test]
fn poisoned_routing_never_recovers_or_returns_a_fallback_pair() {
    let original = binding();
    let routing = Routing::new(original.clone());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = routing.current.write().unwrap();
        panic!("poison the routing lock");
    }));
    assert!(panic.is_err());
    assert!(matches!(routing.capture(), Err(Error::Io)));
    assert_eq!(routing.confirm(&original), Err(Error::Io));
}

#[tokio::test]
async fn transition_waits_for_exact_freeze_before_inspecting_its_source() {
    let root = Node {
        key: NodeKey {
            repository: [1; 16].into(),
            revision: Default::default(),
            node: 0,
            exposure: 0,
        },
        kind: NodeKind::Directory,
        size: 0,
        mode: 0o555,
    };
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(NoIo),
            root.clone(),
            Arc::new(NoIo),
            Default::default(),
        )
        .unwrap(),
    );
    let state = namespace.state.lock().await;
    let mut preparing = Box::pin(crate::transition_prepare::prepare(
        namespace.clone(),
        root.key,
        Default::default(),
        Default::default(),
        Default::default(),
    ));
    std::future::poll_fn(|cx| {
        assert!(matches!(
            preparing.as_mut().poll(cx),
            std::task::Poll::Pending
        ));
        std::task::Poll::Ready(())
    })
    .await;
    assert!(namespace.operation_freeze.frozen());
    drop(preparing);
    assert!(!namespace.operation_freeze.frozen());
    drop(state);
}

#[cfg(unix)]
#[tokio::test]
async fn synchronous_owner_checks_do_not_take_namespace_state_and_poison_fails_closed() {
    let fixture = crate::namespace::file_access::tests::fixture(true).await;
    let model = fixture.model.as_ref().unwrap();
    let handle = model.opendir(1).await.unwrap();
    let backend = fixture.namespace.backend().unwrap();
    let state = fixture.namespace.state.lock().await;
    assert!(fixture.namespace.uses_backend(&backend).unwrap());
    assert!(Arc::ptr_eq(&fixture.namespace.backend().unwrap(), &backend));
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        model.readdir(handle, 0, 4),
    )
    .await
    .expect("Model liveness must not wait for Namespace.state")
    .unwrap();
    drop(state);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = fixture.namespace.routing.current.write().unwrap();
        panic!("poison the routing lock");
    }));
    assert!(panic.is_err());
    assert!(matches!(fixture.namespace.backend(), Err(Error::Io)));
    assert!(matches!(
        fixture.namespace.transition_policy(),
        Err(Error::Io)
    ));
    assert_eq!(fixture.namespace.uses_backend(&backend), Err(Error::Io));
    assert!(matches!(
        model.readdir(handle, 0, 4).await,
        Err(crate::linux_working::Error::Core(Error::Io))
    ));
}
