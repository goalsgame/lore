// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! One frozen operation's current backend and effective source policy. Retaining
//! this capture never retains writer exclusion or grants access to a target.

use super::*;

pub(crate) struct OperationRouting {
    namespace: std::sync::Weak<Namespace>,
    token: u64,
    binding: Arc<NamespaceBinding>,
}
impl OperationRouting {
    pub(crate) fn backend(&self) -> &Arc<dyn Backend> {
        &self.binding.backend
    }
    pub(crate) fn check(&self, namespace: &Arc<Namespace>, token: u64) -> Result<()> {
        if token != self.token
            || !std::sync::Weak::ptr_eq(&self.namespace, &Arc::downgrade(namespace))
            || !namespace.operation_freeze.matches(token)
        {
            return Err(Error::Stale.into());
        }
        namespace
            .confirms_binding(&self.binding)
            .map_err(Into::into)
    }
    pub(crate) fn policy(&self) -> Arc<dyn AccessGate> {
        self.binding.policy.clone()
    }
    pub(crate) async fn authorize_source(
        &self,
        namespace: &Arc<Namespace>,
        token: u64,
        authority: &NodeKey,
    ) -> Result<()> {
        self.check(namespace, token)?;
        self.binding.gate.authorize(authority, Access::Read).await?;
        self.check(namespace, token)
    }
}
impl Namespace {
    pub(crate) async fn capture_operation_routing(
        self: &Arc<Self>,
        lease: &FrozenNamespace,
    ) -> Result<OperationRouting> {
        let state = self.state.lock().await;
        if !Arc::ptr_eq(self, lease.namespace()) || !lease.valid() {
            return Err(Error::Stale.into());
        }
        Ok(OperationRouting {
            namespace: Arc::downgrade(self),
            token: lease.token(),
            binding: self.current_binding_locked(&state)?,
        })
    }
}

#[cfg(test)]
mod tests {
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
            Box::pin(async { panic!("routing capture performs no policy IO") })
        }
    }
    #[tokio::test]
    async fn cancelled_capture_never_adds_a_freeze_pin_or_runs_io_under_the_lock() {
        let root = Node {
            key: NodeKey {
                repository: [1; 16].into(),
                revision: lore_base::types::Address::default(),
                node: 0,
                exposure: 0,
            },
            kind: NodeKind::Directory,
            size: 0,
            mode: 0o555,
        };
        let namespace = Arc::new(
            Namespace::new(Arc::new(NoIo), root, Arc::new(NoIo), Default::default()).unwrap(),
        );
        let lease = namespace.freeze_operation().await.unwrap();
        let locked = namespace.state.lock().await;
        let mut pending = Box::pin(namespace.capture_operation_routing(&lease));
        std::future::poll_fn(|cx| {
            assert!(matches!(
                std::future::Future::poll(pending.as_mut(), cx),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
        drop(lease);
        drop(locked);
        let lease = namespace.freeze_operation().await.unwrap();
        let routing = namespace.capture_operation_routing(&lease).await.unwrap();
        drop(lease);
        assert!(routing.check(&namespace, routing.token).is_err());
        drop(namespace.freeze_operation().await.unwrap());
    }
}
