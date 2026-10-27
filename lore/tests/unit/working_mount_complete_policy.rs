// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use std::sync::atomic::AtomicUsize;

struct Policy {
    inner: Arc<dyn AccessGate>,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl AccessGate for Policy {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        self.inner.authorize(key, access)
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.inner.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.pause.load(Ordering::Acquire) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.inner.authorize_target_head(proof).await
        })
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        self.inner.authorize_target_node(proof, key, access)
    }
}
struct Factory {
    local: LocalCheckoutFactory,
    calls: AtomicUsize,
    source: Mutex<Option<Arc<Policy>>>,
}
impl AccessGateFactory for Factory {
    fn bind(&self, backend: Arc<dyn Backend>) -> lore_vfs::core::Result<Arc<dyn AccessGate>> {
        let policy = Arc::new(Policy {
            inner: self.local.bind(backend)?,
            pause: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
            *self.source.lock().unwrap() = Some(policy.clone());
        }
        Ok(policy)
    }
}
struct Fixture {
    _owner: Arc<ServiceOwner>,
    guard: Option<ServiceStartGuard>,
    namespace: Arc<lore_vfs::namespace::Namespace>,
    backend: Arc<RevisionBackend>,
    factory: Arc<Factory>,
    scope: NodeKey,
    target: Address,
}
impl Fixture {
    async fn new() -> Self {
        let (owner, guard, admission) = admitted().await;
        let execution = super::super::working_mount::execution();
        let (backend, target) = LORE_CONTEXT
            .scope(execution.clone(), async {
                let repository = repository().await;
                let source = State::new();
                add_file(&repository, &source, 0, "file", b"source bytes").await;
                let source = commit_state(&repository, source, Hash::default()).await;
                let target = State::new();
                add_file(&repository, &target, 0, "file", b"target bytes").await;
                let target = commit_state(&repository, target, source).await;
                (
                    Arc::new(RevisionBackend::new(repository, source).await.unwrap()),
                    Address::zero_context_hash(target),
                )
            })
            .await;
        let scope = backend.root().key;
        let factory = Arc::new(Factory {
            local: LocalCheckoutFactory::new(admission, execution).unwrap(),
            calls: AtomicUsize::new(0),
            source: Mutex::new(None),
        });
        let namespace = Arc::new(
            lore_vfs::namespace::Namespace::new_with_gate_factory(
                backend.clone(),
                backend.root(),
                factory.clone(),
                Default::default(),
            )
            .unwrap(),
        );
        Self {
            _owner: owner,
            guard: Some(guard),
            namespace,
            backend,
            factory,
            scope,
            target,
        }
    }
    fn source(&self) -> Arc<Policy> {
        self.factory
            .source
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
    }
}

#[tokio::test]
async fn complete_local_binding_preserves_old_and_target_keys_without_retaining_namespace() {
    let mut fixture = Fixture::new().await;
    let weak = Arc::downgrade(&fixture.namespace);
    let source = fixture.source();
    let complete = lore_vfs::test_util::prepare_complete_policy(
        fixture.namespace.clone(),
        fixture.scope,
        fixture.target,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(fixture.factory.calls.load(Ordering::Acquire), 2);
    drop(fixture.namespace.freeze_operation().await.unwrap());
    let backend = complete.backend();
    let old = backend.lookup(&fixture.scope, b"file").await.unwrap();
    let target = backend
        .lookup(
            &NodeKey {
                revision: fixture.target,
                ..fixture.scope
            },
            b"file",
        )
        .await
        .unwrap();
    assert_eq!(
        backend.read(&old.key, 0, 64).await.unwrap(),
        b"source bytes"
    );
    assert_eq!(
        backend.read(&target.key, 0, 64).await.unwrap(),
        b"target bytes"
    );
    for key in [&old.key, &target.key] {
        complete
            .policy()
            .authorize(key, Access::Read)
            .await
            .unwrap();
        complete
            .policy()
            .authorize(key, Access::Write)
            .await
            .unwrap();
    }
    assert!(source.authorize(&target.key, Access::Read).await.is_err());
    assert!(
        fixture
            .backend
            .revalidate(&target.key, false)
            .await
            .is_err()
    );
    drop(fixture.namespace);
    assert!(weak.upgrade().is_none());
    fixture.guard.take();
    for key in [&old.key, &target.key] {
        assert_eq!(
            complete.policy().authorize(key, Access::Read).await,
            Err(Error::PermissionDenied)
        );
    }
}

struct NoWrite(Arc<dyn AccessGate>);
impl AccessGate for NoWrite {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if access == Access::Write {
                return Err(Error::PermissionDenied);
            }
            self.0.authorize(key, access).await
        })
    }
    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        self.0.authorize_selection(selection)
    }
    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        self.0.authorize_target_head(proof)
    }
    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        self.0.authorize_target_node(proof, key, access)
    }
}
#[tokio::test]
async fn complete_binding_reuses_strict_injected_policy_without_write_upgrade() {
    let fixture = Fixture::new().await;
    let strict: Arc<dyn AccessGate> = Arc::new(NoWrite(fixture.source()));
    let namespace = Arc::new(
        lore_vfs::namespace::Namespace::new(
            fixture.backend.clone(),
            fixture.backend.root(),
            strict.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let complete = lore_vfs::test_util::prepare_complete_policy(
        namespace,
        fixture.scope,
        fixture.target,
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&complete.policy(), &strict));
    let backend = complete.backend();
    for root in [
        fixture.scope,
        NodeKey {
            revision: fixture.target,
            ..fixture.scope
        },
    ] {
        let file = backend.lookup(&root, b"file").await.unwrap();
        assert_eq!(
            complete.policy().authorize(&file.key, Access::Write).await,
            Err(Error::PermissionDenied)
        );
    }
}

#[tokio::test]
async fn cancelled_or_revoked_complete_preparation_releases_source_freeze() {
    for revoke in [false, true] {
        let mut fixture = Fixture::new().await;
        let source = fixture.source();
        source.pause.store(true, Ordering::Release);
        let mut pending = Box::pin(lore_vfs::test_util::prepare_complete_policy(
            fixture.namespace.clone(),
            fixture.scope,
            fixture.target,
            Default::default(),
            Default::default(),
        ));
        tokio::select! {
            _=source.entered.notified()=>{},
            result=&mut pending=>panic!("source pause bypassed: {}",result.is_ok()),
            _=tokio::time::sleep(Duration::from_secs(10))=>panic!("source pause not reached"),
        }
        if revoke {
            fixture.guard.take();
            source.pause.store(false, Ordering::Release);
            source.resume.notify_one();
            assert!(pending.await.is_err());
        } else {
            drop(pending);
            source.pause.store(false, Ordering::Release);
        }
        assert_eq!(fixture.factory.calls.load(Ordering::Acquire), 1);
        drop(fixture.namespace.freeze_operation().await.unwrap());
    }
}
