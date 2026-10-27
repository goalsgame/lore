// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::namespace::file_access::tests::Policy;
use crate::namespace::file_access::tests::fixture;

struct Reader {
    inner: Arc<dyn Backend>,
    reads: AtomicUsize,
    digests: AtomicUsize,
    policy: Arc<Policy>,
    revoke: AtomicBool,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    hash: u8,
}
impl Backend for Reader {
    fn lookup<'a>(&'a self, key: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        self.inner.lookup(key, name)
    }
    fn list<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        count: usize,
    ) -> BackendFuture<'a, BackendPage> {
        self.inner.list(key, offset, count)
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.pause.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            let bytes = self.inner.read(key, offset, size).await?;
            if self.revoke.load(Ordering::SeqCst) {
                self.policy.denied.store(true, Ordering::SeqCst);
            }
            Ok(bytes)
        })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async move {
            self.digests.fetch_add(1, Ordering::SeqCst);
            if self.revoke.load(Ordering::SeqCst) {
                self.policy.denied.store(true, Ordering::SeqCst);
            }
            Ok(Some([self.hash; 32]))
        })
    }
}
// A private routing fixture replaces only the fixed binding, without exposing an
// activation API or changing concrete RevisionBackend allocation state.
fn bind(namespace: &mut Arc<Namespace>, policy: Arc<Policy>, hash: u8) -> Arc<Reader> {
    let ns = Arc::get_mut(namespace).unwrap();
    let reader = Arc::new(Reader {
        inner: ns.current_binding().unwrap().backend.clone(),
        reads: 0.into(),
        digests: 0.into(),
        policy: policy.clone(),
        revoke: false.into(),
        pause: false.into(),
        entered: Default::default(),
        resume: Default::default(),
        hash,
    });
    let erased: Arc<dyn Backend> = reader.clone();
    ns.routing = routing::Routing::new(Arc::new(NamespaceBinding {
        backend: erased.clone(),
        gate: Arc::new(BackendGate {
            backend: erased,
            gate: policy.clone(),
            underlays: Default::default(),
        }),
        policy,
    }));
    reader
}

#[tokio::test]
async fn contextual_metadata_and_model_ignore_denied_overlay_default() {
    let f = fixture(true).await;
    f.old.denied.store(true, Ordering::SeqCst);
    assert!(f.overlay.metadata().await.is_err());
    assert_eq!(f.namespace.file_metadata(f.inode).await.unwrap().1.size, 4);
    let model = f.model.unwrap();
    assert_eq!(model.getattr(f.inode).await.unwrap().size, 4);
    f.current.denied.store(true, Ordering::SeqCst);
    assert!(f.namespace.file_metadata(f.inode).await.is_err());
    assert!(model.getattr(f.inode).await.is_err());
    model.shutdown().await.unwrap();
}

#[tokio::test]
async fn digest_is_payload_free_and_rechecks_selected_policy() {
    let mut f = fixture(false).await;
    let reader = bind(&mut f.namespace, f.current.clone(), 9);
    f.old.denied.store(true, Ordering::SeqCst);
    assert_eq!(f.namespace.digest(f.inode).await.unwrap(), Some([9; 32]));
    assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    reader.revoke.store(true, Ordering::SeqCst);
    assert!(f.namespace.digest(f.inode).await.is_err());
    assert_eq!(reader.digests.load(Ordering::SeqCst), 2);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn frozen_reads_use_selected_backend_and_do_not_hold_namespace_lock() {
    let mut f = fixture(false).await;
    let reader = bind(&mut f.namespace, f.current.clone(), 9);
    f.old.denied.store(true, Ordering::SeqCst);
    let freeze = f.namespace.freeze_operation().await.unwrap();
    reader.pause.store(true, Ordering::SeqCst);
    let mut read = Box::pin(f.namespace.read_frozen(freeze.token(), f.inode, 0, 4));
    tokio::select! { result = &mut read => panic!("unexpected completion: {result:?}"), () = reader.entered.notified() => {} }
    assert!(f.namespace.state.try_lock().is_ok());
    reader.revoke.store(true, Ordering::SeqCst);
    reader.resume.notify_one();
    assert!(read.await.is_err());
    assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
    drop(freeze);
    f.current.denied.store(false, Ordering::SeqCst);
    let freeze = f.namespace.freeze_operation().await.unwrap();
    let mut read = Box::pin(f.namespace.read_frozen(freeze.token(), f.inode, 0, 4));
    tokio::select! { result = &mut read => panic!("unexpected completion: {result:?}"), () = reader.entered.notified() => {} }
    drop(read);
    drop(freeze);
    assert!(f.namespace.freeze_operation().await.is_ok());
}

#[tokio::test]
async fn detached_reads_keep_retired_policy_and_backend() {
    let mut f = fixture(false).await;
    let retired = bind(&mut f.namespace, f.current.clone(), 9);
    f.namespace.remove(1, b"file", false).await.unwrap();
    let current = Arc::new(Policy::default());
    bind(&mut f.namespace, current.clone(), 7);
    f.old.denied.store(true, Ordering::SeqCst);
    assert_eq!(f.namespace.digest(f.inode).await.unwrap(), Some([9; 32]));
    assert_eq!(f.namespace.file_metadata(f.inode).await.unwrap().1.size, 4);
    let freeze = f.namespace.freeze_operation().await.unwrap();
    assert_eq!(
        f.namespace
            .read_frozen(freeze.token(), f.inode, 0, 4)
            .await
            .unwrap(),
        b"base"
    );
    drop(freeze);
    assert_eq!(retired.reads.load(Ordering::SeqCst), 1);
    f.current.denied.store(true, Ordering::SeqCst);
    assert!(!current.denied.load(Ordering::SeqCst));
    assert!(f.namespace.digest(f.inode).await.is_err());
    assert!(f.namespace.freeze_operation().await.is_err());
}

#[tokio::test]
async fn writer_scan_uses_current_route_and_failed_freeze_releases_admission() {
    let f = fixture(false).await;
    let access = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    let writer = f
        .overlay
        .open_captured(&access, Access::Write)
        .await
        .unwrap();
    f.old.denied.store(true, Ordering::SeqCst);
    assert!(matches!(
        f.namespace.freeze_operation().await,
        Err(NamespaceError::Busy)
    ));
    f.overlay.release(writer).await.unwrap();
    let freeze = f.namespace.freeze_operation().await.unwrap();
    assert!(matches!(
        f.overlay.open_captured(&access, Access::Write).await,
        Err(Error::Busy)
    ));
    drop(freeze);
    let writer = f
        .overlay
        .open_captured(&access, Access::Write)
        .await
        .unwrap();
    f.overlay.release(writer).await.unwrap();
}

#[tokio::test]
async fn dirty_and_unknown_receipt_digests_never_fall_back_to_immutable_hash() {
    let mut f = fixture(false).await;
    let reader = bind(&mut f.namespace, f.current.clone(), 9);
    let access = f
        .namespace
        .capture_file_access(f.inode, &f.overlay)
        .await
        .unwrap();
    let writer = f
        .overlay
        .open_captured(&access, Access::Write)
        .await
        .unwrap();
    f.overlay.write(writer, 0, b"edit").await.unwrap();
    f.overlay.release(writer).await.unwrap();
    f.old.denied.store(true, Ordering::SeqCst);
    assert_eq!(f.namespace.digest(f.inode).await.unwrap(), None);
    let (_, metadata) = f.namespace.file_metadata(f.inode).await.unwrap();
    let mut proof = f
        .namespace
        .capture_record_read(f.inode, None)
        .await
        .unwrap()
        .entry
        .base
        .unwrap();
    proof.mode = 0o444;
    *receipt_lock(&f.overlay.receipt_cell()).unwrap() = Some(Receipt {
        inode: f.inode,
        authority: f.overlay.key(),
        generation: metadata.generation,
        mode: metadata.mode,
        proof: crate::overlay::CommittedProof {
            node: proof,
            address: Default::default(),
        },
        sha256: None,
    });
    assert_eq!(f.namespace.digest(f.inode).await.unwrap(), None);
    assert_eq!(reader.digests.load(Ordering::SeqCst), 0);
    assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    let freeze = f.namespace.freeze_operation().await.unwrap();
    assert_eq!(
        f.namespace
            .read_frozen(freeze.token(), f.inode, 0, 4)
            .await
            .unwrap(),
        b"edit"
    );
    f.current.denied.store(true, Ordering::SeqCst);
    assert!(
        f.namespace
            .read_frozen(freeze.token(), f.inode, 0, 4)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_read_proof_rejects_foreign_overlay_and_changed_record_identity() {
    let f = fixture(false).await;
    let foreign = fixture(false).await;
    let read = f
        .namespace
        .capture_record_read(f.inode, None)
        .await
        .unwrap();
    assert!(
        foreign
            .overlay
            .metadata_bound(read.access.as_ref().unwrap())
            .await
            .is_err()
    );
    {
        let mut state = f.namespace.state.lock().await;
        state.nodes.get_mut(&f.inode).unwrap().receipt = receipt_cell();
    }
    assert!(matches!(
        f.namespace.confirm_record_read(&read, None).await,
        Err(NamespaceError::Backend(Error::Stale))
    ));
}
