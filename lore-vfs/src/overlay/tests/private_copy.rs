// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::storage_budget::BudgetedStore;
use crate::storage_budget::StorageBudget;

async fn image() -> (Arc<OverlayFile>, Arc<Base>, Arc<Gate>, FileSnapshot) {
    let (file, base, _, gate) = fixture(Limits::default());
    let writer = file.open(Access::Write).await.unwrap();
    file.write(writer, 0, b"ABCDEFGH").await.unwrap();
    file.release(writer).await.unwrap();
    let snapshot = file.checkpoint_guard().await.capture(100).await.unwrap();
    (file, base, gate, snapshot)
}
fn budgeted(store: Arc<Store>, budget: Arc<StorageBudget>) -> Arc<dyn SparseStore> {
    Arc::new(BudgetedStore::new(store, budget))
}

#[tokio::test]
async fn copy_extent_budget_does_not_reduce_runtime_file_limits() {
    let (_, base, gate, snapshot) = image().await;
    let limits = Limits {
        max_extents: 128,
        ..Limits::default()
    };
    let store = Arc::new(Store::default());
    let budget = Arc::new(StorageBudget::new(100));
    let copy = OverlayFile::copy_private_bounded(
        &snapshot,
        1,
        2,
        base,
        gate,
        budgeted(store, budget),
        CopyLimits {
            file: limits,
            extents: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(copy.limits.max_extents, 128);
    let writer = copy.open(Access::Write).await.unwrap();
    copy.write(writer, 10, b"Z").await.unwrap();
    copy.release(writer).await.unwrap();
    let reader = copy.open(Access::Read).await.unwrap();
    assert_eq!(copy.read(reader, 0, 11).await.unwrap(), b"ABCDEFGH\0\0Z");
}

struct BadCountStore {
    count: usize,
    calls: AtomicUsize,
}
impl SparseStore for BadCountStore {
    fn read<'a>(&'a self, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn write<'a>(&'a self, _: u64, _: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.count)
        })
    }
}
#[tokio::test]
async fn invalid_copy_identities_and_zero_or_oversized_counts_never_return_overlay() {
    let (_, base, gate, snapshot) = image().await;
    let budget = Arc::new(StorageBudget::new(100));
    let store = Arc::new(BadCountStore {
        count: 8,
        calls: AtomicUsize::new(0),
    });
    for (old, new) in [(0, 2), (1, 0), (1, 1)] {
        let wrapped: Arc<dyn SparseStore> =
            Arc::new(BudgetedStore::new(store.clone(), budget.clone()));
        assert!(matches!(
            OverlayFile::copy_private(
                &snapshot,
                old,
                new,
                base.clone(),
                gate.clone(),
                wrapped,
                Limits::default()
            )
            .await,
            Err(Error::InvalidInput)
        ));
    }
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    assert_eq!(budget.reserved_bytes(), 0);
    for count in [0, 9] {
        let store = Arc::new(BadCountStore {
            count,
            calls: AtomicUsize::new(0),
        });
        let budget = Arc::new(StorageBudget::new(100));
        let wrapped: Arc<dyn SparseStore> =
            Arc::new(BudgetedStore::new(store.clone(), budget.clone()));
        assert!(matches!(
            OverlayFile::copy_private(
                &snapshot,
                1,
                2,
                base.clone(),
                gate.clone(),
                wrapped,
                Limits::default()
            )
            .await,
            Err(Error::Io)
        ));
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert_eq!(budget.reserved_bytes(), 8);
    }
}

#[tokio::test]
async fn fresh_copy_compacts_storage_but_preserves_sparse_base_and_old_readers() {
    let (source, base, _, gate) = fixture(Limits::default());
    let writer = source.open(Access::Write).await.unwrap();
    source.write(writer, 8, b"discarded").await.unwrap();
    source.truncate(writer, 4).await.unwrap();
    source.truncate(writer, 10).await.unwrap();
    source.write(writer, 6, b"XY").await.unwrap();
    source.release(writer).await.unwrap();
    let old_reader = source.open(Access::Read).await.unwrap();
    let snapshot = source.checkpoint_guard().await.capture(100).await.unwrap();
    assert_eq!(snapshot.base_end, 4);
    assert_eq!(snapshot.extents[0].stored, 9);
    let reads = base.reads.load(Ordering::SeqCst);
    let store = Arc::new(Store::default());
    let budget = Arc::new(StorageBudget::new(100));
    let copy = OverlayFile::copy_private(
        &snapshot,
        7,
        8,
        base.clone(),
        gate,
        budgeted(store.clone(), budget.clone()),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        base.reads.load(Ordering::SeqCst),
        reads,
        "copy must not hydrate immutable holes"
    );
    assert_eq!(copy.node, snapshot.node);
    assert_eq!(copy.metadata().await.unwrap().reserved_storage_bytes, 2);
    assert_eq!(budget.reserved_bytes(), 2);
    assert_eq!(*store.bytes.lock().await, b"XY");
    let writer = copy.open(Access::Write).await.unwrap();
    copy.write(writer, 0, b"ZZ").await.unwrap();
    copy.release(writer).await.unwrap();
    let reader = copy.open(Access::Read).await.unwrap();
    assert_eq!(copy.read(reader, 0, 10).await.unwrap(), b"ZZcd\0\0XY\0\0");
    assert_eq!(
        source.read(old_reader, 0, 10).await.unwrap(),
        b"abcd\0\0XY\0\0"
    );
    assert_eq!(
        source.checkpoint_guard().await.capture(100).await.unwrap(),
        snapshot
    );
}

#[tokio::test]
async fn short_copy_reads_span_split_extents_and_later_appends_keep_retry_gaps() {
    let (_, base, gate, snapshot) = image().await;
    let store = Arc::new(Store::default());
    store.short.store(2, Ordering::SeqCst);
    let budget = Arc::new(StorageBudget::new(100));
    let copy = OverlayFile::copy_private(
        &snapshot,
        1,
        2,
        base,
        gate,
        budgeted(store.clone(), budget.clone()),
        Limits::default(),
    )
    .await
    .unwrap();
    let copied = copy.checkpoint_guard().await.capture(100).await.unwrap();
    assert_eq!(
        copied
            .extents
            .iter()
            .map(|extent| extent.stored)
            .collect::<Vec<_>>(),
        vec![0, 8, 14, 18]
    );
    assert_eq!(copied.metadata.reserved_storage_bytes, 20);
    assert_eq!(copied.stored_bytes(), 8);
    assert_eq!(budget.reserved_bytes(), 20);
    let reader = copy.open(Access::Read).await.unwrap();
    assert_eq!(copy.read(reader, 0, 8).await.unwrap(), b"ABCDEFGH");
    let before = store.bytes.lock().await.clone();
    store.short.store(0, Ordering::SeqCst);
    let writer = copy.open(Access::Write).await.unwrap();
    copy.write(writer, 8, b"IJ").await.unwrap();
    copy.release(writer).await.unwrap();
    assert_eq!(copy.read(reader, 0, 10).await.unwrap(), b"ABCDEFGHIJ");
    assert_eq!(&store.bytes.lock().await[..before.len()], before.as_slice());
    assert_eq!(budget.reserved_bytes(), 22);
}

#[tokio::test]
async fn copy_limits_refuse_before_next_attempt_and_keep_consumed_shared_debt() {
    for limit in [0, 1, 2] {
        let (source, base, gate, snapshot) = image().await;
        assert_eq!(snapshot.extents.len(), 1);
        let store = Arc::new(Store::default());
        store
            .short
            .store(if limit == 0 { 1 } else { 2 }, Ordering::SeqCst);
        let budget = Arc::new(StorageBudget::new(if limit == 2 { 10 } else { 100 }));
        let limits = Limits {
            max_extents: if limit == 0 { 2 } else { 100 },
            max_storage_bytes: if limit == 1 { 10 } else { 100 },
            ..Limits::default()
        };
        assert!(matches!(
            OverlayFile::copy_private(
                &snapshot,
                1,
                2,
                base,
                gate,
                budgeted(store.clone(), budget.clone()),
                limits
            )
            .await,
            Err(Error::ResourceLimit)
        ));
        assert_eq!(budget.reserved_bytes(), if limit == 0 { 15 } else { 8 });
        assert_eq!(
            store.bytes.lock().await.len() as u64,
            budget.reserved_bytes()
        );
        assert_eq!(
            source.checkpoint_guard().await.capture(100).await.unwrap(),
            snapshot
        );
    }
}

#[tokio::test]
async fn shared_pool_adds_copy_requests_to_existing_source_debt() {
    for limit in [15, 16] {
        let (template, base, store, gate) = fixture(Limits::default());
        let budget = Arc::new(StorageBudget::new(limit));
        let source = OverlayFile::new(
            template.node.clone(),
            base.clone(),
            gate.clone(),
            budgeted(store, budget.clone()),
            Limits::default(),
        )
        .unwrap();
        let writer = source.open(Access::Write).await.unwrap();
        source.write(writer, 0, b"ABCDEFGH").await.unwrap();
        source.release(writer).await.unwrap();
        let snapshot = source.checkpoint_guard().await.capture(100).await.unwrap();
        assert_eq!(budget.reserved_bytes(), 8);
        let destination = Arc::new(Store::default());
        let result = OverlayFile::copy_private(
            &snapshot,
            1,
            2,
            base,
            gate,
            budgeted(destination.clone(), budget.clone()),
            Limits::default(),
        )
        .await;
        if limit == 15 {
            assert!(matches!(result, Err(Error::ResourceLimit)));
            assert_eq!(budget.reserved_bytes(), 8);
            assert!(destination.bytes.lock().await.is_empty());
        } else {
            assert_eq!(
                result
                    .unwrap()
                    .metadata()
                    .await
                    .unwrap()
                    .reserved_storage_bytes,
                8
            );
            assert_eq!(budget.reserved_bytes(), 16);
        }
        assert_eq!(
            source.checkpoint_guard().await.capture(100).await.unwrap(),
            snapshot
        );
    }
}

#[tokio::test]
async fn cancelled_or_revoked_copy_never_returns_an_overlay_and_does_not_refund() {
    for revoke in [false, true] {
        let (source, base, gate, snapshot) = image().await;
        let store = Arc::new(Store::default());
        store.blocked.store(true, Ordering::SeqCst);
        let budget = Arc::new(StorageBudget::new(100));
        let mut pending = Box::pin(OverlayFile::copy_private(
            &snapshot,
            1,
            2,
            base,
            gate.clone(),
            budgeted(store.clone(), budget.clone()),
            Limits::default(),
        ));
        tokio::select! {
            _=store.started.notified()=>{},
            result=&mut pending=>panic!("copy did not await private IO: {}", result.is_ok()),
            _=tokio::time::sleep(std::time::Duration::from_secs(10))=>panic!("copy IO never started"),
        }
        assert_eq!(budget.reserved_bytes(), 8);
        if revoke {
            gate.deny.store(true, Ordering::SeqCst);
            store.resume.notify_one();
            assert!(matches!(pending.await, Err(Error::PermissionDenied)));
            assert_eq!(store.bytes.lock().await.len(), 8);
            gate.deny.store(false, Ordering::SeqCst);
        } else {
            drop(pending);
        }
        assert_eq!(budget.reserved_bytes(), 8);
        assert_eq!(
            source.checkpoint_guard().await.capture(100).await.unwrap(),
            snapshot
        );
    }
}

struct ProofBase {
    base: Arc<Base>,
    anchor: NodeKey,
    proof: CommittedProof,
    checks: AtomicUsize,
}
impl Backend for ProofBase {
    fn lookup<'a>(&'a self, key: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        self.base.lookup(key, name)
    }
    fn list<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        self.base.list(key, offset, limit)
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        self.base.read(key, offset, size)
    }
    fn digest<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        self.base.digest(key)
    }
    fn validate_committed<'a>(
        &'a self,
        anchor: &'a NodeKey,
        proof: &'a CommittedProof,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.checks.fetch_add(1, Ordering::SeqCst);
            if *anchor != self.anchor || *proof != self.proof {
                Err(Error::InvalidInput)
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn receipt_is_validated_and_reissued_in_one_fresh_shared_cell_for_local_anchor() {
    let (template, base, store, gate) = fixture(Limits::default());
    let anchor = NodeKey {
        node: 0,
        ..template.node.key
    };
    let source = OverlayFile::empty(
        anchor,
        0o644,
        base.clone(),
        gate.clone(),
        store,
        Limits::default(),
    )
    .unwrap();
    let writer = source.open(Access::Write).await.unwrap();
    source.write(writer, 0, b"ABCDEFGH").await.unwrap();
    source.release(writer).await.unwrap();
    let metadata = source.metadata().await.unwrap();
    let proof = CommittedProof {
        node: Node {
            key: NodeKey { node: 33, ..anchor },
            kind: NodeKind::File,
            size: 8,
            mode: 0o444,
        },
        address: Address::default(),
    };
    let receipt = Receipt {
        inode: 7,
        authority: anchor,
        generation: metadata.generation,
        mode: metadata.mode,
        proof: proof.clone(),
        sha256: None,
    };
    *receipt_lock(&source.receipt).unwrap() = Some(receipt.clone());
    let snapshot = source.checkpoint_guard().await.capture(100).await.unwrap();
    let backend = Arc::new(ProofBase {
        base,
        anchor,
        proof,
        checks: AtomicUsize::new(0),
    });
    let store = Arc::new(Store::default());
    let budget = Arc::new(StorageBudget::new(100));
    assert!(matches!(
        OverlayFile::copy_private(
            &snapshot,
            8,
            99,
            backend.clone(),
            gate.clone(),
            budgeted(store.clone(), budget.clone()),
            Limits::default()
        )
        .await,
        Err(Error::InvalidInput)
    ));
    assert_eq!(backend.checks.load(Ordering::SeqCst), 0);
    assert_eq!(budget.reserved_bytes(), 0);
    let mut forged = snapshot.clone();
    forged.receipt.as_mut().unwrap().proof.address =
        Address::zero_context_hash(lore_base::types::Hash::from_u64(9));
    assert!(matches!(
        OverlayFile::copy_private(
            &forged,
            7,
            99,
            backend.clone(),
            gate.clone(),
            budgeted(store.clone(), budget.clone()),
            Limits::default()
        )
        .await,
        Err(Error::InvalidInput)
    ));
    assert_eq!(budget.reserved_bytes(), 0);
    let copy = OverlayFile::copy_private(
        &snapshot,
        7,
        99,
        backend.clone(),
        gate,
        budgeted(store, budget),
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(backend.checks.load(Ordering::SeqCst), 3);
    let record_cell = copy.receipt_cell();
    assert!(!Arc::ptr_eq(&record_cell, &source.receipt_cell()));
    let reissued = receipt_lock(&record_cell).unwrap().clone().unwrap();
    assert_eq!(reissued.inode, 99);
    assert_eq!(reissued.authority, anchor);
    assert_eq!(reissued.generation, receipt.generation);
    assert_eq!(reissued.proof, receipt.proof);
    let writer = copy.open(Access::Write).await.unwrap();
    assert!(receipt_lock(&record_cell).unwrap().is_none());
    assert_eq!(
        *receipt_lock(&source.receipt_cell()).unwrap(),
        Some(receipt)
    );
    copy.release(writer).await.unwrap();
    assert_eq!(
        source.checkpoint_guard().await.capture(100).await.unwrap(),
        snapshot
    );
}
