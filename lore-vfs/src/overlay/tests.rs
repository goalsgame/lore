// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use lore_base::types::Address;
use lore_base::types::RepositoryId;

use super::*;
use crate::core::BackendPage;

#[path = "tests/private_copy.rs"]
mod private_copy;

struct Base {
    bytes: Vec<u8>,
    reads: AtomicUsize,
    fail: AtomicBool,
}

impl Backend for Base {
    fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
        Box::pin(async { Err(Error::Unsupported) })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Io);
            }
            Ok(self.bytes[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Ok(Some([42; 32])) })
    }
}

#[derive(Default)]
struct Gate {
    deny: AtomicBool,
    seen: std::sync::Mutex<Vec<(NodeKey, Access)>>,
}
impl AccessGate for Gate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().unwrap().push((*key, access));
            if self.deny.load(Ordering::SeqCst) {
                Err(Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}

#[derive(Default)]
struct Store {
    bytes: Mutex<Vec<u8>>,
    short: AtomicUsize,
    fail: AtomicBool,
    malformed_read: AtomicBool,
    blocked: AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl SparseStore for Store {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Io);
            }
            if self.malformed_read.load(Ordering::SeqCst) {
                return Ok(Vec::new());
            }
            Ok(self.bytes.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            if self.blocked.load(Ordering::SeqCst) {
                self.started.notify_one();
                self.resume.notified().await;
            }
            // Deliberately write before reporting failure to model an uncertain I/O result.
            let mut storage = self.bytes.lock().await;
            storage.resize(offset as usize + bytes.len(), 0);
            storage[offset as usize..].copy_from_slice(bytes);
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::Io);
            }
            let short = self.short.load(Ordering::SeqCst);
            Ok(if short == 0 {
                bytes.len()
            } else {
                short.min(bytes.len())
            })
        })
    }
}

fn fixture(limits: Limits) -> (Arc<OverlayFile>, Arc<Base>, Arc<Store>, Arc<Gate>) {
    let base = Arc::new(Base {
        bytes: b"abcdefgh".to_vec(),
        reads: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let store = Arc::new(Store::default());
    let gate = Arc::new(Gate::default());
    let node = Node {
        key: NodeKey {
            repository: RepositoryId::from([7; 16]),
            revision: Address::default(),
            node: 12,
            exposure: 19,
        },
        kind: NodeKind::File,
        size: 8,
        mode: 0o644,
    };
    let file = Arc::new(
        OverlayFile::new(node, base.clone(), gate.clone(), store.clone(), limits).unwrap(),
    );
    (file, base, store, gate)
}

#[tokio::test]
async fn writable_admission_revokes_digest_and_preserves_shared_identity() {
    let (file, base, _, gate) = fixture(Limits::default());
    let reader = file.open(Access::Read).await.unwrap();
    assert_eq!(file.digest().await.unwrap(), Some([42; 32]));
    let writer = file.open(Access::Write).await.unwrap();
    assert_eq!(file.digest().await.unwrap(), None);
    assert_eq!(base.reads.load(Ordering::SeqCst), 0);
    file.write(writer, 0, b"ABCDEFGH").await.unwrap();
    let alias = file.clone();
    assert_eq!(alias.read(reader, 0, 8).await.unwrap(), b"ABCDEFGH");
    assert_eq!(base.reads.load(Ordering::SeqCst), 0);
    file.set_executable(writer, true).await.unwrap();
    assert_eq!(file.metadata().await.unwrap().mode, 0o755);
    file.set_executable(writer, false).await.unwrap();
    assert_eq!(file.metadata().await.unwrap().mode, 0o644);
    file.release(writer).await.unwrap();
    assert_eq!(file.metadata().await.unwrap().writers, 0);
    assert_eq!(file.digest().await.unwrap(), None);
    assert_eq!(file.write(writer, 0, b"x").await, Err(Error::BadHandle));
    assert_eq!(
        file.write(reader, 0, b"x").await,
        Err(Error::PermissionDenied)
    );
    assert!(
        gate.seen
            .lock()
            .unwrap()
            .iter()
            .all(|(key, _)| *key == file.key())
    );
}

#[tokio::test]
async fn sparse_extension_overlap_and_truncation_never_resurrect_base_bytes() {
    let (file, _, _, _) = fixture(Limits::default());
    let h = file.open(Access::Write).await.unwrap();
    file.write(h, 2, b"1234").await.unwrap();
    file.write(h, 3, b"XY").await.unwrap();
    assert_eq!(file.read(h, 0, 8).await.unwrap(), b"ab1XY4gh");
    file.write(h, 12, b"Z").await.unwrap();
    assert_eq!(file.read(h, 6, 99).await.unwrap(), b"gh\0\0\0\0Z");
    file.truncate(h, 4).await.unwrap();
    file.truncate(h, 16).await.unwrap();
    assert_eq!(
        file.read(h, 0, 16).await.unwrap(),
        b"ab1X\0\0\0\0\0\0\0\0\0\0\0\0"
    );
    assert!(file.read(h, u64::MAX, 5).await.unwrap().is_empty());
    assert_eq!(
        file.read(h, 0, MAX_READ_SIZE + 1).await,
        Err(Error::InvalidInput)
    );
    assert_eq!(
        file.write(h, u64::MAX, b"xx").await,
        Err(Error::InvalidInput)
    );
    file.truncate(h, 0).await.unwrap();
    file.truncate(h, 8).await.unwrap();
    assert_eq!(file.read(h, 0, 8).await.unwrap(), [0; 8]);
}

#[tokio::test]
async fn io_failure_and_short_writes_preserve_the_acknowledged_view() {
    let (file, base, store, _) = fixture(Limits::default());
    let h = file.open(Access::Write).await.unwrap();
    file.write(h, 2, b"WXYZ").await.unwrap();
    store.fail.store(true, Ordering::SeqCst);
    assert_eq!(file.write(h, 1, b"bad").await, Err(Error::Io));
    store.fail.store(false, Ordering::SeqCst);
    assert_eq!(file.read(h, 0, 8).await.unwrap(), b"abWXYZgh");
    store.short.store(2, Ordering::SeqCst);
    assert_eq!(file.write(h, 3, b"12345").await.unwrap(), 2);
    assert_eq!(file.read(h, 0, 8).await.unwrap(), b"abW12Zgh");
    assert_eq!(file.metadata().await.unwrap().reserved_storage_bytes, 12);
    store.malformed_read.store(true, Ordering::SeqCst);
    assert_eq!(file.read(h, 2, 1).await, Err(Error::Io));
    base.fail.store(true, Ordering::SeqCst);
    assert_eq!(file.read(h, 0, 1).await, Err(Error::Io));
}

#[tokio::test]
async fn resources_and_revoked_owner_access_are_enforced() {
    let (file, _, _, gate) = fixture(Limits {
        max_handles: 1,
        max_storage_bytes: 4,
        max_extents: 2,
        max_file_size: 16,
    });
    let h = file.open(Access::Write).await.unwrap();
    assert_eq!(file.open(Access::Read).await, Err(Error::ResourceLimit));
    file.write(h, 0, b"1234").await.unwrap();
    assert_eq!(file.write(h, 0, b"x").await, Err(Error::ResourceLimit));
    assert_eq!(file.truncate(h, 17).await, Err(Error::ResourceLimit));
    gate.deny.store(true, Ordering::SeqCst);
    assert_eq!(file.read(h, 0, 4).await, Err(Error::PermissionDenied));
    assert_eq!(file.write(h, 0, b"x").await, Err(Error::PermissionDenied));
    assert_eq!(file.truncate(h, 1).await, Err(Error::PermissionDenied));
    assert_eq!(file.digest().await, Err(Error::PermissionDenied));
    file.release(h).await.unwrap();
    gate.deny.store(false, Ordering::SeqCst);
    let next = file.open(Access::Read).await.unwrap();
    assert_ne!(next, h);
    assert_eq!(file.read(h, 0, 1).await, Err(Error::BadHandle));
    assert_eq!(file.release(h).await, Err(Error::BadHandle));
}

#[tokio::test]
async fn concurrent_writers_serialize_and_cancelled_appends_are_not_reused() {
    let (file, _, store, _) = fixture(Limits::default());
    let first = file.open(Access::Write).await.unwrap();
    let second = file.open(Access::Write).await.unwrap();
    store.blocked.store(true, Ordering::SeqCst);
    // This fixture has no Lore execution context; tasks only use the fake backend.
    #[allow(clippy::disallowed_methods)]
    let writer = {
        let file = file.clone();
        tokio::spawn(async move { file.write(first, 0, b"old").await })
    };
    store.started.notified().await;
    let later = file.write(second, 1, b"new");
    tokio::pin!(later);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut later)
            .await
            .is_err()
    );
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    store.blocked.store(false, Ordering::SeqCst);
    later.await.unwrap();
    assert_eq!(file.read(first, 0, 8).await.unwrap(), b"anewefgh");
    assert_eq!(file.metadata().await.unwrap().reserved_storage_bytes, 6);
}

#[tokio::test]
async fn external_file_store_is_exclusive_and_serves_sparse_overlay_bytes() {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "lore-overlay-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let store = Arc::new(FileStore::create(&path).await.unwrap());
    assert!(matches!(FileStore::create(&path).await, Err(Error::Io)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            tokio::fs::metadata(&path)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let (_, base, _, gate) = fixture(Limits::default());
    let node = Node {
        key: NodeKey {
            repository: RepositoryId::default(),
            revision: Address::default(),
            node: 1,
            exposure: 1,
        },
        kind: NodeKind::File,
        size: 8,
        mode: 0o644,
    };
    let file =
        OverlayFile::new(node, base.clone(), gate, store.clone(), Limits::default()).unwrap();
    let h = file.open(Access::Write).await.unwrap();
    file.write(h, 1 << 30, b"external").await.unwrap();
    assert_eq!(
        file.read(h, (1 << 30) - 2, 20).await.unwrap(),
        b"\0\0external"
    );
    assert_eq!(tokio::fs::metadata(&path).await.unwrap().len(), 8);
    assert_eq!(base.reads.load(Ordering::SeqCst), 0);
    drop(file);
    drop(store);
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn repeated_overwrites_and_truncates_match_a_byte_model() {
    let (file, _, _, _) = fixture(Limits::default());
    let h = file.open(Access::Write).await.unwrap();
    let mut expected = b"abcdefgh".to_vec();
    let mut random = 13u64;
    for step in 0..400 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let offset = ((random >> 32) % 100) as usize;
        if step % 7 == 0 {
            file.truncate(h, offset as u64).await.unwrap();
            expected.resize(offset, 0);
        } else {
            let bytes = [step as u8; 9];
            file.write(h, offset as u64, &bytes).await.unwrap();
            expected.resize(expected.len().max(offset + bytes.len()), 0);
            expected[offset..offset + bytes.len()].copy_from_slice(&bytes);
        }
        assert_eq!(file.read(h, 0, 200).await.unwrap(), expected, "step {step}");
    }
}

#[tokio::test]
async fn concurrent_successful_writers_are_ordered_and_visible_to_old_handles() {
    let (file, _, store, _) = fixture(Limits::default());
    let a = file.open(Access::Write).await.unwrap();
    let b = file.open(Access::Write).await.unwrap();
    store.blocked.store(true, Ordering::SeqCst);
    // Fake backends require no Lore task context.
    #[allow(clippy::disallowed_methods)]
    let writer = {
        let file = file.clone();
        tokio::spawn(async move { file.write(a, 0, b"1111").await })
    };
    store.started.notified().await;
    let later = file.write(b, 2, b"2222");
    tokio::pin!(later);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut later)
            .await
            .is_err()
    );
    store.blocked.store(false, Ordering::SeqCst);
    store.resume.notify_one();
    assert_eq!(writer.await.unwrap().unwrap(), 4);
    assert_eq!(later.await.unwrap(), 4);
    assert_eq!(file.read(a, 0, 8).await.unwrap(), b"112222gh");
    assert_eq!(file.metadata().await.unwrap().writers, 2);
    assert!(file.metadata().await.unwrap().generation >= 4);
}

#[tokio::test]
async fn extent_limit_rejects_before_touching_external_storage() {
    let (file, _, _, _) = fixture(Limits {
        max_extents: 1,
        ..Limits::default()
    });
    let h = file.open(Access::Write).await.unwrap();
    file.write(h, 2, b"one").await.unwrap();
    assert_eq!(file.write(h, 3, b"two").await, Err(Error::ResourceLimit));
    assert_eq!(file.metadata().await.unwrap().reserved_storage_bytes, 3);
    assert_eq!(file.read(h, 0, 8).await.unwrap(), b"abonefgh");
}

#[tokio::test]
async fn rejected_writable_admission_does_not_revoke_an_immutable_digest() {
    let (file, _, _, gate) = fixture(Limits::default());
    gate.deny.store(true, Ordering::SeqCst);
    assert_eq!(file.open(Access::Write).await, Err(Error::PermissionDenied));
    gate.deny.store(false, Ordering::SeqCst);
    assert_eq!(file.digest().await.unwrap(), Some([42; 32]));
    let metadata = file.metadata().await.unwrap();
    assert_eq!(metadata.generation, 0);
    assert_eq!(metadata.writers, 0);
    assert!(!metadata.mutable);
}

#[tokio::test]
async fn snapshot_restores_base_holes_budget_and_rechecks_full_identity() {
    let limits = Limits {
        max_storage_bytes: 6,
        ..Limits::default()
    };
    let (file, base, store, gate) = fixture(limits);
    let handle = file.open(Access::Write).await.unwrap();
    file.write(handle, 2, b"XY").await.unwrap();
    file.write(handle, 3, b"Z").await.unwrap();
    store.fail.store(true, Ordering::SeqCst);
    assert_eq!(file.write(handle, 6, b"xx").await, Err(Error::Io));
    store.fail.store(false, Ordering::SeqCst);
    let guard = file.checkpoint_guard().await;
    assert!(matches!(guard.capture(1).await, Err(Error::ResourceLimit)));
    store.malformed_read.store(true, Ordering::SeqCst);
    assert!(matches!(guard.capture(8).await, Err(Error::Io)));
    store.malformed_read.store(false, Ordering::SeqCst);
    let snapshot = guard.capture(8).await.unwrap();
    assert_eq!(snapshot.stored_bytes(), 2);
    assert_eq!(snapshot.metadata.reserved_storage_bytes, 5);
    assert_eq!(snapshot.metadata.writers, 0);
    drop(guard);
    let fresh = Arc::new(Store::default());
    let restored =
        OverlayFile::restore(snapshot.clone(), base.clone(), gate.clone(), fresh, limits)
            .await
            .unwrap();
    assert_eq!(restored.key(), file.key());
    let handle = restored.open(Access::Write).await.unwrap();
    assert_eq!(restored.read(handle, 0, 8).await.unwrap(), b"abXZefgh");
    assert_eq!(
        restored.write(handle, 0, b"xx").await,
        Err(Error::ResourceLimit)
    );
    assert_eq!(restored.digest().await.unwrap(), None);
    gate.deny.store(true, Ordering::SeqCst);
    let fresh = Arc::new(Store::default());
    assert!(matches!(
        OverlayFile::restore(snapshot, base, gate, fresh.clone(), limits).await,
        Err(Error::PermissionDenied)
    ));
    assert!(fresh.bytes.lock().await.is_empty());
}

#[tokio::test]
async fn malformed_snapshot_rejects_overlapping_ranges_before_writing_storage() {
    let (file, base, _, gate) = fixture(Limits::default());
    let handle = file.open(Access::Write).await.unwrap();
    file.write(handle, 0, b"X").await.unwrap();
    file.write(handle, 3, b"Y").await.unwrap();
    let guard = file.checkpoint_guard().await;
    let mut snapshot = guard.capture(8).await.unwrap();
    snapshot.extents[1].stored = snapshot.extents[0].stored;
    let store = Arc::new(Store::default());
    assert!(matches!(
        OverlayFile::restore(snapshot, base, gate, store.clone(), Limits::default()).await,
        Err(Error::InvalidInput)
    ));
    assert!(store.bytes.lock().await.is_empty());
}
