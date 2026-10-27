// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Shared append-storage accounting for a mutable mount's private stores.
//!
//! Wrap every newly created or restored file store with the same budget. A write
//! reserves its entire request before entering the underlying store. Failed,
//! short and cancelled writes retain that reservation because the store may have
//! consumed disk space even without publishing an overlay extent. Reservations
//! are not reclaimed on truncate, unlink or handle release; reclaim requires a
//! separate owner-controlled compaction that also disposes of the old stores.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::core::BackendFuture;
use crate::core::Error;
use crate::core::MAX_READ_SIZE;
use crate::core::Result;
use crate::overlay::SparseStore;

/// One bounded pool for all append requests over a storage owner's lifetime.
/// The limit counts requested bytes, not filesystem allocation granularity.
pub struct StorageBudget {
    limit: u64,
    reserved: AtomicU64,
}

impl StorageBudget {
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: AtomicU64::new(0),
        }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }

    /// Retain at least this many historical reservations when restoring an owner.
    /// Call before admitting new mount operations, after restoring into fresh
    /// budgeted stores. Their writes are already charged, so this establishes the
    /// maximum of restore charges and persisted reservations without double-counting.
    /// This is a monotonic floor, not an additive reservation for another owner.
    pub fn account_existing(&self, minimum: u64) -> Result<()> {
        if minimum > self.limit {
            return Err(Error::ResourceLimit);
        }
        self.reserved.fetch_max(minimum, Ordering::AcqRel);
        Ok(())
    }

    fn reserve(&self, count: u64) -> Result<()> {
        self.reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(count).filter(|next| *next <= self.limit)
            })
            .map(|_| ())
            .map_err(|_full| Error::ResourceLimit)
    }
}

/// A private store charged to a shared mount-wide budget. Keep the underlying
/// store private: writes through an unwrapped alias bypass this accounting.
pub struct BudgetedStore {
    inner: Arc<dyn SparseStore>,
    budget: Arc<StorageBudget>,
}

impl BudgetedStore {
    pub fn new(inner: Arc<dyn SparseStore>, budget: Arc<StorageBudget>) -> Self {
        Self { inner, budget }
    }
}

impl SparseStore for BudgetedStore {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        self.inner.read(offset, size)
    }

    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            if bytes.len() > MAX_READ_SIZE || offset.checked_add(bytes.len() as u64).is_none() {
                return Err(Error::InvalidInput);
            }
            self.budget.reserve(bytes.len() as u64)?;
            let count = self.inner.write(offset, bytes).await?;
            if count > bytes.len() {
                return Err(Error::Io);
            }
            Ok(count)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::Notify;

    use super::*;

    struct Store {
        result: Result<usize>,
        writes: AtomicUsize,
        entered: Option<Arc<Notify>>,
    }

    impl SparseStore for Store {
        fn read<'a>(&'a self, _: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
            Box::pin(async move { Ok(vec![7; size]) })
        }

        fn write<'a>(&'a self, _: u64, _: &'a [u8]) -> BackendFuture<'a, usize> {
            Box::pin(async move {
                self.writes.fetch_add(1, Ordering::SeqCst);
                if let Some(entered) = &self.entered {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                }
                self.result
            })
        }
    }

    fn store(result: Result<usize>) -> Arc<Store> {
        Arc::new(Store {
            result,
            writes: AtomicUsize::new(0),
            entered: None,
        })
    }

    #[tokio::test]
    async fn recovery_retains_historical_reservations_without_double_charging() {
        let budget = Arc::new(StorageBudget::new(10));
        let file = BudgetedStore::new(store(Ok(3)), budget.clone());
        assert_eq!(file.write(0, b"old").await, Ok(3));
        budget.account_existing(8).unwrap();
        assert_eq!(budget.reserved_bytes(), 8);
        budget.account_existing(2).unwrap();
        assert_eq!(budget.reserved_bytes(), 8);
        assert_eq!(budget.account_existing(11), Err(Error::ResourceLimit));
        assert_eq!(budget.reserved_bytes(), 8);
        assert_eq!(file.write(3, b"new").await, Err(Error::ResourceLimit));
        budget.account_existing(10).unwrap();
        assert_eq!(file.write(3, b"x").await, Err(Error::ResourceLimit));
    }

    #[tokio::test]
    async fn failed_short_and_cancelled_writes_consume_shared_budget() {
        let budget = Arc::new(StorageBudget::new(12));
        let failed = BudgetedStore::new(store(Err(Error::Io)), budget.clone());
        assert_eq!(failed.write(0, b"fail").await, Err(Error::Io));
        let short = BudgetedStore::new(store(Ok(1)), budget.clone());
        assert_eq!(short.write(0, b"part").await, Ok(1));
        let entered = Arc::new(Notify::new());
        let blocked = Arc::new(BudgetedStore::new(
            Arc::new(Store {
                result: Ok(4),
                writes: AtomicUsize::new(0),
                entered: Some(entered.clone()),
            }),
            budget.clone(),
        ));
        // This store-only fixture has no repository execution context.
        #[allow(clippy::disallowed_methods)]
        let task = tokio::spawn(async move { blocked.write(0, b"wait").await });
        entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(budget.reserved_bytes(), 12);
        assert_eq!(short.write(4, b"x").await, Err(Error::ResourceLimit));
        assert_eq!(short.read(0, 3).await.unwrap(), vec![7; 3]);
        assert_eq!(budget.reserved_bytes(), 12);
        drop((failed, short));
        assert_eq!(budget.reserved_bytes(), 12);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_files_cannot_overbook_or_enter_store_after_limit() {
        let budget = Arc::new(StorageBudget::new(17));
        let inner = store(Ok(1));
        let mut tasks = Vec::new();
        for _ in 0..64 {
            let file = BudgetedStore::new(inner.clone(), budget.clone());
            // This store-only fixture has no repository execution context.
            #[allow(clippy::disallowed_methods)]
            let task = tokio::spawn(async move { file.write(0, b"x").await });
            tasks.push(task);
        }
        let mut accepted = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(1) => accepted += 1,
                Err(Error::ResourceLimit) => {}
                other => panic!("unexpected write result: {other:?}"),
            }
        }
        assert_eq!(accepted, 17);
        assert_eq!(inner.writes.load(Ordering::SeqCst), 17);
        assert_eq!(budget.reserved_bytes(), budget.limit());
    }

    #[tokio::test]
    async fn invalid_ranges_and_unpolled_requests_do_not_reserve() {
        let budget = Arc::new(StorageBudget::new(2));
        let inner = store(Ok(1));
        let file = BudgetedStore::new(inner.clone(), budget.clone());
        drop(file.write(0, b"x"));
        assert_eq!(file.write(u64::MAX, b"x").await, Err(Error::InvalidInput));
        assert_eq!(
            file.write(0, &vec![0; MAX_READ_SIZE + 1]).await,
            Err(Error::InvalidInput)
        );
        assert_eq!(budget.reserved_bytes(), 0);
        assert_eq!(inner.writes.load(Ordering::SeqCst), 0);
        assert_eq!(file.write(0, b"x").await, Ok(1));
    }
}
