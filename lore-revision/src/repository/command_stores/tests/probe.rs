// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use lore_storage::Address;
use lore_storage::CallerBuffer;
use lore_storage::Context;
use lore_storage::Fragment;
use lore_storage::Partition;
use lore_storage::PayloadRead;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::StoreObliterateStats;
use lore_storage::gc_event::GcEventSinkRef;
use lore_storage::immutable_store::CopyBehavior;
use lore_storage::local::store_lock::StoreGuard;
use lore_storage::local::store_lock::StoreLock;

use super::*;

#[derive(Default)]
pub(super) struct Stats {
    pub(super) calls: parking_lot::Mutex<Vec<&'static str>>,
    pub(super) dropped_under_io: AtomicBool,
    pub(super) flush_pause: Option<Arc<FlushPause>>,
}
#[derive(Default)]
pub(super) struct FlushPause {
    pub(super) reached: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Notify,
}
pub(super) struct ProbeImmutable {
    pub(super) inner: Arc<dyn ImmutableStore>,
    pub(super) stats: Arc<Stats>,
}
impl Drop for ProbeImmutable {
    fn drop(&mut self) {
        self.stats
            .dropped_under_io
            .store(IoLease::current().is_some(), Ordering::Release);
    }
}
#[async_trait::async_trait]
impl ImmutableStore for ProbeImmutable {
    fn is_local(&self) -> bool {
        self.stats.calls.lock().push("is_local");
        self.inner.is_local()
    }
    async fn hold_for_command(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        self.stats.calls.lock().push("hold_for_command");
        self.inner.clone().hold_for_command().await
    }
    fn try_join_claim(&self) -> Option<StoreGuard> {
        self.stats.calls.lock().push("try_join_claim");
        assert!(IoLease::current().is_some());
        self.inner.try_join_claim()
    }
    fn store_lock(&self) -> Option<Arc<StoreLock>> {
        self.stats.calls.lock().push("store_lock");
        self.inner.store_lock()
    }
    async fn hold_for_task(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        self.stats.calls.lock().push("hold_for_task");
        self.inner.clone().hold_for_task().await
    }
    fn isolates_partitions(&self) -> bool {
        self.stats.calls.lock().push("isolates_partitions");
        self.inner.isolates_partitions()
    }
    fn read_scope(&self) -> StoreMatch {
        self.stats.calls.lock().push("read_scope");
        self.inner.read_scope()
    }
    fn query_scope(&self) -> StoreMatch {
        self.stats.calls.lock().push("query_scope");
        self.inner.query_scope()
    }
    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        self.stats.calls.lock().push("get");
        self.inner.clone().get(partition, address).await
    }
    async fn get_into(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        dst: &mut CallerBuffer,
    ) -> Result<(Fragment, PayloadRead), StoreError> {
        self.stats.calls.lock().push("get_into");
        self.inner.clone().get_into(partition, address, dst).await
    }
    async fn is_available(self: Arc<Self>, timeout: Duration) -> bool {
        self.stats.calls.lock().push("is_available");
        self.inner.clone().is_available(timeout).await
    }
    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        self.stats.calls.lock().push("query");
        self.inner
            .clone()
            .query(partition, addresses, results)
            .await
    }
    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        self.stats.calls.lock().push("get_metadata");
        self.inner.clone().get_metadata(partition, address).await
    }
    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
        force: bool,
    ) -> Result<(), StoreError> {
        self.stats.calls.lock().push("put");
        self.inner
            .clone()
            .put(partition, address, fragment, payload, force)
            .await
    }
    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        self.stats.calls.lock().push("obliterate");
        self.inner
            .clone()
            .obliterate(partition, address, stats)
            .await
    }
    async fn evict(
        self: Arc<Self>,
        max_capacity: usize,
        sync_data: bool,
        sink: Option<GcEventSinkRef>,
    ) -> Result<usize, StoreError> {
        self.stats.calls.lock().push("evict");
        self.inner
            .clone()
            .evict(max_capacity, sync_data, sink)
            .await
    }
    async fn compact(
        self: Arc<Self>,
        max_size: usize,
        at: Option<usize>,
        sync_data: bool,
        sink: Option<GcEventSinkRef>,
    ) -> Result<Option<usize>, StoreError> {
        self.stats.calls.lock().push("compact");
        self.inner
            .clone()
            .compact(max_size, at, sync_data, sink)
            .await
    }
    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        self.stats.calls.lock().push("compact_resume_at");
        self.inner.clone().compact_resume_at().await
    }
    async fn load_index(self: Arc<Self>) -> Result<(), StoreError> {
        self.stats.calls.lock().push("load_index");
        self.inner.clone().load_index().await
    }
    async fn stop_gc(self: Arc<Self>, terminate: bool) {
        self.stats.calls.lock().push("stop_gc");
        self.inner.clone().stop_gc(terminate).await;
    }
    fn max_query_batch(&self) -> Option<usize> {
        self.stats.calls.lock().push("max_query_batch");
        self.inner.max_query_batch()
    }
    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        self.stats.calls.lock().push("flush");
        if let Some(pause) = &self.stats.flush_pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
        self.inner.clone().flush(sync_data).await
    }
    async fn fragment_count(self: Arc<Self>) -> Option<usize> {
        self.stats.calls.lock().push("fragment_count");
        self.inner.clone().fragment_count().await
    }
    async fn verify(self: Arc<Self>, heal: bool) -> Result<(), StoreError> {
        self.stats.calls.lock().push("verify");
        self.inner.clone().verify(heal).await
    }
    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        self.stats.calls.lock().push("copy");
        self.inner
            .clone()
            .copy(
                source_partition,
                source_address,
                destination_partition,
                destination_context,
                behavior,
            )
            .await
    }
}
