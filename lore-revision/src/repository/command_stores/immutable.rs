// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

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

pub(super) struct CommandImmutable {
    pub(super) inner: Arc<dyn ImmutableStore>,
    pub(super) load: Arc<BoundLoad>,
}
#[async_trait::async_trait]
impl ImmutableStore for CommandImmutable {
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }
    async fn hold_for_command(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().hold_for_command().await })
            .await
    }
    fn try_join_claim(&self) -> Option<StoreGuard> {
        operation(&self.load)
            .ok()?
            .sync_scope(|| self.inner.try_join_claim())
    }
    fn store_lock(&self) -> Option<Arc<StoreLock>> {
        self.inner.store_lock()
    }
    async fn hold_for_task(self: Arc<Self>) -> Result<Option<StoreGuard>, StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().hold_for_task().await })
            .await
    }
    fn isolates_partitions(&self) -> bool {
        self.inner.isolates_partitions()
    }
    fn read_scope(&self) -> StoreMatch {
        self.inner.read_scope()
    }
    fn query_scope(&self) -> StoreMatch {
        self.inner.query_scope()
    }
    async fn get(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().get(partition, address).await })
            .await
    }
    async fn get_into(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        dst: &mut CallerBuffer,
    ) -> Result<(Fragment, PayloadRead), StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().get_into(partition, address, dst).await })
            .await
    }
    async fn is_available(self: Arc<Self>, timeout: Duration) -> bool {
        let Ok(scope) = operation(&self.load) else {
            return false;
        };
        scope
            .scope(async move { self.inner.clone().is_available(timeout).await })
            .await
    }
    async fn query(
        self: Arc<Self>,
        partition: Partition,
        addresses: &[Address],
        results: &mut [StoreMatchResult],
    ) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .query(partition, addresses, results)
                    .await
            })
            .await
    }
    async fn get_metadata(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
    ) -> Result<StoreGetData, StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().get_metadata(partition, address).await })
            .await
    }
    async fn put(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
        force: bool,
    ) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .put(partition, address, fragment, payload, force)
                    .await
            })
            .await
    }
    async fn obliterate(
        self: Arc<Self>,
        partition: Partition,
        address: Address,
        stats: Arc<StoreObliterateStats>,
    ) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .obliterate(partition, address, stats)
                    .await
            })
            .await
    }
    async fn evict(
        self: Arc<Self>,
        max_capacity: usize,
        sync_data: bool,
        sink: Option<GcEventSinkRef>,
    ) -> Result<usize, StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .evict(max_capacity, sync_data, sink)
                    .await
            })
            .await
    }
    async fn compact(
        self: Arc<Self>,
        max_size: usize,
        at: Option<usize>,
        sync_data: bool,
        sink: Option<GcEventSinkRef>,
    ) -> Result<Option<usize>, StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .compact(max_size, at, sync_data, sink)
                    .await
            })
            .await
    }
    async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
        let scope = operation(&self.load).ok()?;
        scope
            .scope(async move { self.inner.clone().compact_resume_at().await })
            .await
    }
    async fn load_index(self: Arc<Self>) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().load_index().await })
            .await
    }
    async fn stop_gc(self: Arc<Self>, terminate: bool) {
        if let Ok(scope) = operation(&self.load) {
            scope
                .scope(async move { self.inner.clone().stop_gc(terminate).await })
                .await;
        }
    }
    fn max_query_batch(&self) -> Option<usize> {
        self.inner.max_query_batch()
    }
    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().flush(sync_data).await })
            .await
    }
    async fn fragment_count(self: Arc<Self>) -> Option<usize> {
        let scope = operation(&self.load).ok()?;
        scope
            .scope(async move { self.inner.clone().fragment_count().await })
            .await
    }
    async fn verify(self: Arc<Self>, heal: bool) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().verify(heal).await })
            .await
    }
    async fn copy(
        self: Arc<Self>,
        source_partition: Partition,
        source_address: Address,
        destination_partition: Partition,
        destination_context: Context,
        behavior: CopyBehavior,
    ) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move {
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
            })
            .await
    }
}
