// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_storage::Hash;
use lore_storage::KeyType;
use lore_storage::KeyValueStream;
use lore_storage::Partition;
use lore_storage::local::store_lock::StoreGuard;

use super::*;

pub(super) struct CommandMutable {
    pub(super) inner: Arc<dyn MutableStore>,
    pub(super) load: Arc<BoundLoad>,
}
#[async_trait::async_trait]
impl MutableStore for CommandMutable {
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
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().load(partition, key, key_type).await })
            .await
    }
    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .store(partition, key, value, key_type)
                    .await
            })
            .await
    }
    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError> {
        operation(&self.load)?
            .scope(async move {
                self.inner
                    .clone()
                    .compare_and_swap(partition, key, expected, value, key_type)
                    .await
            })
            .await
    }
    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().list(partition, key_type).await })
            .await
    }
    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
        operation(&self.load)?
            .scope(async move { self.inner.clone().flush(sync_data).await })
            .await
    }
}
