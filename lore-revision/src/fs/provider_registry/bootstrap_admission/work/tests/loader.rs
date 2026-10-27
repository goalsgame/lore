// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::interface::ExecutionContext;
use crate::repository::RepositoryAccess;
use crate::repository::{self};
use crate::runtime::LORE_CONTEXT;

#[tokio::test]
async fn loaded_writer_releases_canonical_client_after_finite_work_without_periodic_gc() {
    let fixture = fixture();
    let resolver: Arc<dyn ProviderResolver> = fixture.registry.clone();
    let mut execution = ExecutionContext::default();
    execution.set_provider_resolver(&resolver);
    LORE_CONTEXT
        .scope(Arc::new(execution), async {
            let loaded = repository::load_and_connect(
                fixture.location.target().path(),
                RepositoryAccess::ReadWrite,
            )
            .await
            .unwrap();
            loaded
                .try_write_mutable_store()
                .unwrap()
                .flush(true)
                .await
                .unwrap();
            let stripped = loaded.without_command_locks();
            drop(loaded);
            let next = tokio::time::timeout(
                Duration::from_secs(5),
                repository::RepositoryWriteToken::acquire(fixture.location.target().path()),
            )
            .await
            .expect("completed opening work must not retain the Client in periodic GC loops");
            // A stripped context retains admission independently of the Client.
            assert_work_busy(&fixture.owner.claim.gate);
            drop(stripped);
            drop(next);
            assert_work_finished(&fixture.owner.claim.gate);
        })
        .await;
}

#[tokio::test]
async fn loaded_escaped_stores_and_actual_listing_retain_exact_admission() {
    let fixture = fixture();
    let resolver: Arc<dyn ProviderResolver> = fixture.registry.clone();
    let mut execution = ExecutionContext::default();
    execution.set_provider_resolver(&resolver);
    LORE_CONTEXT
        .scope(Arc::new(execution), async {
            let loaded = repository::load_and_connect(
                fixture.location.target().path(),
                RepositoryAccess::ReadOnly,
            )
            .await
            .unwrap();
            let escaped = loaded.without_command_locks();
            let immutable = escaped.immutable_store();
            let store_lock = immutable.store_lock().unwrap();
            assert_work_busy(&fixture.owner.claim.gate);
            drop(loaded);

            let pause = lore_storage::test_util::pause_at(PausePoint::Listing);
            let parent_dropped = Arc::new(AtomicBool::new(false));
            let parent = IoLease::new(Arc::new(Parent {
                dropped: parent_dropped.clone(),
            }));
            let stream = parent
                .scope(
                    escaped
                        .read_mutable_store()
                        .list(Partition::default(), lore_storage::KeyType::BranchMetadata),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), pause.reached.notified())
                .await
                .unwrap();
            drop(stream.channel());
            drop(escaped);
            drop(immutable);
            drop(parent);
            assert!(!parent_dropped.load(Ordering::Acquire));
            assert!(store_lock.is_held());
            assert_work_busy(&fixture.owner.claim.gate);
            pause.resume.notify_one();
            wait_until(|| parent_dropped.load(Ordering::Acquire)).await;
            wait_until(|| !store_lock.is_held()).await;
            assert_work_finished(&fixture.owner.claim.gate);
        })
        .await;
}

#[tokio::test]
async fn stripped_loaded_context_keeps_adapters_and_refuses_retired_work() {
    let fixture = fixture();
    let resolver: Arc<dyn ProviderResolver> = fixture.registry.clone();
    let mut execution = ExecutionContext::default();
    execution.set_provider_resolver(&resolver);
    LORE_CONTEXT
        .scope(Arc::new(execution), async {
            let loaded = repository::load_and_connect(
                fixture.location.target().path(),
                RepositoryAccess::ReadOnly,
            )
            .await
            .unwrap();
            let escaped = loaded.without_command_locks();
            let immutable = escaped.immutable_store();
            drop(loaded);
            fixture.owner.retire().unwrap();
            assert!(
                immutable
                    .get(Partition::default(), Default::default())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("admission refused")
            );
            assert!(
                escaped
                    .read_mutable_store()
                    .list(Partition::default(), lore_storage::KeyType::BranchMetadata,)
                    .await
                    .is_err()
            );
        })
        .await;
}
