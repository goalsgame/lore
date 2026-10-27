// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::capture_memory::CaptureLimits;
use crate::capture_memory::CaptureMemory;
use crate::transition_prepare::Budget;

fn capture_limits(nodes: usize) -> CaptureLimits {
    CaptureLimits {
        max_buffer_bytes: 8 * 1024 * 1024,
        max_metadata_nodes: nodes,
        max_name_bytes: 1024 * 1024,
    }
}

#[tokio::test]
async fn model_metadata_prebuilds_exact_detached_index_without_changing_live_handles() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let (gone_handle, gone) = f.model.create(1, b"gone", 0o644, WRITE).await.unwrap();
            f.model.release(gone_handle).await.unwrap();
            f.model.remove(1, b"gone", false).await.unwrap();
            let old = f.model.open(f.entry.inode, READ).await.unwrap();
            let directory = f.model.opendir(1).await.unwrap();
            let page = f.model.readdir(directory, 0, 16).await.unwrap();
            let old_unlinked = f.model.state.lock().unwrap().unlinked.clone();
            let generation = f.model.directory_generation.load(Ordering::Acquire);
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let new_inode = *resources.files.keys().next().unwrap();
            let candidate = resources.into_owned_candidate().await.unwrap();
            assert_eq!(
                candidate.test_model_generations(),
                (generation, generation + 1)
            );
            assert_eq!(
                f.model.directory_generation.load(Ordering::Acquire),
                generation
            );
            assert_eq!(f.model.state.lock().unwrap().unlinked, old_unlinked);
            assert!(candidate.test_model_unlinked().contains(&gone.entry.inode));
            assert!(candidate.test_model_unlinked().contains(&f.entry.inode));
            assert!(!candidate.test_model_unlinked().contains(&1));
            assert!(!candidate.test_model_unlinked().contains(&new_inode));
            assert_eq!(f.model.read(old, 0, 8).await.unwrap(), b"DIRTY123");
            assert!(
                f.model
                    .getattr_with_handle(f.entry.inode, Some(old))
                    .await
                    .unwrap()
                    .linked
            );
            let again = f.model.readdir(directory, 0, 16).await.unwrap();
            assert_eq!(again.entries, page.entries);
            let another = f.model.open(f.entry.inode, READ).await.unwrap();
            f.model.release(another).await.unwrap();
            candidate.test_check_model_metadata().unwrap();
            candidate.state.validate_source().await.unwrap();
            let expected = candidate.test_model_unlinked().clone();
            drop(plan);
            let captured = candidate
                .capture_checkpoint(None, capture_limits(100_000))
                .await
                .unwrap();
            let actual = captured
                .test_snapshot()
                .nodes
                .iter()
                .filter(|node| !node.attached)
                .map(|node| node.entry.inode)
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, expected);
            f.model.release(old).await.unwrap();
            f.model.release(directory).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn model_metadata_detached_index_checks_budget_receiver_and_capture_retention() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let foreign = Fixture::new(Default::default(), 4096, b"OTHER123", false).await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let candidate = resources.into_owned_candidate().await.unwrap();
            let count = candidate.test_model_unlinked().len();
            assert_ne!(count, 0);
            let mut budget = Budget::new(transition_prepare::Limits::default()).unwrap();
            let remaining = budget.remaining_nodes();
            assert!(matches!(
                candidate
                    .state
                    .prepare_model_unlinked(&foreign.model.namespace, &mut budget),
                Err(core::Error::Stale)
            ));
            assert_eq!(budget.remaining_nodes(), remaining);
            let mut exhausted = Budget::new(transition_prepare::Limits {
                max_nodes: 1,
                ..Default::default()
            })
            .unwrap();
            exhausted.node().unwrap();
            assert!(matches!(
                candidate
                    .state
                    .prepare_model_unlinked(&f.model.namespace, &mut exhausted),
                Err(core::Error::ResourceLimit)
            ));
            let mut memory = CaptureMemory::new(capture_limits(count)).unwrap();
            candidate.test_retain_model_metadata(&mut memory).unwrap();
            assert_eq!(memory.reserved_nodes(), count);
            let mut memory = CaptureMemory::new(capture_limits(count)).unwrap();
            memory.nodes(1).unwrap();
            assert!(matches!(
                candidate.test_retain_model_metadata(&mut memory),
                Err(core::Error::ResourceLimit)
            ));
            candidate.test_check_model_metadata().unwrap();
        })
        .await;
}

#[tokio::test]
async fn model_metadata_waits_for_names_before_capturing_a_bookkeeping_tail() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let names = f.model.names.lock().await;
            let reads = f.stores.control.reads.load(Ordering::Acquire);
            let mut owned = Box::pin(resources.into_owned_candidate());
            std::future::poll_fn(|cx| {
                assert!(matches!(owned.as_mut().poll(cx), std::task::Poll::Pending));
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(f.stores.control.reads.load(Ordering::Acquire), reads);
            // Private fixture for an admitted name mutation's synchronous tail.
            let next = f.model.next_directory_generation().unwrap();
            f.model.directory_generation.store(next, Ordering::Release);
            drop(names);
            let candidate = owned.await.unwrap();
            assert_eq!(candidate.test_model_generations(), (next, next + 1));
            candidate.test_check_model_metadata().unwrap();
        })
        .await;
}

#[tokio::test]
async fn model_metadata_cancelled_names_wait_releases_admission_and_keeps_source() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(
                Limits {
                    max_requests: 1,
                    ..Default::default()
                },
                4096,
                b"DIRTY123",
                false,
            )
            .await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let generation = f.model.directory_generation.load(Ordering::Acquire);
            let unlinked = f.model.state.lock().unwrap().unlinked.clone();
            let names = f.model.names.lock().await;
            let mut owned = Box::pin(resources.into_owned_candidate());
            std::future::poll_fn(|cx| {
                assert!(matches!(owned.as_mut().poll(cx), std::task::Poll::Pending));
                std::task::Poll::Ready(())
            })
            .await;
            drop(owned);
            drop(names);
            assert_eq!(f.model.requests.available_permits(), 1);
            assert_eq!(
                f.model.directory_generation.load(Ordering::Acquire),
                generation
            );
            assert_eq!(f.model.state.lock().unwrap().unlinked, unlinked);
            assert!(f.model.names.try_lock().is_ok());
            assert!(f.model.namespace.freeze_operation().await.is_err());
            drop(plan);
            drop(f.model.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn model_metadata_overflow_preflight_precedes_owned_conversion_io() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let reads = f.stores.control.reads.load(Ordering::Acquire);
            let stores = f.stores.count.load(Ordering::Acquire);
            let next_handle = f.model.state.lock().unwrap().next;
            {
                let _names = f.model.names.lock().await;
                f.model
                    .directory_generation
                    .store(i64::MAX as u64, Ordering::Release);
            }
            assert!(matches!(
                resources.into_owned_candidate().await,
                Err(PreparationError::Model(super::super::super::Error::Core(
                    core::Error::ResourceLimit
                )))
            ));
            assert_eq!(f.stores.control.reads.load(Ordering::Acquire), reads);
            assert_eq!(f.stores.count.load(Ordering::Acquire), stores);
            assert_eq!(f.model.state.lock().unwrap().next, next_handle);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            f.model.live().unwrap();
        })
        .await;
}

#[tokio::test]
async fn model_metadata_numeric_witness_rejects_changed_generation() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let resources = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap();
            let candidate = resources.into_owned_candidate().await.unwrap();
            let _names = f.model.names.lock().await;
            let next = f.model.next_directory_generation().unwrap();
            f.model.directory_generation.store(next, Ordering::Release);
            assert!(matches!(
                candidate.test_check_model_metadata(),
                Err(PreparationError::Model(super::super::super::Error::Core(
                    core::Error::Stale
                )))
            ));
        })
        .await;
}
