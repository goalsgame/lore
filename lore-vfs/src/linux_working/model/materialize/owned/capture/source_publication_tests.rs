// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::capture_memory::CaptureLimits;

async fn prepared(f: &Fixture) -> crate::linux_working::OwnedSourcePreparation {
    let plan = f.plan().await;
    f.model
        .prepare_private_resources(f.layout(&plan).await)
        .await
        .unwrap()
        .into_owned_candidate()
        .await
        .unwrap()
        .capture_checkpoint(
            None,
            CaptureLimits {
                max_buffer_bytes: 8 << 20,
                max_metadata_nodes: 100_000,
                max_name_bytes: 1 << 20,
            },
        )
        .await
        .unwrap()
        .prepare_source(plan)
        .await
        .unwrap()
}
async fn inactive(owner: &crate::checkpoint_owner::CheckpointOwner) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.test_source_active() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn source_publication_joins_then_acknowledges_exact_bytes_and_retains_full_bundle() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(
                Limits {
                    max_requests: 1,
                    ..Default::default()
                },
                4096,
                b"DIRTY123",
                true,
            )
            .await;
            let source = prepared(&f).await;
            let pointers = source
                .test_source()
                .files
                .values()
                .flat_map(|file| {
                    file.extents
                        .iter()
                        .map(|extent| extent.data.as_ptr() as usize)
                })
                .collect::<Vec<_>>();
            let generations = source.test_intent().file_generations().to_vec();
            let acknowledged = source.publish_source().await.unwrap();
            let image = acknowledged.test_source();
            let disk = std::fs::read(f._directory.0.join("recovery/checkpoint")).unwrap();
            assert_eq!(image.bytes(), disk);
            assert_eq!(image.identity().length, u64::try_from(disk.len()).unwrap());
            assert_eq!(
                image.identity().sha256.as_slice(),
                ring::digest::digest(&ring::digest::SHA256, &disk).as_ref()
            );
            assert_eq!(
                pointers,
                image
                    .snapshot()
                    .files
                    .values()
                    .flat_map(|file| file
                        .extents
                        .iter()
                        .map(|extent| extent.data.as_ptr() as usize))
                    .collect::<Vec<_>>()
            );
            assert_eq!(acknowledged.test_intent().file_generations(), generations);
            assert_eq!(acknowledged.test_intent().scope(), f.scope);
            assert_eq!(acknowledged.test_intent().target().revision, f.target);
            assert!(!acknowledged.test_intent().dry_run());
            assert_eq!(f.model.requests.available_permits(), 0);
            assert!(f.model.checkpoint.test_source_serial_locked());
            assert!(f.model.namespace.freeze_operation().await.is_err());
            assert!(f.model.namespace.file_cache_mutation().is_err());
            drop(acknowledged);
            inactive(&f.model.checkpoint).await;
            assert_eq!(f.model.requests.available_permits(), 1);
            assert!(!f.model.checkpoint.test_source_serial_locked());
            drop(f.model.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

#[tokio::test]
async fn source_publication_reauthorizes_retained_preparation_before_admission() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let source = prepared(&f).await;
            f.gate.source_read.store(true, Ordering::Release);
            assert!(source.publish_source().await.is_err());
            assert_eq!(f.model.checkpoint.test_source_active(), 0);
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            assert!(!f.model.checkpoint.is_poisoned());
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
        })
        .await;
}

#[tokio::test]
async fn source_publication_actual_exit_precedes_ack_and_postchecks_refuse_revocation() {
    LORE_CONTEXT
        .scope(execution(), async {
            for metadata in [false, true] {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let source = prepared(&f).await;
                let exit = f.model.checkpoint.test_source_pause(true);
                let task = lore_base::lore_spawn!(source.publish_source());
                exit.started.notified().await;
                assert!(f.model.checkpoint.test_snapshot().unwrap().is_some());
                assert!(!task.is_finished());
                assert_ne!(f.model.checkpoint.test_source_active(), 0);
                if metadata {
                    let _names = f.model.names.lock().await;
                    let next = f.model.next_directory_generation().unwrap();
                    f.model.directory_generation.store(next, Ordering::Release);
                } else {
                    f.gate.source_read.store(true, Ordering::Release);
                }
                exit.resume();
                assert!(task.await.unwrap().is_err());
                inactive(&f.model.checkpoint).await;
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests
                );
                assert!(!f.model.checkpoint.is_poisoned());
            }
        })
        .await;
}

#[tokio::test]
async fn source_publication_caller_cancel_retains_claims_and_blocks_shutdown_until_join() {
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
            let source = prepared(&f).await;
            let pause = f.model.checkpoint.test_source_pause(false);
            let task = lore_base::lore_spawn!(source.publish_source());
            pause.started.notified().await;
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
            assert_eq!(f.model.requests.available_permits(), 0);
            assert_eq!(f.model.checkpoint.test_source_active(), 1);
            assert!(f.model.checkpoint.test_source_serial_locked());
            assert!(f.model.namespace.freeze_operation().await.is_err());
            let model = f.model.clone();
            let shutdown = lore_base::lore_spawn!(async move { model.shutdown().await });
            tokio::task::yield_now().await;
            assert!(!shutdown.is_finished());
            pause.resume();
            shutdown.await.unwrap().unwrap();
            assert_eq!(f.model.requests.available_permits(), 1);
            assert_eq!(f.model.checkpoint.test_source_active(), 0);
        })
        .await;
}

#[tokio::test]
async fn source_publication_spawn_failures_and_wrong_owner_never_publish() {
    LORE_CONTEXT
        .scope(execution(), async {
            for joiner in [false, true] {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let source = prepared(&f).await;
                let hooks = f.model.checkpoint.test_source_hooks();
                if joiner {
                    hooks.fail_joiner.store(true, Ordering::Release);
                } else {
                    hooks.fail_publisher.store(true, Ordering::Release);
                }
                assert!(source.publish_source().await.is_err());
                assert_eq!(f.model.checkpoint.test_source_active(), 0);
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests
                );
                assert!(!f.model.checkpoint.is_poisoned());
                assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
                assert!(!f.model.checkpoint.test_source_serial_locked());
            }
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let other = Fixture::new(Default::default(), 4096, b"OTHER123", false).await;
            let source = prepared(&f).await;
            assert!(
                other
                    .model
                    .checkpoint
                    .publish_owned_source(source)
                    .await
                    .is_err()
            );
            assert_eq!(other.model.checkpoint.test_source_active(), 0);
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
        })
        .await;
}

#[tokio::test]
async fn source_publication_close_at_admission_refuses_a_prestarted_joiner() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let source = prepared(&f).await;
            f.model.checkpoint.test_source_pause_admission();
            let task = lore_base::lore_spawn!(source.publish_source());
            f.model.checkpoint.test_source_admission_started().await;
            f.model.checkpoint.close();
            f.model.checkpoint.test_source_resume_admission();
            assert!(task.await.unwrap().is_err());
            assert_eq!(f.model.checkpoint.test_source_active(), 0);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
        })
        .await;
}

#[tokio::test]
async fn source_publication_handoff_failures_poison_while_detached_worker_keeps_resources() {
    LORE_CONTEXT
        .scope(execution(), async {
            for failure in 0..3 {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let source = prepared(&f).await;
                let pause = f.model.checkpoint.test_source_pause(false);
                let hooks = f.model.checkpoint.test_source_hooks();
                match failure {
                    0 => hooks.disconnect.store(true, Ordering::Release),
                    1 => hooks.panic_handoff.store(true, Ordering::Release),
                    _ => hooks.panic_joiner.store(true, Ordering::Release),
                }
                let task = lore_base::lore_spawn!(source.publish_source());
                pause.started.notified().await;
                let result = task.await;
                assert!(result.is_err() || result.unwrap().is_err());
                assert!(f.model.checkpoint.is_poisoned());
                assert_eq!(f.model.checkpoint.test_source_active(), 1);
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests - 1
                );
                assert!(f.model.checkpoint.test_source_serial_locked());
                pause.resume();
                inactive(&f.model.checkpoint).await;
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests
                );
            }
        })
        .await;
}

#[tokio::test]
async fn source_publication_panic_and_uncertain_io_never_acknowledge() {
    LORE_CONTEXT
        .scope(execution(), async {
            for panic in [false, true] {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let source = prepared(&f).await;
                if panic {
                    f.model
                        .checkpoint
                        .test_source_hooks()
                        .panic_publisher
                        .store(true, Ordering::Release);
                } else {
                    f.model
                        .checkpoint
                        .test_fault(Some(crate::recovery::Boundary::DirectorySync));
                }
                assert!(source.publish_source().await.is_err());
                assert_eq!(f.model.checkpoint.test_source_active(), 0);
                assert!(f.model.checkpoint.is_poisoned());
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests
                );
            }
        })
        .await;
}

#[tokio::test]
async fn source_publication_abandoned_receiver_drops_all_owned_resources_before_counter() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let source = prepared(&f).await;
            let pause = f.model.checkpoint.test_source_pause(false);
            let active = f.model.checkpoint.test_source_active_probe();
            let weak_model = Arc::downgrade(&f.model);
            let weak_factory = Arc::downgrade(&f.stores);
            let task = lore_base::lore_spawn!(source.publish_source());
            pause.started.notified().await;
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
            let Fixture {
                model,
                stores,
                gate,
                _directory,
                ..
            } = f;
            drop(model);
            drop(stores);
            drop(gate);
            assert!(weak_model.upgrade().is_some());
            assert!(weak_factory.upgrade().is_some());
            assert_eq!(active(), 1);
            pause.resume();
            tokio::time::timeout(Duration::from_secs(5), async {
                while active() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(weak_model.upgrade().is_none());
            assert!(weak_factory.upgrade().is_none());
            drop(_directory);
        })
        .await;
}

#[tokio::test]
async fn source_publication_cancelled_postjoin_policy_drops_resources_before_completion() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let source = prepared(&f).await;
            let pause = f.model.checkpoint.test_source_pause(false);
            let task = lore_base::lore_spawn!(source.publish_source());
            pause.started.notified().await;
            f.gate.pause_read.store(true, Ordering::Release);
            pause.resume();
            f.gate.read_entered.notified().await;
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_some());
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests - 1
            );
            assert!(f.model.checkpoint.test_source_serial_locked());
            task.abort();
            assert!(matches!(task.await, Err(error) if error.is_cancelled()));
            inactive(&f.model.checkpoint).await;
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            assert!(!f.model.checkpoint.test_source_serial_locked());
            assert!(!f.model.checkpoint.is_poisoned());
        })
        .await;
}

#[test]
fn source_publication_runtime_teardown_preserves_the_external_publisher_and_joiner() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (f, pause, active, weak_model, weak_factory) =
        runtime.block_on(LORE_CONTEXT.scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let source = prepared(&f).await;
            let pause = f.model.checkpoint.test_source_pause(false);
            let active = f.model.checkpoint.test_source_active_probe();
            let weak_model = Arc::downgrade(&f.model);
            let weak_factory = Arc::downgrade(&f.stores);
            let _caller = lore_base::lore_spawn!(source.publish_source());
            pause.started.notified().await;
            (f, pause, active, weak_model, weak_factory)
        }));
    drop(runtime);
    let Fixture {
        model,
        stores,
        gate,
        _directory,
        ..
    } = f;
    drop(model);
    drop(stores);
    drop(gate);
    assert!(weak_model.upgrade().is_some());
    assert!(weak_factory.upgrade().is_some());
    assert_eq!(active(), 1);
    pause.resume();
    let started = std::time::Instant::now();
    while active() != 0 {
        assert!(started.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(weak_model.upgrade().is_none());
    assert!(weak_factory.upgrade().is_none());
    drop(_directory);
}
