// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::capture_memory::CaptureLimits;

fn limits() -> CaptureLimits {
    CaptureLimits {
        max_buffer_bytes: 8 << 20,
        max_metadata_nodes: 100_000,
        max_name_bytes: 1 << 20,
    }
}
#[tokio::test]
async fn source_preparation_moves_original_buffers_and_retains_seal_serial_and_admission() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", true).await;
            let plan = f.plan().await;
            let pointers = plan.test_source_buffers();
            let file_generations = plan.intent().file_generations().to_vec();
            let captured = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap()
                .capture_checkpoint(None, limits())
                .await
                .unwrap();
            let reads = f.stores.control.reads.load(Ordering::Acquire);
            let writes = f.stores.control.writes.load(Ordering::Acquire);
            let prepared = captured.prepare_source(plan).await.unwrap();
            fn owned<T: Send + 'static>(_: &T) {}
            owned(&prepared);
            assert_eq!(
                pointers,
                prepared
                    .test_source()
                    .files
                    .values()
                    .flat_map(|file| file
                        .extents
                        .iter()
                        .map(|extent| extent.data.as_ptr() as usize))
                    .collect::<Vec<_>>()
            );
            assert_eq!(prepared.test_intent().file_generations(), file_generations);
            assert!(!prepared.test_intent().dry_run());
            assert_eq!(prepared.test_intent().scope(), f.scope);
            assert_eq!(f.stores.control.reads.load(Ordering::Acquire), reads);
            assert_eq!(f.stores.control.writes.load(Ordering::Acquire), writes);
            assert!(f.model.namespace.snapshot(4096).await.is_err());
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(10),
                    f.model.checkpoint.lock_source_preparation()
                )
                .await
                .is_err()
            );
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests - 1
            );
            prepared.test_authorize().await.unwrap();
            drop(prepared);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            drop(f.model.checkpoint.lock_source_preparation().await.unwrap());
            drop(f.model.namespace.freeze_operation().await.unwrap());
        })
        .await;
}
#[tokio::test]
async fn foreign_same_numeric_source_witness_cannot_pair_with_candidate() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let foreign = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let other_plan = foreign.plan().await;
            assert_eq!(plan.test_source_witness(), other_plan.test_source_witness());
            let captured = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap()
                .capture_checkpoint(None, limits())
                .await
                .unwrap();
            assert!(captured.prepare_source(other_plan).await.is_err());
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            drop(plan);
            drop(f.model.namespace.freeze_operation().await.unwrap());
            drop(foreign.model.namespace.freeze_operation().await.unwrap());
        })
        .await;
}
#[tokio::test]
async fn source_preparation_refuses_stale_epoch_policy_and_dry_run_without_private_io() {
    LORE_CONTEXT
        .scope(execution(), async {
            for failure in 0..3 {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let plan = if failure == 2 {
                    transition_prepare::prepare(
                        f.model.namespace.clone(),
                        f.scope,
                        f.target,
                        Options {
                            local: LocalPolicy::KeepLocal,
                            dry_run: true,
                            ..Default::default()
                        },
                        Default::default(),
                    )
                    .await
                    .unwrap()
                } else {
                    f.plan().await
                };
                let captured = f
                    .model
                    .prepare_private_resources(f.layout(&plan).await)
                    .await
                    .unwrap()
                    .into_owned_candidate()
                    .await
                    .unwrap()
                    .capture_checkpoint(None, limits())
                    .await
                    .unwrap();
                if failure == 0 {
                    let cold = f.model.namespace.lookup(1, b"cold").await.unwrap();
                    f.model.namespace.lookup(cold.inode, b"leaf").await.unwrap();
                } else if failure == 1 {
                    f.gate.source_read.store(true, Ordering::Release);
                }
                let reads = f.stores.control.reads.load(Ordering::Acquire);
                let writes = f.stores.control.writes.load(Ordering::Acquire);
                assert!(
                    captured.prepare_source(plan).await.is_err(),
                    "case {failure}"
                );
                assert_eq!(f.stores.control.reads.load(Ordering::Acquire), reads);
                assert_eq!(f.stores.control.writes.load(Ordering::Acquire), writes);
                assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
                assert_eq!(
                    f.model.requests.available_permits(),
                    f.model.limits.max_requests
                );
            }
        })
        .await;
}
#[tokio::test]
async fn source_preparation_waits_model_names_tail_and_cancel_or_changed_metadata_is_safe() {
    LORE_CONTEXT.scope(execution(), async {
        for changed in [false, true] {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let captured = f.model.prepare_private_resources(f.layout(&plan).await).await.unwrap().into_owned_candidate().await.unwrap().capture_checkpoint(None, limits()).await.unwrap();
            let names = f.model.names.lock().await;
            let mut preparation = Box::pin(captured.prepare_source(plan));
            tokio::select! { _=tokio::task::yield_now()=>{}, result=&mut preparation=>panic!("did not wait for names tail: {}", result.is_ok()) }
            if changed {
                f.model.directory_generation.fetch_add(1, Ordering::AcqRel);
                drop(names);
                assert!(preparation.await.is_err());
            } else { drop(preparation); drop(names); }
            assert_eq!(f.model.requests.available_permits(), f.model.limits.max_requests);
            drop(f.model.checkpoint.lock_source_preparation().await.unwrap());
            drop(f.model.namespace.freeze_operation().await.unwrap());
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
        }
    }).await;
}

#[tokio::test]
async fn source_authorization_releases_namespace_and_names_and_cancellation_rolls_back() {
    LORE_CONTEXT.scope(execution(), async {
        for revoke in [false, true] {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let captured = f.model.prepare_private_resources(f.layout(&plan).await).await.unwrap().into_owned_candidate().await.unwrap().capture_checkpoint(None, limits()).await.unwrap();
            f.gate.pause_read.store(true, Ordering::Release);
            let mut preparation = Box::pin(captured.prepare_source(plan));
            tokio::select! { _=f.gate.read_entered.notified()=>{}, result=&mut preparation=>panic!("policy did not pause: {}",result.is_ok()) }
            drop(tokio::time::timeout(Duration::from_secs(1), f.model.names.lock()).await.unwrap());
            // NS lock is available too; the subsequent policy callback can await.
            let mut getattr = Box::pin(f.model.namespace.getattr(f.entry.inode));
            tokio::select! { _=f.gate.read_entered.notified()=>{}, _=&mut getattr=>panic!("getattr unexpectedly completed") }
            drop(getattr);
            if revoke {
                f.gate.source_read.store(true, Ordering::Release);
                f.gate.pause_read.store(false, Ordering::Release);
                f.gate.read_resume.notify_one();
                assert!(preparation.await.is_err());
            } else { drop(preparation); }
            f.gate.pause_read.store(false, Ordering::Release);
            f.gate.source_read.store(false, Ordering::Release);
            assert_eq!(f.model.requests.available_permits(), f.model.limits.max_requests);
            drop(f.model.checkpoint.lock_source_preparation().await.unwrap());
            drop(f.model.namespace.freeze_operation().await.unwrap());
            assert!(f.model.checkpoint.test_snapshot().unwrap().is_none());
        }
    }).await;
}

#[tokio::test]
async fn exhausted_authority_vector_budget_refuses_before_policy_or_private_reads() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let mut captured = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap()
                .capture_checkpoint(None, limits())
                .await
                .unwrap();
            captured.test_exhaust_source_authority_memory();
            f.gate.pause_read.store(true, Ordering::Release);
            let reads = f.stores.control.reads.load(Ordering::Acquire);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), captured.prepare_source(plan))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(f.stores.control.reads.load(Ordering::Acquire), reads);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
            f.gate.pause_read.store(false, Ordering::Release);
            drop(f.model.namespace.freeze_operation().await.unwrap());
        })
        .await;
}

struct ScopedPolicy {
    inner: Arc<dyn AccessGate>,
    key: core::NodeKey,
    deny: AtomicBool,
}
impl AccessGate for ScopedPolicy {
    fn authorize<'a>(&'a self, key: &'a core::NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if *key == self.key && self.deny.load(Ordering::Acquire) {
                Err(core::Error::PermissionDenied)
            } else {
                self.inner.authorize(key, access).await
            }
        })
    }
}
#[tokio::test]
async fn detached_source_uses_its_retired_policy_instead_of_the_current_gate() {
    LORE_CONTEXT
        .scope(execution(), async {
            let (mut f, left, right, baseline, child) =
                super::capture_tests::linked_fixture().await;
            let retired = f.model.lookup(right.inode, b"file").await.unwrap().entry;
            let original = f.model.namespace.test_effective_source_gate();
            let old = Arc::new(ScopedPolicy {
                inner: original.clone(),
                key: retired.authority,
                deny: AtomicBool::new(false),
            });
            f.model.namespace.test_replace_source_gate(old.clone());
            f.model.remove(right.inode, b"file", false).await.unwrap();
            let current = Arc::new(ScopedPolicy {
                inner: original,
                key: retired.authority,
                deny: AtomicBool::new(true),
            });
            f.model.namespace.test_replace_source_gate(current);
            f.scope = left.authority;
            f.target = Address::zero_context_hash(commit_file(&child, b"target-left", 74).await.0);
            let plan = f.plan().await;
            assert_eq!(plan.intent().expected_baseline(), Some(&baseline));
            let captured = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap()
                .capture_checkpoint(None, limits())
                .await
                .unwrap();
            let prepared = captured.prepare_source(plan).await.unwrap();
            assert_eq!(prepared.test_intent().expected_baseline(), Some(&baseline));
            prepared.test_authorize().await.unwrap();
            old.deny.store(true, Ordering::Release);
            assert!(matches!(
                prepared.test_authorize().await,
                Err(core::Error::PermissionDenied)
            ));
            drop(prepared);
            assert_eq!(
                f.model.requests.available_permits(),
                f.model.limits.max_requests
            );
        })
        .await;
}
