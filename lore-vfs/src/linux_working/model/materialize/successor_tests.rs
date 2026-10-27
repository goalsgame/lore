// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use super::*;
use crate::namespace::NamespaceSnapshot;

async fn candidate(f: &Fixture) -> (super::super::owned::UnpublishedCandidate, Arc<dyn Backend>) {
    let plan = f.plan().await;
    let resources = f
        .model
        .prepare_private_resources(f.layout(&plan).await)
        .await
        .unwrap();
    let target = resources.view.backend();
    let candidate = resources.into_owned_candidate().await.unwrap();
    drop(plan);
    (candidate, target)
}
async fn claim_target(
    f: &Fixture,
    target: Arc<dyn Backend>,
    label: &str,
) -> crate::linux_working::Result<Arc<Model>> {
    let root = target
        .as_any()
        .unwrap()
        .downcast_ref::<RevisionBackend>()
        .unwrap()
        .root();
    let namespace =
        Arc::new(Namespace::new(target, root, f.gate.clone(), Default::default()).unwrap());
    Model::new(
        namespace,
        Arc::new(Stores::default()),
        Arc::new(StorageBudget::new(4096)),
        Recovery::open(f._directory.0.join(label), Default::default()).unwrap(),
        Default::default(),
    )
    .await
}
fn unchanged(before: &NamespaceSnapshot, after: &NamespaceSnapshot) {
    assert_eq!(before.original_root, after.original_root);
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.next_inode, after.next_inode);
    assert_eq!(before.active_underlays, after.active_underlays);
    assert_eq!(before.backend_state, after.backend_state);
    assert_eq!(before.baselines, after.baselines);
    assert_eq!(before.files, after.files);
    assert_eq!(before.nodes.len(), after.nodes.len());
    for (left, right) in before.nodes.iter().zip(&after.nodes) {
        assert_eq!(left.entry, right.entry);
        assert_eq!(left.attached, right.attached);
        assert_eq!(left.names, right.names);
        assert_eq!(left.receipt, right.receipt);
    }
}
#[tokio::test]
async fn successor_reservation_blocks_target_owner_and_rollback_permits_target_use() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let (candidate, target) = candidate(&f).await;
            let before = f.model.namespace.snapshot(4096).await.unwrap();
            let debt = f.model.budget.reserved_bytes();
            let attempts = f.stores.count.load(Ordering::Acquire);
            let reservation = candidate.reserve_successor(&f.model).await.unwrap();
            f.model.live().unwrap();
            reservation
                .check(
                    &f.model.ownership,
                    candidate.state.successor_issuer().unwrap(),
                )
                .unwrap();
            assert!(matches!(
                candidate.reserve_successor(&f.model).await,
                Err(PreparationError::Model(crate::linux_working::Error::Core(
                    core::Error::Busy
                )))
            ));
            assert!(matches!(
                claim_target(&f, target.clone(), "reserved-target").await,
                Err(crate::linux_working::Error::Core(core::Error::Busy))
            ));
            unchanged(&before, &f.model.namespace.snapshot(4096).await.unwrap());
            assert_eq!(f.model.budget.reserved_bytes(), debt);
            assert_eq!(f.stores.count.load(Ordering::Acquire), attempts);
            assert!(!f._directory.0.join("recovery").join("checkpoint").exists());
            drop(reservation);
            let independent = claim_target(&f, target, "released-target").await.unwrap();
            assert!(matches!(
                candidate.reserve_successor(&f.model).await,
                Err(PreparationError::Model(crate::linux_working::Error::Core(
                    core::Error::Busy
                )))
            ));
            f.model.live().unwrap();
            drop(candidate);
            independent.shutdown().await.unwrap();
            f.model.shutdown().await.unwrap();
        })
        .await;
}
#[tokio::test]
async fn successor_refuses_independently_owned_target_without_source_changes() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let (candidate, target) = candidate(&f).await;
            let before = f.model.namespace.snapshot(4096).await.unwrap();
            let independent = claim_target(&f, target, "independent-target")
                .await
                .unwrap();
            assert!(matches!(
                candidate.reserve_successor(&f.model).await,
                Err(PreparationError::Model(crate::linux_working::Error::Core(
                    core::Error::Busy
                )))
            ));
            unchanged(&before, &f.model.namespace.snapshot(4096).await.unwrap());
            f.model.live().unwrap();
            independent.live().unwrap();
            drop(candidate);
            independent.shutdown().await.unwrap();
            f.model.shutdown().await.unwrap();
        })
        .await;
}
#[tokio::test]
async fn successor_refuses_wrong_receiver_closed_and_stale_sources_before_target_reservation() {
    LORE_CONTEXT
        .scope(execution(), async {
            for case in 0..3 {
                let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
                let (candidate, target) = candidate(&f).await;
                let other = if case == 0 {
                    Some(Fixture::new(Default::default(), 4096, b"OTHER123", false).await)
                } else {
                    None
                };
                if case == 1 {
                    f.model.close_requests();
                }
                if case == 2 {
                    let cold = f.model.lookup(1, b"cold").await.unwrap();
                    f.model.lookup(cold.entry.inode, b"leaf").await.unwrap();
                }
                let receiver = other.as_ref().map_or(&f.model, |other| &other.model);
                let result = candidate.reserve_successor(receiver).await;
                match case {
                    0 => assert!(matches!(
                        result,
                        Err(PreparationError::Model(crate::linux_working::Error::Core(
                            core::Error::Stale
                        )))
                    )),
                    1 => assert!(matches!(
                        result,
                        Err(PreparationError::Model(crate::linux_working::Error::Core(
                            core::Error::Unmounted
                        )))
                    )),
                    _ => assert!(matches!(
                        result,
                        Err(PreparationError::Transition(
                            transition_prepare::Error::StaleSource
                        ))
                    )),
                }
                let independent = claim_target(&f, target, "not-reserved").await.unwrap();
                drop(candidate);
                independent.shutdown().await.unwrap();
                if let Some(other) = other {
                    other.model.shutdown().await.unwrap();
                }
                f.model.shutdown().await.unwrap();
            }
        })
        .await;
}

#[tokio::test]
async fn successor_reservations_bind_exact_candidate_issuers_without_upgrading_source() {
    LORE_CONTEXT
        .scope(execution(), async {
            let f = Fixture::new(Default::default(), 4096, b"DIRTY123", false).await;
            let plan = f.plan().await;
            let first = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            let second = f
                .model
                .prepare_private_resources(f.layout(&plan).await)
                .await
                .unwrap()
                .into_owned_candidate()
                .await
                .unwrap();
            drop(plan);
            let first_reservation = first.reserve_successor(&f.model).await.unwrap();
            assert_eq!(
                first_reservation
                    .check(&f.model.ownership, second.state.successor_issuer().unwrap()),
                Err(core::Error::Stale)
            );
            let second_reservation = second.reserve_successor(&f.model).await.unwrap();
            first_reservation
                .check(&f.model.ownership, first.state.successor_issuer().unwrap())
                .unwrap();
            second_reservation
                .check(&f.model.ownership, second.state.successor_issuer().unwrap())
                .unwrap();
            f.model.live().unwrap();
            drop(first_reservation);
            drop(second_reservation);
            drop(first);
            drop(second);
            f.model.shutdown().await.unwrap();
        })
        .await;
}
