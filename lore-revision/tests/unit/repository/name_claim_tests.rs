// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![allow(clippy::disallowed_methods)]

use std::sync::Arc;

use lore_base::types::Context;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;

async fn stores() -> (Arc<dyn ImmutableStore>, Arc<dyn MutableStore>) {
    repository::create_client_memory_stores()
        .await
        .expect("in-memory stores should be creatable")
}

fn server_context(
    stores: &(Arc<dyn ImmutableStore>, Arc<dyn MutableStore>),
    id: RepositoryId,
) -> Arc<RepositoryContext> {
    Arc::new(RepositoryContext::new_server_context(
        stores.0.clone(),
        stores.1.clone(),
        id,
    ))
}

fn repository_id(n: u8) -> RepositoryId {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    RepositoryId::from(Context::from(bytes))
}

#[tokio::test]
async fn concurrent_claims_of_one_name_have_one_winner() {
    let stores = stores().await;
    let ids: Vec<RepositoryId> = (1..=8).map(repository_id).collect();
    let claims = ids.iter().map(|id| {
        let context = server_context(&stores, *id);
        async move { repository::claim_name(context, "goalables/app", *id).await }
    });
    let results = futures::future::join_all(claims).await;

    let winners: Vec<RepositoryId> = ids
        .iter()
        .zip(&results)
        .filter(|(_, result)| matches!(result, Ok(None)))
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(winners.len(), 1, "exactly one create may claim the name");
    for result in &results {
        match result {
            Ok(None) => {}
            Ok(Some(holder)) => assert_eq!(*holder, winners[0], "losers are told who holds it"),
            Err(err) => panic!("claim failed: {err}"),
        }
    }
    let resolved = repository::id_from_name(server_context(&stores, ids[0]), "goalables/app")
        .await
        .expect("the claimed name resolves");
    assert_eq!(resolved, winners[0]);
}

#[tokio::test]
async fn a_repository_can_claim_its_own_name_again() {
    let stores = stores().await;
    let id = repository_id(1);
    let context = server_context(&stores, id);
    assert_eq!(
        repository::claim_name(context.clone(), "a", id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repository::claim_name(context, "a", id).await.unwrap(),
        None
    );
}

#[tokio::test]
async fn a_released_name_can_be_claimed_by_another_repository() {
    let stores = stores().await;
    let (first, second) = (repository_id(1), repository_id(2));
    assert_eq!(
        repository::claim_name(server_context(&stores, first), "a", first)
            .await
            .unwrap(),
        None
    );
    repository::release_name(server_context(&stores, first), "a", first)
        .await
        .unwrap();
    assert_eq!(
        repository::claim_name(server_context(&stores, second), "a", second)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn releasing_does_not_take_a_name_another_repository_holds() {
    let stores = stores().await;
    let (holder, other) = (repository_id(1), repository_id(2));
    repository::claim_name(server_context(&stores, holder), "a", holder)
        .await
        .unwrap();
    repository::release_name(server_context(&stores, other), "a", other)
        .await
        .unwrap();
    assert_eq!(
        repository::claim_name(server_context(&stores, other), "a", other)
            .await
            .unwrap(),
        Some(holder)
    );
}
