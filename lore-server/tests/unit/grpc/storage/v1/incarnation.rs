// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::types::Hash;
use lore_base::types::RepositoryId;
use lore_proto::lore::model::v1::Address as ProtoAddress;
use lore_proto::lore::storage::v1 as storage_v1;
use lore_server::auth::jwt::AuthorizationToken;
use lore_server::auth::jwt::ResourcePermission;
use lore_server::authnz::repository_authorizer::AuthClientAuthorizer;
use lore_server::authnz::repository_authorizer::IncarnationGuard;
use lore_server::authnz::repository_authorizer::RepositoryAuthorizer;
use lore_server::authnz::repository_authorizer::VerifiedToken;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::Status;

use crate::authnz::switchable_resolver::SwitchableResolver;
use crate::store::test_support::test_store_create;

fn histogram() -> Arc<opentelemetry::metrics::Histogram<f64>> {
    Arc::new(
        opentelemetry::global::meter("test")
            .f64_histogram("test")
            .build(),
    )
}

fn claims(entry: String) -> AuthorizationToken {
    AuthorizationToken {
        resources: Some(vec![ResourcePermission {
            resource_id: entry,
            permission: vec!["read".to_string(), "write".to_string()],
        }]),
        ..Default::default()
    }
}

/// The guard the handlers build from the partition-access layer's answer for
/// a caller holding `entry`.
async fn guard_for(
    resolver: &Arc<SwitchableResolver>,
    repository: RepositoryId,
    entry: String,
) -> Option<IncarnationGuard> {
    let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(
        AuthClientAuthorizer::new("https://auth.invalid".to_string())
            .with_name_resolver(resolver.clone()),
    );
    let claims = claims(entry);
    let token = VerifiedToken {
        raw: "raw.jwt",
        claims: &claims,
    };
    let regranted = authorizer
        .granted_access_tracked(Some(&token), repository)
        .await
        .unwrap();
    IncarnationGuard::new(authorizer, Some(&token), repository, regranted.incarnation)
}

fn first() -> Hash {
    Hash::from([1u8; 32])
}

fn second() -> Hash {
    Hash::from([2u8; 32])
}

#[tokio::test]
async fn a_get_stream_is_refused_after_a_name_scoped_repository_is_recreated() {
    let (immutable_store, _mutable_store, execution) =
        test_store_create().await.expect("Failed to create stores");
    lore_base::runtime::LORE_CONTEXT
        .scope(execution, async move {
            let repository: RepositoryId = rand::random();
            let resolver = SwitchableResolver::new();
            resolver.set(repository, first(), "goalables/app");
            let guard = guard_for(&resolver, repository, "urc-name:goalables/*".into()).await;
            let (requests, rx) = tokio::sync::mpsc::channel(4);
            let mut responses = lore_server::grpc::storage::v1::get::serve(
                ReceiverStream::new(rx),
                repository,
                String::new(),
                String::new(),
                immutable_store,
                guard,
                histogram(),
            )
            .await;
            let item = || Ok::<_, Status>(ProtoAddress::default());

            requests.send(item()).await.unwrap();
            assert!(responses.next().await.unwrap().is_ok());

            resolver.set(repository, second(), "game");
            requests.send(item()).await.unwrap();
            let refused = responses.next().await.unwrap().unwrap_err();
            assert_eq!(refused.code(), Code::PermissionDenied);
            assert!(responses.next().await.is_none());
        })
        .await;
}

#[tokio::test]
async fn an_id_scoped_get_stream_is_unaffected() {
    let (immutable_store, _mutable_store, execution) =
        test_store_create().await.expect("Failed to create stores");
    lore_base::runtime::LORE_CONTEXT
        .scope(execution, async move {
            let repository: RepositoryId = rand::random();
            let resolver = SwitchableResolver::new();
            resolver.set(repository, first(), "goalables/app");
            let guard = guard_for(&resolver, repository, format!("urc-{repository}")).await;
            assert!(guard.is_none());
            let (requests, rx) = tokio::sync::mpsc::channel(4);
            let mut responses = lore_server::grpc::storage::v1::get::serve(
                ReceiverStream::new(rx),
                repository,
                String::new(),
                String::new(),
                immutable_store,
                guard,
                histogram(),
            )
            .await;

            resolver.set(repository, second(), "game");
            requests
                .send(Ok::<_, Status>(ProtoAddress::default()))
                .await
                .unwrap();
            assert!(responses.next().await.unwrap().is_ok());
            assert_eq!(resolver.incarnations(), 0);
        })
        .await;
}

#[tokio::test]
async fn a_put_stream_is_refused_after_a_name_scoped_repository_is_recreated() {
    let (immutable_store, _mutable_store, execution) =
        test_store_create().await.expect("Failed to create stores");
    lore_base::runtime::LORE_CONTEXT
        .scope(execution, async move {
            let repository: RepositoryId = rand::random();
            let resolver = SwitchableResolver::new();
            resolver.set(repository, first(), "goalables/app");
            let guard = guard_for(&resolver, repository, "urc-name:goalables/*".into()).await;
            let (requests, rx) = tokio::sync::mpsc::channel(4);
            let mut responses = lore_server::grpc::storage::v1::put::serve(
                ReceiverStream::new(rx),
                repository,
                String::new(),
                String::new(),
                immutable_store,
                guard,
                histogram(),
            )
            .await;
            let item = || Ok::<_, Status>(storage_v1::PutRequest::default());

            requests.send(item()).await.unwrap();
            let admitted = responses.next().await.unwrap().unwrap_err();
            assert_eq!(admitted.code(), Code::InvalidArgument);

            resolver.set(repository, second(), "game");
            requests.send(item()).await.unwrap();
            let refused = responses.next().await.unwrap().unwrap_err();
            assert_eq!(refused.code(), Code::PermissionDenied);
        })
        .await;
}
