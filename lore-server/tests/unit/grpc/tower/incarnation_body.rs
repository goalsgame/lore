// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::body::Body;
use axum::body::Bytes;
use http::Request;
use http::Response;
use http_body_util::BodyExt;
use lore_base::lore_spawn;
use lore_base::types::Hash;
use lore_revision::lore::RepositoryId;
use lore_server::auth::jwt::AuthorizationToken;
use lore_server::auth::jwt::ResourcePermission;
use lore_server::authnz::repository_authorizer::AuthClientAuthorizer;
use lore_server::authnz::repository_authorizer::PartitionGrants;
use lore_server::authnz::repository_authorizer::RawToken;
use lore_server::authnz::repository_authorizer::RepositoryAuthorizer;
use lore_server::authnz::repository_authorizer::VerifiedToken;
use lore_server::grpc::tower::incarnation_body::IncarnationBodyLayer;
use lore_server::grpc::tower::incarnation_body::IncarnationBodyService;
use tokio::sync::Notify;
use tonic::Code;
use tonic::Status;
use tower::Layer;
use tower::Service;

use crate::authnz::switchable_resolver::SwitchableResolver;

/// Reads the whole request body and answers with the status the failure
/// would reach the client as; counts the bodies it received intact.
#[derive(Clone, Default)]
struct Handler(Arc<AtomicUsize>);

impl<B> Service<Request<B>> for Handler
where
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<Option<Status>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let received = self.0.clone();
        Box::pin(async move {
            let status = match request.into_body().collect().await {
                Ok(_) => {
                    received.fetch_add(1, Ordering::SeqCst);
                    None
                }
                Err(error) => Some(Status::from_error(error.into())),
            };
            Ok(Response::new(status))
        })
    }
}

fn first() -> Hash {
    Hash::from([1u8; 32])
}

fn second() -> Hash {
    Hash::from([2u8; 32])
}

fn claims(entries: &[String]) -> AuthorizationToken {
    AuthorizationToken {
        resources: Some(
            entries
                .iter()
                .map(|resource_id| ResourcePermission {
                    resource_id: resource_id.clone(),
                    permission: vec!["read".to_string()],
                })
                .collect(),
        ),
        ..Default::default()
    }
}

/// A request the partition-access layer admitted for `entries`, whose body
/// arrives only when `release` is notified.
async fn admitted(
    resolver: &Arc<SwitchableResolver>,
    repository: RepositoryId,
    entries: &[String],
    release: Arc<Notify>,
) -> Request<Body> {
    let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(
        AuthClientAuthorizer::new("https://auth.invalid".to_string())
            .with_name_resolver(resolver.clone()),
    );
    let claims = claims(entries);
    let verified = VerifiedToken {
        raw: "raw.jwt",
        claims: &claims,
    };
    let regranted = authorizer
        .granted_access_tracked(Some(&verified), repository)
        .await
        .unwrap();
    let body = futures::stream::once(async move {
        release.notified().await;
        Ok::<_, Infallible>(Bytes::from_static(b"message"))
    });
    let mut request = Request::new(Body::from_stream(body));
    request.extensions_mut().insert(PartitionGrants {
        repository_id: repository,
        grants: regranted.grants.unwrap(),
        incarnation: regranted.incarnation,
    });
    request.extensions_mut().insert(claims);
    request.extensions_mut().insert(RawToken("raw.jwt".into()));
    request
}

fn service(resolver: &Arc<SwitchableResolver>) -> (IncarnationBodyService<Handler>, Handler) {
    let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(
        AuthClientAuthorizer::new("https://auth.invalid".to_string())
            .with_name_resolver(resolver.clone()),
    );
    let handler = Handler::default();
    (
        IncarnationBodyLayer::new(authorizer).layer(handler.clone()),
        handler,
    )
}

async fn deliver_after_flip(
    entries: &[String],
    repository: RepositoryId,
    resolver: &Arc<SwitchableResolver>,
) -> (Option<Status>, usize) {
    let (mut service, handler) = service(resolver);
    let release = Arc::new(Notify::new());
    let request = admitted(resolver, repository, entries, release.clone()).await;
    let pending = lore_spawn!(async move { service.call(request).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(20)).await;

    resolver.set(repository, second(), "game");
    release.notify_one();
    let response = pending.await.unwrap();
    (response.into_body(), handler.0.load(Ordering::SeqCst))
}

#[tokio::test]
async fn a_name_scoped_body_delivered_after_a_recreate_is_refused() {
    let repository: RepositoryId = rand::random();
    let resolver = SwitchableResolver::new();
    resolver.set(repository, first(), "goalables/app");

    let (status, received) =
        deliver_after_flip(&["urc-name:goalables/*".to_string()], repository, &resolver).await;

    assert_eq!(status.unwrap().code(), Code::PermissionDenied);
    assert_eq!(received, 0);
}

#[tokio::test]
async fn a_body_whose_retained_grants_went_stale_is_aborted() {
    let repository: RepositoryId = rand::random();
    let resolver = SwitchableResolver::new();
    resolver.set(repository, first(), "goalables/app");

    let (status, received) = deliver_after_flip(
        &[
            "urc-name:goalables/*".to_string(),
            format!("urc-{repository}"),
        ],
        repository,
        &resolver,
    )
    .await;

    assert_eq!(status.unwrap().code(), Code::Aborted);
    assert_eq!(received, 0);
}

#[tokio::test]
async fn an_id_scoped_body_is_delivered_after_a_recreate() {
    let repository: RepositoryId = rand::random();
    let resolver = SwitchableResolver::new();
    resolver.set(repository, first(), "goalables/app");

    let (status, received) =
        deliver_after_flip(&[format!("urc-{repository}")], repository, &resolver).await;

    assert!(status.is_none());
    assert_eq!(received, 1);
    assert_eq!(resolver.incarnations(), 0);
}
