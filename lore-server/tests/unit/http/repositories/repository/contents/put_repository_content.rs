// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::convert::Infallible;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use axum::http::StatusCode;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::encode;
use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_revision::lore::RepositoryId;
use lore_server::auth::jwk::JWKService;
use lore_server::auth::jwk::JWKServiceError;
use lore_server::auth::jwt::AuthorizationToken;
use lore_server::auth::jwt::JwtVerifier;
use lore_server::auth::jwt::ResourcePermission;
use lore_server::authnz::repository_authorizer::AuthClientAuthorizer;
use lore_server::http::server::LoreHttpServerSettings;
use lore_server::http::server::ServerHealth;
use lore_server::http::server::ServerState;
use lore_server::http::server::create_router;
use rand::random;
use tokio::sync::Notify;
use tower::Service;

use crate::authnz::switchable_resolver::SwitchableResolver;
use crate::store::test_support::test_store_create;

const SECRET: &str = "put-content-test-secret";
const AUDIENCE: &str = "lore-test";

mockall::mock! {
    TestJWKService {}

    #[async_trait]
    impl JWKService for TestJWKService {
        async fn get_key(
            &self,
            kid: &str,
        ) -> Result<(DecodingKey, jsonwebtoken::Algorithm), JWKServiceError>;

        fn get_cached_key(
            &self,
            kid: &str,
        ) -> Option<(DecodingKey, jsonwebtoken::Algorithm)>;

        async fn refresh_key(
            &self,
            kid: &str,
        ) -> Result<Option<(DecodingKey, jsonwebtoken::Algorithm)>, JWKServiceError>;
    }
}

fn verifier() -> JwtVerifier {
    let mut jwk_service = MockTestJWKService::new();
    jwk_service
        .expect_get_key()
        .returning(|_| Ok((DecodingKey::from_secret(SECRET.as_ref()), Algorithm::HS256)));
    JwtVerifier {
        jwk_service: Arc::new(jwk_service),
        jwt_issuer: None,
        jwt_audience: Some(vec![AUDIENCE.to_string()]),
        jwt_typ: None,
    }
}

fn bearer(resource_id: &str) -> String {
    let claims = AuthorizationToken {
        user_id: "test-user".to_string(),
        issuer: "test-issuer".to_string(),
        issued_at: 1,
        audience: vec![AUDIENCE.to_string()],
        expires: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .add(Duration::from_secs(60))
            .as_secs(),
        resources: Some(vec![ResourcePermission {
            resource_id: resource_id.to_string(),
            permission: vec!["read".to_string(), "write".to_string()],
        }]),
        ..Default::default()
    };
    let header = Header {
        kid: Some("test-kid".to_string()),
        ..Header::new(Algorithm::HS256)
    };
    let token = encode(&header, &claims, &EncodingKey::from_secret(SECRET.as_ref())).unwrap();
    format!("Bearer {token}")
}

fn app(
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    resolver: &Arc<SwitchableResolver>,
) -> Router {
    let health = ServerHealth::new_without_availability(immutable_store.clone());
    let state = ServerState {
        immutable_store,
        mutable_store,
        jwt_verifier: Some(verifier()),
        repository_authorizer: Arc::new(
            AuthClientAuthorizer::new("https://auth.invalid".to_string())
                .with_name_resolver(resolver.clone()),
        ),
        max_file_size: 1000,
        presign_config: None,
    };
    create_router(state, health, &LoreHttpServerSettings::test_default())
}

/// A PUT whose body arrives only when `release` is notified.
fn upload(repository: Context, authorization: &str, release: Arc<Notify>) -> Request<Body> {
    let body = futures::stream::once(async move {
        release.notified().await;
        Ok::<_, Infallible>(Bytes::from_static(b"payload"))
    });
    Request::builder()
        .method("PUT")
        .uri(format!("/v1/repository/{repository}/content"))
        .header("authorization", authorization)
        .body(Body::from_stream(body))
        .unwrap()
}

async fn admitted(resolver: &SwitchableResolver) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while resolver.incarnations() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the upload was never admitted");
}

#[tokio::test]
async fn an_upload_admitted_before_the_repository_was_recreated_is_refused() {
    let (immutable_store, mutable_store, execution) =
        test_store_create().await.expect("Failed to create stores");
    LORE_CONTEXT
        .scope(execution, async move {
            let context = random::<Context>();
            let repository: RepositoryId = context.into();
            let resolver = SwitchableResolver::new();
            resolver.set(repository, Hash::from([1u8; 32]), "goalables/app");
            let router = app(immutable_store, mutable_store, &resolver);

            let release = Arc::new(Notify::new());
            let mut pending = router.clone();
            let request = upload(context, &bearer("urc-name:goalables/*"), release.clone());
            let name_scoped = lore_spawn!(async move { pending.call(request).await.unwrap() });
            admitted(&resolver).await;

            resolver.set(repository, Hash::from([2u8; 32]), "game");
            release.notify_one();
            let response = name_scoped.await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);

            let release = Arc::new(Notify::new());
            release.notify_one();
            let mut router = router;
            let by_id = upload(context, &bearer(&format!("urc-{repository}")), release);
            let response = router.call(by_id).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        })
        .await;
}
