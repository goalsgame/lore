// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_proto::lore::repository::v1::RepositoryGetRequest;
use lore_proto::lore::repository::v1::RepositoryGetResponse;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::forwarded_requests::CallerContext;
use crate::grpc::repository::v1::repository_get::repository_get_implementation;

/// Handler that takes a `RepositoryGet` request forwarded on from peer's `RepositoryService`
/// and executes it, returning the result to the other server for forwarding on to its
/// client
///
/// This peer-to-peer path predates `RepositoryAuthorizer`. The receiving
/// server — this one — is the only side with the real name/id mapping and
/// metadata for a partition it owns, so it is also the only side that can
/// run a meaningful authorization check; the originating server does not
/// (it only forwards, see `grpc/repository/v1/repository_get.rs::handler`).
/// That means this handler must verify the forwarded raw bearer token
/// (`CallerContext::authorization`) itself, into real claims, rather than
/// trusting the originating server's decision or fabricating an
/// already-authenticated token — a `GlobalGrantsAuthorizer` only ever needs
/// `Some(token)` to be present to answer a plain reachability question
/// (LEP 2026-08-20-oidc-oauth2-authentication, D8/D9), so a fabricated
/// default token would authorize every forwarded request unconditionally
/// regardless of which authorizer this deployment configures.
#[tracing::instrument(name = "ForwardedRepository::v1::RepositoryGet::Handler", skip_all)]
pub async fn handler(
    request: Request<RepositoryGetRequest>,
    jwt_verifier: Option<JwtVerifier>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
) -> Result<Response<RepositoryGetResponse>, Status> {
    let caller_context = CallerContext::from_forwarded_request(&request)?;

    // Mirrors what `JWTAuthnInterceptor` does for a directly-received
    // request: reject outright when this deployment has auth configured and
    // the caller did not present a token that verifies, rather than falling
    // back to an anonymous, and therefore potentially over-permissive,
    // check.
    let token =
        match (&jwt_verifier, caller_context.authorization.as_deref()) {
            (Some(verifier), Some(raw)) => {
                let bearer = raw.strip_prefix("Bearer ").unwrap_or(raw);
                Some(verifier.verify_token(bearer).await.map_err(|_err| {
                    Status::unauthenticated("invalid forwarded authorization token")
                })?)
            }
            (Some(_), None) => {
                return Err(Status::unauthenticated("authorization header required"));
            }
            (None, _) => None,
        };

    repository_get_implementation(
        request.into_inner(),
        caller_context,
        repository_authorizer,
        token,
        immutable_store,
        mutable_store,
    )
    .await
}

#[cfg(test)]
mod test {
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::Context;
    use lore_proto::lore::repository::v1::repository_get_request::Query;
    use lore_revision::lore::RepositoryId;
    use lore_revision::repository::RepositoryContext;
    use rand::random;
    use tonic::Request;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;
    use crate::store::test_store_create;

    fn allow_all() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(AllowAllRepositoryAuthorizer)
    }

    async fn store_repository(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        id: RepositoryId,
        name: &str,
    ) {
        let repository = Arc::new(RepositoryContext::new_server_context(
            immutable_store,
            mutable_store,
            id,
        ));
        let metadata = lore_revision::repository::RepositoryMetadata {
            name: name.to_string(),
            description: "a description".into(),
            default_branch: Context::from(uuid::Uuid::now_v7()),
            default_branch_name: "main".into(),
            creator: "alice".into(),
            created: 12345,
        };
        let metadata_hash =
            lore_revision::repository::metadata_store(repository.clone(), metadata.clone())
                .await
                .expect("Failed to store repository metadata");
        lore_revision::repository::metadata_store_hash(repository.clone(), metadata_hash)
            .await
            .expect("Failed to store repository metadata hash");
        lore_revision::repository::store_name_to_id(repository, name, id)
            .await
            .expect("Failed to store repository name to id mapping");
    }

    fn make_forwarded_request(query: Query) -> Request<RepositoryGetRequest> {
        CallerContext {
            repository_id: RepositoryId::default(),
            user_id: "alice".into(),
            correlation_id: String::new(),
            authorization: None,
        }
        .to_forwarded_request(RepositoryGetRequest { query: Some(query) })
        .expect("CallerContext::to_forwarded_request failed in test")
    }

    #[tokio::test]
    async fn missing_user_id_returns_internal_error() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            // Deliberately omit on-behalf-of-user-id to test the missing-field error
            let request = Request::new(RepositoryGetRequest {
                query: Some(Query::Name("my-repo".into())),
            });

            let err = handler(request, None, allow_all(), immutable_store, mutable_store)
                .await
                .expect_err("missing user id should fail");

            assert_eq!(err.code(), tonic::Code::Internal);
            assert!(err.message().contains("on-behalf-of-user-id"));
        }))
        .await;
    }

    // Happy and unhappy paths verify that whatever the underlying
    // `repository_get_implementation` returns is forwarded on correctly.
    mod base_repository_get_handler {
        use super::*;

        #[tokio::test]
        async fn get_by_name_returns_full_repository_record() {
            let id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                store_repository(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    id,
                    "my-repo",
                )
                .await;

                let response = handler(
                    make_forwarded_request(Query::Name("my-repo".into())),
                    None, /* no auth */
                    allow_all(),
                    immutable_store,
                    mutable_store,
                )
                .await
                .expect("Request failed");

                let repository = response
                    .into_inner()
                    .repository
                    .expect("response should include Repository");
                assert_eq!(repository.name, "my-repo");
                assert_eq!(repository.creator, "alice");
                assert_eq!(repository.id, bytes::Bytes::from(id));
            }))
            .await;
        }

        #[tokio::test]
        async fn get_unknown_id_returns_not_found() {
            let id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let err = handler(
                    make_forwarded_request(Query::Id(id.into())),
                    None,
                    allow_all(),
                    immutable_store,
                    mutable_store,
                )
                .await
                .expect_err("unknown id should fail");

                assert_eq!(err.code(), tonic::Code::NotFound);
            }))
            .await;
        }
    }

    /// Exercises the fix for the latent bypass this handler used to have:
    /// `repository_authorizer(auth_url, None)` ignored `[server.auth]`
    /// entirely, so it could only ever build `AuthClientAuthorizer` or
    /// `AllowAllRepositoryAuthorizer` — never `GlobalGrantsAuthorizer` — and
    /// the token it built was a fabricated, already-"authenticated" default
    /// regardless of what the forwarded caller actually presented. On a
    /// Tier 1 deployment this meant any forwarded `RepositoryGet` was
    /// authorized with zero real checks, even with no token at all.
    mod tier1 {
        use std::ops::Add;
        use std::sync::Arc;
        use std::time::Duration;
        use std::time::SystemTime;
        use std::time::UNIX_EPOCH;

        use async_trait::async_trait;
        use jsonwebtoken::Algorithm;
        use jsonwebtoken::DecodingKey;
        use jsonwebtoken::EncodingKey;
        use jsonwebtoken::Header;
        use jsonwebtoken::encode;

        use super::*;
        use crate::auth::jwk::JWKService;
        use crate::auth::jwk::JWKServiceError;
        use crate::auth::jwt::AuthorizationToken;
        use crate::grpc::forwarded_requests::CallerContext;

        const ALGORITHM: Algorithm = Algorithm::HS256;
        const SIGNING_SECRET: &str = "forwarded-repository-get-test-secret";
        const TEST_AUDIENCE: &str = "lore-test";

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
            let mut service = MockTestJWKService::new();
            service
                .expect_get_key()
                .returning(|_| Ok((DecodingKey::from_secret(SIGNING_SECRET.as_ref()), ALGORITHM)));
            JwtVerifier {
                jwk_service: Arc::new(service),
                jwt_issuer: None,
                jwt_audience: Some(vec![TEST_AUDIENCE.to_string()]),
            }
        }

        /// `groups` is an ordinary named `AuthorizationToken` field (the Dex
        /// convention), so `GlobalGrantsAuthorizer::new(Some("groups"))`
        /// reads it directly.
        fn make_jwt(groups: Option<Vec<String>>) -> String {
            let claims = AuthorizationToken {
                user_id: "test-user".to_string(),
                issuer: "test-issuer".to_string(),
                issued_at: 1,
                expires: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .add(Duration::from_secs(60))
                    .as_secs(),
                audience: vec![TEST_AUDIENCE.to_string()],
                groups,
                ..Default::default()
            };
            let key = EncodingKey::from_secret(SIGNING_SECRET.as_ref());
            let mut header = Header::new(ALGORITHM);
            header.kid = Some("test-kid".to_string());
            encode(&header, &claims, &key).unwrap()
        }

        fn tier1_authorizer() -> Arc<dyn RepositoryAuthorizer> {
            Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string())))
        }

        fn make_forwarded_request_with_auth(
            query: Query,
            authorization: Option<String>,
        ) -> Request<RepositoryGetRequest> {
            CallerContext {
                repository_id: RepositoryId::default(),
                user_id: "alice".into(),
                correlation_id: String::new(),
                authorization,
            }
            .to_forwarded_request(RepositoryGetRequest { query: Some(query) })
            .expect("CallerContext::to_forwarded_request failed in test")
        }

        #[tokio::test]
        async fn denies_forwarded_request_with_no_token() {
            let id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                store_repository(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    id,
                    "my-repo",
                )
                .await;

                let err = handler(
                    make_forwarded_request_with_auth(Query::Name("my-repo".into()), None),
                    Some(verifier()),
                    tier1_authorizer(),
                    immutable_store,
                    mutable_store,
                )
                .await
                .expect_err("no token should be denied under Tier 1");

                assert_eq!(err.code(), tonic::Code::Unauthenticated);
            }))
            .await;
        }

        #[tokio::test]
        async fn denies_forwarded_request_with_invalid_token() {
            let id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                store_repository(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    id,
                    "my-repo",
                )
                .await;

                let err = handler(
                    make_forwarded_request_with_auth(
                        Query::Name("my-repo".into()),
                        Some("Bearer not-a-real-jwt".to_string()),
                    ),
                    Some(verifier()),
                    tier1_authorizer(),
                    immutable_store,
                    mutable_store,
                )
                .await
                .expect_err("an unverifiable token should be denied under Tier 1");

                assert_eq!(err.code(), tonic::Code::Unauthenticated);
            }))
            .await;
        }

        #[tokio::test]
        async fn allows_forwarded_request_with_valid_token() {
            let id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                store_repository(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    id,
                    "my-repo",
                )
                .await;

                let token = format!("Bearer {}", make_jwt(Some(vec!["anything".to_string()])));
                let response = handler(
                    make_forwarded_request_with_auth(Query::Name("my-repo".into()), Some(token)),
                    Some(verifier()),
                    tier1_authorizer(),
                    immutable_store,
                    mutable_store,
                )
                .await
                .expect("a verified caller should be allowed under Tier 1");

                let repository = response
                    .into_inner()
                    .repository
                    .expect("response should include Repository");
                assert_eq!(repository.name, "my-repo");
            }))
            .await;
        }
    }
}
