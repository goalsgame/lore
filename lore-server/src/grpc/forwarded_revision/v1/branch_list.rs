// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_proto::lore::revision::v1::BranchListRequest;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::READ_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::forwarded_requests::CallerContext;
use crate::grpc::revision::v1::branch_list::BranchListStream;
use crate::grpc::revision::v1::branch_list::branch_list_implementation;

/// Handler that takes a `BranchList` request forwarded on from peer's `RevisionService`
/// and executes it, streaming the response back to the other server for forwarding on
/// to its client.
///
/// See `forwarded_revision/v1/branch_get.rs` for why this handler, not the
/// originating server, must verify the forwarded raw bearer token and run
/// the access check itself.
#[tracing::instrument(name = "ForwardedRevision::v1::BranchList::Handler", skip_all)]
pub async fn handler(
    request: Request<BranchListRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    jwt_verifier: Option<JwtVerifier>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
) -> Result<Response<BranchListStream>, Status> {
    let caller_context = CallerContext::from_forwarded_request(&request)?;

    let claims =
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

    let verified_token = crate::grpc::verified_token(&claims, &caller_context.authorization);
    repository_authorizer
        .check_repository_access(
            verified_token.as_ref(),
            caller_context.repository_id,
            Some(READ_ACTION),
        )
        .await
        .map_err(|_err| Status::permission_denied("Permission denied"))?;

    let req = request.into_inner();

    branch_list_implementation(req, caller_context, immutable_store, mutable_store).await
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use lore_base::runtime::LORE_CONTEXT;
    use lore_proto::lore::revision::v1::BranchListRequest;
    use lore_revision::lore::RepositoryId;
    use lore_revision::repository::RepositoryContext;
    use lore_transport::grpc::REPOSITORY_ID_KEY;
    use rand::random;
    use tokio_stream::StreamExt;
    use tonic::Request;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::grpc::forwarded_requests::CallerContext;
    use crate::grpc::revision::v1::branch_list::BranchListStream;
    use crate::store::test_store_create;

    fn allow_all_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(AllowAllRepositoryAuthorizer)
    }

    fn make_forwarded_request(repository: RepositoryId) -> Request<BranchListRequest> {
        CallerContext {
            repository_id: repository,
            user_id: "lily".into(),
            correlation_id: String::new(),
            authorization: None,
        }
        .to_forwarded_request(BranchListRequest {
            creator: None,
            include_deleted: false,
        })
        .expect("CallerContext::to_forwarded_request failed in test")
    }

    #[tokio::test]
    async fn missing_user_id_returns_internal_error() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            // No on-behalf-of-user-id in metadata
            let mut request = Request::new(BranchListRequest {
                creator: None,
                include_deleted: false,
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let err = handler(
                request,
                immutable_store,
                mutable_store,
                None,
                allow_all_authorizer(),
            )
            .await
            .map(|_| ())
            .expect_err("missing user id should fail");

            assert_eq!(err.code(), tonic::Code::Internal);
            assert!(err.message().contains("on-behalf-of-user-id"));
        }))
        .await;
    }

    // Happy and unhappy paths verify that whatever the underlying
    // `branch_list_implementation` returns is forwarded on correctly.
    mod base_branch_list_handler {
        use super::*;
        use crate::grpc::revision::v1::branch_list::test::create_root_branch;

        async fn collect(response: Response<BranchListStream>) -> Vec<String> {
            response
                .into_inner()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|r| r.expect("stream item ok"))
                .map(|item| item.branch.unwrap().name)
                .collect()
        }

        #[tokio::test]
        async fn list_returns_branches() {
            let repository = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository_context = Arc::new(RepositoryContext::new_server_context(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    repository,
                ));
                create_root_branch(&repository_context, "main", "lily").await;
                create_root_branch(&repository_context, "feature", "james").await;

                let response = handler(
                    make_forwarded_request(repository),
                    immutable_store,
                    mutable_store,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");

                let names = collect(response).await;
                assert_eq!(names.len(), 2);
                assert!(names.contains(&"main".to_string()));
                assert!(names.contains(&"feature".to_string()));
            }))
            .await;
        }

        #[tokio::test]
        async fn empty_repository_yields_empty_stream() {
            let repository = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let response = handler(
                    make_forwarded_request(repository),
                    immutable_store,
                    mutable_store,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");

                let names = collect(response).await;
                assert!(names.is_empty());
            }))
            .await;
        }
    }

    /// Second, independent layer of defense on top of the originating
    /// server's own front-door check — see `branch_get.rs`'s equivalent
    /// module for the shared rationale.
    mod authorization {
        use std::ops::Add;
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
        use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;

        const ALGORITHM: Algorithm = Algorithm::HS256;
        const SIGNING_SECRET: &str = "forwarded-branch-list-test-secret";
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

        fn make_forwarded_request_with_token(
            repository: RepositoryId,
            token: String,
        ) -> Request<BranchListRequest> {
            CallerContext {
                repository_id: repository,
                user_id: "lily".into(),
                correlation_id: String::new(),
                authorization: Some(format!("Bearer {token}")),
            }
            .to_forwarded_request(BranchListRequest {
                creator: None,
                include_deleted: false,
            })
            .expect("CallerContext::to_forwarded_request failed in test")
        }

        /// A forwarded caller lacking `read` is denied.
        #[tokio::test]
        async fn denies_forwarded_caller_without_read_action() {
            let repository = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let token = make_jwt(Some(vec!["push".to_string()]));
                let err = handler(
                    make_forwarded_request_with_token(repository, token),
                    immutable_store,
                    mutable_store,
                    Some(verifier()),
                    tier1_authorizer(),
                )
                .await
                .map(|_| ())
                .expect_err("a forwarded caller without read must be denied");
                assert_eq!(err.code(), tonic::Code::PermissionDenied);
            }))
            .await;
        }

        /// A forwarded caller holding `read` succeeds.
        #[tokio::test]
        async fn allows_forwarded_caller_with_read_action() {
            let repository = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let token = make_jwt(Some(vec!["read".to_string()]));
                handler(
                    make_forwarded_request_with_token(repository, token),
                    immutable_store,
                    mutable_store,
                    Some(verifier()),
                    tier1_authorizer(),
                )
                .await
                .expect("a forwarded caller holding read should be allowed");
            }))
            .await;
        }
    }
}
