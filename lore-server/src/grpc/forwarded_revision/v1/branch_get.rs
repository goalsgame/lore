// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_proto::lore::revision::v1::BranchGetRequest;
use lore_proto::lore::revision::v1::BranchGetResponse;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::READ_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::forwarded_requests::CallerContext;
use crate::grpc::revision::v1::branch_get::branch_get_implementation;

/// Handler that takes a `BranchGet` request forwarded on from peer's `RevisionService`
/// and executes it, returning the result to the other server for forwarding on to its
/// client.
///
/// This peer-to-peer path predates `RepositoryAuthorizer` — see
/// `forwarded_repository/v1/repository_get.rs` for the shared rationale:
/// the receiving server — this one — is the only side with the real
/// name/id mapping and metadata for a partition it owns, so it is also the
/// only side that can run a meaningful authorization check; the
/// originating server does not (it only forwards, see
/// `grpc/revision/v1/branch_get.rs::handler`). That means this handler
/// must verify the forwarded raw bearer token (`CallerContext::authorization`)
/// itself, into real claims, rather than trusting the originating server's
/// decision or fabricating an already-authenticated token.
#[tracing::instrument(name = "ForwardedRevision::v1::BranchGet::Handler", skip_all)]
pub async fn handler(
    request: Request<BranchGetRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    jwt_verifier: Option<JwtVerifier>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
) -> Result<Response<BranchGetResponse>, Status> {
    let caller_context = CallerContext::from_forwarded_request(&request)?;

    // See `grpc::verify_forwarded_caller`'s doc comment: reject outright
    // when this deployment has auth configured and the caller did not
    // present a token that verifies, rather than falling back to an
    // anonymous, and therefore potentially over-permissive, check.
    let claims =
        crate::grpc::verify_forwarded_caller(&jwt_verifier, &caller_context.authorization).await?;

    let verified_token = crate::grpc::verified_token(&claims, &caller_context.authorization);
    repository_authorizer
        .check_repository_access(
            verified_token.as_ref(),
            caller_context.repository_id,
            Some(READ_ACTION),
        )
        .await
        .map_err(|_err| Status::permission_denied("Permission denied"))?;

    branch_get_implementation(
        request.into_inner(),
        caller_context,
        immutable_store,
        mutable_store,
    )
    .await
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::BranchPoint;
    use lore_base::types::Hash;
    use lore_proto::lore::revision::v1::BranchGetRequest;
    use lore_proto::lore::revision::v1::branch_get_request::Query as BranchGetQuery;
    use lore_revision::branch;
    use lore_revision::branch::DEFAULT_HISTORY_STEP_SIZE;
    use lore_revision::lore::BranchId;
    use lore_revision::lore::RepositoryId;
    use lore_revision::repository::RepositoryContext;
    use lore_transport::grpc::REPOSITORY_ID_KEY;
    use rand::random;
    use tonic::Request;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::grpc::get_write_token;
    use crate::grpc::handlers::branch_push;
    use crate::store::test_store_create;

    fn allow_all_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(AllowAllRepositoryAuthorizer)
    }

    async fn create_test_branch(
        repository_context: Arc<RepositoryContext>,
        branch: BranchId,
    ) -> Hash {
        let write_token = get_write_token();
        let main = lore_revision::branch::create(
            repository_context.clone(),
            &write_token,
            BranchId::from(uuid::Uuid::now_v7()),
            branch::DEFAULT_DEFAULT_NAME,
            branch::default_category(),
            "test-creator",
            1,
            vec![],
            false,
            false,
        )
        .await
        .expect("Could not create main branch");

        let state = Arc::new(lore_revision::state::State::new());
        state.set_parent_self(Hash::default());
        state.set_revision_number(1);
        let state_hash = state
            .serialize(repository_context.clone(), &write_token)
            .await
            .expect("Failed to serialize state");

        let latest = branch_push::push(
            repository_context.clone(),
            main,
            state_hash,
            true,
            true,
            false,
            DEFAULT_HISTORY_STEP_SIZE,
            crate::grpc::server::RevisionListAcceleration::default(),
        )
        .await
        .expect("Failed to push latest revision")
        .revision;

        lore_revision::branch::create(
            repository_context.clone(),
            &write_token,
            branch,
            "test-name",
            branch::personal_category(),
            "BranchCreator",
            12345,
            vec![BranchPoint {
                branch: main,
                revision: latest,
            }],
            false,
            false,
        )
        .await
        .expect("Could not create test branch");

        latest
    }

    fn make_forwarded_request(
        repository: RepositoryId,
        query: BranchGetQuery,
    ) -> Request<BranchGetRequest> {
        CallerContext {
            repository_id: repository,
            user_id: "alice".into(),
            correlation_id: String::new(),
            authorization: None,
        }
        .to_forwarded_request(BranchGetRequest { query: Some(query) })
        .expect("CallerContext::to_forwarded_request failed in test")
    }

    #[tokio::test]
    async fn missing_user_id_returns_internal_error() {
        let repository = random::<RepositoryId>();
        let branch_id = BranchId::from(uuid::Uuid::now_v7());
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            // No on-behalf-of-user-id in metadata
            let mut request = Request::new(BranchGetRequest {
                query: Some(BranchGetQuery::Id(branch_id.into())),
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
            .expect_err("missing user id should fail");

            assert_eq!(err.code(), tonic::Code::Internal);
            assert!(err.message().contains("on-behalf-of-user-id"));
        }))
        .await;
    }

    // Happy and unhappy paths verify that whatever the underlying
    // `branch_get_implementation` returns is forwarded on correctly.
    mod base_branch_get_handler {
        use super::*;

        #[tokio::test]
        async fn get_by_id_returns_branch_record() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository_context = Arc::new(RepositoryContext::new_server_context(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    repository,
                ));
                let latest = create_test_branch(repository_context, branch_id).await;

                let response = handler(
                    make_forwarded_request(repository, BranchGetQuery::Id(branch_id.into())),
                    immutable_store,
                    mutable_store,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");

                let branch = response
                    .into_inner()
                    .branch
                    .expect("response should include Branch");
                assert!(!branch.deleted);
                assert_eq!(branch.name, "test-name");
                assert_eq!(branch.creator, "BranchCreator");
                assert_eq!(branch.latest, bytes::Bytes::from(latest));
            }))
            .await;
        }

        #[tokio::test]
        async fn get_by_name_returns_branch_record() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository_context = Arc::new(RepositoryContext::new_server_context(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    repository,
                ));
                create_test_branch(repository_context, branch_id).await;

                let response = handler(
                    make_forwarded_request(repository, BranchGetQuery::Name("test-name".into())),
                    immutable_store,
                    mutable_store,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");

                let branch = response
                    .into_inner()
                    .branch
                    .expect("response should include Branch");
                assert!(!branch.deleted);
                assert_eq!(branch.name, "test-name");
            }))
            .await;
        }

        #[tokio::test]
        async fn get_unknown_id_returns_not_found() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let err = handler(
                    make_forwarded_request(repository, BranchGetQuery::Id(branch_id.into())),
                    immutable_store,
                    mutable_store,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect_err("unknown id should fail");
                assert_eq!(err.code(), tonic::Code::NotFound);
            }))
            .await;
        }
    }

    /// Exercises the second, independent layer of defense this handler adds
    /// on top of the originating server's own front-door check: previously
    /// `LoreForwardedRevisionV1Service` had no `jwt_verifier`/
    /// `repository_authorizer` fields at all and trusted the originating
    /// server's decision unconditionally. These tests prove a forwarded
    /// caller is actually checked here too, mirroring
    /// `forwarded_repository/v1/repository_get.rs`'s `tier1` test module.
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
        const SIGNING_SECRET: &str = "forwarded-branch-get-test-secret";
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
            branch_id: BranchId,
            token: String,
        ) -> Request<BranchGetRequest> {
            CallerContext {
                repository_id: repository,
                user_id: "alice".into(),
                correlation_id: String::new(),
                authorization: Some(format!("Bearer {token}")),
            }
            .to_forwarded_request(BranchGetRequest {
                query: Some(BranchGetQuery::Id(branch_id.into())),
            })
            .expect("CallerContext::to_forwarded_request failed in test")
        }

        /// A forwarded caller lacking `read` is denied.
        #[tokio::test]
        async fn denies_forwarded_caller_without_read_action() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let token = make_jwt(Some(vec!["push".to_string()]));
                let err = handler(
                    make_forwarded_request_with_token(repository, branch_id, token),
                    immutable_store,
                    mutable_store,
                    Some(verifier()),
                    tier1_authorizer(),
                )
                .await
                .expect_err("a forwarded caller without read must be denied");
                assert_eq!(err.code(), tonic::Code::PermissionDenied);
            }))
            .await;
        }

        /// A forwarded caller holding `read` succeeds.
        #[tokio::test]
        async fn allows_forwarded_caller_with_read_action() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository_context = Arc::new(RepositoryContext::new_server_context(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    repository,
                ));
                create_test_branch(repository_context, branch_id).await;

                let token = make_jwt(Some(vec!["read".to_string()]));
                handler(
                    make_forwarded_request_with_token(repository, branch_id, token),
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
