// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_proto::lore::revision::v1::BranchCreateRequest;
use lore_proto::lore::revision::v1::BranchCreateResponse;
use lore_revision::notification::NotificationSender;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::PUSH_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::forwarded_requests::CallerContext;
use crate::grpc::revision::v1::branch_create::branch_create_implementation;
use crate::hooks::HookDispatcher;

/// Handler that takes a `BranchCreate` request forwarded on from peer's `RevisionService`
/// and executes it, returning the result to the other server for forwarding on to its
/// client
///
/// This peer-to-peer path predates `RepositoryAuthorizer` — see
/// `forwarded_revision/v1/branch_get.rs` for the shared rationale: this
/// service is mounted on the internal mTLS server with no JWT interceptor
/// of its own, so this handler must verify the forwarded raw bearer token
/// (`CallerContext::authorization`) itself, into real claims, rather than
/// trusting the originating server's decision.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(name = "ForwardedRevision::v1::BranchCreate::Handler", skip_all)]
pub async fn handler(
    request: Request<BranchCreateRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    notification_sender: Arc<dyn NotificationSender>,
    hook_dispatcher: &HookDispatcher,
    instrument_provider: &impl InstrumentProvider,
    jwt_verifier: Option<JwtVerifier>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
) -> Result<Response<BranchCreateResponse>, Status> {
    let caller_context = CallerContext::from_forwarded_request(&request)?;

    // Mirrors what `JWTInterceptor` does for a directly-received request:
    // reject outright when this deployment has auth configured and the
    // caller did not present a token that verifies, rather than falling
    // back to an anonymous, and therefore potentially over-permissive,
    // check.
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
            Some(PUSH_ACTION),
        )
        .await
        .map_err(|_err| Status::permission_denied("Permission denied"))?;

    branch_create_implementation(
        request.into_inner(),
        caller_context,
        immutable_store,
        mutable_store,
        notification_sender,
        hook_dispatcher,
        instrument_provider,
    )
    .await
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use lore_base::runtime::LORE_CONTEXT;
    use lore_proto::lore::revision::v1::BranchCreateRequest;
    use lore_revision::lore::BranchId;
    use lore_revision::lore::RepositoryId;
    use lore_telemetry::InstrumentProvider;
    use lore_transport::grpc::REPOSITORY_ID_KEY;
    use opentelemetry::KeyValue;
    use rand::random;
    use tonic::Request;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::hooks::HookDispatcher;
    use crate::notification::testing::MockNotificationSender;
    use crate::store::test_store_create;

    fn allow_all_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(AllowAllRepositoryAuthorizer)
    }

    struct TestInstrumentProvider {}

    impl InstrumentProvider for TestInstrumentProvider {
        fn namespace(&self) -> &'static str {
            "test"
        }
        fn labels(&self) -> &[KeyValue] {
            &[]
        }
    }

    fn make_forwarded_request(
        repository: RepositoryId,
        branch_id: BranchId,
        name: &str,
    ) -> Request<BranchCreateRequest> {
        CallerContext {
            repository_id: repository,
            user_id: "alice".into(),
            correlation_id: String::new(),
            authorization: None,
        }
        .to_forwarded_request(BranchCreateRequest {
            id: branch_id.into(),
            name: name.into(),
            creator: Some("alice".into()),
            category: "default".into(),
            stack: vec![],
        })
        .expect("CallerContext::to_forwarded_request failed in test")
    }

    #[tokio::test]
    async fn missing_user_id_returns_internal_error() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let notification_sender = Arc::new(MockNotificationSender::new());
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            // No on-behalf-of-user-id set in metadata
            let mut request = Request::new(BranchCreateRequest {
                id: BranchId::from(uuid::Uuid::now_v7()).into(),
                name: "main".into(),
                creator: Some("alice".into()),
                category: "default".into(),
                stack: vec![],
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let hook_dispatcher = HookDispatcher::empty();
            let err = handler(
                request,
                immutable_store,
                mutable_store,
                notification_sender,
                &hook_dispatcher,
                &instrument_provider,
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

    // Happy and Unhappy paths that whatever the original `branch_create` implementation
    // returns is forwarded on to the forwarded handler
    mod base_branch_create_handler {
        use super::*;

        #[tokio::test]
        async fn create_returns_full_branch_record() {
            let repository = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            let mut notification_sender = MockNotificationSender::new();
            notification_sender
                .expect_branch_created()
                .return_once(|_, _| ());
            let notification_sender = Arc::new(notification_sender);
            let instrument_provider = TestInstrumentProvider {};

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let branch_id = BranchId::from(uuid::Uuid::now_v7());
                let hook_dispatcher = HookDispatcher::empty();

                let response = handler(
                    make_forwarded_request(repository, branch_id, "main"),
                    immutable_store,
                    mutable_store,
                    notification_sender,
                    &hook_dispatcher,
                    &instrument_provider,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");

                let branch = response
                    .into_inner()
                    .branch
                    .expect("response should include Branch");
                assert_eq!(branch.name, "main");
                assert_eq!(branch.creator, "alice");
                assert_eq!(branch.category, "default");
                assert!(!branch.deleted);
                assert!(branch.created > 0);
                assert!(!branch.id.is_empty());
                assert!(!branch.metadata.is_empty());
            }))
            .await;
        }

        #[tokio::test]
        async fn duplicate_id_returns_already_exists() {
            let repository = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            let mut notification_sender = MockNotificationSender::new();
            notification_sender
                .expect_branch_created()
                .return_once(|_, _| ());
            let notification_sender = Arc::new(notification_sender);
            let instrument_provider = TestInstrumentProvider {};

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let branch_id = BranchId::from(uuid::Uuid::now_v7());
                let hook_dispatcher = HookDispatcher::empty();

                handler(
                    make_forwarded_request(repository, branch_id, "main"),
                    immutable_store.clone(),
                    mutable_store.clone(),
                    notification_sender.clone(),
                    &hook_dispatcher,
                    &instrument_provider,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("first create should succeed");

                let err = handler(
                    make_forwarded_request(repository, branch_id, "main"),
                    immutable_store,
                    mutable_store,
                    notification_sender,
                    &hook_dispatcher,
                    &instrument_provider,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect_err("duplicate id should fail");

                assert_eq!(err.code(), tonic::Code::AlreadyExists);
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
    /// `forwarded_revision/v1/branch_get.rs`'s `authorization` test module.
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
        const SIGNING_SECRET: &str = "forwarded-branch-create-test-secret";
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
        ) -> Request<BranchCreateRequest> {
            CallerContext {
                repository_id: repository,
                user_id: "alice".into(),
                correlation_id: String::new(),
                authorization: Some(format!("Bearer {token}")),
            }
            .to_forwarded_request(BranchCreateRequest {
                id: branch_id.into(),
                name: "main".into(),
                creator: Some("alice".into()),
                category: "default".into(),
                stack: vec![],
            })
            .expect("CallerContext::to_forwarded_request failed in test")
        }

        /// A forwarded caller lacking `push` is denied.
        #[tokio::test]
        async fn denies_forwarded_caller_without_push_action() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");
            let notification_sender = Arc::new(MockNotificationSender::new());
            let instrument_provider = TestInstrumentProvider {};

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let hook_dispatcher = HookDispatcher::empty();
                let token = make_jwt(Some(vec!["read".to_string()]));
                let err = handler(
                    make_forwarded_request_with_token(repository, branch_id, token),
                    immutable_store,
                    mutable_store,
                    notification_sender,
                    &hook_dispatcher,
                    &instrument_provider,
                    Some(verifier()),
                    tier1_authorizer(),
                )
                .await
                .expect_err("a forwarded caller without push must be denied");
                assert_eq!(err.code(), tonic::Code::PermissionDenied);
            }))
            .await;
        }

        /// A forwarded caller holding `push` succeeds.
        #[tokio::test]
        async fn allows_forwarded_caller_with_push_action() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");
            let mut notification_sender = MockNotificationSender::new();
            notification_sender
                .expect_branch_created()
                .return_once(|_, _| ());
            let notification_sender = Arc::new(notification_sender);
            let instrument_provider = TestInstrumentProvider {};

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let hook_dispatcher = HookDispatcher::empty();
                let token = make_jwt(Some(vec!["push".to_string()]));
                handler(
                    make_forwarded_request_with_token(repository, branch_id, token),
                    immutable_store,
                    mutable_store,
                    notification_sender,
                    &hook_dispatcher,
                    &instrument_provider,
                    Some(verifier()),
                    tier1_authorizer(),
                )
                .await
                .expect("a forwarded caller holding push should be allowed");
            }))
            .await;
        }

        /// No `jwt_verifier` configured (`None`): the deployment has no
        /// `[server.auth]` at all, so the forwarded request is never
        /// verified and `AllowAllRepositoryAuthorizer` keeps working exactly
        /// as it does for the local, non-forwarded path.
        #[tokio::test]
        async fn no_verifier_configured_allows_forwarded_create() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");
            let mut notification_sender = MockNotificationSender::new();
            notification_sender
                .expect_branch_created()
                .return_once(|_, _| ());
            let notification_sender = Arc::new(notification_sender);
            let instrument_provider = TestInstrumentProvider {};

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let hook_dispatcher = HookDispatcher::empty();
                handler(
                    make_forwarded_request(repository, branch_id, "main"),
                    immutable_store,
                    mutable_store,
                    notification_sender,
                    &hook_dispatcher,
                    &instrument_provider,
                    None,
                    allow_all_authorizer(),
                )
                .await
                .expect("no verifier configured keeps the gate open");
            }))
            .await;
        }
    }
}
