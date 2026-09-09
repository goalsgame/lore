// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryCreateResponse;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::forwarded_requests::CallerContext;
use crate::grpc::repository::v1::repository_create::repository_create_implementation;
use crate::hooks::HookDispatcher;

/// Handler that takes a `RepositoryCreate` request forwarded on from peer's `RepositoryService`
/// and executes it, returning the result to the other server for forwarding on to its
/// client
///
/// This peer-to-peer path predates `RepositoryAuthorizer`, same as
/// `repository_get.rs`'s forwarded handler (see its doc comment for the full
/// reasoning). The receiving server — this one — is the only side that can
/// run a meaningful `push` check, so it must verify the forwarded raw
/// bearer token (`CallerContext::authorization`) itself into real claims
/// rather than trusting the originating server's decision or fabricating an
/// already-authenticated token.
#[tracing::instrument(name = "ForwardedRepository::v1::RepositoryCreate::Handler", skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn handler(
    request: Request<RepositoryCreateRequest>,
    auth_url: Option<String>,
    jwt_verifier: Option<JwtVerifier>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hook_dispatcher: &HookDispatcher,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<RepositoryCreateResponse>, Status> {
    let caller_context = CallerContext::from_forwarded_request(&request)?;

    // Mirrors what `JWTAuthnInterceptor` does for a directly-received
    // request, and exactly what `forwarded_repository/v1/repository_get.rs`
    // does for the same reason: reject outright when this deployment has
    // auth configured and the caller did not present a token that
    // verifies, rather than falling back to an anonymous, and therefore
    // potentially over-permissive, check.
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

    repository_create_implementation(
        request.into_inner(),
        caller_context,
        token,
        repository_authorizer,
        auth_url,
        immutable_store,
        mutable_store,
        hook_dispatcher,
        instrument_provider,
    )
    .await
}

#[cfg(test)]
mod test {
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::Context;
    use lore_revision::lore::RepositoryId;
    use lore_telemetry::InstrumentProvider;
    use rand::random;
    use tonic::Request;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::hooks::HookDispatcher;
    use crate::store::test_store_create;

    fn allow_all() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(AllowAllRepositoryAuthorizer)
    }

    struct TestInstrumentProvider;

    impl InstrumentProvider for TestInstrumentProvider {
        fn namespace(&self) -> &'static str {
            "test"
        }
    }

    fn make_body(repository_id: RepositoryId, name: &str) -> RepositoryCreateRequest {
        let id_bytes: Context = repository_id.into();
        RepositoryCreateRequest {
            id: bytes::Bytes::from(id_bytes),
            name: name.into(),
            description: String::new(),
            default_branch_id: bytes::Bytes::from(Context::from(uuid::Uuid::now_v7())),
            default_branch_name: "main".into(),
            creator: Some("alice".into()),
        }
    }

    fn make_forwarded_request(
        repository_id: RepositoryId,
        name: &str,
    ) -> Request<RepositoryCreateRequest> {
        CallerContext {
            repository_id,
            user_id: "alice".into(),
            correlation_id: String::new(),
            authorization: None,
        }
        .to_forwarded_request(make_body(repository_id, name))
        .expect("CallerContext::to_forwarded_request failed in test")
    }

    #[tokio::test]
    async fn missing_user_id_returns_internal_error() {
        let repository_id = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            // Deliberately omit on-behalf-of-user-id to test the missing-field error
            let request = Request::new(make_body(repository_id, "my-repo"));

            let hook_dispatcher = HookDispatcher::empty();
            let err = handler(
                request,
                None,
                None,
                allow_all(),
                immutable_store,
                mutable_store,
                &hook_dispatcher,
                &TestInstrumentProvider,
            )
            .await
            .expect_err("missing user id should fail");

            assert_eq!(err.code(), tonic::Code::Internal);
            assert!(err.message().contains("on-behalf-of-user-id"));
        }))
        .await;
    }

    // Happy and unhappy paths verify that whatever the underlying
    // `repository_create_implementation` returns is forwarded on correctly.
    mod base_repository_create_handler {
        use super::*;

        #[tokio::test]
        async fn create_returns_full_repository_record() {
            let repository_id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let hook_dispatcher = HookDispatcher::empty();

                let response = handler(
                    make_forwarded_request(repository_id, "my-repo"),
                    None, /* no auth */
                    None,
                    allow_all(),
                    immutable_store,
                    mutable_store,
                    &hook_dispatcher,
                    &TestInstrumentProvider,
                )
                .await
                .expect("Request failed");

                let repo = response
                    .into_inner()
                    .repository
                    .expect("response should include Repository");
                assert_eq!(repo.name, "my-repo");
                assert_eq!(repo.creator, "alice");
                assert!(repo.created > 0);
                assert!(!repo.id.is_empty());
            }))
            .await;
        }

        #[tokio::test]
        async fn duplicate_id_returns_already_exists() {
            let repository_id = random::<RepositoryId>();
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let hook_dispatcher = HookDispatcher::empty();

                handler(
                    make_forwarded_request(repository_id, "my-repo"),
                    None,
                    None,
                    allow_all(),
                    immutable_store.clone(),
                    mutable_store.clone(),
                    &hook_dispatcher,
                    &TestInstrumentProvider,
                )
                .await
                .expect("first create should succeed");

                // Same id, different name — should be AlreadyExists
                let err = handler(
                    make_forwarded_request(repository_id, "other-name"),
                    None,
                    None,
                    allow_all(),
                    immutable_store,
                    mutable_store,
                    &hook_dispatcher,
                    &TestInstrumentProvider,
                )
                .await
                .expect_err("duplicate id with different name should fail");
                assert_eq!(err.code(), tonic::Code::AlreadyExists);
            }))
            .await;
        }
    }
}
