// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_server::grpc::handlers::repository_create::*;
use tonic::Code;

mod input_length_validation {
    use lore_revision::repository;

    use super::*;

    #[test]
    fn accepts_valid_input() {
        validate_create_input("my-repo", "a description", "main", "alice")
            .expect("valid input should pass");
    }

    #[test]
    fn accepts_name_at_max_length() {
        let name = "a".repeat(repository::MAX_NAME_LEN);
        validate_create_input(&name, "desc", "main", "alice")
            .expect("name at exactly MAX_NAME_LEN should pass");
    }

    #[test]
    fn rejects_oversized_repository_name() {
        let long_name = "a".repeat(repository::MAX_NAME_LEN + 1);
        let err = validate_create_input(&long_name, "desc", "main", "alice")
            .expect_err("should reject oversized name");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message()
                .contains("Repository name exceeds maximum length")
        );
    }

    #[test]
    fn rejects_oversized_description() {
        let long_desc = "a".repeat(repository::MAX_DESCRIPTION_LEN + 1);
        let err = validate_create_input("my-repo", &long_desc, "main", "alice")
            .expect_err("should reject oversized description");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("description exceeds maximum length"));
    }

    #[test]
    fn rejects_oversized_branch_name() {
        let long_branch = "a".repeat(repository::MAX_NAME_LEN + 1);
        let err = validate_create_input("my-repo", "desc", &long_branch, "alice")
            .expect_err("should reject oversized branch name");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("Branch name exceeds maximum length"));
    }

    #[test]
    fn rejects_oversized_creator() {
        let long_creator = "a".repeat(repository::MAX_NAME_LEN + 1);
        let err = validate_create_input("my-repo", "desc", "main", &long_creator)
            .expect_err("should reject oversized creator");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("Creator exceeds maximum length"));
    }
}

mod name_scoped_token {
    use std::sync::Arc;

    use lore_base::runtime::LORE_CONTEXT;
    use lore_revision::lore::RepositoryId;
    use lore_server::auth::jwt::AuthorizationToken;
    use lore_server::auth::jwt::ResourcePermission;
    use lore_server::authnz::repository_authorizer::RawToken;
    use lore_server::authnz::repository_registry::RepositoryRegistry;
    use lore_server::hooks::HookDispatcher;
    use lore_telemetry::InstrumentProvider;
    use opentelemetry::KeyValue;
    use tonic::Request;

    use super::*;
    use crate::store::test_support::test_store_create;

    struct TestInstrumentProvider {}

    impl InstrumentProvider for TestInstrumentProvider {
        fn namespace(&self) -> &'static str {
            "test"
        }

        fn labels(&self) -> &[KeyValue] {
            &[]
        }
    }

    fn scoped_request(name: &str) -> Request<lore_proto::RepositoryCreateRequest> {
        let mut request = Request::new(lore_proto::RepositoryCreateRequest {
            id: rand::random::<RepositoryId>().into(),
            name: name.to_string(),
            description: String::new(),
            default_branch_id: uuid::Uuid::now_v7().as_bytes().to_vec().into(),
            default_branch_name: "main".to_string(),
            creator: "creator".to_string(),
            created: 1,
        });
        request.extensions_mut().insert(AuthorizationToken {
            resources: Some(vec![ResourcePermission {
                resource_id: "urc-name:goalables/*".to_string(),
                permission: vec!["create".to_string()],
            }]),
            ..Default::default()
        });
        request
            .extensions_mut()
            .insert(RawToken("raw.jwt".to_string()));
        request
    }

    #[tokio::test]
    async fn denies_disallowed_name_before_any_write() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let instrument_provider = TestInstrumentProvider {};
        let hook_dispatcher = HookDispatcher::empty();

        LORE_CONTEXT
            .scope(execution, async move {
                let request = scoped_request("game");
                let id = request.get_ref().id.clone();
                let err = handler(
                    request,
                    &RepositoryRegistry::Unregistered,
                    immutable_store.clone(),
                    mutable_store.clone(),
                    &hook_dispatcher,
                    &instrument_provider,
                )
                .await
                .expect_err("scoped token must not create 'game'");
                assert_eq!(err.code(), Code::PermissionDenied);

                let repository: RepositoryId = lore_base::types::Context::from(id).into();
                let context = Arc::new(
                    lore_revision::repository::RepositoryContext::new_server_context(
                        immutable_store,
                        mutable_store,
                        repository,
                    ),
                );
                assert!(
                    lore_server::grpc::handlers::repository_query::repository_query_id(
                        context, repository, None, None,
                    )
                    .await
                    .is_err(),
                    "no repository may be written"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn allows_matching_name() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let instrument_provider = TestInstrumentProvider {};
        let hook_dispatcher = HookDispatcher::empty();

        LORE_CONTEXT
            .scope(execution, async move {
                handler(
                    scoped_request("goalables/app"),
                    &RepositoryRegistry::Unregistered,
                    immutable_store,
                    mutable_store,
                    &hook_dispatcher,
                    &instrument_provider,
                )
                .await
                .expect("scoped token may create a matching name");
            })
            .await;
    }
}
