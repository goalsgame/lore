// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_proto::BranchGetRequest;
use lore_proto::BranchGetResponse;
use lore_revision::branch;
use lore_revision::lore::BranchId;
use lore_revision::repository::RepositoryContext;
use lore_telemetry::tracing::fields::BRANCH_ID;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::warn;

use crate::authnz::repository_authorizer::READ_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::extract_authorization_header;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_repository;
use crate::grpc::get_user_id;
use crate::util::setup_execution;

#[tracing::instrument(name = "BranchGet::handle", skip_all)]
pub async fn handler(
    request: Request<BranchGetRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
) -> Result<Response<BranchGetResponse>, Status> {
    let repository = get_repository(request.metadata())?;
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();

    let authorization = extract_authorization_header(&request);
    let claims_for_authz = get_authorization(request.extensions()).ok();
    let verified_token = crate::grpc::verified_token(&claims_for_authz, &authorization);
    repository_authorizer
        .check_repository_access(verified_token.as_ref(), repository, Some(READ_ACTION))
        .await
        .map_err(|_err| Status::permission_denied("Permission denied"))?;

    let req = request.into_inner();
    let branch = BranchId::from(req.branch);

    debug!({BRANCH_ID} = %branch, "Handling branch get request");

    let execution = setup_execution(module_path!(), correlation_id, user_id);

    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        repository,
    ));
    LORE_CONTEXT
        .scope(execution, async move {
            branch_get_handler(repository, branch).await
        })
        .await
}

async fn branch_get_handler(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
) -> Result<Response<BranchGetResponse>, Status> {
    let metadata = branch::metadata(repository.clone(), branch)
        .await
        .filter_slow_down()?
        .map_err(|err| {
            warn!("Failed to get branch metadata: {err}");
            Status::not_found(err.to_string())
        })?;

    let branch = branch::branch_metadata(repository.clone(), branch, &metadata)
        .await
        .filter_slow_down()?
        .map_err(|err| {
            warn!("Failed to resolve branch metadata: {err}");
            Status::not_found(err.to_string())
        })?;

    Ok(Response::new(BranchGetResponse {
        branch: Some(branch.into()),
    }))
}

#[cfg(test)]
mod tests {
    use lore_base::types::Context;
    use lore_base::types::Hash;
    use lore_revision::branch::BranchLatestStatus;
    use lore_transport::grpc::REPOSITORY_ID_KEY;
    use rand::random;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::grpc::get_write_token;
    use crate::store::test_store_create;

    fn allow_all_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(AllowAllRepositoryAuthorizer)
    }

    #[tokio::test]
    async fn test_handle() {
        let repository = random::<Context>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                let repository = Arc::new(RepositoryContext::new_server_context(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    repository.into(),
                ));
                let write_token = get_write_token();
                // Create the main branch (without parent)
                let main = lore_revision::branch::create(
                    repository.clone(),
                    &write_token,
                    BranchId::from(uuid::Uuid::now_v7()),
                    lore_revision::branch::DEFAULT_DEFAULT_NAME,
                    lore_revision::branch::default_category(),
                    "BranchCreator",
                    12345,
                    vec![],
                    false,
                    false,
                )
                .await
                .expect("Could not create main branch");

                // Create another branch1
                let payload_branch1 = random::<[u8; size_of::<Hash>()]>().to_vec();
                let hash_branch1 = Hash::hash_buffer(&payload_branch1);
                let branch1 = lore_revision::branch::create(
                    repository.clone(),
                    &write_token,
                    BranchId::from(uuid::Uuid::now_v7()),
                    "branch1",
                    lore_revision::branch::default_category(),
                    "BranchCreator",
                    12345,
                    vec![],
                    false,
                    false,
                )
                .await
                .expect("Could not create branch1 branch");

                // Try getting main
                let mut request = Request::new(BranchGetRequest {
                    branch: main.into(),
                });
                request.metadata_mut().insert_bin(
                    REPOSITORY_ID_KEY,
                    tonic::metadata::BinaryMetadataValue::from_bytes(repository.id.data()),
                );
                let response = handler(
                    request,
                    immutable_store.clone(),
                    mutable_store.clone(),
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");
                let response_branch = response
                    .into_inner()
                    .branch
                    .expect("Did not get branch metadata as expected");
                let expected = lore_proto::Branch {
                    id: main.into(),
                    name: lore_revision::branch::DEFAULT_DEFAULT_NAME.to_string(),
                    category: lore_revision::branch::default_category().to_string(),
                    latest: Hash::default().into(),
                    parent_deprecated: Some(Context::default().into()),
                    branch_point_deprecated: Some(Hash::default().into()),
                    creator: "BranchCreator".to_string(),
                    created: 12345,
                    stack: vec![],
                };
                assert_eq!(response_branch, expected);

                // Update branch1 branch
                branch::store_latest(
                    repository.clone(),
                    branch1,
                    Hash::default(),
                    hash_branch1,
                    BranchLatestStatus::Convergent,
                )
                .await
                .expect("Failed to store head");

                // Try getting branch1
                let mut request = Request::new(BranchGetRequest {
                    branch: branch1.into(),
                });
                request.metadata_mut().insert_bin(
                    REPOSITORY_ID_KEY,
                    tonic::metadata::BinaryMetadataValue::from_bytes(repository.id.data()),
                );
                let response = handler(
                    request,
                    immutable_store.clone(),
                    mutable_store.clone(),
                    allow_all_authorizer(),
                )
                .await
                .expect("Request failed");
                let response_branch = response
                    .into_inner()
                    .branch
                    .expect("Did not get branch metadata as expected");
                let expected = lore_proto::Branch {
                    id: branch1.into(),
                    name: "branch1".to_string(),
                    category: lore_revision::branch::default_category().to_string(),
                    latest: hash_branch1.into(),
                    parent_deprecated: Some(Context::default().into()),
                    branch_point_deprecated: Some(Hash::default().into()),
                    creator: "BranchCreator".to_string(),
                    created: 12345,
                    stack: vec![],
                };
                assert_eq!(response_branch, expected);
            })
            .await;
    }

    mod authorization {
        use lore_revision::lore::RepositoryId;

        use super::*;
        use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;

        fn roles_authorizer() -> Arc<dyn RepositoryAuthorizer> {
            Arc::new(GlobalGrantsAuthorizer::new(Some("roles".to_string())))
        }

        fn make_request(
            repository: RepositoryId,
            branch: BranchId,
            roles: &[&str],
        ) -> Request<BranchGetRequest> {
            let mut request = Request::new(BranchGetRequest {
                branch: branch.into(),
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );
            let serde_json::Value::Object(extra) = serde_json::json!({ "roles": roles }) else {
                unreachable!()
            };
            request
                .extensions_mut()
                .insert(crate::auth::jwt::AuthorizationToken {
                    user_id: "ci-bot".into(),
                    extra,
                    ..crate::auth::jwt::AuthorizationToken::default()
                });
            request
        }

        /// A token holding `read` succeeds — `BranchGet` is classified as a
        /// read operation.
        #[tokio::test]
        async fn allows_caller_with_read_action() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            LORE_CONTEXT
                .scope(execution.clone(), async move {
                    let repository_context = Arc::new(RepositoryContext::new_server_context(
                        immutable_store.clone(),
                        mutable_store.clone(),
                        repository,
                    ));
                    let write_token = get_write_token();
                    lore_revision::branch::create(
                        repository_context,
                        &write_token,
                        branch_id,
                        "test-branch",
                        lore_revision::branch::default_category(),
                        "creator",
                        1,
                        vec![],
                        false,
                        false,
                    )
                    .await
                    .expect("Failed to create branch");

                    handler(
                        make_request(repository, branch_id, &["read"]),
                        immutable_store,
                        mutable_store,
                        roles_authorizer(),
                    )
                    .await
                    .expect("a caller holding read should be allowed to BranchGet");
                })
                .await;
        }

        /// A token holding only `push` (not `read`) is denied — `push` does
        /// not imply `read`.
        #[tokio::test]
        async fn denies_caller_with_only_push_action() {
            let repository = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            LORE_CONTEXT
                .scope(execution.clone(), async move {
                    let err = handler(
                        make_request(repository, branch_id, &["push"]),
                        immutable_store,
                        mutable_store,
                        roles_authorizer(),
                    )
                    .await
                    .expect_err("a caller without read must be denied");
                    assert_eq!(err.code(), tonic::Code::PermissionDenied);
                })
                .await;
        }
    }
}
