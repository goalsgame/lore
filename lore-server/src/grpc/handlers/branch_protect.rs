// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_proto::BranchProtectRequest;
use lore_proto::BranchProtectResponse;
use lore_revision::branch;
use lore_revision::lore::BranchId;
use lore_revision::repository::RepositoryContext;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::info;
use tracing::warn;

use crate::authnz::repository_authorizer::PUSH_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::extract_authorization_header;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_repository;
use crate::grpc::get_user_id;
use crate::util::setup_execution;

#[tracing::instrument(name = "BranchProtect::handle", skip_all)]
pub async fn handler(
    request: Request<BranchProtectRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
) -> Result<Response<BranchProtectResponse>, Status> {
    let repository_id = get_repository(request.metadata())?;
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();

    let authorization = extract_authorization_header(&request);
    let claims_for_authz = get_authorization(request.extensions()).ok();
    let verified_token = crate::grpc::verified_token(&claims_for_authz, &authorization);
    repository_authorizer
        .check_repository_access(verified_token.as_ref(), repository_id, Some(PUSH_ACTION))
        .await
        .map_err(|_err| Status::permission_denied("Permission denied"))?;

    let req = request.into_inner();
    let branch = BranchId::from(req.branch);

    info!("Handling branch protect in repository {repository_id}: branch {branch}");

    let execution = setup_execution(module_path!(), correlation_id, user_id);

    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        repository_id,
    ));

    LORE_CONTEXT
        .scope(execution, async move {
            match branch::protect(repository, branch).await.filter_slow_down()? {
                Ok(_) => {
                    info!("Branch protected in repository {repository_id}: branch {branch}");
                    Ok(Response::new(BranchProtectResponse {}))
                }
                Err(err) if err.is_branch_not_found() => {
                    info!("Failed to protect non-existent branch in repository {repository_id}: branch {branch}");
                    Err(Status::not_found(err.to_string()))
                }
                Err(err) => {
                    warn!("Failed to protect branch {branch} in repository {repository_id}: {err}");
                    Err(Status::internal(err.to_string()))
                }
            }
        })
        .await
}
