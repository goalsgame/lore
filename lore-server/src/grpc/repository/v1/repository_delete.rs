// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_proto::lore::repository::v1::RepositoryDeleteRequest;
use lore_proto::lore::repository::v1::RepositoryDeleteResponse;
use lore_revision::branch;
use lore_revision::lore::RepositoryId;
use lore_revision::lore::execution_context;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_telemetry::InstrumentProvider;
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::info;

use super::record::build_repository;
use super::repository_get::repository_load_id;
use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::ServerResultExt;
use crate::grpc::extract_authorization_header;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_user_id;
use crate::grpc::handlers::repository_delete::DELETE_ACTIONS;
use crate::grpc::handlers::repository_delete::repository_delete_auth_resource;
use crate::util::setup_execution;

/// `lore.repository.v1.RepositoryService.RepositoryDelete` handler.
///
/// Hard-deletes the repository: clears its name → id mapping, zeroes the
/// metadata pointer, and tears down all branch metadata/HEAD pointers.
/// The response carries the last-known repository record so the caller
/// can confirm what was deleted without a separate Get.
#[tracing::instrument(name = "RepositoryDelete::v1::handle", skip_all)]
pub async fn handler(
    request: Request<RepositoryDeleteRequest>,
    auth_url: Option<String>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<RepositoryDeleteResponse>, Status> {
    let user_info = get_authorization(request.extensions());
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let authorization = extract_authorization_header(&request);
    let req = request.into_inner();

    let id: RepositoryId = Context::from(req.id).into();
    let execution = setup_execution(module_path!(), correlation_id, user_id);
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        id,
    ));

    LORE_CONTEXT
        .scope(execution, async move {
            // The internal lookup is not the access decision — the checks
            // below are — so it must not itself be denied by whichever
            // authorizer the deployment configures for external callers.
            let (metadata, metadata_hash) = repository_load_id(
                repository.clone(),
                id,
                Arc::new(AllowAllRepositoryAuthorizer),
                None, /* token */
                None, /* raw_token */
            )
            .await
            .filter_slow_down()?
            .map_err(|_err| Status::not_found(format!("Repository {id} not found")))?;

            let user_id = execution_context().user_id().await;
            if let Some(auth_url) = auth_url {
                repository_delete_auth_resource(auth_url, authorization, id).await?;
            } else {
                // Replaces the `is_service_account` bypass of the creator
                // check (LEP 2026-08-20-oidc-oauth2-authentication, D4):
                // deletion is allowed for the creator, as before, or for a
                // caller who explicitly holds `owner` or `admin` through the
                // configured authorizer.
                let is_creator = metadata.creator == user_id;
                let holds_delete_action = match user_info.as_ref() {
                    Ok(claims) => {
                        let raw = authorization.as_deref().unwrap_or_default();
                        let verified_token = VerifiedToken::new(raw, claims);
                        let mut holds_any = false;
                        for action in DELETE_ACTIONS {
                            if repository_authorizer
                                .check_repository_access(
                                    Some(&verified_token),
                                    id,
                                    Some(action),
                                )
                                .await
                                .is_ok()
                            {
                                holds_any = true;
                                break;
                            }
                        }
                        holds_any
                    }
                    Err(_) => false,
                };

                if !is_creator && !holds_delete_action {
                    info!(
                        "Repository delete refused, user {user_id} is not creator {} and holds neither owner nor admin",
                        metadata.creator
                    );
                    return Err(Status::permission_denied("Not repository owner"));
                }
            }

            repository::store_name_to_id(
                repository.clone(),
                metadata.name.as_str(),
                RepositoryId::default(),
            )
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!("Failed to delete repository name mapping: {err}"))
            })?;

            repository::metadata_store_hash(repository.clone(), Hash::default())
                .await
                .filter_slow_down()?
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to delete repository metadata: {err}"))
                })?;

            // no filter_slow_down()? usage here: the repository record is
            // already torn down above, so this purge is past the point of no
            // return. A retryable status would invite a retry that only finds
            // the repository gone, leaving these keys orphaned.
            if let Ok(mut branch_stream) = branch::list(repository.clone()).await {
                let mut branch_list = vec![];
                while let Some(branch) = branch_stream.next().await {
                    branch_list.push(branch);
                }

                for branch in branch_list {
                    if let Ok(branch_metadata) = branch::metadata(repository.clone(), branch).await
                    {
                        let name = branch::name(&branch_metadata).unwrap_or_default();
                        if !name.is_empty() {
                            let _ = branch::delete_name_to_id(repository.clone(), name)
                                .await
                                .inspect_err(|err| {
                                    debug!(
                                        "Branch delete failed to remove name to ID mapping: {err}"
                                    );
                                });
                        }
                    }

                    let _ = branch::mutable_delete(repository.clone(), branch::LATEST, branch)
                        .await
                        .inspect_err(|err| {
                            debug!("Branch delete failed to remove HEAD pointer: {err}");
                        });

                    let _ = branch::mutable_delete(repository.clone(), branch::METADATA, branch)
                        .await
                        .inspect_err(|err| {
                            debug!("Branch delete failed to remove metadata pointer: {err}");
                        });
                }
            }

            info!(
                "Deleted repository {} with ID {}",
                metadata.name, repository.id
            );

            instrument_provider
                .counter("num_repositories_deleted")
                .add(1, &[]);

            Ok(Response::new(RepositoryDeleteResponse {
                repository: Some(build_repository(id, &metadata, metadata_hash)),
            }))
        })
        .await
}
