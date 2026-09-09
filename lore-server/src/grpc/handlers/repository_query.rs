// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;
use std::sync::Arc;

use lore_base::error::RepositoryNotFound;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_error_set::prelude::*;
use lore_proto::RepositoryQueryRequest;
use lore_proto::RepositoryQueryResponse;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryError;
use lore_transport::RepositoryData;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::info;
use tracing::warn;

use crate::auth::jwt::AuthorizationToken;
use crate::authnz::repository_authorizer::READ_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::extract_authorization_header;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_user_id;
use crate::util::setup_execution;

#[tracing::instrument(name = "RepositoryQuery::handle", skip_all)]
pub async fn handler(
    request: Request<RepositoryQueryRequest>,
    authorizer: Arc<dyn RepositoryAuthorizer>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
) -> Result<Response<RepositoryQueryResponse>, Status> {
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let token = get_authorization(request.extensions()).ok();
    let raw_token = extract_authorization_header(&request);
    let req = request.into_inner();

    let Some(query) = req.query else {
        return Err(Status::invalid_argument("Invalid query"));
    };

    let execution = setup_execution(module_path!(), correlation_id, user_id);

    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        RepositoryId::default(),
    ));

    LORE_CONTEXT
        .scope(execution, async move {
            let repository = match query {
                lore_proto::repository_query_request::Query::Id(id) => {
                    let id: RepositoryId = Context::from(id).into();
                    repository_query_id(
                        repository.clone(),
                        id,
                        authorizer,
                        token.as_ref(),
                        raw_token.as_deref(),
                    )
                    .await
                    .filter_slow_down()?
                    .map_err(|err| {
                        warn!("Repository ID {id} not known: {err}",);
                        Status::not_found(err.to_string())
                    })?
                }
                lore_proto::repository_query_request::Query::Name(name) => repository_query_name(
                    repository.clone(),
                    name.as_str(),
                    authorizer,
                    token.as_ref(),
                    raw_token.as_deref(),
                )
                .await
                .filter_slow_down()?
                .map_err(|err| {
                    warn!("Repository name {name} not known: {err}");
                    Status::not_found(err.to_string())
                })?,
            };
            Ok(Response::new(RepositoryQueryResponse {
                repository: Some(lore_proto::Repository {
                    id: repository.id.into(),
                    name: repository.name,
                    metadata: repository.metadata.into(),
                }),
            }))
        })
        .await
}

#[allow(clippy::map_err_ignore)]
pub async fn repository_query_id(
    repository: Arc<RepositoryContext>,
    id: RepositoryId,
    authorizer: Arc<dyn RepositoryAuthorizer>,
    token: Option<&AuthorizationToken>,
    raw_token: Option<&str>,
) -> Result<RepositoryData, RepositoryError> {
    check_repository_query_authorization(&authorizer, token, raw_token, id)
        .await
        .map_err(|status| {
            warn!("User authorization failed: {status}");
            RepositoryError::from(RepositoryNotFound {
                repository: id.to_string(),
            })
        })?;

    let repository = Arc::new(repository.to_server_context(id));
    let metadata_hash = repository::metadata_hash(repository.clone())
        .await
        .forward_with::<RepositoryError, _>(|| {
            format!("Repository {id} metadata hash not found")
        })?;
    let metadata = repository::metadata(repository.clone(), metadata_hash)
        .await
        .forward_with::<RepositoryError, _>(|| format!("Repository {id} metadata not found"))?;

    // Verify the name -> ID mapping resolves back to the same ID, repair if missing
    let name_repository = Arc::new(repository.to_server_context(RepositoryId::default()));
    match repository::id_from_name(name_repository, &metadata.name).await {
        Ok(resolved_id) if resolved_id != id => {
            warn!(
                "Repository {} name {} maps to different repository {}, returning not found",
                id, metadata.name, resolved_id
            );
            return Err(RepositoryError::from(RepositoryNotFound {
                repository: id.to_string(),
            }));
        }
        Err(_) => {
            info!(
                "Repairing missing name -> ID mapping: {} -> {}",
                metadata.name, id
            );
            // no filter_slow_down()? usage here: repairing the name mapping is
            // best-effort, and the repository has already been resolved.
            let _ = repository::store_name_to_id(repository.clone(), &metadata.name, id)
                .await
                .inspect_err(|err| warn!("Failed to repair name -> ID mapping: {err}"));
        }
        Ok(_) => {}
    }

    info!("Repository query ID {id} found {metadata:?}");
    Ok(RepositoryData {
        id,
        name: metadata.name,
        metadata: metadata_hash,
    })
}

#[allow(clippy::map_err_ignore)]
pub async fn repository_query_name(
    repository: Arc<RepositoryContext>,
    name: &str,
    authorizer: Arc<dyn RepositoryAuthorizer>,
    token: Option<&AuthorizationToken>,
    raw_token: Option<&str>,
) -> Result<RepositoryData, RepositoryError> {
    // If the name is a parseable context ID, use the query-by-ID path directly
    if let Ok(id) = RepositoryId::from_str(name) {
        return repository_query_id(repository, id, authorizer, token, raw_token).await;
    }

    let name_repository = Arc::new(repository.to_server_context(RepositoryId::default()));
    let id = repository::id_from_name(name_repository, name).await?;

    check_repository_query_authorization(&authorizer, token, raw_token, id)
        .await
        .map_err(|status| {
            warn!("User authorization failed: {status}");
            RepositoryError::from(RepositoryNotFound {
                repository: name.to_string(),
            })
        })?;

    let repository = Arc::new(repository.to_server_context(id));
    let metadata_hash = repository::metadata_hash(repository.clone())
        .await
        .forward_with::<RepositoryError, _>(|| {
            format!("Repository {name} metadata hash not found")
        })?;
    let metadata = repository::metadata(repository.clone(), metadata_hash)
        .await
        .forward_with::<RepositoryError, _>(|| format!("Repository {name} metadata not found"))?;

    // Verify the metadata name matches the queried name — if not, the name -> ID mapping is stale
    if metadata.name != name {
        warn!(
            "Stale name -> ID mapping: {} maps to {} but metadata name is {}, deleting mapping",
            name, id, metadata.name
        );
        let _ = repository::delete_name_to_id(repository.clone(), name)
            .await
            .inspect_err(|err| warn!("Failed to delete stale name -> ID mapping: {err}"));
        return Err(RepositoryError::from(RepositoryNotFound {
            repository: name.to_string(),
        }));
    }

    info!("Repository query name {name} found {metadata:?}");
    Ok(RepositoryData {
        id,
        name: metadata.name,
        metadata: metadata_hash,
    })
}

/// Moved onto the configured authorizer (LEP
/// 2026-08-20-oidc-oauth2-authentication, D9): this used to construct
/// `AuthClientAuthorizer` directly and skip the check entirely when no
/// legacy `auth_url` was configured, which denied nothing under Tier 1 or
/// Tier 2 but also granted nothing extra — it simply never asked. Going
/// through the injected authorizer instead means a Tier 1 deployment's
/// `GlobalGrantsAuthorizer` actually answers this (any authenticated
/// caller reaches every partition), while `AllowAllRepositoryAuthorizer`
/// and legacy `AuthClientAuthorizer` behave exactly as before.
///
/// Asks `Some(READ_ACTION)` rather than plain reachability (`None`): the
/// LEP's own migration-plan table groups v0's `RepositoryQuery` and v1's
/// `RepositoryGet` (local and forwarded) together as one enforcement point
/// requiring `read`, since all three are conceptually the same "look up a
/// repository" operation. This is the single shared function all three
/// funnel through, so the requirement is applied uniformly to all of them
/// here rather than at each call site. It is deliberately *not* used for
/// `RepositoryCreate`'s/`RepositoryDelete`'s own internal existence-lookup
/// calls to `repository_query_id`/`repository_query_name`/
/// `repository_load_id`/`repository_load_name` — those pass
/// `AllowAllRepositoryAuthorizer` directly for that internal lookup, which
/// ignores the action (and everything else) regardless of what is asked
/// here.
pub(crate) async fn check_repository_query_authorization(
    authorizer: &Arc<dyn RepositoryAuthorizer>,
    token: Option<&AuthorizationToken>,
    raw_token: Option<&str>,
    repository_id: RepositoryId,
) -> Result<(), Status> {
    let verified_token =
        token.map(|claims| VerifiedToken::new(raw_token.unwrap_or_default(), claims));
    authorizer
        .check_repository_access(verified_token.as_ref(), repository_id, Some(READ_ACTION))
        .await
}

#[cfg(test)]
mod tests {
    use lore_base::types::Context;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;
    use crate::authnz::repository_authorizer::PUSH_ACTION;

    fn repository() -> RepositoryId {
        Context::from([7u8; 16]).into()
    }

    fn token_with_groups(groups: Vec<String>) -> AuthorizationToken {
        AuthorizationToken {
            groups: Some(groups),
            ..Default::default()
        }
    }

    /// `RepositoryQuery` (v0), `RepositoryGet` (v1, local and forwarded) all
    /// funnel through this one function, which now asks for `read`
    /// specifically rather than plain reachability.
    #[tokio::test]
    async fn a_read_holding_token_is_authorized() {
        let authorizer: Arc<dyn RepositoryAuthorizer> =
            Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string())));
        let claims = token_with_groups(vec![READ_ACTION.to_string()]);

        check_repository_query_authorization(&authorizer, Some(&claims), None, repository())
            .await
            .expect("a token holding read is authorized");
    }

    #[tokio::test]
    async fn a_push_only_token_is_denied() {
        let authorizer: Arc<dyn RepositoryAuthorizer> =
            Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string())));
        let claims = token_with_groups(vec![PUSH_ACTION.to_string()]);

        let err =
            check_repository_query_authorization(&authorizer, Some(&claims), None, repository())
                .await
                .expect_err("push does not imply read");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn an_authenticated_token_with_no_actions_is_denied() {
        // Before this change, plain reachability (`None`) was enough: any
        // authenticated principal reached every partition under Tier 1.
        // Now the token must actually hold `read`.
        let authorizer: Arc<dyn RepositoryAuthorizer> =
            Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string())));
        let claims = token_with_groups(vec![]);

        let err =
            check_repository_query_authorization(&authorizer, Some(&claims), None, repository())
                .await
                .expect_err("no actions held means read is not held either");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    /// `AllowAllRepositoryAuthorizer` ignores the action entirely, so a
    /// deployment with no `[server.auth]` configured is unaffected by the
    /// `read` requirement.
    #[tokio::test]
    async fn allow_all_is_unaffected() {
        let authorizer: Arc<dyn RepositoryAuthorizer> = Arc::new(AllowAllRepositoryAuthorizer);
        check_repository_query_authorization(&authorizer, None, None, repository())
            .await
            .expect("no auth configured always allows");
    }
}
