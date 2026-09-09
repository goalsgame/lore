// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_revision::immutable;
use lore_revision::lore::RepositoryId;
use lore_revision::repository::RepositoryContext;
use lore_storage::options::WriteOptions;
use lore_transport::grpc::CORRELATION_ID_HEADER;
use serde::Serialize;
use tracing::debug;
use tracing::info;

use crate::auth::jwt::AuthorizationToken;
use crate::authnz::repository_authorizer::PUSH_ACTION;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::http::server::ServerState;
use crate::util::get_user_id_from_token;
use crate::util::setup_execution;

#[derive(Serialize)]
struct ResponseData {
    address: String,
}

#[derive(Serialize)]
struct ResponseSuccess {
    data: ResponseData,
}

/// The bearer token exactly as presented, without the `Bearer ` prefix.
/// Needed only so a legacy `AuthClientAuthorizer` can forward it to the auth
/// service; `jwt_axum_middleware` decodes it but does not retain the raw
/// form, so it is re-extracted here from the same header (mirrors
/// `presign_repository_content.rs::extract_bearer_token`).
fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
}

/// Baseline `push` requirement (closing the Tier 1 gap where any
/// authenticated caller could upload repository content with no group
/// membership at all). Mirrors
/// `presign_repository_content.rs::check_presign_permission`: with no
/// verifier configured, `user_info` is `None` and
/// `AllowAllRepositoryAuthorizer` ignores the action and permits it.
async fn check_push_permission(
    state: &ServerState,
    user_info: &Option<AuthorizationToken>,
    headers: &HeaderMap,
    repository: RepositoryId,
) -> Result<(), Response> {
    let verified_token = user_info.as_ref().map(|claims| {
        VerifiedToken::new(extract_bearer_token(headers).unwrap_or_default(), claims)
    });
    state
        .reachability_authorizer
        .authorizer
        .check_repository_access(verified_token.as_ref(), repository, Some(PUSH_ACTION))
        .await
        .map_err(|_err| {
            let mut header_error = HeaderMap::new();
            header_error.insert("content-type", HeaderValue::from_str("text/plain").unwrap());
            (
                StatusCode::FORBIDDEN,
                header_error,
                Body::from("caller does not hold the push action"),
            )
                .into_response()
        })
}

pub async fn handler(
    State(state): State<Arc<ServerState>>,
    Path(repository_id): Path<String>,
    Extension(user_info): Extension<Option<AuthorizationToken>>,
    headers: HeaderMap,
    data: Bytes,
) -> Response {
    info!("Put repository {} data {} bytes", repository_id, data.len());
    info!("User info: {:?}", user_info);

    let mut header_error = HeaderMap::new();
    header_error.insert("content-type", HeaderValue::from_str("text/plain").unwrap());

    let correlation_id = headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|header_value| header_value.to_str().map(str::to_string).ok())
        .unwrap_or_default();

    let parsed_repository_id = match repository_id.parse::<Context>() {
        Ok(id) => id,
        Err(error) => {
            debug!("Error parsing the repository ID {}", error);
            return (
                StatusCode::BAD_REQUEST,
                header_error,
                Body::from("Wrong repository"),
            )
                .into_response();
        }
    };

    if let Err(response) =
        check_push_permission(&state, &user_info, &headers, parsed_repository_id.into()).await
    {
        return response;
    }

    let user_id = get_user_id_from_token(user_info);
    let execution = setup_execution(module_path!(), correlation_id, user_id);
    LORE_CONTEXT
        .scope(execution, async move {
            let repository = Arc::new(RepositoryContext::new_server_context(
                state.immutable_store.clone(),
                state.mutable_store.clone(),
                parsed_repository_id.into(),
            ));

            let context = uuid::Uuid::now_v7().into();
            let address = match immutable::write(
                repository.clone(),
                context,
                data,
                WriteOptions::default().with_remote_write(),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    debug!("Failed to write into immutable store. {}", error);
                    return (
                        StatusCode::BAD_REQUEST,
                        header_error,
                        Body::from("Malformed data"),
                    )
                        .into_response();
                }
            };

            (
                StatusCode::OK,
                Json(ResponseSuccess {
                    data: ResponseData {
                        address: format!("{address}"),
                    },
                }),
            )
                .into_response()
        })
        .await
}

#[cfg(test)]
mod tests {
    use rand::random;
    use serde_json::json;

    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;
    use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
    use crate::http::server::create_router;
    use crate::store::test_store_create;

    fn state_with_authorizer(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        repository_authorizer: Arc<dyn crate::authnz::repository_authorizer::RepositoryAuthorizer>,
    ) -> ServerState {
        ServerState {
            immutable_store,
            mutable_store,
            jwt_verifier: None,
            reachability_authorizer: ReachabilityAuthorizer {
                authorizer: repository_authorizer,
                legacy_resource_claim: false,
            },
            max_file_size: 100,
            presign_config: None,
        }
    }

    #[tokio::test]
    async fn no_auth_configured_may_push() {
        let (immutable_store, mutable_store, _) =
            test_store_create().await.expect("Failed to create stores");
        let state = state_with_authorizer(
            immutable_store,
            mutable_store,
            Arc::new(AllowAllRepositoryAuthorizer),
        );
        check_push_permission(&state, &None, &HeaderMap::new(), random())
            .await
            .expect("AllowAllRepositoryAuthorizer keeps the gate open with no verifier");
    }

    #[tokio::test]
    async fn caller_holding_push_action_may_push() {
        let (immutable_store, mutable_store, _) =
            test_store_create().await.expect("Failed to create stores");
        let state = state_with_authorizer(
            immutable_store,
            mutable_store,
            Arc::new(GlobalGrantsAuthorizer::new(Some("roles".to_string()))),
        );
        let serde_json::Value::Object(extra) = json!({ "roles": ["push"] }) else {
            unreachable!()
        };
        let token = Some(AuthorizationToken {
            extra,
            ..Default::default()
        });
        check_push_permission(&state, &token, &HeaderMap::new(), random())
            .await
            .expect("a caller holding the push action may push");
    }

    #[tokio::test]
    async fn caller_without_push_action_may_not_push() {
        let (immutable_store, mutable_store, _) =
            test_store_create().await.expect("Failed to create stores");
        let state = state_with_authorizer(
            immutable_store,
            mutable_store,
            Arc::new(GlobalGrantsAuthorizer::new(Some("roles".to_string()))),
        );
        let token = Some(AuthorizationToken::default());
        let response = check_push_permission(&state, &token, &HeaderMap::new(), random())
            .await
            .expect_err("a caller not holding the push action may not push");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn router_denies_put_without_push_action() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        lore_base::runtime::LORE_CONTEXT
            .scope(execution, async move {
                let test_health = crate::http::server::ServerHealth::new_without_availability(
                    immutable_store.clone(),
                );
                let state = state_with_authorizer(
                    immutable_store,
                    mutable_store,
                    Arc::new(GlobalGrantsAuthorizer::new(Some("roles".to_string()))),
                );
                let settings = crate::http::server::LoreHttpServerSettings::test_default();
                let app = create_router(state, test_health, &settings);
                let test_server = axum_test::TestServer::new(app).unwrap();

                let repository = random::<Context>();
                let response = test_server
                    .put(&format!("/v1/repository/{repository}/content"))
                    .bytes(Bytes::from_static(b"hello"))
                    .await;

                assert_eq!(response.status_code(), StatusCode::FORBIDDEN);
            })
            .await;
    }
}
