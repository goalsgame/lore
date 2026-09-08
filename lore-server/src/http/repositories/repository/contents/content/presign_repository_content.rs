// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::SystemTime;
use std::time::SystemTimeError;
use std::time::UNIX_EPOCH;

use axum::Extension;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use hex::FromHexError;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_revision::lore::RepositoryId;
use lore_storage::StoreMatch;
use lore_storage::immutable_store::query_one;
use lore_transport::grpc::CORRELATION_ID_HEADER;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tracing::warn;

use crate::auth::jwt::AuthorizationToken;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::http::log_http_error;
use crate::http::presign_token::CURRENT_TOKEN_VERSION;
use crate::http::presign_token::PresignTokenPayload;
use crate::http::presign_token::sign;
use crate::http::server::ServerState;
use crate::util::get_user_id_from_token;
use crate::util::setup_execution;

/// The action that replaces the `is_service_account` gate on presigned URLs
/// (LEP 2026-08-20-oidc-oauth2-authentication, D4). One behavior is
/// preserved exactly: with no verifier configured, `token` is `None` below
/// and `AllowAllRepositoryAuthorizer::check_repository_access` ignores the
/// action and permits it — the presign gate stays open, as it is today.
const PRESIGN_ACTION: &str = "presign";

#[derive(Debug, Error)]
pub enum PresignError {
    #[error("Failed to parse repository: {0}")]
    ParseRepository(FromHexError),
    #[error("Failed to parse address: {0}")]
    ParseAddress(FromHexError),
    #[error("Presign feature is not configured")]
    NotConfigured,
    #[error("Caller does not hold the presign action")]
    PermissionDenied,
    #[error("content_type is not allowed: {0}")]
    DisallowedContentType(String),
    #[error("header value is not valid: {0}")]
    InvalidHeaderValue(String),
    #[error("Content not found")]
    NotFound,
    #[error("Store error checking content existence")]
    StoreError,
    #[error("System clock error: {0}")]
    SystemTime(SystemTimeError),
}

impl IntoResponse for PresignError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match &self {
            PresignError::ParseRepository(_)
            | PresignError::ParseAddress(_)
            | PresignError::DisallowedContentType(_)
            | PresignError::InvalidHeaderValue(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            PresignError::NotConfigured => (
                StatusCode::NOT_FOUND,
                "presigned URL feature is not enabled".to_string(),
            ),
            PresignError::PermissionDenied => (
                StatusCode::FORBIDDEN,
                "caller does not hold the presign action".to_string(),
            ),
            PresignError::StoreError | PresignError::SystemTime(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Something went wrong. See server log for more info.".to_string(),
            ),
            PresignError::NotFound => (StatusCode::NOT_FOUND, "address not found".to_string()),
        };

        log_http_error(&self, status);

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        (status, headers, msg).into_response()
    }
}

#[derive(Deserialize)]
pub struct PresignRequest {
    pub ttl_seconds: Option<u64>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
}

#[derive(Serialize)]
pub struct PresignResponse {
    pub url_suffix: String,
    pub expires_at: u64,
}

/// The bearer token exactly as presented, without the `Bearer ` prefix.
/// Needed only so a legacy `AuthClientAuthorizer` can forward it to the auth
/// service; `jwt_axum_middleware` decodes it but does not retain the raw
/// form, so it is re-extracted here from the same header.
fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
}

/// Checks that the caller holds the `presign` action on `repository`
/// (LEP 2026-08-20-oidc-oauth2-authentication, D4). Replaces the
/// `is_service_account` gate: with no verifier configured, `user_info` is
/// `None` and `AllowAllRepositoryAuthorizer` ignores the action and permits
/// it, which is what keeps the presign gate open exactly as it is today for
/// a deployment with no OIDC integration.
async fn check_presign_permission(
    state: &ServerState,
    user_info: &Option<AuthorizationToken>,
    headers: &HeaderMap,
    repository: RepositoryId,
) -> Result<(), PresignError> {
    let verified_token = user_info.as_ref().map(|claims| {
        VerifiedToken::new(extract_bearer_token(headers).unwrap_or_default(), claims)
    });
    state
        .reachability_authorizer
        .authorizer
        .check_repository_access(verified_token.as_ref(), repository, Some(PRESIGN_ACTION))
        .await
        .map_err(|_err| PresignError::PermissionDenied)
}

pub async fn handler(
    State(state): State<Arc<ServerState>>,
    Path((repository_id, address)): Path<(String, String)>,
    Extension(user_info): Extension<Option<AuthorizationToken>>,
    headers: HeaderMap,
    Json(body): Json<PresignRequest>,
) -> Result<impl IntoResponse, PresignError> {
    let presign_config = state
        .presign_config
        .as_ref()
        .ok_or(PresignError::NotConfigured)?
        .clone();

    let repository = repository_id
        .parse::<RepositoryId>()
        .map_err(PresignError::ParseRepository)?;
    let parsed_address = address
        .parse::<Address>()
        .map_err(PresignError::ParseAddress)?;

    check_presign_permission(&state, &user_info, &headers, repository).await?;

    // Fast-feedback rejection; redeem also enforces the allowlist for
    // already issued tokens.
    if let Some(content_type) = body.content_type.as_deref()
        && !presign_config
            .content_type_allowlist
            .is_allowed(content_type)
    {
        return Err(PresignError::DisallowedContentType(
            content_type.to_string(),
        ));
    }

    // Reject values redeem could not serialize into a response header, so a
    // token mint accepts is always one redeem can serve.
    for value in [
        &body.content_type,
        &body.content_encoding,
        &body.content_disposition,
    ]
    .into_iter()
    .flatten()
    {
        HeaderValue::from_str(value)
            .map_err(|_err| PresignError::InvalidHeaderValue(value.clone()))?;
    }

    let correlation_id = headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().map(str::to_string).ok())
        .unwrap_or_default();
    let execution = setup_execution(
        module_path!(),
        correlation_id,
        get_user_id_from_token(user_info),
    );

    let immutable_store = state.immutable_store.clone();

    LORE_CONTEXT
        .scope(execution, async move {
            // Verify the address exists before issuing a URL for it.
            let match_result = query_one(&immutable_store, repository, parsed_address)
                .await
                .map_err(|e| {
                    warn!(%e, "Presign resolve check failed");
                    PresignError::StoreError
                })?;

            if match_result.match_made != StoreMatch::MatchFull {
                return Err(PresignError::NotFound);
            }

            // Clamp TTL to configured bounds.
            let ttl = body
                .ttl_seconds
                .unwrap_or(presign_config.default_ttl_seconds);
            let ttl = ttl.clamp(
                presign_config.min_ttl_seconds,
                presign_config.max_ttl_seconds,
            );

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(PresignError::SystemTime)?
                .as_secs();
            let expires_at = now + ttl;

            let payload = PresignTokenPayload {
                version: CURRENT_TOKEN_VERSION,
                key_id: presign_config.key_id.clone(),
                repository: repository_id.clone(),
                address: address.clone(),
                expires_at,
                content_type: body.content_type,
                content_encoding: body.content_encoding,
                content_disposition: body.content_disposition,
            };

            let token_str = sign(&payload, &presign_config.hmac_key);

            let url_suffix = format!("/v1/presigned/{repository_id}/{address}?token={token_str}");

            Ok((
                StatusCode::OK,
                Json(PresignResponse {
                    url_suffix,
                    expires_at,
                }),
            ))
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::HeaderMap;
    use axum::http::StatusCode;
    use axum_test::TestServer;
    use lore_base::runtime::LORE_CONTEXT;
    use rand::random;
    use serde_json::json;

    use super::PresignError;
    use super::check_presign_permission;
    use crate::auth::jwt::AuthorizationToken;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;
    use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
    use crate::http::security_headers::ContentTypePolicy;
    use crate::http::server::LoreHttpServerSettings;
    use crate::http::server::ServerHealth;
    use crate::http::server::ServerState;
    use crate::http::server::create_router;
    use crate::http::test_utils::content_type_policy;
    use crate::http::test_utils::presign_config_with_policy;
    use crate::store::test_store_create;

    fn state_with_authorizer(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        repository_authorizer: Arc<dyn crate::authnz::repository_authorizer::RepositoryAuthorizer>,
        policy: ContentTypePolicy,
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
            presign_config: Some(presign_config_with_policy(policy)),
        }
    }

    #[tokio::test]
    async fn no_auth_configured_may_vend() {
        let (immutable_store, mutable_store, _) =
            test_store_create().await.expect("Failed to create stores");
        let state = state_with_authorizer(
            immutable_store,
            mutable_store,
            Arc::new(AllowAllRepositoryAuthorizer),
            ContentTypePolicy::default(),
        );
        check_presign_permission(&state, &None, &HeaderMap::new(), random())
            .await
            .expect("AllowAllRepositoryAuthorizer keeps the gate open with no verifier");
    }

    #[tokio::test]
    async fn caller_holding_presign_action_may_vend() {
        let (immutable_store, mutable_store, _) =
            test_store_create().await.expect("Failed to create stores");
        let state = state_with_authorizer(
            immutable_store,
            mutable_store,
            Arc::new(GlobalGrantsAuthorizer::new(Some("roles".to_string()))),
            ContentTypePolicy::default(),
        );
        let serde_json::Value::Object(extra) = json!({ "roles": ["presign"] }) else {
            unreachable!()
        };
        let token = Some(AuthorizationToken {
            extra,
            ..Default::default()
        });
        check_presign_permission(&state, &token, &HeaderMap::new(), random())
            .await
            .expect("a caller holding the presign action may vend");
    }

    #[tokio::test]
    async fn caller_without_presign_action_may_not_vend() {
        let (immutable_store, mutable_store, _) =
            test_store_create().await.expect("Failed to create stores");
        let state = state_with_authorizer(
            immutable_store,
            mutable_store,
            Arc::new(GlobalGrantsAuthorizer::new(Some("roles".to_string()))),
            ContentTypePolicy::default(),
        );
        let token = Some(AuthorizationToken::default());
        let err = check_presign_permission(&state, &token, &HeaderMap::new(), random())
            .await
            .expect_err("a caller not holding the presign action may not vend");
        assert!(matches!(err, PresignError::PermissionDenied));
    }

    async fn mint(body: serde_json::Value) -> axum_test::TestResponse {
        mint_with_policy(body, ContentTypePolicy::default()).await
    }

    /// Posts `body` to the mint endpoint of a server whose allowlist comes from
    /// `policy`. The store is fresh, so the address does not exist and requests
    /// that pass validation reach the existence check.
    async fn mint_with_policy(
        body: serde_json::Value,
        policy: ContentTypePolicy,
    ) -> axum_test::TestResponse {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        LORE_CONTEXT
            .scope(execution, async move {
                let repository = random::<lore_revision::lore::RepositoryId>();
                let repo_hex = format!("{repository}");
                let address = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff-ffffffffffffffffffffffffffffffff";

                let test_health = ServerHealth::new_without_availability(immutable_store.clone());
                let state = state_with_authorizer(
                    immutable_store,
                    mutable_store,
                    Arc::new(AllowAllRepositoryAuthorizer),
                    policy,
                );
                let settings = LoreHttpServerSettings::test_default();
                let server =
                    TestServer::new(create_router(state, test_health, &settings)).unwrap();

                server
                    .post(&format!("/v1/repository/{repo_hex}/content/{address}/presign"))
                    .json(&body)
                    .await
            })
            .await
    }

    #[tokio::test]
    async fn returns_404_when_address_not_found() {
        let response = mint(json!({"ttl_seconds": 3600})).await;
        assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    }

    /// The S3 default type passes the allowlist, so it reaches the existence
    /// check and returns 404 rather than 400.
    #[tokio::test]
    async fn accepts_s3_binary_octet_stream() {
        let response = mint(json!({"content_type": "binary/octet-stream"})).await;
        assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn returns_400_for_disallowed_content_type() {
        let response = mint(json!({"content_type": "text/html"})).await;
        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    }

    /// A type added through config passes the allowlist, so it reaches the
    /// existence check and returns 404 rather than 400.
    #[tokio::test]
    async fn accepts_configured_extra_content_type() {
        let response = mint_with_policy(
            json!({"content_type": "application/zip"}),
            content_type_policy(&["application/zip"], &[]),
        )
        .await;

        assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
    }

    /// A built-in type removed through config is rejected at mint.
    #[tokio::test]
    async fn returns_400_for_configured_denied_content_type() {
        let response = mint_with_policy(
            json!({"content_type": "application/pdf"}),
            content_type_policy(&[], &["application/pdf"]),
        )
        .await;

        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn returns_400_for_unserializable_header_value() {
        // Allowlisted media type, but a control char in the parameter makes it
        // an invalid header value; mint must reject rather than let redeem 500.
        let response = mint(json!({"content_type": "image/png; x=\u{7}"})).await;
        assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    }
}
