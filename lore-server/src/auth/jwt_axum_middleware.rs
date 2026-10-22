// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;

use axum::body::Body;
use axum::extract::Path;
use axum::extract::Request;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use lore_base::types::Context;
use lore_telemetry::tracing::fields::USER_ID;
use serde::Deserialize;
use tracing::Span;

use crate::auth::jwt::AuthorizationToken;
use crate::authnz::repository_authorizer::IncarnationGuard;
use crate::authnz::repository_authorizer::PartitionGrants;
use crate::authnz::repository_authorizer::RawToken;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::http::server::ServerState;

#[derive(Deserialize)]
pub struct Params {
    repository_id: String,
}

pub async fn jwt_axum_verify_authorization(
    State(state): State<ServerState>,
    Path(params): Path<Params>,
    mut request: Request,
    next: Next,
) -> Response {
    if let Some(jwt_verifier) = state.jwt_verifier {
        if let Some(accesstoken) = extract_bearer_token(&request) {
            if let Ok(user_info) = jwt_verifier.verify_token(&accesstoken).await {
                let repository: lore_revision::lore::RepositoryId =
                    Context::from_str(params.repository_id.as_str())
                        .unwrap_or_default()
                        .into();
                let token = VerifiedToken {
                    raw: &accesstoken,
                    claims: &user_info,
                };
                let access = state
                    .repository_authorizer
                    .granted_access_tracked(Some(&token), repository)
                    .await;
                if let Err(status) = &access
                    && status.code() == tonic::Code::ResourceExhausted
                {
                    return access_failure_response(status);
                }
                if let Ok(regranted) = access {
                    Span::current().record(USER_ID, user_info.identity());
                    // Handlers that read a request body re-check the
                    // incarnation after it arrives.
                    let guard = IncarnationGuard::new(
                        state.repository_authorizer.clone(),
                        Some(&token),
                        repository,
                        regranted.incarnation,
                    );
                    request.extensions_mut().insert(guard);
                    if let Some(grants) = regranted.grants {
                        request.extensions_mut().insert(PartitionGrants {
                            repository_id: repository,
                            grants,
                            incarnation: regranted.incarnation,
                        });
                    }
                    // Set `user_info` as a request extension so it can be used down the stack
                    request.extensions_mut().insert(RawToken(accesstoken));
                    request.extensions_mut().insert(Some(user_info));

                    return next.run(request).await;
                }
            }
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::empty())
                .unwrap()
        } else {
            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(Body::empty())
                .unwrap()
        }
    } else {
        let no_user_info: Option<AuthorizationToken> = None;
        request.extensions_mut().insert(no_user_info);
        request.extensions_mut().insert(None::<IncarnationGuard>);
        next.run(request).await
    }
}

/// The HTTP answer for an authorization failure: 429 for backpressure, 403
/// for everything else.
pub fn access_failure_response(status: &tonic::Status) -> Response {
    let code = if status.code() == tonic::Code::ResourceExhausted {
        StatusCode::TOO_MANY_REQUESTS
    } else {
        StatusCode::FORBIDDEN
    };
    Response::builder()
        .status(code)
        .body(Body::empty())
        .unwrap()
}

fn extract_bearer_token(request: &Request) -> Option<String> {
    request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| {
            if header.starts_with("Bearer ") {
                Some(header.trim_start_matches("Bearer ").to_string())
            } else {
                None
            }
        })
}
