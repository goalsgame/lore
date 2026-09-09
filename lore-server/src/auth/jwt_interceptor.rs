// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use anyhow::Result;
use lore_base::runtime::runtime;
use lore_telemetry::tracing::fields::USER_ID;
use tokio::task;
use tonic::service::Interceptor;
use tracing::Span;
use tracing::debug;

use super::jwt::JwtVerifier;
use crate::auth::jwt::AuthorizationToken;
use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
use crate::grpc::get_repository;

fn add_auth_fields_to_current_span(auth: &AuthorizationToken) {
    let span = Span::current();
    span.record(USER_ID, auth.user_id.clone());
}

/// Resolve the bearer token to an [`AuthorizationToken`]. The cached signing key serves the
/// hot path synchronously; the blocking fallback runs only when the cache cannot answer —
/// no key for the id, or a key that rejected the signature and may therefore have been
/// rotated out. A token that fails on its own claims is refused without blocking. That is
/// what lets this run inside tonic's synchronous [`Interceptor::call`].
fn authorize(verifier: &JwtVerifier, token: &str) -> Result<AuthorizationToken, tonic::Status> {
    match verifier.try_verify_token_cached(token) {
        Ok(Some(authorization)) => Ok(authorization),
        // Reached only when the cache cannot answer, so the core handed off here is one the
        // hot path never gives up.
        #[allow(clippy::disallowed_methods)]
        Ok(None) => task::block_in_place(|| runtime().block_on(verifier.verify_token(token))),
        Err(e) => Err(e),
    }
    .map_err(|e| {
        // The reason stays in the log. Told apart, "the signature is wrong", "the token
        // expired", "no such key id" and "the JWKS endpoint is unwell" are an oracle for a
        // caller who has not authenticated — and not one of them is something that caller
        // could act on.
        debug!(error = ?e, "Rejecting request: token verification failed");
        tonic::Status::permission_denied("Not allowed")
    })
}

#[derive(Clone)]
pub struct JWTInterceptor {
    jwt_verifier: JwtVerifier,
    /// Answers the plain "may this caller reach this repository at all"
    /// question once per request. See
    /// [`ReachabilityAuthorizer`] for why this call, specifically, is
    /// answered differently for a legacy `UrcAuthApi` deployment than
    /// [`RepositoryAuthorizer::check_repository_access`] is everywhere
    /// else: this decision point is picking the interceptor as the
    /// enforcement point for the plain reachability question, per LEP
    /// 2026-08-20-oidc-oauth2-authentication (D9). The alternative the LEP
    /// leaves open — pushing this into every RPC handler across Storage,
    /// Revision, Lock, Notification and ThinClient — would be a far larger
    /// change today: none of those services currently take an injected
    /// authorizer at all, so every one of their handlers would need new
    /// plumbing for a question the interceptor can already answer
    /// synchronously for both Tier 1 and legacy deployments (see
    /// `ReachabilityAuthorizer::check_reachability_sync`). Action-specific
    /// checks (`obliterate`, `owner`, `push-protected`, ...) still live in
    /// handlers regardless of this choice, since the interceptor has no way
    /// to know which action a request performs.
    ///
    /// This was reopened when the baseline `read`/`push` actions were added
    /// (closing the gap where plain reachability granted full read and push
    /// access to any authenticated principal): could *those* checks move
    /// here instead, since — unlike the six privileged actions above — they
    /// gate nearly every request? No. `Interceptor::call` receives a
    /// `tonic::Request<()>` that tonic builds by stripping the URI off the
    /// underlying `http::Request` before invoking the interceptor and
    /// splicing it back in afterward (see `InterceptedService::call` in
    /// `tonic::service::interceptor`, upstream): `request.metadata()` holds
    /// only ordinary headers, never the `:path` pseudo-header, and
    /// `request.extensions()` never receives the URI either. So the
    /// interceptor has no way to learn which RPC method is being called —
    /// the same limitation that already keeps the six privileged actions in
    /// handlers, just now load-bearing for nearly every request instead of
    /// a handful of them. `read`/`push` are wired in per-handler instead,
    /// the same way `obliterate`/`push-protected`/etc. already are.
    ///
    /// [`RepositoryAuthorizer::check_repository_access`]: crate::authnz::repository_authorizer::RepositoryAuthorizer::check_repository_access
    reachability_authorizer: ReachabilityAuthorizer,
}

impl JWTInterceptor {
    pub fn new(
        jwt_verifier: &JwtVerifier,
        reachability_authorizer: ReachabilityAuthorizer,
    ) -> Self {
        Self {
            jwt_verifier: jwt_verifier.clone(),
            reachability_authorizer,
        }
    }
}

impl Interceptor for JWTInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let token = extract_bearer_token(request.metadata()).ok_or(
            tonic::Status::unauthenticated("authorization header required"),
        )?;

        let authorization = authorize(&self.jwt_verifier, &token)?;
        add_auth_fields_to_current_span(&authorization);

        let repository = get_repository(request.metadata()).unwrap_or_default();
        self.reachability_authorizer
            .check_reachability_sync(&authorization, repository)
            .map_err(|_err| crate::grpc::no_repository_access_status())?;

        request.extensions_mut().insert(authorization);

        Ok(request)
    }
}

#[derive(Clone)]
pub struct JWTAuthnInterceptor {
    jwt_verifier: JwtVerifier,
}

impl JWTAuthnInterceptor {
    pub fn new(jwt_verifier: &JwtVerifier) -> Self {
        Self {
            jwt_verifier: jwt_verifier.clone(),
        }
    }
}

impl Interceptor for JWTAuthnInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let token = extract_bearer_token(request.metadata()).ok_or(
            tonic::Status::unauthenticated("authorization header required"),
        )?;

        // TODO(UCS-13506): Placeholder authn verifier until separate authz flow for repository service is in place
        let authorization = authorize(&self.jwt_verifier, &token)?;
        add_auth_fields_to_current_span(&authorization);

        request.extensions_mut().insert(authorization);

        Ok(request)
    }
}

pub(crate) fn extract_bearer_token(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|header| {
            if header.starts_with("Bearer ") {
                Some(header.trim_start_matches("Bearer ").to_string())
            } else {
                None
            }
        })
}
