// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;
use lore_base::types::RepositoryId;
use lore_proto::auth::CheckUserPermissionRequest;
use thiserror::Error;
use tonic::Code;
use tonic::Status;

use super::auth::grpc_get_auth_client;
use super::common::create_request_with_authorization;
use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::verify_authorization;
use crate::grpc::ServerResultExt;
use crate::settings::AuthSettings;

/// The baseline actions every partition-scoped read and write now requires
/// on top of plain authentication, closing the Tier 1 gap where
/// `check_repository_access(.., None)` — "reachability" — granted full read
/// and push access to any authenticated principal with no group membership
/// at all. These sit below the six pre-existing privileged actions
/// (`obliterate`, `owner`/`admin`, `migrate`, `push-protected`, `presign`),
/// which stay exactly as they are and stack on top of `push` where noted at
/// their own call sites (see `PUSH_PROTECTED_ACTION`).
///
/// Unlike those six — each a rename of a permission string a legacy
/// `UrcAuthApi` deployment's `resources` claim already granted for the
/// equivalent privileged operation — `read` and `push` have no legacy
/// equivalent: ordinary (non-privileged) access under `UrcAuthApi` has
/// always been "listed among the allowed resources at all"
/// (`AuthClientAuthorizer`'s `None` case), never gated behind a specific
/// permission string. `AuthClientAuthorizer::check_repository_access`
/// special-cases these two actions for exactly that reason — see its doc
/// comment.
pub(crate) const READ_ACTION: &str = "read";
pub(crate) const PUSH_ACTION: &str = "push";

/// A JWT the caller has already verified, carried both as the raw compact
/// serialization and as decoded claims.
///
/// Both halves are needed because no single implementation of
/// [`RepositoryAuthorizer`] can be built from either alone: a claims-based
/// implementation (like [`GlobalGrantsAuthorizer`]) reads `claims` and never
/// touches `raw`, while [`AuthClientAuthorizer`] forwards `raw` unchanged to
/// the legacy auth service and never decodes `claims` itself. Re-verifying a
/// token the interceptor already verified would be redundant at best and a
/// second source of truth at worst, so this struct exists purely to avoid
/// that: it is a view onto a verification the caller already performed.
#[derive(Clone, Copy)]
pub struct VerifiedToken<'a> {
    /// The compact JWT serialization exactly as it arrived on the wire
    /// (without the `Bearer ` prefix), for authorizers that must forward it.
    pub raw: &'a str,
    /// The decoded, verified claims.
    pub claims: &'a AuthorizationToken,
}

impl<'a> VerifiedToken<'a> {
    pub fn new(raw: &'a str, claims: &'a AuthorizationToken) -> Self {
        Self { raw, claims }
    }
}

/// Answers every partition-access question in the server: "may this caller
/// reach this repository at all" (`action: None`) and "does this caller hold
/// this specific action on this repository" (`action: Some(_)`).
///
/// `token` is `None` exactly when no verifier is configured for the
/// deployment (see [`AllowAllRepositoryAuthorizer`]) — every other caller has
/// already been authenticated by the time this is asked, so an
/// implementation that requires a verified caller (like
/// [`GlobalGrantsAuthorizer`]) treats `None` as unauthenticated rather than
/// as "allow".
#[async_trait]
pub trait RepositoryAuthorizer: Send + Sync {
    async fn check_repository_access(
        &self,
        token: Option<&VerifiedToken<'_>>,
        repository: RepositoryId,
        action: Option<&str>,
    ) -> Result<(), Status>;
}

/// Always allows access. Used when no `[server.auth]` is configured at all,
/// which is what keeps a local server able to do everything without any
/// OIDC integration.
pub struct AllowAllRepositoryAuthorizer;

#[async_trait]
impl RepositoryAuthorizer for AllowAllRepositoryAuthorizer {
    async fn check_repository_access(
        &self,
        _token: Option<&VerifiedToken<'_>>,
        _repository: RepositoryId,
        _action: Option<&str>,
    ) -> Result<(), Status> {
        Ok(())
    }
}

/// Checks repository access against the legacy `UrcAuthApi` auth service.
/// One online `CheckUserPermission` call per check, with no cache — this is
/// the pre-existing behavior, kept unchanged for deployments still running
/// their own `UrcAuthApi` implementation.
///
/// `CheckUserPermissionRequest` has no field to name a specific action, but
/// its response already carries the caller's granted permission strings for
/// the resource (`ResourcePermission.permission`) — the same shape the
/// legacy `resources` JWT claim used. A `Some(action)` call is checked
/// against that list rather than silently degrading to the plain
/// reachability question `action: None` asks: doing the latter would grant
/// e.g. `push-protected` or `obliterate` to anyone who can merely reach the
/// repository, a privilege escalation versus what the caller actually holds.
pub struct AuthClientAuthorizer {
    auth_url: String,
}

impl AuthClientAuthorizer {
    pub fn new(auth_url: String) -> Self {
        Self { auth_url }
    }
}

#[async_trait]
impl RepositoryAuthorizer for AuthClientAuthorizer {
    async fn check_repository_access(
        &self,
        token: Option<&VerifiedToken<'_>>,
        repository_id: RepositoryId,
        action: Option<&str>,
    ) -> Result<(), Status> {
        let mut client = grpc_get_auth_client(self.auth_url.clone()).await?;
        let resource_id = format!("urc-{repository_id}");
        let request = create_request_with_authorization(
            CheckUserPermissionRequest {
                resource_id: vec![resource_id.clone()],
                target_user: None,
            },
            token.map(|t| t.raw.to_string()),
        )?;

        let permissions = client
            .check_user_permission(request)
            .await
            .warn_map_err(|err| {
                if err.code() == Code::PermissionDenied {
                    return Status::permission_denied("Query resource denied");
                } else if err.code() == Code::Unauthenticated {
                    return Status::unauthenticated("Query resource failed - unauthenticated");
                }
                Status::internal(format!("Failed to call auth check_user_permission: {err}"))
            })?;

        let matched = permissions
            .into_inner()
            .allowed_resource_permission
            .into_iter()
            .find(|permission| permission.resource_id == resource_id)
            .ok_or(Status::internal("No permissions for resource"))?;

        if resource_permission_satisfies(&matched.permission, action) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "caller does not hold the '{}' action",
                action.unwrap_or("<reachability>")
            )))
        }
    }
}

/// Whether a resource's granted `permission` strings (from a legacy
/// `UrcAuthApi` `CheckUserPermission` response) satisfy `action`.
///
/// `None` (plain reachability) and the two new baseline actions, `read` and
/// `push`, all resolve the same way: being listed among the allowed
/// resources at all is enough. That is deliberate for `read`/`push`, not an
/// oversight — see their doc comment above. Every other action (the six
/// pre-existing privileged ones) requires the literal permission string,
/// matching every pre-existing caller of this authorizer.
fn resource_permission_satisfies(permission: &[String], action: Option<&str>) -> bool {
    match action {
        None => true,
        Some(READ_ACTION) | Some(PUSH_ACTION) => true,
        Some(action) => permission.iter().any(|held| held == action),
    }
}

/// Tier 1's authorizer (LEP 2026-08-20-oidc-oauth2-authentication, D8).
/// Answers from a *global* action set read out of an ordinary OIDC role or
/// group claim, ignoring the repository parameter entirely: an authenticated
/// principal reaches every partition the server holds, and a small number of
/// privileged actions (`obliterate`, `owner`, `admin`, `migrate`,
/// `push-protected`, `presign`, ...) are held everywhere or nowhere.
///
/// Deliberately reads only `permission_claim`. `resource_claim`,
/// `resource_id_template` and `resource_wildcard` are Tier 2's mechanism
/// (per-partition grants, answered by the not-yet-implemented
/// `ResourceGrantsAuthorizer`) and this type does not consult them.
pub struct GlobalGrantsAuthorizer {
    /// Dotted path of the claim carrying the caller's global actions, e.g.
    /// `realm_access.roles` on Keycloak or `groups` on Dex. `None` means the
    /// deployment configured `[server.auth]` without naming a claim, so
    /// every authenticated principal holds ordinary access and no
    /// privileged action.
    permission_claim: Option<String>,
}

impl GlobalGrantsAuthorizer {
    pub fn new(permission_claim: Option<String>) -> Self {
        Self { permission_claim }
    }

    /// The flat set of action strings the token's `permission_claim` names.
    /// Absent, non-array-or-string, or non-string entries all resolve to no
    /// actions held — this fails closed on a malformed claim rather than
    /// panicking or guessing.
    fn held_actions(&self, claims: &AuthorizationToken) -> HashSet<String> {
        let Some(path) = self.permission_claim.as_deref() else {
            return HashSet::new();
        };
        match claims.claim_at(path) {
            Some(serde_json::Value::Array(items)) => items
                .into_iter()
                .filter_map(|item| match item {
                    serde_json::Value::String(s) => Some(s),
                    _ => None,
                })
                .collect(),
            Some(serde_json::Value::String(single)) => std::iter::once(single).collect(),
            _ => HashSet::new(),
        }
    }
}

#[async_trait]
impl RepositoryAuthorizer for GlobalGrantsAuthorizer {
    async fn check_repository_access(
        &self,
        token: Option<&VerifiedToken<'_>>,
        _repository: RepositoryId,
        action: Option<&str>,
    ) -> Result<(), Status> {
        let Some(token) = token else {
            // Tier 1 only runs once a verifier is configured, so every real
            // caller arrives here already authenticated. `None` reaching
            // this authorizer is a wiring bug, not an anonymous caller, and
            // must not be treated as "allow".
            return Err(Status::unauthenticated(
                "authentication required for global-grants authorization",
            ));
        };

        let Some(action) = action else {
            // Plain reachability: any authenticated principal reaches every
            // partition the server holds under Tier 1.
            return Ok(());
        };

        if self.held_actions(token.claims).contains(action) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "caller does not hold the '{action}' action"
            )))
        }
    }
}

/// Returned when `[server.auth]` selects Tier 2 (`resource_claim` set) but no
/// deployment is legacy `UrcAuthApi`. `ResourceGrantsAuthorizer` is out of
/// scope for this build, so this is a hard, explicit failure rather than a
/// silent fall back to Tier 1 (which would discard the least-privilege
/// property the operator asked for without saying so) or a panic reached
/// only once a request happens to need it.
#[derive(Debug, Error)]
pub enum RepositoryAuthorizerError {
    #[error(
        "server.auth.resource_claim is set, which selects the Tier 2 ResourceGrantsAuthorizer \
         (per-partition resource grants via RFC 8693 token exchange). That authorizer is not \
         implemented in this build, so the server refuses to start rather than silently falling \
         back to Tier 1 or failing on the first request that needs it. Unset \
         server.auth.resource_claim to run Tier 1 (GlobalGrantsAuthorizer, global per-action \
         grants read from server.auth.permission_claim), or implement ResourceGrantsAuthorizer \
         before enabling Tier 2."
    )]
    ResourceGrantsNotImplemented,
}

/// Creates the appropriate authorizer for a deployment's configuration.
///
/// Branches exactly as LEP 2026-08-20-oidc-oauth2-authentication (D8)
/// describes:
///
/// - No `[server.auth]` at all: [`AllowAllRepositoryAuthorizer`]. Every
///   check passes, matching today's behavior for a deployment with no OIDC
///   integration.
/// - A legacy `auth_url` configured (`[environment.endpoint].auth_url`):
///   [`AuthClientAuthorizer`], unconditionally. This mirrors the existing
///   factory exactly — legacy deployments select this authorizer whether or
///   not `[server.auth]` itself is also present, which is the shape today's
///   callers already rely on.
/// - `[server.auth]` present, no legacy `auth_url`, no `resource_claim`:
///   Tier 1's [`GlobalGrantsAuthorizer`], reading `permission_claim`.
/// - `[server.auth]` present, no legacy `auth_url`, `resource_claim` set:
///   Tier 2. Not implemented — see [`RepositoryAuthorizerError`].
///   `Settings::load`'s startup validation rejects this configuration before
///   the server ever accepts a request; this is the defense-in-depth
///   backstop for any caller that builds an authorizer without going
///   through that validation.
pub fn repository_authorizer(
    auth_url: Option<String>,
    auth_settings: Option<&AuthSettings>,
) -> Result<Arc<dyn RepositoryAuthorizer>, RepositoryAuthorizerError> {
    if let Some(url) = auth_url {
        return Ok(Arc::new(AuthClientAuthorizer::new(url)));
    }

    let Some(auth) = auth_settings else {
        return Ok(Arc::new(AllowAllRepositoryAuthorizer));
    };

    if auth.resource_claim.is_some() {
        return Err(RepositoryAuthorizerError::ResourceGrantsNotImplemented);
    }

    Ok(Arc::new(GlobalGrantsAuthorizer::new(
        auth.permission_claim.clone(),
    )))
}

/// Answers the plain "can this token reach this repository at all" question
/// — what every enforcement point in D9 asks with `action: None` — for the
/// call sites that run once per request or, worse, once per item in a
/// stream: the gRPC interceptor, the cross-partition link-read closure, and
/// the two `Copy` handlers that check the source repository per fragment.
///
/// A legacy `UrcAuthApi` deployment answers this locally from the token's
/// own embedded `resources` claim (minted by the auth service at exchange
/// time) exactly as it always has, rather than through
/// [`AuthClientAuthorizer`]'s online `CheckUserPermission` call. Routing
/// this specific, high-frequency question through the network instead would
/// add a round trip to every single gRPC request (Storage, Revision, Lock,
/// Notification, ThinClient) or copy item, not just the handful of
/// operations that already pay for one today (repository query, metadata
/// get/set). `AuthClientAuthorizer`'s online check keeps running exactly
/// where it already does — this wrapper does not change that.
///
/// Lower-frequency call sites (repository query, metadata get/set,
/// repository delete, branch push, presign, notification subscribe, and the
/// once-per-session QUIC connect/authorize) call
/// [`RepositoryAuthorizer::check_repository_access`] on
/// [`Self::authorizer`] directly and do not need this wrapper: for those,
/// going through the configured authorizer unconditionally — legacy
/// deployments included — either matches their existing behavior exactly
/// (query, metadata get/set already call the configured authorizer
/// unconditionally today) or costs no more than one online check per
/// connection, which is the granularity `AuthClientAuthorizer` was built
/// for.
#[derive(Clone)]
pub struct ReachabilityAuthorizer {
    pub authorizer: Arc<dyn RepositoryAuthorizer>,
    /// `true` for a legacy `UrcAuthApi` deployment (a legacy `auth_url` is
    /// configured), which is exactly when [`Self::authorizer`] is an
    /// [`AuthClientAuthorizer`] that this wrapper deliberately bypasses for
    /// the reason documented on the type.
    pub legacy_resource_claim: bool,
}

impl ReachabilityAuthorizer {
    pub fn new(
        auth_url: Option<String>,
        auth_settings: Option<&AuthSettings>,
    ) -> Result<Self, RepositoryAuthorizerError> {
        let legacy_resource_claim = auth_url.is_some();
        let authorizer = repository_authorizer(auth_url, auth_settings)?;
        Ok(Self {
            authorizer,
            legacy_resource_claim,
        })
    }

    /// Shared implementation behind [`Self::check_reachability`],
    /// [`Self::check_read`] and [`Self::check_push`]. `action: None` is
    /// plain reachability; `Some(READ_ACTION)`/`Some(PUSH_ACTION)` are the
    /// baseline actions. All three take the *same* legacy branch — a local,
    /// no-network claims check — rather than only `check_reachability`
    /// doing so, because `read`/`push` have no legacy equivalent (see their
    /// doc comment on the module): ordinary access under a legacy
    /// `UrcAuthApi` deployment has always been "listed among the allowed
    /// resources at all," never gated behind a specific permission string,
    /// so checking `read` or `push` for such a deployment is defined to
    /// answer the identical question `check_reachability` already does.
    /// This matters on hot, per-item paths (the two per-item `Copy`
    /// handlers) where routing `Some(READ_ACTION)` through
    /// [`Self::authorizer`] directly would reach [`AuthClientAuthorizer`]'s
    /// online `CheckUserPermission` call once per fragment — exactly the
    /// per-item network cost this type exists to avoid.
    async fn check_action(
        &self,
        claims: &AuthorizationToken,
        repository: RepositoryId,
        action: Option<&str>,
    ) -> Result<(), Status> {
        if self.legacy_resource_claim {
            verify_authorization(claims, repository)
                .map_err(|_err| Status::permission_denied("Unauthorized"))
        } else {
            let token = VerifiedToken::new("", claims);
            self.authorizer
                .check_repository_access(Some(&token), repository, action)
                .await
        }
    }

    /// Async form, for the call sites that can await: the two per-item
    /// `Copy` handlers and the HTTP axum middleware.
    ///
    /// Takes no raw token: the legacy branch reads only `claims`, and the
    /// non-legacy branch only ever reaches [`AllowAllRepositoryAuthorizer`]
    /// or [`GlobalGrantsAuthorizer`], neither of which reads
    /// [`VerifiedToken::raw`] either — only [`AuthClientAuthorizer`] does,
    /// and this wrapper exists specifically to never reach it. There is
    /// therefore no real value to thread through the call sites that use
    /// this method, several of which do not have the raw compact
    /// serialization to hand in the first place.
    pub async fn check_reachability(
        &self,
        claims: &AuthorizationToken,
        repository: RepositoryId,
    ) -> Result<(), Status> {
        self.check_action(claims, repository, None).await
    }

    /// Checks the baseline `read` action, degrading to the same local
    /// claims check as [`Self::check_reachability`] for a legacy
    /// deployment — see [`Self::check_action`]'s doc comment. Used on hot,
    /// per-item paths (the per-fragment `Copy` source check in both the
    /// gRPC v1 and legacy `urc/0.2` transports) where a fresh online call
    /// per item would be unacceptable.
    pub async fn check_read(
        &self,
        claims: &AuthorizationToken,
        repository: RepositoryId,
    ) -> Result<(), Status> {
        self.check_action(claims, repository, Some(READ_ACTION))
            .await
    }

    /// Checks the baseline `push` action. See [`Self::check_read`].
    pub async fn check_push(
        &self,
        claims: &AuthorizationToken,
        repository: RepositoryId,
    ) -> Result<(), Status> {
        self.check_action(claims, repository, Some(PUSH_ACTION))
            .await
    }

    /// Synchronous form, for the gRPC interceptor and the cross-partition
    /// link-read closure, neither of which can await anything.
    ///
    /// This never actually blocks: the legacy branch is plain, non-async
    /// code, and the non-legacy branch only ever reaches
    /// [`AllowAllRepositoryAuthorizer`] or [`GlobalGrantsAuthorizer`] — by
    /// construction, [`AuthClientAuthorizer`] is selected exactly when
    /// `legacy_resource_claim` is `true`, which takes the other branch —
    /// and neither of those implementations awaits anything either.
    pub fn check_reachability_sync(
        &self,
        claims: &AuthorizationToken,
        repository: RepositoryId,
    ) -> Result<(), Status> {
        self.check_reachability(claims, repository)
            .now_or_never()
            .expect(
                "check_reachability resolves synchronously: the legacy branch never awaits, \
                 and the non-legacy branch never reaches an online authorizer",
            )
    }
}

#[cfg(test)]
mod tests {
    use lore_base::types::Context;
    use serde_json::json;

    use super::*;

    fn repository() -> RepositoryId {
        Context::from([1u8; 16]).into()
    }

    fn token_with_extra(extra: serde_json::Value) -> AuthorizationToken {
        let serde_json::Value::Object(extra) = extra else {
            panic!("extra claims must be a JSON object");
        };
        AuthorizationToken {
            user_id: "the-user".to_string(),
            extra,
            ..Default::default()
        }
    }

    mod allow_all {
        use super::*;

        #[tokio::test]
        async fn permits_every_check_with_no_token() {
            let authorizer = AllowAllRepositoryAuthorizer;
            authorizer
                .check_repository_access(None, repository(), None)
                .await
                .expect("no token, no action");
            authorizer
                .check_repository_access(None, repository(), Some("obliterate"))
                .await
                .expect("no token, an action");
        }

        #[tokio::test]
        async fn permits_every_check_with_a_token_that_holds_nothing() {
            let claims = token_with_extra(json!({}));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = AllowAllRepositoryAuthorizer;
            authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect("AllowAll ignores the action and the claims");
        }
    }

    mod global_grants {
        use super::*;

        #[tokio::test]
        async fn no_token_is_unauthenticated() {
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            let err = authorizer
                .check_repository_access(None, repository(), None)
                .await
                .expect_err("Tier 1 with no verified token must not be treated as allow");
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
        }

        #[tokio::test]
        async fn no_action_is_reachability_only_and_passes_once_authenticated() {
            let claims = token_with_extra(json!({}));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            authorizer
                .check_repository_access(Some(&token), repository(), None)
                .await
                .expect("any authenticated principal reaches every partition under Tier 1");
        }

        #[tokio::test]
        async fn an_action_the_claim_holds_is_permitted() {
            let claims = token_with_extra(json!({
                "realm_access": { "roles": ["obliterate", "admin"] }
            }));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect("the claim names this action");
            authorizer
                .check_repository_access(Some(&token), repository(), Some("admin"))
                .await
                .expect("the claim also names this action");
        }

        #[tokio::test]
        async fn an_action_the_claim_does_not_hold_is_denied() {
            let claims = token_with_extra(json!({
                "realm_access": { "roles": ["obliterate"] }
            }));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some("admin"))
                .await
                .expect_err("the claim does not name this action");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        #[tokio::test]
        async fn a_flat_claim_like_dex_groups_is_read_the_same_way() {
            // `groups` is a named field on `AuthorizationToken`, not an
            // `extra` entry, so it is set directly here rather than through
            // `token_with_extra` (which would land in `extra` and be
            // shadowed by the named field's `None` default).
            let claims = AuthorizationToken {
                groups: Some(vec!["obliterate".to_string()]),
                ..Default::default()
            };
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("groups".to_string()));
            authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect("a flat top-level claim resolves the same as a nested one");
        }

        #[tokio::test]
        async fn a_single_string_claim_value_is_treated_as_one_action() {
            let claims = token_with_extra(json!({ "role": "admin" }));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("role".to_string()));
            authorizer
                .check_repository_access(Some(&token), repository(), Some("admin"))
                .await
                .expect("a bare string claim is one held action");
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some("owner"))
                .await
                .expect_err("the single value does not name this action");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        #[tokio::test]
        async fn an_absent_claim_holds_no_actions() {
            let claims = token_with_extra(json!({}));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect_err("no claim means no privileged actions, fail closed");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        #[tokio::test]
        async fn a_malformed_claim_holds_no_actions_rather_than_panicking() {
            // An object where an array of strings was expected.
            let claims = token_with_extra(json!({ "realm_access": { "roles": "not-an-array" } }));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            // A bare string claim value IS treated as one action (see
            // `a_single_string_claim_value_is_treated_as_one_action`), so this
            // asserts membership rather than shape: "not-an-array" is not
            // "obliterate".
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect_err("a mismatched string value does not name the requested action");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);

            // Genuinely malformed: a number where the claim should be an
            // array or a string.
            let claims = token_with_extra(json!({ "realm_access": { "roles": 42 } }));
            let token = VerifiedToken::new("raw", &claims);
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect_err("a claim shaped as neither array nor string holds no actions");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        #[tokio::test]
        async fn non_string_array_entries_are_ignored_not_fatal() {
            let claims = token_with_extra(json!({
                "realm_access": { "roles": ["obliterate", 1, null, "admin"] }
            }));
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("realm_access.roles".to_string()));
            authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect("string entries alongside malformed ones still resolve");
            authorizer
                .check_repository_access(Some(&token), repository(), Some("admin"))
                .await
                .expect("string entries alongside malformed ones still resolve");
        }

        #[tokio::test]
        async fn no_permission_claim_configured_grants_no_privileged_action() {
            let claims = token_with_extra(json!({ "groups": ["obliterate"] }));
            let token = VerifiedToken::new("raw", &claims);
            // `[server.auth]` present but no `permission_claim` named.
            let authorizer = GlobalGrantsAuthorizer::new(None);
            authorizer
                .check_repository_access(Some(&token), repository(), None)
                .await
                .expect("ordinary reachability still works with no permission_claim");
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some("obliterate"))
                .await
                .expect_err(
                    "with no permission_claim configured, no principal holds a privileged action",
                );
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        #[tokio::test]
        async fn ignores_the_repository_parameter() {
            let claims = AuthorizationToken {
                groups: Some(vec!["obliterate".to_string()]),
                ..Default::default()
            };
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("groups".to_string()));
            let other_repository: RepositoryId = Context::from([9u8; 16]).into();
            authorizer
                .check_repository_access(Some(&token), other_repository, Some("obliterate"))
                .await
                .expect("Tier 1 grants are global, not scoped to a partition");
        }

        /// `read` and `push` are ordinary actions under Tier 1, not special
        /// cases: a token must literally hold the claim value, exactly like
        /// the six pre-existing actions. The legacy-only degrade lives
        /// solely in `AuthClientAuthorizer` (see `resource_permission_satisfies`
        /// tests below) — `GlobalGrantsAuthorizer` never sees it.
        #[tokio::test]
        async fn read_and_push_are_ordinary_actions_requiring_the_literal_claim() {
            let claims = AuthorizationToken {
                groups: Some(vec!["read".to_string()]),
                ..Default::default()
            };
            let token = VerifiedToken::new("raw", &claims);
            let authorizer = GlobalGrantsAuthorizer::new(Some("groups".to_string()));

            authorizer
                .check_repository_access(Some(&token), repository(), Some(READ_ACTION))
                .await
                .expect("the claim names read");
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some(PUSH_ACTION))
                .await
                .expect_err("the claim does not name push");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }
    }

    mod auth_client_resource_permission {
        use super::*;

        /// Plain reachability (`None`) is satisfied by mere presence in the
        /// allowed-resource list, regardless of what permission strings it
        /// carries — unchanged from before `read`/`push` existed.
        #[test]
        fn reachability_is_satisfied_regardless_of_permission_strings() {
            assert!(resource_permission_satisfies(&[], None));
            assert!(resource_permission_satisfies(
                &["unrelated".to_string()],
                None
            ));
        }

        /// `read` and `push` degrade to plain reachability for a legacy
        /// `UrcAuthApi` deployment: being listed at all is enough, since
        /// ordinary access there was never gated behind a specific
        /// permission string (unlike the six pre-existing privileged
        /// actions). This is what keeps a legacy deployment's existing
        /// grants working unchanged once `read`/`push` checks start being
        /// asked at every read/push call site.
        #[test]
        fn read_and_push_degrade_to_reachability_for_legacy_deployments() {
            assert!(resource_permission_satisfies(&[], Some(READ_ACTION)));
            assert!(resource_permission_satisfies(&[], Some(PUSH_ACTION)));
            assert!(resource_permission_satisfies(
                &["obliterate".to_string()],
                Some(READ_ACTION)
            ));
        }

        /// Every other action — the six pre-existing ones, and any future
        /// one — still requires the literal permission string.
        #[test]
        fn other_actions_require_the_literal_permission_string() {
            assert!(resource_permission_satisfies(
                &["obliterate".to_string()],
                Some("obliterate")
            ));
            assert!(!resource_permission_satisfies(&[], Some("obliterate")));
            assert!(!resource_permission_satisfies(
                &["read".to_string()],
                Some("obliterate")
            ));
        }
    }

    mod factory {
        use super::*;

        fn auth_settings(extra: &str) -> AuthSettings {
            toml::from_str(&format!(
                r#"
                jwt_issuer = "https://auth.example.com"
                jwt_audience = ["lore"]
                {extra}
                "#
            ))
            .expect("[server.auth] should deserialize")
        }

        #[tokio::test]
        async fn no_server_auth_and_no_legacy_url_allows_everything() {
            let authorizer =
                repository_authorizer(None, None).expect("no config never fails to construct");
            authorizer
                .check_repository_access(None, repository(), Some("obliterate"))
                .await
                .expect("AllowAllRepositoryAuthorizer permits everything");
        }

        #[tokio::test]
        async fn a_legacy_auth_url_selects_the_auth_client_authorizer_even_without_server_auth() {
            // Constructing it does not reach the network, so this only checks
            // which type came back by checking that it is neither of the
            // other two observable behaviors: AllowAll always succeeds with
            // no token, and GlobalGrants without a token is unauthenticated.
            // AuthClientAuthorizer instead tries a real connection and fails
            // with an internal/transport error rather than either of those.
            let authorizer = repository_authorizer(Some("http://127.0.0.1:0".to_string()), None)
                .expect("a legacy auth_url always constructs");
            let err = authorizer
                .check_repository_access(None, repository(), None)
                .await
                .expect_err("no local server is listening on port 0");
            assert_ne!(err.code(), tonic::Code::Unauthenticated);
        }

        #[tokio::test]
        async fn legacy_auth_url_wins_over_server_auth_resource_claim() {
            // Matches the existing factory's shape: a legacy auth_url selects
            // AuthClientAuthorizer regardless of what else is configured,
            // including a resource_claim that would otherwise be a Tier 2
            // startup error.
            let settings = auth_settings(r#"resource_claim = "resources""#);
            let authorizer =
                repository_authorizer(Some("http://127.0.0.1:0".to_string()), Some(&settings))
                    .expect("legacy auth_url must not hit the Tier 2 error path");
            let err = authorizer
                .check_repository_access(None, repository(), None)
                .await
                .expect_err("no local server is listening on port 0");
            assert_ne!(err.code(), tonic::Code::Unauthenticated);
        }

        #[tokio::test]
        async fn server_auth_with_no_resource_claim_selects_global_grants() {
            let settings = auth_settings(r#"permission_claim = "realm_access.roles""#);
            let authorizer = repository_authorizer(None, Some(&settings))
                .expect("Tier 1 configuration always constructs");
            let err = authorizer
                .check_repository_access(None, repository(), None)
                .await
                .expect_err("GlobalGrantsAuthorizer treats a missing token as unauthenticated");
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
        }

        #[test]
        fn server_auth_with_resource_claim_and_no_legacy_url_is_not_implemented() {
            let settings = auth_settings(r#"resource_claim = "resources""#);
            // `Arc<dyn RepositoryAuthorizer>` is not `Debug`, so `expect_err`
            // (which requires the `Ok` side to be `Debug`) does not apply
            // here; match explicitly instead.
            match repository_authorizer(None, Some(&settings)) {
                Err(RepositoryAuthorizerError::ResourceGrantsNotImplemented) => {}
                Ok(_) => panic!("Tier 2 must fail loudly rather than silently degrade"),
            }
        }
    }

    mod reachability_authorizer {
        use std::str::FromStr;

        use super::*;

        fn auth_settings(extra: &str) -> AuthSettings {
            toml::from_str(&format!(
                r#"
                jwt_issuer = "https://auth.example.com"
                jwt_audience = ["lore"]
                {extra}
                "#
            ))
            .expect("[server.auth] should deserialize")
        }

        #[tokio::test]
        async fn tier_1_reachability_passes_once_authenticated_sync_and_async() {
            let settings = auth_settings(r#"permission_claim = "realm_access.roles""#);
            let reachability = ReachabilityAuthorizer::new(None, Some(&settings))
                .expect("Tier 1 configuration always constructs");
            assert!(!reachability.legacy_resource_claim);

            let claims = token_with_extra(json!({}));
            reachability
                .check_reachability(&claims, repository())
                .await
                .expect("async path");
            reachability
                .check_reachability_sync(&claims, repository())
                .expect("sync path resolves without awaiting anything");
        }

        #[tokio::test]
        async fn legacy_reachability_reads_the_embedded_resources_claim_locally() {
            // A legacy auth_url selects AuthClientAuthorizer, but reachability
            // must answer from the token's own `resources` claim rather than
            // making an online call, matching the pre-existing interceptor
            // behavior exactly.
            let reachability =
                ReachabilityAuthorizer::new(Some("http://127.0.0.1:0".to_string()), None)
                    .expect("a legacy auth_url always constructs");
            assert!(reachability.legacy_resource_claim);

            let allowed_repository = "urc-0194b726b34e72b0b45550b88a967076";
            let claims = AuthorizationToken {
                resources: Some(vec![crate::auth::jwt::ResourcePermission {
                    resource_id: allowed_repository.to_string(),
                    permission: vec![],
                }]),
                ..Default::default()
            };
            let allowed: RepositoryId =
                lore_base::types::Context::from_str("0194b726b34e72b0b45550b88a967076")
                    .unwrap()
                    .into();
            let denied: RepositoryId =
                lore_base::types::Context::from_str("f6ca55437aa34198ba0f0fdc33154d51")
                    .unwrap()
                    .into();

            // No network call is reachable at 127.0.0.1:0, so a pass here can
            // only come from the local claims check, and a failure on the
            // unlisted repository proves it is a real check rather than an
            // accidental allow-all.
            reachability
                .check_reachability(&claims, allowed)
                .await
                .expect("the embedded resources claim names this repository");
            reachability
                .check_reachability_sync(&claims, allowed)
                .expect("the sync path reads the same local claim");
            reachability
                .check_reachability(&claims, denied)
                .await
                .expect_err("the embedded resources claim does not name this repository");
        }
    }
}
