// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::FutureExt;
use lore_base::runtime::core_runtime;
use lore_base::types::RepositoryId;
use lore_proto::auth::CheckUserPermissionRequest;
use lore_proto::auth::CheckUserPermissionResponse;
use lore_revision::event::EventError;
use lore_revision::interface::LoreError;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryError;
use thiserror::Error;
use tokio::task;
use tonic::Code;
use tonic::Status;
use tracing::debug;

use super::acl_config;
use super::auth::grpc_get_auth_client;
use super::common::create_request_with_authorization;
use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::verify_authorization;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::ServerResultExt;
use crate::grpc::warn_error_to_status;
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

    /// Checks access against an already-known repository *name* rather than
    /// a [`RepositoryId`] to resolve one from. Exists for `RepositoryCreate`
    /// specifically: at that call site there is no `RepositoryMetadata` to
    /// resolve yet -- the repository does not exist -- so the *requested*
    /// name, the one thing a name-pattern-matching authorizer can actually
    /// check, has to be threaded through directly instead of resolved from
    /// an id (see `docs/proposals/2026-09-09-goals-repository-acl-config.md`,
    /// Security Considerations).
    ///
    /// The default implementation delegates to [`Self::check_repository_access`]
    /// with [`RepositoryId::default()`] as an inert sentinel. This is a
    /// no-op change for every authorizer that does not consult the
    /// repository parameter for this call
    /// ([`AllowAllRepositoryAuthorizer`] ignores it unconditionally;
    /// [`GlobalGrantsAuthorizer`] and [`AuthClientAuthorizer`] both ignore it
    /// for exactly this call site -- see `RepositoryCreate`'s own doc
    /// comments for why), so only [`ConfiguredGrantsAuthorizer`], which
    /// genuinely pattern-matches on the name, needs to override it.
    async fn check_repository_access_by_name(
        &self,
        token: Option<&VerifiedToken<'_>>,
        _repository_name: &str,
        action: Option<&str>,
    ) -> Result<(), Status> {
        self.check_repository_access(token, RepositoryId::default(), action)
            .await
    }
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

        interpret_check_user_permission_response(permissions.into_inner(), &resource_id, action)
    }
}

/// Interprets a `CheckUserPermissionResponse` already received for `resource_id` — no more
/// network calls happen past this point, so every `Err` this returns is a genuine "no" from
/// the auth service, never a failure of the check itself (that already would have short-
/// circuited via `warn_map_err` in `check_repository_access`, above).
///
/// Two distinct shapes both mean "denied", and both must resolve to `Code::PermissionDenied`
/// for [`action_held`] to recognize them: the resource is simply absent from
/// `allowed_resource_permission` — the auth service granting the caller no access to it at
/// all — or it is present but its granted `permission` strings don't satisfy `action`. Prior
/// to this function existing, the first case returned `Status::internal("No permissions for
/// resource")` — indistinguishable, to [`action_held`], from an actual infra failure, so a
/// legacy deployment's *only* way of denying `read`/`push` (a repository a caller cannot
/// reach at all is never listed) was misclassified as "the check itself failed" and
/// `Connect::handle_auth`/`AuthorizeStart` turned every such denial into
/// `MessageHandleError::InternalError` instead of a clean rejection.
fn interpret_check_user_permission_response(
    response: CheckUserPermissionResponse,
    resource_id: &str,
    action: Option<&str>,
) -> Result<(), Status> {
    let matched = response
        .allowed_resource_permission
        .into_iter()
        .find(|permission| permission.resource_id == resource_id);

    match matched {
        None => Err(Status::permission_denied(format!(
            "caller has no permissions for resource '{resource_id}'"
        ))),
        Some(matched) if resource_permission_satisfies(&matched.permission, action) => Ok(()),
        Some(_) => Err(Status::permission_denied(format!(
            "caller does not hold the '{}' action",
            action.unwrap_or("<reachability>")
        ))),
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

/// GOALS-fork authorizer implementing
/// `docs/proposals/2026-09-09-goals-repository-acl-config.md`: config-driven,
/// per-repository group grants, as an alternative to
/// [`GlobalGrantsAuthorizer`]'s flat, repository-unaware ones. See
/// [`acl_config`] for the pure grant-resolution engine this wraps.
///
/// **The sync/async bridge lives at the call site, not here.**
/// [`Self::check_repository_access`] resolves a repository's name via
/// [`Self::resolve_name`] with a *plain* `.await` -- a cache hit returns
/// without suspending, and a cache miss genuinely suspends on real store
/// I/O, exactly like any other async fn. This type does no
/// `block_in_place`/`block_on` bridging itself, deliberately: nearly every
/// caller of `check_repository_access` is already async (repository query,
/// metadata get/set, repository delete, branch push, presign, notification
/// subscribe, `resolve_baseline_actions`'s QUIC connect/`AuthorizeStart`
/// checks) and can simply `.await` a cache miss like any other I/O, with no
/// need to ever bridge into a synchronous context at all. Only
/// [`ReachabilityAuthorizer::check_reachability_sync`]'s two genuinely
/// synchronous call sites (the gRPC interceptor and the cross-partition
/// link-read closure, neither of which can await anything) need a
/// sync/async bridge, and that bridge is implemented once, there, rather
/// than duplicated (and paid for by every async caller) inside this type --
/// see that method's doc comment for the bridge itself and why it, not
/// this type, owns it.
pub struct ConfiguredGrantsAuthorizer {
    /// Reused verbatim from [`GlobalGrantsAuthorizer`]: the dotted claim
    /// path naming the caller's FoxIDs groups (the design doc directs
    /// reusing this claim-extraction mechanism as-is; only what the claim is
    /// interpreted as changes -- group membership matched against the ACL
    /// config's `group` field, rather than a flat, directly-held action
    /// set).
    permission_claim: Option<String>,
    /// The parsed, static grant list. Loaded once at startup (see
    /// [`crate::settings::validate_auth_config`] and
    /// [`repository_authorizer_with_stores`]) and held read-only thereafter
    /// -- no hot reload, per the design doc's Non-Goals.
    config: acl_config::AclConfig,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    /// `RepositoryId -> name`, populated on first resolution and never
    /// invalidated: repository renaming is not a supported operation
    /// (`NAME` is a `READ_ONLY_KEY` in `lore-revision`'s metadata layer, and
    /// there is no `RepositoryRename` RPC), so a populated entry is valid
    /// for the life of the process. Unbounded, matching the design doc's own
    /// assessment that grant/repository counts stay in the tens-to-hundreds
    /// range for this deployment; revisit with an LRU cap (mirroring
    /// `lore-server/src/auth/jwk.rs`'s JWK cache) if that assumption ever
    /// stops holding.
    name_cache: DashMap<RepositoryId, String>,
}

impl ConfiguredGrantsAuthorizer {
    pub fn new(
        permission_claim: Option<String>,
        config: acl_config::AclConfig,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
    ) -> Self {
        Self {
            permission_claim,
            config,
            immutable_store,
            mutable_store,
            name_cache: DashMap::new(),
        }
    }

    /// Identical extraction to [`GlobalGrantsAuthorizer::held_actions`] --
    /// same claim shapes accepted (a JSON array of strings, or a single bare
    /// string), same fail-closed behavior on anything else -- just returning
    /// every value found rather than testing membership, since the caller
    /// needs the whole group set to hand to [`acl_config::resolve`].
    fn caller_groups(&self, claims: &AuthorizationToken) -> Vec<String> {
        let Some(path) = self.permission_claim.as_deref() else {
            return Vec::new();
        };
        match claims.claim_at(path) {
            Some(serde_json::Value::Array(items)) => items
                .into_iter()
                .filter_map(|item| match item {
                    serde_json::Value::String(s) => Some(s),
                    _ => None,
                })
                .collect(),
            Some(serde_json::Value::String(single)) => vec![single],
            _ => Vec::new(),
        }
    }

    /// Resolves `repository`'s human-readable name: a cache hit returns
    /// without suspending, and a cache miss `.await`s the two real store
    /// reads (`metadata_hash` then `metadata`, per `lore_revision::repository`)
    /// like any other async I/O -- no `block_in_place`, no special bridging.
    /// Every caller of [`RepositoryAuthorizer::check_repository_access`] is
    /// already async and can simply await a cache miss (see the type's doc
    /// comment); the two callers that cannot await anything
    /// ([`ReachabilityAuthorizer::check_reachability_sync`]'s gRPC
    /// interceptor and cross-partition link-read closure) get their
    /// sync/async bridge there, not here.
    ///
    /// Confirmed local-only: a server context's `remote` is always
    /// `RemoteState::Offline` (`RepositoryContext::new_server_context`), so
    /// this never depends on network I/O beyond the local store's own
    /// latency.
    ///
    /// A repository that genuinely does not exist is reported as
    /// [`Status::permission_denied`], not [`Status::not_found`]: from an
    /// authorization function's perspective this is a "no" like any
    /// other -- there is nothing to grant access to -- and
    /// [`action_held`] (relied on by every once-per-connection caller that
    /// resolves `read`/`push` up front, e.g. `resolve_baseline_actions`)
    /// only recognizes `PermissionDenied`/`Unauthenticated` as a genuine
    /// denial, folding anything else into "the check itself failed".
    /// Reporting `NotFound` here would misclassify an ordinary, expected
    /// denial (a client referencing a repository that was never created,
    /// or a stale id) as an infrastructure failure -- the same
    /// misclassification class the `AuthClientAuthorizer` "resource absent
    /// from response" fix (`interpret_check_user_permission_response_tests`)
    /// exists to prevent.
    ///
    /// Crucially, this distinction is made on the *classified* error
    /// (`RepositoryError::translated()`, which separates `NotFound`-style
    /// variants from `Disconnected`/`SlowDown`/`Maintenance`/`NotConnected`/
    /// etc.), not blindly on "any `Err` from resolution": a transient store
    /// hiccup or timeout is not the same event as a nonexistent repository,
    /// and folding it into the same `PermissionDenied` would be the exact
    /// inverse bug -- a legitimate caller silently denied access during a
    /// transient outage instead of getting a retryable error. See
    /// [`Self::resolution_error_to_status`].
    async fn resolve_name(&self, repository: RepositoryId) -> Result<String, Status> {
        if let Some(name) = self.name_cache.get(&repository) {
            return Ok(name.clone());
        }

        let repository_context = Arc::new(RepositoryContext::new_server_context(
            self.immutable_store.clone(),
            self.mutable_store.clone(),
            repository,
        ));
        let metadata_hash = repository::metadata_hash(repository_context.clone())
            .await
            .filter_slow_down()?
            .map_err(|err| Self::resolution_error_to_status(repository, err))?;
        let resolved = repository::metadata(repository_context, metadata_hash)
            .await
            .filter_slow_down()?
            .map_err(|err| Self::resolution_error_to_status(repository, err))?
            .name;

        self.name_cache.insert(repository, resolved.clone());
        Ok(resolved)
    }

    /// Classifies a [`RepositoryError`] encountered while resolving a
    /// repository's name for authorization: only a genuine not-found --
    /// `RepositoryError::translated()` mapping to [`LoreError::NotFound`],
    /// [`LoreError::AddressNotFound`] or [`LoreError::PayloadNotFound`] --
    /// becomes [`Status::permission_denied`], see [`Self::resolve_name`]'s
    /// doc comment for why. All three are genuinely "does not exist," not
    /// just the generically-named `NotFound`/`RepositoryNotFound` variants:
    /// `metadata_hash`'s mutable-store lookup reports a missing key as
    /// `AddressNotFound` specifically (confirmed against
    /// `MutableStore::load`'s contract and implementation), and
    /// `metadata`'s immutable-store read can equally surface as
    /// `AddressNotFound` or `PayloadNotFound`.
    ///
    /// Everything else (`Disconnected`, `Maintenance`, `NotConnected`, or
    /// any other variant `translated()` maps to
    /// [`LoreError::Internal`]/[`LoreError::Connection`]) is a real
    /// infrastructure failure and surfaces as one via
    /// [`warn_error_to_status`], so [`action_held`] treats it as
    /// retryable rather than as a clean denial. `SlowDown` is peeled off by
    /// [`FilterSlowDownExt::filter_slow_down`] before this is even reached,
    /// matching every other `RepositoryError`-to-`Status` conversion in
    /// this codebase.
    fn resolution_error_to_status(repository: RepositoryId, err: RepositoryError) -> Status {
        // `metadata_hash`'s mutable-store lookup reports a missing key as
        // `AddressNotFound` (confirmed via `MutableStore::load`'s own doc
        // comment and implementation), not the generic `NotFound`/
        // `RepositoryNotFound` variants -- an immutable-store miss inside
        // `metadata`'s `Metadata::deserialize` can equally surface as
        // `AddressNotFound` or `PayloadNotFound`. All three are "genuinely
        // does not exist," the same as `NotFound`/`RepositoryNotFound`;
        // nothing else is.
        if matches!(
            err.translated(),
            LoreError::NotFound | LoreError::AddressNotFound | LoreError::PayloadNotFound
        ) {
            debug!(%repository, %err, "repository not found while resolving name for authorization");
            return Status::permission_denied(format!(
                "caller does not hold any grant on repository {repository}"
            ));
        }
        warn_error_to_status(&err, |err| {
            Status::internal(format!(
                "failed to resolve repository {repository} for authorization: {err}"
            ))
        })
    }

    /// Shared by both [`RepositoryAuthorizer`] methods once each has settled
    /// on the repository name to check against: unions the caller's grants
    /// via [`acl_config::resolve`] and turns the answer into the same
    /// `Result<(), Status>` shape every authorizer returns.
    fn authorize_against(
        &self,
        caller_groups: &[String],
        repository_name: &str,
        action: Option<&str>,
    ) -> Result<(), Status> {
        if acl_config::resolve(&self.config, repository_name, caller_groups, action) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "caller does not hold the '{}' action on repository '{repository_name}'",
                action.unwrap_or("<reachability>")
            )))
        }
    }
}

#[async_trait]
impl RepositoryAuthorizer for ConfiguredGrantsAuthorizer {
    async fn check_repository_access(
        &self,
        token: Option<&VerifiedToken<'_>>,
        repository: RepositoryId,
        action: Option<&str>,
    ) -> Result<(), Status> {
        let Some(token) = token else {
            return Err(Status::unauthenticated(
                "authentication required for configured-grants authorization",
            ));
        };

        let repository_name = self.resolve_name(repository).await?;
        let caller_groups = self.caller_groups(token.claims);
        self.authorize_against(&caller_groups, &repository_name, action)
    }

    async fn check_repository_access_by_name(
        &self,
        token: Option<&VerifiedToken<'_>>,
        repository_name: &str,
        action: Option<&str>,
    ) -> Result<(), Status> {
        let Some(token) = token else {
            return Err(Status::unauthenticated(
                "authentication required for configured-grants authorization",
            ));
        };

        let caller_groups = self.caller_groups(token.claims);
        self.authorize_against(&caller_groups, repository_name, action)
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

    /// `server.auth.acl_config_path` (GOALS fork) is set but the file it
    /// names failed to load or parse. `Settings::load`'s
    /// `validate_auth_config` already checks this once at startup (see its
    /// doc comment); this is the same defense-in-depth backstop pattern as
    /// [`Self::ResourceGrantsNotImplemented`], made again here where the
    /// authorizer is actually constructed.
    #[error("failed to load server.auth.acl_config_path: {0}")]
    AclConfig(#[from] acl_config::AclConfigError),

    /// `server.auth.acl_config_path` is set, which selects
    /// [`ConfiguredGrantsAuthorizer`], but no repository-metadata store was
    /// supplied to resolve names against. Reached only by a caller that
    /// builds an authorizer via [`repository_authorizer`] /
    /// [`ReachabilityAuthorizer::new`] (which pass no stores at all) for a
    /// deployment that also sets `acl_config_path` — server bootstrap always
    /// uses [`repository_authorizer_with_stores`] /
    /// [`ReachabilityAuthorizer::new_with_stores`] instead, which cannot
    /// reach this.
    #[error(
        "server.auth.acl_config_path is set, which selects ConfiguredGrantsAuthorizer, but no \
         repository metadata store was supplied to resolve repository names against. Construct \
         the authorizer via repository_authorizer_with_stores / \
         ReachabilityAuthorizer::new_with_stores instead."
    )]
    AclConfigRequiresStores,
}

/// Creates the appropriate authorizer for a deployment's configuration.
///
/// Branches exactly as LEP 2026-08-20-oidc-oauth2-authentication (D8)
/// describes, plus one GOALS-fork addition (see
/// [`repository_authorizer_with_stores`]'s doc comment for the full
/// decision tree, including where `ConfiguredGrantsAuthorizer` fits):
///
/// - No `[server.auth]` at all: [`AllowAllRepositoryAuthorizer`]. Every
///   check passes, matching today's behavior for a deployment with no OIDC
///   integration.
/// - A legacy `auth_url` configured (`[environment.endpoint].auth_url`):
///   [`AuthClientAuthorizer`], unconditionally. This mirrors the existing
///   factory exactly — legacy deployments select this authorizer whether or
///   not `[server.auth]` itself is also present, which is the shape today's
///   callers already rely on.
/// - `[server.auth]` present, no legacy `auth_url`, no `resource_claim`, no
///   `acl_config_path`: Tier 1's [`GlobalGrantsAuthorizer`], reading
///   `permission_claim`.
/// - `[server.auth]` present, no legacy `auth_url`, `resource_claim` set:
///   Tier 2. Not implemented — see [`RepositoryAuthorizerError`].
///   `Settings::load`'s startup validation rejects this configuration before
///   the server ever accepts a request; this is the defense-in-depth
///   backstop for any caller that builds an authorizer without going
///   through that validation.
///
/// This is a thin, stores-less wrapper around
/// [`repository_authorizer_with_stores`] (`immutable_store`/`mutable_store`
/// both `None`) kept for the many existing callers — mostly test fixtures
/// building an authorizer that is never asked to select
/// `ConfiguredGrantsAuthorizer` — that have no store to offer. See that
/// function's doc comment for why the store-accepting form is a separate
/// function rather than a signature change here.
pub fn repository_authorizer(
    auth_url: Option<String>,
    auth_settings: Option<&AuthSettings>,
) -> Result<Arc<dyn RepositoryAuthorizer>, RepositoryAuthorizerError> {
    repository_authorizer_with_stores(auth_url, auth_settings, None, None)
}

/// Stores-aware counterpart of [`repository_authorizer`]: the same decision
/// tree, plus the GOALS-fork branch that needs a repository-metadata store
/// to resolve names against.
///
/// Kept as a separate function — rather than adding
/// `immutable_store`/`mutable_store` parameters to [`repository_authorizer`]
/// directly — so the ~30 existing call sites across `lore-server` and
/// `lore-integration-tests` that construct an authorizer purely for
/// [`AllowAllRepositoryAuthorizer`]/[`GlobalGrantsAuthorizer`]/
/// [`AuthClientAuthorizer`] behavior in tests, none of which ever set
/// `acl_config_path`, need no changes at all. `immutable_store`/
/// `mutable_store` are `None` exactly when the caller has none to offer
/// (i.e. every call through the plain [`repository_authorizer`]); that is
/// only an error if `acl_config_path` is *also* set, selecting
/// [`ConfiguredGrantsAuthorizer`] (see
/// [`RepositoryAuthorizerError::AclConfigRequiresStores`]) — every other
/// branch ignores both parameters entirely, so passing `None` is free for
/// them.
///
/// The full decision tree:
///
/// - No `[server.auth]` at all: [`AllowAllRepositoryAuthorizer`].
/// - A legacy `auth_url` configured: [`AuthClientAuthorizer`],
///   unconditionally, regardless of what else is set.
/// - `[server.auth]` present, no legacy `auth_url`, `resource_claim` set:
///   Tier 2, not implemented — see [`RepositoryAuthorizerError::ResourceGrantsNotImplemented`].
/// - `[server.auth]` present, no legacy `auth_url`, no `resource_claim`,
///   `acl_config_path` set (GOALS fork,
///   `docs/proposals/2026-09-09-goals-repository-acl-config.md`):
///   [`ConfiguredGrantsAuthorizer`], loading and parsing the named file via
///   [`acl_config::load_from_path`]. `Settings::load`'s `validate_auth_config`
///   already parses this file once at startup as a fail-fast check (see its
///   doc comment); this is that same defense-in-depth pattern, parsing it
///   again where the authorizer is actually built.
/// - Otherwise: Tier 1's [`GlobalGrantsAuthorizer`], reading
///   `permission_claim`.
pub fn repository_authorizer_with_stores(
    auth_url: Option<String>,
    auth_settings: Option<&AuthSettings>,
    immutable_store: Option<Arc<dyn lore_storage::ImmutableStore>>,
    mutable_store: Option<Arc<dyn lore_storage::MutableStore>>,
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

    if let Some(acl_config_path) = auth.acl_config_path.as_deref() {
        let (immutable_store, mutable_store) = match (immutable_store, mutable_store) {
            (Some(immutable_store), Some(mutable_store)) => (immutable_store, mutable_store),
            _ => return Err(RepositoryAuthorizerError::AclConfigRequiresStores),
        };
        let config = acl_config::load_from_path(Path::new(acl_config_path))?;
        return Ok(Arc::new(ConfiguredGrantsAuthorizer::new(
            auth.permission_claim.clone(),
            config,
            immutable_store,
            mutable_store,
        )));
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
    /// Stores-less constructor: cannot select `ConfiguredGrantsAuthorizer`
    /// for a deployment that sets `acl_config_path`, since it has no
    /// repository-metadata store to hand it (see
    /// [`repository_authorizer_with_stores`]'s doc comment). Used by every
    /// call site except server bootstrap — none of them ever configure
    /// `acl_config_path`.
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

    /// Stores-aware constructor, for the one real caller that can select
    /// `ConfiguredGrantsAuthorizer`: server bootstrap (`server.rs`), which
    /// has `immutable_store`/`mutable_store` in hand before constructing
    /// this. See [`repository_authorizer_with_stores`] for the full
    /// decision tree and why this is a separate entry point rather than a
    /// signature change to [`Self::new`].
    pub fn new_with_stores(
        auth_url: Option<String>,
        auth_settings: Option<&AuthSettings>,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
    ) -> Result<Self, RepositoryAuthorizerError> {
        let legacy_resource_claim = auth_url.is_some();
        let authorizer = repository_authorizer_with_stores(
            auth_url,
            auth_settings,
            Some(immutable_store),
            Some(mutable_store),
        )?;
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
    /// non-legacy branch only ever reaches [`AllowAllRepositoryAuthorizer`],
    /// [`GlobalGrantsAuthorizer`] or [`ConfiguredGrantsAuthorizer`] (GOALS
    /// fork), none of which reads [`VerifiedToken::raw`] either — only
    /// [`AuthClientAuthorizer`] does, and this wrapper exists specifically
    /// to never reach it. There is therefore no real value to thread
    /// through the call sites that use this method, several of which do
    /// not have the raw compact serialization to hand in the first place.
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
    /// link-read closure — both run on `lore_base::runtime::net_runtime()`
    /// (every gRPC/QUIC endpoint is spawned there via `lore_spawn_net!` in
    /// `server.rs`), and neither can await anything: tonic's `Interceptor`
    /// trait and `lore_revision::state::CanReadRepository` are both plain
    /// sync signatures.
    ///
    /// For [`AllowAllRepositoryAuthorizer`], [`GlobalGrantsAuthorizer`] and
    /// the legacy claims-only branch, `check_reachability`'s future never
    /// awaits anything at all, so `now_or_never()` always succeeds on the
    /// first, fast path below. [`ConfiguredGrantsAuthorizer`] (GOALS fork)
    /// breaks that invariant on a name-cache miss: resolving a
    /// `RepositoryId` to a name is genuine async store I/O (see
    /// [`ConfiguredGrantsAuthorizer::resolve_name`]), so its future can
    /// legitimately return `Pending` on first poll. Rather than requiring
    /// every [`RepositoryAuthorizer`] impl to somehow never suspend (which
    /// is what made the earlier, since-removed `resolve_name_sync` bridge
    /// fragile), this method handles both outcomes directly: `now_or_never()`
    /// as the zero-cost fast path, falling back to
    /// `task::block_in_place` + `core_runtime().block_on(..)` — the same
    /// cache-then-block_in_place shape
    /// [`crate::auth::jwt_interceptor::authorize`] already uses for the
    /// identical "a sync call site needs a value backed by async I/O"
    /// problem — only when the fast path doesn't resolve.
    ///
    /// `core_runtime()`, not `lore_base::runtime::runtime()`, is used
    /// deliberately for the future actually being driven: `runtime()`
    /// resolves to `Handle::try_current()`, which on these two call sites
    /// is `net_runtime`'s own handle — the "safe to block" runtime is
    /// `core_runtime`, per its own doc comment ("blocking work must land on
    /// core's pool wherever it is issued from"), not whatever happens to be
    /// ambient. This does **not** eliminate every cost of blocking here:
    /// `task::block_in_place` itself hands this *thread's* remaining
    /// `net_runtime` work to a replacement drawn from `net_runtime`'s own
    /// blocking pool (`max_blocking_threads(1)`) for as long as the
    /// fallback runs, regardless of which runtime's `block_on` is called
    /// inside it — that part is unavoidable for a genuinely synchronous
    /// bridge reached from `net_runtime` (the same constraint
    /// `jwt_interceptor::authorize`'s own JWK-cache-miss fallback is
    /// already subject to, pre-existing and unrelated to this change). What
    /// keeps this bounded rather than a standing risk: the name cache never
    /// invalidates (see [`ConfiguredGrantsAuthorizer`]'s `name_cache` field), so this
    /// fallback is reached at most once per distinct repository for the
    /// life of the process, not on every request.
    pub fn check_reachability_sync(
        &self,
        claims: &AuthorizationToken,
        repository: RepositoryId,
    ) -> Result<(), Status> {
        match self.check_reachability(claims, repository).now_or_never() {
            Some(result) => result,
            None => {
                // See the doc comment above: reached only when the fast
                // path's `now_or_never()` observes a genuine suspension
                // (a `ConfiguredGrantsAuthorizer` name-cache miss) —
                // `AllowAllRepositoryAuthorizer`, `GlobalGrantsAuthorizer`
                // and the legacy claims branch never reach this arm.
                #[allow(clippy::disallowed_methods)]
                task::block_in_place(|| {
                    core_runtime().block_on(self.check_reachability(claims, repository))
                })
            }
        }
    }
}

/// Distinguishes a genuine "denied" answer from a failure of the check
/// itself.
///
/// Every [`RepositoryAuthorizer`] implementation returns
/// [`Code::PermissionDenied`] or [`Code::Unauthenticated`] for an actual
/// "no" (see each impl's `check_repository_access` above) and anything else
/// — [`AuthClientAuthorizer`]'s `Status::internal` for a failed or timed-out
/// online call, most notably — for a failure of the check itself, not an
/// answer to the question asked. Callers that resolve `read` and `push` as
/// two independent calls and cache the pair for a connection or session's
/// entire lifetime (`Connect::handle_auth`, `StorageServiceV4`'s
/// `AuthorizeStart`) must not fold the latter into "not held": doing so
/// pins the connection to whatever the other, unaffected call happened to
/// answer, indistinguishable from a real, deliberate denial, for as long as
/// the cached value lives. Propagating the failure as `Err` instead lets
/// the caller surface it as the infrastructure problem it is.
pub(crate) fn action_held(result: Result<(), Status>) -> Result<bool, Status> {
    match result {
        Ok(()) => Ok(true),
        Err(status)
            if matches!(
                status.code(),
                Code::PermissionDenied | Code::Unauthenticated
            ) =>
        {
            Ok(false)
        }
        Err(status) => Err(status),
    }
}

/// Resolves whether `token` holds the `read` and `push` baseline actions on
/// `repository` against `authorizer` as two independent calls, applying
/// [`action_held`] to each so a failure of either check propagates as `Err`
/// rather than being silently folded into "not held" — see its doc comment.
///
/// Shared by the two once-per-connection/session sites that resolve both
/// actions up front and cache the pair for the connection or session's
/// entire lifetime: `Connect::handle_auth` (legacy `urc/0.2` QUIC) and
/// `StorageServiceV4`'s `AuthorizeStart` (v4 QUIC). Both go through
/// `authorizer` directly rather than [`ReachabilityAuthorizer`]'s
/// legacy-degrading `check_read`/`check_push`: this runs once per
/// connection or session, not per fragment, so it can afford
/// [`AuthClientAuthorizer`]'s online call at the granularity that
/// authorizer was built for, rather than the local approximation the
/// per-fragment `Copy` checks use.
pub(crate) async fn resolve_baseline_actions(
    authorizer: &dyn RepositoryAuthorizer,
    token: &VerifiedToken<'_>,
    repository: RepositoryId,
) -> Result<(bool, bool), Status> {
    let holds_read = action_held(
        authorizer
            .check_repository_access(Some(token), repository, Some(READ_ACTION))
            .await,
    )?;
    let holds_push = action_held(
        authorizer
            .check_repository_access(Some(token), repository, Some(PUSH_ACTION))
            .await,
    )?;
    Ok((holds_read, holds_push))
}

#[cfg(test)]
mod tests {
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::Context;
    use lore_revision::repository::RepositoryMetadata;
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

    /// Stores `name` as `repository`'s `RepositoryMetadata`, the same two
    /// writes `RepositoryCreate` performs
    /// (`metadata_store`/`metadata_store_hash`) -- enough for
    /// `ConfiguredGrantsAuthorizer::resolve_name` (via
    /// `repository::metadata_hash`/`metadata`) to resolve `repository` back
    /// to `name`. Must run inside a `LORE_CONTEXT::scope` matching the
    /// stores' own execution context, same as every other test that seeds
    /// repository metadata in this codebase (see
    /// `grpc/handlers/repository_metadata_get.rs`'s `seed_metadata`).
    async fn seed_repository(
        immutable: Arc<dyn lore_storage::ImmutableStore>,
        mutable: Arc<dyn lore_storage::MutableStore>,
        repository: RepositoryId,
        name: &str,
    ) {
        let repo_ctx = Arc::new(RepositoryContext::new_server_context(
            immutable, mutable, repository,
        ));
        let hash = lore_revision::repository::metadata_store(
            repo_ctx.clone(),
            RepositoryMetadata {
                name: name.to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("metadata_store should succeed against an in-memory test store");
        lore_revision::repository::metadata_store_hash(repo_ctx, hash)
            .await
            .expect("metadata_store_hash should succeed against an in-memory test store");
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

    /// `ConfiguredGrantsAuthorizer` (GOALS fork): claim extraction, name
    /// resolution, and grant matching exercised end to end through the real
    /// `RepositoryAuthorizer` trait methods -- not re-testing
    /// `acl_config::resolve` itself, which already has its own extensive
    /// coverage in `acl_config.rs`, but proving the *wrapper* around it
    /// (claim extraction, the `RepositoryId -> name` cache, the sync
    /// bridge) actually wires up correctly.
    mod configured_grants {
        use lore_base::lore_spawn_net;

        use super::*;

        fn config_with(grants: Vec<acl_config::Grant>) -> acl_config::AclConfig {
            acl_config::AclConfig { grants }
        }

        fn grant(group: &str, repos: &[&str], permissions: &[&str]) -> acl_config::Grant {
            acl_config::Grant {
                group: group.to_string(),
                repos: repos.iter().map(|s| s.to_string()).collect(),
                permissions: permissions.iter().map(|s| s.to_string()).collect(),
            }
        }

        async fn authorizer_with(
            grants: Vec<acl_config::Grant>,
        ) -> (
            ConfiguredGrantsAuthorizer,
            Arc<dyn lore_storage::ImmutableStore>,
            Arc<dyn lore_storage::MutableStore>,
            Arc<lore_revision::interface::ExecutionContext>,
        ) {
            let (immutable, mutable, execution) =
                crate::store::test_store_create().await.expect("stores");
            let authorizer = ConfiguredGrantsAuthorizer::new(
                Some("groups".to_string()),
                config_with(grants),
                immutable.clone(),
                mutable.clone(),
            );
            (authorizer, immutable, mutable, execution)
        }

        fn token_in_groups(groups: &[&str]) -> AuthorizationToken {
            AuthorizationToken {
                groups: Some(groups.iter().map(|s| s.to_string()).collect()),
                ..Default::default()
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn end_to_end_claim_extraction_name_resolution_and_grant_matching() {
            let (authorizer, immutable, mutable, execution) =
                authorizer_with(vec![grant("art-leads", &["art/*"], &["read", "push"])]).await;

            LORE_CONTEXT
                .scope(execution, async move {
                    seed_repository(immutable, mutable, repository(), "art/characters").await;

                    let claims = token_in_groups(&["art-leads"]);
                    let token = VerifiedToken::new("raw", &claims);

                    authorizer
                        .check_repository_access(Some(&token), repository(), Some(READ_ACTION))
                        .await
                        .expect("art-leads holds read on art/characters via the art/* pattern");
                    authorizer
                        .check_repository_access(Some(&token), repository(), Some(PUSH_ACTION))
                        .await
                        .expect("art-leads also holds push");
                    let err = authorizer
                        .check_repository_access(Some(&token), repository(), Some("admin"))
                        .await
                        .expect_err("admin was never granted");
                    assert_eq!(err.code(), tonic::Code::PermissionDenied);

                    // A caller not in the granted group is denied even
                    // though the repository name matches.
                    let other_claims = token_in_groups(&["qa"]);
                    let other_token = VerifiedToken::new("raw", &other_claims);
                    let err = authorizer
                        .check_repository_access(
                            Some(&other_token),
                            repository(),
                            Some(READ_ACTION),
                        )
                        .await
                        .expect_err("qa holds no grant on this repository");
                    assert_eq!(err.code(), tonic::Code::PermissionDenied);
                })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn denies_when_the_resolved_name_does_not_match_any_pattern() {
            let (authorizer, immutable, mutable, execution) =
                authorizer_with(vec![grant("art-leads", &["art/*"], &["read"])]).await;

            LORE_CONTEXT
                .scope(execution, async move {
                    // Same group, but the resolved name falls outside every
                    // pattern this group holds a grant for.
                    seed_repository(immutable, mutable, repository(), "backend").await;

                    let claims = token_in_groups(&["art-leads"]);
                    let token = VerifiedToken::new("raw", &claims);
                    let err = authorizer
                        .check_repository_access(Some(&token), repository(), Some(READ_ACTION))
                        .await
                        .expect_err("the resolved name does not match art/*");
                    assert_eq!(err.code(), tonic::Code::PermissionDenied);
                })
                .await;
        }

        /// A repository whose name cannot be resolved at all (unknown id,
        /// or a store failure) must be reported as `PermissionDenied`, not
        /// `NotFound`: `action_held` (relied on by `resolve_baseline_actions`,
        /// which resolves `read`/`push` up front and caches the pair for a
        /// connection or session's entire lifetime) only recognizes
        /// `PermissionDenied`/`Unauthenticated` as a genuine "no", folding
        /// anything else into "the check itself failed" and propagating it
        /// as `Err` rather than a clean denial. This is the same
        /// misclassification class the `AuthClientAuthorizer`
        /// "resource absent from response" fix
        /// (`interpret_check_user_permission_response_tests`, the "Round 5
        /// regression") exists to prevent, for a new reason: a client
        /// referencing a repository that was never created (or a stale id)
        /// must be denied cleanly, not surfaced to a QUIC/gRPC caller as a
        /// 500-class internal error.
        #[tokio::test]
        async fn unresolvable_repository_is_a_clean_denial_not_an_infra_failure() {
            let (authorizer, _immutable, _mutable, _execution) =
                authorizer_with(vec![grant("engineering", &["**"], &["read"])]).await;
            let claims = token_in_groups(&["engineering"]);
            let token = VerifiedToken::new("raw", &claims);

            // No metadata was ever seeded for this id.
            let unknown_repository: RepositoryId = Context::from([9u8; 16]).into();
            let err = authorizer
                .check_repository_access(Some(&token), unknown_repository, Some(READ_ACTION))
                .await
                .expect_err("an unresolvable repository must be denied, not succeed");
            assert_eq!(
                err.code(),
                tonic::Code::PermissionDenied,
                "must be a recognized denial, not e.g. NotFound, which action_held cannot \
                 distinguish from an infra failure"
            );

            let held = action_held(Err(err)).expect(
                "action_held must recognize this as a clean denial, not propagate it as an \
                 infra failure",
            );
            assert!(!held, "an unresolvable repository holds no action");
        }

        /// `MutableStore` wrapper that fails `load` unconditionally with a
        /// `Disconnected`-shaped error, delegating everything else to
        /// `inner`. Used to distinguish a genuine store outage from a
        /// genuinely nonexistent repository -- the two must not be
        /// classified the same way (see
        /// `unresolvable_repository_is_a_clean_denial_not_an_infra_failure`
        /// for the not-found case, and
        /// `store_outage_is_an_infra_failure_not_a_clean_denial` below for
        /// this one). Mirrors
        /// `grpc/revision/v1/revision_list.rs`'s `FailingMutableStore`.
        struct FailingMutableStore {
            inner: Arc<dyn lore_storage::MutableStore>,
        }

        #[async_trait::async_trait]
        impl lore_storage::MutableStore for FailingMutableStore {
            async fn load(
                self: Arc<Self>,
                _partition: lore_storage::Partition,
                _key: lore_storage::Hash,
                _key_type: lore_storage::KeyType,
            ) -> Result<lore_storage::Hash, lore_storage::StoreError> {
                Err(lore_storage::StoreError::from(
                    lore_storage::errors::Disconnected,
                ))
            }

            async fn store(
                self: Arc<Self>,
                partition: lore_storage::Partition,
                key: lore_storage::Hash,
                value: lore_storage::Hash,
                key_type: lore_storage::KeyType,
            ) -> Result<(), lore_storage::StoreError> {
                self.inner
                    .clone()
                    .store(partition, key, value, key_type)
                    .await
            }

            async fn compare_and_swap(
                self: Arc<Self>,
                partition: lore_storage::Partition,
                key: lore_storage::Hash,
                expected: lore_storage::Hash,
                value: lore_storage::Hash,
                key_type: lore_storage::KeyType,
            ) -> Result<lore_storage::Hash, lore_storage::StoreError> {
                self.inner
                    .clone()
                    .compare_and_swap(partition, key, expected, value, key_type)
                    .await
            }

            async fn list(
                self: Arc<Self>,
                partition: lore_storage::Partition,
                key_type: lore_storage::KeyType,
            ) -> Result<lore_storage::KeyValueStream, lore_storage::StoreError> {
                self.inner.clone().list(partition, key_type).await
            }

            async fn flush(
                self: Arc<Self>,
                sync_data: bool,
            ) -> Result<(), lore_storage::StoreError> {
                self.inner.clone().flush(sync_data).await
            }
        }

        /// The other half of the distinction `resolution_error_to_status`
        /// exists to make: a store read that fails with a genuine
        /// infrastructure error (`Disconnected`, translated to
        /// `LoreError::Connection`, unlike the `AddressNotFound`/
        /// `PayloadNotFound`/`NotFound` trio that means "genuinely does not
        /// exist") must surface as a real error -- so `action_held`
        /// classifies it as retryable -- and must NOT be collapsed into
        /// `Status::permission_denied`. Getting this wrong would silently
        /// deny a legitimate caller during a transient store outage instead
        /// of giving them a retryable error: the exact inverse of the bug
        /// `unresolvable_repository_is_a_clean_denial_not_an_infra_failure`
        /// guards against.
        #[tokio::test]
        async fn store_outage_is_an_infra_failure_not_a_clean_denial() {
            let (authorizer, immutable, mutable, _execution) =
                authorizer_with(vec![grant("engineering", &["**"], &["read"])]).await;
            let failing_mutable: Arc<dyn lore_storage::MutableStore> =
                Arc::new(FailingMutableStore { inner: mutable });
            // Rebuild against the failing store: `ConfiguredGrantsAuthorizer`
            // takes its stores at construction, so a fresh instance is
            // needed to point `metadata_hash`'s lookup at the fake.
            let authorizer = ConfiguredGrantsAuthorizer::new(
                authorizer.permission_claim,
                authorizer.config,
                immutable,
                failing_mutable,
            );

            let claims = token_in_groups(&["engineering"]);
            let token = VerifiedToken::new("raw", &claims);
            let err = authorizer
                .check_repository_access(Some(&token), repository(), Some(READ_ACTION))
                .await
                .expect_err("a store outage must not silently succeed either");
            assert_ne!(
                err.code(),
                tonic::Code::PermissionDenied,
                "a transient store outage must not be classified as a clean denial"
            );

            let held = action_held(Err(err));
            assert!(
                held.is_err(),
                "action_held must propagate a store outage as an infra failure (Err), not fold \
                 it into Ok(false) as if it were a clean denial"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn reachability_none_succeeds_on_any_grant_but_is_not_a_specific_action() {
            let (authorizer, immutable, mutable, execution) =
                authorizer_with(vec![grant("engineering", &["**"], &["push"])]).await;

            LORE_CONTEXT
                .scope(execution, async move {
                    seed_repository(immutable, mutable, repository(), "any/name").await;
                    let claims = token_in_groups(&["engineering"]);
                    let token = VerifiedToken::new("raw", &claims);

                    authorizer
                        .check_repository_access(Some(&token), repository(), None)
                        .await
                        .expect("the caller holds *something* here (push)");
                    let err = authorizer
                        .check_repository_access(Some(&token), repository(), Some(READ_ACTION))
                        .await
                        .expect_err(
                            "reachability succeeding must not be treated as authorizing read",
                        );
                    assert_eq!(err.code(), tonic::Code::PermissionDenied);
                })
                .await;
        }

        #[tokio::test]
        async fn no_token_is_unauthenticated() {
            let (authorizer, _immutable, _mutable, _execution) = authorizer_with(vec![]).await;
            let err = authorizer
                .check_repository_access(None, repository(), None)
                .await
                .expect_err("a missing token must not be treated as allow");
            assert_eq!(err.code(), tonic::Code::Unauthenticated);
        }

        /// `check_repository_access_by_name`: the `RepositoryCreate`
        /// name-threading path. No `RepositoryId` is resolved at all here --
        /// the override goes straight from the caller-supplied name to
        /// `acl_config::resolve`, which is exactly what makes it usable for
        /// a repository that does not exist yet (no store lookup involved,
        /// so no store is even seeded in this test).
        #[tokio::test]
        async fn check_repository_access_by_name_matches_directly_with_no_store_lookup() {
            let (authorizer, ..) =
                authorizer_with(vec![grant("art-leads", &["art/*"], &["push"])]).await;

            let claims = token_in_groups(&["art-leads"]);
            let token = VerifiedToken::new("raw", &claims);

            authorizer
                .check_repository_access_by_name(
                    Some(&token),
                    "art/newly-created",
                    Some(PUSH_ACTION),
                )
                .await
                .expect("the requested name matches art/* for a group the caller belongs to");

            let err = authorizer
                .check_repository_access_by_name(
                    Some(&token),
                    "unrelated/newly-created",
                    Some(PUSH_ACTION),
                )
                .await
                .expect_err("the requested name does not match any pattern this group holds");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        /// `check_reachability_sync`'s no-panic property, proven under the
        /// *real* constrained runtime both production call sites (the gRPC
        /// interceptor and the link-read closure) actually run on:
        /// `lore_base::runtime::net_runtime()`, built with
        /// `max_blocking_threads(1)`. This deliberately does **not** run
        /// under this test's own ambient `#[tokio::test]` runtime (default,
        /// much larger blocking pool) -- that would prove nothing, since
        /// this property would hold trivially there even if the bridge
        /// were unsafe under `net_runtime`'s real constraint. `lore_spawn_net!`
        /// (the same macro every gRPC/QUIC endpoint uses in `server.rs`)
        /// spawns the real work onto `net_runtime` exactly the way
        /// production does. Neither `check_reachability_sync` nor the store
        /// reads it triggers need `LORE_CONTEXT` (confirmed:
        /// `repository::metadata_hash`/`metadata` and `Metadata::serialize`/
        /// `deserialize` never call `execution_context()`), so this test
        /// does not thread one through the spawned task.
        #[tokio::test]
        async fn check_reachability_sync_does_not_panic_under_net_runtimes_constrained_blocking_pool()
         {
            let (authorizer, immutable, mutable, execution) =
                authorizer_with(vec![grant("engineering", &["**"], &["read"])]).await;
            LORE_CONTEXT
                .scope(
                    execution,
                    seed_repository(immutable, mutable, repository(), "backend"),
                )
                .await;

            let reachability = ReachabilityAuthorizer {
                authorizer: Arc::new(authorizer),
                legacy_resource_claim: false,
            };
            let claims = token_in_groups(&["engineering"]);
            let denied_claims = token_in_groups(&["qa"]);

            lore_spawn_net!(async move {
                // Cold cache: `resolve_name` has never seen this
                // `RepositoryId` before, so this goes through the
                // `block_in_place` + `core_runtime().block_on(..)`
                // fallback -- on a real `net_runtime` worker thread, with
                // `net_runtime`'s real, single-slot blocking pool. This
                // must not panic (the historical failure mode: an async,
                // I/O-bound authorizer plugged into `now_or_never()`
                // unchanged panics here on first real use) and must answer
                // correctly.
                reachability
                    .check_reachability_sync(&claims, repository())
                    .expect(
                        "cold-cache resolution must not panic under net_runtime's constrained \
                         blocking pool",
                    );

                // Warm cache: answered purely from the cache -- the fast
                // `now_or_never()` path, no blocking-pool involvement at
                // all.
                reachability
                    .check_reachability_sync(&claims, repository())
                    .expect("warm-cache resolution must not panic either");

                // A denied caller on the same (now warm) cache entry also
                // must not panic, proving the property for the deny path
                // too, not just the allow path.
                let err = reachability
                    .check_reachability_sync(&denied_claims, repository())
                    .expect_err("qa holds no grant here");
                assert_eq!(err.code(), tonic::Code::PermissionDenied);
            })
            .await
            .expect("the task spawned onto net_runtime must not panic");
        }

        /// Blocking-pool *safety* under real contention, not just a single
        /// call succeeding: two distinct repositories, both cold, resolved
        /// *concurrently* on `net_runtime`. `net_runtime`'s blocking pool
        /// has exactly one slot (`max_blocking_threads(1)`), so the second
        /// `block_in_place` call here cannot get a replacement thread until
        /// the first one's fallback finishes -- if that serialization were
        /// instead a deadlock (e.g. two block_in_place calls each waiting
        /// on the other), this test would hang rather than complete. Both
        /// calls are independent (neither depends on the other's
        /// progress), so correct behavior is serialization, not deadlock.
        #[tokio::test]
        async fn check_reachability_sync_serializes_rather_than_deadlocks_under_concurrent_cache_misses()
         {
            let (authorizer, immutable, mutable, execution) =
                authorizer_with(vec![grant("engineering", &["**"], &["read"])]).await;
            let repository_a: RepositoryId = Context::from([7u8; 16]).into();
            let repository_b: RepositoryId = Context::from([8u8; 16]).into();
            LORE_CONTEXT
                .scope(execution, async {
                    seed_repository(immutable.clone(), mutable.clone(), repository_a, "a").await;
                    seed_repository(immutable, mutable, repository_b, "b").await;
                })
                .await;

            let reachability = Arc::new(ReachabilityAuthorizer {
                authorizer: Arc::new(authorizer),
                legacy_resource_claim: false,
            });
            let claims = token_in_groups(&["engineering"]);

            let task_a = {
                let reachability = reachability.clone();
                let claims = claims.clone();
                lore_spawn_net!(async move {
                    reachability.check_reachability_sync(&claims, repository_a)
                })
            };
            let task_b = lore_spawn_net!(async move {
                reachability.check_reachability_sync(&claims, repository_b)
            });

            let (result_a, result_b) = tokio::join!(task_a, task_b);
            result_a
                .expect("task a must not panic")
                .expect("repository_a resolution must succeed despite concurrent contention");
            result_b
                .expect("task b must not panic")
                .expect("repository_b resolution must succeed despite concurrent contention");
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

    /// Round 5 regression: `AuthClientAuthorizer`'s *only* way of denying
    /// `read`/`push` for a legacy deployment — the requested resource is
    /// simply absent from `CheckUserPermissionResponse.allowed_resource_permission`
    /// — must classify as `Code::PermissionDenied`, the same as every other
    /// genuine "no", not `Code::Internal`. Before this fix, `action_held`
    /// (in `resolve_baseline_actions`) could not tell this apart from an
    /// actual auth-service failure, so a legacy caller correctly denied
    /// `push` (say) got `MessageHandleError::InternalError` out of
    /// `Connect::handle_auth`/`AuthorizeStart` instead of a clean rejection
    /// — the one denial shape a legacy deployment actually produces was the
    /// one `action_held` could never recognize.
    mod interpret_check_user_permission_response_tests {
        use lore_proto::auth::ResourcePermission;

        use super::*;

        fn response(entries: &[(&str, &[&str])]) -> CheckUserPermissionResponse {
            CheckUserPermissionResponse {
                allowed_resource_permission: entries
                    .iter()
                    .map(|(resource_id, permission)| ResourcePermission {
                        resource_id: (*resource_id).to_string(),
                        permission: permission.iter().map(|p| p.to_string()).collect(),
                    })
                    .collect(),
                denied_resource_permission: vec![],
            }
        }

        /// The exact shape this whole regression is about: the resource
        /// simply isn't in the response at all. This is `AuthClientAuthorizer`'s
        /// only way of expressing "the caller cannot reach this repository",
        /// which is also its only way of denying `read`/`push` — so this
        /// must be a real, recognizable denial (`PermissionDenied`), not
        /// `Internal`.
        #[test]
        fn resource_absent_from_response_is_permission_denied_not_internal() {
            let resp = response(&[]);
            let err =
                interpret_check_user_permission_response(resp, "urc-target", Some(PUSH_ACTION))
                    .expect_err("resource not listed at all must be denied");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        /// Same, but the response lists *other* resources — still absent
        /// for the one actually requested.
        #[test]
        fn resource_absent_among_other_listed_resources_is_permission_denied() {
            let resp = response(&[("urc-other", &["read", "push"])]);
            let err =
                interpret_check_user_permission_response(resp, "urc-target", Some(READ_ACTION))
                    .expect_err("the requested resource is not among those listed");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        /// The pre-existing denial shape (resource present, permission
        /// string insufficient) is unaffected by this fix — still
        /// `PermissionDenied`.
        #[test]
        fn resource_present_without_the_action_is_permission_denied() {
            let resp = response(&[("urc-target", &["obliterate"])]);
            let err = interpret_check_user_permission_response(
                resp,
                "urc-target",
                Some("push-protected"),
            )
            .expect_err("listed but without the requested action");
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
        }

        /// The success path is unaffected: resource present and its
        /// permissions satisfy the action.
        #[test]
        fn resource_present_with_the_action_is_ok() {
            let resp = response(&[("urc-target", &["push"])]);
            interpret_check_user_permission_response(resp, "urc-target", Some(PUSH_ACTION))
                .expect("listed with the requested action");
        }

        /// `read`/`push` degrade to plain reachability for a legacy
        /// deployment (see `auth_client_resource_permission` above): being
        /// listed at all — any permission strings, even unrelated ones —
        /// is enough.
        #[test]
        fn resource_present_satisfies_read_and_push_regardless_of_permission_strings() {
            let resp = response(&[("urc-target", &["some-other-permission"])]);
            interpret_check_user_permission_response(resp.clone(), "urc-target", Some(READ_ACTION))
                .expect("read degrades to reachability for a legacy deployment");
            interpret_check_user_permission_response(resp, "urc-target", Some(PUSH_ACTION))
                .expect("push degrades to reachability for a legacy deployment");
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
                Err(other) => panic!("expected ResourceGrantsNotImplemented, got {other:?}"),
            }
        }

        /// Writes `contents` to a fresh scratch file and returns its path.
        /// Callers are responsible for cleanup (`std::fs::remove_file`).
        fn scratch_acl_config(contents: &str) -> std::path::PathBuf {
            let path = std::env::temp_dir().join(format!(
                "lore-repository-authorizer-test-{}-{}.toml",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::write(&path, contents).expect("scratch file should be writable");
            path
        }

        /// GOALS fork: `acl_config_path` selects `ConfiguredGrantsAuthorizer`,
        /// but only through the stores-aware entry point -- the plain,
        /// stores-less `repository_authorizer` (used here) can never supply
        /// the repository-metadata store that authorizer needs, so it must
        /// fail loudly rather than silently falling back to Tier 1. No real
        /// store is involved at all here, by design: this is exactly the
        /// case the stores-less entry point cannot handle.
        #[test]
        fn acl_config_path_without_stores_is_a_hard_error() {
            let path = scratch_acl_config(
                "[[grant]]\ngroup = \"g\"\nrepos = [\"*\"]\npermissions = [\"read\"]\n",
            );
            let settings = auth_settings(&format!(r#"acl_config_path = "{}""#, path.display()));

            let result = repository_authorizer(None, Some(&settings));
            let _ = std::fs::remove_file(&path);

            match result {
                Err(RepositoryAuthorizerError::AclConfigRequiresStores) => {}
                Err(other) => panic!("expected AclConfigRequiresStores, got {other:?}"),
                Ok(_) => panic!(
                    "acl_config_path must not silently construct without a store to resolve \
                     names against"
                ),
            }
        }

        /// A malformed (or missing) `acl_config_path` file is a hard
        /// construction error, not a silent fallback -- matching the design
        /// doc's "a malformed config file refuses server startup"
        /// requirement, made again here (see `validate_auth_config`'s
        /// earlier check) where the authorizer is actually built. Stores are
        /// supplied so this specifically isolates the parse failure, not
        /// `AclConfigRequiresStores` from the previous test.
        #[tokio::test]
        async fn acl_config_path_with_stores_but_missing_file_fails_to_construct() {
            let settings = auth_settings(r#"acl_config_path = "/nonexistent/does-not-exist.toml""#);
            let (immutable, mutable, _) = crate::store::test_store_create().await.expect("stores");

            match repository_authorizer_with_stores(
                None,
                Some(&settings),
                Some(immutable),
                Some(mutable),
            ) {
                Err(RepositoryAuthorizerError::AclConfig(_)) => {}
                Err(other) => panic!("expected AclConfig, got {other:?}"),
                Ok(_) => panic!("a missing ACL config file must not construct successfully"),
            }
        }

        /// The full happy path: `acl_config_path` set, stores supplied,
        /// valid file -- selects `ConfiguredGrantsAuthorizer`, whose grants
        /// then actually govern the outcome (as opposed to merely
        /// constructing without error, and unlike Tier 1/AllowAll, which
        /// would not require a matching group at all or would treat a
        /// missing token as allow, respectively).
        #[tokio::test(flavor = "multi_thread")]
        async fn acl_config_path_with_stores_selects_configured_grants_authorizer() {
            let (immutable, mutable, execution) =
                crate::store::test_store_create().await.expect("stores");
            let path = scratch_acl_config(
                "[[grant]]\ngroup = \"engineering\"\nrepos = [\"**\"]\npermissions = [\"read\"]\n",
            );
            let settings = auth_settings(&format!(
                "acl_config_path = \"{}\"\npermission_claim = \"groups\"",
                path.display()
            ));

            LORE_CONTEXT
                .scope(execution, async move {
                    seed_repository(immutable.clone(), mutable.clone(), repository(), "any-name")
                        .await;
                    let authorizer = repository_authorizer_with_stores(
                        None,
                        Some(&settings),
                        Some(immutable),
                        Some(mutable),
                    )
                    .expect("valid acl_config_path with stores should construct");
                    let _ = std::fs::remove_file(&path);

                    let claims = AuthorizationToken {
                        groups: Some(vec!["engineering".to_string()]),
                        ..Default::default()
                    };
                    let token = VerifiedToken::new("raw", &claims);
                    authorizer
                        .check_repository_access(Some(&token), repository(), Some(READ_ACTION))
                        .await
                        .expect(
                            "ConfiguredGrantsAuthorizer's own grants govern the outcome, not \
                             AllowAll/GlobalGrants",
                        );
                    let err = authorizer
                        .check_repository_access(None, repository(), None)
                        .await
                        .expect_err(
                            "ConfiguredGrantsAuthorizer treats a missing token as unauthenticated, \
                             unlike AllowAllRepositoryAuthorizer",
                        );
                    assert_eq!(err.code(), tonic::Code::Unauthenticated);
                })
                .await;
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
