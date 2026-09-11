// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::time::Duration;

use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryCreateResponse;
use lore_proto::lore::repository::v1::RepositoryGetRequest;
use lore_proto::lore::repository::v1::RepositoryGetResponse;
use lore_proto::lore::repository::v1::forwarded_repository_service_server::ForwardedRepositoryService;
use lore_revision::environment::EnvironmentConfig;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use super::repository_create;
use super::repository_get;
use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::timeout_grpc;
use crate::hooks::HookDispatcher;

#[derive(Clone)]
struct ForwardedRepositoryServiceInstrumentProvider;

impl InstrumentProvider for ForwardedRepositoryServiceInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "lore.forwarded_repository.v1.service"
    }
}

/// Mirrors particular RPCs of `LoreRepositoryV1Service`
#[derive(Clone)]
pub struct LoreForwardedRepositoryV1Service {
    environment: EnvironmentConfig,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hook_dispatcher: Arc<HookDispatcher>,
    instrument_provider: ForwardedRepositoryServiceInstrumentProvider,
    rpc_timeout: Duration,
    /// Verifies the forwarded caller's raw token into real claims for
    /// `repository_get` and `repository_create`, both of which have to make
    /// a real access decision on the receiving end (LEP
    /// 2026-08-20-oidc-oauth2-authentication, D8/D9). `None` when this
    /// deployment has no `[server.auth]` at all, matching
    /// `AllowAllRepositoryAuthorizer` everywhere else.
    jwt_verifier: Option<JwtVerifier>,
    /// The same process-wide authorizer every other gRPC surface uses. Never
    /// built locally from `auth_url` alone: doing so ignored `[server.auth]`
    /// entirely and could only ever select `AuthClientAuthorizer` or
    /// `AllowAllRepositoryAuthorizer`, never `GlobalGrantsAuthorizer`.
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl LoreForwardedRepositoryV1Service {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        environment: EnvironmentConfig,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        hook_dispatcher: Arc<HookDispatcher>,
        rpc_timeout: Duration,
        jwt_verifier: Option<JwtVerifier>,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    ) -> Self {
        let instrument_provider = ForwardedRepositoryServiceInstrumentProvider;
        Self {
            environment,
            immutable_store,
            mutable_store,
            hook_dispatcher,
            rpc_timeout,
            instrument_provider,
            jwt_verifier,
            repository_authorizer,
        }
    }

    /// See `RepositoryService::auth_url`'s doc comment (grpc/repository_service.rs) --
    /// an OIDC `auth_url` must not come back `Some` here either.
    fn auth_url(&self) -> Option<String> {
        self.environment
            .endpoint
            .as_ref()
            .and_then(|endpoint| endpoint.auth_url.clone())
            .filter(|auth_url| !auth_url.is_empty())
            .filter(|auth_url| crate::settings::is_legacy_auth_url(auth_url))
    }
}

#[tonic::async_trait]
impl ForwardedRepositoryService for LoreForwardedRepositoryV1Service {
    async fn repository_create(
        &self,
        request: Request<RepositoryCreateRequest>,
    ) -> Result<Response<RepositoryCreateResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_create::handler(
                request,
                self.auth_url(),
                self.jwt_verifier.clone(),
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                &self.hook_dispatcher,
                &self.instrument_provider,
            ),
        )
        .await
    }

    async fn repository_get(
        &self,
        request: Request<RepositoryGetRequest>,
    ) -> Result<Response<RepositoryGetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_get::handler(
                request,
                self.jwt_verifier.clone(),
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }
}
