// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::time::Duration;

use lore_proto::lore::revision::v1::BranchCreateRequest;
use lore_proto::lore::revision::v1::BranchCreateResponse;
use lore_proto::lore::revision::v1::BranchDeleteRequest;
use lore_proto::lore::revision::v1::BranchDeleteResponse;
use lore_proto::lore::revision::v1::BranchGetRequest;
use lore_proto::lore::revision::v1::BranchGetResponse;
use lore_proto::lore::revision::v1::BranchListRequest;
use lore_proto::lore::revision::v1::forwarded_revision_service_server::ForwardedRevisionService;
use lore_revision::notification::NotificationSender;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use super::branch_create;
use super::branch_delete;
use super::branch_get;
use super::branch_list;
use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::revision::v1::branch_list::BranchListStream;
use crate::grpc::timeout_grpc;
use crate::hooks::HookDispatcher;

#[derive(Clone)]
struct ForwardedRevisionServiceInstrumentProvider;

impl InstrumentProvider for ForwardedRevisionServiceInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "lore.forwarded_revision.v1.service"
    }
}

/// Mirrors particular RPCs of `LoreRevisionV1Service`
#[derive(Clone)]
pub struct LoreForwardedRevisionV1Service {
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    notification: Arc<dyn NotificationSender>,
    hook_dispatcher: Arc<HookDispatcher>,
    instrument_provider: ForwardedRevisionServiceInstrumentProvider,
    rpc_timeout: Duration,
    /// Verifies the forwarded caller's raw token into real claims — this
    /// service is mounted on the internal mTLS server with no JWT
    /// interceptor of its own, so each handler must authenticate the
    /// `on-behalf-of-authorization` token itself before making an access
    /// decision (LEP 2026-08-20-oidc-oauth2-authentication, D8/D9). `None`
    /// when this deployment has no `[server.auth]` at all, matching
    /// `AllowAllRepositoryAuthorizer` everywhere else. Mirrors
    /// `LoreForwardedRepositoryV1Service::jwt_verifier` exactly.
    jwt_verifier: Option<JwtVerifier>,
    /// The same process-wide authorizer every other gRPC surface uses. Never
    /// built locally from `auth_url` alone — see
    /// `LoreForwardedRepositoryV1Service::repository_authorizer`.
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl LoreForwardedRevisionV1Service {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        notification: Arc<dyn NotificationSender>,
        hook_dispatcher: Arc<HookDispatcher>,
        rpc_timeout: Duration,
        jwt_verifier: Option<JwtVerifier>,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    ) -> Self {
        let instrument_provider = ForwardedRevisionServiceInstrumentProvider;
        Self {
            immutable_store,
            mutable_store,
            notification,
            hook_dispatcher,
            rpc_timeout,
            instrument_provider,
            jwt_verifier,
            repository_authorizer,
        }
    }
}

#[tonic::async_trait]
impl ForwardedRevisionService for LoreForwardedRevisionV1Service {
    async fn branch_create(
        &self,
        request: Request<BranchCreateRequest>,
    ) -> Result<Response<BranchCreateResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            branch_create::handler(
                request,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                self.notification.clone(),
                &self.hook_dispatcher,
                &self.instrument_provider,
                self.jwt_verifier.clone(),
                self.repository_authorizer.clone(),
            ),
        )
        .await
    }

    async fn branch_delete(
        &self,
        request: Request<BranchDeleteRequest>,
    ) -> Result<Response<BranchDeleteResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            branch_delete::handler(
                request,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                self.notification.clone(),
                &self.hook_dispatcher,
                &self.instrument_provider,
                self.jwt_verifier.clone(),
                self.repository_authorizer.clone(),
            ),
        )
        .await
    }

    async fn branch_get(
        &self,
        request: Request<BranchGetRequest>,
    ) -> Result<Response<BranchGetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            branch_get::handler(
                request,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                self.jwt_verifier.clone(),
                self.repository_authorizer.clone(),
            ),
        )
        .await
    }

    type BranchListStream = BranchListStream;

    async fn branch_list(
        &self,
        request: Request<BranchListRequest>,
    ) -> Result<Response<Self::BranchListStream>, Status> {
        branch_list::handler(
            request,
            self.immutable_store.clone(),
            self.mutable_store.clone(),
            self.jwt_verifier.clone(),
            self.repository_authorizer.clone(),
        )
        .await
    }
}
