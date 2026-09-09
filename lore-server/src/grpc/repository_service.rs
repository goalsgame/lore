// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::Duration;

use lore_revision::environment::EnvironmentConfig;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use super::handlers::repository_create;
use super::handlers::repository_delete;
use super::handlers::repository_list;
use super::handlers::repository_metadata_get;
use super::handlers::repository_metadata_set;
use super::handlers::repository_query;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::timeout_grpc;
use crate::hooks::HookDispatcher;
use crate::legacy::rpc::repository_service_server::RepositoryService;

#[derive(Clone)]
pub struct LoreRepositoryService {
    environment: EnvironmentConfig,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hook_dispatcher: Arc<HookDispatcher>,
    rpc_timeout: Duration,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl InstrumentProvider for LoreRepositoryService {
    fn namespace(&self) -> &'static str {
        "urc.repository_service"
    }
}

impl LoreRepositoryService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        environment: EnvironmentConfig,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        hook_dispatcher: Arc<HookDispatcher>,
        rpc_timeout: Duration,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    ) -> Self {
        Self {
            environment,
            immutable_store,
            mutable_store,
            hook_dispatcher,
            rpc_timeout,
            repository_authorizer,
        }
    }

    /// Auth-service URL extracted from the environment, if configured. Used
    /// only by the legacy paths (`repository_create`, `repository_delete`'s
    /// rebac-resource cleanup, `repository_list`) that predate
    /// `RepositoryAuthorizer` and still speak to the auth service directly.
    fn auth_url(&self) -> Option<String> {
        self.environment
            .endpoint
            .as_ref()
            .and_then(|endpoint| endpoint.auth_url.clone())
    }
}

#[tonic::async_trait]
impl RepositoryService for LoreRepositoryService {
    async fn repository_create(
        &self,
        request: Request<lore_proto::RepositoryCreateRequest>,
    ) -> Result<Response<lore_proto::RepositoryCreateResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_create::handler(
                request,
                self.auth_url(),
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                &self.hook_dispatcher,
                self,
            ),
        )
        .await
    }

    async fn repository_delete(
        &self,
        request: Request<lore_proto::RepositoryDeleteRequest>,
    ) -> Result<Response<lore_proto::RepositoryDeleteResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_delete::handler(
                request,
                self.auth_url(),
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                self,
            ),
        )
        .await
    }

    async fn repository_query(
        &self,
        request: Request<lore_proto::RepositoryQueryRequest>,
    ) -> Result<Response<lore_proto::RepositoryQueryResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_query::handler(
                request,
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }

    async fn repository_list(
        &self,
        request: Request<lore_proto::RepositoryListRequest>,
    ) -> Result<Response<lore_proto::RepositoryListResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_list::handler(
                request,
                self.auth_url(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }

    async fn repository_metadata_get(
        &self,
        request: Request<lore_proto::RepositoryMetadataGetRequest>,
    ) -> Result<Response<lore_proto::RepositoryMetadataGetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_metadata_get::handler(
                request,
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }

    async fn repository_metadata_set(
        &self,
        request: Request<lore_proto::RepositoryMetadataSetRequest>,
    ) -> Result<Response<lore_proto::RepositoryMetadataSetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_metadata_set::handler(
                request,
                self.repository_authorizer.clone(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }
}
