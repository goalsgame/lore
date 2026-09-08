// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use lore_base::types::Context;
use lore_proto::lore::notification::PublishRequest;
use lore_revision::lore::RepositoryId;
use lore_telemetry::tracing::fields::REPOSITORY_ID;
use lore_telemetry::tracing::fields::USER_ID;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::instrument;

use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::grpc::get_authorization;
use crate::grpc::get_user_id;
use crate::grpc::no_repository_access_status;

#[derive(Clone)]
pub struct NotificationService {
    sender: Arc<crate::notification::local::NotificationSender>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl NotificationService {
    pub fn new(
        sender: Arc<crate::notification::local::NotificationSender>,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    ) -> Self {
        Self {
            sender,
            repository_authorizer,
        }
    }
}

type SubscribeResponseStream =
    Pin<Box<dyn Stream<Item = Result<lore_proto::lore::notification::Event, Status>> + Send>>;

#[async_trait]
impl lore_notification::NotificationService for NotificationService {
    type SubscribeStream = SubscribeResponseStream;

    #[instrument(name = "NotificationService::Subscribe", skip_all)]
    async fn subscribe(
        &self,
        request: Request<lore_proto::lore::notification::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let user_id = get_user_id(request.extensions());

        // The interceptor authorizes `get_repository(request.metadata())`, a
        // different field, so it cannot answer for this RPC: the partition
        // subscribed to comes from the request *body*
        // (LEP 2026-08-20-oidc-oauth2-authentication, D9). The check
        // belongs here instead.
        let token = get_authorization(request.extensions()).ok();
        let raw_token = crate::auth::jwt_interceptor::extract_bearer_token(request.metadata());
        let repository: RepositoryId = Context::from(request.into_inner().repository).into();

        if repository.is_zero() {
            return Err(Status::failed_precondition("invalid stream"));
        }

        let verified_token = token
            .as_ref()
            .map(|claims| VerifiedToken::new(raw_token.as_deref().unwrap_or_default(), claims));
        self.repository_authorizer
            .check_repository_access(verified_token.as_ref(), repository, None)
            .await
            .map_err(|_err| no_repository_access_status())?;

        let rx = self.sender.register(repository);

        debug!(
            { REPOSITORY_ID } = %repository,
            { USER_ID } = user_id,
            "User subscribed to notifications"
        );

        let stream = BroadcastStream::new(rx).filter_map(|res| {
            match res {
                Ok(item) => Some(Ok(item)),
                // Ignore if client is lagging behind, just drop the event
                Err(BroadcastStreamRecvError::Lagged(_)) => None,
            }
        });

        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }

    async fn publish(&self, _request: Request<PublishRequest>) -> Result<Response<()>, Status> {
        Err(Status::permission_denied(
            "Publish is not supported by the local notification service",
        ))
    }
}
