// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

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

use crate::authnz::repository_authorizer::IncarnationGuard;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::authnz::repository_authorizer::token_is_name_scoped;
use crate::grpc::access_failure_status;
use crate::grpc::authorization_timeout_status;
use crate::grpc::get_user_id;
use crate::grpc::get_verified_token;

#[derive(Clone)]
pub struct NotificationService {
    sender: Arc<crate::notification::local::NotificationSender>,
    authorizer: Arc<dyn RepositoryAuthorizer>,
    /// Bound on the subscribe authorization check alone, so a stalled online
    /// authorizer cannot park subscribers. This is the authorization budget
    /// every public gRPC access check answers to, not the longer
    /// request-handler budget: the check is one online call, and sizing it
    /// for a whole request is what lets a stalled authorizer hold a
    /// subscriber for the length of a request instead of the length of a
    /// permission question.
    authorization_timeout: Duration,
}

impl NotificationService {
    pub fn new(
        sender: Arc<crate::notification::local::NotificationSender>,
        authorizer: Arc<dyn RepositoryAuthorizer>,
        authorization_timeout: Duration,
    ) -> Self {
        Self {
            sender,
            authorizer,
            authorization_timeout,
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
        let (_, extensions, message) = request.into_parts();
        let user_id = get_user_id(&extensions);
        let repository: RepositoryId = Context::from(message.repository).into();

        if repository.is_zero() {
            return Err(Status::failed_precondition("invalid stream"));
        }

        // Checked against the partition in the request *body*. The
        // default authorization middleware validates access using the
        // request metadata fields. The services passing a repository
        // in the body need custom verification logic. A denial is folded
        // into the boolean inside the bound, so elapsing is the only other
        // thing the check reports.
        let verified = get_verified_token(&extensions);
        let incarnation = tokio::time::timeout(self.authorization_timeout, async {
            match verified
                .as_ref()
                .filter(|token| token_is_name_scoped(token))
            {
                Some(token) => self
                    .authorizer
                    .granted_access_tracked(Some(token), repository)
                    .await
                    .map(|regranted| regranted.incarnation),
                None => self
                    .authorizer
                    .check_repository_access(verified.as_ref(), repository, None)
                    .await
                    .map_err(access_failure_status)
                    .map(|()| None),
            }
        })
        .await
        .map_err(|_elapsed| authorization_timeout_status())??;
        let guard = IncarnationGuard::new(
            self.authorizer.clone(),
            verified.as_ref(),
            repository,
            incarnation,
        );

        let rx = self.sender.register(repository);

        debug!(
            { REPOSITORY_ID } = %repository,
            { USER_ID } = user_id,
            "User subscribed to notifications"
        );

        let stream = futures::stream::unfold(
            (BroadcastStream::new(rx), guard, false),
            |(mut events, mut guard, ended)| async move {
                if ended {
                    return None;
                }
                loop {
                    match events.next().await? {
                        Ok(event) => {
                            if let Some(guard) = guard.as_mut()
                                && let Err(status) = guard.check().await
                            {
                                return Some((Err(status), (events, None, true)));
                            }
                            return Some((Ok(event), (events, guard, false)));
                        }
                        // Ignore if client is lagging behind, just drop the event
                        Err(BroadcastStreamRecvError::Lagged(_)) => {}
                    }
                }
            },
        );

        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }

    async fn publish(&self, _request: Request<PublishRequest>) -> Result<Response<()>, Status> {
        Err(Status::permission_denied(
            "Publish is not supported by the local notification service",
        ))
    }
}
