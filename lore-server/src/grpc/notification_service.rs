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
use tokio::sync::watch;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::instrument;

use crate::authnz::repository_authorizer::Action;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::authorization_timeout_status;
use crate::grpc::get_user_id;
use crate::grpc::get_verified_token;
use crate::grpc::no_repository_access_status;

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
    /// Becomes `true` when the server stops accepting connections; open subscriptions then end.
    closing: Option<watch::Receiver<bool>>,
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
            closing: None,
        }
    }

    /// End every subscription with `UNAVAILABLE` once `closing` becomes `true`, so that a
    /// subscriber reconnects to another server instead of holding this one open until it exits.
    #[must_use]
    pub fn closing_on(mut self, closing: watch::Receiver<bool>) -> Self {
        self.closing = Some(closing);
        self
    }
}

/// Ends `events` with `UNAVAILABLE` once `closing` becomes `true`.
fn end_on_close(
    events: impl Stream<Item = Result<lore_proto::lore::notification::Event, Status>> + Send + 'static,
    mut closing: watch::Receiver<bool>,
) -> impl Stream<Item = Result<lore_proto::lore::notification::Event, Status>> + Send + 'static {
    async_stream::stream! {
        tokio::pin!(events);
        loop {
            tokio::select! {
                biased;
                Ok(()) = async { closing.wait_for(|&closing| closing).await.map(drop) } => {
                    yield Err(Status::unavailable("server is shutting down"));
                    break;
                }
                event = events.next() => match event {
                    Some(event) => yield event,
                    None => break,
                },
            }
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
        let permitted = tokio::time::timeout(self.authorization_timeout, async {
            self.authorizer
                .check_repository_access(
                    get_verified_token(&extensions).as_ref(),
                    repository,
                    Action::Read,
                )
                .await
                .is_ok()
        })
        .await
        .map_err(|_elapsed| authorization_timeout_status())?;
        if !permitted {
            return Err(no_repository_access_status());
        }

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

        let stream: Self::SubscribeStream = match self.closing.clone() {
            Some(closing) => Box::pin(end_on_close(stream, closing)),
            None => Box::pin(stream),
        };
        Ok(Response::new(stream))
    }

    async fn publish(&self, _request: Request<PublishRequest>) -> Result<Response<()>, Status> {
        Err(Status::permission_denied(
            "Publish is not supported by the local notification service",
        ))
    }
}
