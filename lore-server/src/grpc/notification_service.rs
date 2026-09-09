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

use crate::authnz::repository_authorizer::PUSH_ACTION;
use crate::authnz::repository_authorizer::READ_ACTION;
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
            .check_repository_access(verified_token.as_ref(), repository, Some(READ_ACTION))
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

    #[instrument(name = "NotificationService::Publish", skip_all)]
    async fn publish(&self, request: Request<PublishRequest>) -> Result<Response<()>, Status> {
        // Same field shape as `subscribe`: the interceptor authorizes
        // `get_repository(request.metadata())`, but the partition this RPC
        // acts on comes from the request *body* (the event's own
        // `repository` field), so the check belongs here instead. The
        // service always denies below regardless of the outcome — `Publish`
        // is not implemented yet — but the check is put in place now so it
        // is not silently forgotten once this stub grows a real
        // implementation.
        let token = get_authorization(request.extensions()).ok();
        let raw_token = crate::auth::jwt_interceptor::extract_bearer_token(request.metadata());
        let event = request
            .into_inner()
            .event
            .ok_or_else(|| Status::invalid_argument("PublishRequest.event must be set"))?;
        let repository: RepositoryId = Context::from(event.repository).into();

        if repository.is_zero() {
            return Err(Status::invalid_argument("invalid repository"));
        }

        let verified_token = token
            .as_ref()
            .map(|claims| VerifiedToken::new(raw_token.as_deref().unwrap_or_default(), claims));
        self.repository_authorizer
            .check_repository_access(verified_token.as_ref(), repository, Some(PUSH_ACTION))
            .await
            .map_err(|_err| no_repository_access_status())?;

        Err(Status::permission_denied(
            "Publish is not supported by the local notification service",
        ))
    }
}

#[cfg(test)]
mod tests {
    // Brings the trait's methods (`subscribe`, `publish`) into scope for
    // direct method-call syntax on `NotificationService` below. Aliased to
    // `_` because the trait and the struct under test share the same name
    // (`impl lore_notification::NotificationService for NotificationService`
    // above never imports the trait itself — it only names it by full path).
    use lore_notification::NotificationService as _;
    use lore_proto::lore::notification::BranchDeleted;
    use lore_proto::lore::notification::Event;
    use lore_proto::lore::notification::event;
    use rand::random;
    use tonic::Code;

    use super::*;
    use crate::auth::jwt::AuthorizationToken;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;

    /// `groups` is an ordinary named `AuthorizationToken` field (the Dex
    /// convention), so a `GlobalGrantsAuthorizer` configured with
    /// `permission_claim = "groups"` reads it directly, matching a real
    /// Tier 1 (FoxIDs/OIDC) token.
    fn groups_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string())))
    }

    fn service_with(repository_authorizer: Arc<dyn RepositoryAuthorizer>) -> NotificationService {
        NotificationService::new(
            Arc::new(crate::notification::local::NotificationSender::default()),
            repository_authorizer,
        )
    }

    fn token_with_groups(groups: Vec<String>) -> AuthorizationToken {
        AuthorizationToken {
            groups: Some(groups),
            ..Default::default()
        }
    }

    fn subscribe_request(
        repository: RepositoryId,
        token: Option<AuthorizationToken>,
    ) -> Request<lore_proto::lore::notification::SubscribeRequest> {
        let mut request = Request::new(lore_proto::lore::notification::SubscribeRequest {
            repository: repository.into(),
        });
        if let Some(token) = token {
            request.extensions_mut().insert(token);
        }
        request
    }

    fn publish_request(
        repository: RepositoryId,
        token: Option<AuthorizationToken>,
    ) -> Request<PublishRequest> {
        let mut request = Request::new(PublishRequest {
            event: Some(Event {
                id: "test-event".to_string(),
                time: None,
                repository: repository.into(),
                event: Some(event::Event::BranchDeleted(BranchDeleted {
                    branch: Vec::new().into(),
                })),
            }),
        });
        if let Some(token) = token {
            request.extensions_mut().insert(token);
        }
        request
    }

    /// A token holding `read` may subscribe.
    #[tokio::test]
    async fn subscribe_with_read_action_succeeds() {
        let repository = random::<RepositoryId>();
        let service = service_with(groups_authorizer());

        service
            .subscribe(subscribe_request(
                repository,
                Some(token_with_groups(vec!["read".to_string()])),
            ))
            .await
            .expect("token holding read must succeed");
    }

    /// An authenticated token that does not hold `read` is denied.
    #[tokio::test]
    async fn subscribe_without_read_action_is_denied() {
        let repository = random::<RepositoryId>();
        let service = service_with(groups_authorizer());

        // `SubscribeStream`'s `Ok` payload is not `Debug`, so `unwrap_err`
        // cannot be used directly (it requires the `Ok` side to be
        // `Debug`) — match explicitly instead.
        let err = match service
            .subscribe(subscribe_request(
                repository,
                Some(token_with_groups(vec!["push".to_string()])),
            ))
            .await
        {
            Ok(_) => panic!("token lacking read must be denied"),
            Err(err) => err,
        };
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    /// A missing/zeroed repository is rejected before authorization runs.
    #[tokio::test]
    async fn subscribe_with_zero_repository_is_failed_precondition() {
        let service = service_with(groups_authorizer());

        let err = match service
            .subscribe(subscribe_request(RepositoryId::default(), None))
            .await
        {
            Ok(_) => panic!("zeroed repository must be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    /// A token holding `push` passes the authorization check, then hits the
    /// unconditional "not supported" denial — proving the check runs and
    /// permits before the stub's own refusal takes over.
    #[tokio::test]
    async fn publish_with_push_action_reaches_the_unimplemented_denial() {
        let repository = random::<RepositoryId>();
        let service = service_with(groups_authorizer());

        let err = service
            .publish(publish_request(
                repository,
                Some(token_with_groups(vec!["push".to_string()])),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
        assert_eq!(
            err.message(),
            "Publish is not supported by the local notification service"
        );
    }

    /// An authenticated token that does not hold `push` is denied by the
    /// authorization check itself, before reaching the stub's own denial.
    #[tokio::test]
    async fn publish_without_push_action_is_denied_by_authorization() {
        let repository = random::<RepositoryId>();
        let service = service_with(groups_authorizer());

        let err = service
            .publish(publish_request(
                repository,
                Some(token_with_groups(vec!["read".to_string()])),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
        assert_eq!(err.message(), "Unauthorized");
    }

    /// A missing event is rejected before authorization runs.
    #[tokio::test]
    async fn publish_with_missing_event_is_invalid_argument() {
        let service = service_with(groups_authorizer());
        let request = Request::new(PublishRequest { event: None });

        let err = service.publish(request).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    /// A zeroed event repository is rejected before authorization runs.
    #[tokio::test]
    async fn publish_with_zero_repository_is_invalid_argument() {
        let service = service_with(groups_authorizer());

        let err = service
            .publish(publish_request(RepositoryId::default(), None))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
