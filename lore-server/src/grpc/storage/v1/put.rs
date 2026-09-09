// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::RepositoryId;
use lore_proto::lore::storage::v1 as storage_v1;
use lore_telemetry::InstrumentProvider;
use lore_telemetry::create_operation_context_attribute;
use lore_telemetry::tracing::fields::CORRELATION_ID;
use lore_telemetry::tracing::fields::PROTOCOL;
use lore_telemetry::tracing::fields::REPOSITORY_ID;
use lore_telemetry::tracing::fields::SAMPLING_TIER_LOW;
use lore_telemetry::tracing::fields::TRANSPORT;
use lore_telemetry::tracing::fields::USER_ID;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::Instrument;
use tracing::debug;
use tracing::info_span;

use super::log_and_code;
use super::record_latency;
use crate::authnz::repository_authorizer::PUSH_ACTION;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_repository;
use crate::grpc::get_user_id;
use crate::grpc::interpret_streaming_error;
use crate::grpc::map_message_handle_error_to_status;
use crate::protocol::storage::messages::LoreResponse;
use crate::protocol::storage::put::UnvalidatedPut;
use crate::protocol::storage::put::handle_put;
use crate::telemetry::StorageProtocol;
use crate::telemetry::Transport;
use crate::util::setup_execution;

pub type PutResponseStream =
    Pin<Box<dyn Stream<Item = Result<storage_v1::PutResponse, Status>> + Send>>;

const METRICS_STREAMING_MESSAGE_HANDLER_LATENCY: &str = "stream.message.handler.duration";

/// `Err` covers the two stream-fatal cases: a request that won't decode, and one that decodes
/// but carries no address. Both leave the failure unattributable — the client demultiplexes
/// responses by address. Everything past that point belongs to a known address and rides back
/// in-band, so one rejected fragment can't sink the rest of the batch.
async fn put_item(
    request: Result<storage_v1::PutRequest, Status>,
    repository: RepositoryId,
    correlation_id: String,
    user_id: String,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
) -> Result<storage_v1::PutResponse, Status> {
    let request = request.map_err(interpret_streaming_error)?;
    let address: Address = request
        .address
        .ok_or_else(|| Status::invalid_argument("PutRequest.address is required"))?
        .into();

    let outcome = match request.fragment {
        None => Err(Status::invalid_argument("PutRequest.fragment is required")),
        Some(fragment) => {
            let unvalidated = UnvalidatedPut {
                address,
                fragment: fragment.into(),
                payload: request.payload,
            };
            match unvalidated.validate() {
                Err(_) => Err(Status::invalid_argument("Payload failed validation")),
                Ok(put) => {
                    match handle_put(&put, repository, correlation_id, user_id, immutable_store)
                        .await
                    {
                        Ok(LoreResponse::Put(_)) => Ok(()),
                        Ok(_) => Err(Status::internal(
                            "Put handler returned the wrong response type",
                        )),
                        Err(err) => Err(map_message_handle_error_to_status(
                            &err,
                            Some(format!("Error storing fragment {address}: {err}")),
                            None,
                        )),
                    }
                }
            }
        }
    };

    Ok(storage_v1::PutResponse {
        address: Some(address.into()),
        status: Some(match outcome {
            Ok(()) => lore_proto::lore::model::v1::ItemStatus::ok(),
            Err(ref status) => status.into(),
        }),
    })
}

#[tracing::instrument(name = "StorageServiceV1::Put", skip_all)]
pub async fn handler(
    request: Request<Streaming<storage_v1::PutRequest>>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<PutResponseStream>, Status> {
    let repository = get_repository(request.metadata())?;
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();

    crate::grpc::check_repository_action(
        &request,
        repository_authorizer.as_ref(),
        repository,
        Some(PUSH_ACTION),
    )
    .await?;

    let mut stream = request.into_inner();

    let (tx, rx) = mpsc::channel(super::STREAM_PROCESS_LIMIT);

    let execution = setup_execution(module_path!(), correlation_id.clone(), user_id.clone());

    let histogram = Arc::new(
        instrument_provider.latency_histogram_ms(METRICS_STREAMING_MESSAGE_HANDLER_LATENCY),
    );

    LORE_CONTEXT
        .scope(execution, async move {
            lore_spawn!(async move {
                let task_limiter = Arc::new(Semaphore::new(super::STREAM_PROCESS_LIMIT));
                while let Some(req) = stream.next().await {
                    let permit = match Arc::clone(&task_limiter).acquire_owned().await {
                        Ok(p) => p,
                        Err(error) => {
                            debug!(?error, "Error acquiring put task permit");
                            break;
                        }
                    };

                    let immutable_store = immutable_store.clone();
                    let tx = tx.clone();
                    let correlation_id = correlation_id.clone();
                    let user_id = user_id.clone();
                    let histogram = histogram.clone();

                    let fragment_span = info_span!(
                        parent: None,
                        "StoragePutItemTask",
                        { SAMPLING_TIER_LOW } = true,
                        { TRANSPORT } = %Transport::Grpc,
                        { PROTOCOL } = %StorageProtocol::StorageV1,
                        { REPOSITORY_ID } = %repository,
                        { CORRELATION_ID } = correlation_id,
                        { USER_ID } = user_id,
                    );

                    lore_spawn!(
                        async move {
                            let start = Instant::now();
                            let metric_context = create_operation_context_attribute("put");

                            let outcome =
                                put_item(req, repository, correlation_id, user_id, immutable_store)
                                    .await;

                            let code = log_and_code(&outcome);
                            record_latency(&histogram, start, code, metric_context);

                            if let Err(err) = tx.send(outcome).await {
                                debug!(err = ?err, "Error sending put response");
                            }
                            drop(permit);
                        }
                        .instrument(fragment_span)
                    );
                }
            });
        })
        .await;

    let recv_stream = ReceiverStream::from(rx);
    Ok(Response::new(Box::pin(recv_stream) as PutResponseStream))
}

#[cfg(test)]
mod tests {
    use lore_base::lore_spawn;
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::Context;
    use rand::random;
    use tonic::Code;

    use super::*;
    use crate::auth::jwt::AuthorizationToken;
    use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;
    use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
    use crate::grpc::storage::v1::test_utils::make_empty_put_stream_request;
    use crate::grpc::storage_service::LoreStorageService;
    use crate::store::test_store_create;

    /// `groups` is an ordinary named `AuthorizationToken` field (the Dex convention), so a
    /// `GlobalGrantsAuthorizer` configured with `permission_claim = "groups"` reads it directly.
    fn groups_authorizer() -> Arc<dyn RepositoryAuthorizer> {
        Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string())))
    }

    fn token_with_groups(groups: &'static [&'static str]) -> AuthorizationToken {
        AuthorizationToken {
            groups: Some(groups.iter().map(|g| g.to_string()).collect()),
            ..Default::default()
        }
    }

    /// The one-time authorization gate runs before the request stream is ever read, so an
    /// empty stream (no items) is enough to distinguish "denied outright" from "allowed to
    /// proceed" — no real fragment data is needed either way.
    ///
    /// Takes `&'static` rather than a plain borrow: `lore_spawn!` requires
    /// everything the spawned future captures to be `'static`, and every
    /// call site already passes a literal slice (eligible for `'static`
    /// promotion), so this costs call sites nothing.
    async fn put_with_groups(
        groups: &'static [&'static str],
    ) -> Result<Response<PutResponseStream>, Status> {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let repository = random::<Context>();
        let token = token_with_groups(groups);

        lore_spawn!(LORE_CONTEXT.scope(execution, async move {
            let service = LoreStorageService::new(
                immutable_store.clone(),
                immutable_store.clone(),
                mutable_store,
                ReachabilityAuthorizer::new(None, None)
                    .expect("no config never fails to construct"),
            );

            let mut request = make_empty_put_stream_request(repository);
            request.extensions_mut().insert(token);

            handler(request, immutable_store, groups_authorizer(), &service).await
        }))
        .await
        .expect("test task failed")
    }

    #[tokio::test]
    async fn token_holding_only_push_can_put() {
        put_with_groups(&["push"])
            .await
            .expect("a token holding push must be allowed to put");
    }

    #[tokio::test]
    async fn token_holding_only_read_is_denied_on_put() {
        // `Response<PutResponseStream>` doesn't implement `Debug` (it boxes a trait object
        // stream), so `expect_err` — which requires the `Ok` side to be `Debug` — doesn't apply
        // here; match explicitly instead.
        let err = match put_with_groups(&["read"]).await {
            Ok(_) => panic!("a token holding only read must not be allowed to put"),
            Err(err) => err,
        };
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    #[tokio::test]
    async fn token_holding_neither_action_is_denied_on_put() {
        let err = match put_with_groups(&[]).await {
            Ok(_) => panic!("a token holding neither action must not be allowed to put"),
            Err(err) => err,
        };
        assert_eq!(err.code(), Code::PermissionDenied);
    }
}
