// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use lore_revision::lore::RepositoryId;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use lore_telemetry::tracing::fields::CONNECTION_ID;
use lore_telemetry::tracing::fields::CORRELATION_ID;
use lore_telemetry::tracing::fields::PROTOCOL;
use lore_telemetry::tracing::fields::QUIC_OPCODE;
use lore_telemetry::tracing::fields::REPOSITORY_ID;
use lore_telemetry::tracing::fields::SAMPLING_TIER_LOW;
use lore_telemetry::tracing::fields::TRANSPORT;
use lore_telemetry::tracing::fields::USER_AGENT;
use lore_telemetry::tracing::fields::USER_ID;
use lore_transport::quic::QuicOpCode;
use lore_transport::quic::QuicServiceError;
use lore_transport::quic::UnknownCommand;
use lore_transport::quic::command_header::CommandHeader;
use lore_transport::quic::storage_service::Command;
use lore_transport::quic::storage_service::MAX_CHUNK_SIZE;
use lore_transport::quic::storage_service::command_name;
use tracing::Span;
use tracing::debug;
use tracing::info_span;

use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
use crate::correlation::CorrelationId;
use crate::protocol::attribute_map::AttributeMap;
use crate::protocol::attribute_map::ConnectionId;
use crate::protocol::client_identify::UserAgentValue;
use crate::protocol::storage::messages::ConnectionAuthorization;
use crate::protocol::storage::messages::LoreResponse;
use crate::protocol::storage::messages::Message;
use crate::protocol::storage::messages::MessageHandleError;
use crate::protocol::storage::messages::MessageParseError;
use crate::protocol::storage::messages::Response;
use crate::protocol::storage::requests;
use crate::quic::NO_CONNECTION_ID;
use crate::quic::NO_CORRELATION_ID;
use crate::quic::NO_REPOSITORY_ID;
use crate::quic::NO_USER_ID;
use crate::quic::ProtocolErrorInfo;
use crate::quic::QuicErrorStatus;
use crate::quic::QuicService;
use crate::telemetry::StorageProtocol;
use crate::telemetry::Transport;

/// Build the per-RPC `OTel` root span for an inbound storage opcode. Shared
/// between v0 (`LoreStorageService`) and v4 (`StorageServiceV4`); the caller
/// passes its `StorageProtocol` so v0 and v4 are distinguishable in traces via
/// the `protocol` attribute.
pub(crate) fn build_storage_protocol_request_span(
    cmd: QuicOpCode,
    protocol: StorageProtocol,
    connection_id: &str,
    repository_id: &str,
    correlation_id: &str,
    user_id: &str,
    user_agent: &str,
) -> Span {
    let command_parse = Command::try_from(cmd);
    let opcode_label = command_parse
        .as_ref()
        .map_or("", |command| command_name(command));
    match command_parse {
        Ok(Command::Authorize) => info_span!(
            parent: None,
            "StorageAuthorizeTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::Get) => info_span!(
            parent: None,
            "StorageGetTask",
            { SAMPLING_TIER_LOW } = true,
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::GetMetadata) => info_span!(
            parent: None,
            "StorageGetMetadataTask",
            { SAMPLING_TIER_LOW } = true,
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::Put) => info_span!(
            parent: None,
            "StoragePutTask",
            { SAMPLING_TIER_LOW } = true,
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::Query) => info_span!(
            parent: None,
            "StorageQueryTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::Verify) => info_span!(
            parent: None,
            "StorageVerifyTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::Copy) => info_span!(
            parent: None,
            "StorageCopyTask",
            { SAMPLING_TIER_LOW } = true,
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::MutableLoad) => info_span!(
            parent: None,
            "StorageMutableLoadTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::MutableStore) => info_span!(
            parent: None,
            "StorageMutableStoreTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::MutableCas) => info_span!(
            parent: None,
            "StorageMutableCompareAndSwapTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::PutResolved) => info_span!(
            parent: None,
            "StoragePutResolvedTask",
            { SAMPLING_TIER_LOW } = true,
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Ok(Command::GetResolved) => info_span!(
            parent: None,
            "StorageGetResolvedTask",
            { SAMPLING_TIER_LOW } = true,
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        // Carries no user agent itself: the value it announces is applied to the connection
        // context after this span is built, so it lands on subsequent requests instead.
        Ok(Command::ClientIdentify) => info_span!(
            parent: None,
            "StorageClientIdentifyTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
        Err(_) => info_span!(
            parent: None,
            "StorageUnknownTask",
            { TRANSPORT } = %Transport::Quic,
            { PROTOCOL } = %protocol,
            { QUIC_OPCODE } = opcode_label,
            { CONNECTION_ID } = connection_id,
            { REPOSITORY_ID } = repository_id,
            { CORRELATION_ID } = correlation_id,
            { USER_ID } = user_id,
            { USER_AGENT } = user_agent,
        ),
    }
}

fn request_identifiers_from_context(
    context: &Arc<AttributeMap>,
) -> (String, String, String, String, Option<Arc<UserAgentValue>>) {
    let (connection_id, repository_id, correlation_id, authorization_token, user_agent) = context
        .get_five::<ConnectionId, RepositoryId, CorrelationId, AuthorizationToken, UserAgentValue>(
    );
    let connection_id =
        connection_id.map_or_else(|| NO_CONNECTION_ID.to_string(), |id| id.0.to_string());
    let repository_id =
        repository_id.map_or_else(|| NO_REPOSITORY_ID.to_string(), |id| id.to_string());
    let correlation_id =
        correlation_id.map_or_else(|| NO_CORRELATION_ID.to_string(), |id| id.to_string());
    let user_id = authorization_token
        .map(|token| token.user_id.clone())
        .filter(|user_id| !user_id.is_empty())
        .unwrap_or_else(|| NO_USER_ID.to_string());
    (
        connection_id,
        repository_id,
        correlation_id,
        user_id,
        user_agent,
    )
}

#[derive(Debug)]
#[enum_dispatch(Message)]
pub enum ParsedStorageRequest {
    Connect(requests::Connect),
    Copy(requests::Copy),
    Get(requests::Get),
    /// Wire-identical to `Get`; the dispatcher routes this to `handle_get_metadata` so the
    /// response carries fragment metadata only — no payload bytes.
    GetMetadata(crate::protocol::storage::get::GetMetadata),
    /// Resolves a mutable key and returns the immutable blob it points at, saving the caller
    /// the round trip a separate `MutableLoad` would cost. v4-only: it needs both stores.
    GetResolved(requests::GetResolved),
    /// Stores a fragment and publishes a mutable key naming it, saving the caller the round trip
    /// a separate `MutableStore` would cost. v4-only: it needs both stores.
    PutResolved(requests::PutResolved),
    Put(requests::Put),
    Query(requests::Query),
    Correlate(requests::Correlate),
    Verify(requests::Verify),
    MutableLoad(requests::MutableLoad),
    MutableStoreOp(requests::MutableStoreOp),
    MutableCas(requests::MutableCas),
}

fn quic_error(message_error: &MessageHandleError) -> QuicServiceError {
    match message_error {
        MessageHandleError::AuthorizationFailure(_) | MessageHandleError::MissingToken => {
            QuicServiceError::NotAuthorized
        }
        MessageHandleError::FragmentNotFound | MessageHandleError::MutableDataNotFound(_) => {
            QuicServiceError::NotFound
        }
        MessageHandleError::SlowDown => QuicServiceError::SlowDown,
        MessageHandleError::Oversized => QuicServiceError::Oversized,
        _ => QuicServiceError::Failed,
    }
}

/// Legacy opcode for the Correlate command, which was removed from the client
/// Command enum in the lore-storage/0.4 protocol but must still be handled
/// server-side for backward compatibility with urc/0.2 clients.
const LEGACY_CORRELATE_OPCODE: QuicOpCode = 5;

pub fn parse_message_for_opcode(
    opcode: QuicOpCode,
    bytes: Bytes,
) -> Result<ParsedStorageRequest, MessageParseError> {
    debug!(
        "Attempting to parse {} bytes for opcode: {opcode}",
        bytes.len()
    );

    // Handle legacy Correlate opcode (removed from client Command enum in lore-storage/0.4)
    if opcode == LEGACY_CORRELATE_OPCODE {
        return Ok(ParsedStorageRequest::Correlate(requests::Correlate::parse(
            bytes,
        )?));
    }

    match opcode
        .try_into()
        .map_err(|_e| MessageParseError::UnknownOpcode(opcode))?
    {
        Command::Authorize => Ok(ParsedStorageRequest::Connect(requests::Connect::parse(
            bytes,
        )?)),
        Command::Get => Ok(ParsedStorageRequest::Get(requests::Get::parse(bytes)?)),
        Command::GetMetadata => Ok(ParsedStorageRequest::GetMetadata(
            crate::protocol::storage::get::GetMetadata::parse(bytes)?,
        )),
        Command::Put => Ok(ParsedStorageRequest::Put(requests::Put::parse(bytes)?)),
        Command::Query => Ok(ParsedStorageRequest::Query(requests::Query::parse(bytes)?)),
        Command::Verify => Ok(ParsedStorageRequest::Verify(requests::Verify::parse(
            bytes,
        )?)),
        Command::Copy => Ok(ParsedStorageRequest::Copy(requests::Copy::parse(bytes)?)),
        Command::MutableLoad => Ok(ParsedStorageRequest::MutableLoad(
            requests::MutableLoad::parse(bytes)?,
        )),
        Command::MutableStore => Ok(ParsedStorageRequest::MutableStoreOp(
            requests::MutableStoreOp::parse(bytes)?,
        )),
        Command::MutableCas => Ok(ParsedStorageRequest::MutableCas(
            requests::MutableCas::parse(bytes)?,
        )),
        Command::GetResolved => Ok(ParsedStorageRequest::GetResolved(
            requests::GetResolved::parse(bytes)?,
        )),
        Command::PutResolved => Ok(ParsedStorageRequest::PutResolved(
            requests::PutResolved::parse(bytes)?,
        )),
        // ClientIdentify is handled at the connection layer, not by this service.
        Command::ClientIdentify => Err(MessageParseError::UnknownOpcode(opcode)),
    }
}

/// `parse_message_for_opcode` variant used by the lore-storage/0.4 service. Identical to the
/// urc/0.2 parser except `Command::Copy` decodes the v4 wire (80 bytes, with `target_context`
/// on the tail) instead of the legacy 64-byte format.
pub fn parse_message_for_opcode_v4(
    opcode: QuicOpCode,
    bytes: Bytes,
) -> Result<ParsedStorageRequest, MessageParseError> {
    if let Ok(Command::Copy) = opcode.try_into() {
        return Ok(ParsedStorageRequest::Copy(requests::Copy::parse_v4(bytes)?));
    }
    parse_message_for_opcode(opcode, bytes)
}

pub fn message_handle_error_to_label(value: &MessageHandleError) -> &'static str {
    match value {
        MessageHandleError::AlreadyConnected => "AlreadyConnected",
        MessageHandleError::BranchExists => "BranchExists",
        MessageHandleError::BranchMismatch => "BranchMismatch",
        MessageHandleError::HashMismatch => "HashMismatch",
        MessageHandleError::FragmentNotFound => "FragmentNotFound",
        MessageHandleError::InvalidParentBranch => "InvalidParentBranch",
        MessageHandleError::InternalError => "InternalError",
        MessageHandleError::MutableDataNotFound(_) => "MutableDataNotFound",
        MessageHandleError::NoSuchBranch => "NoSuchBranch",
        MessageHandleError::NotConnected => "NotConnected",
        MessageHandleError::QueryResultSizeMismatch => "QueryResultSizeMismatch",
        MessageHandleError::StoreFailure => "StoreFailure",
        MessageHandleError::AuthorizationFailure(_) => "AuthorizationFailure",
        MessageHandleError::MissingToken => "MissingToken",
        MessageHandleError::BranchProtected => "BranchProtected",
        MessageHandleError::Metadata => "Metadata",
        MessageHandleError::NotImplemented => "NotImplemented",
        MessageHandleError::SlowDown => "SlowDown",
        MessageHandleError::Oversized => "Oversized",
        MessageHandleError::HashFailed => "HashFailed",
        MessageHandleError::InvalidFragment => "InvalidFragment",
        MessageHandleError::HandlerTimeout => "HandlerTimeout",
        MessageHandleError::SessionLimitReached => "SessionLimitReached",
    }
}

pub fn is_internal_error(error: &MessageHandleError) -> bool {
    match error {
        MessageHandleError::AuthorizationFailure(_)
        | MessageHandleError::AlreadyConnected
        | MessageHandleError::BranchExists
        | MessageHandleError::BranchMismatch
        | MessageHandleError::BranchProtected
        | MessageHandleError::FragmentNotFound
        | MessageHandleError::HashMismatch
        | MessageHandleError::InvalidParentBranch
        | MessageHandleError::MissingToken
        | MessageHandleError::MutableDataNotFound(_)
        | MessageHandleError::NoSuchBranch
        | MessageHandleError::NotConnected
        | MessageHandleError::Oversized
        | MessageHandleError::Metadata
        | MessageHandleError::HashFailed
        | MessageHandleError::InvalidFragment
        | MessageHandleError::SessionLimitReached => false,
        MessageHandleError::HandlerTimeout
        | MessageHandleError::InternalError
        | MessageHandleError::NotImplemented
        | MessageHandleError::QueryResultSizeMismatch
        | MessageHandleError::StoreFailure
        | MessageHandleError::SlowDown => true,
    }
}

pub struct StorageService {
    jwt_verifier: Arc<Option<JwtVerifier>>,
    reachability_authorizer: ReachabilityAuthorizer,
    immutable_store: Arc<dyn ImmutableStore>,
    local_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
}

impl StorageService {
    pub fn new(
        jwt_verifier: Arc<Option<JwtVerifier>>,
        reachability_authorizer: ReachabilityAuthorizer,
        immutable_store: Arc<dyn ImmutableStore>,
        local_store: Arc<dyn ImmutableStore>,
        mutable_store: Arc<dyn MutableStore>,
    ) -> Self {
        Self {
            jwt_verifier,
            reachability_authorizer,
            immutable_store,
            local_store,
            mutable_store,
        }
    }
}

#[async_trait]
impl QuicService for StorageService {
    type ParsedRequestType = ParsedStorageRequest;
    type RequestParseErrorType = MessageParseError;
    type RequestHandlerError = MessageHandleError;

    fn get_service_name_label(&self) -> &'static str {
        StorageProtocol::StorageV0.as_str()
    }

    fn parse_request_bytes(
        &self,
        header: &lore_transport::quic::command_header::CommandHeader,
        bytes: Bytes,
    ) -> Result<Self::ParsedRequestType, Self::RequestParseErrorType> {
        parse_message_for_opcode(header.cmd, bytes)
    }

    async fn run_request_handler(
        &self,
        context: Arc<AttributeMap>,
        request: Self::ParsedRequestType,
    ) -> Result<Vec<Bytes>, Self::RequestHandlerError> {
        // Baseline `read`/`push` gate, centralized here rather than
        // duplicated across each `Message::handle`/`handle_mutable`
        // implementation — the same approach `StorageServiceV4` takes
        // against its session-cached flags, applied here against the
        // connection-cached flags `Connect` establishes (see
        // `ConnectionAuthorization`). `Connect` itself is exempt: it is
        // what produces this state, checked separately inside
        // `handle_auth`. `Correlate` (stream-linking metadata, no
        // repository data) needs neither action.
        //
        // `Copy`'s own `push` (destination) and `read` (per-fragment
        // source, which may differ from this connection's repository) are
        // both layered on top of this, not replaced by it: `push` here is
        // what makes reaching `Copy::handle` possible at all, and its
        // separate source check inside `handle_copy`/`Copy::handle` still
        // runs afterward.
        if !matches!(
            request,
            ParsedStorageRequest::Connect(_) | ParsedStorageRequest::Correlate(_)
        ) {
            let (holds_read, holds_push) = match context.get::<ConnectionAuthorization>() {
                Some(auth) => {
                    // The connection's cached read/push below is trusted
                    // for as long as the connection lives, not re-checked
                    // against the authorizer per request (see
                    // `ConnectionAuthorization::Verified`'s doc comment) --
                    // but that must not outlive the JWT that produced it.
                    if auth.is_expired() {
                        return Err(MessageHandleError::AuthorizationFailure(
                            "connection's authorization token has expired".to_string(),
                        ));
                    }
                    match auth.as_ref() {
                        ConnectionAuthorization::Open => (true, true),
                        ConnectionAuthorization::Verified {
                            holds_read,
                            holds_push,
                            ..
                        } => (*holds_read, *holds_push),
                    }
                }
                // Always present on an established connection (`Connect`
                // inserts it unconditionally) — see `ConnectionAuthorization`'s
                // doc comment on why absence here is a wiring bug, not a
                // legitimate "no auth" state, and must fail closed rather
                // than be read as "everything permitted".
                None => return Err(MessageHandleError::MissingToken),
            };
            let required_action_held = match &request {
                ParsedStorageRequest::Get(_)
                | ParsedStorageRequest::GetMetadata(_)
                | ParsedStorageRequest::GetResolved(_)
                | ParsedStorageRequest::Query(_)
                | ParsedStorageRequest::Verify(_)
                | ParsedStorageRequest::MutableLoad(_) => holds_read,
                ParsedStorageRequest::Put(_)
                | ParsedStorageRequest::PutResolved(_)
                | ParsedStorageRequest::MutableStoreOp(_)
                | ParsedStorageRequest::MutableCas(_)
                | ParsedStorageRequest::Copy(_) => holds_push,
                ParsedStorageRequest::Connect(_) | ParsedStorageRequest::Correlate(_) => {
                    unreachable!("excluded by the outer match above")
                }
            };
            if !required_action_held {
                return Err(MessageHandleError::AuthorizationFailure(
                    "caller does not hold the required action".to_string(),
                ));
            }
        }

        let lore_response = match request {
            ParsedStorageRequest::Connect(request) => {
                request
                    .handle_auth(
                        context,
                        self.jwt_verifier.clone(),
                        self.reachability_authorizer.clone(),
                    )
                    .await
            }
            ParsedStorageRequest::MutableLoad(_)
            | ParsedStorageRequest::MutableStoreOp(_)
            | ParsedStorageRequest::MutableCas(_) => {
                request
                    .handle_mutable(context, self.mutable_store.clone())
                    .await
            }
            ParsedStorageRequest::Verify(verify) => {
                verify.handle(context, self.local_store.clone()).await
            }
            other => other.handle(context, self.immutable_store.clone()).await,
        }?;

        Ok(lore_response.data())
    }

    fn command_to_metrics_label(&self, opcode: QuicOpCode) -> &'static str {
        if opcode == LEGACY_CORRELATE_OPCODE {
            return "correlate";
        }
        let command: Result<Command, UnknownCommand> = opcode.try_into();
        match command {
            Ok(command) => command_name(&command),
            Err(_) => "unknown",
        }
    }

    fn transform_protocol_error(&self, error: &Self::RequestHandlerError) -> ProtocolErrorInfo {
        let service_error = quic_error(error);
        let is_appropriate_for_logging = !matches!(
            service_error,
            QuicServiceError::SlowDown | QuicServiceError::NotFound
        );

        ProtocolErrorInfo {
            response_error_code: service_error as QuicErrorStatus,
            message_handle_label: message_handle_error_to_label(error),
            is_internal_error: is_internal_error(error),
            is_appropriate_for_logging,
        }
    }

    fn max_chunk_size(&self) -> usize {
        MAX_CHUNK_SIZE
    }

    fn build_request_span(
        &self,
        header: &CommandHeader,
        _message: &Self::ParsedRequestType,
        context: &Arc<AttributeMap>,
    ) -> Span {
        let (connection_id, repository_id, correlation_id, user_id, user_agent) =
            request_identifiers_from_context(context);
        build_storage_protocol_request_span(
            header.cmd,
            StorageProtocol::StorageV0,
            &connection_id,
            &repository_id,
            &correlation_id,
            &user_id,
            user_agent
                .as_ref()
                .map_or(crate::quic::NO_USER_AGENT, |v| v.0.as_ref()),
        )
    }
}

#[cfg(test)]
mod tests {
    use lore_transport::quic::command_header::CommandHeader;
    use rand::random;

    use super::*;
    use crate::auth::jwt::AuthorizationToken;
    use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
    use crate::quic::QuicService;
    use crate::store::test_store_create;

    /// Exercises the fix for the gap where `Connect` checked plain
    /// reachability once and every subsequent request on the connection
    /// went unchecked: a per-request `read`/`push` gate, centralized in
    /// `run_request_handler`, against the flags `Connect` caches on
    /// `ConnectionAuthorization`.
    fn make_service(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
    ) -> StorageService {
        StorageService::new(
            Arc::new(None),
            ReachabilityAuthorizer::new(None, None).expect("no config never fails to construct"),
            immutable_store.clone(),
            immutable_store,
            mutable_store,
        )
    }

    fn context_with_authorization(
        repository: RepositoryId,
        holds_read: bool,
        holds_push: bool,
    ) -> Arc<AttributeMap> {
        let context = Arc::new(AttributeMap::default());
        context.insert(repository);
        context.insert(ConnectionAuthorization::Verified {
            // Not `AuthorizationToken::default()`'s `expires: 0`: these
            // tests are about the holds_read/holds_push gate, not expiry,
            // and a zero `exp` would trip the new expiry check first.
            token: Box::new(AuthorizationToken {
                expires: u64::MAX,
                ..Default::default()
            }),
            reachability_authorizer: ReachabilityAuthorizer::new(None, None)
                .expect("no config never fails to construct"),
            holds_read,
            holds_push,
        });
        context
    }

    #[tokio::test]
    async fn denies_read_command_when_connection_lacks_read() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);
        let context = context_with_authorization(random(), false, true);

        let header = CommandHeader {
            cmd: Command::Query as u8,
            ..CommandHeader::default()
        };
        let parsed = service
            .parse_request_bytes(&header, Bytes::new())
            .expect("an empty Query payload parses");

        let err = service
            .run_request_handler(context, parsed)
            .await
            .expect_err("a push-only connection must be denied a read command");
        assert!(matches!(err, MessageHandleError::AuthorizationFailure(_)));
    }

    /// The connection's cached `holds_read`/`holds_push` must not outlive
    /// the JWT that produced them: even a connection that holds both is
    /// denied once its token's `exp` has passed.
    #[tokio::test]
    async fn denies_command_once_the_connection_token_has_expired() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);
        let repository = random::<RepositoryId>();

        let context = Arc::new(AttributeMap::default());
        context.insert(repository);
        context.insert(ConnectionAuthorization::Verified {
            token: Box::new(AuthorizationToken {
                expires: 1,
                ..Default::default()
            }),
            reachability_authorizer: ReachabilityAuthorizer::new(None, None)
                .expect("no config never fails to construct"),
            holds_read: true,
            holds_push: true,
        });

        let header = CommandHeader {
            cmd: Command::Query as u8,
            ..CommandHeader::default()
        };
        let parsed = service
            .parse_request_bytes(&header, Bytes::new())
            .expect("an empty Query payload parses");

        let err = service
            .run_request_handler(context, parsed)
            .await
            .expect_err("an expired connection must be denied even holding read and push");
        assert!(matches!(err, MessageHandleError::AuthorizationFailure(_)));
    }

    #[tokio::test]
    async fn allows_read_command_when_connection_holds_read() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);
        let context = context_with_authorization(random(), true, false);

        let header = CommandHeader {
            cmd: Command::Query as u8,
            ..CommandHeader::default()
        };
        let parsed = service
            .parse_request_bytes(&header, Bytes::new())
            .expect("an empty Query payload parses");

        let result = service.run_request_handler(context, parsed).await;
        assert!(
            !matches!(result, Err(MessageHandleError::AuthorizationFailure(_))),
            "a read-holding connection must be allowed a read command, got {result:?}"
        );
    }

    #[tokio::test]
    async fn denies_push_command_when_connection_lacks_push() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);
        let context = context_with_authorization(random(), true, false);

        let header = CommandHeader {
            cmd: Command::MutableStore as u8,
            ..CommandHeader::default()
        };
        // key Hash (32 zero bytes) ++ value Hash (32 zero bytes) ++ KeyType::Untyped (0)
        let payload = Bytes::from(vec![0u8; 65]);
        let parsed = service
            .parse_request_bytes(&header, payload)
            .expect("a valid MutableStore payload parses");

        let err = service
            .run_request_handler(context, parsed)
            .await
            .expect_err("a read-only connection must be denied a push command");
        assert!(matches!(err, MessageHandleError::AuthorizationFailure(_)));
    }

    #[tokio::test]
    async fn allows_push_command_when_connection_holds_push() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);
        let context = context_with_authorization(random(), false, true);

        let header = CommandHeader {
            cmd: Command::MutableStore as u8,
            ..CommandHeader::default()
        };
        let payload = Bytes::from(vec![0u8; 65]);
        let parsed = service
            .parse_request_bytes(&header, payload)
            .expect("a valid MutableStore payload parses");

        let result = service.run_request_handler(context, parsed).await;
        assert!(
            !matches!(result, Err(MessageHandleError::AuthorizationFailure(_))),
            "a push-holding connection must be allowed a push command, got {result:?}"
        );
    }

    #[tokio::test]
    async fn denies_any_command_when_connection_authorization_missing() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);

        // No ConnectionAuthorization or RepositoryId inserted: a message
        // handled before Connect, or a future transport that forgets to
        // call it.
        let context = Arc::new(AttributeMap::default());

        let header = CommandHeader {
            cmd: Command::Query as u8,
            ..CommandHeader::default()
        };
        let parsed = service
            .parse_request_bytes(&header, Bytes::new())
            .expect("an empty Query payload parses");

        let err = service
            .run_request_handler(context, parsed)
            .await
            .expect_err("a connection with no established authorization must be denied");
        assert!(matches!(err, MessageHandleError::MissingToken));
    }
}
