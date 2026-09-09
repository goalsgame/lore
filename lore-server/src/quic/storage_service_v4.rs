// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use lore_telemetry::user_agent_filter::UserAgentFilter;
use lore_transport::quic::QuicOpCode;
use lore_transport::quic::QuicServiceError;
use lore_transport::quic::UnknownCommand;
use lore_transport::quic::command_header::COMMAND_HEADER_SIZE_V4;
use lore_transport::quic::command_header::CommandHeader;
use lore_transport::quic::storage_service::Command;
use lore_transport::quic::storage_service::MAX_CHUNK_SIZE;
use lore_transport::quic::storage_service::command_name;
use tracing::Span;
use tracing::debug;

use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::authnz::repository_authorizer::VerifiedToken;
use crate::authnz::repository_authorizer::resolve_baseline_actions;
use crate::protocol::attribute_map::AttributeMap;
use crate::protocol::attribute_map::ConnectionId;
use crate::protocol::client_identify::ClientIdentify;
use crate::protocol::storage::authorize::AuthorizeAction;
use crate::protocol::storage::authorize::parse_authorize;
use crate::protocol::storage::copy::handle_copy;
use crate::protocol::storage::get::handle_get;
use crate::protocol::storage::messages::MessageHandleError;
use crate::protocol::storage::messages::MessageParseError;
use crate::protocol::storage::messages::Response;
use crate::protocol::storage::mutable_cas::handle_mutable_cas;
use crate::protocol::storage::mutable_load::handle_mutable_load;
use crate::protocol::storage::mutable_store_handler::handle_mutable_store;
use crate::protocol::storage::put::handle_put;
use crate::protocol::storage::query::handle_query;
use crate::protocol::storage::session::SessionError;
use crate::protocol::storage::session::SessionMap;
use crate::protocol::storage::verify::handle_verify;
use crate::quic::NO_CONNECTION_ID;
use crate::quic::NO_CORRELATION_ID;
use crate::quic::NO_REPOSITORY_ID;
use crate::quic::NO_USER_ID;
use crate::quic::ProtocolErrorInfo;
use crate::quic::QuicErrorStatus;
use crate::quic::QuicService;
use crate::quic::storage_service::build_storage_protocol_request_span;
use crate::quic::storage_service::is_internal_error;
use crate::quic::storage_service::message_handle_error_to_label;
use crate::quic::storage_service::parse_message_for_opcode_v4;
use crate::telemetry::StorageProtocol;

const RESERVED_OPCODE_PING: QuicOpCode = 4;
const RESERVED_OPCODE_CORRELATE: QuicOpCode = 5;

#[derive(Debug)]
pub enum ParsedStorageRequestV4 {
    AuthorizeStart {
        repository: lore_revision::lore::RepositoryId,
        correlation_id: String,
        auth_token: Vec<u8>,
    },
    AuthorizeStop {
        session_id: u32,
    },
    StorageCommand {
        session_id: u32,
        opcode: QuicOpCode,
        payload: Bytes,
    },
    ClientIdentify(ClientIdentify),
}

fn quic_error_v4(error: &MessageHandleError) -> QuicServiceError {
    match error {
        MessageHandleError::AuthorizationFailure(_) | MessageHandleError::MissingToken => {
            QuicServiceError::NotAuthorized
        }
        MessageHandleError::FragmentNotFound | MessageHandleError::MutableDataNotFound(_) => {
            QuicServiceError::NotFound
        }
        MessageHandleError::SlowDown | MessageHandleError::SessionLimitReached => {
            QuicServiceError::SlowDown
        }
        MessageHandleError::Oversized => QuicServiceError::Oversized,
        _ => QuicServiceError::Failed,
    }
}

pub struct StorageServiceV4 {
    jwt_verifier: Arc<Option<JwtVerifier>>,
    /// Answers the session-start `read`/`push` questions directly (LEP
    /// 2026-08-20-oidc-oauth2-authentication, D9): "once per session rather
    /// than per operation", so — unlike the gRPC interceptor's per-request
    /// check — going through the configured authorizer unconditionally,
    /// `AuthClientAuthorizer`'s online check included, costs no more than
    /// two checks per connection (one for each baseline action), which is
    /// the granularity this was built for. The two booleans this resolves
    /// are cached on the `SessionEntry` (see `SessionMap::start`) and
    /// consulted per `StorageCommand` from then on, never re-asked of this
    /// authorizer for the life of the session.
    repository_authorizer: Arc<dyn RepositoryAuthorizer>,
    immutable_store: Arc<dyn ImmutableStore>,
    local_store: Arc<dyn ImmutableStore>,
    mutable_store: Arc<dyn MutableStore>,
    session_map: Arc<SessionMap>,
    user_agent_filter: Arc<UserAgentFilter>,
}

impl StorageServiceV4 {
    pub fn new(
        jwt_verifier: Arc<Option<JwtVerifier>>,
        repository_authorizer: Arc<dyn RepositoryAuthorizer>,
        immutable_store: Arc<dyn ImmutableStore>,
        local_store: Arc<dyn ImmutableStore>,
        mutable_store: Arc<dyn MutableStore>,
        user_agent_filter: Arc<UserAgentFilter>,
    ) -> Self {
        Self {
            jwt_verifier,
            repository_authorizer,
            immutable_store,
            local_store,
            mutable_store,
            session_map: Arc::new(SessionMap::default()),
            user_agent_filter,
        }
    }
}

#[async_trait]
impl QuicService for StorageServiceV4 {
    type ParsedRequestType = ParsedStorageRequestV4;
    type RequestParseErrorType = MessageParseError;
    type RequestHandlerError = MessageHandleError;

    fn get_service_name_label(&self) -> &'static str {
        StorageProtocol::StorageV4.as_str()
    }

    fn parse_request_bytes(
        &self,
        header: &CommandHeader,
        bytes: Bytes,
    ) -> Result<Self::ParsedRequestType, Self::RequestParseErrorType> {
        let opcode = header.cmd;
        let session_id = header.session_id;

        if opcode == RESERVED_OPCODE_PING || opcode == RESERVED_OPCODE_CORRELATE {
            return Err(MessageParseError::UnknownOpcode(opcode));
        }

        if opcode == Command::ClientIdentify as u8 {
            return Ok(ParsedStorageRequestV4::ClientIdentify(
                ClientIdentify::parse(bytes, false)?,
            ));
        }

        if opcode == Command::Authorize as u8 {
            let action = parse_authorize(session_id, bytes)?;
            return match action {
                AuthorizeAction::Start(start) => Ok(ParsedStorageRequestV4::AuthorizeStart {
                    repository: start.repository,
                    correlation_id: start.correlation_id,
                    auth_token: start.auth_token,
                }),
                AuthorizeAction::Stop(stop) => Ok(ParsedStorageRequestV4::AuthorizeStop {
                    session_id: stop.session_id,
                }),
            };
        }

        // Validate this is a known storage opcode (but don't parse yet — we need session context)
        let _command: Command = opcode
            .try_into()
            .map_err(|_err| MessageParseError::UnknownOpcode(opcode))?;

        Ok(ParsedStorageRequestV4::StorageCommand {
            session_id,
            opcode,
            payload: bytes,
        })
    }

    async fn run_request_handler(
        &self,
        context: Arc<AttributeMap>,
        request: Self::ParsedRequestType,
    ) -> Result<Vec<Bytes>, Self::RequestHandlerError> {
        match request {
            ParsedStorageRequestV4::ClientIdentify(msg) => {
                msg.apply(&context, &self.user_agent_filter);
                Ok(vec![])
            }
            ParsedStorageRequestV4::AuthorizeStart {
                repository,
                correlation_id,
                auth_token,
            } => {
                let mut user_id = String::new();
                // No `[server.auth]` configured at all: matches
                // `AllowAllRepositoryAuthorizer`'s semantics everywhere
                // else — every session holds both baseline actions.
                let mut holds_read = true;
                let mut holds_push = true;

                if let Some(jwt_verifier) = self.jwt_verifier.as_ref() {
                    let token_str = String::from_utf8(auth_token).map_err(|err| {
                        MessageHandleError::AuthorizationFailure(format!(
                            "invalid token encoding: {err}"
                        ))
                    })?;

                    if token_str.is_empty() {
                        return Err(MessageHandleError::MissingToken);
                    }

                    let authorization = jwt_verifier
                        .verify_token(&token_str)
                        .await
                        .map_err(|err| MessageHandleError::AuthorizationFailure(err.to_string()))?;

                    let verified_token = VerifiedToken::new(&token_str, &authorization);
                    // Resolves and caches `read`/`push` once here, at
                    // session-authorize time — matching the "once per
                    // session rather than per operation" principle this
                    // service already used for plain reachability (see
                    // `Self::repository_authorizer`'s doc comment) — rather
                    // than asking the authorizer again for every
                    // `StorageCommand` this session goes on to issue.
                    // `read`/`push` together subsume the old, plain
                    // reachability question: a legacy `AuthClientAuthorizer`
                    // deployment answers both from the same "listed among
                    // the allowed resources at all" check reachability
                    // used (see `RepositoryAuthorizer`'s doc comment on
                    // `read`/`push` having no legacy equivalent), so a
                    // caller who is reachable at all under a legacy
                    // deployment holds both; a caller who holds neither was
                    // never reachable either way.
                    // Two independent calls against the configured
                    // authorizer, each of which can fail on its own (most
                    // notably `AuthClientAuthorizer`'s online call timing
                    // out or erroring) rather than answering "denied" —
                    // `resolve_baseline_actions` keeps that distinguishable
                    // rather than folding it into "holds neither", which
                    // would otherwise get cached on the session for this
                    // session's entire lifetime.
                    (holds_read, holds_push) = resolve_baseline_actions(
                        self.repository_authorizer.as_ref(),
                        &verified_token,
                        repository,
                    )
                    .await
                    .map_err(|status| {
                        tracing::warn!("Failed to resolve read/push actions: {status}");
                        MessageHandleError::InternalError
                    })?;

                    if !holds_read && !holds_push {
                        return Err(MessageHandleError::AuthorizationFailure(
                            "caller holds neither read nor push".to_string(),
                        ));
                    }

                    user_id = crate::util::get_user_id_from_token(Some(authorization));
                }

                let session_map = self.session_map.clone();
                match session_map.start(repository, correlation_id, user_id, holds_read, holds_push)
                {
                    Ok((session_id, correlation_id)) => {
                        debug!(
                            session_id,
                            repository = %repository,
                            correlation_id,
                            holds_read,
                            holds_push,
                            "Authorized session"
                        );
                        let response_data = vec![Bytes::copy_from_slice(&session_id.to_le_bytes())];
                        Ok(response_data)
                    }
                    Err(SessionError::LimitReached) => Err(MessageHandleError::SessionLimitReached),
                    Err(SessionError::CounterExhausted | SessionError::NotFound) => {
                        Err(MessageHandleError::InternalError)
                    }
                }
            }
            ParsedStorageRequestV4::AuthorizeStop { session_id } => {
                let session_map = self.session_map.clone();
                match session_map.stop(session_id) {
                    Ok(()) => {
                        debug!(session_id, "Session stopped");
                        Ok(vec![])
                    }
                    Err(SessionError::NotFound) => Err(MessageHandleError::NotConnected),
                    Err(_) => Err(MessageHandleError::InternalError),
                }
            }
            ParsedStorageRequestV4::StorageCommand {
                session_id,
                opcode,
                payload,
            } => {
                let session_map = self.session_map.clone();
                let session = session_map
                    .get(session_id)
                    .ok_or(MessageHandleError::NotConnected)?;

                let repository = session.repository;
                let correlation_id = session.correlation_id.clone();
                let user_id = session.user_id.clone();
                let holds_read = session.holds_read;
                let holds_push = session.holds_push;
                drop(session);

                // Parse the storage command payload using v4-aware parsers — Copy carries an
                // extra `target_context` field on the wire that the legacy parser cannot decode.
                let parsed = parse_message_for_opcode_v4(opcode, payload).map_err(|err| {
                    tracing::warn!("Failed to parse v4 storage command: {err}");
                    MessageHandleError::InternalError
                })?;

                // Per-command action check, against the `read`/`push` this
                // session cached at `AuthorizeStart` — not a fresh
                // authorizer call, per that field's own doc comment. This
                // is what actually gates each operation: `AuthorizeStart`
                // establishing the session is necessary but not
                // sufficient, since a session can hold `read` xor `push`
                // rather than both. `Copy`'s destination is this session's
                // own repository (`push`, checked here); its cross-
                // partition *source* gets its own `read` check inside
                // `handle_copy` via `SessionMap::has_read_access`, since the
                // source may be a different repository than this session's.
                use crate::quic::storage_service::ParsedStorageRequest;
                match &parsed {
                    ParsedStorageRequest::Get(_)
                    | ParsedStorageRequest::GetMetadata(_)
                    | ParsedStorageRequest::GetResolved(_)
                    | ParsedStorageRequest::Query(_)
                    | ParsedStorageRequest::Verify(_)
                    | ParsedStorageRequest::MutableLoad(_) => {
                        if !holds_read {
                            return Err(MessageHandleError::AuthorizationFailure(
                                "caller does not hold the read action".to_string(),
                            ));
                        }
                    }
                    ParsedStorageRequest::Put(_)
                    | ParsedStorageRequest::PutResolved(_)
                    | ParsedStorageRequest::MutableStoreOp(_)
                    | ParsedStorageRequest::MutableCas(_)
                    | ParsedStorageRequest::Copy(_) => {
                        if !holds_push {
                            return Err(MessageHandleError::AuthorizationFailure(
                                "caller does not hold the push action".to_string(),
                            ));
                        }
                    }
                    // v2-only, handled as reserved opcodes before parsing is
                    // ever reached for a v4 StorageCommand — never actually
                    // produced by `parse_message_for_opcode_v4`.
                    ParsedStorageRequest::Connect(_) | ParsedStorageRequest::Correlate(_) => {}
                }

                // Dispatch to standalone handler functions with explicit session context
                let response = match parsed {
                    crate::quic::storage_service::ParsedStorageRequest::Get(get) => {
                        handle_get(
                            get.address,
                            repository,
                            correlation_id,
                            user_id,
                            self.immutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::GetMetadata(get) => {
                        crate::protocol::storage::get::handle_get_metadata(
                            get.address,
                            repository,
                            correlation_id,
                            user_id,
                            self.immutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::Put(put) => {
                        handle_put(
                            &put,
                            repository,
                            correlation_id,
                            user_id,
                            self.immutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::Query(_query) => {
                        // Query uses the raw bytes, not the parsed struct.
                        // Re-parse is needed because parse_message_for_opcode_v4 consumed the bytes.
                        // However, the Query struct stores the bytes internally.
                        handle_query(&_query.address, repository, self.immutable_store.clone())
                            .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::Verify(verify) => {
                        handle_verify(
                            verify.address,
                            verify.heal,
                            repository,
                            correlation_id,
                            user_id,
                            self.local_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::Copy(copy) => {
                        handle_copy(
                            copy.source_repository,
                            copy.source_address,
                            repository,
                            copy.target_context,
                            correlation_id,
                            user_id,
                            Some(&session_map),
                            self.immutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::MutableLoad(load) => {
                        handle_mutable_load(
                            load.key,
                            load.key_type,
                            repository,
                            correlation_id,
                            user_id,
                            self.mutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::GetResolved(resolved) => {
                        crate::protocol::storage::get_resolved::handle_get_resolved(
                            resolved.key,
                            resolved.context,
                            resolved.flags,
                            repository,
                            correlation_id,
                            user_id,
                            self.mutable_store.clone(),
                            self.immutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::PutResolved(resolved) => {
                        crate::protocol::storage::put_resolved::handle_put_resolved(
                            resolved.key,
                            resolved.put(),
                            resolved.address,
                            repository,
                            correlation_id,
                            user_id,
                            self.mutable_store.clone(),
                            self.immutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::MutableStoreOp(store) => {
                        handle_mutable_store(
                            store.key,
                            store.value,
                            store.key_type,
                            repository,
                            correlation_id,
                            user_id,
                            self.mutable_store.clone(),
                        )
                        .await
                    }
                    crate::quic::storage_service::ParsedStorageRequest::MutableCas(cas) => {
                        handle_mutable_cas(
                            cas.key,
                            cas.expected,
                            cas.value,
                            cas.key_type,
                            repository,
                            correlation_id,
                            user_id,
                            self.mutable_store.clone(),
                        )
                        .await
                    }
                    // Connect and Correlate are v2-only, handled as reserved opcodes above
                    crate::quic::storage_service::ParsedStorageRequest::Connect(_)
                    | crate::quic::storage_service::ParsedStorageRequest::Correlate(_) => {
                        Err(MessageHandleError::NotImplemented)
                    }
                }?;

                Ok(response.data())
            }
        }
    }

    fn command_to_metrics_label(&self, opcode: QuicOpCode) -> &'static str {
        if opcode == RESERVED_OPCODE_PING || opcode == RESERVED_OPCODE_CORRELATE {
            return "reserved";
        }
        if opcode == Command::Authorize as u8 {
            return "authorize";
        }
        let command: Result<Command, UnknownCommand> = opcode.try_into();
        match command {
            Ok(command) => command_name(&command),
            Err(_) => "unknown",
        }
    }

    fn transform_protocol_error(&self, error: &Self::RequestHandlerError) -> ProtocolErrorInfo {
        let service_error = quic_error_v4(error);
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

    fn header_size(&self) -> usize {
        COMMAND_HEADER_SIZE_V4
    }

    fn build_request_span(
        &self,
        header: &CommandHeader,
        _message: &Self::ParsedRequestType,
        context: &Arc<AttributeMap>,
    ) -> Span {
        let connection_id = context
            .get::<ConnectionId>()
            .map_or_else(|| NO_CONNECTION_ID.to_string(), |id| id.0.to_string());

        let session = if header.session_id != 0 {
            self.session_map.get(header.session_id)
        } else {
            None
        };

        let (repository_id, correlation_id, user_id) = match session {
            Some(session) => {
                let repository_id = session.repository.to_string();
                let repository_id = if repository_id.is_empty() {
                    NO_REPOSITORY_ID.to_string()
                } else {
                    repository_id
                };
                let correlation_id = if session.correlation_id.is_empty() {
                    NO_CORRELATION_ID.to_string()
                } else {
                    session.correlation_id.clone()
                };
                let user_id = if session.user_id.is_empty() {
                    NO_USER_ID.to_string()
                } else {
                    session.user_id.clone()
                };
                (repository_id, correlation_id, user_id)
            }
            None => (
                NO_REPOSITORY_ID.to_string(),
                NO_CORRELATION_ID.to_string(),
                NO_USER_ID.to_string(),
            ),
        };

        let user_agent = context.get::<crate::protocol::client_identify::UserAgentValue>();
        build_storage_protocol_request_span(
            header.cmd,
            StorageProtocol::StorageV4,
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
    use bytes::Bytes;
    use lore_telemetry::user_agent_filter::UserAgentFilter;
    use lore_transport::quic::QuicServiceError;
    use lore_transport::quic::command_header::CommandHeader;
    use rand::random;

    use super::*;
    use crate::protocol::storage::session::MAX_CONCURRENT_SESSIONS;
    use crate::quic::QuicService;
    use crate::store::test_store_create;

    fn make_service(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
    ) -> StorageServiceV4 {
        StorageServiceV4::new(
            Arc::new(None),
            Arc::new(crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer),
            immutable_store.clone(),
            immutable_store.clone(),
            mutable_store,
            Arc::new(UserAgentFilter::default()),
        )
    }

    fn make_header(cmd: u8) -> CommandHeader {
        CommandHeader {
            cmd,
            ..CommandHeader::default()
        }
    }

    #[tokio::test]
    async fn parse_client_identify_opcode_returns_variant() {
        use lore_transport::quic::storage_service::Command;
        // The stores are unused by parse_request_bytes, so a minimal service suffices.
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);

        let header = make_header(Command::ClientIdentify as u8);
        let payload = Bytes::from("my-client/1.0");

        let parsed = service
            .parse_request_bytes(&header, payload)
            .expect("parsing a ClientIdentify request must succeed");

        assert!(
            matches!(parsed, ParsedStorageRequestV4::ClientIdentify(_)),
            "expected ClientIdentify variant, got {parsed:?}"
        );
    }

    #[tokio::test]
    async fn parse_client_identify_stores_value() {
        use lore_transport::quic::storage_service::Command;
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);

        let header = make_header(Command::ClientIdentify as u8);
        let payload = Bytes::from("my-client/1.0");

        let parsed = service
            .parse_request_bytes(&header, payload)
            .expect("parsing a ClientIdentify request must succeed");
        let ParsedStorageRequestV4::ClientIdentify(ci) = parsed else {
            panic!("wrong variant");
        };
        assert_eq!(ci.user_agent, Some("my-client/1.0".to_string()));
    }

    #[tokio::test]
    async fn run_request_handler_client_identify_returns_empty_ok() {
        let (immutable_store, mutable_store, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service = make_service(immutable_store, mutable_store);

        let ci = crate::protocol::client_identify::ClientIdentify {
            user_agent: Some("my-client/1.0".to_string()),
            is_trusted: false,
        };

        let response = service
            .run_request_handler(
                Arc::new(AttributeMap::default()),
                ParsedStorageRequestV4::ClientIdentify(ci),
            )
            .await
            .expect("ClientIdentify must be handled successfully");

        assert!(response.is_empty(), "expected empty response vec");
    }

    /// Fill the session map to capacity then attempt one more `AuthorizeStart`,
    /// verifying the handler returns `SlowDown` and that `transform_protocol_error`
    /// classifies it the same way `stream_handler` would.
    #[tokio::test]
    async fn authorize_start_returns_slow_down_when_session_limit_reached() {
        let (immutable_store, mutable_store, _execution) =
            test_store_create().await.expect("Failed to create stores");

        let service = make_service(immutable_store, mutable_store);

        let repo = random::<lore_revision::lore::RepositoryId>();

        // Fill the session map to capacity via the handler (jwt_verifier is None,
        // so each call goes straight to session_map.start with no I/O).
        for i in 0..MAX_CONCURRENT_SESSIONS {
            let result = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::AuthorizeStart {
                        repository: repo,
                        correlation_id: format!("fill-{i}"),
                        auth_token: vec![],
                    },
                )
                .await;
            assert!(result.is_ok(), "session {i} should succeed");
        }

        // One more must hit the limit.
        let err = service
            .run_request_handler(
                AttributeMap::default().into(),
                ParsedStorageRequestV4::AuthorizeStart {
                    repository: repo,
                    correlation_id: "over-limit".into(),
                    auth_token: vec![],
                },
            )
            .await
            .expect_err("expected SlowDown when session limit is reached");

        assert!(
            matches!(err, MessageHandleError::SessionLimitReached),
            "expected SessionLimitReached, got {err:?}"
        );

        // Verify stream_handler classification: SlowDown on the wire, not an internal
        // error, and suppressed from logging (same suppression path as SlowDown).
        let error_info = service.transform_protocol_error(&err);
        assert_eq!(
            error_info.response_error_code,
            QuicServiceError::SlowDown as QuicErrorStatus,
        );
        assert_eq!(error_info.message_handle_label, "SessionLimitReached");
        assert!(!error_info.is_internal_error);
        assert!(!error_info.is_appropriate_for_logging);
    }

    /// Exercises the fix for the gap where `AuthorizeStart` checked plain
    /// reachability once and every subsequent `StorageCommand` on the
    /// session went unchecked: a per-command `read`/`push` gate against
    /// the flags cached on the session at authorize time.
    // Shared JWT test fixtures: used by both `baseline_action_gating` (session
    // gating, valid tokens) and `authorize_start_infra_failures` (the
    // error-collapsing regression, below) — kept at this level, not inside
    // either, so neither module has to duplicate them.
    use std::ops::Add;
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use jsonwebtoken::Algorithm;
    use jsonwebtoken::DecodingKey;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use jsonwebtoken::encode;

    use crate::auth::jwk::JWKService;
    use crate::auth::jwk::JWKServiceError;
    use crate::auth::jwt::AuthorizationToken;

    const TEST_ALGORITHM: Algorithm = Algorithm::HS256;
    const TEST_SIGNING_SECRET: &str = "storage-v4-baseline-action-test-secret";
    const TEST_AUDIENCE: &str = "lore-test";

    mockall::mock! {
        TestJWKService {}

        #[async_trait]
        impl JWKService for TestJWKService {
            async fn get_key(
                &self,
                kid: &str,
            ) -> Result<(DecodingKey, jsonwebtoken::Algorithm), JWKServiceError>;

            fn get_cached_key(
                &self,
                kid: &str,
            ) -> Option<(DecodingKey, jsonwebtoken::Algorithm)>;

            async fn refresh_key(
                &self,
                kid: &str,
            ) -> Result<Option<(DecodingKey, jsonwebtoken::Algorithm)>, JWKServiceError>;
        }
    }

    fn make_jwt(groups: Vec<String>) -> Vec<u8> {
        let claims = AuthorizationToken {
            user_id: "test-user".to_string(),
            issuer: "test-issuer".to_string(),
            issued_at: 1,
            expires: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .add(Duration::from_secs(60))
                .as_secs(),
            audience: vec![TEST_AUDIENCE.to_string()],
            groups: Some(groups),
            ..Default::default()
        };
        let key = EncodingKey::from_secret(TEST_SIGNING_SECRET.as_ref());
        let mut header = Header::new(TEST_ALGORITHM);
        header.kid = Some("test-kid".to_string());
        encode(&header, &claims, &key).unwrap().into_bytes()
    }

    mod baseline_action_gating {
        use lore_transport::quic::storage_service::Command;

        use super::*;
        use crate::auth::jwt::JwtVerifier;
        use crate::authnz::repository_authorizer::GlobalGrantsAuthorizer;

        fn make_tier1_service(
            immutable_store: Arc<dyn lore_storage::ImmutableStore>,
            mutable_store: Arc<dyn lore_storage::MutableStore>,
        ) -> StorageServiceV4 {
            let mut jwk_service = MockTestJWKService::new();
            jwk_service.expect_get_key().returning(|_| {
                Ok((
                    DecodingKey::from_secret(TEST_SIGNING_SECRET.as_ref()),
                    TEST_ALGORITHM,
                ))
            });
            let verifier = JwtVerifier {
                jwk_service: Arc::new(jwk_service),
                jwt_issuer: None,
                jwt_audience: Some(vec![TEST_AUDIENCE.to_string()]),
            };
            StorageServiceV4::new(
                Arc::new(Some(verifier)),
                Arc::new(GlobalGrantsAuthorizer::new(Some("groups".to_string()))),
                immutable_store.clone(),
                immutable_store,
                mutable_store,
                Arc::new(UserAgentFilter::default()),
            )
        }

        async fn authorize(
            service: &StorageServiceV4,
            repository: lore_revision::lore::RepositoryId,
            groups: Vec<String>,
        ) -> u32 {
            let response = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::AuthorizeStart {
                        repository,
                        correlation_id: "corr".into(),
                        auth_token: make_jwt(groups),
                    },
                )
                .await
                .expect("AuthorizeStart should succeed for a caller holding read or push");
            u32::from_le_bytes(response[0][..4].try_into().unwrap())
        }

        fn valid_mutable_store_payload() -> Bytes {
            // key Hash (32 zero bytes) ++ value Hash (32 zero bytes) ++ KeyType::Untyped (0)
            Bytes::from(vec![0u8; 65])
        }

        #[tokio::test]
        async fn authorize_start_fails_when_caller_holds_neither_action() {
            let (immutable_store, mutable_store, _exec) =
                test_store_create().await.expect("Failed to create stores");
            let service = make_tier1_service(immutable_store, mutable_store);

            let err = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::AuthorizeStart {
                        repository: random(),
                        correlation_id: "corr".into(),
                        auth_token: make_jwt(vec!["obliterate".to_string()]),
                    },
                )
                .await
                .expect_err("a caller holding neither read nor push must be denied a session");
            assert!(matches!(err, MessageHandleError::AuthorizationFailure(_)));
        }

        #[tokio::test]
        async fn push_only_session_denies_query_but_allows_mutable_store() {
            let (immutable_store, mutable_store, _exec) =
                test_store_create().await.expect("Failed to create stores");
            let service = make_tier1_service(immutable_store, mutable_store);
            let repository = random();

            let session_id = authorize(&service, repository, vec!["push".to_string()]).await;

            let err = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::StorageCommand {
                        session_id,
                        opcode: Command::Query as u8,
                        payload: Bytes::new(),
                    },
                )
                .await
                .expect_err("a push-only session must be denied a read command");
            assert!(matches!(err, MessageHandleError::AuthorizationFailure(_)));

            let result = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::StorageCommand {
                        session_id,
                        opcode: Command::MutableStore as u8,
                        payload: valid_mutable_store_payload(),
                    },
                )
                .await;
            assert!(
                !matches!(result, Err(MessageHandleError::AuthorizationFailure(_))),
                "a push-only session must be allowed to issue a push command, got {result:?}"
            );
        }

        #[tokio::test]
        async fn read_only_session_allows_query_but_denies_mutable_store() {
            let (immutable_store, mutable_store, _exec) =
                test_store_create().await.expect("Failed to create stores");
            let service = make_tier1_service(immutable_store, mutable_store);
            let repository = random();

            let session_id = authorize(&service, repository, vec!["read".to_string()]).await;

            let result = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::StorageCommand {
                        session_id,
                        opcode: Command::Query as u8,
                        payload: Bytes::new(),
                    },
                )
                .await;
            assert!(
                !matches!(result, Err(MessageHandleError::AuthorizationFailure(_))),
                "a read-only session must be allowed to issue a read command, got {result:?}"
            );

            let err = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::StorageCommand {
                        session_id,
                        opcode: Command::MutableStore as u8,
                        payload: valid_mutable_store_payload(),
                    },
                )
                .await
                .expect_err("a read-only session must be denied a push command");
            assert!(matches!(err, MessageHandleError::AuthorizationFailure(_)));
        }
    }

    /// Regression for the error-collapsing bug: `AuthorizeStart` resolves
    /// `read` and `push` as two independent authorizer calls. A failure of
    /// one of those calls (e.g. a legacy `AuthClientAuthorizer` deployment's
    /// online check timing out) must surface as a real error, not get
    /// silently folded into "does not hold this action" and cached on the
    /// session as such for its entire lifetime.
    mod authorize_start_infra_failures {
        use tonic::Status;

        use super::*;
        use crate::authnz::repository_authorizer::PUSH_ACTION;
        use crate::authnz::repository_authorizer::VerifiedToken;

        /// Answers `read` normally but fails `push` with an infrastructure
        /// error, simulating an online authorizer whose call for one of the
        /// two actions errors or times out while the other succeeds.
        struct FlakyPushAuthorizer;

        #[async_trait]
        impl RepositoryAuthorizer for FlakyPushAuthorizer {
            async fn check_repository_access(
                &self,
                _token: Option<&VerifiedToken<'_>>,
                _repository: lore_revision::lore::RepositoryId,
                action: Option<&str>,
            ) -> Result<(), Status> {
                if action == Some(PUSH_ACTION) {
                    Err(Status::internal("simulated auth service timeout"))
                } else {
                    Ok(())
                }
            }
        }

        fn make_flaky_push_service(
            immutable_store: Arc<dyn lore_storage::ImmutableStore>,
            mutable_store: Arc<dyn lore_storage::MutableStore>,
        ) -> StorageServiceV4 {
            let mut jwk_service = MockTestJWKService::new();
            jwk_service.expect_get_key().returning(|_| {
                Ok((
                    DecodingKey::from_secret(TEST_SIGNING_SECRET.as_ref()),
                    TEST_ALGORITHM,
                ))
            });
            let verifier = crate::auth::jwt::JwtVerifier {
                jwk_service: Arc::new(jwk_service),
                jwt_issuer: None,
                jwt_audience: Some(vec![TEST_AUDIENCE.to_string()]),
            };
            StorageServiceV4::new(
                Arc::new(Some(verifier)),
                Arc::new(FlakyPushAuthorizer),
                immutable_store.clone(),
                immutable_store,
                mutable_store,
                Arc::new(UserAgentFilter::default()),
            )
        }

        #[tokio::test]
        async fn authorize_start_surfaces_infra_failure_instead_of_read_only_fallback() {
            let (immutable_store, mutable_store, _exec) =
                test_store_create().await.expect("Failed to create stores");
            let service = make_flaky_push_service(immutable_store, mutable_store);

            let err = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::AuthorizeStart {
                        repository: random(),
                        correlation_id: "corr".into(),
                        auth_token: make_jwt(vec![]),
                    },
                )
                .await
                .expect_err("a failed push check must not be silently treated as 'push not held'");

            // Must be a real, distinguishable failure — never the same
            // `AuthorizationFailure` a genuine denial produces, which would
            // be indistinguishable from "this caller holds neither action"
            // and would pin the session read-only for its entire lifetime.
            assert!(
                matches!(err, MessageHandleError::InternalError),
                "expected InternalError for a failed check, got {err:?}"
            );
        }

        /// Round 5 regression: a legacy `AuthClientAuthorizer` deployment's
        /// *only* way of denying `read`/`push` is the requested resource
        /// being absent from `CheckUserPermissionResponse
        /// .allowed_resource_permission` — which
        /// `interpret_check_user_permission_response`
        /// (`authnz/repository_authorizer.rs`) maps to
        /// `Status::permission_denied`, not `Status::internal`. This mock
        /// reproduces exactly that shape (denies both actions with
        /// `PermissionDenied`, mirroring what a real `AuthClientAuthorizer`
        /// now returns when a repository is simply unreachable) and proves
        /// `action_held` recognizes it as a real denial: a clean
        /// `AuthorizationFailure`, never `InternalError`.
        struct AuthClientShapedDenialAuthorizer;

        #[async_trait]
        impl RepositoryAuthorizer for AuthClientShapedDenialAuthorizer {
            async fn check_repository_access(
                &self,
                _token: Option<&VerifiedToken<'_>>,
                _repository: lore_revision::lore::RepositoryId,
                action: Option<&str>,
            ) -> Result<(), Status> {
                Err(Status::permission_denied(format!(
                    "caller has no permissions for resource (action: {action:?})"
                )))
            }
        }

        fn make_auth_client_shaped_denial_service(
            immutable_store: Arc<dyn lore_storage::ImmutableStore>,
            mutable_store: Arc<dyn lore_storage::MutableStore>,
        ) -> StorageServiceV4 {
            let mut jwk_service = MockTestJWKService::new();
            jwk_service.expect_get_key().returning(|_| {
                Ok((
                    DecodingKey::from_secret(TEST_SIGNING_SECRET.as_ref()),
                    TEST_ALGORITHM,
                ))
            });
            let verifier = crate::auth::jwt::JwtVerifier {
                jwk_service: Arc::new(jwk_service),
                jwt_issuer: None,
                jwt_audience: Some(vec![TEST_AUDIENCE.to_string()]),
            };
            StorageServiceV4::new(
                Arc::new(Some(verifier)),
                Arc::new(AuthClientShapedDenialAuthorizer),
                immutable_store.clone(),
                immutable_store,
                mutable_store,
                Arc::new(UserAgentFilter::default()),
            )
        }

        #[tokio::test]
        async fn authorize_start_gives_a_clean_rejection_for_an_auth_client_shaped_denial() {
            let (immutable_store, mutable_store, _exec) =
                test_store_create().await.expect("Failed to create stores");
            let service = make_auth_client_shaped_denial_service(immutable_store, mutable_store);

            let err = service
                .run_request_handler(
                    AttributeMap::default().into(),
                    ParsedStorageRequestV4::AuthorizeStart {
                        repository: random(),
                        correlation_id: "corr".into(),
                        auth_token: make_jwt(vec![]),
                    },
                )
                .await
                .expect_err("a caller denied both actions must be rejected");

            assert!(
                matches!(err, MessageHandleError::AuthorizationFailure(_)),
                "expected a clean AuthorizationFailure for a real denial, got {err:?}"
            );
        }
    }
}
