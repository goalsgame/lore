// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fmt::Debug;
use std::string::FromUtf8Error;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use lore_storage::StoreError;
use thiserror::Error;
use tracing::warn;

use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::JwtVerifier;
use crate::authnz::repository_authorizer::ReachabilityAuthorizer;
use crate::protocol::attribute_map::AttributeMap;
use crate::protocol::storage::responses;

/// The connection's authorization state, established once by `Connect`
/// (`protocol/storage/connect.rs`) and read:
/// - per-request, by the central dispatcher (`quic/storage_service.rs`'s
///   `run_request_handler`), which checks `holds_read`/`holds_push` before
///   dispatching to any message other than `Connect` itself;
/// - per-fragment, by `Copy` (`protocol/storage/copy.rs`), which needs its
///   own `read` check because a copy's source repository may differ from
///   the one this connection authorized against.
///
/// A successful `Connect` always inserts this into the connection's
/// `AttributeMap` — `Open` when no verifier is configured, `Verified`
/// otherwise — as a single entry, so its absence on an established
/// connection is always a wiring bug (a message handled before `Connect`, a
/// future transport, or a refactor that forgets to call it), never a
/// legitimate "no auth configured" state. Bundling the token and the
/// authorizer into one entry, rather than two independent, individually
/// optional ones, also makes "one present without the other" impossible by
/// construction instead of merely unlikely.
#[derive(Clone)]
pub enum ConnectionAuthorization {
    /// No verifier is configured for this deployment: matches
    /// `AllowAllRepositoryAuthorizer`'s semantics everywhere else — every
    /// request holds both baseline actions.
    Open,
    /// A caller was verified at connect time. `token` is boxed because
    /// `AuthorizationToken` is much larger than `Open`'s no-data variant
    /// (clippy::large_enum_variant); this is inserted once per connection,
    /// not once per fragment, so the extra indirection is not a hot path.
    Verified {
        token: Box<AuthorizationToken>,
        reachability_authorizer: ReachabilityAuthorizer,
        /// Whether `Connect` found this connection's caller holding the
        /// baseline `read`/`push` actions on the repository it connected
        /// to, resolved once here (LEP 2026-08-20-oidc-oauth2-authentication,
        /// D9's "once per session/connection rather than per operation"
        /// principle, the same one already applied to plain reachability)
        /// rather than asking the authorizer again for every subsequent
        /// request on this connection.
        holds_read: bool,
        holds_push: bool,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum MessageParseError {
    #[error("Failed to parse branch name: {0}")]
    BranchNameParseFailure(#[from] FromUtf8Error),
    #[error("Unable to parse empty slice")]
    EmptySlice,
    #[error("Invalid field length")]
    InvalidFieldLength,
    #[error("Invalid ping value")]
    InvalidPingValue,
    #[error("Invalid query length, should be a multiple of {}", size_of::<Address>())]
    InvalidQueryLength,
    #[error("Failed to parse message: {0}")]
    ParseFailure(&'static str),
    #[error("Expected {0} bytes, but got {1}")]
    SizeMismatch(usize, usize),
    #[error("Too many fragments specified (maximum: {0}, got: {1})")]
    TooManyFragments(usize, usize),
    #[error("Unknown/unsupported opcode: {0}")]
    UnknownOpcode(u8),
}

#[derive(Debug, Error)]
pub enum MessageHandleError {
    #[error("Authorization failed ({0})")]
    AuthorizationFailure(String),
    #[error("Already connected to a repository")]
    AlreadyConnected,
    #[error("Branch already exists")]
    BranchExists,
    #[error("Branch name mismatch")]
    BranchMismatch,
    #[error("Branch is protected")]
    BranchProtected,
    #[error("Fragment not found")]
    FragmentNotFound,
    #[error("Hash for content did not match the provided hash")]
    HashMismatch,
    #[error("Branch parent does not match commit parent")]
    InvalidParentBranch,
    #[error("Internal error")]
    InternalError,
    #[error("Authorization failed: missing token")]
    MissingToken,
    #[error("Mutable data not found for hash: {0}")]
    MutableDataNotFound(Hash),
    #[error("Branch does not exist")]
    NoSuchBranch,
    #[error("Not connected to a repository")]
    NotConnected,
    #[error("Operation not implemented")]
    NotImplemented,
    #[error("Failed to query fragments, size of results did not match size of fragments")]
    QueryResultSizeMismatch,
    #[error("Store operation failed")]
    StoreFailure,
    #[error("Server overloaded, slow down")]
    SlowDown,
    #[error("Fragment or blob exceeded size limit")]
    Oversized,
    #[error("Metadata operation failed")]
    Metadata,
    #[error("Failed to compute hash for content")]
    HashFailed,
    #[error("Failed to validate fragment")]
    InvalidFragment,
    #[error("Failed to handle the request in time")]
    HandlerTimeout,
    #[error("Session Limit Reached")]
    SessionLimitReached,
}

impl From<StoreError> for MessageHandleError {
    fn from(value: StoreError) -> Self {
        warn!("Received store error: {value:?}");
        match value {
            StoreError::SlowDown(_) => MessageHandleError::SlowDown,
            StoreError::Oversized(_) => MessageHandleError::Oversized,
            _ => MessageHandleError::StoreFailure,
        }
    }
}

#[async_trait]
#[enum_dispatch]
pub trait Message: Debug + Send + Sync {
    async fn handle(
        &self,
        _context: Arc<AttributeMap>,
        _immutable_store: Arc<dyn ImmutableStore>,
    ) -> Result<LoreResponse, MessageHandleError> {
        Err(MessageHandleError::NotImplemented)
    }

    async fn handle_auth(
        &self,
        _context: Arc<AttributeMap>,
        _jwt_verifier: Arc<Option<JwtVerifier>>,
        _reachability_authorizer: ReachabilityAuthorizer,
    ) -> Result<LoreResponse, MessageHandleError> {
        Err(MessageHandleError::NotImplemented)
    }

    async fn handle_mutable(
        &self,
        _context: Arc<AttributeMap>,
        _mutable_store: Arc<dyn MutableStore>,
    ) -> Result<LoreResponse, MessageHandleError> {
        Err(MessageHandleError::NotImplemented)
    }
}

#[enum_dispatch]
pub trait Response {
    fn data(&self) -> Vec<Bytes>;
}

#[derive(Debug, PartialEq)]
#[enum_dispatch(Response)]
pub enum LoreResponse {
    Connect(responses::ConnectResponse),
    Copy(responses::CopyResponse),
    Get(responses::GetResponse),
    GetResolved(responses::GetResolvedResponse),
    PutResolved(responses::PutResolvedResponse),
    Put(responses::PutResponse),
    Query(responses::QueryResponse),
    Ping(responses::PingResponse),
    Correlate(responses::CorrelateResponse),
    Verify(responses::VerifyResponse),
    MutableLoad(responses::MutableLoadResponse),
    MutableStore(responses::MutableStoreResponse),
    MutableCas(responses::MutableCasResponse),
}
