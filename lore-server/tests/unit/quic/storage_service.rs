// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use lore_server::auth::jwt::AuthorizationToken;
use lore_server::protocol::attribute_map::AttributeMap;
use lore_server::protocol::storage::messages::MessageHandleError;
use lore_server::quic::storage_service::reject_expired_connection;

fn context_with_token_expiring_at(expires: u64) -> Arc<AttributeMap> {
    let context = Arc::new(AttributeMap::default());
    context.insert(AuthorizationToken {
        user_id: "test-user".to_string(),
        expires,
        ..Default::default()
    });
    context
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

/// A connection keeps serving for as long as it is held open, so expiry has
/// to be rechecked per request rather than only at `Connect`.
#[test]
fn refuses_a_token_that_has_expired() {
    let context = context_with_token_expiring_at(now() - 60);
    assert!(matches!(
        reject_expired_connection(&context),
        Err(MessageHandleError::AuthorizationFailure(_))
    ));
}

#[test]
fn allows_a_token_that_has_not_expired() {
    let context = context_with_token_expiring_at(now() + 600);
    assert!(reject_expired_connection(&context).is_ok());
}

/// No token means no verifier was configured; `Connect` already decided that.
#[test]
fn allows_a_connection_carrying_no_token() {
    let context = Arc::new(AttributeMap::default());
    assert!(reject_expired_connection(&context).is_ok());
}
