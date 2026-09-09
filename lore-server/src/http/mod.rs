// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
pub mod health_check;
pub mod presign_token;
pub mod presigned;
pub mod repositories;
pub mod security_headers;
pub mod server;
#[cfg(test)]
pub(crate) mod test_utils;
pub mod tracing;

use ::tracing::debug;
use ::tracing::warn;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use lore_transport::grpc::CORRELATION_ID_HEADER;
pub use server::LoreHttpServer;

pub(crate) fn log_http_error(error: &impl std::fmt::Debug, status: StatusCode) {
    if status.is_server_error() {
        warn!(?error, "http server error");
    } else {
        debug!(?error, "http user error");
    }
}

/// The bearer token exactly as presented, without the `Bearer ` prefix.
/// Needed only so a legacy `AuthClientAuthorizer` can forward it to the auth
/// service; `jwt_axum_middleware` decodes it but does not retain the raw
/// form, so it is re-extracted here from the same header.
///
/// Shared by every `repositories/repository/contents` handler that builds
/// its own `VerifiedToken` (`get_repository_content`,
/// `put_repository_content`, `presign_repository_content`) — previously
/// duplicated verbatim in each of the three.
pub(crate) fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
}

/// Extracts correlation IDs from `http::Request` headers
pub fn extract_correlation_id<B>(req: &http::Request<B>) -> Option<String> {
    match req.headers().get(CORRELATION_ID_HEADER) {
        Some(val) => val.to_str().map(|s| s.to_string()).ok(),
        None => None,
    }
}
