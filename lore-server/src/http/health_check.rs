// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::http::server::ServerHealth;

/// Body of the 503 answered while the server drains before shutting down.
pub const DRAINING_BODY: &str = "draining";

pub async fn handler(State(state): State<Arc<ServerHealth>>) -> impl IntoResponse {
    if state.draining.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, DRAINING_BODY).into_response();
    }
    if state.store_health_check && !state.available.load(Ordering::Relaxed) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    StatusCode::OK.into_response()
}
