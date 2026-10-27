// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#[cfg(not(unix))]
mod legacy;
#[cfg(not(unix))]
pub use legacy::*;
#[cfg(unix)]
use lore_base::error::InvalidPath;
#[cfg(unix)]
use lore_error_set::error_set;
#[cfg(unix)]
#[error_set]
pub enum ServiceMainError {
    InvalidPath,
}
#[cfg(unix)]
impl lore_revision::event::EventError for ServiceMainError {}

/// Runs an owned Unix service until its exact run has drained and stopped.
#[cfg(unix)]
pub async fn service_main(
    globals: crate::interface::LoreGlobalArgs,
    state: std::sync::Arc<lore_revision::service_state::ServiceStateImpl>,
) -> Result<(), ServiceMainError> {
    super::actor::run(globals, state).await
}

#[cfg(unix)]
pub(super) async fn report_build_that_is_not_the_configured_one() {
    let Ok(configured) = crate::remote::service_process::service_executable().await else {
        return;
    };
    let Ok(running) = std::env::current_exe() else {
        return;
    };

    // Compared through the filesystem where possible, so that a link or a
    // relative path to the same build does not read as a different one.
    let resolve =
        |path: &std::path::Path| std::fs::canonicalize(path).unwrap_or(path.to_path_buf());
    if resolve(&configured) == resolve(&running) {
        return;
    }

    println!(
        "Serving from {}, which is not the configured Lore service ({})",
        running.display(),
        configured.display()
    );
}
