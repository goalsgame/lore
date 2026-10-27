// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact ready admission precedes command store construction and upgrades.

use lore_io::lease::IoLease;

use super::*;
use crate::fs::provider_registry::BoundLoad;
use crate::fs::provider_registry::ProviderResolver;

pub(super) struct WorkingLoad {
    pub provider: Arc<dyn FilesystemProvider>,
    pub load: Arc<BoundLoad>,
    pub io: IoLease,
}

pub(super) fn admit(
    paths: &ResolvedRepositoryPaths,
    resolver: Option<&Arc<dyn ProviderResolver>>,
    token: Option<&RepositoryWriteToken>,
) -> Result<Option<WorkingLoad>, RepositoryError> {
    let Some(location) = &paths.working else {
        return Ok(None);
    };
    let resolver = resolver.ok_or_else(|| {
        RepositoryError::internal("Working store admission requires its service resolver")
    })?;
    let (provider, load) = resolver
        .resolve_bootstrap(location)
        .map_err(|error| RepositoryError::internal(error.to_string()))?
        .into_parts();
    let work = load
        .begin_work()
        .map_err(|error| RepositoryError::internal(error.to_string()))?
        .into_io_lease();
    let io = IoLease::new(Arc::new(OpeningIo {
        _token: token.map(RepositoryWriteToken::share),
        _work: work,
    }));
    Ok(Some(WorkingLoad {
        provider,
        load: Arc::new(load),
        io,
    }))
}

// Actual opening jobs retain the canonical Client token through their cleanup;
// work and inherited service accounting finish after releasing that token.
struct OpeningIo {
    _token: Option<RepositoryWriteToken>,
    _work: IoLease,
}
