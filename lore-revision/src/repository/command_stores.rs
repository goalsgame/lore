// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private adapters for supported command-store methods and their spawned jobs.
//! They do not cover arbitrary low-level in-process `StoreLock` use, qualify
//! bootstrap, acquire Client tokens, or provide an exclusive handoff.
//!
//! Returned `StoreGuard`s retain their store claims, not root admission. Supported
//! command lifetimes must retain the facade/load, and finite background jobs must
//! retain their admitted IO lease and actual resources until completion. A guard
//! alone admits no operation; manipulating its raw lock or refresh state is outside
//! this boundary. Local claim delegates finish their reload before returning.

use std::sync::Arc;

use lore_io::lease::IoLease;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use lore_storage::StoreError;

use crate::fs::provider_registry::BoundLoad;
use crate::fs::provider_registry::BoundWork;

mod immutable;
mod mutable;

pub(crate) fn wrap(
    immutable: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    load: Arc<BoundLoad>,
) -> (Arc<dyn ImmutableStore>, Arc<dyn MutableStore>) {
    (
        Arc::new(immutable::CommandImmutable {
            inner: immutable,
            load: load.clone(),
        }),
        Arc::new(mutable::CommandMutable {
            inner: mutable,
            load,
        }),
    )
}
fn operation(load: &BoundLoad) -> Result<IoLease, StoreError> {
    load.begin_work()
        .map(BoundWork::into_io_lease)
        .map_err(|error| {
            StoreError::internal_with_context(error, "Working store admission refused")
        })
}

#[cfg(test)]
mod tests;
