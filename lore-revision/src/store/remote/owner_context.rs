// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit owner-aware remote stores; ordinary constructors are unchanged.

use lore_transport::OwnerConnectionSource;

use super::*;
use crate::interface::ExecutionContext;

impl RemoteImmutableStore {
    /// Uses only the established source's live credentials during lazy setup.
    /// Finite store work/errors retain caller context and session correlation.
    pub async fn for_background_owner(
        source: OwnerConnectionSource,
        execution: &ExecutionContext,
    ) -> Result<Self, ProtocolError> {
        Ok(Self::from_connections(
            protocol::RemoteConnections::for_background_owner(source, execution).await?,
        ))
    }
    #[lore_macro::test_pub]
    pub(crate) fn from_connections(connections: protocol::RemoteConnections) -> Self {
        Self { connections }
    }
}
impl RemoteMutableStore {
    /// Uses the established source's live credentials. This is not a provider
    /// admission or publication capability and does not adopt cached principals.
    pub async fn for_background_owner(
        source: OwnerConnectionSource,
        execution: &ExecutionContext,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            connections: protocol::RemoteConnections::for_background_owner(source, execution)
                .await?,
        })
    }
}
