// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit connection-owner context and live principal. Ordinary constructors
//! retain caller-scoped behavior. This cannot detach already spawned transports
//! or adopt a different credential object from an ordinary connection cache.

use lore_transport::OwnerConnectionSource;

use super::*;
use crate::interface::ExecutionContext;
use crate::runtime::LORE_CONTEXT;

/// Source and execution are immutable and paired before any lazy setup.
/// Retaining the source retains its live credential object, not a connection,
/// command context, registry or command-work capability.
pub(super) struct OwnerConnection {
    source: OwnerConnectionSource,
    execution: Arc<ExecutionContext>,
    max_connections: usize,
    connect: OwnerConnect,
}
type OwnerConnect = fn(
    OwnerConnectionSource,
    RepositoryId,
    usize,
) -> BoxFuture<'static, Result<Arc<Connection>, ProtocolError>>;

impl RemoteConnections {
    /// Constructs lazy connections from an established principal and its live
    /// rotating credentials, with a detached background execution context.
    /// Credential authority comes only from `source`, never snapshots of the
    /// context's supplied tokens. Finite setup retains the current IO lease.
    pub async fn for_background_owner(
        source: OwnerConnectionSource,
        execution: &ExecutionContext,
    ) -> Result<Self, ProtocolError> {
        if source.auth_mode() != execution.globals().auth_mode {
            return Err(ProtocolError::internal(
                "owner context authentication mode differs from its source",
            ));
        }
        let execution = Arc::new(execution.for_background_owner().await);
        LORE_CONTEXT.sync_scope(execution.clone(), || source.check_environment())?;
        let mut connections = Self::new(source.remote_url(), Some(source.identity()));
        connections.owner = Some(OwnerConnection {
            source,
            max_connections: execution.globals().max_connections as usize,
            execution,
            connect: |source, repository, maximum| {
                async move { lore_transport::connect_owned(&source, repository, maximum).await }
                    .boxed()
            },
        });
        Ok(connections)
    }

    #[cfg(unix)]
    pub(crate) fn owner_remote_future(
        self: &Arc<Self>,
        repository: RepositoryId,
    ) -> Result<RemoteFuture, ProtocolError> {
        let owner = self
            .owner
            .as_ref()
            .ok_or_else(|| ProtocolError::internal("missing owner connection authority"))?;
        let connections = self.clone();
        Ok(LORE_CONTEXT.sync_scope(owner.execution.clone(), || {
            lore_io::lease::sync_scope(None, || {
                shared_connect(async move { connections.connection(repository).await })
            })
        }))
    }

    pub(super) fn start_connection(&self, repository: RepositoryId) -> RemoteFuture {
        if let Some(owner) = &self.owner {
            // Scope creation, not only polling: the synchronous connector and
            // shared_connect's outer spawned wrapper must both capture owner.
            LORE_CONTEXT.sync_scope(owner.execution.clone(), || {
                let source = owner.source.clone();
                let connect = owner.connect;
                let maximum = owner.max_connections;
                // Recheck process fallback environment on every attempt, before
                // invoking even a test connector. No token snapshot is written.
                shared_connect(async move {
                    source.check_environment()?;
                    let result = connect(source.clone(), repository, maximum).await;
                    source.check_environment()?;
                    result
                })
            })
        } else {
            shared_connect((self.connect)(
                self.remote_url.clone(),
                self.identity.clone(),
                repository,
            ))
        }
    }

    pub(crate) fn check_owner_environment(&self) -> Result<(), ProtocolError> {
        match &self.owner {
            Some(owner) => LORE_CONTEXT
                .sync_scope(owner.execution.clone(), || owner.source.check_environment()),
            None => Ok(()),
        }
    }

    /// Scope only lazy transport setup. Callers retain their context around
    /// finite store methods and error reporting, and pass their correlation ID.
    #[lore_macro::test_pub]
    pub(crate) async fn owner_setup<T, F>(&self, setup: F) -> Result<T, ProtocolError>
    where
        F: Future<Output = Result<T, ProtocolError>>,
    {
        match &self.owner {
            Some(owner) => {
                LORE_CONTEXT
                    .scope(owner.execution.clone(), async {
                        owner.source.check_environment()?;
                        let result = setup.await;
                        owner.source.check_environment()?;
                        result
                    })
                    .await
            }
            None => setup.await,
        }
    }

    /// Connector injection uses the same checked source/context constructor.
    #[cfg(feature = "test-util")]
    pub async fn with_connect_for_background_owner(
        connect: OwnerConnect,
        source: OwnerConnectionSource,
        execution: &ExecutionContext,
    ) -> Result<Self, ProtocolError> {
        let mut connections = Self::for_background_owner(source, execution).await?;
        connections.owner.as_mut().unwrap().connect = connect;
        Ok(connections)
    }
}
