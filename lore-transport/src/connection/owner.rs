// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! A live credential source for owner connections outside the ordinary global
//! connection cache. It does not adopt a different credential object installed
//! there later; reauthentication requires an explicit new source.

use super::*;

/// The exact principal and live credentials of an established connection. This
/// retains no connection, execution context, registry, or raw token snapshot.
#[derive(Clone)]
pub struct OwnerConnectionSource {
    pub(super) principal: ConnectionKey,
    pub(super) credentials: Arc<SuppliedCredentials>,
}
impl OwnerConnectionSource {
    pub fn remote_url(&self) -> &str {
        &self.principal.0
    }
    pub fn identity(&self) -> &str {
        &self.principal.1
    }
    pub fn auth_mode(&self) -> AuthMode {
        self.principal.4
    }
    /// Check credential-store and supplied-credential mode without performing IO.
    /// Run this under the execution context that will create owner transports.
    pub fn check_environment(&self) -> Result<(), ProtocolError> {
        if self.principal.3 != credential_store() {
            return Err(ProtocolError::internal(
                "Owner connection credential store changed",
            ));
        }
        if self.principal.2 != self.credentials.from_supplied_credentials() {
            return Err(ProtocolError::internal(
                "Owner connection credential mode changed",
            ));
        }
        Ok(())
    }
    pub(super) fn check_connection(&self, connection: &Connection) -> Result<(), ProtocolError> {
        if self.principal != connection.principal
            || self.remote_url() != connection.remote_url.as_str()
            || self.identity() != connection.identity
            || self.auth_mode() != connection.auth_mode
            || !Arc::ptr_eq(&self.credentials, &connection.credentials)
        {
            return Err(ProtocolError::internal(
                "Owner connection principal changed",
            ));
        }
        self.check_environment()
    }
}
impl Connection {
    /// Capture a source after this connection's storage setup succeeds. Each
    /// operation still performs its own authorization; this grants no repository
    /// access, Client token, bootstrap qualification, or publication permission.
    /// This may await setup started in the original caller context; minting a
    /// source cannot detach tasks already spawned there. Establish the initial
    /// connection under its appropriate owner context before capturing a source.
    pub async fn owner_source(&self) -> Result<OwnerConnectionSource, ProtocolError> {
        let source = OwnerConnectionSource {
            principal: self.principal.clone(),
            credentials: self.credentials.clone(),
        };
        source.check_connection(self)?;
        self.storage_ready.wait().await?;
        if self.stale.load(Ordering::Relaxed) {
            return Err(ProtocolError::internal("Owner connection source is stale"));
        }
        source.check_connection(self)?;
        Ok(source)
    }
}

/// Open a fresh owner connection using the source's current live credentials.
/// No ordinary cache lookup, insertion, or credential update is performed.
/// Caller cancellation leaves only the existing finite transport task cleanup;
/// callers must retain actual IO ownership around this future when required.
pub async fn connect_owned(
    source: &OwnerConnectionSource,
    repository: RepositoryId,
    max_connections: usize,
) -> Result<Arc<Connection>, ProtocolError> {
    source.check_environment()?;
    let (remote_url, protocol) = parse(source.remote_url())?;
    if remote_url.as_str() != source.remote_url() {
        return Err(ProtocolError::internal(
            "Owner connection URL is not canonical",
        ));
    }
    Box::pin(connect_impl(
        protocol,
        remote_url,
        source.identity().to_string(),
        repository,
        max_connections,
        String::new(),
        String::new(),
        source.auth_mode(),
        Some(source.clone()),
    ))
    .await
}

#[cfg(test)]
mod tests;
