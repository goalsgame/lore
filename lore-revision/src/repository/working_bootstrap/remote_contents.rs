// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit live remote-read authority for a privately owned working instance.
//! It contains no source repository, command token, context, or IO lease.
use lore_base::types::RepositoryId;
use lore_transport::OwnerConnectionSource;

use super::Error;
use super::ImportReadPolicy;
use super::Result;
use crate::interface::ExecutionContext;
use crate::repository::RepositoryConfig;
use crate::repository::RepositoryContext;

pub struct RemoteContents {
    source: OwnerConnectionSource,
    policy: ImportReadPolicy,
}
impl RemoteContents {
    pub fn new(source: OwnerConnectionSource, policy: ImportReadPolicy) -> Result<Self> {
        source.check_environment().map_err(failure)?;
        Ok(Self { source, policy })
    }
    pub fn repository_id(&self) -> RepositoryId {
        self.policy.repository_id()
    }
    pub fn remote_url(&self) -> &str {
        self.source.remote_url()
    }
    pub fn identity(&self) -> &str {
        self.source.identity()
    }
    pub fn validate(
        &self,
        repository: RepositoryId,
        config: &RepositoryConfig,
        execution: &ExecutionContext,
    ) -> Result<()> {
        self.source.check_environment().map_err(failure)?;
        let requested = execution.globals().identity.to_string();
        let identity = if requested.is_empty() {
            config.identity.as_deref().unwrap_or_default()
        } else {
            requested.as_str()
        };
        if self.repository_id() != repository
            || execution.globals().offline()
            || execution.globals().trust_remote()
            || execution.globals().auth_mode != self.source.auth_mode()
            || config.remote_url.as_deref() != Some(self.remote_url())
            || identity != self.identity()
        {
            return Err(Error::InvalidBinding);
        }
        Ok(())
    }
    pub(super) async fn apply(
        &self,
        repository: RepositoryContext,
        execution: &ExecutionContext,
    ) -> Result<RepositoryContext> {
        let repository = self.policy.apply(repository)?;
        repository.set_disable_cache(false);
        repository.set_disable_upload(true);
        repository
            .with_owner_remote(self.source.clone(), execution)
            .await
            .map_err(failure)
    }
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::Preparation(format!("owner remote contents: {error}"))
}
