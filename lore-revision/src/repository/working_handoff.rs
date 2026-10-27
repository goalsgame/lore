// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Command-owned, provider-free resources for strict read-only anchor inspection.
//! This is pre-intent exclusion. It grants no anchor write or installation right.

mod preparation;
use lore_io::lease::IoLease;

use super::*;
use crate::fs::provider_registry::BoundLoad;
use crate::fs::provider_registry::CommandExclusion;
use crate::instance::strict_anchors;

pub(super) struct WorkingResources {
    immutable: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    issuer: Arc<BoundLoad>,
}
impl WorkingResources {
    // Called only by the actual loader, with the final pair before facade wrapping.
    pub(super) fn new(
        immutable: Arc<dyn ImmutableStore>,
        mutable: Arc<dyn MutableStore>,
        issuer: Arc<BoundLoad>,
    ) -> Arc<Self> {
        Arc::new(Self {
            immutable,
            mutable,
            issuer,
        })
    }
}

/// Retains the actual command stores, claims and canonical Client token while
/// other loads and ordinary store operations are excluded. No raw store handles
/// escape. Dropping the last retained owner reopens admission only before intent.
pub struct WorkingCommandHandoff {
    anchors: RepositoryContext,
    _resources: Arc<WorkingResources>,
    _repository_lock: Option<Arc<RepositoryLock>>,
    _store_hold: Option<Arc<lore_storage::local::store_lock::StoreHold>>,
    _token: RepositoryWriteToken,
    exclusion: CommandExclusion,
    _parent: Option<IoLease>,
}
impl RepositoryContext {
    fn validate_handoff_source(&self) -> Result<(), RepositoryError> {
        let resources = self.working_resources.as_ref().ok_or_else(refused)?;
        let issuer = self.working_admission.as_ref().ok_or_else(refused)?;
        let location = issuer.location();
        let token = self.write_token.as_ref().ok_or_else(refused)?;
        let RepositoryWriteToken::Client(client) = token else {
            return Err(refused());
        };
        if !Arc::ptr_eq(issuer, &resources.issuer)
            || self.id != location.repository()
            || self.root_id != location.repository()
            || self.instance_id != location.instance()
            || self.mount.is_some()
            || self.repo_lock.is_none()
            || self.path() != Some(location.target().path())
            || self.dot_dir_path().ok() != Some(location.metadata().path())
            || !Arc::ptr_eq(
                tokio::sync::OwnedMutexGuard::mutex(client.as_ref()),
                &write_token::write_mutex_for_path(location.target().path()),
            )
        {
            return Err(refused());
        }
        Ok(())
    }

    // The staged rebase needs a real provider operation and a serialization
    // token. Run that finite read-only filesystem operation before exclusion,
    // discarding every mutable-store write while preserving immutable output.
    pub(crate) async fn prepare_root_anchors(
        &self,
        target: lore_base::types::Hash,
        branch: lore_base::types::Context,
        options: crate::fs::filesystem_provider::HoldOptions,
    ) -> Result<
        (
            Arc<WorkingCommandHandoff>,
            strict_anchors::Snapshot,
            strict_anchors::Snapshot,
        ),
        RepositoryError,
    > {
        self.validate_handoff_source()?;
        if branch.is_zero() {
            return Err(refused());
        }
        let (old, staged) = self
            .prepare_io(async {
                let old = strict_anchors::read(self)
                    .await
                    .map_err(|error| RepositoryError::internal(error.to_string()))?;
                let staged = if options.dry_run || old.staged == Some(target) {
                    old.staged
                } else if let Some(staged) = old.staged {
                    let prepared = Arc::new(RepositoryContext {
                        mutable_store: Arc::new(
                            crate::store::write_discarding::WriteDiscardingMutableStore::new(
                                self.mutable_store.clone(),
                            ),
                        ),
                        ..self.inherited()
                    });
                    Box::pin(crate::state::rebase_staged_state(
                        prepared,
                        staged,
                        target,
                        options.overwrite_local,
                    ))
                    .await
                    .map_err(|error| RepositoryError::internal(error.to_string()))?
                } else {
                    None
                };
                if !options.dry_run {
                    self.immutable_store
                        .clone()
                        .flush(true)
                        .await
                        .map_err(|error| RepositoryError::internal(error.to_string()))?;
                }
                Ok((old, staged))
            })
            .await?;
        let handoff = self.try_working_handoff()?;
        if handoff.read_anchors().await? != old {
            return Err(RepositoryError::internal(
                "Working anchors changed during root preparation",
            ));
        }
        let new = strict_anchors::Snapshot {
            current: target,
            branch,
            staged,
            ..old
        };
        Ok((handoff, old, new))
    }

    /// Try to exclude sibling loads and retained work without waiting while the
    /// current command holds its Client. This initial boundary accepts only the
    /// exact loaded root partition; linked/layer scopes need separate route proof.
    pub fn try_working_handoff(&self) -> Result<Arc<WorkingCommandHandoff>, RepositoryError> {
        self.validate_handoff_source()?;
        let resources = self.working_resources.as_ref().ok_or_else(refused)?;
        let issuer = self.working_admission.as_ref().ok_or_else(refused)?;
        let location = issuer.location();
        let token = self.write_token.as_ref().ok_or_else(refused)?;
        // Build a fresh provider-free context. Inheriting the command context
        // would retain callbacks/provider state and could form a publication cycle.
        let anchors = RepositoryContext::new_with_state(
            None,
            resources.immutable.clone(),
            resources.mutable.clone(),
            location.repository(),
            location.instance(),
            RemoteState::Offline,
            Arc::default(),
            Some(crate::fs::invalid_filesystem_provider::InvalidFilesystemProvider::new()),
        );
        let resources = resources.clone();
        let repository_lock = self.repo_lock.clone();
        let store_hold = self.own_stores.clone();
        let token = token.share();
        let parent = IoLease::current();
        let exclusion = issuer.exclude_command().map_err(admission_error)?;
        Ok(Arc::new(WorkingCommandHandoff {
            anchors,
            _resources: resources,
            _repository_lock: repository_lock,
            _store_hold: store_hold,
            _token: token,
            exclusion,
            _parent: parent,
        }))
    }
}
impl WorkingCommandHandoff {
    fn io_lease(self: &Arc<Self>) -> IoLease {
        IoLease::new(Arc::new(HandoffIo {
            _owner: self.clone(),
            _parent: IoLease::current(),
        }))
    }

    /// Withdraw exact READY routing without waiting for this command's own
    /// permits or service work to drain. Terminal cleanup runs after the worker.
    pub fn quarantine(&self) -> Result<(), RepositoryError> {
        self.exclusion.quarantine().map_err(admission_error)
    }

    /// Publish only the exact tuple named by a trusted durable provider receipt.
    /// The external pending worker retains its source seal and all images until
    /// actual completion. Cancellation fences routing before releasing this pin.
    pub async fn publish_anchors(
        self: &Arc<Self>,
        receipt: &dyn crate::fs::filesystem_provider::DurableRootIntent,
    ) -> Result<strict_anchors::Snapshot, RepositoryError> {
        let old = receipt.expected_anchors();
        let new = receipt.replacement_anchors();
        if old.repository != self.anchors.id
            || old.instance != self.anchors.instance_id
            || new.repository != old.repository
            || new.instance != old.instance
            || new.branch.is_zero()
            || new.staged.is_some_and(|revision| revision.is_zero())
            || old == new
            || self.read_anchors().await? != old
        {
            return Err(refused());
        }
        let mut fence = AnchorFence {
            owner: self.clone(),
            armed: true,
        };
        let owner = self.clone();
        let written = owner
            .io_lease()
            .scope(async move {
                let store = owner.anchors.write_mutable_store(&owner._token);
                for (name, value) in [
                    (
                        crate::instance::ANCHOR_CURRENT_BRANCH,
                        lore_base::types::Hash::from_context(new.branch),
                    ),
                    (crate::instance::ANCHOR_CURRENT, new.current),
                    (
                        crate::instance::ANCHOR_STAGED,
                        new.staged.unwrap_or_default(),
                    ),
                ] {
                    let (key, kind) =
                        crate::instance::anchor_key(owner.anchors.salt(), name, new.instance);
                    store
                        .store(new.repository, key, value, kind)
                        .await
                        .map_err(|error| RepositoryError::internal(error.to_string()))?;
                }
                store
                    .flush(true)
                    .await
                    .map_err(|error| RepositoryError::internal(error.to_string()))?;
                let actual = owner.read_anchors().await?;
                if actual != new {
                    return Err(RepositoryError::internal(
                        "Durable working anchors do not match intent",
                    ));
                }
                Ok(actual)
            })
            .await?;
        fence.armed = false;
        Ok(written)
    }
    /// Inspect only the immutable root repository/instance identity captured by
    /// the loaded registration. Read errors never select a recovery image.
    pub async fn read_anchors(
        self: &Arc<Self>,
    ) -> Result<strict_anchors::Snapshot, RepositoryError> {
        self.exclusion.check().map_err(admission_error)?;
        let owner = self.clone();
        owner
            .io_lease()
            .scope(async move {
                let snapshot = strict_anchors::read(&owner.anchors)
                    .await
                    .map_err(|error| RepositoryError::internal(error.to_string()))?;
                owner.exclusion.check().map_err(admission_error)?;
                Ok(snapshot)
            })
            .await
    }
}
struct AnchorFence {
    owner: Arc<WorkingCommandHandoff>,
    armed: bool,
}
impl Drop for AnchorFence {
    fn drop(&mut self) {
        if self.armed && self.owner.quarantine().is_err() {
            // A failed exact withdrawal cannot permit normal writes to resume.
            // Retain the concrete Client/store/exclusion owner until process exit.
            let _retained = Arc::into_raw(self.owner.clone());
        }
    }
}
fn refused() -> RepositoryError {
    RepositoryError::internal(
        "Working command handoff requires the exact loaded root and its canonical Client token",
    )
}
fn admission_error(error: crate::fs::provider_registry::Error) -> RepositoryError {
    RepositoryError::internal(format!("Working command handoff refused: {error}"))
}

#[cfg(test)]
mod tests;

struct HandoffIo {
    _owner: Arc<WorkingCommandHandoff>,
    _parent: Option<IoLease>,
}
