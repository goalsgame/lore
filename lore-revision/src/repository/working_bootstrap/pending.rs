// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact pending metadata work. This does not qualify recovery or a mounted owner.
mod publication;
pub use publication::DeferredPublicationCleanup;
pub use publication::OwnerBuildBusy;
pub use publication::OwnerBuildPayload;
pub use publication::PendingPublication;
pub use publication::PreparedPendingPublication;

mod stores;
use lore_io::lease::IoLease;
pub use stores::PendingWorkingContext;

use super::*;
use crate::fs::provider_registry::PendingQualification;
use crate::fs::provider_registry::qualify_pending_metadata;

/// An admitted metadata result retains exclusion until terminal cleanup. It
/// exposes facts only; it cannot publish a provider or enter ordinary store setup.
pub struct PendingMetadata {
    metadata: PreparedMetadata,
    _owner: Arc<PendingIoOwner>,
}
impl PendingMetadata {
    pub fn location(&self) -> &WorkingLocation {
        self.metadata.location()
    }
    pub fn config(&self) -> &RepositoryConfig {
        self.metadata.config()
    }
}
/// Before qualification, rejection returns the caller's actual token ownership.
/// After qualification every failure terminally retires that exact registration.
pub enum PendingError {
    Rejected {
        error: Error,
        registration: Arc<Registration>,
        token: RepositoryWriteToken,
    },
    Retired(Error),
}
impl std::fmt::Debug for PendingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { error, .. } => f.debug_tuple("Rejected").field(error).finish(),
            Self::Retired(error) => f.debug_tuple("Retired").field(error).finish(),
        }
    }
}
impl std::fmt::Display for PendingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected { error, .. } => write!(formatter, "pending metadata rejected: {error}"),
            Self::Retired(error) => write!(formatter, "pending metadata retired: {error}"),
        }
    }
}
impl std::error::Error for PendingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected { error, .. } | Self::Retired(error) => Some(error),
        }
    }
}
struct Retire {
    registration: Arc<Registration>,
    armed: bool,
}
impl Drop for Retire {
    fn drop(&mut self) {
        if self.armed {
            let _retired = self.registration.retire();
        }
    }
}
struct PendingIoOwner {
    // Withdraw pending admission before releasing either exclusion or Client.
    _retire: Retire,
    _qualification: PendingQualification,
    token: Option<RepositoryWriteToken>,
    _parent: Option<IoLease>,
}
impl MetadataInitializer {
    /// Consumes an unshared canonical Client token before counting admission.
    /// The caller must already own that token; this method never reacquires it.
    /// Generic `prepare` remains independent of this pending-entry protocol.
    pub async fn prepare_pending(
        &self,
        registration: Arc<Registration>,
        token: RepositoryWriteToken,
        mode: Mode,
    ) -> std::result::Result<PendingMetadata, PendingError> {
        let location = match registration.bootstrap_location() {
            Ok(location) => location,
            Err(error) => {
                return Err(PendingError::Rejected {
                    error: error.into(),
                    registration,
                    token,
                });
            }
        };
        let token = match unique_client(token, location.target().path()) {
            Ok(token) => token,
            Err((error, token)) => {
                return Err(PendingError::Rejected {
                    error,
                    registration,
                    token,
                });
            }
        };
        let qualification = match qualify_pending_metadata(&registration, &location) {
            Ok(value) => value,
            Err(error) => {
                return Err(PendingError::Rejected {
                    error: error.into(),
                    registration,
                    token,
                });
            }
        };
        let owner = Arc::new(PendingIoOwner {
            _retire: Retire {
                registration: registration.clone(),
                armed: true,
            },
            _qualification: qualification,
            token: Some(token),
            _parent: IoLease::current(),
        });
        // Only the qualified owner can mint this child token. The result's
        // parent lease keeps exclusion alive through actual queued/running IO.
        let child = owner
            .token
            .as_ref()
            .ok_or(PendingError::Retired(Error::Runtime))?
            .share();
        let scope = IoLease::new(owner.clone());
        let result = scope.scope(self.prepare(registration, child, mode)).await;
        match result {
            Ok(metadata) => Ok(PendingMetadata {
                metadata,
                _owner: owner,
            }),
            Err(error) => Err(PendingError::Retired(error)),
        }
    }
}
fn unique_client(
    token: RepositoryWriteToken,
    path: &std::path::Path,
) -> std::result::Result<RepositoryWriteToken, (Error, RepositoryWriteToken)> {
    let RepositoryWriteToken::Client(guard) = token else {
        return Err((Error::InvalidToken, token));
    };
    if !Arc::ptr_eq(
        tokio::sync::OwnedMutexGuard::mutex(guard.as_ref()),
        &super::super::write_token::write_mutex_for_path(path),
    ) {
        return Err((Error::InvalidToken, RepositoryWriteToken::Client(guard)));
    }
    match Arc::try_unwrap(guard) {
        Ok(guard) => Ok(RepositoryWriteToken::Client(Arc::new(guard))),
        Err(guard) => Err((Error::Busy, RepositoryWriteToken::Client(guard))),
    }
}
