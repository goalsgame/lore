// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Actual metadata-job resources precede their inherited IO owner on every exit.
use lore_io::lease::IoLease;
use tokio::sync::OwnedSemaphorePermit;

use super::*;

pub(super) struct MetadataJob {
    registration: Arc<Registration>,
    token: RepositoryWriteToken,
    location: WorkingLocation,
    mode: Option<Mode>,
    _permit: OwnedSemaphorePermit,
    #[cfg(feature = "test-util")]
    pause: Option<MetadataPause>,
    // This may own service work accounting: release it after job capabilities.
    parent: Option<IoLease>,
}
impl MetadataJob {
    pub(super) fn new(
        registration: Arc<Registration>,
        token: RepositoryWriteToken,
        location: WorkingLocation,
        mode: Mode,
        permit: OwnedSemaphorePermit,
        parent: Option<IoLease>,
    ) -> Self {
        Self {
            registration,
            token,
            location,
            mode: Some(mode),
            _permit: permit,
            #[cfg(feature = "test-util")]
            pause: None,
            parent,
        }
    }
    #[cfg(feature = "test-util")]
    pub(super) fn with_pause(mut self, pause: Option<MetadataPause>) -> Self {
        self.pause = pause;
        self
    }
    pub(super) fn run(mut self) -> Result<PreparedMetadata> {
        let mode = self.mode.take().ok_or(Error::Runtime)?;
        #[cfg(feature = "test-util")]
        let pause = self.pause.take();
        let config = lore_io::lease::sync_scope(self.parent.clone(), || {
            #[cfg(feature = "test-util")]
            if let Some(pause) = pause {
                pause();
            }
            initialize(&self.registration, &self.location, mode)
        })?;
        Ok(PreparedMetadata {
            registration: self.registration,
            token: self.token,
            location: self.location,
            config,
            // Move the inherited ownership without a release/reacquire window.
            parent: self.parent,
        })
    }
}
