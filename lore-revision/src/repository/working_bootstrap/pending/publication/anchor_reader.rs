// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Actual startup anchor reads while the opaque pending owner still holds its
//! canonical Client, exclusion and exact initialized stores.
use super::*;
use crate::instance::strict_anchors;

impl OwnerBuildPayload {
    pub fn location(&self) -> &WorkingLocation {
        &self.context.location
    }
    pub async fn read_anchors(&mut self) -> std::result::Result<strict_anchors::Snapshot, Error> {
        self.check_anchor_reader()?;
        let lease = self.publication.io_lease()?;
        let result = lease
            .scope(
                crate::runtime::LORE_CONTEXT.scope(self.context.execution.clone(), async {
                    strict_anchors::read(&self.context.repository).await
                }),
            )
            .await;
        drop(lease);
        // Completed reads cannot leave an escaped setup job owning Client or
        // backing IO before a recovery claim is transferred.
        self.publication.settle_anchor_reads()?;
        self.check_anchor_reader()?;
        let snapshot = result.map_err(|error| Error::Preparation(error.to_string()))?;
        if snapshot.repository != self.context.location.repository()
            || snapshot.instance != self.context.location.instance()
        {
            return Err(Error::InvalidBinding);
        }
        Ok(snapshot)
    }
    fn check_anchor_reader(&self) -> std::result::Result<(), Error> {
        if self.publication.failed
            || self.publication.preflighted
            || self.publication.provider.is_some()
            || self.context.location != self.publication.location
            || self.context.repository.id != self.context.location.repository()
            || self.context.repository.instance_id != self.context.location.instance()
        {
            return Err(Error::InvalidBinding);
        }
        let PublicationState::Shared(owner) = &self.publication.state else {
            return Err(Error::Runtime);
        };
        if !matches!(owner.token.as_ref(), Some(RepositoryWriteToken::Client(_)))
            || !self
                .context
                .registration
                .same_reservation(&owner._retire.registration)
        {
            return Err(Error::InvalidToken);
        }
        self.context
            .registration
            .check_pending_location(&self.context.location)?;
        Ok(())
    }
}
impl PendingPublication {
    fn settle_anchor_reads(&mut self) -> std::result::Result<(), Error> {
        let state = std::mem::replace(&mut self.state, PublicationState::Spent);
        match state {
            PublicationState::Shared(owner) => match Arc::try_unwrap(owner) {
                Ok(owner) => {
                    self.state = PublicationState::Shared(Arc::new(owner));
                    Ok(())
                }
                Err(owner) => {
                    self.state = PublicationState::Shared(owner);
                    Err(Error::Busy)
                }
            },
            state => {
                self.state = state;
                Err(Error::Runtime)
            }
        }
    }
}
