// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Movable source preparation with an explicit retained-publication boundary.
mod publication;
pub(crate) use publication::SourceContinuation;
use tokio::sync::OwnedMutexGuard;

use super::*;
pub(crate) use crate::checkpoint_owner::SourceAcknowledgment;
use crate::namespace::MetadataSeal;
use crate::namespace::source_authorities::SourceAuthorities;
use crate::transition_prepare::OwnedSourceImage;
use crate::transition_prepare::PreparedTransition;

pub(crate) struct OwnedSourcePreparation {
    source: OwnedSourceImage,
    authorities: SourceAuthorities,
    _seal: MetadataSeal,
    _serial: OwnedMutexGuard<()>,
    // Retain the original request/Model claim until every other field drops.
    captured: CapturedCandidate,
}
impl CapturedCandidate {
    pub(crate) async fn prepare_source(
        mut self,
        plan: PreparedTransition,
    ) -> PreparedResult<OwnedSourcePreparation> {
        let source = plan.into_owned_source()?;
        if source.intent().dry_run() {
            return Err(crate::transition_prepare::Error::Unsupported(
                "dry-run source preparation",
            )
            .into());
        }
        self.candidate.state.confirms_source_image(&source)?;
        let owner = self.candidate.admission.owner.clone();
        owner.live()?;
        let mut closed = owner.closed.subscribe();
        owner.live()?;
        let prepare = async {
            let serial = owner
                .checkpoint
                .lock_source_preparation()
                .await
                .map_err(super::super::super::super::Error::from)?;
            let names = owner.names.lock().await;
            owner.live()?;
            self.candidate.metadata.check_source(&owner)?;
            self.candidate.state.confirms_source_image(&source)?;
            self.candidate.state.validate_source().await?;
            let authorities = owner
                .namespace
                .capture_source_authorities(&source, &mut self.memory)
                .await?;
            drop(names);
            authorities.authorize().await?;
            let names = owner.names.lock().await;
            owner.live()?;
            self.candidate.metadata.check_source(&owner)?;
            self.candidate.state.confirms_source_image(&source)?;
            self.candidate.state.validate_source().await?;
            let seal = owner
                .namespace
                .seal_source_authorities(&source, &authorities)
                .await?;
            self.candidate.metadata.check_source(&owner)?;
            self.candidate.state.validate_source().await?;
            owner.live()?;
            drop(names);
            Ok::<_, PreparationError>((serial, authorities, seal))
        };
        let (serial, authorities, seal) = tokio::select! { biased;
            _=closed.changed()=>return Err(core::Error::Unmounted.into()),
            result=prepare=>result?,
        };
        owner.live()?;
        Ok(OwnedSourcePreparation {
            source,
            authorities,
            _seal: seal,
            _serial: serial,
            captured: self,
        })
    }
}

#[cfg(test)]
impl OwnedSourcePreparation {
    pub(crate) fn test_source(&self) -> &crate::namespace::NamespaceSnapshot {
        self.source.snapshot()
    }
    pub(crate) fn test_intent(&self) -> &crate::transition_prepare::IntentSpec {
        self.source.intent()
    }
    pub(crate) async fn test_authorize(&self) -> core::Result<()> {
        self.authorities.authorize().await
    }
}
