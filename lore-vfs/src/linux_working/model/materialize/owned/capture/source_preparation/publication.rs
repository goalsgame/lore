// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Typed source publication and post-join checks. No intent or activation right.

mod install;

use super::*;
use crate::checkpoint_owner::CheckpointOwner;
use crate::recovery::published::PublishedCheckpointImage;
use crate::transition_prepare::SourceFacts;

pub(crate) struct SourceContinuation {
    facts: SourceFacts,
    authorities: SourceAuthorities,
    _seal: MetadataSeal,
    _serial: OwnedMutexGuard<()>,
    captured: CapturedCandidate,
}
impl OwnedSourcePreparation {
    pub(crate) async fn publish_source(self) -> PreparedResult<SourceAcknowledgment> {
        if self.source.intent().dry_run() {
            return Err(crate::transition_prepare::Error::Unsupported(
                "dry-run source publication",
            )
            .into());
        }
        let owner = self.captured.candidate.admission.owner.checkpoint.clone();
        let joined = owner.publish_owned_source(self).await?;
        joined.acknowledge().await
    }
    pub(crate) async fn validate_for_publication(&self) -> PreparedResult<()> {
        validate(&self.captured, self.source.facts(), &self.authorities).await
    }
    pub(crate) fn check_publication_owner(
        &self,
        owner: &Arc<CheckpointOwner>,
    ) -> crate::checkpoint_owner::Result<()> {
        check_owner(&self.captured, self.source.facts(), owner)
    }
    pub(crate) fn into_publication_parts(self) -> (NamespaceSnapshot, SourceContinuation) {
        let Self {
            source,
            authorities,
            _seal,
            _serial,
            captured,
        } = self;
        let (snapshot, facts) = source.into_parts();
        (
            snapshot,
            SourceContinuation {
                facts,
                authorities,
                _seal,
                _serial,
                captured,
            },
        )
    }
}
impl SourceContinuation {
    pub(crate) fn check_transition_owner(
        &self,
        owner: &Arc<CheckpointOwner>,
    ) -> crate::checkpoint_owner::Result<()> {
        check_owner(&self.captured, &self.facts, owner)
    }

    pub(crate) fn artifact_parts(
        &mut self,
    ) -> (
        &SourceFacts,
        CapturedCheckpointImage<'_>,
        &mut CaptureMemory,
    ) {
        let (candidate, memory) = self.captured.artifact_parts();
        (&self.facts, candidate, memory)
    }

    pub(crate) fn memory_mut(&mut self) -> &mut CaptureMemory {
        &mut self.captured.memory
    }
    pub(crate) async fn validate_joined(
        &self,
        source: &PublishedCheckpointImage,
    ) -> PreparedResult<()> {
        validate(&self.captured, &self.facts, &self.authorities).await?;
        let witness = self.facts.witness();
        if source.snapshot().original_root != *self.facts.original_root()
            || source.snapshot().generation != witness.generation
            || source.snapshot().next_inode != witness.next_inode
        {
            return Err(core::Error::Stale.into());
        }
        Ok(())
    }
}
fn check_owner(
    captured: &CapturedCandidate,
    facts: &SourceFacts,
    checkpoint: &Arc<CheckpointOwner>,
) -> crate::checkpoint_owner::Result<()> {
    use crate::checkpoint_owner::Error as PublicationError;
    if facts.intent().dry_run() {
        return Err(PublicationError::Core(core::Error::Unsupported));
    }
    let model = &captured.candidate.admission.owner;
    if !Arc::ptr_eq(&model.checkpoint, checkpoint)
        || !Arc::ptr_eq(checkpoint.namespace(), &model.namespace)
    {
        return Err(PublicationError::WrongNamespace);
    }
    if *model.closed.borrow() {
        return Err(PublicationError::Core(core::Error::Unmounted));
    }
    if !facts.freeze().valid() {
        return Err(PublicationError::Core(core::Error::Stale));
    }
    facts
        .routing()
        .check(&model.namespace, facts.freeze().token())?;
    let ticket = model
        .namespace
        .check_working_owner(&model.ownership)
        .map_err(PublicationError::Core)?;
    checkpoint.check_publication_owner(&model.ownership)?;
    checkpoint.available()?;
    model
        .namespace
        .confirm_working_owner_ticket(&ticket)
        .map_err(PublicationError::Core)
}
async fn validate(
    captured: &CapturedCandidate,
    facts: &SourceFacts,
    authorities: &SourceAuthorities,
) -> PreparedResult<()> {
    let owner = &captured.candidate.admission.owner;
    owner.live()?;
    let mut closed = owner.closed.subscribe();
    owner.live()?;
    let check = async {
        let names = owner.names.lock().await;
        owner.live()?;
        captured.candidate.metadata.check_source(owner)?;
        captured.candidate.state.confirms_source_facts(facts)?;
        captured.candidate.state.validate_source().await?;
        drop(names);
        authorities.authorize().await?;
        let _names = owner.names.lock().await;
        owner.live()?;
        captured.candidate.metadata.check_source(owner)?;
        captured.candidate.state.confirms_source_facts(facts)?;
        captured.candidate.state.validate_source().await?;
        owner.live()?;
        Ok::<_, PreparationError>(())
    };
    tokio::select! { biased;
        _=closed.changed()=>Err(core::Error::Unmounted.into()),
        result=check=>result,
    }
}

#[cfg(test)]
impl SourceContinuation {
    pub(crate) fn test_intent(&self) -> &crate::transition_prepare::IntentSpec {
        self.facts.intent()
    }
}
