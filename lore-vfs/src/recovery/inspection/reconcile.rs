// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Startup selection uses the opaque pending owner's actual strict stores, not
//! a caller-supplied tuple or an inferred latest revision.
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use super::artifacts::Artifact;
use super::artifacts::TransitionFacts;
use super::*;
use crate::recovery::Recovery;
use crate::recovery::intent::Intent;
use crate::recovery::intent::Selection;
use crate::recovery::transition_completion;
use crate::working_storage::WorkingStorage;

enum Cleanup {
    Missing,
    Malformed,
    Checked(Box<Intent>),
}
pub(crate) struct ReconciliationPlan {
    cleanup: Cleanup,
    facts: Option<TransitionFacts>,
    committed: crate::recovery::LoadedCheckpoint,
    record: Intent,
    selected: crate::recovery::intent::CheckpointIdentity,
}
impl RecoveryInspection {
    pub(crate) fn check_directory_identity(&self, current: &std::fs::Metadata) -> Result<()> {
        let held = self.directory.metadata()?;
        if (held.dev(), held.ino()) != (current.dev(), current.ino()) {
            return Err(Error::PrivateStorageRequired);
        }
        let current = super::open_file(&self.directory, c"owner.lock")?;
        let current = current.metadata()?;
        super::super::private_metadata(&current, false)?;
        let held = self._lock.metadata()?;
        if (held.dev(), held.ino()) != (current.dev(), current.ino()) {
            return Err(Error::PrivateStorageRequired);
        }
        Ok(())
    }
    pub(crate) async fn prepare_reconciliation(
        &self,
        storage: &Arc<WorkingStorage>,
        payload: &mut lore_revision::repository::working_bootstrap::OwnerBuildPayload,
        memory: &mut CaptureMemory,
    ) -> Result<ReconciliationPlan> {
        storage.verify_owner().map_err(failure)?;
        let location = payload.location();
        if storage.repository() != location.repository()
            || storage.instance() != location.instance()
        {
            return Err(Error::Corrupt);
        }
        let binding = lore_revision::repository::working_bootstrap::BackingBinding::read(
            location.metadata().path(),
            location.repository(),
            location.instance(),
        )
        .map_err(failure)?;
        if binding.root() != storage.root() {
            return Err(Error::PrivateStorageRequired);
        }
        let path = storage.root().join("recovery");
        let current = std::fs::symlink_metadata(&path)?;
        super::super::private_metadata(&current, true)?;
        self.check_directory_identity(&current)?;
        let cleanup = cleanup_marker(&self.directory, self.limit, memory)?;
        let facts = self.load_transition_budgeted(memory)?;
        let full = facts
            .as_ref()
            .is_some_and(|facts| facts.check_image_relation().is_ok());
        let record = match &cleanup {
            Cleanup::Missing | Cleanup::Malformed => {
                if !full {
                    return Err(Error::Corrupt);
                }
                let Artifact::Checked(record) = facts.as_ref().unwrap().intent() else {
                    return Err(Error::Corrupt);
                };
                *record.parsed()
            }
            Cleanup::Checked(record) => **record,
        };
        check_remaining(facts.as_ref(), &record)?;
        let anchors = payload.read_anchors().await.map_err(failure)?;
        let selected = match record
            .classify(anchors, self.limit)
            .map_err(|_tuple| Error::Corrupt)?
        {
            Selection::Source => record.source,
            Selection::Candidate => record.candidate,
            Selection::Unresolved => return Err(Error::StaleSnapshot),
        };
        let committed = self
            .load_committed_budgeted(memory)?
            .ok_or(Error::StaleSnapshot)?;
        if ![record.source, record.candidate].contains(&committed.identity())
            || committed.snapshot().original_root.key != record.original_root
        {
            return Err(Error::StaleSnapshot);
        }
        let source = facts.as_ref().and_then(|facts| match facts.source() {
            Artifact::Checked(value) => Some(value),
            _ => None,
        });
        let candidate = facts.as_ref().and_then(|facts| match facts.candidate() {
            Artifact::Checked(value) => Some(value),
            _ => None,
        });
        for image in [source, candidate].into_iter().flatten() {
            if image.snapshot().original_root != committed.snapshot().original_root {
                return Err(Error::Corrupt);
            }
        }
        let _chosen = if selected == record.source {
            source
        } else {
            candidate
        }
        .or_else(|| (committed.identity() == selected).then_some(&committed))
        .ok_or(Error::Corrupt)?;
        Ok(ReconciliationPlan {
            cleanup,
            facts,
            committed,
            record,
            selected,
        })
    }
    pub(crate) fn finish_reconciliation(
        self,
        storage: &Arc<WorkingStorage>,
        plan: ReconciliationPlan,
        memory: &mut CaptureMemory,
    ) -> Result<Recovery> {
        let ReconciliationPlan {
            cleanup,
            facts,
            committed,
            record,
            selected,
        } = plan;
        let source = facts.as_ref().and_then(|facts| match facts.source() {
            Artifact::Checked(value) => Some(value),
            _ => None,
        });
        let candidate = facts.as_ref().and_then(|facts| match facts.candidate() {
            Artifact::Checked(value) => Some(value),
            _ => None,
        });
        let chosen = if selected == record.source {
            source
        } else {
            candidate
        }
        .or_else(|| (committed.identity() == selected).then_some(&committed))
        .ok_or(Error::Corrupt)?;
        let path = storage.recovery_directory().to_path_buf();
        let mut recovery = Recovery {
            directory: path,
            _lock: self._lock,
            directory_file: self.directory,
            limits: super::super::Limits {
                max_checkpoint_bytes: self.limit,
            },
            uncertain: false,
            publication_pending: false,
            #[cfg(test)]
            fault: None,
        };
        // Unknown or inconsistent pending bytes are preserved. Only prefixes of
        // independently checked source/candidate images are retryable here.
        transition_completion::discard_known_pending(
            &mut recovery,
            source.map_or(chosen.bytes(), |image| image.bytes()),
            candidate.map_or(chosen.bytes(), |image| image.bytes()),
            memory,
        )?;
        transition_completion::publish_selected(
            &mut recovery,
            chosen.snapshot(),
            chosen.bytes(),
            selected,
            &record,
            memory,
        )?;
        match cleanup {
            Cleanup::Checked(_) => transition_completion::cleanup_marked(&mut recovery)?,
            Cleanup::Missing => transition_completion::cleanup(&mut recovery, &record, memory)?,
            Cleanup::Malformed => {
                // This is fixed protocol metadata, not an image. The full
                // original bundle and actual anchor choice were proven above.
                transition_completion::discard_partial_cleanup(&mut recovery)?;
                transition_completion::cleanup(&mut recovery, &record, memory)?;
            }
        }
        storage.verify_owner().map_err(failure)?;
        recovery.verify_owner()?;
        Ok(recovery)
    }
}
fn cleanup_marker(directory: &File, limit: usize, memory: &mut CaptureMemory) -> Result<Cleanup> {
    let mut file = match transition_completion::read_file(directory, c"transition.cleanup") {
        Ok(file) => file,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Cleanup::Missing);
        }
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() != crate::recovery::intent::ENCODED_SIZE as u64 {
        return Ok(Cleanup::Malformed);
    }
    memory
        .buffer(crate::recovery::intent::ENCODED_SIZE)
        .map_err(|_limit| Error::ResourceLimit)?;
    let mut bytes = [0; crate::recovery::intent::ENCODED_SIZE];
    if let Err(error) = file.read_exact(&mut bytes) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(Cleanup::Malformed)
        } else {
            Err(error.into())
        };
    }
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Ok(Cleanup::Malformed);
    }
    match Intent::decode(&bytes, limit) {
        Ok(record) => {
            memory
                .buffer(std::mem::size_of::<Intent>())
                .map_err(|_limit| Error::ResourceLimit)?;
            Ok(Cleanup::Checked(Box::new(record)))
        }
        Err(crate::recovery::intent::Error::Corrupt) => Ok(Cleanup::Malformed),
        Err(crate::recovery::intent::Error::Incompatible) => Err(Error::Incompatible),
        Err(crate::recovery::intent::Error::ResourceLimit) => Err(Error::ResourceLimit),
        Err(_invalid) => Err(Error::Corrupt),
    }
}
fn check_remaining(facts: Option<&TransitionFacts>, record: &Intent) -> Result<()> {
    let Some(facts) = facts else {
        return Ok(());
    };
    if facts.inventory().unknown_or_duplicate() || facts.inventory().excess() {
        return Err(Error::Corrupt);
    }
    for (artifact, expected) in [
        (facts.source(), record.source),
        (facts.candidate(), record.candidate),
    ] {
        match artifact {
            Artifact::Missing => {}
            Artifact::Checked(image)
                if image.identity() == expected
                    && image.snapshot().original_root.key == record.original_root => {}
            _ => return Err(Error::Corrupt),
        }
    }
    for artifact in [facts.intent(), facts.pending_intent()] {
        match artifact {
            Artifact::Missing => {}
            Artifact::Checked(fact) if fact.parsed() == record => {}
            _ => return Err(Error::Corrupt),
        }
    }
    Ok(())
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(error.to_string()))
}

#[cfg(test)]
mod tests;
