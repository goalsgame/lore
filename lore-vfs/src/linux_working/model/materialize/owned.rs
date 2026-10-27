// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Fully owned, uninstalled state. No checkpoint serialization or anchor ownership.
mod capture;
pub(crate) use capture::CapturedCheckpointImage;
pub(crate) use capture::OwnedSourcePreparation;
pub(crate) use capture::SourceContinuation;
mod install;
mod metadata;
use metadata::PreparedModelMetadata;
mod successor;

use super::*;
use crate::namespace::unpublished::PreparedRecordFile;
use crate::namespace::unpublished::UnpublishedState;

pub(crate) struct UnpublishedCandidate {
    pub(super) state: UnpublishedState,
    pub(super) files: BTreeMap<u64, FileCell>,
    metadata: PreparedModelMetadata,
    // Keep exact Model ownership/admission through all resource destruction.
    admission: Admission,
}
impl PreparedPrivateResources<'_> {
    pub(crate) async fn into_owned_candidate(self) -> PreparedResult<UnpublishedCandidate> {
        let PreparedPrivateResources {
            layout,
            view,
            files,
            admission,
            mut budget,
        } = self;
        let owner = &admission.owner;
        owner.live()?;
        let mut closed = owner.closed.subscribe();
        owner.live()?;
        let prepare = async {
            let _names = owner.names.lock().await;
            owner.live()?;
            let witness = owner.model_metadata_witness()?;
            layout.validate_source().await?;
            let existing = owner.files.lock().await;
            layout.check_namespace(&owner.namespace)?;
            for _ in existing.values() {
                budget.node()?;
            }
            let mut cells = existing.clone();
            drop(existing);
            layout.validate_source().await?;
            let mut records = BTreeMap::new();
            for (inode, prepared) in files {
                budget.node()?;
                budget.node()?;
                records.insert(
                    inode,
                    PreparedRecordFile {
                        overlay: prepared.file.overlay.clone(),
                        receipt: prepared.receipt,
                    },
                );
                if cells
                    .insert(inode, Arc::new(AsyncMutex::new(Some(prepared.file))))
                    .is_some()
                {
                    return Err(PreparationError::from(core::Error::InvalidInput));
                }
            }
            let recipe = layout.into_owned(&mut budget)?;
            let state = owner
                .namespace
                .prepare_owned_state(recipe, view, records, &mut budget)
                .await?;
            let metadata = PreparedModelMetadata::prepare(owner, &state, witness, &mut budget)?;
            state.validate_source().await?;
            owner.live()?;
            metadata.check_source(owner)?;
            Ok::<_, PreparationError>((state, cells, metadata))
        };
        let (state, files, metadata) = tokio::select! { biased;
            _=closed.changed()=>return Err(core::Error::Unmounted.into()),
            result=prepare=>result?,
        };
        owner.live()?;
        Ok(UnpublishedCandidate {
            state,
            files,
            metadata,
            admission,
        })
    }
}
