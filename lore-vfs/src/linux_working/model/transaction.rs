// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Actual root-hold preparation against this exact running Model.

use lore_revision::fs::filesystem_provider::FsError;
use lore_revision::fs::filesystem_provider::RootHoldOutcome;
use lore_revision::fs::filesystem_provider::RootHoldRequest;

use super::*;
use crate::capture_memory::CaptureLimits;

impl Model {
    pub(crate) async fn transactional_root_hold(
        self: &Arc<Self>,
        request: RootHoldRequest,
    ) -> std::result::Result<RootHoldOutcome, FsError> {
        self.live().map_err(problem)?;
        let original = self.namespace.backend().map_err(problem)?;
        let backend = original
            .as_any()
            .and_then(|backend| backend.downcast_ref::<crate::revision::RevisionBackend>())
            .ok_or_else(|| FsError::internal("Transactional root requires a revision backend"))?;
        let root = backend.root();
        if root.key.repository != request.old_anchors().repository {
            return Err(FsError::internal(
                "Working command root does not match mounted backend",
            ));
        }
        let options = request.options();
        let limits = crate::transition_prepare::Limits::default();
        let plan = crate::transition_prepare::prepare(
            self.namespace.clone(),
            root.key,
            lore_base::types::Address {
                hash: request.target(),
                context: root.key.revision.context,
            },
            crate::transition_prepare::Options {
                local: if options.overwrite_local {
                    crate::transition_prepare::LocalPolicy::OverwriteLocal
                } else if options.keep_local {
                    crate::transition_prepare::LocalPolicy::KeepLocal
                } else {
                    crate::transition_prepare::LocalPolicy::RejectConflicts
                },
                dry_run: options.dry_run,
                ..Default::default()
            },
            limits,
        )
        .await
        .map_err(|error| match error {
            crate::transition_prepare::Error::Conflicts(_) => {
                FsError::from(lore_base::error::LocalModifications)
            }
            other => problem(other),
        })?;
        let source_head = plan
            .intent()
            .expected_baseline()
            .map_or(plan.intent().scope().revision, |baseline| {
                baseline.head.revision
            });
        if source_head.hash != request.old_anchors().current {
            return Err(FsError::internal(
                "Working command anchors do not name the exact held root baseline",
            ));
        }
        if options.dry_run {
            return Ok(RootHoldOutcome::Preview);
        }
        if request.old_anchors() == request.new_anchors() {
            if plan.changes().is_empty() {
                return Ok(RootHoldOutcome::Unchanged);
            }
            return Err(FsError::internal(
                "A changed working view with unchanged anchors is unsupported",
            ));
        }
        let prepared = plan.prepare_complete_backend().await.map_err(problem)?;
        let binding = self
            .namespace
            .bind_complete_backend(prepared)
            .map_err(problem)?;
        let layout = plan
            .prepare_install_layout(binding)
            .await
            .map_err(problem)?;
        let candidate = self
            .prepare_private_resources(layout)
            .await
            .map_err(problem)?
            .into_owned_candidate()
            .await
            .map_err(problem)?;
        let reservation = candidate.reserve_successor(self).await.map_err(problem)?;
        let maximum = self.checkpoint.capture_byte_limit();
        let captured = candidate
            .capture_checkpoint(
                None,
                CaptureLimits {
                    max_buffer_bytes: maximum
                        .checked_mul(6)
                        .ok_or_else(|| FsError::internal("Transition buffer limit overflow"))?,
                    max_metadata_nodes: limits
                        .max_nodes
                        .checked_mul(8)
                        .ok_or_else(|| FsError::internal("Transition node limit overflow"))?,
                    max_name_bytes: limits
                        .max_name_bytes
                        .checked_mul(8)
                        .ok_or_else(|| FsError::internal("Transition name limit overflow"))?,
                },
            )
            .await
            .map_err(problem)?;
        let source = captured.prepare_source(plan).await.map_err(problem)?;
        backend
            .flush_transition_dependencies()
            .await
            .map_err(problem)?;
        let ack = source.publish_source().await.map_err(problem)?;
        self.checkpoint
            .publish_root_transition(ack, request, reservation, backend.captured_execution())
            .await?;
        Ok(RootHoldOutcome::Applied)
    }
}
fn problem(error: impl std::fmt::Debug) -> FsError {
    FsError::internal(format!("Preparing working root hold: {error:?}"))
}
