// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Root-only transactional dispatch. No eager whole-file caching or legacy freeze.

use super::*;
use crate::fs::filesystem_provider::HoldOptions;
use crate::fs::filesystem_provider::RootHoldOutcome;
use crate::fs::filesystem_provider::RootHoldRequest;

pub(super) async fn hold(
    repository: &Arc<RepositoryContext>,
    state_target: &Arc<State>,
    options: &SyncOptions,
    requested_branch: Option<BranchId>,
    anchor_branch: BranchId,
    view_change: bool,
) -> Result<RootHoldOutcome, SyncError> {
    if view_change || options.reset || !options.root_files.is_empty() {
        return Err(FsError::internal(
            "Transactional working holds require one unchanged root view",
        )
        .into());
    }
    let revision = state_target.revision();
    let desired_branch = match requested_branch.filter(|branch| !branch.is_zero()) {
        Some(branch) => branch,
        None => state_target
            .revision_metadata(repository.clone())
            .await
            .ok()
            .map(|metadata| metadata.branch)
            .filter(|branch| !branch.is_zero())
            .unwrap_or(anchor_branch),
    };
    // Preserve the legacy branch-point rule: a revision from the branch's
    // parent does not by itself move the instance off its existing branch.
    let branch = if desired_branch != anchor_branch {
        let metadata = match branch::metadata_local(repository.clone(), anchor_branch).await {
            Ok(metadata) => Some(metadata),
            Err(_local_error) => match repository.remote().await {
                Ok(remote) => branch::metadata_remote(remote, repository.clone(), anchor_branch)
                    .await
                    .ok(),
                Err(_no_remote) => None,
            },
        };
        let at_branch_point = metadata
            .map(|metadata| branch::stack(&metadata))
            .is_some_and(|stack| {
                stack
                    .first()
                    .is_some_and(|point| point.revision == revision)
            });
        if at_branch_point {
            anchor_branch
        } else {
            desired_branch
        }
    } else {
        anchor_branch
    };
    let globals = execution_context();
    let globals = globals.globals();
    let request = RootHoldRequest::prepare(
        repository,
        revision,
        branch,
        HoldOptions {
            overwrite_local: globals.force(),
            keep_local: options.forward_changes,
            dry_run: globals.dry_run(),
        },
    )
    .await
    .forward::<SyncError>("Preparing transactional root hold")?;
    repository
        .file_system()
        .transactional_root_hold(request)
        .await
        .forward::<SyncError>("Publishing transactional root hold")
}
