// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Read-only proof checks for explicit candidate-edit authorization requests.

use super::*;
use crate::overlay::CandidateEdit;
use crate::overlay::CandidateEditAction;
use crate::overlay::ProvenTargetMetadata;
use crate::overlay::candidate_edit::Location;
use crate::transition_prepare::Kind;

pub(super) fn path_parts(path: &[u8], maximum: usize) -> Result<Vec<&str>, Error> {
    let path = std::str::from_utf8(path).map_err(|_encoding| Error::InvalidInput)?;
    if path.len() > maximum.checked_mul(1001).ok_or(Error::ResourceLimit)? {
        return Err(Error::ResourceLimit);
    }
    let mut result = Vec::new();
    if path.is_empty() {
        return Ok(result);
    }
    for part in path.split('/') {
        if result.len() == maximum {
            return Err(Error::ResourceLimit);
        }
        if part.is_empty()
            || part.len() > 1000
            || part.contains(['\\', '\0'])
            || part == "."
            || part == ".."
            || is_reserved_node_name(part)
        {
            return Err(Error::InvalidInput);
        }
        result.push(part);
    }
    Ok(result)
}

impl RevisionBackend {
    /// Validate metadata evidence for a local decision. This never writes an
    /// immutable target or authorizes namespace installation or anchor changes.
    pub async fn revalidate_candidate_edit(&self, edit: &CandidateEdit) -> Result<(), Error> {
        self.revalidate_target_head(edit.proof()).await?;
        let parts = path_parts(edit.path(), edit.proof().max_depth)?;
        if edit.action() == CandidateEditAction::AdvanceSelectedHead {
            if !matches!(edit.location, Location::Head)
                || !parts.is_empty()
                || edit.source_before().is_some()
                || edit.target_before().is_some()
                || edit.candidate_after().is_some()
                || edit.change().is_some()
            {
                return Err(Error::InvalidInput);
            }
            return Ok(());
        }
        if edit.source_before().is_none()
            && edit.target_before().is_none()
            && edit.candidate_after().is_none()
        {
            return Err(Error::InvalidInput);
        }
        if [
            edit.source_before(),
            edit.target_before(),
            edit.candidate_after(),
        ]
        .into_iter()
        .flatten()
        .any(|summary| summary.kind == Kind::Link || summary.link.is_some())
        {
            return Err(Error::Unsupported);
        }
        match &edit.location {
            Location::Head => return Err(Error::InvalidInput),
            Location::Absent => {
                if edit.proof().root().is_some() || edit.target_before().is_some() {
                    return Err(Error::InvalidInput);
                }
            }
            Location::Node { node, path } => {
                let (actual, summary) = self
                    .candidate_node_metadata(edit.proof(), &node.key)
                    .await?;
                let actual_path = self.candidate_node_path(edit.proof(), &node.key).await?;
                if &actual != node
                    || actual_path != *path
                    || edit.target_before() != Some(&summary)
                    || std::str::from_utf8(path)
                        .map_err(|_encoding| Error::InvalidInput)?
                        .to_lowercase()
                        != std::str::from_utf8(edit.path())
                            .map_err(|_encoding| Error::InvalidInput)?
                            .to_lowercase()
                {
                    return Err(Error::PermissionDenied);
                }
            }
            Location::Parent {
                node,
                path,
                tail,
                entry,
                missing,
            } => {
                let (actual, _) = self
                    .candidate_node_metadata(edit.proof(), &node.key)
                    .await?;
                let actual_path = self.candidate_node_path(edit.proof(), &node.key).await?;
                let parent_parts = path_parts(path, edit.proof().max_depth)?;
                let tail_parts = path_parts(tail, edit.proof().max_depth)?;
                if &actual != node
                    || actual.kind != NodeKind::Directory
                    || actual_path != *path
                    || tail_parts.is_empty()
                    || parts.len() != parent_parts.len() + tail_parts.len()
                    || !parts
                        .iter()
                        .zip(&parent_parts)
                        .all(|(left, right)| left.to_lowercase() == right.to_lowercase())
                    || parts[parent_parts.len()..] != tail_parts
                {
                    return Err(Error::PermissionDenied);
                }
                match (entry, edit.target_before()) {
                    (None, None) => {
                        if !missing
                            .as_ref()
                            .is_some_and(|proof| proof.matches(edit, &node.key, tail))
                        {
                            return Err(Error::PermissionDenied);
                        }
                    }
                    (Some(entry), Some(expected)) => {
                        let (actual, summary) = self
                            .candidate_node_metadata(edit.proof(), &entry.key)
                            .await?;
                        let child_path = self.candidate_node_path(edit.proof(), &entry.key).await?;
                        let child_parts = path_parts(&child_path, edit.proof().max_depth)?;
                        let target_edge = edit
                            .candidate_after()
                            .is_none_or(|after| after.kind != expected.kind)
                            || child_path != edit.path();
                        if missing.is_some()
                            || edit.change().is_none() && !target_edge
                            || &actual != entry
                            || &summary != expected
                            || tail_parts.len() != 1
                            || child_parts.len() != parent_parts.len() + 1
                            || child_parts[..parent_parts.len()] != parent_parts
                            || child_parts.last().is_none_or(|part| {
                                part.to_lowercase() != tail_parts[0].to_lowercase()
                            })
                        {
                            return Err(Error::PermissionDenied);
                        }
                    }
                    _ => return Err(Error::InvalidInput),
                }
            }
        }
        self.revalidate_target_head(edit.proof()).await
    }

    pub(crate) async fn candidate_node_metadata(
        &self,
        proof: &ProvenTargetMetadata,
        key: &NodeKey,
    ) -> Result<(Node, crate::transition_prepare::NodeSummary), Error> {
        self.revalidate_target_node(proof, key, false).await?;
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                let mut owner = self.owner(&proof.selection().original_scope())?;
                owner.state = proof.state.clone();
                let node = owner
                    .state
                    .node(owner.repository.clone(), key.node)
                    .await
                    .map_err(state_error)?;
                if node.is_link() {
                    return Err(Error::Unsupported);
                }
                target::validate_metadata(&node)?;
                let result = (
                    describe(&owner, key.node, &node),
                    transition::summary(&node),
                );
                self.revalidate_target_head(proof).await?;
                Ok(result)
            })
            .await
    }

    async fn candidate_node_path(
        &self,
        proof: &ProvenTargetMetadata,
        key: &NodeKey,
    ) -> Result<Vec<u8>, Error> {
        self.revalidate_target_node(proof, key, false).await?;
        LORE_CONTEXT
            .scope(self.execution.clone(), async {
                let root = proof.root().ok_or(Error::PermissionDenied)?;
                let owner = self.owner(&proof.selection().original_scope())?;
                let mut parts = Vec::new();
                let mut id = key.node;
                for _ in 0..=proof.max_depth {
                    if id == root.key.node {
                        parts.reverse();
                        let result = parts.join("/").into_bytes();
                        self.revalidate_target_head(proof).await?;
                        return Ok(result);
                    }
                    if parts.len() == proof.max_depth {
                        return Err(Error::ResourceLimit);
                    }
                    let node = proof
                        .state
                        .node(owner.repository.clone(), id)
                        .await
                        .map_err(state_error)?;
                    if node.name_length == 0 || node.name_length > 1000 {
                        return Err(Error::InvalidInput);
                    }
                    let name = proof
                        .state
                        .node_name_clone(owner.repository.clone(), id)
                        .await
                        .map_err(state_error)?;
                    let validated = path_parts(name.as_bytes(), 1)?;
                    if validated.len() != 1 {
                        return Err(Error::InvalidInput);
                    }
                    parts.push(name);
                    id = node.parent;
                }
                Err(Error::ResourceLimit)
            })
            .await
    }
}
