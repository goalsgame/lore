// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Immutable authorization requests, not namespace installation capabilities.

use super::*;
use crate::transition_prepare::Change;
use crate::transition_prepare::NodeSummary;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateEditAction {
    AdvanceSelectedHead,
    EntryChange,
}

pub enum CandidateLocation<'a> {
    SelectedHead,
    TargetNode(&'a Node),
    TargetDirectoryParent { parent: &'a Node, tail: &'a [u8] },
    AbsentSelectedScope,
}

#[derive(Clone)]
pub(crate) enum Location {
    Head,
    Node {
        node: Node,
        path: Vec<u8>,
    },
    Parent {
        node: Node,
        path: Vec<u8>,
        tail: Vec<u8>,
        entry: Option<Node>,
        missing: Option<Box<crate::revision::MissingTargetPath>>,
    },
    Absent,
}

/// A plan-minted request. Summaries separately describe mounted source data,
/// immutable target data, and the prepared candidate, including carried edits.
/// Successful authorization does not authorize installation or anchor writes.
#[derive(Clone)]
pub struct CandidateEdit {
    pub(crate) proof: Arc<ProvenTargetMetadata>,
    pub(crate) action: CandidateEditAction,
    pub(crate) path: Vec<u8>,
    pub(crate) source_before: Option<NodeSummary>,
    pub(crate) target_before: Option<NodeSummary>,
    pub(crate) candidate_after: Option<NodeSummary>,
    pub(crate) change: Option<Change>,
    pub(crate) location: Location,
}
impl CandidateEdit {
    pub fn proof(&self) -> &ProvenTargetMetadata {
        &self.proof
    }
    pub fn action(&self) -> CandidateEditAction {
        self.action
    }
    pub fn path(&self) -> &[u8] {
        &self.path
    }
    pub fn source_before(&self) -> Option<&NodeSummary> {
        self.source_before.as_ref()
    }
    pub fn target_before(&self) -> Option<&NodeSummary> {
        self.target_before.as_ref()
    }
    pub fn candidate_after(&self) -> Option<&NodeSummary> {
        self.candidate_after.as_ref()
    }
    /// The actual mounted-source-to-candidate change. Carried local differences
    /// can require authorization even when no namespace change is needed.
    pub fn change(&self) -> Option<&Change> {
        self.change.as_ref()
    }
    pub fn location(&self) -> CandidateLocation<'_> {
        match &self.location {
            Location::Head => CandidateLocation::SelectedHead,
            Location::Node { node, .. } => CandidateLocation::TargetNode(node),
            Location::Parent { node, tail, .. } => {
                CandidateLocation::TargetDirectoryParent { parent: node, tail }
            }
            Location::Absent => CandidateLocation::AbsentSelectedScope,
        }
    }
}
