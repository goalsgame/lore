// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Explicit local checkout editing policy. It grants local edits under the
//! admitted OS identity; remote publication still uses the ordinary server checks.
#![cfg_attr(
    not(target_os = "linux"),
    allow(dead_code, reason = "Service-owned mount selection is Linux-only.")
)]

use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_revision::interface::ExecutionContext;
use lore_vfs::core::Backend;
use lore_vfs::core::BackendFuture;
use lore_vfs::core::Error;
use lore_vfs::core::NodeKey;
use lore_vfs::core::Result;
use lore_vfs::overlay::Access;
use lore_vfs::overlay::AccessGate;
use lore_vfs::overlay::AccessGateFactory;
use lore_vfs::overlay::CandidateEdit;
use lore_vfs::overlay::ProvenTargetMetadata;
use lore_vfs::overlay::TargetSelection;
use lore_vfs::revision::RevisionBackend;

use crate::service::state::WorkingAdmission;

/// Created explicitly from trusted service work, with no default policy choice.
/// Each binding is immutable and belongs to one final source/restored backend.
#[lore_macro::test_pub]
pub(crate) struct LocalCheckoutFactory {
    admission: WorkingAdmission,
    execution: Arc<ExecutionContext>,
}

impl AccessGateFactory for LocalCheckoutFactory {
    fn bind(&self, backend: Arc<dyn Backend>) -> Result<Arc<dyn AccessGate>> {
        LocalCheckoutFactory::bind(self, backend)
    }
}

impl LocalCheckoutFactory {
    #[lore_macro::test_pub]
    pub(crate) fn new(
        admission: WorkingAdmission,
        execution: Arc<ExecutionContext>,
    ) -> Result<Self> {
        admission
            .validate()
            .map_err(|_denied| Error::PermissionDenied)?;
        if !execution.is_client() {
            return Err(Error::PermissionDenied);
        }
        Ok(Self {
            admission,
            execution,
        })
    }

    /// Call only after restore has selected the actual backend. An unrelated
    /// generic backend's permissive default revalidation cannot grant access.
    #[lore_macro::test_pub]
    pub(crate) fn bind(&self, backend: Arc<dyn Backend>) -> Result<Arc<dyn AccessGate>> {
        self.admission
            .validate()
            .map_err(|_denied| Error::PermissionDenied)?;
        let revision = revision(&backend)?;
        if !Arc::ptr_eq(&self.execution, &revision.captured_execution()) {
            return Err(Error::InvalidInput);
        }
        Ok(Arc::new(LocalCheckoutPolicy {
            admission: self.admission.clone(),
            execution: self.execution.clone(),
            backend,
        }))
    }
}

struct LocalCheckoutPolicy {
    admission: WorkingAdmission,
    execution: Arc<ExecutionContext>,
    backend: Arc<dyn Backend>,
}

fn revision(backend: &Arc<dyn Backend>) -> Result<&RevisionBackend> {
    backend
        .as_any()
        .and_then(|value| value.downcast_ref::<RevisionBackend>())
        .ok_or(Error::Unsupported)
}

impl AccessGate for LocalCheckoutPolicy {
    fn authorize_candidate_edit<'a>(&'a self, edit: &'a CandidateEdit) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)?;
            if !Arc::ptr_eq(
                &self.execution,
                &edit.proof().selection().captured_execution(),
            ) {
                return Err(Error::InvalidInput);
            }
            // This explicit local policy authorizes planned namespace edits.
            // Immutable target writes, anchors and publication remain separate.
            revision(&self.backend)?
                .revalidate_candidate_edit(edit)
                .await?;
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)
        }))
    }

    fn authorize_target_head<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
    ) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)?;
            if !Arc::ptr_eq(&self.execution, &proof.selection().captured_execution()) {
                return Err(Error::InvalidInput);
            }
            revision(&self.backend)?
                .revalidate_target_head(proof)
                .await?;
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)
        }))
    }

    fn authorize_target_node<'a>(
        &'a self,
        proof: &'a ProvenTargetMetadata,
        key: &'a NodeKey,
        access: Access,
    ) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)?;
            if !Arc::ptr_eq(&self.execution, &proof.selection().captured_execution()) {
                return Err(Error::InvalidInput);
            }
            revision(&self.backend)?
                .revalidate_target_node(proof, key, access == Access::Write)
                .await?;
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)
        }))
    }

    fn authorize_selection<'a>(&'a self, selection: &'a TargetSelection) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)?;
            if !Arc::ptr_eq(&self.execution, &selection.captured_execution()) {
                return Err(Error::InvalidInput);
            }
            revision(&self.backend)?
                .revalidate_selection_source(selection)
                .await?;
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)
        }))
    }

    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(LORE_CONTEXT.scope(self.execution.clone(), async move {
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)?;
            // Invoke the concrete revision implementation, never the generic
            // Backend default. Its current ancestor route, pin and exposure
            // checks also apply to cached/local overlay data. Unpublished target
            // keys remain unsupported until explicitly bound to a proven view.
            Backend::revalidate(revision(&self.backend)?, key, access == Access::Write).await?;
            self.admission
                .validate()
                .map_err(|_denied| Error::PermissionDenied)
        }))
    }
}
