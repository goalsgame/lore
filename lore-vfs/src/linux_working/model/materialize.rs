// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Uninstalled private files for one exact Model. The caller's borrowed plan
//! still owns writer exclusion; dropping resources only releases request admission.
//! Namespace consistency does not establish an exclusive publication owner.
#![allow(dead_code, reason = "Prepared resources have no installation caller.")]

mod owned;
pub(crate) use owned::CapturedCheckpointImage;
pub(crate) use owned::OwnedSourcePreparation;
pub(crate) use owned::SourceContinuation;

use super::*;
use crate::namespace::prepared_view::PreparedEffectiveView;
use crate::overlay::CopyLimits;
use crate::overlay::ReceiptCell;
use crate::transition_prepare::PreparedNamespaceLayout;

#[derive(Debug)]
pub(crate) enum PreparationError {
    Model(super::Error),
    Transition(crate::transition_prepare::Error),
}
type PreparedResult<T> = std::result::Result<T, PreparationError>;
impl From<super::Error> for PreparationError {
    fn from(error: super::Error) -> Self {
        Self::Model(error)
    }
}
impl From<core::Error> for PreparationError {
    fn from(error: core::Error) -> Self {
        Self::Model(error.into())
    }
}
impl From<crate::transition_prepare::Error> for PreparationError {
    fn from(error: crate::transition_prepare::Error) -> Self {
        Self::Transition(error)
    }
}

impl From<crate::checkpoint_owner::Error> for PreparationError {
    fn from(error: crate::checkpoint_owner::Error) -> Self {
        Self::Model(error.into())
    }
}

struct Admission {
    owner: Arc<Model>,
    permit: Option<OwnedSemaphorePermit>,
}
impl Drop for Admission {
    fn drop(&mut self) {
        self.permit.take();
        self.owner.activity.notify_waiters();
    }
}
struct PreparedFile {
    file: Arc<File>,
    receipt: ReceiptCell,
}
pub(crate) struct PreparedPrivateResources<'plan> {
    layout: PreparedNamespaceLayout<'plan>,
    view: PreparedEffectiveView,
    files: BTreeMap<u64, PreparedFile>,
    budget: crate::transition_prepare::Budget,
    // Release admission only after every prepared gate/file/store has dropped.
    admission: Admission,
}
struct UnadmittedResources<'plan> {
    budget: crate::transition_prepare::Budget,
    layout: PreparedNamespaceLayout<'plan>,
    view: PreparedEffectiveView,
    files: BTreeMap<u64, PreparedFile>,
}

impl Model {
    pub(crate) async fn prepare_private_resources<'plan>(
        self: &Arc<Self>,
        layout: PreparedNamespaceLayout<'plan>,
    ) -> PreparedResult<PreparedPrivateResources<'plan>> {
        self.live()?;
        let permit = match self.requests.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_full) => {
                self.live()?;
                return Err(core::Error::Busy.into());
            }
        };
        let admission = Admission {
            owner: self.clone(),
            permit: Some(permit),
        };
        let mut closed = self.closed.subscribe();
        self.live()?;
        let prepared = tokio::select! { biased;
            _=closed.changed()=>return Err(core::Error::Unmounted.into()),
            result=self.materialize_layout(layout)=>result?,
        };
        self.live()?;
        Ok(PreparedPrivateResources {
            layout: prepared.layout,
            view: prepared.view,
            files: prepared.files,
            admission,
            budget: prepared.budget,
        })
    }

    async fn materialize_layout<'plan>(
        self: &Arc<Self>,
        layout: PreparedNamespaceLayout<'plan>,
    ) -> PreparedResult<UnadmittedResources<'plan>> {
        layout.check_namespace(&self.namespace)?;
        if !Arc::ptr_eq(self.checkpoint.namespace(), &self.namespace) {
            return Err(core::Error::InvalidInput.into());
        }
        self.live()?;
        layout.validate_source().await?;
        let mut budget = layout.resource_budget()?;
        let mut stores = 0u64;
        let mut bytes = 0u64;
        let mut extents = 0usize;
        for spec in layout.private_files() {
            let spec = spec?;
            spec.image.validate(self.limits.file)?;
            if spec.new_inode == 0 || spec.new_inode == spec.source_inode {
                return Err(core::Error::InvalidInput.into());
            }
            if let Some(receipt) = &spec.image.receipt {
                receipt.validate(
                    spec.source_inode,
                    spec.image.node.key,
                    spec.image.metadata.generation,
                    spec.image.metadata.size,
                    spec.image.metadata.mode,
                )?;
            }
            stores = stores.checked_add(1).ok_or(core::Error::ResourceLimit)?;
            let mut previous = None;
            for extent in &spec.image.extents {
                let length = u64::try_from(extent.data.len())
                    .map_err(|_length| core::Error::ResourceLimit)?;
                bytes = bytes
                    .checked_add(length)
                    .ok_or(core::Error::ResourceLimit)?;
                if previous != Some(extent.start) {
                    extents = extents.checked_add(1).ok_or(core::Error::ResourceLimit)?;
                }
                previous = Some(
                    extent
                        .start
                        .checked_add(length)
                        .ok_or(core::Error::ResourceLimit)?,
                );
            }
            // Fresh overlay, receipt cell, File wrapper and output-map entry.
            for _ in 0..4 {
                budget.node()?;
            }
        }
        if self
            .counters
            .stores
            .load(Ordering::Acquire)
            .checked_add(stores)
            .is_none_or(|count| count > self.limits.max_stores)
        {
            return Err(super::Error::StoreLimit.into());
        }
        if self
            .budget
            .reserved_bytes()
            .checked_add(bytes)
            .is_none_or(|bytes| bytes > self.budget.limit())
        {
            return Err(core::Error::ResourceLimit.into());
        }
        let checkpoint_limit = u64::try_from(self.limits.max_checkpoint_data_bytes)
            .map_err(|_limit| core::Error::ResourceLimit)?;
        if layout
            .source_private_bytes()?
            .checked_add(bytes)
            .is_none_or(|bytes| bytes > checkpoint_limit)
        {
            return Err(core::Error::ResourceLimit.into());
        }
        // This is a temporary, revocable decision, not an installation permit.
        layout.authorize_materialization().await?;
        let view = self
            .namespace
            .prepare_layout_view(&layout, &mut budget)
            .await?;
        if budget.remaining_nodes() < extents {
            return Err(core::Error::ResourceLimit.into());
        }
        for spec in layout.private_files() {
            let spec = spec?;
            view.gate()
                .authorize(&spec.image.node.key, Access::Write)
                .await?;
        }
        self.live()?;
        let mut files = BTreeMap::new();
        for spec in layout.private_files() {
            let spec = spec?;
            self.live()?;
            let store = self.store().await?;
            let copied = OverlayFile::copy_private_bounded(
                spec.image,
                spec.source_inode,
                spec.new_inode,
                view.backend(),
                view.gate(),
                store,
                CopyLimits {
                    file: self.limits.file,
                    extents: budget.remaining_nodes(),
                },
            )
            .await?;
            let count = copied.checkpoint_guard().await.extent_count();
            for _ in 0..count {
                budget.node()?;
            }
            let overlay = Arc::new(self.namespace.attach_prepared_file(copied));
            let receipt = overlay.receipt_cell();
            let file = Arc::new(File {
                overlay,
                mutations: AsyncMutex::new(()),
            });
            if files
                .insert(spec.new_inode, PreparedFile { file, receipt })
                .is_some()
            {
                return Err(core::Error::InvalidInput.into());
            }
        }
        layout.authorize_materialization().await?;
        self.live()?;
        Ok(UnadmittedResources {
            budget,
            layout,
            view,
            files,
        })
    }
}

#[cfg(all(test, unix))]
#[path = "materialize_tests.rs"]
mod tests;
