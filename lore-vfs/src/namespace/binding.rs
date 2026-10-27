// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Immutable namespace routing and authorization. Detached records retain the
//! pair they retired from; attached records always select the current binding.

use super::*;

pub(super) struct NamespaceBinding {
    pub(super) backend: Arc<dyn Backend>,
    pub(super) gate: Arc<dyn AccessGate>,
    pub(super) policy: Arc<dyn AccessGate>,
}
impl Namespace {
    pub(super) fn record_binding(
        record: &Record,
        current: &Arc<NamespaceBinding>,
    ) -> Result<Arc<NamespaceBinding>> {
        if record.attached {
            Ok(current.clone())
        } else {
            record.retired_binding.clone().ok_or(Error::Stale.into())
        }
    }
}
