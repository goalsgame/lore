// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Persist immutable transition metadata through the exact owning store.

use super::*;
impl RevisionBackend {
    pub(crate) async fn flush_transition_dependencies(&self) -> Result<(), Error> {
        let immutable = {
            let owners = self.owners.lock().map_err(|_poison| Error::Io)?;
            let root = owners
                .by_exposure
                .get(&self.root.key.exposure)
                .ok_or(Error::Stale)?;
            let immutable = root.repository.immutable_store();
            for owner in owners.by_exposure.values() {
                if !Arc::ptr_eq(&immutable, &owner.repository.immutable_store()) {
                    return Err(Error::Unsupported);
                }
            }
            immutable
        };
        immutable.flush(true).await.map_err(|_io| Error::Io)
    }
}
