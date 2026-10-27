// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_io::lease::IoLease;

use super::*;

/// One finite operation admitted against an exact ready registration. Retained
/// submitted work may finish after retirement; this cannot admit another operation.
pub struct BoundWork {
    _guard: bootstrap_gate::WorkLease,
    _claim: ClaimPin,
}
impl BoundLoad {
    /// Recheck the exact ready reservation before starting finite work. No IO or
    /// provider callbacks run under the registry or admission locks.
    pub fn begin_work(&self) -> Result<BoundWork> {
        let claim = self.claim.registration();
        let mut guard = None;
        let result = {
            let state = claim.registry.state.lock();
            (|| {
                let entry = current_location(&state, &self.location)?;
                if !claim.matches(entry) {
                    return Err(Error::Stale);
                }
                if entry.provider.is_none() {
                    return Err(Error::NotReady);
                }
                guard = Some(
                    entry
                        .gate
                        .begin_work(&self.principal)
                        .map_err(admission_error)?,
                );
                Ok(())
            })()
        };
        result?;
        Ok(BoundWork {
            _guard: guard.ok_or(Error::Stale)?,
            _claim: self.claim.clone(),
        })
    }
}
impl BoundWork {
    /// Capture inherited IO ownership and retain both through scoped polls,
    /// cleanup, and submitted jobs that propagate this lease. This owns no Client
    /// token or store claims; delegates must retain their actual resources.
    pub fn into_io_lease(self) -> IoLease {
        let parent = IoLease::current();
        IoLease::new(Arc::new(WorkIoOwner {
            _work: self,
            _parent: parent,
        }))
    }
}
// Work/claim destruction precedes the inherited service accounting owner.
struct WorkIoOwner {
    _work: BoundWork,
    _parent: Option<IoLease>,
}

#[cfg(test)]
mod tests;
