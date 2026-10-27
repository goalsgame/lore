// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Path accounting before input construction. This grants no recovery or factory
//! qualification; already prepared inputs retain their compatibility path.
#![allow(
    dead_code,
    reason = "pre-input reservation is not exposed by command dispatch"
)]
use std::os::unix::fs::MetadataExt;

use super::*;

pub(super) struct ReservedPaths {
    location: lore_revision::fs::provider_registry::WorkingLocation,
    backing: PathBuf,
}
impl ReservedPaths {
    fn capture(
        registration: &lore_revision::fs::provider_registry::Registration,
        backing: &Path,
    ) -> Result<Self> {
        let location = registration
            .bootstrap_location()
            .map_err(|error| Error::Registry(error.to_string()))?;
        let target = crate::mount::validate_mountpoint(
            location.target().path(),
            Some(location.metadata().path()),
        )
        .map_err(|error| Error::Mountpoint(error.to_string()))?;
        let metadata = location.metadata().path();
        let stat = std::fs::symlink_metadata(metadata)
            .map_err(|error| Error::Storage(error.to_string()))?;
        // SAFETY: geteuid reads effective identity without pointers.
        let uid = unsafe { libc::geteuid() };
        if target != location.target().path()
            || !stat.is_dir()
            || stat.file_type().is_symlink()
            || stat.uid() != uid
            || stat.mode() & 0o777 != 0o700
            || metadata
                .canonicalize()
                .map_err(|error| Error::Storage(error.to_string()))?
                != metadata
        {
            return Err(Error::InvalidBinding);
        }
        let backing = candidate(backing)?;
        let paths = Self { location, backing };
        if paths
            .paths()
            .iter()
            .any(|path| path.as_os_str().as_encoded_bytes().len() > 4096)
            || overlaps(paths.target(), paths.metadata())
            || overlaps(paths.target(), &paths.backing)
            || overlaps(paths.metadata(), &paths.backing)
        {
            return Err(Error::Overlap);
        }
        Ok(paths)
    }
    // Compatibility inputs already own storage. This does not establish that
    // their earlier metadata/store preparation had recovery admission.
    pub(super) fn from_input(input: &StartupInput) -> Self {
        Self {
            location: input.prepared.location.clone(),
            backing: input.storage.root().to_path_buf(),
        }
    }
    pub(super) fn paths(&self) -> [&Path; 3] {
        [self.target(), self.metadata(), &self.backing]
    }
    pub(super) fn target(&self) -> &Path {
        self.location.target().path()
    }
    pub(super) fn metadata(&self) -> &Path {
        self.location.metadata().path()
    }
    fn matches(&self, input: &StartupInput) -> bool {
        self.location == input.prepared.location && self.backing == input.storage.root()
    }
}
fn candidate(path: &Path) -> Result<PathBuf> {
    let error = |error: std::io::Error| Error::Storage(error.to_string());
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > 4096 {
        return Err(Error::InvalidBinding);
    }
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            path.canonicalize().map_err(error)
        }
        Ok(_) => Err(Error::InvalidBinding),
        Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or(Error::InvalidBinding)?
                .canonicalize()
                .map_err(error)?;
            let name = path.file_name().ok_or(Error::InvalidBinding)?;
            Ok(parent.join(name))
        }
        Err(problem) => Err(error(problem)),
    }
}

#[lore_macro::test_pub]
pub(crate) struct BootstrapReservation {
    ticket: StartupReservation,
}
#[lore_macro::test_pub]
pub(crate) struct PromotionError {
    pub error: Error,
    pub input: Box<StartupInput>,
    pub handle: InstanceHandle,
}
impl Supervisor {
    /// Reserves the same bounded record used by later startup. No directories,
    /// stores, recovery owner or provider are created by this method.
    #[lore_macro::test_pub]
    pub(crate) fn reserve_bootstrap(
        self: &Arc<Self>,
        registration: Arc<lore_revision::fs::provider_registry::Registration>,
        backing: &Path,
    ) -> Result<BootstrapReservation> {
        if !registration.belongs_to(&self.registry) {
            return Err(Error::WrongOwner);
        }
        let paths = ReservedPaths::capture(&registration, backing)?;
        let ticket = self
            .reserve_record(registration, paths, None)
            .map_err(|rejected| rejected.error)?;
        Ok(BootstrapReservation { ticket })
    }
}
impl BootstrapReservation {
    pub(super) fn record(&self) -> Arc<Record> {
        self.ticket.record.clone()
    }
    pub(super) fn cancel(&self) -> Result<()> {
        self.ticket.supervisor.cancel_startup(&self.ticket.handle())
    }
    #[lore_macro::test_pub]
    pub(crate) fn handle(&self) -> InstanceHandle {
        self.ticket.handle()
    }
    #[lore_macro::test_pub]
    pub(crate) fn check_live(&self) -> Result<()> {
        self.ticket.check_live()
    }
    /// Records existing input in this reservation; it does not retroactively
    /// qualify its bootstrap IO or add recovery authority.
    #[lore_macro::test_pub]
    pub(crate) fn promote(
        self,
        input: StartupInput,
    ) -> std::result::Result<StartupReservation, PromotionError> {
        let handle = self.handle();
        let checked = self
            .ticket
            .supervisor
            .validate_input(&input)
            .and_then(|()| {
                if !self
                    .ticket
                    .record
                    ._registration
                    .same_reservation(&input.prepared.registration)
                    || !self.ticket.record.paths.matches(&input)
                {
                    Err(Error::InvalidBinding)
                } else {
                    Ok(())
                }
            });
        if let Err(error) = checked {
            return Err(PromotionError {
                error,
                input: Box::new(input),
                handle,
            });
        }
        // Validation may perform registry queries; none run under these locks.
        let supervisor = self.ticket.supervisor.clone();
        let retained = self.ticket.record.clone();
        let outcome = (|| {
            let state = supervisor.state.lock().map_err(|_poison| Error::Poisoned)?;
            if state.closed
                || state
                    .records
                    .get(&retained.id)
                    .is_none_or(|record| !Arc::ptr_eq(record, &retained))
            {
                return Err(Error::Closed);
            }
            let phase = retained.state.lock().map_err(|_poison| Error::Poisoned)?;
            if phase.phase != Phase::Starting || !phase.startup_active {
                return Err(Error::Closed);
            }
            Ok((state, phase))
        })();
        match outcome {
            Err(error) => Err(PromotionError {
                error,
                input: Box::new(input),
                handle,
            }),
            Ok((state, record)) => {
                let installed = self.ticket.record.input.set(input);
                drop(record);
                drop(state);
                match installed {
                    Ok(()) => Ok(self.ticket),
                    Err(input) => Err(PromotionError {
                        error: Error::InvalidBinding,
                        input: Box::new(input),
                        handle,
                    }),
                }
            }
        }
    }
}
