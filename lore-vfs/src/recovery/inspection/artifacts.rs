// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Independently checked fixed-name transition facts. Inventory bits describe
//! observed names only; flagged or partial facts grant no admission, selected
//! image, coherent bundle, or reconciliation permission. All reads retain the
//! inspection claim and leave every artifact untouched.

mod relation;

use std::ffi::CStr;
use std::fs::File;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;

use super::RecoveryInspection;
use crate::capture_memory::CaptureMemory;
use crate::recovery::Error;
use crate::recovery::LoadedCheckpoint;
use crate::recovery::Result;
use crate::recovery::intent;

pub(crate) struct Inventory {
    observed_known: u8,
    unknown_or_duplicate: bool,
    excess: bool,
}
impl Inventory {
    /// Bits: source=1, candidate=2, intent.pending=4, intent=8. This is not
    /// a complete inventory when either flag is set.
    pub(crate) fn observed_known(&self) -> u8 {
        self.observed_known
    }
    pub(crate) fn unknown_or_duplicate(&self) -> bool {
        self.unknown_or_duplicate
    }
    pub(crate) fn excess(&self) -> bool {
        self.excess
    }
}

pub(crate) enum InvalidArtifact {
    Unsafe,
    Io(std::io::Error),
    Corrupt,
    Incompatible,
    Intent(intent::Error),
}
pub(crate) enum Artifact<'a, T> {
    Missing,
    Checked(&'a T),
    Invalid(&'a InvalidArtifact),
}
// Fixed slots avoid a heap box for each large checked image or intent array.
struct ArtifactValue<T> {
    checked: Option<T>,
    invalid: Option<InvalidArtifact>,
}
impl<T> ArtifactValue<T> {
    fn view(&self) -> Artifact<'_, T> {
        if let Some(value) = &self.checked {
            Artifact::Checked(value)
        } else if let Some(error) = &self.invalid {
            Artifact::Invalid(error)
        } else {
            Artifact::Missing
        }
    }
    fn read(read: impl FnOnce() -> Result<Option<T>>) -> Result<Self> {
        match read() {
            Ok(checked) => Ok(Self {
                checked,
                invalid: None,
            }),
            Err(Error::ResourceLimit) => Err(Error::ResourceLimit),
            Err(error) => Ok(Self {
                checked: None,
                invalid: Some(match error {
                    Error::Io(error) => InvalidArtifact::Io(error),
                    Error::PrivateStorageRequired => InvalidArtifact::Unsafe,
                    Error::Incompatible => InvalidArtifact::Incompatible,
                    Error::Corrupt => InvalidArtifact::Corrupt,
                    // The fixed read helpers cannot produce publication outcomes.
                    _ => return Err(error),
                }),
            }),
        }
    }
}
pub(crate) struct IntentFact {
    bytes: [u8; intent::ENCODED_SIZE],
    parsed: intent::Intent,
}
impl IntentFact {
    pub(crate) fn bytes(&self) -> &[u8; intent::ENCODED_SIZE] {
        &self.bytes
    }
    pub(crate) fn parsed(&self) -> &intent::Intent {
        &self.parsed
    }
}
pub(crate) struct TransitionFacts {
    inventory: Inventory,
    source: ArtifactValue<LoadedCheckpoint>,
    candidate: ArtifactValue<LoadedCheckpoint>,
    pending_intent: ArtifactValue<IntentFact>,
    intent: ArtifactValue<IntentFact>,
}
impl TransitionFacts {
    pub(crate) fn inventory(&self) -> &Inventory {
        &self.inventory
    }
    pub(crate) fn source(&self) -> Artifact<'_, LoadedCheckpoint> {
        self.source.view()
    }
    pub(crate) fn candidate(&self) -> Artifact<'_, LoadedCheckpoint> {
        self.candidate.view()
    }
    pub(crate) fn pending_intent(&self) -> Artifact<'_, IntentFact> {
        self.pending_intent.view()
    }
    pub(crate) fn intent(&self) -> Artifact<'_, IntentFact> {
        self.intent.view()
    }
}
impl RecoveryInspection {
    /// Uses the caller's aggregate retained-image ledger, including other images
    /// it already holds. No pair identity/root relation is asserted here.
    pub(crate) fn load_transition_budgeted(
        &self,
        memory: &mut CaptureMemory,
    ) -> Result<Option<TransitionFacts>> {
        let directory = match open_directory(&self.directory, c"transition") {
            Ok(directory) => directory,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        crate::recovery::private_metadata(&directory.metadata()?, true)?;
        // Fixed inventory/slot metadata; entry probes have no retained names.
        memory.nodes(12).map_err(|_limit| Error::ResourceLimit)?;
        let inventory = inventory(&directory)?;
        let source = ArtifactValue::read(|| image(&directory, c"source", self.limit, memory))?;
        let candidate =
            ArtifactValue::read(|| image(&directory, c"candidate", self.limit, memory))?;
        let pending_intent = read_intent(&directory, c"intent.pending", self.limit, memory)?;
        let intent = read_intent(&directory, c"intent", self.limit, memory)?;
        Ok(Some(TransitionFacts {
            inventory,
            source,
            candidate,
            pending_intent,
            intent,
        }))
    }
}
fn open_directory(parent: &File, name: &CStr) -> Result<File> {
    // SAFETY: retained parent and fixed NUL-terminated single component; all
    // flags are read-only, non-following and nonblocking.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: this new owned descriptor is transferred exactly once.
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn artifact_file(directory: &File, name: &CStr) -> Result<Option<File>> {
    let file = match super::open_file(directory, name) {
        Ok(file) => file,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    crate::recovery::private_metadata(&file.metadata()?, false)?;
    Ok(Some(file))
}
fn image(
    directory: &File,
    name: &CStr,
    limit: usize,
    memory: &mut CaptureMemory,
) -> Result<Option<LoadedCheckpoint>> {
    artifact_file(directory, name)?
        .map(|file| LoadedCheckpoint::read_budgeted(file, limit, memory))
        .transpose()
}
fn read_intent(
    directory: &File,
    name: &CStr,
    limit: usize,
    memory: &mut CaptureMemory,
) -> Result<ArtifactValue<IntentFact>> {
    let mut invalid_intent = None;
    let mut value = ArtifactValue::read(|| {
        let Some(mut file) = artifact_file(directory, name)? else {
            return Ok(None);
        };
        let before = stamp(&file.metadata()?);
        if before.0 != intent::ENCODED_SIZE as u64 {
            return Err(Error::Corrupt);
        }
        memory
            .buffer(intent::ENCODED_SIZE)
            .map_err(|_limit| Error::ResourceLimit)?;
        memory.nodes(1).map_err(|_limit| Error::ResourceLimit)?;
        let mut bytes = [0; intent::ENCODED_SIZE];
        file.read_exact(&mut bytes).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::Corrupt
            } else {
                Error::Io(error)
            }
        })?;
        let mut extra = [0];
        loop {
            match file.read(&mut extra) {
                Ok(0) => break,
                Ok(_) => return Err(Error::Corrupt),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        if before != stamp(&file.metadata()?) {
            return Err(Error::Corrupt);
        }
        let parsed = match intent::Intent::decode(&bytes, limit) {
            Ok(parsed) => parsed,
            Err(intent::Error::ResourceLimit) => return Err(Error::ResourceLimit),
            Err(error) => {
                invalid_intent = Some(error);
                return Err(Error::Corrupt);
            }
        };
        Ok(Some(IntentFact { bytes, parsed }))
    })?;
    if let Some(error) = invalid_intent {
        value.invalid = Some(InvalidArtifact::Intent(error));
    }
    Ok(value)
}
fn stamp(metadata: &std::fs::Metadata) -> (u64, Option<std::time::SystemTime>, i64, i64) {
    use std::os::unix::fs::MetadataExt;
    (
        metadata.len(),
        metadata.modified().ok(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

struct DirectoryStream(*mut libc::DIR);
impl Drop for DirectoryStream {
    fn drop(&mut self) {
        // SAFETY: fdopendir transferred ownership once; only this guard closes it.
        unsafe {
            libc::closedir(self.0);
        }
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn inventory(directory: &File) -> Result<Inventory> {
    // A new open-file-description gives every scan an independent stream offset.
    let fresh = open_directory(directory, c".")?;
    let fd = fresh.into_raw_fd();
    // SAFETY: fd is a new owned directory descriptor. On success DIR owns it.
    let pointer = unsafe { libc::fdopendir(fd) };
    if pointer.is_null() {
        let error = std::io::Error::last_os_error();
        // SAFETY: failed fdopendir did not consume fd.
        unsafe {
            libc::close(fd);
        }
        return Err(error.into());
    }
    let stream = DirectoryStream(pointer);
    let mut result = Inventory {
        observed_known: 0,
        unknown_or_duplicate: false,
        excess: false,
    };
    let mut dots = 0u8;
    for visit in 0..7 {
        // SAFETY: errno pointer belongs to this thread on the supported platform.
        unsafe {
            *errno() = 0;
        }
        // SAFETY: the guard owns a live stream and the entry is used before the next call.
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(0) {
                return Err(error.into());
            }
            return Ok(result);
        }
        if visit == 6 {
            result.excess = true;
            return Ok(result);
        }
        // SAFETY: readdir returned a kernel-populated record with d_reclen bytes.
        let record_length = unsafe { std::ptr::addr_of!((*entry).d_reclen).read() };
        let offset = std::mem::offset_of!(libc::dirent, d_name);
        let Some(length) = usize::from(record_length)
            .checked_sub(offset)
            .filter(|length| *length > 0)
        else {
            result.unknown_or_duplicate = true;
            return Ok(result);
        };
        // Bound access to the actual record, not a potentially larger d_name array.
        // SAFETY: offset and length describe this returned directory record.
        let name = unsafe { std::slice::from_raw_parts(entry.cast::<u8>().add(offset), length) };
        let Some(end) = name.iter().position(|byte| *byte == 0) else {
            result.unknown_or_duplicate = true;
            return Ok(result);
        };
        let bit = match &name[..end] {
            b"." => {
                if dots & 1 != 0 {
                    result.unknown_or_duplicate = true;
                }
                dots |= 1;
                continue;
            }
            b".." => {
                if dots & 2 != 0 {
                    result.unknown_or_duplicate = true;
                }
                dots |= 2;
                continue;
            }
            b"source" => 1,
            b"candidate" => 2,
            b"intent.pending" => 4,
            b"intent" => 8,
            _ => {
                result.unknown_or_duplicate = true;
                continue;
            }
        };
        if result.observed_known & bit != 0 {
            result.unknown_or_duplicate = true;
        }
        result.observed_known |= bit;
    }
    unreachable!()
}
#[cfg(target_os = "linux")]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}
#[cfg(target_os = "macos")]
unsafe fn errno() -> *mut libc::c_int {
    unsafe { libc::__error() }
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn inventory(_directory: &File) -> Result<Inventory> {
    Err(Error::PrivateStorageRequired)
}

#[cfg(test)]
mod tests;
