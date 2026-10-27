// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Blocking publication of the existing fixed transition-artifact protocol.
//! The external worker retains the same source acknowledgment, command/store
//! lease, serial, seal and candidate through actual completion, including errors.
//! A receipt is a durable byte fact, not anchor-write or installation authority.

use std::ffi::CStr;
use std::fs::File;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::fs::MetadataExt;

use super::*;
use crate::capture_memory::CaptureMemory;
use crate::recovery::intent::CheckpointIdentity;
use crate::recovery::intent::Intent;
use crate::recovery::published::PublishedCheckpointImage;

pub(crate) struct DurableIntentReceipt {
    record: Intent,
}
impl DurableIntentReceipt {
    pub(crate) fn record(&self) -> &Intent {
        &self.record
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
    CreateDirectory,
    ContainerSync,
    SourceWrite,
    SourceSync,
    CandidateWrite,
    CandidateSync,
    ImageDirectorySync,
    IntentWrite,
    IntentSync,
    Rename,
    DirectorySync,
}
struct Fault {
    #[cfg(test)]
    before: Option<Step>,
}
impl Fault {
    fn hit(&self, step: Step) -> Result<()> {
        #[cfg(test)]
        if self.before == Some(step) {
            return Err(std::io::Error::other("injected transition publication failure").into());
        }
        let _checked_fault = (self, step);
        Ok(())
    }
}
impl Recovery {
    /// Borrowed inputs must come from the same opaque `SourceAcknowledgment`.
    /// They are already retained in this operation's memory ledger. The owner
    /// coordinator checks exact source facts, policy and anchor authority before
    /// entering this blocking method; it retains them on every error outcome.
    pub(crate) fn publish_transition(
        &mut self,
        source: &PublishedCheckpointImage,
        candidate: &crate::linux_working::CapturedCheckpointImage<'_>,
        record: &Intent,
        memory: &mut CaptureMemory,
    ) -> Result<DurableIntentReceipt> {
        self.publish_transition_inner(
            source,
            candidate.snapshot(),
            candidate.bytes(),
            record,
            memory,
            &Fault {
                #[cfg(test)]
                before: None,
            },
        )
    }
    fn publish_transition_inner(
        &mut self,
        source: &PublishedCheckpointImage,
        candidate: &NamespaceSnapshot,
        encoded_candidate: &[u8],
        record: &Intent,
        memory: &mut CaptureMemory,
        fault: &Fault,
    ) -> Result<DurableIntentReceipt> {
        if self.uncertain {
            return Err(Error::Uncertain);
        }
        self.verify_owner()?;
        if self.publication_pending || self.has_pending()? {
            return Err(Error::Pending);
        }
        let maximum = self.limits.max_checkpoint_bytes;
        let source_identity = CheckpointIdentity::from_bytes(source.bytes(), maximum)
            .map_err(|_limit| Error::ResourceLimit)?;
        let candidate_identity = CheckpointIdentity::from_bytes(encoded_candidate, maximum)
            .map_err(|_limit| Error::ResourceLimit)?;
        if source_identity != source.identity()
            || source_identity != record.source
            || candidate_identity != record.candidate
            || source.snapshot().original_root.key != record.original_root
            || source.snapshot().original_root != candidate.original_root
        {
            return Err(Error::Corrupt);
        }
        // This fixed stack buffer and all validator/load scratch are charged
        // before any artifact can be created. No complete image is re-encoded.
        memory
            .buffer(
                intent::ENCODED_SIZE
                    .checked_mul(2)
                    .ok_or(Error::ResourceLimit)?,
            )
            .map_err(|_limit| Error::ResourceLimit)?;
        memory.nodes(6).map_err(|_limit| Error::ResourceLimit)?;
        let encoded_intent = record.encode(maximum).map_err(|error| match error {
            intent::Error::ResourceLimit => Error::ResourceLimit,
            _ => Error::Corrupt,
        })?;
        budgeted::validate(candidate, maximum, memory)?;
        budgeted::successor(source.snapshot(), candidate, memory)?;
        let current = self
            .load_exact_budgeted(memory)?
            .ok_or(Error::StaleSnapshot)?;
        if current.identity() != source_identity {
            return Err(Error::StaleSnapshot);
        }
        drop(current);
        self.verify_owner()?;
        fault.hit(Step::CreateDirectory)?;
        mkdir(&self.directory_file)?;
        // From here every failure retains the container and fences ordinary
        // publication. No removal, truncation or implied rollback occurs.
        self.uncertain = true;
        fault.hit(Step::ContainerSync)?;
        self.directory_file.sync_all()?;
        let directory = open_directory(&self.directory_file)?;
        write_image(
            &directory,
            c"source",
            source.bytes(),
            fault,
            Step::SourceWrite,
            Step::SourceSync,
        )?;
        write_image(
            &directory,
            c"candidate",
            encoded_candidate,
            fault,
            Step::CandidateWrite,
            Step::CandidateSync,
        )?;
        fault.hit(Step::ImageDirectorySync)?;
        directory.sync_all()?;
        write_image(
            &directory,
            c"intent.pending",
            &encoded_intent,
            fault,
            Step::IntentWrite,
            Step::IntentSync,
        )?;
        fault.hit(Step::Rename)?;
        // The newly created private container is exclusively owned by this
        // cooperating recovery owner. These checks do not claim hostile-writer
        // path atomicity or protect against an adversarial same-user process.
        absent(&directory, c"intent")?;
        // SAFETY: fixed components within the same retained directory.
        let renamed = unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                c"intent.pending".as_ptr(),
                directory.as_raw_fd(),
                c"intent".as_ptr(),
            )
        };
        if renamed != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        fault.hit(Step::DirectorySync)?;
        directory.sync_all()?;
        check_owner_paths(self)?;
        check_container(&self.directory_file, &directory)?;
        self.uncertain = false;
        Ok(DurableIntentReceipt { record: *record })
    }
}
fn mkdir(parent: &File) -> Result<()> {
    // SAFETY: live parent descriptor and a fixed single pathname component.
    if unsafe { libc::mkdirat(parent.as_raw_fd(), c"transition".as_ptr(), 0o700) } != 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::AlreadyExists {
            Err(Error::TransitionPending)
        } else {
            Err(error.into())
        };
    }
    Ok(())
}
fn open_directory(parent: &File) -> Result<File> {
    // SAFETY: live parent; directory-only/non-following fixed component.
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            c"transition".as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: new owned descriptor transferred exactly once.
    let directory = unsafe { File::from_raw_fd(descriptor) };
    private_metadata(&directory.metadata()?, true)?;
    Ok(directory)
}
fn write_image(
    directory: &File,
    name: &CStr,
    bytes: &[u8],
    fault: &Fault,
    write: Step,
    sync: Step,
) -> Result<()> {
    // SAFETY: fixed names and retained parent. O_EXCL forbids silent adoption
    // or replacement of any existing file, including symlinks.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK
                | libc::O_CLOEXEC,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: new owned descriptor transferred exactly once.
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    private_metadata(&file.metadata()?, false)?;
    fault.hit(write)?;
    file.write_all(bytes)?;
    fault.hit(sync)?;
    file.sync_all()?;
    Ok(())
}
fn absent(directory: &File, name: &CStr) -> Result<()> {
    let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: valid output pointer and fixed relative name; no link traversal.
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            observed.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } == 0
    {
        return Err(Error::TransitionPending);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(error.into())
    }
}
pub(super) fn check_owner_paths(recovery: &Recovery) -> Result<()> {
    private(&recovery.directory, true)?;
    let current = fs::symlink_metadata(&recovery.directory)?;
    let held = recovery.directory_file.metadata()?;
    if (current.dev(), current.ino()) != (held.dev(), held.ino()) {
        return Err(Error::PrivateStorageRequired);
    }
    // SAFETY: retained root and a fixed non-following read-only lock name.
    let descriptor = unsafe {
        libc::openat(
            recovery.directory_file.as_raw_fd(),
            c"owner.lock".as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: new owned descriptor transferred exactly once.
    let current = unsafe { File::from_raw_fd(descriptor) };
    let current = current.metadata()?;
    private_metadata(&current, false)?;
    let held = recovery._lock.metadata()?;
    if (current.dev(), current.ino()) != (held.dev(), held.ino()) {
        return Err(Error::PrivateStorageRequired);
    }
    Ok(())
}

fn check_container(parent: &File, held: &File) -> Result<()> {
    let current = open_directory(parent)?;
    let current = current.metadata()?;
    let held = held.metadata()?;
    if (current.dev(), current.ino()) != (held.dev(), held.ino()) {
        return Err(Error::PrivateStorageRequired);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;

impl lore_revision::fs::filesystem_provider::DurableRootIntent for DurableIntentReceipt {
    fn expected_anchors(&self) -> lore_revision::instance::strict_anchors::Snapshot {
        anchor_snapshot(self.record.old)
    }
    fn replacement_anchors(&self) -> lore_revision::instance::strict_anchors::Snapshot {
        anchor_snapshot(self.record.new)
    }
}
fn anchor_snapshot(value: intent::Anchors) -> lore_revision::instance::strict_anchors::Snapshot {
    use zerocopy::IntoBytes;
    let mut instance = lore_revision::instance::InstanceId::default();
    instance.as_mut_bytes().copy_from_slice(&value.instance);
    lore_revision::instance::strict_anchors::Snapshot {
        repository: value.repository,
        instance,
        current: value.current,
        branch: value.branch,
        staged: value.staged,
    }
}
