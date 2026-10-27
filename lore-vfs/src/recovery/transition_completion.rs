// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Exact selected checkpoint publication precedes durable transition cleanup.
//! A fixed cleanup marker retains the existing Intent bytes until every removed
//! artifact name and the selected checkpoint are durable.
use std::ffi::CStr;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::fs::MetadataExt;

use super::*;
use crate::capture_memory::CaptureMemory;
use crate::recovery::intent::CheckpointIdentity;
use crate::recovery::intent::Intent;
use crate::recovery::intent::Selection;
use crate::recovery::intent_writer::DurableIntentReceipt;

/// Exact receipt and actual serialized anchor read, retained through completion.
/// Only the receipt-bound check below can construct this proof.
pub(crate) struct CompletionProof<'a> {
    receipt: &'a DurableIntentReceipt,
    _command: std::sync::Arc<lore_revision::repository::WorkingCommandHandoff>,
}
impl DurableIntentReceipt {
    pub(crate) async fn confirm_completion(
        &self,
        command: &std::sync::Arc<lore_revision::repository::WorkingCommandHandoff>,
        maximum: usize,
    ) -> Result<CompletionProof<'_>> {
        let anchors = command
            .read_anchors()
            .await
            .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?;
        if self
            .record()
            .classify(anchors, maximum)
            .map_err(|_invalid| Error::Corrupt)?
            != Selection::Candidate
        {
            return Err(Error::StaleSnapshot);
        }
        Ok(CompletionProof {
            receipt: self,
            _command: command.clone(),
        })
    }
}
impl Recovery {
    /// The proof retains the exact root command handoff. The acknowledgment and
    /// installed candidate remain owned until completion and actual worker join.
    pub(crate) fn complete_transition(
        &mut self,
        proof: &CompletionProof<'_>,
        candidate: &crate::linux_working::CapturedCheckpointImage<'_>,
        memory: &mut CaptureMemory,
    ) -> Result<()> {
        let record = proof.receipt.record();
        publish_selected(
            self,
            candidate.snapshot(),
            candidate.bytes(),
            record.candidate,
            record,
            memory,
        )?;
        cleanup(self, record, memory)
    }
}
// Only opaque candidate/loaded-image paths call this private engine. No separate
// snapshot/bytes pair is accepted at the public transition-completion boundary.
pub(super) fn publish_selected(
    recovery: &mut Recovery,
    snapshot: &NamespaceSnapshot,
    bytes: &[u8],
    selected: CheckpointIdentity,
    record: &Intent,
    memory: &mut CaptureMemory,
) -> Result<()> {
    crate::recovery::intent_writer::check_owner_paths(recovery)?;
    if snapshot.original_root.key != record.original_root
        || CheckpointIdentity::from_bytes(bytes, recovery.limits.max_checkpoint_bytes)
            .map_err(|_limit| Error::ResourceLimit)?
            != selected
        || ![record.source, record.candidate].contains(&selected)
    {
        return Err(Error::Corrupt);
    }
    budgeted::validate(snapshot, recovery.limits.max_checkpoint_bytes, memory)?;
    let current = recovery
        .load_exact_budgeted(memory)?
        .ok_or(Error::StaleSnapshot)?;
    if ![record.source, record.candidate].contains(&current.identity()) {
        return Err(Error::StaleSnapshot);
    }
    drop(current);
    discard_known_pending(recovery, bytes, bytes, memory)?;
    // The source was acknowledged before intent. Intent selection may restore
    // it after an interrupted candidate completion, so this is not an ordinary
    // monotonically newer publication and never uses LATEST.
    recovery.uncertain = true;
    recovery.write_checkpoint_bytes(bytes)?;
    crate::recovery::intent_writer::check_owner_paths(recovery)?;
    Ok(())
}
pub(super) fn cleanup(
    recovery: &mut Recovery,
    record: &Intent,
    memory: &mut CaptureMemory,
) -> Result<()> {
    memory
        .buffer(intent::ENCODED_SIZE)
        .map_err(|_limit| Error::ResourceLimit)?;
    let bytes = record
        .encode(recovery.limits.max_checkpoint_bytes)
        .map_err(|_record| Error::Corrupt)?;
    recovery.uncertain = true;
    // A newly durable marker precedes every deletion from the original bundle.
    let marker = match create(&recovery.directory_file, c"transition.cleanup") {
        Ok(mut marker) => {
            marker.write_all(&bytes)?;
            marker
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut marker = read_file(&recovery.directory_file, c"transition.cleanup")?;
            let mut observed = [0; intent::ENCODED_SIZE];
            marker.read_exact(&mut observed)?;
            let mut extra = [0];
            if marker.read(&mut extra)? != 0 || observed != bytes {
                return Err(Error::Corrupt);
            }
            marker
        }
        Err(error) => return Err(error),
    };
    marker.sync_all()?;
    recovery.directory_file.sync_all()?;
    drop(marker);
    cleanup_marked(recovery)
}
pub(super) fn cleanup_marked(recovery: &mut Recovery) -> Result<()> {
    match open_transition(&recovery.directory_file) {
        Ok(directory) => {
            for name in [c"source", c"candidate", c"intent.pending", c"intent"] {
                remove(&directory, name, false)?;
            }
            directory.sync_all()?;
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    remove(&recovery.directory_file, c"transition", true)?;
    recovery.directory_file.sync_all()?;
    remove(&recovery.directory_file, c"transition.cleanup", false)?;
    recovery.directory_file.sync_all()?;
    recovery.publication_pending = false;
    recovery.uncertain = false;
    Ok(())
}
/// Startup may discard only a private interrupted pending file whose complete
/// bytes are an exact prefix of one independently validated intent image.
pub(super) fn discard_known_pending(
    recovery: &mut Recovery,
    source: &[u8],
    candidate: &[u8],
    memory: &mut CaptureMemory,
) -> Result<()> {
    // SAFETY: fixed pending filename and retained root directory; read-only/non-following.
    let descriptor = unsafe {
        libc::openat(
            recovery.directory_file.as_raw_fd(),
            c"pending".as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error.into())
        };
    }
    // SAFETY: new owned non-following descriptor transferred exactly once.
    let mut pending = unsafe { File::from_raw_fd(descriptor) };
    let before = pending.metadata()?;
    private_metadata(&before, false)?;
    let length = usize::try_from(before.len()).map_err(|_length| Error::ResourceLimit)?;
    let mut source_matches = length <= source.len();
    let mut candidate_matches = length <= candidate.len();
    if !source_matches && !candidate_matches {
        return Err(Error::Pending);
    }
    memory.buffer(4096).map_err(|_limit| Error::ResourceLimit)?;
    let mut scratch = [0; 4096];
    let mut position = 0;
    while position < length {
        let count = scratch.len().min(length - position);
        pending.read_exact(&mut scratch[..count])?;
        source_matches &= source.get(position..position + count) == Some(&scratch[..count]);
        candidate_matches &= candidate.get(position..position + count) == Some(&scratch[..count]);
        position += count;
    }
    let mut extra = [0];
    if pending.read(&mut extra)? != 0 {
        return Err(Error::Pending);
    }
    let after = pending.metadata()?;
    if !source_matches && !candidate_matches || stamp(&before) != stamp(&after) {
        return Err(Error::Pending);
    }
    // SAFETY: fixed pending filename and retained root directory; read-only/non-following.
    let descriptor = unsafe {
        libc::openat(
            recovery.directory_file.as_raw_fd(),
            c"pending".as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: new owned descriptor transferred exactly once.
    let current = unsafe { File::from_raw_fd(descriptor) };
    if stamp(&current.metadata()?) != stamp(&before) {
        return Err(Error::Pending);
    }
    remove(&recovery.directory_file, c"pending", false)?;
    recovery.directory_file.sync_all()?;
    recovery.publication_pending = false;
    Ok(())
}
fn stamp(metadata: &std::fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}
pub(super) fn read_file(parent: &File, name: &CStr) -> Result<File> {
    // SAFETY: fixed name within the retained parent; no symlink or blocking special-file traversal.
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: new owned descriptor transferred exactly once.
    let file = unsafe { File::from_raw_fd(descriptor) };
    private_metadata(&file.metadata()?, false)?;
    Ok(file)
}
pub(super) fn discard_partial_cleanup(recovery: &mut Recovery) -> Result<()> {
    // The caller already proved the complete original bundle and actual anchor choice.
    remove(&recovery.directory_file, c"transition.cleanup", false)?;
    recovery.directory_file.sync_all()?;
    Ok(())
}
fn create(parent: &File, name: &CStr) -> Result<File> {
    // SAFETY: fixed component and retained parent; exclusive, non-following creation.
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
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
    let file = unsafe { File::from_raw_fd(descriptor) };
    private_metadata(&file.metadata()?, false)?;
    Ok(file)
}
fn open_transition(parent: &File) -> Result<File> {
    // SAFETY: fixed directory component, retained parent, no link traversal.
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
    let file = unsafe { File::from_raw_fd(descriptor) };
    private_metadata(&file.metadata()?, true)?;
    Ok(file)
}
fn remove(parent: &File, name: &CStr, directory: bool) -> Result<()> {
    // SAFETY: fixed names under retained parent; no recursive/pathname following.
    if unsafe {
        libc::unlinkat(
            parent.as_raw_fd(),
            name.as_ptr(),
            if directory { libc::AT_REMOVEDIR } else { 0 },
        )
    } != 0
    {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(error.into());
        }
    }
    Ok(())
}
