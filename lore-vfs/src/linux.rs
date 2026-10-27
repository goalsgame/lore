// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Linux FUSE transport with explicitly controlled immutable revision views.
//!
//! Mounts are read-only and private to the mounting user. The runtime supplied to
//! `mount` must remain alive until the returned mount has been unmounted.
//! Mount ownership and synchronous teardown belong outside Tokio worker threads;
//! async callers should use `Mounted::unmount_async` to let backend IO progress.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;

use fuser::BackgroundSession;
use fuser::FileAttr;
use fuser::FileType;
use fuser::Filesystem;
use fuser::MountOption;
use fuser::ReplyAttr;
use fuser::ReplyCreate;
use fuser::ReplyData;
use fuser::ReplyDirectory;
use fuser::ReplyEmpty;
use fuser::ReplyEntry;
use fuser::ReplyOpen;
use fuser::ReplyWrite;
use fuser::ReplyXattr;
use fuser::Request;
use fuser::TimeOrNow;
use tokio::runtime::Handle;

use crate::core::Backend;
use crate::core::Core;
use crate::core::Entry;
use crate::core::Error;
use crate::core::MAX_DIRECTORY_ENTRIES;
use crate::core::MAX_READ_SIZE;
use crate::core::Node;
use crate::core::NodeKind;
use crate::core::transition::Invalidation;
use crate::core::transition::Invalidator;
use crate::core::transition::TransitionError;
use crate::core::transition::TransitionResult;
use crate::digest::XATTR_NAME;

// Cache positive entries only. ENOENT replies carry no negative TTL: kernels
// with incomplete negative-dentry invalidation cannot acknowledge cached misses.
const TTL: Duration = Duration::from_secs(1);
const NAME_MAX: usize = 255;

/// Owns the mounted session. Both explicit unmount and drop drain FUSE callbacks
/// after cancelling backend calls and revoking handles. A core belongs to exactly
/// one mount.
pub struct Mounted {
    session: Option<BackgroundSession>,
    core: Arc<Core>,
    ended: Option<tokio::sync::oneshot::Receiver<()>>,
    control: ReadOnlyControl,
}

impl Mounted {
    /// Control pinned read-only views without changing mount or CLI defaults.
    pub fn transition_control(&self) -> ReadOnlyControl {
        self.control.clone()
    }
    /// Receives normal destroy, session EOF or adapter drop. Teardown must still
    /// join the mounted owner before releasing its runtime and backend resources.
    pub fn take_session_end(&mut self) -> Option<tokio::sync::oneshot::Receiver<()>> {
        self.ended.take()
    }

    pub fn unmount(mut self) -> io::Result<()> {
        self.stop()
    }

    /// Unmount without blocking a Tokio worker needed by backend operations.
    pub async fn unmount_async(self) -> io::Result<()> {
        // Teardown needs no caller authorization; backend callbacks retain their
        // captured execution context independently until the session is drained.
        lore_base::lore_spawn_blocking_nocontext!(move || self.unmount())
            .await
            .map_err(|error| io::Error::other(format!("FUSE unmount task failed: {error}")))?
    }

    fn stop(&mut self) -> io::Result<()> {
        // Revoke before joining: the callback thread may be waiting on storage
        // that will never complete. Core shutdown drops those backend futures.
        let shutdown = self
            .core
            .shutdown()
            .map_err(|error| io::Error::from_raw_os_error(errno(error)));
        let result = self.session.take().map_or(Ok(()), |session| {
            // fuser 0.16 joins with unwrap; contain session errors and callback
            // panics so dropping a mount cannot unwind the application's stack.
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| session.join()))
                .map_err(|_session_panic| io::Error::other("FUSE session failed while unmounting"))
        });
        result.and(shutdown)
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Mount without `allow_other`, using kernel permission checks and a read-only
/// superblock. Requires Linux `/dev/fuse` and an installed `fusermount3` helper
/// for unprivileged mounting; this build does not link to libfuse.
pub fn mount(
    core: Arc<Core>,
    runtime: Handle,
    mountpoint: impl AsRef<Path>,
    uid: u32,
    gid: u32,
) -> io::Result<Mounted> {
    if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "FUSE requires a multi-thread Tokio runtime",
        ));
    }
    let (ended_sender, ended_receiver) = tokio::sync::oneshot::channel();
    let transition_capability = Arc::new(AtomicU8::new(0));
    let adapter = Linux {
        core: Arc::clone(&core),
        runtime,
        uid,
        gid,
        ended: Some(ended_sender),
        transition_capability: transition_capability.clone(),
    };
    let session = fuser::spawn_mount2(
        adapter,
        mountpoint,
        &[
            MountOption::RO,
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
            MountOption::FSName("lore".into()),
        ],
    )?;
    let control = ReadOnlyControl {
        core: core.clone(),
        invalidator: Arc::new(KernelInvalidator {
            notifier: session.notifier(),
            capability: transition_capability,
        }),
    };
    Ok(Mounted {
        control,
        session: Some(session),
        core,
        ended: Some(ended_receiver),
    })
}

/// Read-only transition control retained independently of the mounted owner.
/// Open file reads and directory enumeration remain pinned; root-relative lookup
/// follows the active view, including through an old root directory descriptor.
#[derive(Clone)]
pub struct ReadOnlyControl {
    core: Arc<Core>,
    invalidator: Arc<KernelInvalidator>,
}
impl ReadOnlyControl {
    fn ready(&self) -> TransitionResult<()> {
        self.core.getattr(1)?;
        transition_support(self.invalidator.capability.load(Ordering::Acquire))
    }
    /// Run outside FUSE callbacks and Tokio workers. Candidate paths can become
    /// visible before acknowledgment; an error rolls the active view back.
    pub fn transition(&self, backend: Arc<dyn Backend>, root: Node) -> TransitionResult<u64> {
        self.ready()?;
        self.core
            .transition(backend, root, self.invalidator.as_ref())
    }
    pub async fn transition_async(
        &self,
        backend: Arc<dyn Backend>,
        root: Node,
    ) -> TransitionResult<u64> {
        self.ready()?;
        self.core
            .clone()
            .transition_async(backend, root, self.invalidator.clone())
            .await
    }
    pub fn recover(&self) -> TransitionResult<()> {
        self.ready()?;
        self.core
            .recover_transition_cache(self.invalidator.as_ref())
    }
}
fn transition_support(status: u8) -> TransitionResult<()> {
    match status {
        2 => Ok(()),
        0 => Err(Error::Stale.into()),
        _ => Err(TransitionError::Unsupported),
    }
}
struct KernelInvalidator {
    notifier: fuser::Notifier,
    capability: Arc<AtomicU8>,
}
impl Invalidator for KernelInvalidator {
    fn invalidate(&self, plan: &Invalidation) -> io::Result<()> {
        // FUSE device writes synchronously return kernel notification errors.
        // Never hold the reply barrier while acquiring the kernel's inode locks.
        for name in &plan.root_names {
            self.notifier.inval_entry(1, OsStr::from_bytes(name))?;
        }
        self.notifier.inval_inode(1, -1, 0)
    }
}
// This scope includes sending the kernel reply, not just the backend future.
macro_rules! callback {
    ($this:ident,$reply:ident) => {
        let _callback = match $this.core.callback_guard() {
            Ok(guard) => guard,
            Err(error) => {
                $reply.error(errno(error));
                return;
            }
        };
    };
}

struct Linux {
    core: Arc<Core>,
    runtime: Handle,
    uid: u32,
    gid: u32,
    ended: Option<tokio::sync::oneshot::Sender<()>>,
    transition_capability: Arc<AtomicU8>,
}

impl Linux {
    fn session_ended(&mut self) {
        let _ = self.core.shutdown();
        if let Some(sender) = self.ended.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for Linux {
    fn drop(&mut self) {
        self.session_ended();
    }
}

fn errno(error: Error) -> i32 {
    match error {
        Error::NotFound => libc::ENOENT,
        Error::PermissionDenied => libc::EACCES,
        Error::NotDirectory => libc::ENOTDIR,
        Error::IsDirectory => libc::EISDIR,
        Error::InvalidInput => libc::EINVAL,
        Error::BadHandle => libc::EBADF,
        Error::Stale => libc::ESTALE,
        Error::ReadOnly => libc::EROFS,
        Error::Unsupported => libc::EOPNOTSUPP,
        Error::NoAttribute => libc::ENODATA,
        Error::Range => libc::ERANGE,
        Error::Io => libc::EIO,
        Error::Unmounted => libc::ENODEV,
        Error::ResourceLimit => libc::EMFILE,
        Error::Busy => libc::EBUSY,
    }
}

fn kind(kind: NodeKind) -> FileType {
    match kind {
        NodeKind::File => FileType::RegularFile,
        NodeKind::Directory => FileType::Directory,
        NodeKind::Symlink => FileType::Symlink,
    }
}

impl Linux {
    fn digest_xattr(&self, ino: u64, name: &OsStr) -> Result<[u8; 32], i32> {
        if name.as_bytes() != XATTR_NAME.as_bytes() {
            return Err(libc::ENODATA);
        }
        self.runtime
            .block_on(self.core.digest(ino))
            .map_err(errno)?
            .ok_or(libc::ENODATA)
    }

    fn attr(&self, entry: &Entry) -> FileAttr {
        FileAttr {
            ino: entry.inode,
            size: entry.node.size,
            blocks: entry.node.size.div_ceil(512),
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            kind: kind(entry.node.kind),
            perm: entry.node.mode & !0o222,
            nlink: if entry.node.kind == NodeKind::Directory {
                2
            } else {
                1
            },
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum XattrValue<'a> {
    Size(u32),
    Data(&'a [u8]),
}

// Size probes report the exact byte count; a short buffer never gets a prefix.
fn xattr_value(value: &[u8], size: u32) -> Result<XattrValue<'_>, i32> {
    if size == 0 {
        Ok(XattrValue::Size(value.len() as u32))
    } else if (size as usize) < value.len() {
        Err(libc::ERANGE)
    } else {
        Ok(XattrValue::Data(value))
    }
}

fn xattr_reply(value: &[u8], size: u32, reply: ReplyXattr) {
    match xattr_value(value, size) {
        Ok(XattrValue::Size(size)) => reply.size(size),
        Ok(XattrValue::Data(data)) => reply.data(data),
        Err(error) => reply.error(error),
    }
}

impl Filesystem for Linux {
    fn init(&mut self, _req: &Request<'_>, config: &mut fuser::KernelConfig) -> Result<(), i32> {
        // Explicit invalidation support provides a conservative protocol gate.
        let supported = config
            .add_capabilities(fuser::consts::FUSE_EXPLICIT_INVAL_DATA)
            .is_ok();
        self.transition_capability
            .store(if supported { 2 } else { 1 }, Ordering::Release);
        Ok(())
    }

    fn destroy(&mut self) {
        self.session_ended();
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        callback!(self, reply);
        if name.as_bytes().len() > NAME_MAX {
            reply.error(libc::ENAMETOOLONG);
            return;
        }
        match self
            .runtime
            .block_on(self.core.lookup(parent, name.as_bytes()))
        {
            Ok(entry) => reply.entry(&TTL, &self.attr(&entry), 0),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        callback!(self, reply);
        match self.core.getattr(ino) {
            Ok(entry) => reply.attr(&TTL, &self.attr(&entry)),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        callback!(self, reply);
        if flags & libc::O_ACCMODE != libc::O_RDONLY
            || flags & (libc::O_TRUNC | libc::O_APPEND) != 0
        {
            reply.error(libc::EROFS);
            return;
        }
        match self
            .runtime
            .block_on(self.core.revalidate(ino))
            .and_then(|()| self.core.open(ino))
        {
            Ok(handle) => reply.opened(handle, 0),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        callback!(self, reply);
        if offset < 0 || size as usize > MAX_READ_SIZE {
            reply.error(libc::EINVAL);
            return;
        }
        match self
            .runtime
            .block_on(self.core.read(fh, offset as u64, size as usize))
        {
            Ok(data) => reply.data(&data),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        callback!(self, reply);
        match self.core.release(fh) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        callback!(self, reply);
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            reply.error(libc::EROFS);
            return;
        }
        match self
            .runtime
            .block_on(self.core.revalidate(ino))
            .and_then(|()| self.core.opendir(ino))
        {
            Ok(handle) => reply.opened(handle, 0),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        callback!(self, reply);
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let entry = match self.core.getattr(ino) {
            Ok(entry) => entry,
            Err(error) => {
                reply.error(errno(error));
                return;
            }
        };
        // Fetch before replying so invalid or stale handles fail even for dot entries.
        let cookie = if offset <= 2 { 0 } else { offset as u64 };
        match self
            .runtime
            .block_on(self.core.readdir(fh, cookie, MAX_DIRECTORY_ENTRIES))
        {
            Ok(page) => {
                if page.entries.iter().any(|child| child.name.len() > NAME_MAX) {
                    reply.error(libc::ENAMETOOLONG);
                    return;
                }
                if offset == 0 && reply.add(ino, 1, FileType::Directory, ".") {
                    reply.ok();
                    return;
                }
                if offset <= 1 && reply.add(entry.parent, 2, FileType::Directory, "..") {
                    reply.ok();
                    return;
                }
                for child in page.entries {
                    if reply.add(
                        child.entry.inode,
                        child.cookie as i64,
                        kind(child.entry.node.kind),
                        OsStr::from_bytes(&child.name),
                    ) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(error) => reply.error(errno(error)),
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        callback!(self, reply);
        match self.core.release(fh) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        callback!(self, reply);
        match self.runtime.block_on(self.core.readlink(ino)) {
            Ok(target) => reply.data(&target),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        callback!(self, reply);
        match self.digest_xattr(ino, name) {
            Ok(digest) => xattr_reply(&digest, size, reply),
            Err(error) => reply.error(error),
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        callback!(self, reply);
        match self.runtime.block_on(self.core.digest(ino)) {
            Ok(Some(_)) => xattr_reply(b"user.sha256\0", size, reply),
            Ok(None) => xattr_reply(b"", size, reply),
            Err(error) => reply.error(errno(error)),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(libc::EROFS);
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }
    fn unlink(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::EROFS);
    }
    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::EROFS);
    }
    fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }
    fn rename(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }
    fn link(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _parent: u64,
        _name: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }
    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(libc::EROFS);
    }
    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(libc::EROFS);
    }
    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }
    fn removexattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::EROFS);
    }
    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }
    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        _ino_in: u64,
        _fh_in: u64,
        _offset_in: i64,
        _ino_out: u64,
        _fh_out: u64,
        _offset_out: i64,
        _len: u64,
        _flags: u32,
        reply: ReplyWrite,
    ) {
        reply.error(libc::EROFS);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use lore_base::types::Address;
    use lore_base::types::RepositoryId;

    use super::*;
    use crate::core::Backend;
    use crate::core::BackendFuture;
    use crate::core::BackendPage;
    use crate::core::NamedNode;
    use crate::core::Node;
    use crate::core::NodeKey;

    struct Fixture {
        root: Node,
        file: Node,
        reads: AtomicUsize,
        deny_open: std::sync::atomic::AtomicBool,
        blocked_read: Option<(tokio::sync::Notify, tokio::sync::Notify)>,
        digest: [u8; 32],
    }

    impl Fixture {
        fn new() -> Self {
            let root = Node {
                key: NodeKey {
                    repository: RepositoryId::from([1; 16]),
                    revision: Address::default(),
                    node: 0,
                    exposure: 0,
                },
                kind: NodeKind::Directory,
                size: 0,
                mode: 0o755,
            };
            let file = Node {
                key: NodeKey {
                    node: 1,
                    ..root.key
                },
                kind: NodeKind::File,
                size: 5,
                mode: 0o644,
            };
            Self {
                root,
                file,
                reads: AtomicUsize::new(0),
                deny_open: std::sync::atomic::AtomicBool::new(false),
                blocked_read: None,
                digest: ring::digest::digest(&ring::digest::SHA256, b"hello")
                    .as_ref()
                    .try_into()
                    .unwrap(),
            }
        }
    }

    impl Backend for Fixture {
        fn revalidate<'a>(&'a self, _key: &'a NodeKey, _write: bool) -> BackendFuture<'a, ()> {
            Box::pin(async move {
                if self.deny_open.load(Ordering::SeqCst) {
                    Err(Error::PermissionDenied)
                } else {
                    Ok(())
                }
            })
        }
        fn lookup<'a>(&'a self, _parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
            Box::pin(async move {
                if name == b"hello" {
                    Ok(self.file.clone())
                } else if name == b"unknown" {
                    Ok(Node {
                        key: NodeKey {
                            node: 2,
                            ..self.file.key
                        },
                        ..self.file.clone()
                    })
                } else {
                    Err(Error::NotFound)
                }
            })
        }
        fn list<'a>(
            &'a self,
            _directory: &'a NodeKey,
            offset: u64,
            limit: usize,
        ) -> BackendFuture<'a, BackendPage> {
            Box::pin(async move {
                let entries: Vec<_> = [
                    NamedNode {
                        name: b"hello".to_vec(),
                        node: self.file.clone(),
                    },
                    NamedNode {
                        name: b"unknown".to_vec(),
                        node: Node {
                            key: NodeKey {
                                node: 2,
                                ..self.file.key
                            },
                            ..self.file.clone()
                        },
                    },
                ]
                .into_iter()
                .skip(offset as usize)
                .take(limit)
                .collect();
                let next = offset + entries.len() as u64;
                Ok(BackendPage {
                    entries,
                    next_offset: (next < 2).then_some(next),
                })
            })
        }
        fn read<'a>(
            &'a self,
            _file: &'a NodeKey,
            offset: u64,
            size: usize,
        ) -> BackendFuture<'a, Vec<u8>> {
            Box::pin(async move {
                self.reads.fetch_add(1, Ordering::SeqCst);
                if let Some((started, resume)) = &self.blocked_read {
                    started.notify_one();
                    resume.notified().await;
                }
                let start = (offset as usize).min(5);
                Ok(b"hello"[start..(start + size).min(5)].to_vec())
            })
        }
        fn digest<'a>(&'a self, file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
            Box::pin(async move { Ok((file.node == 1).then_some(self.digest)) })
        }
    }

    fn getxattr(path: &Path, name: &str, value: Option<&mut [u8]>) -> io::Result<usize> {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let name = CString::new(name).unwrap();
        let (buffer, size) = value.map_or((std::ptr::null_mut(), 0), |v| {
            (v.as_mut_ptr().cast(), v.len())
        });
        // SAFETY: path/name are NUL-terminated and buffer is valid for size bytes.
        let result = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), buffer, size) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    #[test]
    fn transition_initialization_is_retryable_and_unsupported_is_explicit() {
        assert!(matches!(
            transition_support(0),
            Err(TransitionError::Core(Error::Stale))
        ));
        assert!(matches!(
            transition_support(1),
            Err(TransitionError::Unsupported)
        ));
        assert!(transition_support(2).is_ok());
    }

    #[test]
    fn xattr_buffer_contract() {
        let digest = [0xab; 32];
        assert_eq!(xattr_value(&digest, 0), Ok(XattrValue::Size(32)));
        assert_eq!(xattr_value(&digest, 31), Err(libc::ERANGE));
        assert_eq!(xattr_value(&digest, 32), Ok(XattrValue::Data(&digest)));
        assert_eq!(xattr_value(&digest, 64), Ok(XattrValue::Data(&digest)));
        assert_eq!(xattr_value(b"user.sha256\0", 0), Ok(XattrValue::Size(12)));
        assert_eq!(xattr_value(b"user.sha256\0", 11), Err(libc::ERANGE));
        assert_eq!(xattr_value(b"", 0), Ok(XattrValue::Size(0)));
        assert_eq!(xattr_value(b"", 1), Ok(XattrValue::Data(b"")));
    }

    #[test]
    fn digest_lookup_does_not_read_content() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let backend = Arc::new(Fixture::new());
        let core = Arc::new(Core::new(backend.clone(), backend.root.clone()).unwrap());
        let entry = runtime.block_on(core.lookup(1, b"hello")).unwrap();
        let adapter = Linux {
            core,
            runtime: runtime.handle().clone(),
            uid: 0,
            gid: 0,
            ended: None,
            transition_capability: Arc::new(AtomicU8::new(0)),
        };
        assert_eq!(
            adapter.digest_xattr(entry.inode, OsStr::new(XATTR_NAME)),
            Ok(backend.digest)
        );
        assert_eq!(
            adapter.digest_xattr(entry.inode, OsStr::new("user.missing")),
            Err(libc::ENODATA)
        );
        assert_eq!(backend.reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn current_thread_runtime_is_rejected_before_mounting() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let backend = Arc::new(Fixture::new());
        let core = Arc::new(Core::new(backend.clone(), backend.root.clone()).unwrap());
        let error = mount(core, runtime.handle().clone(), "/unused", 0, 0)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn session_destroy_revokes_core_handles() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let backend = Arc::new(Fixture::new());
        let core = Arc::new(Core::new(backend.clone(), backend.root.clone()).unwrap());
        let handle = core.opendir(1).unwrap();
        let mut adapter = Linux {
            core: core.clone(),
            runtime: runtime.handle().clone(),
            uid: 0,
            gid: 0,
            ended: None,
            transition_capability: Arc::new(AtomicU8::new(0)),
        };
        adapter.destroy();
        assert_eq!(core.release(handle), Err(Error::Unmounted));
        assert_eq!(core.getattr(1), Err(Error::Unmounted));
    }

    fn listxattr(path: &Path, value: Option<&mut [u8]>) -> io::Result<usize> {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let (buffer, size) = value.map_or((std::ptr::null_mut(), 0), |v| {
            (v.as_mut_ptr().cast(), v.len())
        });
        // SAFETY: path is NUL-terminated and buffer is valid for size bytes.
        let result = unsafe { libc::listxattr(path.as_ptr(), buffer, size) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    /// Linux runtime gate: /dev/fuse, fusermount3 in PATH, permission to mount FUSE.
    /// Run: `cargo test -p lore-vfs linux::tests::mounted_contract -- --ignored --exact`
    #[test]
    #[ignore = "requires a Linux host with unprivileged FUSE mounting enabled"]
    fn mounted_contract() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let backend = Arc::new(Fixture::new());
        let core = Arc::new(Core::new(backend.clone(), backend.root.clone()).unwrap());
        let directory = std::env::temp_dir().join(format!(
            "lore-fuse-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        // SAFETY: getuid/getgid have no arguments or preconditions.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        let mounted = mount(core.clone(), runtime.handle().clone(), &directory, uid, gid).unwrap();
        let file = directory.join("hello");
        assert_eq!(getxattr(&file, XATTR_NAME, None).unwrap(), 32);
        let mut short = [0; 31];
        assert_eq!(
            getxattr(&file, XATTR_NAME, Some(&mut short))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ERANGE)
        );
        let mut digest = [0; 32];
        assert_eq!(getxattr(&file, XATTR_NAME, Some(&mut digest)).unwrap(), 32);
        assert_eq!(digest, backend.digest);
        assert_eq!(listxattr(&file, None).unwrap(), 12);
        let mut names = [0; 12];
        assert_eq!(listxattr(&file, Some(&mut names)).unwrap(), 12);
        assert_eq!(&names, b"user.sha256\0");
        assert_eq!(
            listxattr(&file, Some(&mut names[..11]))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ERANGE)
        );
        let unknown = directory.join("unknown");
        assert_eq!(
            getxattr(&unknown, XATTR_NAME, None)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENODATA)
        );
        assert_eq!(listxattr(&unknown, None).unwrap(), 0);
        assert_eq!(
            getxattr(&file, "user.missing", None)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENODATA)
        );
        assert_eq!(
            backend.reads.load(Ordering::SeqCst),
            0,
            "xattr must not hydrate content"
        );
        let names: Vec<_> = fs::read_dir(&directory)
            .unwrap()
            .map(|v| v.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![OsStr::new("hello"), OsStr::new("unknown")]);
        assert_eq!(fs::read(&file).unwrap(), b"hello");
        assert_eq!(
            fs::write(&file, b"other").unwrap_err().raw_os_error(),
            Some(libc::EROFS)
        );
        assert_eq!(
            fs::remove_file(&file).unwrap_err().raw_os_error(),
            Some(libc::EROFS)
        );
        assert_eq!(
            fs::create_dir(directory.join("new"))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EROFS)
        );
        // Name and file data are warm. A new open must still check authority.
        backend.deny_open.store(true, Ordering::SeqCst);
        assert_eq!(
            fs::read(&file).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(
            matches!(fs::read_dir(&directory), Err(error) if error.kind() == io::ErrorKind::PermissionDenied)
        );
        mounted.unmount().unwrap();
        assert_eq!(core.getattr(1), Err(Error::Unmounted));
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    #[ignore = "requires a Linux host with unprivileged FUSE mounting enabled"]
    fn unmount_cancels_blocked_backend_read() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let backend = Arc::new(Fixture {
            blocked_read: Some((tokio::sync::Notify::new(), tokio::sync::Notify::new())),
            ..Fixture::new()
        });
        let core = Arc::new(Core::new(backend.clone(), backend.root.clone()).unwrap());
        let directory = std::env::temp_dir().join(format!(
            "lore-fuse-blocked-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        // SAFETY: getuid/getgid have no arguments or preconditions.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        let mounted = mount(core, runtime.handle().clone(), &directory, uid, gid).unwrap();
        let path = directory.join("hello");
        let reader = std::thread::spawn(move || fs::read(path));
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(5),
                backend.blocked_read.as_ref().unwrap().0.notified(),
            )
            .await
            .unwrap();
        });
        let (done, result) = std::sync::mpsc::channel();
        let owner = std::thread::spawn(move || {
            done.send(mounted.unmount()).unwrap();
        });
        let stopped = result.recv_timeout(Duration::from_secs(5));
        // Release the fixture even on a regression, so the test can clean up.
        backend.blocked_read.as_ref().unwrap().1.notify_one();
        owner.join().unwrap();
        let read = reader.join().unwrap();
        fs::remove_dir(directory).unwrap();
        stopped
            .expect("unmount waited for blocked storage instead of cancelling it")
            .unwrap();
        assert!(
            read.is_err(),
            "shutdown must not publish a late successful read"
        );
    }
}

#[cfg(test)]
#[path = "linux_transition_tests.rs"]
mod transition_tests;
