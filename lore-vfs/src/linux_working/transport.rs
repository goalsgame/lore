// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
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
use fuser::ReplyStatfs;
use fuser::ReplyWrite;
use fuser::ReplyXattr;
use fuser::Request;
use fuser::TimeOrNow;
use tokio::runtime::Handle;

use super::Attributes;
use super::Error;
use super::Model;
use super::OpenFlags;
use crate::core::MAX_READ_SIZE;
use crate::core::NodeKind;
use crate::core::{self};
use crate::digest::XATTR_NAME;
use crate::namespace::NamespaceError;

pub struct Mounted {
    session: Option<BackgroundSession>,
    model: Arc<Model>,
    runtime: Handle,
    ended: Option<tokio::sync::oneshot::Receiver<()>>,
    ready: Option<tokio::sync::oneshot::Receiver<io::Result<()>>>,
}
impl Mounted {
    pub fn take_session_end(&mut self) -> Option<tokio::sync::oneshot::Receiver<()>> {
        self.ended.take()
    }
    /// Resolves when FUSE initialization accepts the requested configuration.
    /// Session termination before initialization resolves this as an error.
    pub fn take_readiness(&mut self) -> Option<tokio::sync::oneshot::Receiver<io::Result<()>>> {
        self.ready.take()
    }
    pub fn unmount(mut self) -> io::Result<()> {
        self.stop()
    }
    /// Teardown owns a dedicated thread so file-backed backend tasks can use a
    /// runtime configured with only one blocking worker.
    pub async fn unmount_async(self) -> io::Result<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("lore-working-unmount".into())
            .spawn(move || {
                let _ = sender.send(self.unmount());
            })?;
        receiver
            .await
            .map_err(|_closed| io::Error::other("mount owner stopped"))?
    }
    fn stop(&mut self) -> io::Result<()> {
        self.model.close_requests();
        let joined = self.session.take().map_or(Ok(()), |session| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| session.join()))
                .map_err(|_panic| io::Error::other("FUSE session failed"))
        });
        let drained = self
            .runtime
            .block_on(async {
                let requests = self.model.drain_requests().await;
                self.model
                    .checkpoint_owner()
                    .drain_async()
                    .await
                    .map_err(Error::from)
                    .and(requests)
            })
            .map_err(|error| io::Error::from_raw_os_error(errno(error)));
        joined.and(drained)
    }
}
impl Drop for Mounted {
    fn drop(&mut self) {
        if self.session.is_some() {
            let _ = self.stop();
        }
    }
}

/// Mount one private writable namespace. The caller supplies the namespace's
/// independent read/write policy; no production policy is synthesized here.
/// Ownership and synchronous drop must remain outside Tokio runtime workers.
pub fn mount(
    model: Arc<Model>,
    runtime: Handle,
    mountpoint: impl AsRef<Path>,
    uid: u32,
    gid: u32,
) -> io::Result<Mounted> {
    if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "FUSE requires a multi-thread runtime",
        ));
    }
    model
        .live()
        .map_err(|error| io::Error::from_raw_os_error(errno(error)))?;
    let target = std::fs::symlink_metadata(mountpoint.as_ref())?;
    if !target.is_dir()
        || target.file_type().is_symlink()
        || std::fs::read_dir(mountpoint.as_ref())?.next().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mountpoint must be an existing empty directory",
        ));
    }
    if model.mounted.swap(true, Ordering::AcqRel) {
        return Err(io::Error::from_raw_os_error(libc::EBUSY));
    }
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let adapter = Linux {
        model: model.clone(),
        runtime: runtime.clone(),
        uid,
        gid,
        ended: Some(sender),
        ready: ReadySignal(Some(ready_sender)),
    };
    let session = fuser::spawn_mount2(
        adapter,
        mountpoint,
        &[
            MountOption::FSName("lore-working-experimental".into()),
            MountOption::DefaultPermissions,
            MountOption::NoDev,
            MountOption::NoSuid,
        ],
    );
    match session {
        Ok(session) => Ok(Mounted {
            session: Some(session),
            model,
            runtime,
            ended: Some(receiver),
            ready: Some(ready_receiver),
        }),
        Err(error) => {
            model.close_requests();
            Err(error)
        }
    }
}

pub(super) fn errno(error: Error) -> i32 {
    match error {
        Error::Core(error) => match error {
            core::Error::NotFound => libc::ENOENT,
            core::Error::PermissionDenied => libc::EACCES,
            core::Error::NotDirectory => libc::ENOTDIR,
            core::Error::IsDirectory => libc::EISDIR,
            core::Error::InvalidInput => libc::EINVAL,
            core::Error::BadHandle => libc::EBADF,
            core::Error::Stale => libc::ESTALE,
            core::Error::ReadOnly => libc::EROFS,
            core::Error::Unsupported => libc::EOPNOTSUPP,
            core::Error::NoAttribute => libc::ENODATA,
            core::Error::Range => libc::ERANGE,
            core::Error::Io => libc::EIO,
            core::Error::Unmounted => libc::ENODEV,
            core::Error::ResourceLimit => libc::ENOSPC,
            core::Error::Busy => libc::EBUSY,
        },
        Error::Namespace(NamespaceError::Backend(error)) => errno(Error::Core(error)),
        Error::Namespace(NamespaceError::Busy) | Error::Recovery(crate::recovery::Error::Busy) => {
            libc::EBUSY
        }
        Error::Namespace(NamespaceError::Exists) => libc::EEXIST,
        Error::Namespace(NamespaceError::NotEmpty) => libc::ENOTEMPTY,
        Error::Namespace(NamespaceError::CrossBoundary) => libc::EXDEV,
        Error::Namespace(NamespaceError::Cycle)
        | Error::ExistingNamespace
        | Error::RestoreRequired => libc::EINVAL,
        Error::Recovery(crate::recovery::Error::ResourceLimit)
        | Error::StoreLimit
        | Error::SnapshotLimit => libc::ENOSPC,
        Error::Recovery(_) | Error::Poisoned => libc::EIO,
        Error::HandleLimit => libc::EMFILE,
        Error::FileTooLarge => libc::EFBIG,
    }
}
fn kind(kind: NodeKind) -> FileType {
    match kind {
        NodeKind::File => FileType::RegularFile,
        NodeKind::Directory => FileType::Directory,
        NodeKind::Symlink => FileType::Symlink,
    }
}
fn open_flags(flags: i32) -> std::result::Result<OpenFlags, i32> {
    if flags & libc::O_ACCMODE == libc::O_ACCMODE || flags & (libc::O_SYNC | libc::O_DSYNC) != 0 {
        return Err(libc::EOPNOTSUPP);
    }
    Ok(OpenFlags {
        write: flags & libc::O_ACCMODE != libc::O_RDONLY,
        append: flags & libc::O_APPEND != 0,
        truncate: flags & libc::O_TRUNC != 0,
    })
}
struct Linux {
    model: Arc<Model>,
    runtime: Handle,
    uid: u32,
    gid: u32,
    ended: Option<tokio::sync::oneshot::Sender<()>>,
    ready: ReadySignal,
}
struct ReadySignal(Option<tokio::sync::oneshot::Sender<io::Result<()>>>);
impl ReadySignal {
    fn complete(&mut self, result: io::Result<()>) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(result);
        }
    }
}
impl Drop for ReadySignal {
    fn drop(&mut self) {
        self.complete(Err(io::Error::from_raw_os_error(libc::ENODEV)));
    }
}
impl Linux {
    fn finish(&mut self) {
        self.ready
            .complete(Err(io::Error::from_raw_os_error(libc::ENODEV)));
        self.model.close_requests();
        if let Some(sender) = self.ended.take() {
            let _ = sender.send(());
        }
    }
    fn attr(&self, attr: &Attributes) -> FileAttr {
        let timestamp = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(attr.generation))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        FileAttr {
            ino: attr.entry.inode,
            size: attr.size,
            blocks: attr.size.div_ceil(512),
            atime: timestamp,
            mtime: timestamp,
            ctime: timestamp,
            crtime: SystemTime::UNIX_EPOCH,
            kind: kind(attr.entry.kind),
            perm: attr.mode,
            nlink: if !attr.linked {
                0
            } else if attr.entry.kind == NodeKind::Directory {
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
    fn xattr(&self, inode: u64) -> std::result::Result<Option<[u8; 32]>, i32> {
        self.runtime
            .block_on(self.model.digest(inode))
            .map_err(errno)
    }
}
impl Drop for Linux {
    fn drop(&mut self) {
        self.finish();
    }
}
fn xattr_reply(value: &[u8], size: u32, reply: ReplyXattr) {
    if size == 0 {
        reply.size(value.len() as u32);
    } else if (size as usize) < value.len() {
        reply.error(libc::ERANGE);
    } else {
        reply.data(value);
    }
}
impl Filesystem for Linux {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> std::result::Result<(), i32> {
        let result = config
            .set_max_write(MAX_READ_SIZE as u32)
            .map(|_previous| ())
            .map_err(|_size| libc::EINVAL);
        self.ready
            .complete(result.map_err(io::Error::from_raw_os_error));
        result
    }
    fn destroy(&mut self) {
        self.finish();
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if name.as_bytes().len() > 255 {
            reply.error(libc::ENAMETOOLONG);
            return;
        }
        match self
            .runtime
            .block_on(self.model.lookup(parent, name.as_bytes()))
        {
            Ok(attr) => reply.entry(&Duration::ZERO, &self.attr(&attr), 0),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn getattr(&mut self, _req: &Request<'_>, inode: u64, fh: Option<u64>, reply: ReplyAttr) {
        match self
            .runtime
            .block_on(self.model.getattr_with_handle(inode, fh))
        {
            Ok(attr) => reply.attr(&Duration::ZERO, &self.attr(&attr)),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn open(&mut self, _req: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        let result = open_flags(flags).and_then(|flags| {
            self.runtime
                .block_on(self.model.open(inode, flags))
                .map_err(errno)
        });
        match result {
            Ok(handle) => reply.opened(handle, fuser::consts::FOPEN_DIRECT_IO),
            Err(error) => reply.error(error),
        }
    }
    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let result = open_flags(flags).and_then(|flags| {
            self.runtime
                .block_on(self.model.create(
                    parent,
                    name.as_bytes(),
                    (mode & !umask & 0o777) as u16,
                    flags,
                ))
                .map_err(errno)
        });
        match result {
            Ok((handle, attr)) => reply.created(
                &Duration::ZERO,
                &self.attr(&attr),
                0,
                handle,
                fuser::consts::FOPEN_DIRECT_IO,
            ),
            Err(error) => reply.error(error),
        }
    }
    fn read(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self
            .runtime
            .block_on(self.model.read(fh, offset as u64, size as usize))
        {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn write(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self.runtime.block_on(self.model.write_with_append(
            fh,
            offset as u64,
            data,
            Some(flags & libc::O_APPEND != 0),
        )) {
            Ok(count) => reply.written(count as u32),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn flush(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.runtime.block_on(self.model.flush(fh)) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn release(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.runtime.block_on(self.model.release(fh)) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.runtime.block_on(self.model.fsync(fh, false)) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.runtime.block_on(self.model.fsync(fh, true)) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if uid.is_some()
            || gid.is_some()
            || atime.is_some()
            || mtime.is_some()
            || ctime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some()
            || flags.is_some()
            || mode.is_some_and(|mode| {
                mode & !(libc::S_IFMT | 0o777) != 0
                    || !matches!(mode & libc::S_IFMT, 0 | libc::S_IFREG)
            })
        {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        match self.runtime.block_on(self.model.setattr(
            inode,
            fh,
            size,
            mode.map(|mode| (mode & 0o777) as u16),
        )) {
            Ok(attr) => reply.attr(&Duration::ZERO, &self.attr(&attr)),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if mode & !umask & 0o777 != 0o755 {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        match self
            .runtime
            .block_on(self.model.mkdir(parent, name.as_bytes()))
        {
            Ok(attr) => reply.entry(&Duration::ZERO, &self.attr(&attr), 0),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self
            .runtime
            .block_on(self.model.remove(parent, name.as_bytes(), false))
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self
            .runtime
            .block_on(self.model.remove(parent, name.as_bytes(), true))
        {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        if flags != 0 {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        match self.runtime.block_on(self.model.rename(
            parent,
            name.as_bytes(),
            newparent,
            newname.as_bytes(),
        )) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn opendir(&mut self, _req: &Request<'_>, inode: u64, _flags: i32, reply: ReplyOpen) {
        match self.runtime.block_on(self.model.opendir(inode)) {
            Ok(handle) => reply.opened(handle, 0),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        _inode: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self
            .runtime
            .block_on(self.model.readdir(fh, offset as u64, 1024))
        {
            Ok(page) => {
                if page.entries.iter().any(|(_, entry)| entry.name.len() > 255) {
                    reply.error(libc::ENAMETOOLONG);
                    return;
                }
                for (cookie, entry) in page.entries {
                    if reply.add(
                        entry.entry.inode,
                        cookie as i64,
                        kind(entry.entry.kind),
                        OsStr::from_bytes(&entry.name),
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
        _inode: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        match self.runtime.block_on(self.model.release(fh)) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(errno(error)),
        }
    }
    fn statfs(&mut self, _req: &Request<'_>, _inode: u64, reply: ReplyStatfs) {
        let budget = self.model.budget();
        let free = budget.limit().saturating_sub(budget.reserved_bytes());
        reply.statfs(budget.limit(), free, free, 0, 0, 4096, 255, 1);
    }
    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        if name.as_bytes() != XATTR_NAME.as_bytes() {
            reply.error(libc::ENODATA);
            return;
        }
        match self.xattr(inode) {
            Ok(Some(digest)) => xattr_reply(&digest, size, reply),
            Ok(None) => reply.error(libc::ENODATA),
            Err(error) => reply.error(error),
        }
    }
    fn listxattr(&mut self, _req: &Request<'_>, inode: u64, size: u32, reply: ReplyXattr) {
        match self.xattr(inode) {
            Ok(Some(_digest)) => {
                let mut name = XATTR_NAME.as_bytes().to_vec();
                name.push(0);
                xattr_reply(&name, size, reply);
            }
            Ok(None) => xattr_reply(&[], size, reply),
            Err(error) => reply.error(error),
        }
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::ReadySignal;
    #[test]
    fn termination_before_initialization_resolves_error() {
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        drop(ReadySignal(Some(sender)));
        assert_eq!(
            receiver.try_recv().unwrap().unwrap_err().raw_os_error(),
            Some(libc::ENODEV)
        );
    }
    #[test]
    fn initialization_result_is_terminal() {
        for code in [None, Some(libc::EINVAL)] {
            let (sender, mut receiver) = tokio::sync::oneshot::channel();
            let mut signal = ReadySignal(Some(sender));
            signal
                .complete(code.map_or(Ok(()), |code| Err(std::io::Error::from_raw_os_error(code))));
            drop(signal);
            let result = receiver.try_recv().unwrap();
            assert_eq!(result.err().and_then(|error| error.raw_os_error()), code);
        }
    }
    #[test]
    fn cancelled_readiness_wait_does_not_block_termination() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        drop(receiver);
        let mut signal = ReadySignal(Some(sender));
        signal.complete(Ok(()));
        drop(signal);
    }
}
