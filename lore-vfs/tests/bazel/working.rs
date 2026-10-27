// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::ffi::CString;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::MetadataExt;

use lore_vfs::linux_working::Model;
use lore_vfs::linux_working::StoreFactory;
use lore_vfs::namespace::Namespace;
use lore_vfs::overlay::Access;
use lore_vfs::overlay::AccessGate;
use lore_vfs::overlay::FileStore;
use lore_vfs::overlay::SparseStore;
use lore_vfs::recovery::Recovery;
use lore_vfs::storage_budget::StorageBudget;

use super::*;

fn digest(path: &Path) -> std::io::Result<Vec<u8>> {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut bytes = [0; 32];
    // SAFETY: path/name are terminated and the writable buffer has the given length.
    let count = unsafe {
        libc::getxattr(
            path.as_ptr(),
            c"user.sha256".as_ptr(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
        )
    };
    if count < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(bytes[..count as usize].to_vec())
    }
}

/// Run against one live writable mount and one persistent Bazel server. Both
/// edits keep the inode and byte length, so success cannot depend on replacement
/// inode identities from the read-only transition fixture.
fn exercise_same_inode_edits(mountpoint: &Path, builds: &Path, view: &View) {
    let input = mountpoint.join("input.txt");
    assert_eq!(build(mountpoint, builds, "initial"), b"old\n");
    assert!(
        view.digest_queries.load(Ordering::SeqCst) > 0,
        "Bazel did not request the clean digest"
    );
    assert_eq!(
        digest(&input).unwrap().as_slice(),
        ring::digest::digest(&ring::digest::SHA256, b"old\n").as_ref()
    );
    build(mountpoint, builds, "warm");
    let inode = fs::metadata(&input).unwrap().ino();
    for (label, bytes) in [("changed", b"new\n"), ("changed-again", b"end\n")] {
        let mut writer = fs::OpenOptions::new().write(true).open(&input).unwrap();
        assert_eq!(
            digest(&input).unwrap_err().raw_os_error(),
            Some(libc::ENODATA),
            "writable admission must revoke the digest before the first write"
        );
        writer.write_all(bytes).unwrap();
        writer.sync_all().unwrap();
        drop(writer);
        assert_eq!(fs::metadata(&input).unwrap().ino(), inode);
        assert_eq!(fs::metadata(&input).unwrap().len(), 4);
        assert_eq!(
            digest(&input).unwrap_err().raw_os_error(),
            Some(libc::ENODATA)
        );
        assert_eq!(build(mountpoint, builds, label), bytes);
    }
    let shutdown = bazel(builds)
        .current_dir(mountpoint)
        .arg("shutdown")
        .output()
        .unwrap();
    assert!(shutdown.status.success());
}

struct FixtureGate;
impl AccessGate for FixtureGate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, _access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if key.repository == [1; 16].into()
                && key.revision.hash == [1; 32].into()
                && key.exposure == 0
            {
                Ok(())
            } else {
                Err(Error::PermissionDenied)
            }
        })
    }
}

struct Stores {
    directory: PathBuf,
    next: AtomicUsize,
}
impl Stores {
    fn new(directory: PathBuf) -> Arc<Self> {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        Arc::new(Self {
            directory,
            next: AtomicUsize::new(0),
        })
    }
}
impl StoreFactory for Stores {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async move {
            let path = self
                .directory
                .join(self.next.fetch_add(1, Ordering::SeqCst).to_string());
            Ok(Arc::new(FileStore::create(&path).await?) as Arc<dyn SparseStore>)
        })
    }
}

#[test]
#[ignore = "requires native Linux writable FUSE and Bazel; run with a process timeout"]
fn mounted_bazel_same_inode_edits_and_checkpoint_restore() {
    let scratch = Directory(std::env::temp_dir().join(format!(
        "lore-bazel-working-{}-{}", std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    )));
    fs::create_dir(&scratch.0).unwrap();
    let ordinary = scratch.0.join("ordinary");
    let mountpoint = scratch.0.join("view");
    fs::create_dir(&ordinary).unwrap();
    fs::create_dir(&mountpoint).unwrap();
    let view = View::new(1, b"old\n", true);
    for (index, name) in NAMES.iter().enumerate() {
        fs::write(
            ordinary.join(std::str::from_utf8(name).unwrap()),
            view.bytes(index as u32 + 1),
        )
        .unwrap();
    }
    assert_eq!(
        build(&ordinary, &scratch.0.join("ordinary-build"), "initial"),
        b"old\n"
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let namespace = Arc::new(
        Namespace::new(
            view.clone(),
            view.node(0),
            Arc::new(FixtureGate),
            Default::default(),
        )
        .unwrap(),
    );
    let recovery_path = scratch.0.join("recovery");
    let model = runtime
        .block_on(Model::new(
            namespace,
            Stores::new(scratch.0.join("stores")),
            Arc::new(StorageBudget::new(1024 * 1024)),
            Recovery::open(&recovery_path, Default::default()).unwrap(),
            Default::default(),
        ))
        .unwrap();
    // SAFETY: these libc calls have no arguments or preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let mounted = lore_vfs::linux_working::mount(
        model.clone(),
        runtime.handle().clone(),
        &mountpoint,
        uid,
        gid,
    )
    .unwrap();
    let builds = scratch.0.join("mounted-build");
    exercise_same_inode_edits(&mountpoint, &builds, &view);
    mounted.unmount().unwrap();
    drop(model);

    let restored = runtime
        .block_on(Model::restore(
            view.clone(),
            view.node(0),
            Arc::new(FixtureGate),
            Default::default(),
            Stores::new(scratch.0.join("restored-stores")),
            Arc::new(StorageBudget::new(1024 * 1024)),
            Recovery::open(&recovery_path, Default::default()).unwrap(),
            Default::default(),
        ))
        .unwrap();
    let mounted =
        lore_vfs::linux_working::mount(restored, runtime.handle().clone(), &mountpoint, uid, gid)
            .unwrap();
    assert_eq!(fs::read(mountpoint.join("input.txt")).unwrap(), b"end\n");
    assert_eq!(
        digest(&mountpoint.join("input.txt"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ENODATA)
    );
    // Reusing the disk action cache after restart may legitimately avoid executing
    // the action. The output comparison still rejects stale pre-edit output.
    assert_eq!(build(&mountpoint, &builds, "warm"), b"end\n");
    assert!(
        bazel(&builds)
            .current_dir(&mountpoint)
            .arg("shutdown")
            .output()
            .unwrap()
            .status
            .success()
    );
    mounted.unmount().unwrap();
}
