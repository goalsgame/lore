// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::ffi::CString;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;

use super::*;
use crate::overlay::FileStore;

#[path = "watchers.rs"]
mod watchers;

struct DiskStores {
    directory: PathBuf,
    next: AtomicU64,
}
impl StoreFactory for DiskStores {
    fn create(&self) -> BackendFuture<'_, Arc<dyn SparseStore>> {
        Box::pin(async move {
            Ok(Arc::new(
                FileStore::create(
                    &self
                        .directory
                        .join(self.next.fetch_add(1, Ordering::AcqRel).to_string()),
                )
                .await?,
            ) as Arc<dyn SparseStore>)
        })
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap()
}
fn ids() -> (u32, u32) {
    // SAFETY: getuid/getgid have no argument preconditions.
    unsafe { (libc::getuid(), libc::getgid()) }
}
fn mount(
    model: Arc<Model>,
    runtime: &tokio::runtime::Runtime,
    path: &std::path::Path,
) -> crate::linux_working::Mounted {
    let (uid, gid) = ids();
    let mut mounted =
        crate::linux_working::mount(model, runtime.handle().clone(), path, uid, gid).unwrap();
    let ready = mounted.take_readiness().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), ready)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    });
    assert!(mounted.take_readiness().is_none());
    mounted
}
fn digest(path: &std::path::Path, size: usize) -> std::io::Result<Vec<u8>> {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut data = vec![0; size]; // SAFETY: live terminated strings and buffer sized as supplied.
    let count = unsafe {
        libc::getxattr(
            path.as_ptr(),
            c"user.sha256".as_ptr(),
            data.as_mut_ptr().cast(),
            data.len(),
        )
    };
    if count < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        data.truncate(count as usize);
        Ok(data)
    }
}
#[test]
#[ignore = "requires native Linux FUSE"]
#[allow(clippy::verbose_file_reads)] // The paths were replaced/unlinked; read the retained handles.
fn mounted_mutations_checkpoint_restart_and_limits() {
    let directory = Directory::new("kernel");
    let runtime = runtime();
    let base = Arc::new(Base::new());
    let gate = Arc::new(Gate::default());
    let stores = directory.0.join("stores");
    std::fs::create_dir(&stores).unwrap();
    let factory = Arc::new(DiskStores {
        directory: stores,
        next: AtomicU64::new(0),
    });
    let limits = Limits {
        max_handles: 16,
        max_stores: 32,
        ..Default::default()
    };
    let model = runtime
        .block_on(Model::new(
            Arc::new(
                Namespace::new(base.clone(), root(), gate.clone(), Default::default()).unwrap(),
            ),
            factory.clone(),
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            limits,
        ))
        .unwrap();
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let mounted = mount(model.clone(), &runtime, &mountpoint);
    let base_path = mountpoint.join("base");
    assert_eq!(digest(&base_path, 32).unwrap(), vec![42; 32]);
    assert_eq!(
        digest(&base_path, 31).unwrap_err().raw_os_error(),
        Some(libc::ERANGE)
    );
    gate.deny_write.store(true, Ordering::Release);
    assert_eq!(std::fs::read(&base_path).unwrap(), b"base");
    assert_eq!(
        std::fs::OpenOptions::new()
            .write(true)
            .open(&base_path)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EACCES)
    );
    gate.deny_write.store(false, Ordering::Release);
    std::fs::create_dir(mountpoint.join("dir")).unwrap();
    let a = mountpoint.join("dir/a");
    let b = mountpoint.join("dir/b");
    let mut old = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&a)
        .unwrap();
    old.write_all(b"old").unwrap();
    let inode = old.metadata().unwrap().ino();
    let mut replaced = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&b)
        .unwrap();
    replaced.write_all(b"new").unwrap();
    assert_eq!(
        digest(&a, 32).unwrap_err().raw_os_error(),
        Some(libc::ENODATA)
    );
    std::fs::rename(&a, &b).unwrap();
    assert_eq!(std::fs::metadata(&b).unwrap().ino(), inode);
    replaced.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    replaced.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"new");
    assert_eq!(replaced.metadata().unwrap().nlink(), 0);
    std::fs::remove_file(&b).unwrap();
    old.seek(SeekFrom::Start(0)).unwrap();
    bytes.clear();
    old.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"old");
    assert_eq!(old.metadata().unwrap().nlink(), 0);
    let path = mountpoint.join("persist");
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.write_all(b"hello").unwrap();
    file.set_len(3).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let recovery_path = directory.0.join("recovery");
    std::fs::set_permissions(&recovery_path, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert_eq!(file.sync_all().unwrap_err().raw_os_error(), Some(libc::EIO));
    assert_eq!(std::fs::read(&path).unwrap(), &b"hello"[..3]);
    std::fs::set_permissions(&recovery_path, std::fs::Permissions::from_mode(0o700)).unwrap();
    file.sync_all().unwrap();
    // Direct I/O must not negotiate writable shared mappings.
    // SAFETY: no dereference occurs; successful unexpected mappings are unmapped.
    let mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            4096,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if mapping != libc::MAP_FAILED {
        unsafe {
            libc::munmap(mapping, 4096);
        };
        panic!("writable mmap unexpectedly admitted");
    }
    {
        let mut append = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(mountpoint.join("append"))
            .unwrap();
        append.write_all(b"a").unwrap();
        // SAFETY: the live descriptor accepts F_GETFL/F_SETFL; no pointer args.
        let flags = unsafe { libc::fcntl(append.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(append.as_raw_fd(), libc::F_SETFL, flags & !libc::O_APPEND) },
            0
        );
        append.seek(SeekFrom::Start(0)).unwrap();
        append.write_all(b"b").unwrap();
        assert_eq!(std::fs::read(mountpoint.join("append")).unwrap(), b"b");
    }
    let dir = std::fs::File::open(mountpoint.join("dir")).unwrap();
    dir.sync_all().unwrap();
    drop(dir);
    drop(file);
    drop(old);
    drop(replaced);
    mounted.unmount().unwrap();
    drop(model);
    let restored = runtime
        .block_on(Model::restore(
            base,
            root(),
            gate,
            Default::default(),
            factory,
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            limits,
        ))
        .unwrap();
    let mounted = mount(restored.clone(), &runtime, &mountpoint);
    assert_eq!(
        std::fs::read(mountpoint.join("persist")).unwrap(),
        &b"hello"[..3]
    );
    assert_eq!(
        std::fs::metadata(mountpoint.join("persist"))
            .unwrap()
            .mode()
            & 0o777,
        0o755
    );
    assert!(!mountpoint.join("dir/a").exists());
    assert!(!mountpoint.join("dir/b").exists());
    assert_eq!(
        digest(&mountpoint.join("persist"), 32)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ENODATA)
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(mountpoint.join("persist"))
        .unwrap();
    assert_eq!(
        file.write(&[1; 1024]).unwrap_err().raw_os_error(),
        Some(libc::ENOSPC)
    );
    drop(file);
    assert_eq!(
        std::fs::read(mountpoint.join("persist")).unwrap(),
        &b"hello"[..3]
    );
    mounted.unmount().unwrap();
}
#[test]
#[ignore = "requires native Linux FUSE"]
fn mounted_teardown_cancels_blocked_read() {
    let directory = Directory::new("kernel-cancel");
    let runtime = runtime();
    let (model, base, _gate, _stores) =
        runtime.block_on(fixture(&directory, Default::default(), 1024));
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let mounted = mount(model.clone(), &runtime, &mountpoint);
    let path = mountpoint.join("base");
    base.block.store(true, Ordering::Release);
    let read = std::thread::spawn(move || std::fs::read(path));
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), base.started.notified())
            .await
            .unwrap();
    });
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(mounted.unmount());
    });
    receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("unmount waited for backend resume")
        .unwrap();
    assert!(read.join().unwrap().is_err());
    runtime.block_on(async {
        assert_eq!(
            model
                .namespace()
                .materialized_file(2)
                .await
                .unwrap()
                .unwrap()
                .metadata()
                .await
                .unwrap()
                .writers,
            0
        );
    });
}

// Retain ownership across assertions: Child::drop alone does not stop a mount.
struct OwnedChildMount {
    child: Option<std::process::Child>,
    mountpoint: PathBuf,
}
impl OwnedChildMount {
    fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self
            .child
            .take()
            .ok_or_else(|| std::io::Error::other("mount owner already reaped"))?;
        let mut failure = None;
        let mut reaped = match child.try_wait() {
            Ok(status) => status,
            Err(error) => {
                failure = Some(error);
                None
            }
        };
        if reaped.is_none()
            && let Err(error) = child.kill()
        {
            failure = Some(error);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = reaped.take() {
                break status;
            }
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {}
                Err(error) => {
                    failure = Some(error);
                }
            }
            if std::time::Instant::now() >= deadline {
                // Keep a reaper even if the kernel delays exit; test cleanup is bounded.
                std::thread::spawn(move || {
                    let _ = child.wait();
                });
                let _ = self.detach();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "owned mount owner exit timed out",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let detached = self.detach();
        if let Some(error) = failure {
            return Err(error);
        }
        detached?;
        Ok(status)
    }
    fn detach(&self) -> std::io::Result<()> {
        // The unique fixture path is owned by this guard. A failed startup may
        // have left it as an ordinary directory rather than a mount.
        let parent = self
            .mountpoint
            .parent()
            .ok_or_else(|| std::io::Error::other("mount parent missing"))?;
        if let (Ok(target), Ok(parent)) = (
            std::fs::metadata(&self.mountpoint),
            std::fs::metadata(parent),
        ) && target.dev() == parent.dev()
        {
            return Ok(());
        }
        let mut helper = std::process::Command::new("fusermount3")
            .arg("-uz")
            .arg(&self.mountpoint)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let polled = match helper.try_wait() {
                Ok(status) => status,
                Err(error) => {
                    let _ = helper.kill();
                    std::thread::spawn(move || {
                        let _ = helper.wait();
                    });
                    return Err(error);
                }
            };
            if let Some(status) = polled {
                return if status.success() {
                    Ok(())
                } else {
                    Err(std::io::Error::other("owned stale mount cleanup failed"))
                };
            }
            if std::time::Instant::now() >= deadline {
                let _ = helper.kill();
                std::thread::spawn(move || {
                    let _ = helper.wait();
                });
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "owned mount cleanup timed out",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for OwnedChildMount {
    fn drop(&mut self) {
        if self.child.is_some()
            && let Err(error) = self.stop()
        {
            eprintln!(
                "owned child/mount cleanup failed at {}: {error}",
                self.mountpoint.display()
            );
        }
    }
}

fn spawn_owner(directory: &Directory) -> OwnedChildMount {
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "linux_working::model::tests::mounted::abrupt_exit_child",
            "--ignored",
            "--nocapture",
        ])
        .env("LORE_WORKING_CHILD_DIRECTORY", &directory.0)
        .spawn()
        .unwrap();
    let mut owned = OwnedChildMount {
        child: Some(child),
        mountpoint: directory.0.join("mount"),
    };
    let start = std::time::Instant::now();
    while !directory.0.join("ready").exists() {
        if let Some(status) = owned.child.as_mut().unwrap().try_wait().unwrap() {
            panic!("owned mount owner failed: {status}");
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("owned mount owner did not become ready");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    owned
}
#[test]
#[ignore = "requires native Linux FUSE and owned child process"]
fn mounted_child_guard_cleans_up_on_unwind() {
    let directory = Directory::new("owner-unwind");
    for name in ["stores", "mount"] {
        std::fs::create_dir(directory.0.join(name)).unwrap();
    }
    let owner = spawn_owner(&directory);
    let pid = owner.child.as_ref().unwrap().id();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _owner = owner;
        panic!("fixture failure after mount readiness");
    }));
    assert!(outcome.is_err());
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "owned child was not reaped"
    );
    assert_eq!(
        std::fs::metadata(directory.0.join("mount")).unwrap().dev(),
        std::fs::metadata(&directory.0).unwrap().dev()
    );
    assert!(
        Recovery::open(directory.0.join("recovery"), Default::default())
            .unwrap()
            .load()
            .unwrap()
            .is_none()
    );
}

#[test]
#[ignore = "helper for the owned abrupt-exit fixture"]
fn abrupt_exit_child() {
    let Some(path) = std::env::var_os("LORE_WORKING_CHILD_DIRECTORY") else {
        return;
    };
    let directory = PathBuf::from(path);
    let runtime = runtime();
    let factory = Arc::new(DiskStores {
        directory: directory.join("stores"),
        next: AtomicU64::new(0),
    });
    let base = Arc::new(Base::new());
    let gate = Arc::new(Gate::default());
    let model = runtime
        .block_on(Model::new(
            Arc::new(Namespace::new(base, root(), gate, Default::default()).unwrap()),
            factory,
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.join("recovery"), Default::default()).unwrap(),
            Default::default(),
        ))
        .unwrap();
    let _mounted = mount(model, &runtime, &directory.join("mount"));
    // The owner never opens its own FUSE paths; the parent is the writer.
    std::fs::write(directory.join("ready"), b"ready").unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
#[ignore = "requires native Linux FUSE and owned child process"]
fn mounted_abrupt_owner_exit_restores_acknowledged_bytes() {
    let directory = Directory::new("abrupt-owner");
    for name in ["stores", "mount", "fresh-stores"] {
        std::fs::create_dir(directory.0.join(name)).unwrap();
    }
    let mut owned = spawn_owner(&directory);
    let mountpoint = directory.0.join("mount");
    let mut writer = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(mountpoint.join("acknowledged"))
        .unwrap();
    writer.write_all(b"acknowledged bytes").unwrap();
    writer.sync_all().unwrap();
    drop(writer);
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(owned.stop().unwrap().signal(), Some(libc::SIGKILL));
    let runtime = runtime();
    let factory = Arc::new(DiskStores {
        directory: directory.0.join("fresh-stores"),
        next: AtomicU64::new(0),
    });
    let model = runtime
        .block_on(Model::restore(
            Arc::new(Base::new()),
            root(),
            Arc::new(Gate::default()),
            Default::default(),
            factory,
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            Default::default(),
        ))
        .unwrap();
    let mounted = mount(model, &runtime, &mountpoint);
    assert_eq!(
        std::fs::read(mountpoint.join("acknowledged")).unwrap(),
        b"acknowledged bytes"
    );
    assert_eq!(
        digest(&mountpoint.join("acknowledged"), 32)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ENODATA)
    );
    mounted.unmount().unwrap();
}

#[path = "provider_mount.rs"]
mod provider_mount;

struct LinkedBase(Base);
fn child_key(node: u32) -> NodeKey {
    NodeKey {
        repository: [2; 16].into(),
        exposure: 2,
        node,
        ..root().key
    }
}
impl Backend for LinkedBase {
    fn lookup<'a>(&'a self, _key: &'a NodeKey, _name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async { Err(core::Error::Unsupported) })
    }
    fn list<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        _limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            let entries = if offset != 0 {
                vec![]
            } else if *key == root().key {
                vec![NamedNode {
                    name: b"vendor".to_vec(),
                    node: Node {
                        key: child_key(0),
                        kind: NodeKind::Directory,
                        size: 0,
                        mode: 0o755,
                    },
                }]
            } else if *key == child_key(0) {
                vec![NamedNode {
                    name: b"content".to_vec(),
                    node: Node {
                        key: child_key(1),
                        kind: NodeKind::File,
                        size: 4,
                        mode: 0o444,
                    },
                }]
            } else {
                return Err(core::Error::NotDirectory);
            };
            Ok(BackendPage {
                entries,
                next_offset: None,
            })
        })
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        self.0.read(key, offset, size)
    }
    fn digest<'a>(&'a self, key: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        self.0.digest(key)
    }
}
struct ChildGate(AtomicBool);
impl AccessGate for ChildGate {
    fn authorize<'a>(&'a self, key: &'a NodeKey, access: Access) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if (*key != root().key && *key != child_key(0) && *key != child_key(1))
                || (access == Access::Write && key.exposure == 2 && self.0.load(Ordering::Acquire))
            {
                Err(core::Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
}
#[test]
#[ignore = "requires native Linux FUSE"]
fn mounted_child_write_gate_and_exact_freeze() {
    let directory = Directory::new("child-gate");
    let runtime = runtime();
    let gate = Arc::new(ChildGate(AtomicBool::new(true)));
    let namespace = Arc::new(
        Namespace::new(
            Arc::new(LinkedBase(Base::new())),
            root(),
            gate.clone(),
            Default::default(),
        )
        .unwrap(),
    );
    let model = runtime
        .block_on(Model::new(
            namespace,
            Arc::new(Stores::default()),
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            Default::default(),
        ))
        .unwrap();
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let mounted = mount(model.clone(), &runtime, &mountpoint);
    let path = mountpoint.join("vendor/content");
    assert_eq!(std::fs::read(&path).unwrap(), b"base");
    assert_eq!(
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EACCES)
    );
    std::fs::write(mountpoint.join("root-file"), b"allowed").unwrap();
    gate.0.store(false, Ordering::Release);
    assert_eq!(
        std::fs::rename(&path, mountpoint.join("moved"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EXDEV)
    );
    let freeze = runtime
        .block_on(model.namespace().freeze_operation())
        .unwrap();
    assert_eq!(
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EBUSY)
    );
    assert_eq!(
        std::fs::create_dir(mountpoint.join("blocked"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EBUSY)
    );
    drop(freeze);
    std::fs::write(&path, b"next").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"next");
    assert_eq!(
        digest(&path, 32).unwrap_err().raw_os_error(),
        Some(libc::ENODATA)
    );
    mounted.unmount().unwrap();
}
