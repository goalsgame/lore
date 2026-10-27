// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::ffi::CStr;
use std::ffi::CString;
use std::fs;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::AtomicBool;

use lore_base::types::Address;

use super::*;
use crate::core::BackendFuture;
use crate::core::BackendPage;
use crate::core::NamedNode;
use crate::core::NodeKey;

struct Snapshot(u8);
impl Snapshot {
    fn node(&self, repository: u8, id: u32) -> Node {
        let revision = Address {
            hash: [if repository == 2 { 9 } else { self.0 }; 32].into(),
            ..Default::default()
        };
        let directory = id == 0 || (repository == 1 && (id == 5 || (id == 3 && self.0 == 2)));
        Node {
            key: NodeKey {
                repository: [repository; 16].into(),
                revision,
                node: id,
                exposure: u64::from(repository - 1),
            },
            kind: if directory {
                NodeKind::Directory
            } else {
                NodeKind::File
            },
            size: if directory { 0 } else { 3 },
            mode: if directory { 0o555 } else { 0o444 },
        }
    }
    fn root(&self) -> Node {
        self.node(1, 0)
    }
    fn names(&self, parent: &NodeKey) -> Vec<&'static [u8]> {
        if parent.repository == [1; 16].into() && parent.node == 0 {
            vec![
                b"file",
                if self.0 == 1 { b"deleted" } else { b"added" },
                b"flip",
                b"dir",
                b"stable",
                b"nested",
            ]
        } else if parent.repository == [1; 16].into() && parent.node == 5 {
            vec![b"child"]
        } else if parent.repository == [2; 16].into() {
            vec![b"payload"]
        } else if parent.repository == [3; 16].into() {
            vec![b"inner"]
        } else if parent.repository == [4; 16].into() {
            vec![b"payload"]
        } else {
            Vec::new()
        }
    }
    fn child(&self, parent: &NodeKey, name: &[u8]) -> crate::core::Result<Node> {
        if !self.names(parent).contains(&name) {
            return Err(Error::NotFound);
        }
        let (repository, id) = match name {
            b"file" => (1, 1),
            b"deleted" => (1, 2),
            b"added" => (1, 4),
            b"flip" => (1, 3),
            b"dir" => (1, 5),
            b"child" => (1, 6),
            b"stable" => (2, 0),
            b"nested" => (3, 0),
            b"inner" => (4, 0),
            b"payload" => (
                if parent.repository == [2; 16].into() {
                    2
                } else {
                    4
                },
                1,
            ),
            _ => return Err(Error::NotFound),
        };
        Ok(self.node(repository, id))
    }
}
impl Backend for Snapshot {
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async move { self.child(parent, name) })
    }
    fn list<'a>(
        &'a self,
        parent: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            let names = self.names(parent);
            let entries = names
                .iter()
                .skip(offset as usize)
                .take(limit)
                .map(|name| {
                    Ok(NamedNode {
                        name: name.to_vec(),
                        node: self.child(parent, name)?,
                    })
                })
                .collect::<crate::core::Result<Vec<_>>>()?;
            let end = offset + entries.len() as u64;
            Ok(BackendPage {
                entries,
                next_offset: (end < names.len() as u64).then_some(end),
            })
        })
    }
    fn read<'a>(
        &'a self,
        key: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let bytes = if key.repository == [2; 16].into() {
                b"pin"
            } else if self.0 == 1 {
                b"old"
            } else {
                b"new"
            };
            Ok(bytes[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Ok(None) })
    }
}
struct Directory {
    path: std::path::PathBuf,
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.path);
    }
}
struct OpenDirectory(*mut libc::DIR);
impl Drop for OpenDirectory {
    fn drop(&mut self) {
        // SAFETY: owns the non-null opendir result exclusively.
        unsafe {
            libc::closedir(self.0);
        }
    }
}
impl OpenDirectory {
    fn open(path: &Path) -> Self {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: NUL-terminated path, returned pointer checked before ownership.
        let pointer = unsafe { libc::opendir(path.as_ptr()) };
        assert!(!pointer.is_null());
        Self(pointer)
    }
    fn names(&self) -> Vec<Vec<u8>> {
        let mut names = Vec::new();
        loop {
            // SAFETY: exclusive live DIR; result remains valid until next readdir call.
            let entry = unsafe { libc::readdir(self.0) };
            if entry.is_null() {
                break;
            }
            // SAFETY: POSIX readdir returns a NUL-terminated d_name.
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }
                .to_bytes()
                .to_vec();
            if name != b"." && name != b".." {
                names.push(name);
            }
        }
        names
    }
    fn read_at(&self, name: &str) -> Vec<u8> {
        use std::os::fd::FromRawFd;
        let name = CString::new(name).unwrap();
        // SAFETY: live DIR and NUL-terminated relative component.
        let fd = unsafe { libc::openat(libc::dirfd(self.0), name.as_ptr(), libc::O_RDONLY) };
        assert!(fd >= 0);
        // SAFETY: newly opened fd has one owning File.
        let file = unsafe { fs::File::from_raw_fd(fd) };
        read(&file)
    }
}
fn read(file: &fs::File) -> Vec<u8> {
    let mut bytes = vec![0; 3];
    assert_eq!(file.read_at(&mut bytes, 0).unwrap(), 3);
    bytes
}

/// Native Linux gate: exercises positive caches, uncached warmed misses, retained
/// descriptors, directory handles, nested/repeated child pins and live path reads.
#[test]
#[ignore = "requires /dev/fuse and unprivileged Linux mounting"]
fn mounted_revision_transition_contract() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let old = Arc::new(Snapshot(1));
    let core = Arc::new(Core::new(old.clone(), old.root()).unwrap());
    let path = std::env::temp_dir().join(format!(
        "lore-transition-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    let directory = Directory { path };
    // SAFETY: getuid/getgid have no arguments or preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let mounted = mount(
        core.clone(),
        runtime.handle().clone(),
        &directory.path,
        uid,
        gid,
    )
    .unwrap();
    let control = mounted.transition_control();
    let main = fs::File::open(directory.path.join("file")).unwrap();
    assert_eq!(read(&main), b"old");
    let deleted = fs::File::open(directory.path.join("deleted")).unwrap();
    let flipped = fs::File::open(directory.path.join("flip")).unwrap();
    let stable = fs::File::open(directory.path.join("stable/payload")).unwrap();
    let nested = fs::File::open(directory.path.join("nested/inner/payload")).unwrap();
    let old_root = OpenDirectory::open(&directory.path);
    let old_directory = OpenDirectory::open(&directory.path.join("dir"));
    assert_eq!(
        fs::metadata(directory.path.join("added"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    let stopped = Arc::new(AtomicBool::new(false));
    let stopping = stopped.clone();
    let reader_path = directory.path.join("file");
    let reader = std::thread::spawn(move || {
        while !stopping.load(Ordering::Acquire) {
            let bytes = fs::read(&reader_path).unwrap();
            assert!(bytes == b"old" || bytes == b"new");
        }
    });
    let next = Arc::new(Snapshot(2));
    control.transition(next.clone(), next.root()).unwrap();
    stopped.store(true, Ordering::Release);
    reader.join().unwrap();
    assert_eq!(fs::read(directory.path.join("file")).unwrap(), b"new");
    assert_eq!(read(&main), b"old");
    assert_eq!(
        fs::metadata(directory.path.join("deleted"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(read(&deleted), b"old");
    assert!(fs::metadata(directory.path.join("flip")).unwrap().is_dir());
    assert_eq!(read(&flipped), b"old");
    assert_eq!(fs::read(directory.path.join("added")).unwrap(), b"new");
    assert_eq!(
        fs::read(directory.path.join("nested/inner/payload")).unwrap(),
        b"new"
    );
    assert_eq!(read(&nested), b"old");
    assert_eq!(
        fs::read(directory.path.join("stable/payload")).unwrap(),
        b"pin"
    );
    assert_eq!(read(&stable), b"pin");
    assert_ne!(
        stable.metadata().unwrap().ino(),
        fs::metadata(directory.path.join("stable/payload"))
            .unwrap()
            .ino()
    );
    let old_names = old_root.names();
    assert!(old_names.contains(&b"deleted".to_vec()));
    assert!(!old_names.contains(&b"added".to_vec()));
    let new_names = fs::read_dir(&directory.path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().as_bytes().to_vec())
        .collect::<Vec<_>>();
    assert!(new_names.contains(&b"added".to_vec()));
    assert!(!new_names.contains(&b"deleted".to_vec()));
    // Root relative lookup is active despite its old enumeration handle. A
    // retained nonroot directory inode resolves children in its pinned old view.
    assert_eq!(old_root.read_at("added"), b"new");
    assert_eq!(old_directory.read_at("child"), b"old");
    drop(old_directory);
    drop(old_root);
    drop(main);
    drop(deleted);
    drop(flipped);
    drop(stable);
    drop(nested);
    mounted.unmount().unwrap();
    assert!(matches!(
        control.transition(old.clone(), old.root()),
        Err(TransitionError::Core(Error::Unmounted))
    ));
}
