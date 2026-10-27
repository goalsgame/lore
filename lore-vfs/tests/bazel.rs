// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Optional native workload gate: Bazel 7.5+, Linux FUSE and fusermount3.
#![cfg(target_os = "linux")]

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use lore_base::types::Address;
use lore_vfs::core::Backend;
use lore_vfs::core::BackendFuture;
use lore_vfs::core::BackendPage;
use lore_vfs::core::Core;
use lore_vfs::core::Error;
use lore_vfs::core::NamedNode;
use lore_vfs::core::Node;
use lore_vfs::core::NodeKey;
use lore_vfs::core::NodeKind;

const NAMES: [&[u8]; 3] = [b"WORKSPACE", b"BUILD.bazel", b"input.txt"];
const BUILD: &[u8] = b"genrule(name = 'result', srcs = ['input.txt'], outs = ['result.txt'], cmd = 'cp $(location input.txt) $@')\n";

#[path = "bazel/working.rs"]
mod working;

struct View {
    generation: u8,
    content: &'static [u8],
    known_digest: bool,
    digest_queries: AtomicUsize,
}

impl View {
    fn bytes(&self, node: u32) -> &'static [u8] {
        match node {
            1 => b"workspace(name = 'vfs_digest_test')\n",
            2 => BUILD,
            3 => self.content,
            _ => b"",
        }
    }

    fn node(&self, id: u32) -> Node {
        let revision = Address {
            hash: [self.generation; 32].into(),
            ..Default::default()
        };
        Node {
            key: NodeKey {
                repository: [1; 16].into(),
                revision,
                node: id,
                exposure: 0,
            },
            kind: if id == 0 {
                NodeKind::Directory
            } else {
                NodeKind::File
            },
            size: self.bytes(id).len() as u64,
            mode: if id == 0 { 0o555 } else { 0o444 },
        }
    }

    fn new(generation: u8, content: &'static [u8], known_digest: bool) -> Arc<Self> {
        Arc::new(Self {
            generation,
            content,
            known_digest,
            digest_queries: AtomicUsize::new(0),
        })
    }
}

impl Backend for View {
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async move {
            if parent.node != 0 {
                return Err(Error::NotDirectory);
            }
            NAMES
                .iter()
                .position(|candidate| *candidate == name)
                .map(|index| self.node(index as u32 + 1))
                .ok_or(Error::NotFound)
        })
    }

    fn list<'a>(
        &'a self,
        parent: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            if parent.node != 0 {
                return Err(Error::NotDirectory);
            }
            let start = usize::try_from(offset).map_err(|_overflow| Error::InvalidInput)?;
            let entries: Vec<_> = NAMES
                .iter()
                .enumerate()
                .skip(start)
                .take(limit)
                .map(|(index, name)| NamedNode {
                    name: name.to_vec(),
                    node: self.node(index as u32 + 1),
                })
                .collect();
            let end = offset + entries.len() as u64;
            Ok(BackendPage {
                entries,
                next_offset: (end < NAMES.len() as u64).then_some(end),
            })
        })
    }

    fn read<'a>(
        &'a self,
        file: &'a NodeKey,
        offset: u64,
        size: usize,
    ) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let start = usize::try_from(offset).map_err(|_overflow| Error::InvalidInput)?;
            self.bytes(file.node)
                .get(start..start + size)
                .map(<[u8]>::to_vec)
                .ok_or(Error::Range)
        })
    }

    fn digest<'a>(&'a self, file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async move {
            if file.node != 3 {
                return Ok(None);
            }
            self.digest_queries.fetch_add(1, Ordering::SeqCst);
            Ok(self.known_digest.then(|| {
                ring::digest::digest(&ring::digest::SHA256, self.content)
                    .as_ref()
                    .try_into()
                    .unwrap()
            }))
        })
    }
}

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        for name in ["ordinary-build", "mounted-build"] {
            let scratch = self.0.join(name);
            if scratch.join("base").exists() {
                let _ = bazel(&scratch)
                    .current_dir(self.0.join("ordinary"))
                    .arg("shutdown")
                    .output();
            }
        }
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn bazel(scratch: &Path) -> Command {
    let mut command =
        Command::new(std::env::var_os("LORE_TEST_BAZEL").unwrap_or_else(|| "bazel".into()));
    command
        .arg("--max_idle_secs=30")
        .arg(format!(
            "--output_user_root={}",
            scratch.join("user").display()
        ))
        .arg(format!("--output_base={}", scratch.join("base").display()))
        .args([
            "--digest_function=SHA256",
            "--unix_digest_hash_attribute_name=user.sha256",
        ]);
    command
}

fn build(workspace: &Path, scratch: &Path, label: &str) -> Vec<u8> {
    fs::create_dir_all(scratch).unwrap();
    let log = scratch.join(format!("{label}.json"));
    let output = bazel(scratch)
        .current_dir(workspace)
        .args([
            "build",
            "//:result",
            "--enable_bzlmod=false",
            "--spawn_strategy=local",
            "--genrule_strategy=local",
            "--color=no",
            "--curses=no",
        ])
        .arg(format!("--symlink_prefix={}/output-", scratch.display()))
        .arg(format!("--execution_log_json_file={}", log.display()))
        .output()
        .expect("Bazel must be installed for this explicit workload gate");
    assert!(
        output.status.success(),
        "Bazel failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let expected = fs::read(workspace.join("input.txt")).unwrap();
    let actual = fs::read(scratch.join("output-bin/result.txt")).unwrap();
    assert_eq!(actual, expected, "Bazel reused stale build output");
    // A changed build must execute the action and record its exact whole-file digest.
    if label != "warm" {
        let digest = ring::digest::digest(&ring::digest::SHA256, &expected);
        let hex: String = digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let execution = fs::read_to_string(log).unwrap();
        assert!(
            execution.contains(&hex),
            "execution log lacks expected input SHA-256: {execution}"
        );
    }
    actual
}

#[test]
#[ignore = "requires native Linux FUSE and Bazel; run with a process timeout"]
fn mounted_bazel_digest_and_same_size_transition() {
    let scratch = Directory(std::env::temp_dir().join(format!(
            "lore-bazel-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    fs::create_dir(&scratch.0).unwrap();
    let ordinary = scratch.0.join("ordinary");
    let mountpoint = scratch.0.join("view");
    fs::create_dir(&ordinary).unwrap();
    fs::create_dir(&mountpoint).unwrap();
    let first = View::new(1, b"old\n", true);
    for (index, name) in NAMES.iter().enumerate() {
        fs::write(
            ordinary.join(std::str::from_utf8(name).unwrap()),
            first.bytes(index as u32 + 1),
        )
        .unwrap();
    }
    let expected = build(&ordinary, &scratch.0.join("ordinary-build"), "initial");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let core = Arc::new(Core::new(first.clone(), first.node(0)).unwrap());
    // SAFETY: these libc calls have no arguments or preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let mounted =
        lore_vfs::linux::mount(core, runtime.handle().clone(), &mountpoint, uid, gid).unwrap();
    let builds = scratch.0.join("mounted-build");
    assert_eq!(build(&mountpoint, &builds, "initial"), expected);
    assert!(
        first.digest_queries.load(Ordering::SeqCst) > 0,
        "Bazel did not request the digest xattr"
    );
    build(&mountpoint, &builds, "warm");
    let second = View::new(2, b"new\n", true);
    mounted
        .transition_control()
        .transition(second.clone(), second.node(0))
        .unwrap();
    assert_eq!(build(&mountpoint, &builds, "changed"), b"new\n");
    assert!(second.digest_queries.load(Ordering::SeqCst) > 0);
    let unknown = View::new(3, b"end\n", false);
    mounted
        .transition_control()
        .transition(unknown.clone(), unknown.node(0))
        .unwrap();
    assert_eq!(build(&mountpoint, &builds, "unknown"), b"end\n");
    assert!(unknown.digest_queries.load(Ordering::SeqCst) > 0);
    // The server retains its workspace cwd; drain it before unmounting.
    let shutdown = bazel(&builds)
        .current_dir(&mountpoint)
        .arg("shutdown")
        .output()
        .unwrap();
    assert!(shutdown.status.success());
    mounted.unmount().unwrap();
}
