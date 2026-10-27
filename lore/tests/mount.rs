// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore::mount::validate_mountpoint;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lore-mount-validation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn validation_does_not_resolve_relative_paths_against_the_process_directory() {
    assert_eq!(
        validate_mountpoint(std::path::Path::new("."), None)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[test]
fn empty_directory_required_and_hidden_files_are_never_shadowed() {
    let temp = Temp::new();
    let mountpoint = temp.0.join("view");
    assert!(validate_mountpoint(&mountpoint, None).is_err());
    std::fs::create_dir(&mountpoint).unwrap();
    assert_eq!(
        validate_mountpoint(&mountpoint, None).unwrap(),
        mountpoint.canonicalize().unwrap()
    );
    std::fs::write(mountpoint.join(".hidden"), b"edits").unwrap();
    assert!(validate_mountpoint(&mountpoint, None).is_err());
    assert_eq!(std::fs::read(mountpoint.join(".hidden")).unwrap(), b"edits");
}

#[test]
fn metadata_and_its_ancestors_are_rejected_even_when_empty() {
    let temp = Temp::new();
    for name in [".lore", ".urc", ".URC"] {
        let metadata = temp.0.join(name);
        std::fs::create_dir_all(&metadata).unwrap();
        assert!(validate_mountpoint(&metadata, None).is_err());
        let nested = metadata.join("empty");
        std::fs::create_dir_all(&nested).unwrap();
        assert!(validate_mountpoint(&nested, None).is_err());
    }
    let external = temp.0.join("external-metadata");
    std::fs::create_dir(&external).unwrap();
    assert!(validate_mountpoint(&external, Some(&external)).is_err());
    assert!(validate_mountpoint(&temp.0, Some(&external)).is_err());
}

#[cfg(unix)]
#[test]
fn symlinks_cannot_bypass_directory_or_metadata_checks() {
    let temp = Temp::new();
    let target = temp.0.join("view");
    std::fs::create_dir(&target).unwrap();
    let link = temp.0.join("link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(validate_mountpoint(&link, None).is_err());
    let metadata = temp.0.join(".lore");
    std::fs::create_dir(&metadata).unwrap();
    let nested = metadata.join("empty");
    std::fs::create_dir(&nested).unwrap();
    let alias = temp.0.join("alias");
    std::os::unix::fs::symlink(&metadata, &alias).unwrap();
    assert!(validate_mountpoint(&alias.join("empty"), None).is_err());
}

#[cfg(unix)]
#[test]
fn digest_cache_creation_is_private_and_separate_from_the_mountpoint() {
    use std::os::unix::fs::MetadataExt;

    use lore::mount::validate_digest_cache;
    let temp = Temp::new();
    let mountpoint = temp.0.join("view");
    std::fs::create_dir(&mountpoint).unwrap();
    let cache = temp.0.join("digests");
    assert_eq!(
        validate_digest_cache(&cache, &mountpoint).unwrap(),
        cache.canonicalize().unwrap()
    );
    assert_eq!(std::fs::metadata(&cache).unwrap().mode() & 0o777, 0o700);
    let inside = mountpoint.join("cache");
    assert!(validate_digest_cache(&inside, &mountpoint).is_err());
    assert!(!inside.exists());
    assert!(validate_digest_cache(&temp.0, &mountpoint).is_err());
    assert!(std::fs::read_dir(&mountpoint).unwrap().next().is_none());
}

#[cfg(unix)]
#[test]
fn unsafe_digest_cache_permissions_and_symlinks_are_rejected() {
    use std::os::unix::fs::PermissionsExt;

    use lore::mount::validate_digest_cache;
    let temp = Temp::new();
    let mountpoint = temp.0.join("view");
    std::fs::create_dir(&mountpoint).unwrap();
    let cache = temp.0.join("digests");
    validate_digest_cache(&cache, &mountpoint).unwrap();
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(validate_digest_cache(&cache, &mountpoint).is_err());
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = temp.0.join("alias");
    std::os::unix::fs::symlink(&cache, &alias).unwrap();
    assert!(validate_digest_cache(&alias, &mountpoint).is_err());
    let view_alias = temp.0.join("view-alias");
    std::os::unix::fs::symlink(&mountpoint, &view_alias).unwrap();
    assert!(validate_digest_cache(&view_alias.join("new"), &mountpoint).is_err());
    assert!(!mountpoint.join("new").exists());
}

#[cfg(target_os = "linux")]
// Deliberately exercise Tokio blocking file I/O and kernel write rejection in a
// private fixture; these calls do not mutate a repository working tree.
#[allow(clippy::disallowed_methods)]
mod linux {
    use std::sync::Arc;
    use std::time::Duration;

    use lore_vfs::core::Backend;
    use lore_vfs::core::BackendFuture;
    use lore_vfs::core::BackendPage;
    use lore_vfs::core::Core;
    use lore_vfs::core::Error;
    use lore_vfs::core::NamedNode;
    use lore_vfs::core::Node;
    use lore_vfs::core::NodeKey;
    use lore_vfs::core::NodeKind;

    use super::*;

    struct Fixture {
        file: Option<PathBuf>,
    }
    fn node(file: bool) -> Node {
        Node {
            key: NodeKey {
                repository: Default::default(),
                revision: Default::default(),
                node: u32::from(file),
                exposure: 0,
            },
            kind: if file {
                NodeKind::File
            } else {
                NodeKind::Directory
            },
            size: if file { 7 } else { 0 },
            mode: 0o555,
        }
    }
    impl Backend for Fixture {
        fn lookup<'a>(&'a self, _: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
            Box::pin(async move {
                if name == b"fixture" {
                    Ok(node(true))
                } else {
                    Err(Error::NotFound)
                }
            })
        }
        fn list<'a>(
            &'a self,
            _: &'a NodeKey,
            offset: u64,
            _: usize,
        ) -> BackendFuture<'a, BackendPage> {
            Box::pin(async move {
                Ok(BackendPage {
                    entries: if offset == 0 {
                        vec![NamedNode {
                            name: b"fixture".to_vec(),
                            node: node(true),
                        }]
                    } else {
                        Vec::new()
                    },
                    next_offset: None,
                })
            })
        }
        fn read<'a>(
            &'a self,
            _: &'a NodeKey,
            offset: u64,
            size: usize,
        ) -> BackendFuture<'a, Vec<u8>> {
            Box::pin(async move {
                let bytes = if let Some(path) = &self.file {
                    tokio::fs::read(path).await.map_err(|_io_error| Error::Io)?
                } else {
                    b"payload".to_vec()
                };
                Ok(bytes[offset as usize..offset as usize + size].to_vec())
            })
        }
        fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
            Box::pin(async { Ok(Some([17; 32])) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native Linux /dev/fuse and fusermount3"]
    async fn foreground_fixture_reads_and_shutdown_unmounts() {
        let temp = Temp::new();
        let mountpoint = temp.0.join("view");
        std::fs::create_dir(&mountpoint).unwrap();
        let core = Arc::new(Core::new(Arc::new(Fixture { file: None }), node(false)).unwrap());
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
        let path = mountpoint.clone();
        let read = async move {
            ready_receiver.await.unwrap();
            let bytes = tokio::fs::read(path.join("fixture")).await.unwrap();
            assert_eq!(bytes, b"payload");
            assert!(
                tokio::fs::write(path.join("fixture"), b"edit")
                    .await
                    .is_err()
            );
            stop_sender.send(()).unwrap();
        };
        let session = lore::mount::foreground(
            core.clone(),
            mountpoint.clone(),
            None,
            async {
                let _ = stop_receiver.await;
            },
            || {
                let _ = ready_sender.send(());
            },
        );
        let (_, mounted) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(read, session)
        })
        .await
        .unwrap();
        mounted.unwrap();
        assert!(std::fs::read_dir(&mountpoint).unwrap().next().is_none());
        assert_eq!(core.getattr(1), Err(Error::Unmounted));
    }
    #[test]
    #[ignore = "requires native Linux /dev/fuse and fusermount3"]
    fn file_backed_reads_progress_with_one_blocking_pool_thread() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let temp = Temp::new();
        let mountpoint = temp.0.join("view");
        std::fs::create_dir(&mountpoint).unwrap();
        let data = temp.0.join("data");
        std::fs::write(&data, b"payload").unwrap();
        let core =
            Arc::new(Core::new(Arc::new(Fixture { file: Some(data) }), node(false)).unwrap());
        runtime.block_on(async {
            let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
            let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
            let path = mountpoint.clone();
            let read = async move {
                ready_receiver.await.unwrap();
                let (bytes_sender, bytes_receiver) = tokio::sync::oneshot::channel();
                // Emulate a separate OS reader. Using Tokio file I/O to read the
                // FUSE mount itself would occupy the backend's only pool thread.
                let reader = std::thread::spawn(move || {
                    let _ = bytes_sender.send(std::fs::read(path.join("fixture")));
                });
                assert_eq!(bytes_receiver.await.unwrap().unwrap(), b"payload");
                reader.join().unwrap();
                stop_sender.send(()).unwrap();
            };
            let session = lore::mount::foreground(
                core,
                mountpoint,
                None,
                async {
                    let _ = stop_receiver.await;
                },
                || {
                    let _ = ready_sender.send(());
                },
            );
            let (_, result) = tokio::time::timeout(Duration::from_secs(30), async {
                tokio::join!(read, session)
            })
            .await
            .unwrap();
            result.unwrap();
        });
    }
}

#[cfg(target_os = "linux")]
mod cancellation {
    use std::sync::Arc;
    use std::time::Duration;

    use lore_vfs::core::Backend;
    use lore_vfs::core::BackendFuture;
    use lore_vfs::core::BackendPage;
    use lore_vfs::core::Core;
    use lore_vfs::core::Error;
    use lore_vfs::core::Node;
    use lore_vfs::core::NodeKey;
    use lore_vfs::core::NodeKind;

    use super::*;

    struct Empty;
    impl Backend for Empty {
        fn lookup<'a>(&'a self, _: &'a NodeKey, _: &'a [u8]) -> BackendFuture<'a, Node> {
            Box::pin(async { Err(Error::NotFound) })
        }
        fn list<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, BackendPage> {
            Box::pin(async {
                Ok(BackendPage {
                    entries: Vec::new(),
                    next_offset: None,
                })
            })
        }
        fn read<'a>(&'a self, _: &'a NodeKey, _: u64, _: usize) -> BackendFuture<'a, Vec<u8>> {
            Box::pin(async { Err(Error::Unsupported) })
        }
        fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
            Box::pin(async { Ok(None) })
        }
    }
    fn core() -> Arc<Core> {
        Arc::new(
            Core::new(
                Arc::new(Empty),
                Node {
                    key: NodeKey {
                        repository: Default::default(),
                        revision: Default::default(),
                        node: 0,
                        exposure: 0,
                    },
                    kind: NodeKind::Directory,
                    size: 0,
                    mode: 0o555,
                },
            )
            .unwrap(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native Linux /dev/fuse and fusermount3"]
    async fn cancelling_foreground_after_readiness_unmounts_an_empty_view() {
        let temp = Temp::new();
        let mountpoint = temp.0.join("view");
        std::fs::create_dir(&mountpoint).unwrap();
        let core = core();
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        {
            let session = lore::mount::foreground(
                core.clone(),
                mountpoint.clone(),
                None,
                std::future::pending(),
                || {
                    let _ = ready_sender.send(());
                },
            );
            tokio::pin!(session);
            tokio::select! {
                result = &mut session => panic!("session stopped before cancellation: {result:?}"),
                ready = ready_receiver => ready.unwrap(),
            }
            assert_eq!(
                validate_mountpoint(&mountpoint, None).unwrap_err().kind(),
                std::io::ErrorKind::AlreadyExists
            );
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            while core.getattr(1) != Err(Error::Unmounted) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(validate_mountpoint(&mountpoint, None).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native Linux /dev/fuse and fusermount3"]
    async fn cancelling_foreground_during_startup_does_not_leave_a_session() {
        let temp = Temp::new();
        let mountpoint = temp.0.join("view");
        std::fs::create_dir(&mountpoint).unwrap();
        let core = core();
        {
            let session = lore::mount::foreground(
                core.clone(),
                mountpoint.clone(),
                None,
                std::future::pending(),
                || {},
            );
            tokio::pin!(session);
            // Poll exactly once: validation and the blocking mount owner are
            // started, then cancellation closes both lifecycle channels.
            std::future::poll_fn(|context| {
                assert!(session.as_mut().poll(context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            while core.getattr(1) != Err(Error::Unmounted) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(validate_mountpoint(&mountpoint, None).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native Linux /dev/fuse and fusermount3"]
    async fn shutdown_during_startup_drains_the_mount_owner() {
        let temp = Temp::new();
        let mountpoint = temp.0.join("view");
        std::fs::create_dir(&mountpoint).unwrap();
        let core = core();
        tokio::time::timeout(
            Duration::from_secs(30),
            lore::mount::foreground(core.clone(), mountpoint.clone(), None, async {}, || {}),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(core.getattr(1), Err(Error::Unmounted));
        assert!(validate_mountpoint(&mountpoint, None).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires native Linux /dev/fuse and fusermount3"]
    async fn external_unmount_ends_foreground_without_a_shutdown_signal() {
        let temp = Temp::new();
        let mountpoint = temp.0.join("view");
        std::fs::create_dir(&mountpoint).unwrap();
        let core = core();
        let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
        let path = mountpoint.clone();
        let unmount = async move {
            ready_receiver.await.unwrap();
            let output =
                lore_base::lore_spawn_blocking_nocontext!(move || std::process::Command::new(
                    "fusermount3"
                )
                .arg("-u")
                .arg(&path)
                .output())
                .await
                .unwrap()
                .unwrap();
            assert!(
                output.status.success(),
                "external unmount failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        let session = lore::mount::foreground(
            core.clone(),
            mountpoint.clone(),
            None,
            std::future::pending(),
            || {
                let _ = ready_sender.send(());
            },
        );
        let (_, result) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(unmount, session)
        })
        .await
        .unwrap();
        result.unwrap();
        assert_eq!(core.getattr(1), Err(Error::Unmounted));
        assert!(validate_mountpoint(&mountpoint, None).is_ok());
    }

    #[tokio::test]
    async fn a_current_thread_runtime_is_rejected_before_mounting() {
        let temp = Temp::new();
        let error = lore::mount::foreground(core(), temp.0.clone(), None, async {}, || {})
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}
