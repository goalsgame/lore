// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_base::types::RepositoryId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::node::NodeFlags;
use lore_revision::relay::EventDispatcher;

use super::*;

struct Fixture {
    immutable: Arc<dyn lore_storage::ImmutableStore>,
    mutable: Arc<dyn lore_storage::MutableStore>,
}

impl Fixture {
    async fn new() -> Self {
        let immutable = lore_storage::local::immutable_store::create(
            None::<&str>,
            lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
            false,
            Default::default(),
        )
        .await
        .unwrap();
        let mutable = lore_storage::local::mutable_store::create(
            None::<&str>,
            Default::default(),
            immutable.clone(),
        )
        .await
        .unwrap();
        Self { immutable, mutable }
    }

    fn repository(&self, id: u8) -> Arc<RepositoryContext> {
        Arc::new(RepositoryContext::new_server_context(
            self.immutable.clone(),
            self.mutable.clone(),
            RepositoryId::from([id; 16]),
        ))
    }
}

fn execution() -> Arc<ExecutionContext> {
    Arc::new(ExecutionContext::new_client_with_user_id(
        LoreGlobalArgs::default(),
        EventDispatcher::no_dispatch(),
        "vfs-test".into(),
    ))
}

async fn directory(
    state: &State,
    repository: &Arc<RepositoryContext>,
    parent: NodeID,
    name: &str,
) -> NodeID {
    state
        .node_add(
            repository.clone(),
            parent,
            RevisionNode {
                name_hash: lore_storage::hash::hash_string(name),
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap()
}

async fn file(
    state: &State,
    repository: &Arc<RepositoryContext>,
    parent: NodeID,
    name: &str,
    contents: &[u8],
) -> NodeID {
    // Small files use the same verified immutable read path as fragmented payloads.
    let address = if contents.is_empty() {
        Address::default()
    } else {
        immutable::write_borrowed(
            repository.clone(),
            Context::from([9; 16]),
            contents,
            lore_storage::options::WriteOptions::default().with_fixed_size_chunk(3),
        )
        .await
        .unwrap()
    };
    state
        .node_add(
            repository.clone(),
            parent,
            RevisionNode {
                flags: NodeFlags::File.bits(),
                size: contents.len() as u64,
                address,
                name_hash: lore_storage::hash::hash_string(name),
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap()
}

async fn link(
    state: &State,
    repository: &Arc<RepositoryContext>,
    parent: NodeID,
    name: &str,
    target: &Arc<RepositoryContext>,
    revision: Hash,
    node: NodeID,
) -> NodeID {
    state
        .node_add(
            repository.clone(),
            parent,
            RevisionNode {
                flags: NodeFlags::Link.bits(),
                child: node,
                address: Address {
                    hash: revision,
                    context: target.id.into(),
                },
                name_hash: lore_storage::hash::hash_string(name),
                ..Default::default()
            },
            name,
        )
        .await
        .unwrap()
}

async fn freeze(repository: &Arc<RepositoryContext>, state: &Arc<State>) -> Hash {
    state
        .serialize(repository.clone(), repository.try_write_token().unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn nested_subtree_links_use_the_owning_revision_for_lookup_listing_and_range_reads() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let leaf = fixture.repository(1);
            let middle = fixture.repository(2);
            let parent = fixture.repository(3);
            let leaf_state = State::new();
            file(&leaf_state, &leaf, ROOT_NODE, "hidden", b"not in subtree").await;
            let subtree = directory(&leaf_state, &leaf, ROOT_NODE, "source").await;
            file(&leaf_state, &leaf, subtree, "data", b"abcdefghijk").await;
            file(&leaf_state, &leaf, subtree, "empty", b"").await;
            let leaf_revision = freeze(&leaf, &leaf_state).await;

            let middle_state = State::new();
            let middle_subtree =
                directory(&middle_state, &middle, ROOT_NODE, "middle-source").await;
            link(
                &middle_state,
                &middle,
                middle_subtree,
                "inner",
                &leaf,
                leaf_revision,
                subtree,
            )
            .await;
            let middle_revision = freeze(&middle, &middle_state).await;

            let parent_state = State::new();
            // Deliberately allocate unrelated nodes with matching numbers in the parent.
            file(&parent_state, &parent, ROOT_NODE, "parent-data", b"PARENT").await;
            link(
                &parent_state,
                &parent,
                ROOT_NODE,
                "outer",
                &middle,
                middle_revision,
                middle_subtree,
            )
            .await;
            let revision = freeze(&parent, &parent_state).await;
            let backend = RevisionBackend::new(parent, revision).await.unwrap();
            let outer = backend.lookup(&backend.root().key, b"outer").await.unwrap();
            assert_eq!(outer.key.repository, middle.id);
            assert_eq!(outer.key.node, middle_subtree);
            let page = backend.list(&outer.key, 0, 10).await.unwrap();
            assert_eq!(page.entries.len(), 1);
            assert_eq!(page.entries[0].name, b"inner");
            let inner = backend.lookup(&outer.key, b"inner").await.unwrap();
            assert_eq!(inner, page.entries[0].node);
            assert_eq!(inner.key.repository, leaf.id);
            assert_eq!(inner.key.node, subtree);
            let page = backend.list(&inner.key, 0, 1).await.unwrap();
            assert_eq!(page.entries.len(), 1);
            assert_eq!(page.next_offset, Some(1));
            let page2 = backend.list(&inner.key, 1, 1).await.unwrap();
            assert_eq!(page2.entries.len(), 1);
            assert_eq!(page2.next_offset, None);
            assert_ne!(page.entries[0].name, page2.entries[0].name);
            assert_eq!(
                backend.lookup(&inner.key, b"hidden").await,
                Err(Error::NotFound)
            );
            let data = backend.lookup(&inner.key, b"data").await.unwrap();
            assert_eq!(backend.read(&data.key, 2, 7).await.unwrap(), b"cdefghi");
            assert_eq!(backend.read(&data.key, 8, 999).await.unwrap(), b"ijk");
            assert!(
                backend
                    .read(&data.key, u64::MAX, 10)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(backend.digest(&data.key).await.unwrap(), None);
            let empty = backend.lookup(&inner.key, b"empty").await.unwrap();
            assert!(backend.read(&empty.key, 0, 10).await.unwrap().is_empty());
        })
        .await;
}

#[tokio::test]
async fn separate_pins_and_multiple_exposures_of_one_repository_do_not_alias() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let child = fixture.repository(4);
            let parent = fixture.repository(5);
            let first = State::new();
            file(&first, &child, ROOT_NODE, "data", b"first").await;
            let first_revision = freeze(&child, &first).await;
            let second = State::new();
            file(&second, &child, ROOT_NODE, "data", b"other").await;
            let second_revision = freeze(&child, &second).await;
            let state = State::new();
            for (name, revision) in [
                ("a", first_revision),
                ("b", second_revision),
                ("c", first_revision),
            ] {
                link(
                    &state, &parent, ROOT_NODE, name, &child, revision, ROOT_NODE,
                )
                .await;
            }
            let revision = freeze(&parent, &state).await;
            let backend = RevisionBackend::new(parent, revision).await.unwrap();
            let a = backend.lookup(&backend.root().key, b"a").await.unwrap();
            let b = backend.lookup(&backend.root().key, b"b").await.unwrap();
            let c = backend.lookup(&backend.root().key, b"c").await.unwrap();
            assert_ne!(a.key, b.key);
            assert_ne!(a.key, c.key);
            assert_eq!(backend.lookup(&backend.root().key, b"a").await.unwrap(), a);
            for (dir, expected) in [(a, b"first"), (b, b"other"), (c, b"first")] {
                let data = backend.lookup(&dir.key, b"data").await.unwrap();
                assert_eq!(&backend.read(&data.key, 0, 100).await.unwrap(), expected);
            }
        })
        .await;
}

#[tokio::test]
async fn child_denial_preserves_parent_listing_but_rejects_traversal() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let child = fixture.repository(6);
            let parent = fixture.repository(7);
            let child_state = State::new();
            file(&child_state, &child, ROOT_NODE, "secret", b"private").await;
            let child_revision = freeze(&child, &child_state).await;
            let state = State::new();
            file(&state, &parent, ROOT_NODE, "public", b"public").await;
            link(
                &state,
                &parent,
                ROOT_NODE,
                "denied",
                &child,
                child_revision,
                ROOT_NODE,
            )
            .await;
            let revision = freeze(&parent, &state).await;
            let denied = child.id;
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(move |id| id != denied)),
            );
            let backend = RevisionBackend::new(parent, revision).await.unwrap();
            let public = backend
                .lookup(&backend.root().key, b"public")
                .await
                .unwrap();
            assert_eq!(backend.read(&public.key, 0, 100).await.unwrap(), b"public");
            let denied_mount = backend
                .lookup(&backend.root().key, b"denied")
                .await
                .unwrap();
            assert_eq!(denied_mount.key.repository, backend.root().key.repository);
            let page = backend.list(&backend.root().key, 0, 100).await.unwrap();
            assert_eq!(page.entries.len(), 2);
            assert!(page.entries.iter().any(|entry| entry.name == b"public"));
            assert_eq!(
                page.entries
                    .iter()
                    .find(|entry| entry.name == b"denied")
                    .unwrap()
                    .node,
                denied_mount
            );
            assert_eq!(
                backend.lookup(&denied_mount.key, b"secret").await,
                Err(Error::PermissionDenied)
            );
            assert!(matches!(
                backend.list(&denied_mount.key, 0, 100).await,
                Err(Error::PermissionDenied)
            ));
            assert_eq!(
                backend.read(&denied_mount.key, 0, 100).await,
                Err(Error::PermissionDenied)
            );
            // Caller-made owner keys must never select arbitrary partitions.
            let mut forged = public.key;
            forged.repository = child.id;
            assert_eq!(backend.read(&forged, 0, 100).await, Err(Error::Stale));
        })
        .await;
}

#[tokio::test]
// A plain worker deliberately carries no Lore context, matching FUSE callbacks.
#[allow(clippy::disallowed_methods)]
async fn callbacks_restore_captured_context_and_failed_content_reads_do_not_become_empty_files() {
    let (backend, good, missing) = LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let repository = fixture.repository(8);
            let state = State::new();
            file(&state, &repository, ROOT_NODE, "good", b"verified").await;
            state
                .node_add(
                    repository.clone(),
                    ROOT_NODE,
                    RevisionNode {
                        flags: NodeFlags::File.bits(),
                        size: 8,
                        address: Address {
                            hash: Hash::from_u64(12345),
                            context: Context::from([12; 16]),
                        },
                        name_hash: lore_storage::hash::hash_string("missing"),
                        ..Default::default()
                    },
                    "missing",
                )
                .await
                .unwrap();
            let revision = freeze(&repository, &state).await;
            let backend = RevisionBackend::new(repository, revision).await.unwrap();
            let good = backend.lookup(&backend.root().key, b"good").await.unwrap();
            let missing = backend
                .lookup(&backend.root().key, b"missing")
                .await
                .unwrap();
            (Arc::new(backend), good, missing)
        })
        .await;
    assert!(lore_revision::runtime::try_execution_context().is_none());
    let runtime = tokio::runtime::Handle::current();
    // Match the adapter's plain worker-thread -> Handle::block_on callback path.
    tokio::task::spawn_blocking(move || {
        runtime.block_on(async {
            assert_eq!(backend.read(&good.key, 0, 100).await.unwrap(), b"verified");
            assert_eq!(backend.read(&missing.key, 0, 100).await, Err(Error::Io));
            assert_eq!(backend.digest(&missing.key).await.unwrap(), None);
            assert_eq!(
                backend.lookup(&backend.root().key, b"absent").await,
                Err(Error::NotFound)
            );
        });
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_revision_and_invalid_namespace_requests_return_errors() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let repository = fixture.repository(9);
            assert!(matches!(
                RevisionBackend::new(repository.clone(), Hash::from_u64(99)).await,
                Err(Error::NotFound)
            ));
            let backend = RevisionBackend::new(repository, Hash::default())
                .await
                .unwrap();
            assert!(
                backend
                    .list(&backend.root().key, 0, 10)
                    .await
                    .unwrap()
                    .entries
                    .is_empty()
            );
            for name in [b"".as_slice(), b".", b"..", b"a/b", b"a\0b", b"\xff"] {
                assert_eq!(
                    backend.lookup(&backend.root().key, name).await,
                    Err(Error::InvalidInput)
                );
            }
            assert_eq!(
                backend.lookup(&backend.root().key, b".LORE").await,
                Err(Error::NotFound)
            );
        })
        .await;
}

/// Real storage plus Linux FUSE gate. Run under an external test timeout.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a Linux host with unprivileged FUSE mounting enabled"]
#[allow(clippy::disallowed_methods)]
fn mounted_revision_contract() {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::os::unix::ffi::OsStrExt;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let cache = crate::digest_index::tests::Directory::new();
    let backend = runtime.block_on(LORE_CONTEXT.scope(execution(), async {
        let fixture = Fixture::new().await;
        let leaf = fixture.repository(20);
        let middle = fixture.repository(21);
        let parent = fixture.repository(22);
        let denied = fixture.repository(23);
        let leaf_state = State::new();
        file(&leaf_state, &leaf, ROOT_NODE, "hidden", b"outside subtree").await;
        let source = directory(&leaf_state, &leaf, ROOT_NODE, "source").await;
        file(&leaf_state, &leaf, source, "data", b"abcdefghijk").await;
        let leaf_revision = freeze(&leaf, &leaf_state).await;
        let middle_state = State::new();
        link(
            &middle_state,
            &middle,
            ROOT_NODE,
            "inner",
            &leaf,
            leaf_revision,
            source,
        )
        .await;
        let middle_revision = freeze(&middle, &middle_state).await;
        let denied_state = State::new();
        file(&denied_state, &denied, ROOT_NODE, "secret", b"private").await;
        let denied_revision = freeze(&denied, &denied_state).await;
        let parent_state = State::new();
        file(&parent_state, &parent, ROOT_NODE, "public", b"public").await;
        link(
            &parent_state,
            &parent,
            ROOT_NODE,
            "outer",
            &middle,
            middle_revision,
            ROOT_NODE,
        )
        .await;
        link(
            &parent_state,
            &parent,
            ROOT_NODE,
            "denied",
            &denied,
            denied_revision,
            ROOT_NODE,
        )
        .await;
        let revision = freeze(&parent, &parent_state).await;
        let denied_id = denied.id;
        let parent = Arc::new(
            parent
                .without_command_locks()
                .with_link_read(Arc::new(move |id| id != denied_id)),
        );
        Arc::new(
            RevisionBackend::new(parent, revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(&cache.0)),
        )
    }));
    let core = Arc::new(crate::core::Core::new(backend.clone(), backend.root()).unwrap());
    struct MountDirectory(std::path::PathBuf);
    impl Drop for MountDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir(&self.0);
        }
    }
    let directory = MountDirectory(std::env::temp_dir().join(format!(
        "lore-revision-fuse-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    )));
    std::fs::create_dir(&directory.0).unwrap();
    // SAFETY: getuid/getgid have no arguments or preconditions.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let mounted =
        crate::linux::mount(core, runtime.handle().clone(), &directory.0, uid, gid).unwrap();
    let mut names: Vec<_> = std::fs::read_dir(&directory.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names, ["denied", "outer", "public"]);
    assert_eq!(
        std::fs::read(directory.0.join("public")).unwrap(),
        b"public"
    );
    let source = directory.0.join("outer/inner");
    assert_eq!(std::fs::read_dir(&source).unwrap().count(), 1);
    let path = source.join("data");
    let mut file = std::fs::File::open(&path).unwrap();
    file.seek(SeekFrom::Start(2)).unwrap();
    let mut range = [0; 7];
    file.read_exact(&mut range).unwrap();
    assert_eq!(&range, b"cdefghi");
    file.seek(SeekFrom::Start(100)).unwrap();
    assert_eq!(file.read(&mut range).unwrap(), 0);
    assert_eq!(
        std::fs::read(source.join("hidden"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::ENOENT)
    );
    assert_eq!(
        std::fs::read_dir(directory.0.join("denied"))
            .and_then(|entries| entries.collect::<std::io::Result<Vec<_>>>())
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EACCES)
    );
    assert_eq!(
        std::fs::read(directory.0.join("denied/secret"))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EACCES)
    );
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let name = c"user.sha256";
    // SAFETY: both strings are NUL terminated; null with size zero is a size probe.
    assert_eq!(
        unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ENODATA)
    );
    let expected = runtime.block_on(async {
        let outer = backend.lookup(&backend.root().key, b"outer").await.unwrap();
        let inner = backend.lookup(&outer.key, b"inner").await.unwrap();
        let data = backend.lookup(&inner.key, b"data").await.unwrap();
        backend.backfill_digest(&data.key, 100).await.unwrap()
    });
    assert_eq!(
        expected.as_slice(),
        ring::digest::digest(&ring::digest::SHA256, b"abcdefghijk").as_ref()
    );
    // SAFETY: both strings are NUL terminated; null with size zero is a size probe.
    assert_eq!(
        unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
        32
    );
    let mut actual = [0u8; 32];
    // SAFETY: actual provides the writable buffer of the supplied length.
    assert_eq!(
        unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                actual.as_mut_ptr().cast(),
                actual.len(),
            )
        },
        32
    );
    assert_eq!(actual, expected);
    drop(file);
    mounted.unmount().unwrap();
}

#[tokio::test]
async fn excessive_terminal_link_depth_fails_without_unbounded_recursion() {
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let repository = fixture.repository(10);
            let leaf = State::new();
            let mut target = file(&leaf, &repository, ROOT_NODE, "data", b"data").await;
            let mut revision = freeze(&repository, &leaf).await;
            for _ in 0..=MAX_LINK_DEPTH {
                let state = State::new();
                target = link(
                    &state,
                    &repository,
                    ROOT_NODE,
                    "mount",
                    &repository,
                    revision,
                    target,
                )
                .await;
                revision = freeze(&repository, &state).await;
            }
            let backend = RevisionBackend::new(repository, revision).await.unwrap();
            assert_eq!(
                backend.lookup(&backend.root().key, b"mount").await,
                Err(Error::Io)
            );
            assert!(backend.owners.lock().unwrap().by_exposure.len() <= MAX_LINK_DEPTH + 1);
        })
        .await;
}

#[tokio::test]
async fn cached_child_keys_recheck_the_authorization_route() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let child = fixture.repository(11);
            let parent = fixture.repository(12);
            let child_state = State::new();
            file(&child_state, &child, ROOT_NODE, "data", b"private").await;
            let child_revision = freeze(&child, &child_state).await;
            let state = State::new();
            link(
                &state,
                &parent,
                ROOT_NODE,
                "child",
                &child,
                child_revision,
                ROOT_NODE,
            )
            .await;
            let revision = freeze(&parent, &state).await;
            let allowed = Arc::new(AtomicBool::new(true));
            let policy = allowed.clone();
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(move |_| policy.load(Ordering::SeqCst))),
            );
            let cache = crate::digest_index::tests::Directory::new();
            let backend = RevisionBackend::new(parent, revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(&cache.0));
            let child = backend.lookup(&backend.root().key, b"child").await.unwrap();
            let data = backend.lookup(&child.key, b"data").await.unwrap();
            let digest = backend.backfill_digest(&data.key, 100).await.unwrap();
            assert_eq!(backend.digest(&data.key).await.unwrap(), Some(digest));
            assert_eq!(backend.read(&data.key, 0, 100).await.unwrap(), b"private");
            allowed.store(false, Ordering::SeqCst);
            assert_eq!(
                backend.read(&data.key, 0, 100).await,
                Err(Error::PermissionDenied)
            );
            assert_eq!(
                backend.lookup(&child.key, b"data").await,
                Err(Error::PermissionDenied)
            );
            assert!(matches!(
                backend.list(&child.key, 0, 10).await,
                Err(Error::PermissionDenied)
            ));
            assert_eq!(
                backend.digest(&data.key).await,
                Err(Error::PermissionDenied)
            );
        })
        .await;
}

#[tokio::test]
async fn verified_backfill_is_explicit_persistent_and_independent_of_content_reads() {
    LORE_CONTEXT
        .scope(execution(), async {
            let cache = crate::digest_index::tests::Directory::new();
            let fixture = Fixture::new().await;
            let repository = fixture.repository(41);
            let state = State::new();
            let contents: Vec<u8> = (0..800_000).map(|i| (i % 251) as u8).collect();
            // A larger chunk size keeps the fixture small while crossing stream boundaries.
            let address = immutable::write_borrowed(
                repository.clone(),
                Context::from([7; 16]),
                &contents,
                lore_storage::options::WriteOptions::default().with_fixed_size_chunk(8192),
            )
            .await
            .unwrap();
            state
                .node_add(
                    repository.clone(),
                    ROOT_NODE,
                    RevisionNode {
                        flags: NodeFlags::File.bits(),
                        size: contents.len() as u64,
                        address,
                        name_hash: lore_storage::hash::hash_string("large"),
                        ..Default::default()
                    },
                    "large",
                )
                .await
                .unwrap();
            file(&state, &repository, ROOT_NODE, "fragmented", b"abcdefghijk").await;
            file(&state, &repository, ROOT_NODE, "empty", b"").await;
            let revision = freeze(&repository, &state).await;
            let backend = RevisionBackend::new(repository.clone(), revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(&cache.0));
            let large = backend.lookup(&backend.root().key, b"large").await.unwrap();
            assert_eq!(backend.digest(&large.key).await.unwrap(), None);
            assert_eq!(
                backend.backfill_digest(&large.key, 100).await,
                Err(Error::ResourceLimit)
            );
            assert_eq!(backend.digest(&large.key).await.unwrap(), None);
            let mut expected = [0; 32];
            expected
                .copy_from_slice(ring::digest::digest(&ring::digest::SHA256, &contents).as_ref());
            assert_eq!(
                backend
                    .backfill_digest(&large.key, contents.len() as u64)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(backend.digest(&large.key).await.unwrap(), Some(expected));
            for (name, bytes) in [
                (b"fragmented".as_slice(), b"abcdefghijk".as_slice()),
                (b"empty", b""),
            ] {
                let node = backend.lookup(&backend.root().key, name).await.unwrap();
                let actual = backend.backfill_digest(&node.key, 100).await.unwrap();
                assert_eq!(
                    actual.as_slice(),
                    ring::digest::digest(&ring::digest::SHA256, bytes).as_ref()
                );
                assert_eq!(backend.digest(&node.key).await.unwrap(), Some(actual));
            }
            fixture
                .immutable
                .clone()
                .obliterate(repository.id, address, Default::default())
                .await
                .unwrap();
            assert_eq!(backend.read(&large.key, 0, 1).await, Err(Error::Io));
            assert_eq!(backend.digest(&large.key).await.unwrap(), Some(expected));
            let restored = RevisionBackend::new(repository, revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(&cache.0));
            let large = restored
                .lookup(&restored.root().key, b"large")
                .await
                .unwrap();
            assert_eq!(restored.digest(&large.key).await.unwrap(), Some(expected));
            std::fs::remove_dir_all(&cache.0).unwrap();
            assert_eq!(restored.digest(&large.key).await.unwrap(), None);
        })
        .await;
}

#[tokio::test]
async fn failed_backfill_never_publishes_for_length_mismatches_or_missing_payloads() {
    LORE_CONTEXT
        .scope(execution(), async {
            let cache = crate::digest_index::tests::Directory::new();
            let fixture = Fixture::new().await;
            let repository = fixture.repository(42);
            let state = State::new();
            let contents: Vec<u8> = (0..300_000).map(|i| (i % 251) as u8).collect();
            let address = immutable::write_borrowed(
                repository.clone(),
                Context::from([9; 16]),
                &contents,
                lore_storage::options::WriteOptions::default().with_fixed_size_chunk(8192),
            )
            .await
            .unwrap();
            for (name, address, size) in [
                ("short", address, contents.len() as u64 - 2),
                ("long", address, contents.len() as u64 + 2),
                ("broken-stream", address, contents.len() as u64),
                (
                    "missing",
                    Address {
                        hash: Hash::from_u64(99999),
                        context: [9; 16].into(),
                    },
                    7,
                ),
            ] {
                state
                    .node_add(
                        repository.clone(),
                        ROOT_NODE,
                        RevisionNode {
                            flags: NodeFlags::File.bits(),
                            address,
                            size,
                            name_hash: lore_storage::hash::hash_string(name),
                            ..Default::default()
                        },
                        name,
                    )
                    .await
                    .unwrap();
            }
            let revision = freeze(&repository, &state).await;
            let backend = RevisionBackend::new(repository.clone(), revision)
                .await
                .unwrap()
                .with_digest_index(DigestIndex::new(&cache.0));
            for name in [b"short".as_slice(), b"long", b"missing"] {
                let node = backend.lookup(&backend.root().key, name).await.unwrap();
                assert_eq!(
                    backend.backfill_digest(&node.key, 400_000).await,
                    Err(Error::Io)
                );
                assert_eq!(backend.digest(&node.key).await.unwrap(), None);
            }
            use lore_storage::TypedBytes;
            let mut options = immutable::read_options_from_repository(&repository);
            options.trust_remote = false;
            let (_, references) = immutable::load_raw(repository.clone(), address, options)
                .await
                .unwrap();
            let references = references.to_aligned::<lore_storage::FragmentReference>();
            let last = references
                .as_type_slice::<lore_storage::FragmentReference>()
                .last()
                .unwrap();
            fixture
                .immutable
                .clone()
                .obliterate(
                    repository.id,
                    Address {
                        hash: last.hash,
                        context: address.context,
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            let broken = backend
                .lookup(&backend.root().key, b"broken-stream")
                .await
                .unwrap();
            assert_eq!(
                backend.backfill_digest(&broken.key, 400_000).await,
                Err(Error::Io)
            );
            assert_eq!(backend.digest(&broken.key).await.unwrap(), None);
            assert!(!cache.0.exists());
        })
        .await;
}

#[derive(Default)]
struct CheckpointStore(tokio::sync::Mutex<Vec<u8>>);
impl crate::overlay::SparseStore for CheckpointStore {
    fn read<'a>(&'a self, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            Ok(self.0.lock().await[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn write<'a>(&'a self, offset: u64, bytes: &'a [u8]) -> BackendFuture<'a, usize> {
        Box::pin(async move {
            let mut data = self.0.lock().await;
            data.resize(offset as usize + bytes.len(), 0);
            data[offset as usize..].copy_from_slice(bytes);
            Ok(bytes.len())
        })
    }
}
struct CheckpointGate;
impl crate::overlay::AccessGate for CheckpointGate {
    fn authorize<'a>(&'a self, _: &'a NodeKey, _: crate::overlay::Access) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
fn checkpoint_stores(
    snapshot: &crate::namespace::NamespaceSnapshot,
) -> BTreeMap<u64, Arc<dyn crate::overlay::SparseStore>> {
    snapshot
        .files
        .keys()
        .map(|&id| {
            (
                id,
                Arc::new(CheckpointStore::default()) as Arc<dyn crate::overlay::SparseStore>,
            )
        })
        .collect()
}
async fn checkpoint_file(
    ns: &crate::namespace::Namespace,
    inode: u64,
) -> Arc<crate::overlay::OverlayFile> {
    ns.file(
        inode,
        Arc::new(CheckpointStore::default()),
        crate::overlay::Limits::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn linked_checkpoint_reconstructs_exact_routes_on_independent_fresh_backend() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use crate::namespace::Namespace;
    use crate::namespace::NamespaceError;
    use crate::overlay::Access;
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let leaf = fixture.repository(71);
            let middle = fixture.repository(72);
            let parent = fixture.repository(73);
            let leaf_state = State::new();
            let hidden = file(&leaf_state, &leaf, ROOT_NODE, "hidden", b"outside").await;
            let subtree = directory(&leaf_state, &leaf, ROOT_NODE, "source").await;
            file(&leaf_state, &leaf, subtree, "data", b"abcdef").await;
            let leaf_revision = freeze(&leaf, &leaf_state).await;
            let middle_state = State::new();
            let middle_source = directory(&middle_state, &middle, ROOT_NODE, "source").await;
            let outside_link = link(
                &middle_state,
                &middle,
                ROOT_NODE,
                "outside-link",
                &leaf,
                leaf_revision,
                subtree,
            )
            .await;
            link(
                &middle_state,
                &middle,
                middle_source,
                "inner",
                &leaf,
                leaf_revision,
                subtree,
            )
            .await;
            let middle_revision = freeze(&middle, &middle_state).await;
            let parent_state = State::new();
            link(
                &parent_state,
                &parent,
                ROOT_NODE,
                "outer",
                &middle,
                middle_revision,
                middle_source,
            )
            .await;
            link(
                &parent_state,
                &parent,
                ROOT_NODE,
                "twin",
                &leaf,
                leaf_revision,
                subtree,
            )
            .await;
            let revision = freeze(&parent, &parent_state).await;
            let allowed = Arc::new(AtomicBool::new(true));
            let policy = allowed.clone();
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(move |_| policy.load(Ordering::SeqCst))),
            );
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), revision)
                    .await
                    .unwrap(),
            );
            // Allocate nested routes first. Namespace enumeration order is irrelevant.
            let outer = backend.lookup(&backend.root().key, b"outer").await.unwrap();
            let inner = backend.lookup(&outer.key, b"inner").await.unwrap();
            let twin = backend.lookup(&backend.root().key, b"twin").await.unwrap();
            assert_ne!(inner.key.exposure, twin.key.exposure);
            let mut escaped = inner.key;
            escaped.node = hidden;
            assert_eq!(backend.revalidate(&escaped, false).await, Err(Error::Stale));
            let ns = Namespace::new(
                backend.clone(),
                backend.root(),
                Arc::new(CheckpointGate),
                Default::default(),
            )
            .unwrap();
            let outer = ns.lookup(1, b"outer").await.unwrap();
            let inner = ns.lookup(outer.inode, b"inner").await.unwrap();
            let data = ns.lookup(inner.inode, b"data").await.unwrap();
            let file = checkpoint_file(&ns, data.inode).await;
            let handle = file.open(Access::Write).await.unwrap();
            file.write(handle, 2, b"XY").await.unwrap();
            let local = ns
                .create_file(
                    inner.inode,
                    b"local",
                    0o644,
                    Arc::new(CheckpointStore::default()),
                    Default::default(),
                )
                .await
                .unwrap();
            let local_file = checkpoint_file(&ns, local.inode).await;
            let local_handle = local_file.open(Access::Write).await.unwrap();
            local_file
                .write(local_handle, 0, b"local-child")
                .await
                .unwrap();
            let snapshot = ns.snapshot(1024).await.unwrap();
            #[cfg(unix)]
            let snapshot = {
                let directory = crate::digest_index::tests::Directory::new();
                let mut recovery =
                    crate::recovery::Recovery::open(&directory.0, Default::default()).unwrap();
                recovery.publish(&snapshot).unwrap();
                recovery.load().unwrap().unwrap()
            };
            let fresh = Arc::new(
                RevisionBackend::new(parent.clone(), revision)
                    .await
                    .unwrap(),
            );
            let fresh_twin = fresh.lookup(&fresh.root().key, b"twin").await.unwrap();
            assert_ne!(fresh_twin.key.exposure, twin.key.exposure);
            let unchanged = fresh.checkpoint_state().await.unwrap();
            let restored = Namespace::restore(
                fresh.clone(),
                fresh.root(),
                Arc::new(CheckpointGate),
                snapshot.clone(),
                checkpoint_stores(&snapshot),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            assert_eq!(fresh.checkpoint_state().await.unwrap(), unchanged);
            assert_eq!(restored.lookup(1, b"outer").await.unwrap(), outer);
            let restored_file = checkpoint_file(&restored, data.inode).await;
            let restored_handle = restored_file.open(Access::Read).await.unwrap();
            assert_eq!(
                restored_file.read(restored_handle, 0, 20).await.unwrap(),
                b"abXYef"
            );
            let local_file = checkpoint_file(&restored, local.inode).await;
            let local_handle = local_file.open(Access::Read).await.unwrap();
            assert_eq!(
                local_file.read(local_handle, 0, 20).await.unwrap(),
                b"local-child"
            );
            let twin_entry = restored.lookup(1, b"twin").await.unwrap();
            let twin_data = restored.lookup(twin_entry.inode, b"data").await.unwrap();
            assert_ne!(twin_data.inode, data.inode);
            let twin_file = checkpoint_file(&restored, twin_data.inode).await;
            let twin_handle = twin_file.open(Access::Read).await.unwrap();
            assert_eq!(twin_file.read(twin_handle, 0, 20).await.unwrap(), b"abcdef");
            // Corrupt routes and source-tree escapes never alter a shared backend.
            let header = 8 + 76;
            let first = header + 9; // Initial allocator floor.
            let route = 1 + 76 + 76 + 9; // Origin and following floor.
            let source_node = 1 + 64;
            let mut corrupt = snapshot.backend_state.clone();
            corrupt[8] ^= 1;
            assert!(matches!(
                fresh.prepare_restore(&corrupt).await,
                Err(Error::Stale)
            ));
            let mut unknown = snapshot.backend_state.clone();
            unknown[first + source_node..first + source_node + 4]
                .copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(matches!(
                fresh.prepare_restore(&unknown).await,
                Err(Error::NotFound | Error::Io)
            ));
            let mut pin = snapshot.backend_state.clone();
            pin[first + 1 + 76] ^= 1;
            assert!(matches!(
                fresh.prepare_restore(&pin).await,
                Err(Error::Stale | Error::InvalidInput)
            ));
            let mut escape = snapshot.backend_state.clone();
            escape[first + route + source_node..first + route + source_node + 4]
                .copy_from_slice(&outside_link.to_le_bytes());
            assert!(matches!(
                fresh.prepare_restore(&escape).await,
                Err(Error::Stale)
            ));
            let mut duplicate = snapshot.backend_state.clone();
            let mut extra = duplicate[first..first + route].to_vec();
            extra[1 + 76 + 68..1 + 76 + 76].copy_from_slice(&4u64.to_le_bytes());
            extra[route - 8..].copy_from_slice(&5u64.to_le_bytes());
            duplicate.extend(extra);
            assert!(matches!(
                fresh.prepare_restore(&duplicate).await,
                Err(Error::InvalidInput)
            ));
            assert_eq!(fresh.checkpoint_state().await.unwrap(), unchanged);
            // Drop replay at an asynchronous boundary; its temporary owner table
            // must never become visible through the shared original backend.
            let mut cancelled = fresh.prepare_restore(&snapshot.backend_state);
            std::future::poll_fn(|context| {
                assert!(cancelled.as_mut().poll(context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(cancelled);
            assert_eq!(fresh.checkpoint_state().await.unwrap(), unchanged);
            allowed.store(false, Ordering::SeqCst);
            // Even wholly local bytes and new files recheck reconstructed link routes.
            assert_eq!(
                restored_file.read(restored_handle, 2, 2).await,
                Err(Error::PermissionDenied)
            );
            assert_eq!(
                local_file.read(local_handle, 0, 20).await,
                Err(Error::PermissionDenied)
            );
            let untouched = Arc::new(RevisionBackend::new(parent, revision).await.unwrap());
            assert!(matches!(
                Namespace::restore(
                    untouched.clone(),
                    untouched.root(),
                    Arc::new(CheckpointGate),
                    snapshot.clone(),
                    checkpoint_stores(&snapshot),
                    Default::default(),
                    Default::default()
                )
                .await,
                Err(NamespaceError::Backend(Error::PermissionDenied))
            ));
            assert_eq!(untouched.owners.lock().unwrap().by_exposure.len(), 1);
        })
        .await;
}

#[tokio::test]
async fn denied_opaque_link_does_not_prevent_parent_dirty_checkpoint_restore() {
    use crate::namespace::Namespace;
    use crate::overlay::Access;
    LORE_CONTEXT
        .scope(execution(), async {
            let fixture = Fixture::new().await;
            let child = fixture.repository(81);
            let parent = fixture.repository(82);
            let child_state = State::new();
            file(&child_state, &child, ROOT_NODE, "secret", b"private").await;
            let child_revision = freeze(&child, &child_state).await;
            let state = State::new();
            file(&state, &parent, ROOT_NODE, "public", b"parent").await;
            link(
                &state,
                &parent,
                ROOT_NODE,
                "denied",
                &child,
                child_revision,
                ROOT_NODE,
            )
            .await;
            let revision = freeze(&parent, &state).await;
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(|_| false)),
            );
            let backend = Arc::new(
                RevisionBackend::new(parent.clone(), revision)
                    .await
                    .unwrap(),
            );
            let ns = Namespace::new(
                backend.clone(),
                backend.root(),
                Arc::new(CheckpointGate),
                Default::default(),
            )
            .unwrap();
            let public = ns.lookup(1, b"public").await.unwrap();
            let file = checkpoint_file(&ns, public.inode).await;
            let handle = file.open(Access::Write).await.unwrap();
            file.write(handle, 0, b"EDIT").await.unwrap();
            let denied = ns.lookup(1, b"denied").await.unwrap();
            let snapshot = ns.snapshot(100).await.unwrap();
            let fresh = Arc::new(RevisionBackend::new(parent, revision).await.unwrap());
            let restored = Namespace::restore(
                fresh.clone(),
                fresh.root(),
                Arc::new(CheckpointGate),
                snapshot.clone(),
                checkpoint_stores(&snapshot),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
            let file = checkpoint_file(&restored, public.inode).await;
            let handle = file.open(Access::Read).await.unwrap();
            assert_eq!(file.read(handle, 0, 10).await.unwrap(), b"EDITnt");
            assert!(matches!(
                restored.list(denied.inode, None, 10).await,
                Err(crate::namespace::NamespaceError::Backend(
                    Error::PermissionDenied
                ))
            ));
            assert!(matches!(
                restored.create_directory(denied.inode, b"no").await,
                Err(crate::namespace::NamespaceError::Backend(
                    Error::PermissionDenied
                ))
            ));
        })
        .await;
}

#[path = "revision_allocation_tests.rs"]
mod allocation;

#[cfg(unix)]
#[tokio::test]
async fn imported_foreign_repository_pin_reads_from_independent_stores_and_rechecks_access() {
    LORE_CONTEXT
        .scope(execution(), async {
            use std::sync::atomic::AtomicBool;
            use std::sync::atomic::Ordering;

            use lore_revision::repository::working_bootstrap::ImmutableImport;

            let source = Fixture::new().await;
            let destination = Fixture::new().await;
            assert!(!Arc::ptr_eq(&source.immutable, &destination.immutable));
            let child = source.repository(81);
            let parent = source.repository(82);
            let child_state = State::new();
            let file_id = file(
                &child_state,
                &child,
                ROOT_NODE,
                "payload.h",
                b"cross-repository imported bytes",
            )
            .await;
            let address = child_state
                .node(child.clone(), file_id)
                .await
                .unwrap()
                .address;
            assert_eq!(address.context, Context::from([9; 16]));
            let child_pin = freeze(&child, &child_state).await;
            let parent_state = State::new();
            link(
                &parent_state,
                &parent,
                ROOT_NODE,
                "mounted",
                &child,
                child_pin,
                ROOT_NODE,
            )
            .await;
            let parent_pin = freeze(&parent, &parent_state).await;
            let allowed = Arc::new(AtomicBool::new(true));
            let decision = allowed.clone();
            let parent = Arc::new(
                parent
                    .without_command_locks()
                    .with_link_read(Arc::new(move |_| decision.load(Ordering::SeqCst))),
            );
            let import = ImmutableImport::new(parent, parent_pin).await.unwrap();
            let destination_parent = Arc::new(
                import.apply_link_policy(destination.repository(82).without_command_locks()),
            );
            import.copy_into(destination_parent.clone()).await.unwrap();
            let backend = RevisionBackend::new(destination_parent, parent_pin)
                .await
                .unwrap();
            let mounted = backend
                .lookup(&backend.root().key, b"mounted")
                .await
                .unwrap();
            assert_eq!(mounted.key.repository, child.id);
            assert_eq!(mounted.key.revision, Address::zero_context_hash(child_pin));
            let file = backend.lookup(&mounted.key, b"payload.h").await.unwrap();
            assert_eq!(file.key.node, file_id);
            assert_eq!(
                backend.content_identity(&file.key).await.unwrap().address,
                address
            );
            assert_eq!(
                backend.read(&file.key, 0, 100).await.unwrap(),
                b"cross-repository imported bytes"
            );
            allowed.store(false, Ordering::SeqCst);
            assert_eq!(
                backend.read(&file.key, 0, 100).await,
                Err(Error::PermissionDenied)
            );
            let opaque = backend
                .lookup(&backend.root().key, b"mounted")
                .await
                .unwrap();
            assert_eq!(opaque.key.repository, backend.root().key.repository);
        })
        .await;
}
