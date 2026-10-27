// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;

use super::*;

struct Snapshot {
    revision: u8,
    block: Option<(Notify, Notify)>,
}
impl Snapshot {
    fn node(&self, id: u32) -> Node {
        let mut revision = Address::default();
        revision.hash = [self.revision; 32].into();
        Node {
            key: NodeKey {
                repository: [1; 16].into(),
                revision,
                node: id,
                exposure: 0,
            },
            kind: if id == 0 || id == 2 || (id == 5 && self.revision == 2) {
                NodeKind::Directory
            } else {
                NodeKind::File
            },
            size: if id == 0 || id == 2 || (id == 5 && self.revision == 2) {
                0
            } else {
                3
            },
            mode: 0o555,
        }
    }
    fn root(&self) -> Node {
        self.node(0)
    }
}
impl Backend for Snapshot {
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async move {
            match name {
                b"file" => Ok(self.node(1)),
                b"dir" => Ok(self.node(2)),
                b"gone" if self.revision == 1 => Ok(self.node(3)),
                b"added" if self.revision == 2 => Ok(self.node(4)),
                b"flip" => Ok(self.node(5)),
                b"child" if parent.node == 2 => Ok(self.node(6)),
                _ => Err(Error::NotFound),
            }
        })
    }
    fn list<'a>(
        &'a self,
        _: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            let names = if self.revision == 1 {
                vec![b"file".as_slice(), b"gone"]
            } else {
                vec![b"file".as_slice(), b"added"]
            };
            let entries = names
                .iter()
                .enumerate()
                .skip(offset as usize)
                .take(limit)
                .map(|(index, name)| NamedNode {
                    name: name.to_vec(),
                    node: self.node(if index == 0 {
                        1
                    } else if self.revision == 1 {
                        3
                    } else {
                        4
                    }),
                })
                .collect::<Vec<_>>();
            let end = offset + entries.len() as u64;
            Ok(BackendPage {
                entries,
                next_offset: (end < names.len() as u64).then_some(end),
            })
        })
    }
    fn read<'a>(&'a self, _: &'a NodeKey, offset: u64, size: usize) -> BackendFuture<'a, Vec<u8>> {
        Box::pin(async move {
            if let Some((started, resume)) = &self.block {
                started.notify_one();
                resume.notified().await;
            }
            let bytes = if self.revision == 1 { b"old" } else { b"new" };
            Ok(bytes[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, _: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async { Ok(None) })
    }
}
#[derive(Default)]
struct Invalidate {
    calls: AtomicUsize,
    fail: AtomicUsize,
    names: Mutex<Vec<Vec<u8>>>,
}
impl Invalidator for Invalidate {
    fn invalidate(&self, plan: &Invalidation) -> io::Result<()> {
        self.names.lock().unwrap().extend(plan.root_names.clone());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call < self.fail.load(Ordering::SeqCst) {
            Err(io::Error::other("injected invalidation failure"))
        } else {
            Ok(())
        }
    }
}
fn snapshot(revision: u8) -> Arc<Snapshot> {
    Arc::new(Snapshot {
        revision,
        block: None,
    })
}
#[tokio::test]
async fn files_and_directory_cookies_pin_while_new_root_paths_switch() {
    let old = snapshot(1);
    let core = Core::new(old.clone(), old.root()).unwrap();
    let old_file = core.lookup(1, b"file").await.unwrap();
    let handle = core.open(old_file.inode).unwrap();
    let root_handle = core.opendir(1).unwrap();
    let first = core.readdir(root_handle, 0, 1).await.unwrap();
    let cookie = first.entries[0].cookie;
    let dir = core.lookup(1, b"dir").await.unwrap();
    assert_eq!(core.lookup(1, b"added").await, Err(Error::NotFound));
    let next = snapshot(2);
    let invalidator = Invalidate::default();
    core.transition(next.clone(), next.root(), &invalidator)
        .unwrap();
    let new_file = core.lookup(1, b"file").await.unwrap();
    assert_ne!(old_file.inode, new_file.inode);
    assert_eq!(core.read(handle, 0, 3).await.unwrap(), b"old");
    let new_handle = core.open(new_file.inode).unwrap();
    assert_eq!(core.read(new_handle, 0, 3).await.unwrap(), b"new");
    assert_eq!(
        core.readdir(root_handle, cookie, 1).await.unwrap().entries[0].name,
        b"gone"
    );
    let new_root = core.opendir(1).unwrap();
    assert_eq!(
        core.readdir(new_root, 0, 2).await.unwrap().entries[1].name,
        b"added"
    );
    assert_eq!(core.lookup(1, b"gone").await, Err(Error::NotFound));
    assert!(core.lookup(1, b"added").await.is_ok());
    assert_eq!(
        core.lookup(dir.inode, b"child")
            .await
            .unwrap()
            .node
            .key
            .revision,
        old.root().key.revision
    );
    assert!(
        invalidator
            .names
            .lock()
            .unwrap()
            .contains(&b"added".to_vec())
    );
    // Returning to an identical pinned backend still allocates a fresh generation.
    core.transition(old.clone(), old.root(), &invalidator)
        .unwrap();
    assert_ne!(core.lookup(1, b"file").await.unwrap().inode, old_file.inode);
}
#[tokio::test]
async fn rollback_retains_candidate_handles_and_poison_requires_explicit_recovery() {
    let old = snapshot(1);
    let core = Core::new(old.clone(), old.root()).unwrap();
    let next = snapshot(2);
    let invalidator = Invalidate {
        fail: AtomicUsize::new(1),
        ..Default::default()
    };
    assert!(matches!(
        core.transition(next.clone(), next.root(), &invalidator),
        Err(TransitionError::Invalidation { rollback: None, .. })
    ));
    assert_eq!(
        core.lookup(1, b"file").await.unwrap().node.key.revision,
        old.root().key.revision
    );
    let invalidator = Invalidate {
        fail: AtomicUsize::new(2),
        ..Default::default()
    };
    assert!(matches!(
        core.transition(next.clone(), next.root(), &invalidator),
        Err(TransitionError::Invalidation {
            rollback: Some(_),
            ..
        })
    ));
    assert!(matches!(
        core.transition(next.clone(), next.root(), &invalidator),
        Err(TransitionError::Poisoned)
    ));
    core.recover_transition_cache(&invalidator).unwrap();
    core.transition(next.clone(), next.root(), &invalidator)
        .unwrap();
}
#[tokio::test]
async fn admitted_reads_finish_their_original_view_and_teardown_rejects_acknowledgment() {
    let old = Arc::new(Snapshot {
        revision: 1,
        block: Some((Notify::new(), Notify::new())),
    });
    let core = Arc::new(Core::new(old.clone(), old.root()).unwrap());
    let entry = core.lookup(1, b"file").await.unwrap();
    let handle = core.open(entry.inode).unwrap();
    #[allow(clippy::disallowed_methods)]
    let read = {
        let core = core.clone();
        tokio::spawn(async move { core.read(handle, 0, 3).await })
    };
    old.block.as_ref().unwrap().0.notified().await;
    let next = snapshot(2);
    core.transition(next.clone(), next.root(), &Invalidate::default())
        .unwrap();
    old.block.as_ref().unwrap().1.notify_one();
    assert_eq!(read.await.unwrap().unwrap(), b"old");
    struct Stop(Arc<Core>);
    impl Invalidator for Stop {
        fn invalidate(&self, _: &Invalidation) -> io::Result<()> {
            self.0.shutdown().unwrap();
            Ok(())
        }
    }
    assert!(matches!(
        core.transition(next.clone(), next.root(), &Stop(core.clone())),
        Err(TransitionError::Core(Error::Unmounted))
    ));
}
#[tokio::test]
async fn generations_names_and_inodes_are_explicitly_bounded() {
    let old = snapshot(1);
    let next = snapshot(2);
    let core = Core::with_limits(
        old.clone(),
        old.root(),
        Limits {
            max_generations: 1,
            max_inodes: 2,
            max_root_names: 1,
        },
    )
    .unwrap();
    core.lookup(1, b"file").await.unwrap();
    assert_eq!(core.lookup(1, b"other").await, Err(Error::ResourceLimit));
    assert!(matches!(
        core.transition(next.clone(), next.root(), &Invalidate::default()),
        Err(TransitionError::Core(Error::ResourceLimit))
    ));
    assert_eq!(
        core.lookup(1, b"file").await.unwrap().node.key.revision,
        old.root().key.revision
    );
}

#[test]
fn callback_reply_barrier_drains_before_publication_without_holding_during_invalidation() {
    let old = snapshot(1);
    let core = Arc::new(Core::new(old.clone(), old.root()).unwrap());
    let (entered, wait_entered) = std::sync::mpsc::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let (miss, wait_miss) = std::sync::mpsc::channel();
    let callback = {
        let core = core.clone();
        std::thread::spawn(move || {
            let _reply = core.callback_guard().unwrap();
            entered.send(()).unwrap();
            wait_miss.recv().unwrap();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            assert_eq!(
                runtime.block_on(core.lookup(1, b"late-miss")),
                Err(Error::NotFound)
            );
            wait_release.recv().unwrap();
        })
    };
    wait_entered.recv().unwrap();
    let (notified, wait_notified) = std::sync::mpsc::channel();
    struct NotifyInvalidator(std::sync::mpsc::Sender<Vec<Vec<u8>>>);
    impl Invalidator for NotifyInvalidator {
        fn invalidate(&self, plan: &Invalidation) -> io::Result<()> {
            self.0.send(plan.root_names.clone()).unwrap();
            Ok(())
        }
    }
    let control = {
        let core = core.clone();
        std::thread::spawn(move || {
            let next = snapshot(2);
            core.transition(next.clone(), next.root(), &NotifyInvalidator(notified))
                .unwrap()
        })
    };
    assert!(matches!(
        wait_notified.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    miss.send(()).unwrap();
    release.send(()).unwrap();
    callback.join().unwrap();
    let names = wait_notified
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(names.contains(&b"late-miss".to_vec()));
    control.join().unwrap();
}
#[test]
fn failed_attempt_retains_handles_opened_on_visible_candidate() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let old = snapshot(1);
    let core = Arc::new(Core::new(old.clone(), old.root()).unwrap());
    struct Candidate {
        core: Arc<Core>,
        runtime: tokio::runtime::Handle,
        handle: Mutex<Option<u64>>,
        calls: AtomicUsize,
    }
    impl Invalidator for Candidate {
        fn invalidate(&self, _: &Invalidation) -> io::Result<()> {
            // Callback admission is open during invalidation, including backend work.
            let _reply = self.core.callback_guard().unwrap();
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let entry = self.runtime.block_on(self.core.lookup(1, b"file")).unwrap();
                *self.handle.lock().unwrap() = Some(self.core.open(entry.inode).unwrap());
                Err(io::Error::other("fail after publication"))
            } else {
                Ok(())
            }
        }
    }
    let invalidator = Candidate {
        core: core.clone(),
        runtime: runtime.handle().clone(),
        handle: Mutex::new(None),
        calls: AtomicUsize::new(0),
    };
    let next = snapshot(2);
    assert!(
        core.transition(next.clone(), next.root(), &invalidator)
            .is_err()
    );
    let handle = invalidator.handle.lock().unwrap().unwrap();
    assert_eq!(runtime.block_on(core.read(handle, 0, 3)).unwrap(), b"new");
    let old_entry = runtime.block_on(core.lookup(1, b"file")).unwrap();
    let old_handle = core.open(old_entry.inode).unwrap();
    assert_eq!(
        runtime.block_on(core.read(old_handle, 0, 3)).unwrap(),
        b"old"
    );
}
#[tokio::test]
async fn cancelled_async_control_finishes_rollback_independently() {
    let old = snapshot(1);
    let core = Arc::new(Core::new(old.clone(), old.root()).unwrap());
    struct Block {
        calls: AtomicUsize,
        started: Notify,
        resume: Mutex<std::sync::mpsc::Receiver<()>>,
        rolled_back: Notify,
    }
    impl Invalidator for Block {
        fn invalidate(&self, _: &Invalidation) -> io::Result<()> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                self.resume.lock().unwrap().recv().unwrap();
                Err(io::Error::other("rollback after cancellation"))
            } else {
                self.rolled_back.notify_one();
                Ok(())
            }
        }
    }
    let (send, resume) = std::sync::mpsc::channel();
    let invalidator = Arc::new(Block {
        calls: AtomicUsize::new(0),
        started: Notify::new(),
        resume: Mutex::new(resume),
        rolled_back: Notify::new(),
    });
    #[allow(clippy::disallowed_methods)]
    let task = {
        let core = core.clone();
        let invalidator = invalidator.clone();
        tokio::spawn(async move {
            let next = snapshot(2);
            core.transition_async(next.clone(), next.root(), invalidator)
                .await
        })
    };
    invalidator.started.notified().await;
    task.abort();
    let _ = task.await;
    send.send(()).unwrap();
    invalidator.rolled_back.notified().await;
    assert_eq!(
        core.lookup(1, b"file").await.unwrap().node.key.revision,
        old.root().key.revision
    );
}
