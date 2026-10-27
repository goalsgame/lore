// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use lore_vfs::core::Backend;
use lore_vfs::core::BackendFuture;
use lore_vfs::core::BackendPage;
use lore_vfs::core::NamedNode;
use lore_vfs::core::Node;
use lore_vfs::core::NodeKey;

use super::*;

#[derive(Default)]
pub(super) struct Fixture {
    pub(super) denied: AtomicBool,
    pub(super) started: AtomicBool,
    pub(super) dropped: AtomicBool,
}
struct DropFlag<'a>(&'a AtomicBool);
impl Drop for DropFlag<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
fn node(number: u32) -> Node {
    Node {
        key: NodeKey {
            repository: [7; 16].into(),
            revision: Default::default(),
            node: number,
            exposure: u64::from(number),
        },
        kind: if number == 0 || number == 2 {
            NodeKind::Directory
        } else {
            NodeKind::File
        },
        size: if number == 1 || number == 3 { 7 } else { 0 },
        mode: 0o555,
    }
}
impl Backend for Fixture {
    fn revalidate<'a>(&'a self, _key: &'a NodeKey, _write: bool) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.denied.load(Ordering::Acquire) {
                Err(Error::PermissionDenied)
            } else {
                Ok(())
            }
        })
    }
    fn lookup<'a>(&'a self, parent: &'a NodeKey, name: &'a [u8]) -> BackendFuture<'a, Node> {
        Box::pin(async move {
            match (parent.node, name) {
                (0, name) if name.eq_ignore_ascii_case(b"file") => Ok(node(1)),
                (0, b"linked") => Ok(node(2)),
                (2, b"child") => Ok(node(3)),
                (_, b"denied") => Err(Error::PermissionDenied),
                (_, b"panic") => panic!("contained fixture backend panic"),
                (_, b"blocked") => {
                    let _drop = DropFlag(&self.dropped);
                    self.started.store(true, Ordering::Release);
                    std::future::pending().await
                }
                _ => Err(Error::NotFound),
            }
        })
    }
    fn list<'a>(
        &'a self,
        directory: &'a NodeKey,
        offset: u64,
        limit: usize,
    ) -> BackendFuture<'a, BackendPage> {
        Box::pin(async move {
            let names = if directory.node == 0 {
                vec![(b"file".to_vec(), node(1)), (b"linked".to_vec(), node(2))]
            } else {
                vec![(b"child".to_vec(), node(3))]
            };
            let total = names.len();
            let entries = names
                .into_iter()
                .skip(offset as usize)
                .take(limit)
                .map(|(name, node)| NamedNode { name, node })
                .collect::<Vec<_>>();
            let end = offset + entries.len() as u64;
            Ok(BackendPage {
                entries,
                next_offset: (end < total as u64).then_some(end),
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
            if self.denied.load(Ordering::Acquire) {
                return Err(Error::PermissionDenied);
            }
            Ok(b"payload"[offset as usize..offset as usize + size].to_vec())
        })
    }
    fn digest<'a>(&'a self, file: &'a NodeKey) -> BackendFuture<'a, Option<[u8; 32]>> {
        Box::pin(async move {
            if self.denied.load(Ordering::Acquire) {
                return Err(Error::PermissionDenied);
            }
            Ok((file.node == 1).then_some([42; 32]))
        })
    }
}
pub(super) fn with_backend(backend: Arc<Fixture>) -> Arc<Core> {
    Arc::new(Core::new(backend, node(0)).unwrap())
}
pub(super) fn core() -> Arc<Core> {
    with_backend(Arc::new(Fixture::default()))
}
/// # Safety
/// The caller must provide a writable session output pointer.
#[cfg(feature = "test-fixture")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lore_fskit_fixture_open(abi: u32, session: *mut u64) -> i32 {
    ffi(|| {
        if session.is_null() {
            return Err(INVALID);
        }
        // SAFETY: validated caller-owned output pointer.
        unsafe {
            session.write(0);
        }
        if abi != ABI {
            return Err(ABI_MISMATCH);
        }
        let id = retain(core())?;
        // SAFETY: validated caller-owned output pointer.
        unsafe {
            session.write(id);
        }
        Ok(())
    })
}
