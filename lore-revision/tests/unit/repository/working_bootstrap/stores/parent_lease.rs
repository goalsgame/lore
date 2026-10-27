// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Per-initializer channels pause an actual file-validation job, not its observer.
#![allow(clippy::disallowed_methods)]
use std::time::Duration;

use lore_io::lease::IoLease;

use super::*;
struct Resume(Option<std::sync::mpsc::Sender<()>>);
impl Resume {
    fn release(mut self) {
        if let Some(resume) = self.0.take() {
            let _closed = resume.send(());
        }
    }
}
impl Drop for Resume {
    fn drop(&mut self) {
        if let Some(resume) = self.0.take() {
            let _closed = resume.send(());
        }
    }
}

struct Parent(std::sync::mpsc::Sender<()>);
impl Drop for Parent {
    fn drop(&mut self) {
        let _closed = self.0.send(());
    }
}
fn parent() -> (
    IoLease,
    std::sync::Weak<Parent>,
    std::sync::mpsc::Receiver<()>,
) {
    let (send, recv) = std::sync::mpsc::channel();
    let parent = Arc::new(Parent(send));
    let weak = Arc::downgrade(&parent);
    (IoLease::new(parent), weak, recv)
}
#[tokio::test]
async fn completed_store_context_retains_neither_metadata_nor_caller_parent() {
    LORE_CONTEXT
        .scope(
            test_execution_with(LoreGlobalArgs {
                offline: 1,
                ..Default::default()
            }),
            async {
                let fixture = Fixture::new();
                let (metadata_parent, weak_metadata, metadata_done) = parent();
                let metadata = metadata_parent
                    .scope(prepare_metadata(&fixture, config(), true))
                    .await;
                drop(metadata_parent);
                assert!(weak_metadata.upgrade().is_some());
                let (caller_parent, weak_caller, caller_done) = parent();
                let initializer = StoreInitializer::new(1).unwrap();
                let context = tokio::time::timeout(
                    Duration::from_secs(20),
                    caller_parent
                        .scope(initializer.prepare(metadata, StoreMode::CreateNew(selection()))),
                )
                .await
                .unwrap()
                .unwrap();
                drop(caller_parent);
                metadata_done.recv_timeout(Duration::from_secs(10)).unwrap();
                caller_done.recv_timeout(Duration::from_secs(10)).unwrap();
                assert!(weak_metadata.upgrade().is_none());
                assert!(weak_caller.upgrade().is_none());
                assert!(context.repository.try_write_token().is_none());
                let token = tokio::time::timeout(Duration::from_secs(10), fixture.token())
                    .await
                    .unwrap();
                drop(token);
                assert!(!context.selection.branch.is_zero());
                drop(context);
            },
        )
        .await;
}
#[test]
fn cancelled_actual_store_validation_retains_parent_after_caller_runtime_drop() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = Fixture::new();
    let (metadata_parent, weak_metadata, metadata_done) = parent();
    let metadata = runtime.block_on(LORE_CONTEXT.scope(
        test_execution_with(LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        }),
        metadata_parent.scope(prepare_metadata(&fixture, config(), true)),
    ));
    drop(metadata_parent);
    let initializer = StoreInitializer::new(1).unwrap();
    let (entered, entering) = std::sync::mpsc::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    let resume = Resume(Some(resume));
    // This closure owns only channels; the observer holds no strong parent.
    initializer.pause_metadata_recheck(move || {
        entered.send(()).unwrap();
        resuming.recv().unwrap();
    });
    let jobs = initializer.clone();
    let (caller_parent, weak_caller, caller_done) = parent();
    let command = runtime.spawn(LORE_CONTEXT.scope(
        test_execution_with(LoreGlobalArgs {
            offline: 1,
            ..Default::default()
        }),
        async move {
            caller_parent
                .scope(jobs.prepare(metadata, StoreMode::CreateNew(selection())))
                .await
        },
    ));
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match entering.try_recv() {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("store pause lost: {error}"),
                }
            }
        })
        .await
        .unwrap();
    });
    command.abort();
    runtime.block_on(async {
        let _cancelled = command.await;
    });
    runtime.shutdown_timeout(Duration::ZERO);
    assert!(weak_metadata.upgrade().is_some());
    assert!(weak_caller.upgrade().is_some());
    assert!(metadata_done.try_recv().is_err());
    assert!(caller_done.try_recv().is_err());
    resume.release();
    metadata_done.recv_timeout(Duration::from_secs(10)).unwrap();
    caller_done.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(weak_metadata.upgrade().is_none());
    assert!(weak_caller.upgrade().is_none());
    assert!(
        !fixture
            .metadata
            .join(".working-store-initializing")
            .exists()
    );
}
