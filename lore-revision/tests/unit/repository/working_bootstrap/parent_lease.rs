// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// A queued actual initializer outlives cancellation and its caller runtime.
#![allow(clippy::disallowed_methods)]
use std::sync::Weak;
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

struct Parent {
    root_mutex: Arc<tokio::sync::Mutex<()>>,
    released: std::sync::mpsc::Sender<bool>,
}
impl Drop for Parent {
    fn drop(&mut self) {
        let unlocked = self.root_mutex.try_lock().is_ok();
        let _closed = self.released.send(unlocked);
    }
}
fn parent(
    token: &RepositoryWriteToken,
) -> (IoLease, Weak<Parent>, std::sync::mpsc::Receiver<bool>) {
    let RepositoryWriteToken::Client(guard) = token else {
        panic!("fixture requires Client token")
    };
    let (released, result) = std::sync::mpsc::channel();
    let parent = Arc::new(Parent {
        root_mutex: tokio::sync::OwnedMutexGuard::mutex(guard.as_ref()).clone(),
        released,
    });
    let weak = Arc::downgrade(&parent);
    (IoLease::new(parent), weak, result)
}
#[tokio::test]
async fn prepared_metadata_moves_parent_without_releasing_it_before_root_token() {
    let fixture = Fixture::new();
    let token = fixture.token().await;
    let (lease, weak, released) = parent(&token);
    let prepared = lease
        .scope(MetadataInitializer::new(1).unwrap().prepare(
            fixture.registration.clone(),
            token,
            Mode::CreateNew(config()),
        ))
        .await
        .unwrap();
    drop(lease);
    assert!(weak.upgrade().is_some());
    assert!(released.try_recv().is_err());
    drop(prepared);
    assert!(released.recv_timeout(Duration::from_secs(10)).unwrap());
    assert!(weak.upgrade().is_none());
}
#[tokio::test]
async fn failed_metadata_destroys_root_capabilities_before_parent_accounting() {
    let fixture = Fixture::new();
    let token = fixture.token().await;
    let (lease, weak, released) = parent(&token);
    let result = lease
        .scope(MetadataInitializer::new(1).unwrap().prepare(
            fixture.registration.clone(),
            token,
            Mode::CreateNew(Default::default()),
        ))
        .await;
    assert!(result.is_err());
    drop(result);
    drop(lease);
    assert!(released.recv_timeout(Duration::from_secs(10)).unwrap());
    assert!(weak.upgrade().is_none());
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
}
#[test]
fn cancelled_queued_metadata_retains_parent_after_caller_runtime_drop() {
    let fixture = Fixture::new();
    let initializer = MetadataInitializer::new(1).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (entered, entering) = std::sync::mpsc::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    let resume = Resume(Some(resume));
    runtime.spawn_blocking(move || {
        entered.send(()).unwrap();
        resuming.recv().unwrap();
    });
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let token = runtime.block_on(fixture.token());
    let (lease, weak, released) = parent(&token);
    let jobs = initializer.clone();
    let registration = fixture.registration.clone();
    let command = runtime.spawn(async move {
        lease
            .scope(jobs.prepare(registration, token, Mode::CreateNew(config())))
            .await
    });
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            while initializer.jobs.available_permits() != 0 {
                tokio::task::yield_now().await;
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
    assert!(weak.upgrade().is_some());
    assert!(released.try_recv().is_err());
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
    resume.release();
    assert!(released.recv_timeout(Duration::from_secs(10)).unwrap());
    assert!(weak.upgrade().is_none());
    assert!(fixture.metadata.join("config.toml").is_file());
}
