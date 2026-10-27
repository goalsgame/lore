// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Private channel-controlled jobs prove ownership beyond caller cancellation.
#![allow(clippy::disallowed_methods)]
mod stores;

use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_io::lease::IoLease;
use lore_revision::repository::working_bootstrap::PendingError;

use super::*;

struct Resume(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Resume {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _closed = sender.send(());
        }
    }
}
struct Callback {
    called: Arc<AtomicBool>,
    entered: Option<std::sync::mpsc::Sender<()>>,
    resume: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
#[async_trait::async_trait]
impl lore_revision::fs::filesystem_provider::FilesystemProvider for Callback {
    fn holds_state(&self) -> bool {
        self.called.store(true, Ordering::Release);
        if let Some(entered) = &self.entered {
            entered.send(()).unwrap();
        }
        if let Some(resume) = self.resume.lock().unwrap().take() {
            resume.recv().unwrap();
        }
        true
    }
    async fn begin_operation(
        &self,
    ) -> std::result::Result<
        Arc<lore_revision::fs::filesystem_provider::InstanceOperationImpl>,
        lore_revision::fs::filesystem_provider::FsError,
    > {
        Err(lore_revision::fs::filesystem_provider::FsError::internal(
            "pending fixture has no operations",
        ))
    }
}
fn callback(called: Arc<AtomicBool>) -> Arc<Callback> {
    Arc::new(Callback {
        called,
        entered: None,
        resume: Mutex::new(None),
    })
}
fn client_mutex(token: &RepositoryWriteToken) -> Arc<tokio::sync::Mutex<()>> {
    let RepositoryWriteToken::Client(guard) = token else {
        panic!("Client required")
    };
    tokio::sync::OwnedMutexGuard::mutex(guard.as_ref()).clone()
}
#[tokio::test]
async fn successful_pending_metadata_retains_exclusion_and_terminally_retires_on_drop() {
    use lore_revision::fs::provider_registry::Error as RegistryError;
    let fixture = Fixture::new();
    let jobs = MetadataInitializer::new(1).unwrap();
    let token = fixture.token().await;
    let mutex = client_mutex(&token);
    let RepositoryWriteToken::Client(guard) = &token else {
        unreachable!()
    };
    let old = Arc::downgrade(guard);
    let metadata = jobs
        .prepare_pending(
            fixture.registration.clone(),
            token,
            Mode::CreateNew(config()),
        )
        .await
        .unwrap();
    assert!(old.upgrade().is_none());
    assert!(mutex.try_lock().is_err());
    assert_eq!(metadata.location().target().path(), fixture.target);
    assert!(metadata.config().is_experimental_working());
    let called = Arc::new(AtomicBool::new(false));
    assert!(matches!(
        fixture.registration.publish(callback(called.clone())),
        Err(RegistryError::AdmissionBusy)
    ));
    assert!(!called.load(Ordering::Acquire));
    assert!(!fixture.metadata.join("immutable").exists());
    assert!(!fixture.metadata.join("mutable").exists());
    drop(metadata);
    assert!(mutex.try_lock().is_ok());
    assert!(matches!(
        fixture.registration.bootstrap_location(),
        Err(RegistryError::Retired)
    ));
    assert!(fixture.metadata.join("config.toml").is_file());
}
#[tokio::test]
async fn pending_rejection_returns_wrong_shared_and_nonclient_tokens_without_files() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let jobs = MetadataInitializer::new(1).unwrap();
    let wrong = other.token().await;
    let error = jobs
        .prepare_pending(
            fixture.registration.clone(),
            wrong,
            Mode::CreateNew(config()),
        )
        .await
        .err()
        .unwrap();
    let PendingError::Rejected {
        error: Error::InvalidToken,
        token,
        ..
    } = error
    else {
        panic!("wrong root accepted")
    };
    drop(token);
    let token = fixture.token().await;
    let sibling = token.share();
    let mutex = client_mutex(&token);
    let error = jobs
        .prepare_pending(
            fixture.registration.clone(),
            token,
            Mode::CreateNew(config()),
        )
        .await
        .err()
        .unwrap();
    let PendingError::Rejected {
        error: Error::Busy,
        token,
        ..
    } = error
    else {
        panic!("shared token accepted")
    };
    drop(token);
    assert!(mutex.try_lock().is_err());
    drop(sibling);
    let error = jobs
        .prepare_pending(
            fixture.registration.clone(),
            RepositoryWriteToken::InMemory,
            Mode::CreateNew(config()),
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        PendingError::Rejected {
            error: Error::InvalidToken,
            ..
        }
    ));
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
    assert!(fixture.registration.bootstrap_location().is_ok());
    let metadata = jobs
        .prepare_pending(
            fixture.registration.clone(),
            fixture.token().await,
            Mode::CreateNew(config()),
        )
        .await
        .unwrap();
    drop(metadata);
}
struct Parent {
    registration: Arc<Registration>,
    mutex: Arc<tokio::sync::Mutex<()>>,
    done: std::sync::mpsc::Sender<bool>,
}
impl Drop for Parent {
    fn drop(&mut self) {
        let safe = matches!(
            self.registration.bootstrap_location(),
            Err(lore_revision::fs::provider_registry::Error::Retired)
        ) && self.mutex.try_lock().is_ok();
        let _closed = self.done.send(safe);
    }
}
#[test]
fn queued_and_running_pending_jobs_keep_client_and_parent_after_runtime_drop() {
    for running in [false, true] {
        let fixture = Fixture::new();
        let jobs = MetadataInitializer::new(1).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let token = runtime.block_on(fixture.token());
        let mutex = client_mutex(&token);
        let (done, completed) = std::sync::mpsc::channel();
        let parent = Arc::new(Parent {
            registration: fixture.registration.clone(),
            mutex: mutex.clone(),
            done,
        });
        let weak = Arc::downgrade(&parent);
        let lease = IoLease::new(parent);
        let (entered, entering) = std::sync::mpsc::channel();
        let (resume, resuming) = std::sync::mpsc::channel();
        let resume = Resume(Some(resume));
        if running {
            jobs.pause_initialize(move || {
                entered.send(()).unwrap();
                resuming.recv().unwrap();
            });
        } else {
            runtime.spawn_blocking(move || {
                entered.send(()).unwrap();
                resuming.recv().unwrap();
            });
            entering.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        let mut command = Box::pin(lease.scope(jobs.prepare_pending(
            fixture.registration.clone(),
            token,
            Mode::CreateNew(config()),
        )));
        runtime.block_on(async {
            assert!(matches!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(command.as_mut().poll(cx))).await,
                std::task::Poll::Pending
            ));
        });
        if running {
            entering.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        drop(command);
        drop(lease);
        runtime.shutdown_timeout(Duration::ZERO);
        assert!(weak.upgrade().is_some());
        assert!(mutex.try_lock().is_err());
        assert!(completed.try_recv().is_err());
        assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 0);
        let called = Arc::new(AtomicBool::new(false));
        assert!(matches!(
            fixture.registration.publish(callback(called.clone())),
            Err(lore_revision::fs::provider_registry::Error::AdmissionBusy)
        ));
        assert!(!called.load(Ordering::Acquire));
        drop(resume);
        assert!(completed.recv_timeout(Duration::from_secs(10)).unwrap());
        assert!(weak.upgrade().is_none());
        assert!(fixture.metadata.join("config.toml").is_file());
    }
}
#[tokio::test]
async fn publication_rechecks_after_callback_races_pending_admission() {
    use lore_revision::fs::provider_registry::Error as RegistryError;
    let fixture = Fixture::new();
    let jobs = MetadataInitializer::new(1).unwrap();
    let base = jobs
        .prepare(
            fixture.registration.clone(),
            fixture.token().await,
            Mode::CreateNew(config()),
        )
        .await
        .unwrap();
    drop(base);
    let called = Arc::new(AtomicBool::new(false));
    let (entered, entering) = std::sync::mpsc::channel();
    let (resume, resuming) = std::sync::mpsc::channel();
    let resume = Resume(Some(resume));
    let provider = Arc::new(Callback {
        called: called.clone(),
        entered: Some(entered),
        resume: Mutex::new(Some(resuming)),
    });
    let registration = fixture.registration.clone();
    let (finished, finishing) = std::sync::mpsc::channel();
    let publisher = std::thread::spawn(move || {
        let result = registration.publish(provider);
        finished.send(result).unwrap();
    });
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let metadata = jobs
        .prepare_pending(
            fixture.registration.clone(),
            fixture.token().await,
            Mode::Reuse,
        )
        .await
        .unwrap();
    drop(resume);
    assert!(matches!(
        finishing.recv_timeout(Duration::from_secs(10)).unwrap(),
        Err(RegistryError::AdmissionBusy)
    ));
    publisher.join().unwrap();
    assert!(called.load(Ordering::Acquire));
    drop(metadata);
    assert!(matches!(
        fixture.registration.bootstrap_location(),
        Err(RegistryError::Retired)
    ));
}
#[tokio::test]
async fn retired_or_ready_registration_rejects_before_metadata_work() {
    use lore_revision::fs::provider_registry::Error as RegistryError;
    for ready in [false, true] {
        let fixture = Fixture::new();
        let jobs = MetadataInitializer::new(1).unwrap();
        if ready {
            let base = jobs
                .prepare(
                    fixture.registration.clone(),
                    fixture.token().await,
                    Mode::CreateNew(config()),
                )
                .await
                .unwrap();
            drop(base);
            fixture
                .registration
                .publish(callback(Arc::new(AtomicBool::new(false))))
                .unwrap();
        } else {
            fixture.registration.retire().unwrap();
        }
        let before = std::fs::read_dir(&fixture.metadata).unwrap().count();
        let result = jobs
            .prepare_pending(
                fixture.registration.clone(),
                fixture.token().await,
                Mode::CreateNew(config()),
            )
            .await;
        assert!(matches!(
            result,
            Err(PendingError::Rejected {
                error: Error::Registry(RegistryError::Retired | RegistryError::AlreadyPublished),
                ..
            })
        ));
        assert_eq!(
            std::fs::read_dir(&fixture.metadata).unwrap().count(),
            before
        );
    }
}

#[tokio::test]
async fn qualified_metadata_error_retires_before_client_and_parent_release() {
    let fixture = Fixture::new();
    let sentinel = fixture.metadata.join("sentinel");
    std::fs::write(&sentinel, b"preserve original bytes").unwrap();
    let jobs = MetadataInitializer::new(1).unwrap();
    let token = fixture.token().await;
    let mutex = client_mutex(&token);
    let (done, completed) = std::sync::mpsc::channel();
    let parent = Arc::new(Parent {
        registration: fixture.registration.clone(),
        mutex: mutex.clone(),
        done,
    });
    let weak = Arc::downgrade(&parent);
    let lease = IoLease::new(parent);
    let error = lease
        .scope(jobs.prepare_pending(
            fixture.registration.clone(),
            token,
            Mode::CreateNew(config()),
        ))
        .await
        .err()
        .unwrap();
    assert!(matches!(error, PendingError::Retired(Error::NotEmpty)));
    assert!(matches!(
        fixture.registration.bootstrap_location(),
        Err(lore_revision::fs::provider_registry::Error::Retired)
    ));
    assert!(mutex.try_lock().is_ok());
    assert_eq!(
        std::fs::read(&sentinel).unwrap(),
        b"preserve original bytes"
    );
    assert_eq!(std::fs::read_dir(&fixture.metadata).unwrap().count(), 1);
    drop(lease);
    assert!(completed.recv_timeout(Duration::from_secs(10)).unwrap());
    assert!(weak.upgrade().is_none());
}
