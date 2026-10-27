// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Private retained-source publication transport. A successful acknowledgment
//! requires actual publisher joining and separate asynchronous source checks.

mod progress;
pub(super) use progress::HandoffGuard;
pub(super) use progress::JoinerPanicGuard;
pub(super) use progress::Progress;

use super::*;
use crate::linux_working::OwnedSourcePreparation;
use crate::linux_working::SourceContinuation;
use crate::recovery::published::PublishedCheckpointImage;

struct PublisherTask {
    _panic: JoinerPanicGuard,
    snapshot: Option<crate::namespace::NamespaceSnapshot>,
    continuation: Option<SourceContinuation>,
    #[cfg(all(test, unix))]
    hooks: Arc<Hooks>,
    progress: Arc<Progress>,
}
// Keep this wrapper intact across post-join awaits: cancellation drops captures
// before progress. Extracting its fields before those awaits reverses that order.
struct WorkerOutput {
    publication: Option<Result<PublishedCheckpointImage>>,
    continuation: Option<SourceContinuation>,
    _progress: Arc<Progress>,
}
#[allow(
    dead_code,
    reason = "Opaque fields retain the published source and exact candidate until the acknowledgment drops."
)]
pub(crate) struct SourceAcknowledgment {
    source: PublishedCheckpointImage,
    continuation: SourceContinuation,
}
pub(crate) struct SourceArtifactInputs<'a> {
    source: &'a PublishedCheckpointImage,
    facts: &'a crate::transition_prepare::SourceFacts,
    candidate: crate::linux_working::CapturedCheckpointImage<'a>,
    memory: &'a mut crate::capture_memory::CaptureMemory,
}
impl SourceAcknowledgment {
    pub(crate) fn check_transition_owner(&self, owner: &Arc<CheckpointOwner>) -> Result<()> {
        self.continuation.check_transition_owner(owner)
    }
    pub(crate) async fn validate_for_transition(
        &self,
    ) -> std::result::Result<(), crate::linux_working::SourcePreparationError> {
        self.continuation.validate_joined(&self.source).await
    }

    pub(crate) async fn install_in_place(
        &mut self,
        reservation: &crate::publication_ownership::successor::SuccessorReservation,
    ) -> std::result::Result<(), crate::linux_working::SourcePreparationError> {
        self.continuation.validate_joined(&self.source).await?;
        self.continuation.install_in_place(reservation).await
    }
    pub(crate) fn artifact_inputs(&mut self) -> SourceArtifactInputs<'_> {
        let (facts, candidate, memory) = self.continuation.artifact_parts();
        SourceArtifactInputs {
            source: &self.source,
            facts,
            candidate,
            memory,
        }
    }
}
impl<'a> SourceArtifactInputs<'a> {
    pub(crate) fn into_parts(
        self,
    ) -> (
        &'a PublishedCheckpointImage,
        &'a crate::transition_prepare::SourceFacts,
        crate::linux_working::CapturedCheckpointImage<'a>,
        &'a mut crate::capture_memory::CaptureMemory,
    ) {
        (self.source, self.facts, self.candidate, self.memory)
    }
}
pub(crate) struct JoinedSource {
    output: WorkerOutput,
}
impl JoinedSource {
    pub(crate) async fn acknowledge(
        mut self,
    ) -> std::result::Result<SourceAcknowledgment, crate::linux_working::SourcePreparationError>
    {
        if self
            .output
            .publication
            .as_ref()
            .expect("publisher result")
            .is_err()
        {
            let error = self
                .output
                .publication
                .take()
                .expect("publisher result")
                .err()
                .expect("publication error");
            return Err(crate::linux_working::Error::from(error).into());
        }
        let source = self
            .output
            .publication
            .as_ref()
            .expect("publisher result")
            .as_ref()
            .expect("published image");
        let continuation = self
            .output
            .continuation
            .as_ref()
            .expect("source continuation");
        continuation.validate_joined(source).await?;
        let source = self
            .output
            .publication
            .take()
            .expect("publisher result")
            .expect("published image");
        let continuation = self
            .output
            .continuation
            .take()
            .expect("source continuation");
        let acknowledged = SourceAcknowledgment {
            source,
            continuation,
        };
        drop(self);
        Ok(acknowledged)
    }
}

struct JoinJob {
    guard: HandoffGuard,
    thread: Option<std::thread::JoinHandle<WorkerOutput>>,
    _progress: Arc<Progress>,
}
enum Command {
    Publisher(JoinJob),
    NoPublisher(Error, Arc<Progress>),
}
impl PublisherTask {
    fn run(mut self) -> WorkerOutput {
        let publication =
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.publish_once())) {
                Ok(publication) => publication,
                Err(_panic) => {
                    self.progress.owner.poisoned.store(true, Ordering::Release);
                    Err(Error::Poisoned)
                }
            };
        WorkerOutput {
            publication: Some(publication),
            continuation: self.continuation.take(),
            _progress: self.progress.clone(),
        }
    }
    fn publish_once(&mut self) -> Result<PublishedCheckpointImage> {
        #[cfg(all(test, unix))]
        {
            if let Some(pause) = self.hooks.exit.lock().expect("exit pause").clone() {
                EXIT.with(|exit| *exit.borrow_mut() = Some(Exit(pause)));
            }
            if let Some(pause) = self.hooks.io.lock().expect("IO pause").clone() {
                pause.enter();
            }
            assert!(
                !self.hooks.panic_publisher.load(Ordering::Acquire),
                "injected source publisher panic"
            );
        }
        let owner = &self.progress.owner;
        let snapshot = self.snapshot.take().expect("source snapshot");
        match owner.recovery.lock() {
            Ok(mut recovery) => {
                let result = recovery.publish_retained(
                    snapshot,
                    self.continuation
                        .as_mut()
                        .expect("source continuation")
                        .memory_mut(),
                );
                if let Err(error) = &result
                    && publication_error_requires_reopen(&recovery, error)
                {
                    owner.poisoned.store(true, Ordering::Release);
                }
                result.map_err(Error::Recovery)
            }
            Err(_poison) => {
                owner.poisoned.store(true, Ordering::Release);
                Err(Error::Poisoned)
            }
        }
    }
}
impl CheckpointOwner {
    pub(crate) async fn publish_owned_source(
        self: &Arc<Self>,
        prepared: OwnedSourcePreparation,
    ) -> std::result::Result<JoinedSource, crate::linux_working::SourcePreparationError> {
        prepared.check_publication_owner(self)?;
        prepared.validate_for_publication().await?;
        let (commands, receive) = std::sync::mpsc::sync_channel(1);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let panic_guard = JoinerPanicGuard::new(self);
        #[cfg(all(test, unix))]
        let hooks = self.source_hooks.clone();
        #[cfg(all(test, unix))]
        if hooks.fail_joiner.load(Ordering::Acquire) {
            return Err(Error::Spawn(std::io::Error::other(
                "injected source joiner spawn failure",
            ))
            .into());
        }
        // The joiner exists before admission; no fallible second joiner spawn
        // can strand an admitted publisher on the caller's runtime.
        std::thread::Builder::new()
            .name("lore-source-join".into())
            .spawn(move || {
                let _panic = panic_guard;
                #[cfg(all(test, unix))]
                if hooks.disconnect.load(Ordering::Acquire) {
                    drop(receive);
                    return;
                }
                let Ok(command) = receive.recv() else {
                    return;
                };
                match command {
                    Command::Publisher(mut job) => {
                        #[cfg(all(test, unix))]
                        assert!(
                            !hooks.panic_joiner.load(Ordering::Acquire),
                            "injected source joiner panic"
                        );
                        let joined = job.thread.take().expect("publisher handle").join();
                        job.guard.disarm();
                        if joined.is_err() {
                            job._progress.owner.poisoned.store(true, Ordering::Release);
                        }
                        // The output owns its own progress. No joiner-held owner
                        // or counter may survive delivery to the caller.
                        drop(job);
                        let result = joined
                            .map(|output| JoinedSource { output })
                            .map_err(|_panic| Error::Poisoned);
                        let _delivered = sender.send(result);
                    }
                    Command::NoPublisher(error, progress) => {
                        drop(progress);
                        let _delivered = sender.send(Err(error));
                    }
                }
            })
            .map_err(Error::Spawn)?;
        #[cfg(all(test, unix))]
        {
            let pause = self
                .source_hooks
                .admission
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .clone();
            if let Some(pause) = pause {
                pause.started.notify_one();
                pause.resume.notified().await;
            }
        }
        {
            let mut active = self
                .publications
                .active
                .lock()
                .map_err(|_poison| Error::Poisoned)?;
            prepared.check_publication_owner(self)?;
            *active += 1;
        }
        let progress = Progress::new(self.clone(), Publishing(self.publications.clone()));
        let mut guard = HandoffGuard::new(&progress);
        let (snapshot, continuation) = prepared.into_publication_parts();
        let task = PublisherTask {
            _panic: JoinerPanicGuard::new(self),
            snapshot: Some(snapshot),
            continuation: Some(continuation),
            #[cfg(all(test, unix))]
            hooks: self.source_hooks.clone(),
            progress: progress.clone(),
        };
        #[cfg(all(test, unix))]
        let forced_failure = self.source_hooks.fail_publisher.load(Ordering::Acquire);
        #[cfg(not(all(test, unix)))]
        let forced_failure = false;
        let spawned = if forced_failure {
            drop(task);
            Err(std::io::Error::other(
                "injected source publisher spawn failure",
            ))
        } else {
            std::thread::Builder::new()
                .name("lore-source-publish".into())
                .spawn(move || task.run())
        };
        match spawned {
            Ok(thread) => {
                let job = JoinJob {
                    guard,
                    thread: Some(thread),
                    _progress: progress,
                };
                #[cfg(all(test, unix))]
                assert!(
                    !self.source_hooks.panic_handoff.load(Ordering::Acquire),
                    "injected source handoff panic"
                );
                if let Err(failed) = commands.send(Command::Publisher(job)) {
                    // Armed drop poisons before detaching. The publisher task
                    // or output independently retains progress after resources.
                    drop(failed);
                    return Err(Error::Poisoned.into());
                }
            }
            Err(error) => {
                guard.disarm();
                let command = Command::NoPublisher(Error::Spawn(error), progress);
                if let Err(failed) = commands.send(command) {
                    if let Command::NoPublisher(error, progress) = failed.0 {
                        drop(progress);
                        return Err(error.into());
                    }
                    unreachable!("spawn failure sends no publisher");
                }
            }
        }
        receiver
            .await
            .map_err(|_closed| Error::Poisoned)?
            .map_err(Into::into)
    }
}

#[cfg(all(test, unix))]
#[derive(Default)]
pub(crate) struct Hooks {
    pub(crate) fail_joiner: AtomicBool,
    pub(crate) fail_publisher: AtomicBool,
    pub(crate) disconnect: AtomicBool,
    pub(crate) panic_handoff: AtomicBool,
    pub(crate) panic_joiner: AtomicBool,
    pub(crate) panic_publisher: AtomicBool,
    pub(crate) io: Mutex<Option<Arc<PublishPause>>>,
    pub(crate) exit: Mutex<Option<Arc<PublishPause>>>,
    pub(super) admission: Mutex<Option<Arc<AdmissionPause>>>,
}
#[cfg(all(test, unix))]
std::thread_local! { static EXIT: std::cell::RefCell<Option<Exit>> = const { std::cell::RefCell::new(None) }; }
#[cfg(all(test, unix))]
struct Exit(Arc<PublishPause>);
#[cfg(all(test, unix))]
impl Drop for Exit {
    fn drop(&mut self) {
        self.0.enter();
    }
}

#[cfg(all(test, unix))]
impl CheckpointOwner {
    pub(crate) fn test_source_hooks(&self) -> Arc<Hooks> {
        self.source_hooks.clone()
    }
    pub(crate) fn test_source_active(&self) -> usize {
        *self.publications.active.lock().unwrap()
    }
    pub(crate) fn test_source_active_probe(
        &self,
    ) -> impl Fn() -> usize + Send + Sync + 'static + use<> {
        let publications = self.publications.clone();
        move || *publications.active.lock().unwrap()
    }
    pub(crate) fn test_source_serial_locked(&self) -> bool {
        self.serial.try_lock().is_err()
    }
    pub(crate) fn test_source_pause(&self, exit: bool) -> Arc<PublishPause> {
        let pause = Arc::new(PublishPause {
            started: tokio::sync::Notify::new(),
            resumed: Mutex::new(false),
            wake: Condvar::new(),
        });
        if exit {
            *self.source_hooks.exit.lock().unwrap() = Some(pause.clone());
        } else {
            *self.source_hooks.io.lock().unwrap() = Some(pause.clone());
        }
        pause
    }
    pub(crate) fn test_source_pause_admission(&self) {
        *self.source_hooks.admission.lock().unwrap() = Some(Arc::new(AdmissionPause {
            started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        }));
    }
    pub(crate) async fn test_source_admission_started(&self) {
        let pause = self.source_hooks.admission.lock().unwrap().clone().unwrap();
        pause.started.notified().await;
    }
    pub(crate) fn test_source_resume_admission(&self) {
        self.source_hooks
            .admission
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .resume
            .notify_one();
    }
}

#[cfg(all(test, unix))]
impl SourceAcknowledgment {
    pub(crate) fn test_source(&self) -> &PublishedCheckpointImage {
        &self.source
    }
    pub(crate) fn test_intent(&self) -> &crate::transition_prepare::IntentSpec {
        self.continuation.test_intent()
    }
}
