// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! External owned transition: intent, anchors, installation and durable completion.

use lore_revision::fs::filesystem_provider::FsError;
use lore_revision::fs::filesystem_provider::RootHoldRequest;
use lore_revision::repository::WorkingCommandHandoff;
use source_publication::HandoffGuard;
use source_publication::JoinerPanicGuard;
use source_publication::Progress;

use super::transition_retirement::RetirementProgress;
use super::*;
use crate::publication_ownership::successor::SuccessorReservation;
use crate::recovery::intent::CheckpointIdentity;
use crate::recovery::intent::Intent;

#[cfg(feature = "test-util")]
pub(super) mod test_hooks;
#[cfg(feature = "test-util")]
use test_hooks::TransitionTestStep;

type Outcome = std::result::Result<(), FsError>;
struct Finished(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for Finished {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _delivered = sender.send(());
        }
    }
}
struct Resources {
    ack: tokio::sync::Mutex<SourceAcknowledgment>,
    handoff: Arc<WorkingCommandHandoff>,
    reservation: SuccessorReservation,
    record: Intent,
    progress: Arc<Progress>,
    // Final field fires after actual retained resources and this progress pin.
    _finished: Finished,
}
struct Task {
    retirement: Arc<RetirementProgress>,
    resources: Arc<Resources>,
    completion: tokio::sync::oneshot::Receiver<()>,
    execution: Arc<lore_revision::interface::ExecutionContext>,
}
struct JoinJob {
    retirement: Arc<RetirementProgress>,
    guard: TransitionJoinGuard,
    thread: Option<std::thread::JoinHandle<Outcome>>,
    progress: Arc<Progress>,
}
struct TransitionJoinGuard {
    checkpoint: HandoffGuard,
    handoff: Arc<WorkingCommandHandoff>,
    armed: bool,
}
impl TransitionJoinGuard {
    fn new(progress: &Arc<Progress>, handoff: Arc<WorkingCommandHandoff>) -> Self {
        Self {
            checkpoint: HandoffGuard::new(progress),
            handoff,
            armed: true,
        }
    }
    fn disarm(&mut self) {
        self.armed = false;
        self.checkpoint.disarm();
    }
}
impl Drop for TransitionJoinGuard {
    fn drop(&mut self) {
        if self.armed && self.handoff.quarantine().is_err() {
            let _retained = Arc::into_raw(self.handoff.clone());
        }
    }
}
enum Command {
    Publisher(JoinJob),
    NoPublisher(std::io::Error, Arc<Progress>),
}
impl Task {
    fn run(self) -> Outcome {
        let Task {
            retirement,
            resources,
            completion,
            execution,
        } = self;
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => return Err(problem(error)),
        };
        let owner = resources.progress.owner().clone();
        let handoff = resources.handoff.clone();
        let mut intent_started = false;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let io = lore_io::lease::IoLease::new(resources.clone());
            let result = runtime.block_on(lore_revision::runtime::LORE_CONTEXT.scope(
                execution.clone(),
                io.scope(resources.execute(&mut intent_started)),
            ));
            drop(io);
            result
        }));
        let result = result.unwrap_or_else(|_panic| {
            Err(FsError::internal("Working transition publisher panicked"))
        });
        if result.is_err() && intent_started {
            resources
                .progress
                .owner()
                .poisoned
                .store(true, Ordering::Release);
            if resources.handoff.quarantine().is_err() {
                let _retained = Arc::into_raw(resources);
                drop(runtime);
                return Err(FsError::internal(
                    "Working transition quarantine requires process recovery",
                ));
            }
        }
        // Drop initiating pins before waiting for propagated actual IO owners.
        // The ACK stays intact in Resources until the final such owner drops.
        drop(resources);
        let drained = runtime.block_on(completion);
        drop(runtime);
        if drained.is_ok() && intent_started {
            retirement.resources_finished(result.is_ok());
        }
        #[cfg(feature = "test-util")]
        if let Err(error) = owner.hit_transition_test(TransitionTestStep::BeforeThreadExit) {
            owner.poisoned.store(true, Ordering::Release);
            if handoff.quarantine().is_err() {
                let _retained = Arc::into_raw(handoff);
            }
            return Err(error);
        }
        #[cfg(not(feature = "test-util"))]
        {
            let _owner = owner;
            let _handoff = handoff;
        }
        drained
            .map_err(|_closed| FsError::internal("Working transition resource completion lost"))?;
        result
    }
}
impl Resources {
    async fn execute(&self, intent_started: &mut bool) -> Outcome {
        // Private retention mutex: propagated IO owns Resources but never locks
        // this ACK. No other code can reenter it, including recovery readers.
        let mut ack = self.ack.lock().await;
        ack.validate_for_transition().await.map_err(debug_problem)?;
        if crate::recovery::intent::Anchors::from(
            self.handoff.read_anchors().await.map_err(problem)?,
        ) != self.record.old
        {
            return Err(FsError::internal(
                "Working source anchors changed before intent",
            ));
        }
        let owner = self.progress.owner().clone();
        *intent_started = true;
        let receipt = {
            let (source, _facts, candidate, memory) = ack.artifact_inputs().into_parts();
            let mut recovery = owner
                .recovery
                .lock()
                .map_err(|_poison| FsError::internal("Working recovery lock poisoned"))?;
            recovery
                .publish_transition(source, &candidate, &self.record, memory)
                .map_err(debug_problem)?
        };
        #[cfg(feature = "test-util")]
        owner.hit_transition_test(TransitionTestStep::AfterIntent)?;
        ack.validate_for_transition().await.map_err(debug_problem)?;
        self.handoff
            .publish_anchors(&receipt)
            .await
            .map_err(problem)?;
        #[cfg(feature = "test-util")]
        owner.hit_transition_test(TransitionTestStep::AfterAnchors)?;
        ack.validate_for_transition().await.map_err(debug_problem)?;
        ack.install_in_place(&self.reservation)
            .await
            .map_err(debug_problem)?;
        let completion = receipt
            .confirm_completion(&self.handoff, owner.max_checkpoint_bytes)
            .await
            .map_err(debug_problem)?;
        {
            let (_source, _facts, candidate, memory) = ack.artifact_inputs().into_parts();
            let mut recovery = owner
                .recovery
                .lock()
                .map_err(|_poison| FsError::internal("Working recovery lock poisoned"))?;
            recovery
                .complete_transition(&completion, &candidate, memory)
                .map_err(debug_problem)?;
        }
        Ok(())
    }
}
impl CheckpointOwner {
    pub(crate) async fn publish_root_transition(
        self: &Arc<Self>,
        mut ack: SourceAcknowledgment,
        request: RootHoldRequest,
        reservation: SuccessorReservation,
        execution: Arc<lore_revision::interface::ExecutionContext>,
    ) -> Outcome {
        ack.check_transition_owner(self).map_err(debug_problem)?;
        ack.validate_for_transition().await.map_err(debug_problem)?;
        let old = request.old_anchors();
        let new = request.new_anchors();
        let target = request.target();
        let handoff = request.handoff().clone();
        if request.options().dry_run {
            return Err(FsError::internal("Dry-run transition cannot publish"));
        }
        drop(request);
        let record = {
            let (source, facts, candidate, _memory) = ack.artifact_inputs().into_parts();
            let spec = facts.intent();
            if spec.scope() != spec.original_root()
                || spec.scope().repository != old.repository
                || spec.target().revision.hash != target
                || target != new.current
                || old.repository != new.repository
                || old.instance != new.instance
            {
                return Err(FsError::internal(
                    "Working command exposure does not match prepared root",
                ));
            }
            let mut transaction = [0; 32];
            ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut transaction)
                .map_err(|_random| FsError::internal("Working transaction identity unavailable"))?;
            let record = Intent {
                transaction,
                original_root: spec.original_root(),
                scope: spec.scope(),
                source: source.identity(),
                candidate: CheckpointIdentity::from_bytes(
                    candidate.bytes(),
                    self.max_checkpoint_bytes,
                )
                .map_err(debug_problem)?,
                old: old.into(),
                new: new.into(),
            };
            record
                .encode(self.max_checkpoint_bytes)
                .map_err(debug_problem)?;
            record
        };
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let panic = JoinerPanicGuard::new(self);
        // Prestart the joiner before admitting/spawning a resource-owning worker.
        std::thread::Builder::new()
            .name("lore-transition-join".into())
            .spawn(move || {
                let _panic = panic;
                if let Ok(command) = receive.recv() {
                    let result = match command {
                        Command::Publisher(mut job) => {
                            let result =
                                job.thread.take().expect("transition worker handle").join();
                            let failed = !matches!(&result, Ok(Ok(())));
                            if failed {
                                let owner = job.progress.owner();
                                if owner
                                    .certify_transition_retirement(
                                        &job.retirement,
                                        &job.guard.handoff,
                                    )
                                    .is_err()
                                {
                                    let _retained = Arc::into_raw(job.guard.handoff.clone());
                                }
                            }
                            if result.is_ok() {
                                job.guard.disarm();
                            } else {
                                job.progress.owner().poisoned.store(true, Ordering::Release);
                                // Keep the exact withdrawal guard armed through
                                // publisher destructor/TLS join failure.
                            }
                            drop(job);
                            result.unwrap_or_else(|_panic| {
                                Err(FsError::internal("Working transition worker join failed"))
                            })
                        }
                        Command::NoPublisher(error, progress) => {
                            drop(progress);
                            Err(problem(error))
                        }
                    };
                    let _delivered = sender.send(result);
                }
            })
            .map_err(problem)?;
        {
            let mut active =
                self.publications.active.lock().map_err(|_poison| {
                    FsError::internal("Working transition accounting poisoned")
                })?;
            self.available().map_err(debug_problem)?;
            *active = active
                .checked_add(1)
                .ok_or_else(|| FsError::internal("Working publisher count overflow"))?;
        }
        let progress = Progress::new(self.clone(), Publishing(self.publications.clone()));
        let mut guard = TransitionJoinGuard::new(&progress, handoff.clone());
        let (finished, completion) = tokio::sync::oneshot::channel();
        let resources = Arc::new(Resources {
            ack: tokio::sync::Mutex::new(ack),
            handoff,
            reservation,
            record,
            progress: progress.clone(),
            _finished: Finished(Some(finished)),
        });
        let retirement = Arc::new(RetirementProgress::default());
        let task = Task {
            retirement: retirement.clone(),
            resources,
            completion,
            execution,
        };
        match std::thread::Builder::new()
            .name("lore-transition-publish".into())
            .spawn(move || {
                #[cfg(feature = "test-util")]
                let tail = test_hooks::PublisherTail::new(task.resources.progress.owner().clone());
                let result = task.run();
                #[cfg(feature = "test-util")]
                drop(tail);
                result
            }) {
            Ok(thread) => {
                let job = JoinJob {
                    retirement,
                    guard,
                    thread: Some(thread),
                    progress,
                };
                if let Err(failed) = send.send(Command::Publisher(job)) {
                    drop(failed); // armed guard poisons before the handle detaches
                    return Err(FsError::internal(
                        "Working transition joiner handoff failed",
                    ));
                }
            }
            Err(error) => {
                guard.disarm();
                if let Err(failed) = send.send(Command::NoPublisher(error, progress)) {
                    drop(failed);
                    return Err(FsError::internal("Working transition spawn failed"));
                }
            }
        }
        receiver
            .await
            .map_err(|_closed| FsError::internal("Working transition completion lost"))?
    }
}
fn problem(error: impl std::fmt::Display) -> FsError {
    FsError::internal(format!("Working root transition: {error}"))
}
fn debug_problem(error: impl std::fmt::Debug) -> FsError {
    FsError::internal(format!("Working root transition: {error:?}"))
}
