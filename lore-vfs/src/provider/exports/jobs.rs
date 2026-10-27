// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::sync::Mutex;

use lore_revision::fs::filesystem_provider::FsError;

const MAX_JOBS: usize = 64;
const MAX_DIRECTORIES: usize = 64;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Directory,
    Create,
    Write,
    Cache,
    Cleanup,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Directory => "lore-export-directory",
            Self::Create => "lore-export-create",
            Self::Write => "lore-export-write",
            Self::Cache => "lore-export-cache",
            Self::Cleanup => "lore-export-cleanup",
        }
    }
}
struct State {
    jobs: usize,
    directories: usize,
    failed: bool,
}
pub(super) struct Jobs {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
    #[cfg(test)]
    pause: Mutex<Option<Arc<Pause>>>,
    #[cfg(test)]
    fail_cleanup_spawn: std::sync::atomic::AtomicBool,
}
impl Jobs {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                jobs: 0,
                directories: 0,
                failed: false,
            }),
            changed: tokio::sync::Notify::new(),
            #[cfg(test)]
            pause: Mutex::new(None),
            #[cfg(test)]
            fail_cleanup_spawn: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub(super) fn directory(self: &Arc<Self>) -> Result<Directory, FsError> {
        let mut state = self.state.lock().map_err(|_poison| failed())?;
        if state.failed {
            return Err(failed());
        }
        if state.directories >= MAX_DIRECTORIES {
            return Err(FsError::internal("retained export directory limit"));
        }
        state.directories += 1;
        Ok(Directory(self.clone()))
    }
    fn admit(self: &Arc<Self>, cleanup: bool) -> Result<Job, FsError> {
        let mut state = self.state.lock().map_err(|_poison| failed())?;
        if !cleanup {
            if state.failed {
                return Err(failed());
            }
            if state.jobs >= MAX_JOBS {
                return Err(FsError::internal("export IO job limit"));
            }
        }
        // Each cleanup retains one of MAX_DIRECTORIES leases; normal work is
        // capped separately. Even abandoned callers use at most 128 threads.
        state.jobs += 1;
        Ok(Job(self.clone()))
    }
    pub(super) async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        kind: Kind,
        work: impl FnOnce() -> Result<T, FsError> + Send + 'static,
    ) -> Result<T, FsError> {
        let job = self.admit(false)?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        #[cfg(test)]
        let pause = self
            .pause
            .lock()
            .unwrap()
            .clone()
            .filter(|pause| pause.kind.is_none_or(|target| target == kind));
        std::thread::Builder::new()
            .name(kind.name().into())
            .spawn(move || {
                #[cfg(test)]
                if let Some(pause) = pause {
                    pause.enter();
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
                if result.is_err() {
                    job.0.fail();
                }
                let result = result
                    .unwrap_or_else(|_panic| Err(FsError::internal("export IO job panicked")));
                drop(job);
                let _ = sender.send(result);
            })
            .map_err(|error| FsError::internal(format!("export IO thread: {error}")))?;
        receiver
            .await
            .map_err(|_closed| FsError::internal("export IO owner stopped"))?
    }
    pub(super) fn cleanup(
        self: &Arc<Self>,
        directory: Directory,
        work: impl FnOnce() -> Result<(), FsError> + Send + 'static,
    ) {
        let job = match self.admit(true) {
            Ok(job) => job,
            Err(_error) => {
                self.fail();
                return;
            }
        };
        let cleanup = CleanupJob {
            work: Some(work),
            directory: Some(directory),
            job: Some(job),
            started: false,
        };
        #[cfg(test)]
        if self
            .fail_cleanup_spawn
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            drop(cleanup);
            return;
        }
        #[cfg(test)]
        let pause = self
            .pause
            .lock()
            .unwrap()
            .clone()
            .filter(|pause| pause.kind.is_none_or(|target| target == Kind::Cleanup));
        let result = std::thread::Builder::new()
            .name(Kind::Cleanup.name().into())
            .spawn(move || {
                #[cfg(test)]
                if let Some(pause) = pause {
                    pause.enter();
                }
                cleanup.run();
            });
        drop(result);
    }
    fn fail(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .failed = true;
        self.changed.notify_waiters();
    }
    pub(super) async fn drain(&self) -> Result<(), FsError> {
        loop {
            let changed = self.changed.notified();
            let outcome = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                (state.jobs == 0 && state.directories == 0)
                    .then_some(state.failed || self.state.is_poisoned())
            };
            if let Some(failure) = outcome {
                return if failure { Err(failed()) } else { Ok(()) };
            }
            changed.await;
        }
    }
    #[cfg(test)]
    pub(super) fn pause(&self, kind: Option<Kind>) -> Arc<Pause> {
        let pause = Arc::new(Pause {
            started: tokio::sync::Notify::new(),
            resumed: Mutex::new(false),
            wake: std::sync::Condvar::new(),
            kind,
        });
        *self.pause.lock().unwrap() = Some(pause.clone());
        pause
    }
    #[cfg(test)]
    pub(super) fn fail_cleanup_spawn(&self) {
        self.fail_cleanup_spawn
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

struct CleanupJob<F: FnOnce() -> Result<(), FsError>> {
    work: Option<F>,
    directory: Option<Directory>,
    job: Option<Job>,
    started: bool,
}
impl<F: FnOnce() -> Result<(), FsError>> CleanupJob<F> {
    fn run(mut self) {
        self.started = true;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            self.work.take().expect("owned cleanup job"),
        ));
        if !matches!(result, Ok(Ok(()))) {
            self.job.as_ref().expect("cleanup accounting").0.fail();
        }
    }
}
impl<F: FnOnce() -> Result<(), FsError>> Drop for CleanupJob<F> {
    fn drop(&mut self) {
        // A failed thread spawn destroys this closure before returning Err.
        // Publish failure before dropping either count, so concurrent drains
        // cannot mistake retained, uncleaned storage for successful quiescence.
        if !self.started {
            self.job.as_ref().expect("cleanup accounting").0.fail();
        }
        drop(self.work.take());
        drop(self.directory.take());
        drop(self.job.take());
    }
}

#[cfg(test)]
pub(crate) struct Pause {
    pub(crate) started: tokio::sync::Notify,
    resumed: Mutex<bool>,
    wake: std::sync::Condvar,
    kind: Option<Kind>,
}
#[cfg(test)]
impl Pause {
    fn enter(&self) {
        self.started.notify_one();
        let mut resumed = self.resumed.lock().unwrap();
        while !*resumed {
            resumed = self.wake.wait(resumed).unwrap();
        }
    }
    pub(crate) fn resume(&self) {
        *self.resumed.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
fn failed() -> FsError {
    FsError::internal("export IO or cleanup failed; retained storage requires inspection")
}
pub(super) struct Directory(Arc<Jobs>);
impl Drop for Directory {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .directories -= 1;
        self.0.changed.notify_waiters();
    }
}
struct Job(Arc<Jobs>);
impl Drop for Job {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .jobs -= 1;
        self.0.changed.notify_waiters();
    }
}
