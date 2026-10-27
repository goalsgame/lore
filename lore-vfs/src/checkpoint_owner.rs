// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! One actual recovery owner shared by a mutable mount and its provider.
//! Capture is cancellable before local publication. An admitted publisher owns
//! its snapshot, serialization guard, exact freeze pin and storage lease until
//! synchronous publication finishes, independently of the caller's runtime.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;

use crate::core;
use crate::linux_working::StoreFactory;
use crate::namespace::FrozenNamespace;
use crate::namespace::Namespace;
use crate::namespace::NamespaceError;
use crate::recovery::Recovery;
use crate::recovery::{self};

#[derive(Debug)]
pub enum Error {
    Core(core::Error),
    Namespace(NamespaceError),
    Recovery(recovery::Error),
    Closed,
    Poisoned,
    WrongNamespace,
    Spawn(std::io::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<NamespaceError> for Error {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}
impl From<recovery::Error> for Error {
    fn from(error: recovery::Error) -> Self {
        Self::Recovery(error)
    }
}
fn publication_error_requires_reopen(recovery: &Recovery, error: &recovery::Error) -> bool {
    matches!(
        error,
        recovery::Error::Uncertain
            | recovery::Error::Pending
            | recovery::Error::TransitionPending
            | recovery::Error::Corrupt
            | recovery::Error::Incompatible
            | recovery::Error::StaleSnapshot
            | recovery::Error::PrivateStorageRequired
    ) || recovery.requires_reopen()
}
struct Publications {
    active: Mutex<usize>,
    drained: Condvar,
}
struct Publishing(Arc<Publications>);
impl Drop for Publishing {
    fn drop(&mut self) {
        let mut active = self
            .0
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *active -= 1;
        self.0.drained.notify_all();
    }
}

mod publication_task;
mod source_publication;
#[cfg(unix)]
mod transition_retirement;
#[cfg(unix)]
mod transition_worker;
use publication_task::PublicationTask;
pub(crate) use source_publication::SourceAcknowledgment;
#[cfg(all(unix, feature = "test-util"))]
pub use transition_worker::test_hooks::TransitionTestPause;
#[cfg(all(unix, feature = "test-util"))]
pub use transition_worker::test_hooks::TransitionTestStep;

pub struct CheckpointOwner {
    namespace: Arc<Namespace>,
    // Retain the root/session lease even for an empty namespace with no stores.
    _factory: Arc<dyn StoreFactory>,
    recovery: Mutex<Recovery>,
    serial: Arc<AsyncMutex<()>>,
    publications: Arc<Publications>,
    max_data: usize,
    max_checkpoint_bytes: usize,
    lineage: Option<crate::publication_ownership::Lineage>,
    closed: watch::Sender<bool>,
    poisoned: AtomicBool,
    #[cfg(unix)]
    transition_retirement: Mutex<Option<transition_retirement::RetirementObligation>>,
    #[cfg(all(test, unix))]
    pause: Mutex<Option<Arc<PublishPause>>>,
    #[cfg(all(test, unix))]
    fail_spawn: AtomicBool,
    #[cfg(all(test, unix))]
    tail_pause: Mutex<Option<Arc<PublishPause>>>,
    #[cfg(all(test, unix))]
    tail_panic: AtomicBool,
    #[cfg(all(test, unix))]
    before_admission: Mutex<Option<Arc<AdmissionPause>>>,
    #[cfg(all(test, unix))]
    source_hooks: Arc<source_publication::Hooks>,
    // Terminal owner reservation drops after namespace, backing factory and recovery locks.
    #[cfg(all(unix, feature = "test-util"))]
    transition_hook: Mutex<Option<Arc<transition_worker::test_hooks::Hook>>>,
    retirement_guard: Mutex<Option<Arc<dyn Send + Sync>>>,
}
impl CheckpointOwner {
    pub(crate) async fn lock_source_preparation(
        self: &Arc<Self>,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        self.available()?;
        let mut closed = self.closed.subscribe();
        self.available()?;
        let serial = tokio::select! { biased;
            _=closed.changed()=>return Err(Error::Closed),
            serial=self.serial.clone().lock_owned()=>serial,
        };
        self.available()?;
        Ok(serial)
    }
    pub(crate) fn capture_byte_limit(&self) -> usize {
        self.max_checkpoint_bytes
    }

    pub(crate) fn new_owned(
        namespace: Arc<Namespace>,
        factory: Arc<dyn StoreFactory>,
        recovery: Recovery,
        max_data: usize,
        lineage: crate::publication_ownership::Lineage,
    ) -> Arc<Self> {
        Self::build(namespace, factory, recovery, max_data, Some(lineage))
    }
    // Standalone publisher fixtures do not establish Model publication ownership.
    #[cfg(test)]
    pub(crate) fn new(
        namespace: Arc<Namespace>,
        factory: Arc<dyn StoreFactory>,
        recovery: Recovery,
        max_data: usize,
    ) -> Arc<Self> {
        Self::build(namespace, factory, recovery, max_data, None)
    }
    fn build(
        namespace: Arc<Namespace>,
        factory: Arc<dyn StoreFactory>,
        recovery: Recovery,
        max_data: usize,
        lineage: Option<crate::publication_ownership::Lineage>,
    ) -> Arc<Self> {
        let poisoned = recovery.requires_reopen();
        let max_checkpoint_bytes = recovery.capture_byte_limit();
        Arc::new(Self {
            namespace,
            _factory: factory,
            recovery: Mutex::new(recovery),
            serial: Arc::new(AsyncMutex::new(())),
            publications: Arc::new(Publications {
                active: Mutex::new(0),
                drained: Condvar::new(),
            }),
            max_data,
            max_checkpoint_bytes,
            lineage,
            closed: watch::channel(false).0,
            poisoned: AtomicBool::new(poisoned),
            #[cfg(unix)]
            transition_retirement: Mutex::new(None),
            #[cfg(all(test, unix))]
            pause: Mutex::new(None),
            #[cfg(all(test, unix))]
            fail_spawn: AtomicBool::new(false),
            #[cfg(all(test, unix))]
            tail_pause: Mutex::new(None),
            #[cfg(all(test, unix))]
            tail_panic: AtomicBool::new(false),
            #[cfg(all(test, unix))]
            before_admission: Mutex::new(None),
            #[cfg(all(test, unix))]
            source_hooks: Arc::new(source_publication::Hooks::default()),
            #[cfg(all(unix, feature = "test-util"))]
            transition_hook: Mutex::new(None),
            retirement_guard: Mutex::new(None),
        })
    }
    /// Retain a terminal external reservation through all checkpoint capabilities.
    /// The caller must first withdraw external provider publication; installing a
    /// live registration here could create a registry/provider ownership cycle.
    /// Repeating the same allocation is allowed; replacing a guard is rejected.
    pub fn retain_retirement_guard(&self, guard: Arc<dyn Send + Sync>) -> Result<()> {
        let mut retained = self
            .retirement_guard
            .lock()
            .map_err(|_poison| Error::Poisoned)?;
        if let Some(existing) = retained.as_ref() {
            if Arc::ptr_eq(existing, &guard) {
                return Ok(());
            }
            return Err(Error::Core(core::Error::Busy));
        }
        *retained = Some(guard);
        Ok(())
    }
    pub fn namespace(&self) -> &Arc<Namespace> {
        &self.namespace
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }
    pub(crate) fn check_publication_owner(
        &self,
        owner: &crate::publication_ownership::OwnerClaim,
    ) -> Result<()> {
        owner
            .check_lineage(self.lineage.as_ref())
            .map_err(Error::Core)
    }
    pub(crate) fn available(&self) -> Result<()> {
        if self.is_poisoned() {
            Err(Error::Poisoned)
        } else if *self.closed.borrow() {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn close(&self) {
        let _admission = self
            .publications
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.closed.send_replace(true);
    }
    pub(crate) async fn publish(self: &Arc<Self>) -> Result<()> {
        self.capture(None).await
    }
    pub(crate) async fn publish_frozen(self: &Arc<Self>, lease: FrozenNamespace) -> Result<()> {
        if !Arc::ptr_eq(lease.namespace(), &self.namespace) {
            return Err(Error::WrongNamespace);
        }
        self.capture(Some(lease)).await
    }
    async fn capture(self: &Arc<Self>, lease: Option<FrozenNamespace>) -> Result<()> {
        self.available()?;
        let mut closed = self.closed.subscribe();
        self.available()?;
        let capture = async {
            let serial = self.serial.clone().lock_owned().await;
            self.available()?;
            if lease.as_ref().is_some_and(|lease| !lease.valid()) {
                return Err(Error::Core(core::Error::Stale));
            }
            let snapshot = self.namespace.snapshot(self.max_data).await?;
            self.available()?;
            if lease.as_ref().is_some_and(|lease| !lease.valid()) {
                return Err(Error::Core(core::Error::Stale));
            }
            Ok::<_, Error>((serial, snapshot))
        };
        let (serial, snapshot) = tokio::select! {biased;_=closed.changed()=>return Err(Error::Closed),result=capture=>result?};
        #[cfg(all(test, unix))]
        {
            let pause = self
                .before_admission
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .clone();
            if let Some(pause) = pause {
                pause.started.notify_one();
                pause.resume.notified().await;
            }
        }
        #[cfg(all(test, unix))]
        let pause = self
            .pause
            .lock()
            .map_err(|_poison| Error::Poisoned)?
            .clone();
        #[cfg(all(test, unix))]
        let tail = publication_task::Tail {
            pause: self
                .tail_pause
                .lock()
                .map_err(|_poison| Error::Poisoned)?
                .clone(),
            panic: self.tail_panic.load(Ordering::Acquire),
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // No await between admission and transfer into one ordered task payload.
        // A failed spawn drops that payload before releasing completion.
        {
            let mut active = self
                .publications
                .active
                .lock()
                .map_err(|_poison| Error::Poisoned)?;
            self.available()?;
            *active += 1;
        }
        let task = PublicationTask::new(
            snapshot,
            lease,
            serial,
            self.clone(),
            Publishing(self.publications.clone()),
            sender,
        );
        #[cfg(all(test, unix))]
        let task = task.observe(pause, tail);
        let task = move || task.run();
        #[cfg(all(test, unix))]
        if self.fail_spawn.load(Ordering::Acquire) {
            drop(task);
            return Err(Error::Spawn(std::io::Error::other(
                "injected publisher spawn failure",
            )));
        }
        std::thread::Builder::new()
            .name("lore-working-checkpoint".into())
            .spawn(task)
            .map_err(Error::Spawn)?;
        receiver
            .await
            .map_err(|_closed| Error::Core(core::Error::Io))?
    }
    #[cfg(all(test, unix))]
    pub(crate) fn test_pause(&self) -> Arc<PublishPause> {
        let pause = Arc::new(PublishPause {
            started: tokio::sync::Notify::new(),
            resumed: Mutex::new(false),
            wake: Condvar::new(),
        });
        *self.pause.lock().unwrap() = Some(pause.clone());
        pause
    }
    #[cfg(all(test, unix))]
    pub(crate) fn test_fault(&self, boundary: Option<crate::recovery::Boundary>) {
        self.recovery.lock().unwrap().test_fault(boundary);
    }
    #[cfg(all(test, unix))]
    pub(crate) fn test_snapshot(
        &self,
    ) -> recovery::Result<Option<crate::namespace::NamespaceSnapshot>> {
        self.recovery
            .lock()
            .map_err(|_poison| std::io::Error::other("checkpoint owner poisoned"))?
            .load()
    }
    /// Waits for admitted publication IO and ordered capture destruction, not OS
    /// thread joining. Call outside a runtime
    /// worker, after cancelling capture/request futures. No remote IO is awaited.
    pub fn drain(&self) -> Result<()> {
        let mut poisoned = false;
        let mut active = match self.publications.active.lock() {
            Ok(active) => active,
            Err(error) => {
                poisoned = true;
                error.into_inner()
            }
        };
        while *active != 0 {
            active = match self.publications.drained.wait(active) {
                Ok(active) => active,
                Err(error) => {
                    poisoned = true;
                    error.into_inner()
                }
            };
        }
        if poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    pub(crate) async fn drain_async(self: &Arc<Self>) -> Result<()> {
        let owner = self.clone();
        lore_base::lore_spawn_blocking_nocontext!(move || owner.drain())
            .await
            .map_err(|_join| Error::Core(core::Error::Io))?
    }
}

#[cfg(all(test, unix))]
struct AdmissionPause {
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[cfg(all(test, unix))]
pub(crate) struct PublishPause {
    pub(crate) started: tokio::sync::Notify,
    resumed: Mutex<bool>,
    wake: Condvar,
}
#[cfg(all(test, unix))]
impl PublishPause {
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
#[cfg(all(test, unix))]
mod tests;
