// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use lore_revision::repository::working_bootstrap::PendingPublication;

use super::Error;
use super::InstanceHandle;
use super::Phase;
use super::PreparationError;
use super::PreparedInstance;
use super::Record;
use super::Result;
use super::retire;

enum Ready {
    Legacy(tokio::sync::oneshot::Sender<Result<()>>),
    Bootstrap(tokio::sync::oneshot::Sender<Result<bool>>),
}

#[cfg(feature = "test-util")]
pub(super) struct PublicationPause {
    pub entered: tokio::sync::oneshot::Sender<()>,
    pub release: std::sync::mpsc::Receiver<()>,
}
pub(super) struct MountWorker {
    done: tokio::sync::watch::Sender<Option<Result<()>>>,
    thread: super::worker::ThreadOwner,
}
impl MountWorker {
    pub async fn drain(&self) -> Result<()> {
        let mut done = self.done.subscribe();
        loop {
            let completed = done.borrow_and_update().clone();
            if let Some(result) = completed {
                result?;
                return self.thread.drain().await;
            }
            tokio::select! {
                changed = done.changed() => changed.map_err(|_closed| Error::Closed)?,
                joined = self.thread.drain() => {
                    joined?;
                    return self.done.borrow().clone().unwrap_or_else(|| {
                        Err(Error::Owner("mount worker exited without completion".into()))
                    });
                }
            }
        }
    }
}
impl PreparedInstance {
    /// Publishes only after kernel initialization and mount identity capture.
    /// The session stays on its dedicated thread through synchronous teardown.
    pub async fn mount(
        self,
        uid: u32,
        gid: u32,
    ) -> std::result::Result<InstanceHandle, PreparationError> {
        let ticket = self.startup;
        let handle = ticket.handle();
        let failure_handle = handle.clone();
        let mut cancelled = super::CancelPreparation {
            supervisor: ticket.supervisor.clone(),
            handle: handle.clone(),
            armed: true,
        };
        let worker = Arc::new(MountWorker {
            done: tokio::sync::watch::channel(None).0,
            thread: super::worker::ThreadOwner::new(),
        });
        *ticket
            .record
            .mount
            .lock()
            .map_err(|_poison| PreparationError::Retiring {
                handle: handle.clone(),
                error: Error::Poisoned,
            })? = Some(worker.clone());
        let failed_worker = worker.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("lore-working-session".into())
            .spawn(move || {
                let record = ticket.record.clone();
                let mut ticket = Some(ticket);
                let mut ready = Some(Ready::Legacy(ready_tx));
                let mut attempted = false;
                let result = run(
                    &record,
                    &mut ticket,
                    uid,
                    gid,
                    &mut ready,
                    &mut attempted,
                    None,
                );
                if let Err(error) = &result {
                    notify_ready(&mut ready, Err(error.clone()));
                }
                drop(ticket);
                if let Err(error) = &result {
                    let _withdrawn = retire(&record, &error.to_string());
                    let mut state = record
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.error = Some(error.to_string());
                }
                worker
                    .done
                    .send_replace(Some(if attempted { result } else { Ok(()) }));
            });
        match spawned {
            Ok(thread) => failed_worker.thread.publish(Some(thread)),
            Err(error) => {
                failed_worker.thread.publish(None);
                failed_worker.done.send_replace(Some(Ok(())));
                return Err(PreparationError::Retiring {
                    handle: failure_handle,
                    error: Error::Owner(error.to_string()),
                });
            }
        }
        let result = ready_rx.await.unwrap_or_else(|_closed| {
            Err(Error::Owner(
                "mount owner terminated before readiness".into(),
            ))
        });
        cancelled.armed = false;
        result
            .map(|()| handle)
            .map_err(|error| PreparationError::Retiring {
                handle: failure_handle,
                error,
            })
    }
}
pub(super) fn run_bootstrap(
    record: &Arc<Record>,
    ticket: &mut Option<super::StartupReservation>,
    uid: u32,
    gid: u32,
    ready: &mut Option<tokio::sync::oneshot::Sender<Result<bool>>>,
    publication: &mut PendingPublication,
) -> Result<()> {
    let mut signal = ready.take().map(Ready::Bootstrap);
    let mut attempted = false;
    let result = run(
        record,
        ticket,
        uid,
        gid,
        &mut signal,
        &mut attempted,
        Some(publication),
    );
    if let Err(error) = &result {
        notify_ready(&mut signal, Err(error.clone()));
    }
    result
}
fn run(
    record: &Arc<Record>,
    ticket: &mut Option<super::StartupReservation>,
    uid: u32,
    gid: u32,
    ready: &mut Option<Ready>,
    attempted: &mut bool,
    publication: Option<&mut PendingPublication>,
) -> Result<()> {
    let owner = record
        .owner
        .lock()
        .map_err(|_poison| Error::Poisoned)?
        .clone()
        .ok_or(Error::InvalidBinding)?;
    let runtime = owner.runtime_handle()?;
    let target = record.paths.target();
    if !mount_ids(target)?.is_empty() {
        return Err(Error::Owner("mount target is already mounted".into()));
    }
    ticket.as_ref().ok_or(Error::Closed)?.check_live()?;
    owner._target.revalidate(record)?;
    super::cache::revalidate(record)?;
    *attempted = true;
    let mut mounted =
        lore_vfs::linux_working::mount(owner.model.clone(), runtime.clone(), target, uid, gid)
            .map_err(|error| Error::Owner(error.to_string()))?;
    // Capture before waiting for INIT so failed initialization still has an
    // exact identity whose disappearance can be checked during cleanup.
    let identities = match mount_ids(target) {
        Ok(ids) => ids,
        Err(error) => {
            notify_ready(ready, Err(error.clone()));
            retain_uncertain(record, ticket, Some(mounted), error);
        }
    };
    let [identity] = identities.as_slice() else {
        let error = Error::Owner("cannot identify the newly created mount".into());
        notify_ready(ready, Err(error.clone()));
        retain_uncertain(record, ticket, Some(mounted), error);
    };
    let identity = *identity;
    let Some(mut ending) = mounted.take_session_end() else {
        let error = Error::Owner("missing kernel termination signal".into());
        notify_ready(ready, Err(error.clone()));
        retain_uncertain(record, ticket, Some(mounted), error);
    };
    let Some(initialized) = mounted.take_readiness() else {
        let error = Error::Owner("missing kernel initialization signal".into());
        notify_ready(ready, Err(error.clone()));
        retain_uncertain(record, ticket, Some(mounted), error);
    };
    let mut cancel = record.cancel.subscribe();
    let initialized=runtime.block_on(async {
        tokio::select! {
            biased;
            _=cancelled(&mut cancel)=>Err(Error::Closed),
            _=&mut ending=>Err(Error::Owner("mount ended during initialization".into())),
            result=tokio::time::timeout(Duration::from_secs(10),initialized)=>result.map_err(|_timeout|Error::Owner("mount initialization timed out".into()))?.map_err(|_closed|Error::Closed)?.map_err(|error|Error::Owner(error.to_string())),
        }
    });
    #[cfg(feature = "test-util")]
    if initialized.is_ok() {
        let pause = record
            .publication_pause
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(pause) = pause {
            let _sent = pause.entered.send(());
            let _released = pause.release.recv();
        }
    }
    // These slots outlive the Record guard even if later caller code unwinds.
    let mut cleanup = None;
    let mut settled_ticket = None;
    let provider: Arc<dyn lore_revision::fs::filesystem_provider::FilesystemProvider> =
        owner.provider.clone();
    let published = initialized.and_then(|()| {
        let mut prepared = match publication {
            Some(permit) => Some(
                permit
                    .prepare(&provider)
                    .map_err(|error| Error::Registry(error.to_string()))?,
            ),
            None => None,
        };
        if let Some(prepared) = prepared.as_mut() {
            prepared
                .release_client()
                .map_err(|error| Error::Registry(error.to_string()))?;
        }
        let mut state = record.state.lock().map_err(|_poison| Error::Poisoned)?;
        if state.phase != Phase::Starting {
            return Err(Error::Closed);
        }
        if let Some(prepared) = prepared.as_mut() {
            cleanup = Some(
                prepared
                    .commit()
                    .map_err(|error| Error::Registry(error.to_string()))?,
            );
        } else {
            record
                .input()?
                .prepared
                .registration
                .publish(owner.provider.clone())
                .map_err(|error| Error::Registry(error.to_string()))?;
        }
        state.phase = Phase::Ready;
        state.startup_active = false;
        if let Some(mut owned) = ticket.take() {
            owned.active = false;
            settled_ticket = Some(owned);
        }
        Ok(())
    });
    if published.is_ok() {
        record.startup_settled.send_replace(true);
    }
    drop(settled_ticket);
    drop(cleanup);
    let successful = published.is_ok();
    notify_ready(ready, published.clone());
    if successful {
        runtime.block_on(async {
            tokio::select! { _=cancelled(&mut cancel)=>{}, _=&mut ending=>{} }
        });
    }
    let _withdrawn = retire(record, "kernel session stopping");
    let table = match mount_table() {
        Ok(table) => table,
        Err(error) => retain_uncertain(record, ticket, Some(mounted), error),
    };
    if !safe_target(&table, target, identity) {
        retain_uncertain(
            record,
            ticket,
            Some(mounted),
            Error::Owner("owned mount moved or a replacement obscures its target".into()),
        );
    }
    // fuser uses a conventional pathname-based unmount. The identity checks
    // detect observed replacement/stacking, but cannot exclude a concurrent
    // same-user external mount between this check and the unmount syscall.
    let unmounted = mounted
        .unmount()
        .map_err(|error| Error::Owner(error.to_string()));
    let table = match mount_table() {
        Ok(table) => table,
        Err(error) => {
            notify_ready(ready, Err(error.clone()));
            retain_uncertain(record, ticket, None, error);
        }
    };
    if table.iter().any(|(id, _)| *id == identity) {
        let error = Error::Owner("owned mount remains after session join".into());
        notify_ready(ready, Err(error.clone()));
        retain_uncertain(record, ticket, None, error);
    }
    if let Err(error) = unmounted {
        notify_ready(ready, Err(error.clone()));
        retain_uncertain(record, ticket, None, error);
    }
    Ok(())
}
fn notify_ready(ready: &mut Option<Ready>, result: Result<()>) {
    if let Some(ready) = ready.take() {
        match ready {
            Ready::Legacy(sender) => {
                let _received = sender.send(result);
            }
            Ready::Bootstrap(sender) => {
                let _received = sender.send(result.map(|()| true));
            }
        }
    }
}
async fn cancelled(cancel: &mut tokio::sync::watch::Receiver<bool>) {
    while !*cancel.borrow_and_update() {
        if cancel.changed().await.is_err() {
            break;
        }
    }
}
fn retain_uncertain(
    record: &Record,
    ticket: &mut Option<super::StartupReservation>,
    mounted: Option<lore_vfs::linux_working::Mounted>,
    error: Error,
) -> ! {
    let _withdrawn = retire(record, &error.to_string());
    drop(ticket.take());
    record
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .error = Some(error.to_string());
    if let Some(worker) = record
        .mount
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .as_ref()
    {
        worker.done.send_replace(Some(Err(error)));
    }
    // Unknown mount ownership remains quarantined even if the session joined.
    // Retain any kernel owner and the record instead of releasing backing
    // ownership or unmounting a replacement. The record remains Retiring.
    let _mounted = mounted;
    loop {
        std::thread::park();
    }
}
fn mount_ids(target: &Path) -> Result<Vec<u64>> {
    Ok(mount_table()?
        .into_iter()
        .filter_map(|(id, path)| (path == target.as_os_str().as_bytes()).then_some(id))
        .collect())
}
fn safe_target(table: &[(u64, Vec<u8>)], target: &Path, owned: u64) -> bool {
    let target = target.as_os_str().as_bytes();
    table.iter().all(|(id, path)| {
        if *id == owned {
            path == target
        } else {
            path != target
        }
    })
}
fn mount_table() -> Result<Vec<(u64, Vec<u8>)>> {
    const MAX_TABLE: usize = 4 * 1024 * 1024;
    let mut bytes = Vec::new();
    std::fs::File::open("/proc/self/mountinfo")
        .and_then(|file| file.take((MAX_TABLE + 1) as u64).read_to_end(&mut bytes))
        .map_err(|error| Error::Owner(error.to_string()))?;
    parse_table(&bytes)
}
fn parse_table(bytes: &[u8]) -> Result<Vec<(u64, Vec<u8>)>> {
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(Error::Owner(
            "kernel mount table exceeds inspection bound".into(),
        ));
    }
    let mut ids = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let separator = line
            .windows(3)
            .position(|part| part == b" - ")
            .ok_or_else(|| Error::Owner("malformed kernel mount table".into()))?;
        let fields: Vec<_> = line[..separator]
            .split(|byte| *byte == b' ')
            .take(6)
            .collect();
        if line[separator + 3..]
            .split(|byte| *byte == b' ')
            .take(3)
            .count()
            != 3
        {
            return Err(Error::Owner("malformed kernel mount table".into()));
        }
        if fields.len() != 6 {
            return Err(Error::Owner("malformed kernel mount table".into()));
        }
        {
            let id = std::str::from_utf8(fields[0])
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| Error::Owner("invalid kernel mount identity".into()))?;
            ids.push((id, unescape(fields[4])?));
        }
    }
    Ok(ids)
}
fn unescape(value: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'\\' {
            let digits = value
                .get(index + 1..index + 4)
                .filter(|digits| digits.iter().all(|digit| (b'0'..=b'7').contains(digit)))
                .ok_or_else(|| Error::Owner("invalid escaped mount path".into()))?;
            let code = u16::from(digits[0] - b'0') * 64
                + u16::from(digits[1] - b'0') * 8
                + u16::from(digits[2] - b'0');
            out.push(
                u8::try_from(code)
                    .map_err(|_overflow| Error::Owner("invalid escaped mount path".into()))?,
            );
            index += 4;
        } else {
            out.push(value[index]);
            index += 1;
        }
    }
    Ok(out)
}

#[cfg(feature = "test-util")]
pub(super) fn test_decision(bytes: &[u8], target: &Path, owned: u64) -> Result<bool> {
    Ok(safe_target(&parse_table(bytes)?, target, owned))
}
