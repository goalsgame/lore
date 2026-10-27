// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::mpsc;
use std::time::Duration;
static BOOTSTRAP_TEST: Mutex<()> = Mutex::new(());

use super::*;

extern "C" fn complete(context: *mut c_void, operation: u64) {
    // SAFETY: submit retains this boxed sender until its exactly-once callback.
    let sender = unsafe { Box::from_raw(context.cast::<mpsc::Sender<u64>>()) };
    let _ = sender.send(operation);
}
fn request(opcode: u32) -> LvfsRequest {
    LvfsRequest {
        opcode,
        reserved: 0,
        inode: 1,
        handle: 0,
        offset: 0,
        count: 0,
        name_length: 0,
        name: std::ptr::null(),
    }
}
fn submit(session: u64, request: &LvfsRequest) -> (u64, mpsc::Receiver<u64>) {
    let (sender, receiver) = mpsc::channel();
    let context = Box::into_raw(Box::new(sender));
    let mut operation = 0;
    // SAFETY: pointers remain live for submit and the boxed context is retained until callback.
    let status = unsafe {
        lore_fskit_submit(
            session,
            request,
            Some(complete),
            context.cast(),
            &mut operation,
        )
    };
    assert_eq!(status, OK);
    (operation, receiver)
}
fn done(session: u64, request: &LvfsRequest) -> (u64, LvfsResult, Vec<u8>) {
    let (operation, receiver) = submit(session, request);
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3)).unwrap(),
        operation
    );
    let mut result = LvfsResult::default();
    // SAFETY: caller-owned result; zero-size payload probes are supported.
    let status =
        unsafe { lore_fskit_operation_result(operation, &mut result, std::ptr::null_mut(), 0) };
    assert!(status == OK || status == RANGE);
    let mut bytes = vec![0; result.payload_length as usize];
    // SAFETY: the allocated buffer matches the reported bounded result size.
    assert_eq!(
        unsafe {
            lore_fskit_operation_result(
                operation,
                &mut result,
                bytes.as_mut_ptr(),
                bytes.len() as u32,
            )
        },
        OK
    );
    (operation, result, bytes)
}
#[test]
fn abi_reads_names_handles_digests_and_double_release() {
    assert_eq!(lore_fskit_abi_version(), ABI);
    assert_eq!(std::mem::size_of::<LvfsEntry>(), 32);
    assert_eq!(std::mem::size_of::<LvfsResult>(), 56);
    assert_eq!(std::mem::size_of::<LvfsRequest>(), 48);
    let session = retain(fixture::core()).unwrap();
    let mut lookup = request(2);
    lookup.name = b"file".as_ptr();
    lookup.name_length = 4;
    let (operation, result, _) = done(session, &lookup);
    assert_eq!(result.status, OK);
    let inode = result.entry.inode;
    assert_eq!(lore_fskit_operation_release(operation), OK);
    assert_eq!(lore_fskit_operation_release(operation), BAD_HANDLE);
    let mut open = request(3);
    open.inode = inode;
    let (operation, result, _) = done(session, &open);
    assert_eq!(lore_fskit_operation_claim(operation), OK);
    let handle = result.value;
    assert_eq!(lore_fskit_operation_release(operation), OK);
    let mut read = request(5);
    read.handle = handle;
    read.count = 7;
    let (operation, result, bytes) = done(session, &read);
    assert_eq!(result.status, OK);
    assert_eq!(bytes, b"payload");
    assert_eq!(lore_fskit_operation_release(operation), OK);
    let mut digest = request(8);
    digest.inode = inode;
    let (operation, result, bytes) = done(session, &digest);
    assert_eq!(result.status, OK);
    assert_eq!(bytes, [42; 32]);
    assert_eq!(lore_fskit_operation_release(operation), OK);
    assert_eq!(lore_fskit_handle_release(session, handle), OK);
    assert_eq!(lore_fskit_handle_release(session, handle), BAD_HANDLE);
    assert_eq!(lore_fskit_session_close(session), OK);
    assert_eq!(lore_fskit_session_close(session), BAD_HANDLE);
}
#[test]
fn cancelled_and_unclaimed_open_does_not_leak_handle() {
    let core = fixture::core();
    let session = retain(core.clone()).unwrap();
    let (lookup, r, _) = done(
        session,
        &LvfsRequest {
            name: b"file".as_ptr(),
            name_length: 4,
            ..request(2)
        },
    );
    lore_fskit_operation_release(lookup);
    let (operation, result, _) = done(
        session,
        &LvfsRequest {
            inode: r.entry.inode,
            ..request(3)
        },
    );
    assert_eq!(lore_fskit_operation_release(operation), OK);
    assert_eq!(core.release(result.value), Err(Error::BadHandle));
    assert_eq!(lore_fskit_session_close(session), OK);
}
#[test]
fn cancellation_panic_and_close_drain_complete_exactly_once() {
    let session = retain(fixture::core()).unwrap();
    let blocked = LvfsRequest {
        name: b"blocked".as_ptr(),
        name_length: 7,
        ..request(2)
    };
    let (operation, receiver) = submit(session, &blocked);
    assert_eq!(lore_fskit_operation_cancel(operation), OK);
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3)).unwrap(),
        operation
    );
    let mut result = LvfsResult::default();
    // SAFETY: valid result output and supported zero-size payload.
    assert_eq!(
        unsafe { lore_fskit_operation_result(operation, &mut result, std::ptr::null_mut(), 0) },
        OK
    );
    assert_eq!(result.status, CANCELLED);
    assert!(receiver.recv_timeout(Duration::from_millis(10)).is_err());
    lore_fskit_operation_release(operation);
    let (operation, result, _) = done(
        session,
        &LvfsRequest {
            name: b"panic".as_ptr(),
            name_length: 5,
            ..request(2)
        },
    );
    assert_eq!(result.status, PANIC);
    lore_fskit_operation_release(operation);
    let (_, receiver) = submit(session, &blocked);
    assert_eq!(lore_fskit_session_close(session), OK);
    receiver.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(receiver.recv_timeout(Duration::from_millis(10)).is_err());
}

#[test]
fn rejected_submit_never_callbacks_and_dispatch_panic_completes_accepted_operation() {
    let session = retain(fixture::core()).unwrap();
    let (sender, receiver) = mpsc::channel::<u64>();
    let context = Box::into_raw(Box::new(sender));
    let mut operation = 123;
    let invalid = request(999);
    // SAFETY: all pointers are valid; rejected submit does not consume context.
    assert_eq!(
        unsafe {
            lore_fskit_submit(
                session,
                &invalid,
                Some(complete),
                context.cast(),
                &mut operation,
            )
        },
        INVALID
    );
    assert_eq!(operation, 0);
    assert!(receiver.try_recv().is_err());
    // SAFETY: callback did not consume a rejected context.
    drop(unsafe { Box::from_raw(context) });
    DISPATCH_PANIC.with(|flag| flag.set(true));
    let (operation, result, _) = done(session, &request(1));
    assert_eq!(result.status, PANIC);
    assert_eq!(lore_fskit_operation_release(operation), OK);
    assert_eq!(lore_fskit_session_close(session), OK);
}

#[test]
fn bootstrap_reservations_are_bounded_and_released_without_starting_setup() {
    let _serial = BOOTSTRAP_TEST.lock().unwrap();
    let permits = (0..4)
        .map(|_| BootstrapGuard::reserve().unwrap())
        .collect::<Vec<_>>();
    assert!(matches!(BootstrapGuard::reserve(), Err(BUSY)));
    drop(permits);
    assert!(BootstrapGuard::reserve().is_ok());
}

extern "C" fn reentrant_close(context: *mut c_void, operation: u64) {
    // SAFETY: the boxed tuple is retained until this exactly-once callback.
    let context = unsafe { Box::from_raw(context.cast::<(u64, mpsc::Sender<(u64, i32)>)>()) };
    let status = lore_fskit_session_close(context.0);
    let _ = context.1.send((operation, status));
}
#[test]
fn callback_close_reentry_is_rejected_without_deadlock() {
    let session = retain(fixture::core()).unwrap();
    let (sender, receiver) = mpsc::channel::<(u64, i32)>();
    let context = Box::into_raw(Box::new((session, sender)));
    let mut operation = 0;
    // Force completion on this non-runtime caller thread, exercising the callback
    // reentry guard independently of the Tokio-worker close restriction.
    DISPATCH_PANIC.with(|flag| flag.set(true));
    // SAFETY: request/output and boxed callback context have the required lifetimes.
    assert_eq!(
        unsafe {
            lore_fskit_submit(
                session,
                &request(1),
                Some(reentrant_close),
                context.cast(),
                &mut operation,
            )
        },
        OK
    );
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3)).unwrap(),
        (operation, BUSY)
    );
    assert_eq!(lore_fskit_session_close(session), OK);
}

#[test]
fn racing_cancel_and_close_deliver_one_completion_and_invalidate_results() {
    for _ in 0..8 {
        let session = retain(fixture::core()).unwrap();
        let (operation, receiver) = submit(
            session,
            &LvfsRequest {
                name: b"blocked".as_ptr(),
                name_length: 7,
                ..request(2)
            },
        );
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let cancel_barrier = barrier.clone();
        let cancel = std::thread::spawn(move || {
            cancel_barrier.wait();
            lore_fskit_operation_cancel(operation)
        });
        let close_barrier = barrier.clone();
        let close = std::thread::spawn(move || {
            close_barrier.wait();
            lore_fskit_session_close(session)
        });
        barrier.wait();
        assert_eq!(close.join().unwrap(), OK);
        assert!(matches!(cancel.join().unwrap(), OK | BAD_HANDLE));
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(3)).unwrap(),
            operation
        );
        assert!(receiver.try_recv().is_err());
        assert_eq!(lore_fskit_operation_release(operation), BAD_HANDLE);
    }
}

#[test]
fn named_lookup_preserves_spelling_and_cached_open_rechecks_authority() {
    let backend = Arc::new(fixture::Fixture::default());
    let session = retain(fixture::with_backend(backend.clone())).unwrap();
    let (operation, result, name) = done(
        session,
        &LvfsRequest {
            name: b"FILE".as_ptr(),
            name_length: 4,
            ..request(10)
        },
    );
    assert_eq!(result.status, OK);
    assert_eq!(name, b"file");
    let inode = result.entry.inode;
    lore_fskit_operation_release(operation);
    backend.denied.store(true, Ordering::Release);
    for opcode in [1, 3, 11] {
        let (operation, result, _) = done(
            session,
            &LvfsRequest {
                inode,
                ..request(opcode)
            },
        );
        assert_eq!(result.status, 3);
        lore_fskit_operation_release(operation);
    }
    assert_eq!(lore_fskit_session_close(session), OK);
}

#[test]
fn bootstrap_deadline_returns_without_leaking_a_late_session_or_its_budget() {
    let _serial = BOOTSTRAP_TEST.lock().unwrap();
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_release = released.clone();
    let result = bootstrap_session(Vec::new(), Duration::from_millis(20), move |_, _| {
        let (mutex, condition) = &*worker_release;
        let mut released = mutex.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        Ok(fixture::core())
    });
    assert_eq!(result, Err(IO));
    assert_eq!(registry().lock().unwrap().bootstraps, 1);
    *released.0.lock().unwrap() = true;
    released.1.notify_all();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while registry().lock().unwrap().bootstraps != 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(unix)]
#[test]
fn bootstrap_rejects_unknown_secret_fields_and_unsafe_files_without_repository_access() {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;
    let directory = std::env::temp_dir().join(format!(
        "lore-fskit-config-{}-{}",
        std::process::id(),
        next().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("config.json");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    file.write_all(
        br#"{"repository":"/not-accessed","revision":"main@LATEST","access_token":"not-rendered"}"#,
    )
    .unwrap();
    drop(file);
    assert!(matches!(
        bootstrap::open(
            path.to_str().unwrap().as_bytes(),
            Arc::new(BootstrapCancel::default())
        ),
        Err(INVALID)
    ));
    // With otherwise valid syntax, accepting this file would reach the missing
    // repository rather than returning the file-policy error.
    std::fs::write(
        &path,
        br#"{"repository":"/not-accessed","revision":"main@LATEST","offline":true}"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(matches!(
        bootstrap::open(
            path.to_str().unwrap().as_bytes(),
            Arc::new(BootstrapCancel::default())
        ),
        Err(INVALID)
    ));
    let alias = directory.join("alias");
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(matches!(
        bootstrap::open(
            alias.to_str().unwrap().as_bytes(),
            Arc::new(BootstrapCancel::default())
        ),
        Err(IO)
    ));
    std::fs::remove_dir_all(directory).unwrap();
}

extern "C" fn observe_future_drop(context: *mut c_void, operation: u64) {
    // SAFETY: boxed context ownership transfers once to the callback.
    let context = unsafe {
        Box::from_raw(context.cast::<(Arc<fixture::Fixture>, mpsc::Sender<(u64, bool)>)>())
    };
    let _ = context
        .1
        .send((operation, context.0.dropped.load(Ordering::Acquire)));
}
#[test]
fn close_drops_backend_future_before_its_completion_callback() {
    let backend = Arc::new(fixture::Fixture::default());
    let session = retain(fixture::with_backend(backend.clone())).unwrap();
    let (sender, receiver) = mpsc::channel::<(u64, bool)>();
    let context = Box::into_raw(Box::new((backend.clone(), sender)));
    let request = LvfsRequest {
        name: b"blocked".as_ptr(),
        name_length: 7,
        ..request(2)
    };
    let mut operation = 0;
    // SAFETY: valid request/output and retained boxed callback context.
    assert_eq!(
        unsafe {
            lore_fskit_submit(
                session,
                &request,
                Some(observe_future_drop),
                context.cast(),
                &mut operation,
            )
        },
        OK
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !backend.started.load(Ordering::Acquire) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(lore_fskit_session_close(session), OK);
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(3)).unwrap(),
        (operation, true)
    );
}
