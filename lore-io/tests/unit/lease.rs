// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use lore_io::lease::IoLease;
use lore_io::lease::scope;
use lore_io::lease::sync_scope;

#[test]
fn nested_scopes_restore_after_return_unwind_and_explicit_clear() {
    let outer = IoLease::new(Arc::new(()));
    let inner = IoLease::new(Arc::new(()));
    assert!(IoLease::current().is_none());
    outer.sync_scope(|| {
        assert!(IoLease::current().unwrap().ptr_eq(&outer));
        inner.sync_scope(|| assert!(IoLease::current().unwrap().ptr_eq(&inner)));
        sync_scope(None, || assert!(IoLease::current().is_none()));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                inner.sync_scope(|| panic!("scope unwind"));
            }))
            .is_err()
        );
        assert!(IoLease::current().unwrap().ptr_eq(&outer));
    });
    assert!(IoLease::current().is_none());
}

#[test]
fn polls_restore_caller_scope_and_cancellation_releases_unsubmitted_ownership() {
    use std::future::Future;
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    let mut future = Box::pin(lease.scope(std::future::poll_fn(|_context| {
        assert!(IoLease::current().is_some());
        Poll::<()>::Pending
    })));
    drop(lease);
    let outer = IoLease::new(Arc::new(()));
    outer.sync_scope(|| {
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(IoLease::current().unwrap().ptr_eq(&outer));
    });
    assert!(weak.upgrade().is_some());
    drop(future);
    assert!(weak.upgrade().is_none());
    let mut empty = Box::pin(scope(
        None,
        std::future::poll_fn(|_context| {
            assert!(IoLease::current().is_none());
            Poll::Ready(())
        }),
    ));
    outer.sync_scope(|| {
        assert!(
            empty
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    });
    assert!(IoLease::current().is_none());
}

#[test]
fn cancelled_queued_and_running_jobs_keep_owner_until_actual_completion() {
    use lore_io::pool::SyscallPool;
    let pool = SyscallPool::new(1);
    let (entered, entering) = std::sync::mpsc::channel();
    let (release, releasing) = std::sync::mpsc::channel();
    let blocker = pool.submit(move || {
        entered.send(()).unwrap();
        releasing.recv().unwrap();
    });
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    let (entered, entering) = std::sync::mpsc::channel();
    let (finish, finishing) = std::sync::mpsc::channel();
    let job = lease.sync_scope(|| {
        pool.submit(move || {
            assert!(IoLease::current().is_some());
            entered.send(()).unwrap();
            finishing.recv().unwrap();
        })
    });
    drop(job);
    drop(lease);
    assert!(weak.upgrade().is_some(), "queued job retains owner");
    release.send(()).unwrap();
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(weak.upgrade().is_some(), "running job retains owner");
    finish.send(()).unwrap();
    let started = Instant::now();
    while weak.upgrade().is_some() {
        assert!(started.elapsed() < Duration::from_secs(10));
        std::thread::yield_now();
    }
    futures::executor::block_on(blocker);
}

#[test]
fn dropping_scope_owner_can_reenter_without_borrowing_tls() {
    struct Reentrant;
    impl Drop for Reentrant {
        fn drop(&mut self) {
            let _scope = IoLease::current();
        }
    }
    sync_scope(Some(IoLease::new(Arc::new(Reentrant))), || {});
    assert!(IoLease::current().is_none());
}

#[cfg(target_os = "linux")]
#[test]
fn uring_abandoned_pending_read_retains_owner_until_kernel_completion() {
    use std::future::Future;
    use std::io::Write;
    use std::os::fd::FromRawFd;

    use lore_io::BackendKind;
    use lore_io::IoDriver;
    use lore_io::IoFile;
    let driver = match IoDriver::new(BackendKind::Uring) {
        Ok(driver) => driver,
        Err(error) => {
            eprintln!("uring IO lease qualification unavailable: {error}");
            return;
        }
    };
    assert_eq!(driver.backend_name(), "uring");
    let mut descriptors = [-1; 2];
    // SAFETY: two writable descriptor slots; each successful descriptor is owned once below.
    assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
    // SAFETY: pipe returned these unique owned descriptors.
    let read = unsafe { std::fs::File::from_raw_fd(descriptors[0]) };
    // SAFETY: this is the other unique owned descriptor returned by pipe.
    let mut write = unsafe { std::fs::File::from_raw_fd(descriptors[1]) };
    // SAFETY: the uring backend places no further requirements on caller-owned handles.
    let file = unsafe { IoFile::from_std(&driver, Arc::new(read)) }.unwrap();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    // The kernel's -1 offset requests the pipe's current position. No data is
    // available until the writer below releases the pending kernel operation.
    let mut read = Box::pin(lease.scope(file.read_at(1, u64::MAX)));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(read);
    drop(lease);
    assert!(
        weak.upgrade().is_some(),
        "kernel operation owns abandoned lease"
    );
    write.write_all(b"x").unwrap();
    let started = Instant::now();
    while weak.upgrade().is_some() {
        assert!(started.elapsed() < Duration::from_secs(10));
        std::thread::yield_now();
    }
    eprintln!("uring IO lease qualification passed using a pending pipe read");
}

#[test]
fn cancellation_destructor_io_keeps_scope_and_restores_tls_even_on_unwind() {
    use std::future::Future;
    use std::pin::Pin;

    use lore_io::pool::SyscallPool;
    struct Cleanup {
        pool: Arc<SyscallPool>,
        work: Option<Box<dyn FnOnce() + Send>>,
        panic: bool,
    }
    impl Future for Cleanup {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            drop(self.pool.submit(self.work.take().unwrap()));
            assert!(!self.panic, "cleanup unwind");
        }
    }
    for panic in [false, true] {
        let pool = Arc::new(SyscallPool::new(1));
        let (entered, entering) = std::sync::mpsc::channel();
        let (release, releasing) = std::sync::mpsc::channel();
        let blocker = pool.submit(move || {
            entered.send(()).unwrap();
            releasing.recv().unwrap();
        });
        entering.recv_timeout(Duration::from_secs(10)).unwrap();
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let lease = IoLease::new(owner);
        let cleanup = lease.scope(Cleanup {
            pool,
            work: Some(Box::new(|| assert!(IoLease::current().is_some()))),
            panic,
        });
        drop(lease);
        let unrelated = IoLease::new(Arc::new(()));
        unrelated.sync_scope(|| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(cleanup)));
            assert_eq!(result.is_err(), panic);
            assert!(IoLease::current().unwrap().ptr_eq(&unrelated));
        });
        assert!(
            weak.upgrade().is_some(),
            "queued destructor IO retains original owner"
        );
        release.send(()).unwrap();
        let started = Instant::now();
        while weak.upgrade().is_some() {
            assert!(started.elapsed() < Duration::from_secs(10));
            std::thread::yield_now();
        }
        futures::executor::block_on(blocker);
    }
}

#[test]
fn unscoped_destructor_io_does_not_capture_unrelated_ambient_owner() {
    use std::future::Future;
    use std::pin::Pin;

    use lore_io::pool::SyscallPool;
    struct Cleanup(Arc<SyscallPool>);
    impl Future for Cleanup {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            drop(self.0.submit(|| assert!(IoLease::current().is_none())));
        }
    }
    let pool = Arc::new(SyscallPool::new(1));
    let (entered, entering) = std::sync::mpsc::channel();
    let (release, releasing) = std::sync::mpsc::channel();
    let blocker = pool.submit(move || {
        entered.send(()).unwrap();
        releasing.recv().unwrap();
    });
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let cleanup = scope(None, Cleanup(pool.clone()));
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let lease = IoLease::new(owner);
    lease.sync_scope(|| drop(cleanup));
    drop(lease);
    assert!(weak.upgrade().is_none());
    release.send(()).unwrap();
    futures::executor::block_on(blocker);
    futures::executor::block_on(pool.submit(|| {}));
}
