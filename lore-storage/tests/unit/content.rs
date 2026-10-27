// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![allow(clippy::disallowed_methods)] // Isolated filesystem and pending-IO fixtures.
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_storage::content::ContentSource;
use lore_storage::error::StorageError;

fn test_address(seed: u64) -> Address {
    Address {
        hash: Hash::from_u64(seed),
        ..Default::default()
    }
}

/// Virtual content names no host path.
#[test]
fn virtual_immutable_has_no_file_path() {
    let source = ContentSource::virtual_immutable(test_address(1), 42);
    assert!(source.file_path().is_none());
}

/// This crate does not open virtual content.
#[tokio::test]
async fn virtual_immutable_open_once_is_unsupported() {
    let source = ContentSource::virtual_immutable(test_address(1), 42);
    match source.open_once().await {
        Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::Unsupported),
        Ok(_) => panic!("virtual content cannot be opened directly"),
    }
}

/// Virtual immutable content displays as the address it names.
#[test]
fn virtual_immutable_display_names_address() {
    let address = test_address(7);
    let source = ContentSource::virtual_immutable(address, 42);
    assert_eq!(format!("{source}"), format!("immutable:{address}"));
}

/// Virtual immutable content answers its declared size, which settles a comparison by size
/// without reading it.
#[tokio::test]
async fn virtual_immutable_size_is_its_declared_size() {
    let source = ContentSource::virtual_immutable(test_address(1), 42);
    assert_eq!(source.size().await.expect("a declared size"), 42);
}

/// Opening or reading virtual content fails at once as an argument error, with no back-off.
#[tokio::test]
async fn virtual_immutable_open_and_read_all_are_invalid_arguments() {
    let source = ContentSource::virtual_immutable(test_address(1), 42);
    let started = std::time::Instant::now();
    match source.open().await {
        Err(err) => assert!(matches!(err, StorageError::InvalidArguments(_)), "{err:?}"),
        Ok(_) => panic!("virtual content cannot be opened by this crate"),
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "the open waited on a back-off"
    );
    let err = source
        .read_all()
        .await
        .expect_err("virtual content cannot be read by this crate");
    assert!(matches!(err, StorageError::InvalidArguments(_)), "{err:?}");
}

struct RemoveFile(std::path::PathBuf);
impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn leased_sources_transfer_storage_ownership_to_cloned_read_handles() {
    use std::sync::Arc;
    let directory = lore_base::test_util::TempDir::new("lore-leased-content-");
    let path = directory.join("content");
    std::fs::write(&path, b"retained bytes").unwrap();
    let owner = Arc::new(RemoveFile(path.clone()));
    let weak = Arc::downgrade(&owner);
    let source = ContentSource::leased_file(path.clone(), owner);
    assert_eq!(source.file_path(), Some(path.as_path()));
    assert_eq!(source.size().await.unwrap(), 14);
    assert_eq!(source.read_all().await.unwrap().as_ref(), b"retained bytes");
    let (handle, size) = source.open().await.unwrap();
    assert_eq!(size, 14);
    let clone = handle.clone();
    drop(handle);
    drop(source);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        clone.read_all(size as usize).await.unwrap().as_ref(),
        b"retained bytes"
    );
    drop(clone);
    let start = std::time::Instant::now();
    while weak.upgrade().is_some() || path.exists() {
        assert!(start.elapsed() < std::time::Duration::from_secs(10));
        tokio::task::yield_now().await;
    }
    assert!(!path.exists());
}

#[cfg(unix)]
#[test]
fn cancelled_leased_source_retains_storage_through_actual_pending_open() {
    use std::future::Future;
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Waker;
    let directory = lore_base::test_util::TempDir::new("lore-leased-open-");
    let path = directory.join("pending");
    let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: the fixed private fixture path is a valid NUL-terminated name.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let owner = Arc::new(RemoveFile(path.clone()));
    let weak = Arc::downgrade(&owner);
    let source = ContentSource::leased_file(path.clone(), owner);
    let mut open = Box::pin(source.open_once());
    assert!(
        open.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(open);
    drop(source);
    assert!(
        weak.upgrade().is_some(),
        "the submitted open owns the storage lease"
    );
    // A writer remains present until the queued/running reader has completed.
    // RDWR avoids blocking this thread if the syscall pool has not started it yet.
    let writer = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&path)
        .unwrap();
    let start = std::time::Instant::now();
    while weak.upgrade().is_some() || path.exists() {
        assert!(start.elapsed() < std::time::Duration::from_secs(10));
        std::thread::yield_now();
    }
    drop(writer);
    assert!(!path.exists());
}

#[cfg(target_os = "linux")]
#[test]
fn cancelled_leased_handle_retains_storage_through_actual_uring_read() {
    use std::future::Future;
    use std::io::Write;
    use std::os::fd::FromRawFd;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Waker;

    use lore_storage::content::ContentHandle;
    use lore_storage::content::WindowRead;
    let driver = match lore_io::IoDriver::new(lore_io::BackendKind::Uring) {
        Ok(driver) => driver,
        Err(error) => {
            eprintln!("leased-handle uring qualification unavailable: {error}");
            return;
        }
    };
    let mut descriptors = [-1; 2];
    // SAFETY: two writable slots receive unique owned descriptors.
    assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
    // SAFETY: pipe returned each of these unique owned descriptors.
    let file = unsafe { std::fs::File::from_raw_fd(descriptors[0]) };
    // SAFETY: this is the other unique pipe descriptor.
    let mut writer = unsafe { std::fs::File::from_raw_fd(descriptors[1]) };
    // SAFETY: the uring backend imposes no additional open-handle requirements.
    let file = unsafe { lore_io::IoFile::from_std(&driver, Arc::new(file)) }.unwrap();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let handle = ContentHandle::LeasedFile {
        file,
        lease: lore_io::lease::IoLease::new(owner),
    };
    let mut read =
        Box::pin(handle.read_window(WindowRead::new(bytes::BytesMut::zeroed(1), 0, 1), u64::MAX));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(read);
    drop(handle);
    assert!(
        weak.upgrade().is_some(),
        "kernel read retains the abandoned lease"
    );
    writer.write_all(b"x").unwrap();
    let start = std::time::Instant::now();
    while weak.upgrade().is_some() {
        assert!(start.elapsed() < std::time::Duration::from_secs(10));
        std::thread::yield_now();
    }
    eprintln!("leased-handle actual pending uring read passed");
}
