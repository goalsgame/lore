// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Kernel-originated notifications only; this does not qualify out-of-band sync.

use std::os::fd::FromRawFd;

use super::*;

#[test]
#[ignore = "requires native Linux FUSE and inotify"]
fn mounted_kernel_mutations_deliver_named_watch_events() {
    let directory = Directory::new("watch-events");
    let runtime = runtime();
    let stores = directory.0.join("stores");
    let mountpoint = directory.0.join("mount");
    std::fs::create_dir(&stores).unwrap();
    std::fs::create_dir(&mountpoint).unwrap();
    let model = runtime
        .block_on(Model::new(
            Arc::new(
                Namespace::new(
                    Arc::new(Base::new()),
                    root(),
                    Arc::new(Gate::default()),
                    Default::default(),
                )
                .unwrap(),
            ),
            Arc::new(DiskStores {
                directory: stores,
                next: AtomicU64::new(0),
            }),
            Arc::new(StorageBudget::new(1024)),
            Recovery::open(directory.0.join("recovery"), Default::default()).unwrap(),
            Default::default(),
        ))
        .unwrap();
    let mounted = mount(model, &runtime, &mountpoint);
    // SAFETY: inotify_init1 takes flags only and returns a newly owned descriptor.
    let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    assert!(fd >= 0, "{}", std::io::Error::last_os_error());
    // SAFETY: fd is a successful fresh descriptor, transferred exactly once.
    let mut watcher = unsafe { std::fs::File::from_raw_fd(fd) };
    let path = CString::new(mountpoint.as_os_str().as_bytes()).unwrap();
    let masks = libc::IN_CREATE
        | libc::IN_MODIFY
        | libc::IN_CLOSE_WRITE
        | libc::IN_ATTRIB
        | libc::IN_MOVED_FROM
        | libc::IN_MOVED_TO
        | libc::IN_DELETE;
    // SAFETY: watcher is live and path is a terminated string.
    let watch = unsafe { libc::inotify_add_watch(watcher.as_raw_fd(), path.as_ptr(), masks) };
    assert!(watch >= 0, "{}", std::io::Error::last_os_error());

    let first = mountpoint.join("first");
    let second = mountpoint.join("second");
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&first)
        .unwrap();
    file.write_all(b"watched").unwrap();
    drop(file);
    std::fs::set_permissions(&first, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::rename(&first, &second).unwrap();
    std::fs::remove_file(&second).unwrap();

    let expected = [
        (b"first".as_slice(), libc::IN_CREATE),
        (b"first".as_slice(), libc::IN_MODIFY),
        (b"first".as_slice(), libc::IN_CLOSE_WRITE),
        (b"first".as_slice(), libc::IN_ATTRIB),
        (b"first".as_slice(), libc::IN_MOVED_FROM),
        (b"second".as_slice(), libc::IN_MOVED_TO),
        (b"second".as_slice(), libc::IN_DELETE),
    ];
    let mut seen = [false; 7];
    let mut moved_from = None;
    let mut moved_to = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !seen.iter().all(|seen| *seen) && std::time::Instant::now() < deadline {
        let mut bytes = [0u8; 4096];
        let count = match watcher.read(&mut bytes) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(error) => panic!("inotify read failed: {error}"),
        };
        let mut offset = 0;
        while offset < count {
            let header = std::mem::size_of::<libc::inotify_event>();
            assert!(count - offset >= header);
            // SAFETY: the complete header is present; byte buffers need not be aligned.
            let event = unsafe {
                bytes
                    .as_ptr()
                    .add(offset)
                    .cast::<libc::inotify_event>()
                    .read_unaligned()
            };
            let end = offset + header + event.len as usize;
            assert!(end <= count);
            assert_eq!(
                event.mask & libc::IN_Q_OVERFLOW,
                0,
                "watch queue overflowed"
            );
            assert_eq!(event.wd, watch);
            let name = &bytes[offset + header..end];
            let name = &name[..name
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(name.len())];
            for (index, (expected_name, mask)) in expected.iter().enumerate() {
                seen[index] |= name == *expected_name && event.mask & mask != 0;
            }
            if name == b"first" && event.mask & libc::IN_MOVED_FROM != 0 {
                moved_from = Some(event.cookie);
            }
            if name == b"second" && event.mask & libc::IN_MOVED_TO != 0 {
                moved_to = Some(event.cookie);
            }
            offset = end;
        }
    }
    assert!(
        seen.iter().all(|seen| *seen),
        "missing named events: {seen:?}"
    );
    assert_ne!(moved_from, Some(0));
    assert_eq!(
        moved_from, moved_to,
        "rename events must share their cookie"
    );
    drop(watcher);
    mounted.unmount().unwrap();
}
