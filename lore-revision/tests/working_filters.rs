// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(unix)]
#![allow(clippy::disallowed_methods)] // Isolated private-file fixtures.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use lore_base::test_util::TempDir;
use lore_revision::fs::filesystem_provider::*;
use lore_revision::repository::MAX_FILTER_FILE_BYTES;
use lore_revision::repository::load_working_filter;
use lore_revision::util::path::RelativePath;

enum Reply {
    Data(Vec<u8>),
    Denied,
}
#[derive(Default)]
struct Provider {
    files: BTreeMap<&'static str, Reply>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl FilesystemProvider for Provider {
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal("not used by metadata loading"))
    }
    async fn read_metadata_file(
        &self,
        path: &RelativePath,
        maximum: usize,
    ) -> Result<Vec<u8>, FsError> {
        assert_eq!(maximum, MAX_FILTER_FILE_BYTES);
        self.calls.lock().unwrap().push(path.to_string());
        match self.files.get(path.as_str()) {
            Some(Reply::Data(bytes)) => Ok(bytes.clone()),
            Some(Reply::Denied) => Err(FsError::internal("permission denied")),
            None => Err(lore_base::error::FileNotFound {
                resource: path.to_string(),
            }
            .into()),
        }
    }
}
fn metadata() -> TempDir {
    let directory = TempDir::new("lore-private-filters-");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}
fn data(bytes: &[u8]) -> Reply {
    Reply::Data(bytes.to_vec())
}

#[tokio::test]
async fn provider_ignore_precedence_and_shared_reserved_exclusions() {
    let directory = metadata();
    let provider = Provider {
        files: BTreeMap::from([
            (".loreignore", data(b"secret\n")),
            (".urcignore", data(b"other\n")),
        ]),
        ..Default::default()
    };
    let filter = load_working_filter(
        &provider,
        std::path::Path::new("/absent-mounted-root"),
        directory.path(),
    )
    .await
    .unwrap();
    assert!(
        filter
            .ignore
            .excludes(&"secret".parse::<RelativePath>().unwrap(), false)
    );
    assert!(
        !filter
            .ignore
            .excludes(&"other".parse::<RelativePath>().unwrap(), false)
    );
    assert!(
        filter
            .ignore
            .excludes(&".lore".parse::<RelativePath>().unwrap(), true)
    );
    assert_eq!(filter.ignore.lines.len(), 7);
    assert_eq!(*provider.calls.lock().unwrap(), [".loreignore"]);
    let fallback = Provider {
        files: BTreeMap::from([(".urcignore", data(b"legacy\n"))]),
        ..Default::default()
    };
    let filter = load_working_filter(&fallback, directory.path(), directory.path())
        .await
        .unwrap();
    assert!(
        filter
            .ignore
            .excludes(&"legacy".parse::<RelativePath>().unwrap(), false)
    );
    assert_eq!(
        *fallback.calls.lock().unwrap(),
        [".loreignore", ".urcignore"]
    );
    let empty = Provider {
        files: BTreeMap::from([
            (".loreignore", data(b"")),
            (".urcignore", data(b"legacy\n")),
        ]),
        ..Default::default()
    };
    let filter = load_working_filter(&empty, directory.path(), directory.path())
        .await
        .unwrap();
    assert_eq!(filter.ignore.lines.len(), 6);
    assert_eq!(*empty.calls.lock().unwrap(), [".loreignore"]);
}

#[tokio::test]
async fn provider_ignore_errors_do_not_fall_back_or_broaden_filter() {
    let directory = metadata();
    for reply in [
        Reply::Denied,
        data(&[0xff]),
        Reply::Data(vec![b'x'; MAX_FILTER_FILE_BYTES + 1]),
    ] {
        let provider = Provider {
            files: BTreeMap::from([(".loreignore", reply), (".urcignore", data(b"secret\n"))]),
            ..Default::default()
        };
        assert!(
            load_working_filter(&provider, directory.path(), directory.path())
                .await
                .is_err()
        );
        assert_eq!(*provider.calls.lock().unwrap(), [".loreignore"]);
    }
    let denied_legacy = Provider {
        files: BTreeMap::from([(".urcignore", Reply::Denied)]),
        ..Default::default()
    };
    assert!(
        load_working_filter(&denied_legacy, directory.path(), directory.path())
            .await
            .is_err()
    );
    let filter = load_working_filter(&Provider::default(), directory.path(), directory.path())
        .await
        .unwrap();
    assert_eq!(filter.ignore.lines.len(), 6);
}

#[tokio::test]
async fn private_view_is_bounded_and_rejects_unsafe_files_without_blocking() {
    let directory = metadata();
    let view = directory.path().join("view");
    std::fs::write(&view, b"visible\n").unwrap();
    let provider = Provider::default();
    let filter = load_working_filter(&provider, directory.path(), directory.path())
        .await
        .unwrap();
    assert_eq!(filter.view.lines.len(), 1);
    std::fs::set_permissions(&view, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(
        load_working_filter(&provider, directory.path(), directory.path())
            .await
            .is_err()
    );
    std::fs::remove_file(&view).unwrap();
    std::os::unix::fs::symlink("outside", &view).unwrap();
    assert!(
        load_working_filter(&provider, directory.path(), directory.path())
            .await
            .is_err()
    );
    std::fs::remove_file(&view).unwrap();
    let name = std::ffi::CString::new(view.as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: the fixture name is NUL terminated and creates only this private FIFO.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        load_working_filter(&provider, directory.path(), directory.path()),
    )
    .await
    .unwrap()
    .unwrap_err();
    std::fs::remove_file(&view).unwrap();
    std::fs::write(&view, vec![b'x'; MAX_FILTER_FILE_BYTES + 1]).unwrap();
    assert!(
        load_working_filter(&provider, directory.path(), directory.path())
            .await
            .is_err()
    );
    std::fs::write(&view, [0xff]).unwrap();
    assert!(
        load_working_filter(&provider, directory.path(), directory.path())
            .await
            .is_err()
    );
    std::fs::remove_file(&view).unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        load_working_filter(&provider, directory.path(), directory.path())
            .await
            .is_err()
    );
}
