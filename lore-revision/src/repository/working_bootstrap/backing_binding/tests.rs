// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
// Private files deliberately exercise corrupt and interrupted bootstrap records.
#![allow(clippy::disallowed_methods)]
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::PermissionsExt;

use super::*;

fn fixture() -> (lore_base::test_util::TempDir, PathBuf, PathBuf) {
    let directory = lore_base::test_util::TempDir::new("working-bootstrap-binding-");
    let metadata = directory.path().join("metadata");
    let root = directory.path().join("backing");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&metadata)
        .unwrap();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    (
        directory,
        metadata.canonicalize().unwrap(),
        root.canonicalize().unwrap(),
    )
}
fn repository() -> RepositoryId {
    [211; 16].into()
}
fn instance() -> InstanceId {
    static INSTANCE: std::sync::OnceLock<InstanceId> = std::sync::OnceLock::new();
    *INSTANCE.get_or_init(InstanceId::generate)
}

#[test]
fn backing_binding_roundtrip_never_overwrites() {
    let (_directory, metadata, root) = fixture();
    BackingBinding::write_new(&metadata, repository(), instance(), &root).unwrap();
    let original = std::fs::read(metadata.join(NAME)).unwrap();
    assert_eq!(
        BackingBinding::read(&metadata, repository(), instance())
            .unwrap()
            .root(),
        root
    );
    assert!(BackingBinding::write_new(&metadata, repository(), instance(), &root).is_err());
    assert_eq!(std::fs::read(metadata.join(NAME)).unwrap(), original);
    assert_eq!(
        std::fs::metadata(metadata.join(NAME))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
#[test]
fn backing_binding_refuses_missing_partial_corrupt_foreign_and_unknown_versions() {
    let (_directory, metadata, root) = fixture();
    assert!(BackingBinding::read(&metadata, repository(), instance()).is_err());
    assert!(!metadata.join(NAME).exists());
    let encoded = BackingBinding::new(repository(), instance(), &root)
        .unwrap()
        .encode()
        .unwrap();
    for length in 0..encoded.len() {
        assert!(BackingBinding::decode(&encoded[..length], repository(), instance()).is_err());
    }
    assert!(BackingBinding::decode(&encoded, [213; 16].into(), instance()).is_err());
    assert!(BackingBinding::decode(&encoded, repository(), InstanceId::generate()).is_err());
    let mut changed = encoded.clone();
    changed[8] = 2;
    assert!(BackingBinding::decode(&changed, repository(), instance()).is_err());
    let mut changed = encoded.clone();
    changed[HEADER] ^= 1;
    assert!(BackingBinding::decode(&changed, repository(), instance()).is_err());
    let mut changed = encoded;
    changed.push(0);
    assert!(BackingBinding::decode(&changed, repository(), instance()).is_err());
}
#[test]
fn backing_binding_refuses_unsafe_files_and_missing_recorded_roots_without_creation() {
    let (_directory, metadata, root) = fixture();
    BackingBinding::write_new(&metadata, repository(), instance(), &root).unwrap();
    let original = std::fs::read(metadata.join(NAME)).unwrap();
    std::fs::remove_dir(&root).unwrap();
    assert!(BackingBinding::read(&metadata, repository(), instance()).is_err());
    assert!(!root.exists());
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(metadata.join(NAME), std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(BackingBinding::read(&metadata, repository(), instance()).is_err());
    std::fs::remove_file(metadata.join(NAME)).unwrap();
    let elsewhere = metadata.join("elsewhere");
    std::fs::write(&elsewhere, &original).unwrap();
    std::os::unix::fs::symlink(&elsewhere, metadata.join(NAME)).unwrap();
    assert!(BackingBinding::read(&metadata, repository(), instance()).is_err());
    assert_eq!(std::fs::read(&elsewhere).unwrap(), original);
    std::fs::remove_file(metadata.join(NAME)).unwrap();
    std::fs::set_permissions(&elsewhere, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::hard_link(&elsewhere, metadata.join(NAME)).unwrap();
    assert!(BackingBinding::read(&metadata, repository(), instance()).is_err());
    assert_eq!(std::fs::read(&elsewhere).unwrap(), original);
}
#[test]
fn backing_binding_enforces_path_and_record_bounds_before_decode_allocation() {
    let (_directory, _metadata, root) = fixture();
    assert!(BackingBinding::new(repository(), instance(), Path::new("relative")).is_err());
    assert!(BackingBinding::new([0; 16].into(), instance(), &root).is_err());
    assert!(BackingBinding::new(repository(), InstanceId::default(), &root).is_err());
    let mut oversized = String::from("/");
    oversized.push_str(&"x".repeat(MAX_PATH));
    assert!(BackingBinding::new(repository(), instance(), &PathBuf::from(oversized)).is_err());
    let encoded = BackingBinding::new(repository(), instance(), &root)
        .unwrap()
        .encode()
        .unwrap();
    let mut changed = encoded;
    changed[44..48].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(BackingBinding::decode(&changed, repository(), instance()).is_err());
    assert!(BackingBinding::decode(&vec![0; MAX_BYTES + 1], repository(), instance()).is_err());
}

#[test]
fn backing_binding_rejects_file_roots_and_length_drift() {
    let (_directory, metadata, root) = fixture();
    let file_root = root.join("file");
    std::fs::write(&file_root, b"not a directory").unwrap();
    assert!(BackingBinding::new(repository(), instance(), &file_root).is_err());
    assert!(BackingBinding::write_new(&metadata, repository(), instance(), &file_root).is_err());
    assert!(!metadata.join(NAME).exists());
    let bytes = BackingBinding::new(repository(), instance(), &root)
        .unwrap()
        .encode()
        .unwrap();
    let exact = read_sized(&mut std::io::Cursor::new(&bytes), bytes.len() as u64).unwrap();
    assert_eq!(exact, bytes);
    assert!(exact.capacity() <= MAX_BYTES);
    assert!(read_sized(&mut std::io::Cursor::new(&bytes), bytes.len() as u64 - 1).is_err());
    assert!(read_sized(&mut std::io::Cursor::new(&bytes), bytes.len() as u64 + 1).is_err());
    assert!(read_sized(&mut std::io::Cursor::new(&bytes), MAX_BYTES as u64 + 1).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn backing_binding_roundtrip_preserves_non_utf8_unix_path() {
    use std::os::unix::ffi::OsStringExt;
    let (_directory, metadata, root) = fixture();
    let root = root.join(std::ffi::OsString::from_vec(vec![b'b', 0xff]));
    std::fs::create_dir(&root).unwrap();
    BackingBinding::write_new(&metadata, repository(), instance(), &root).unwrap();
    assert_eq!(
        BackingBinding::read(&metadata, repository(), instance())
            .unwrap()
            .root(),
        root
    );
}
