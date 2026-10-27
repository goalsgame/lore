// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Removable whole-file SHA-256 records in a caller-owned, trusted cache directory.
//!
//! Records are partitioned by repository and full immutable address. Checksums detect
//! accidental corruption; this is not an authentication boundary against a writer
//! with access to the cache directory. Concurrent publishers use atomic replacement.

use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use lore_base::types::RepositoryId;
use ring::digest::SHA256;
use ring::digest::digest;

use crate::digest::ContentIdentity;

const MAGIC: &[u8; 8] = b"LOREDG01";
const ALGORITHM_SHA256: u8 = 1;
const KEY_SIZE: usize = 16 + 16 + 32 + 8 + 1;
const RECORD_SIZE: usize = 8 + KEY_SIZE + 32 + 32;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// An optional cache. Deleting its directory only removes digest availability.
/// The directory must be private to the authorized user and not writable by others.
#[derive(Clone)]
pub struct DigestIndex {
    directory: PathBuf,
}

impl DigestIndex {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    /// Loads a small record, without opening any content store or working-tree path.
    /// Missing, unreadable, oversized, incompatible or corrupt records are cache misses.
    /// Unix records must be regular, owned by this user and not writable by others.
    pub fn lookup(&self, repository: RepositoryId, content: ContentIdentity) -> Option<[u8; 32]> {
        let key = key(repository, content);
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // Never follow a record symlink or block opening a FIFO/device.
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(self.path(&key)).ok()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = file.metadata().ok()?;
            // SAFETY: getuid has no memory-safety preconditions.
            let uid = unsafe { libc::getuid() };
            if !metadata.is_file() || metadata.uid() != uid || metadata.mode() & 0o022 != 0 {
                return None;
            }
        }
        let mut record = Vec::with_capacity(RECORD_SIZE + 1);
        file.take((RECORD_SIZE + 1) as u64)
            .read_to_end(&mut record)
            .ok()?;
        if record.len() != RECORD_SIZE || &record[..8] != MAGIC || record[8..8 + KEY_SIZE] != key {
            return None;
        }
        let checksum_offset = RECORD_SIZE - 32;
        if digest(&SHA256, &record[..checksum_offset]).as_ref() != &record[checksum_offset..] {
            return None;
        }
        record[8 + KEY_SIZE..checksum_offset].try_into().ok()
    }

    /// Cache records supplied by Lore's successful commit boundary. Ingestion alone
    /// does not supply this notification, so failed and dry-run commits cannot publish.
    pub fn publish_committed(
        &self,
        digests: &lore_revision::fs::filesystem_provider::CommittedContentDigests,
    ) -> io::Result<()> {
        for content in digests.content() {
            self.publish(
                digests.repository(),
                ContentIdentity {
                    address: content.address(),
                    size: content.size(),
                },
                content.sha256(),
            )?;
        }
        Ok(())
    }

    // Only verified whole-content readers may publish records.
    pub(crate) fn publish(
        &self,
        repository: RepositoryId,
        content: ContentIdentity,
        sha256: [u8; 32],
    ) -> io::Result<()> {
        let mut directory = fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(&self.directory)?;
        let key = key(repository, content);
        let mut record = Vec::with_capacity(RECORD_SIZE);
        record.extend_from_slice(MAGIC);
        record.extend_from_slice(&key);
        record.extend_from_slice(&sha256);
        let checksum = digest(&SHA256, &record);
        record.extend_from_slice(checksum.as_ref());
        let (temporary, mut file) = loop {
            let path = self.directory.join(format!(
                ".digest-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => break (path, file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        };
        let cleanup = Temporary(temporary);
        file.write_all(&record)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&cleanup.0, self.path(&key))?;
        // This cache is disposable: a lost rename after a crash is a safe miss.
        Ok(())
    }

    fn path(&self, key: &[u8; KEY_SIZE]) -> PathBuf {
        let hash = digest(&SHA256, key);
        let mut name = String::with_capacity(64);
        use std::fmt::Write as _;
        for byte in hash.as_ref() {
            write!(name, "{byte:02x}").expect("writing to a string");
        }
        self.directory.join(name)
    }
}

fn key(repository: RepositoryId, content: ContentIdentity) -> [u8; KEY_SIZE] {
    let mut key = [0; KEY_SIZE];
    key[..16].copy_from_slice(repository.data());
    key[16..32].copy_from_slice(content.address.context.data());
    key[32..64].copy_from_slice(content.address.hash.data());
    key[64..72].copy_from_slice(&content.size.to_le_bytes());
    key[72] = ALGORITHM_SHA256;
    key
}

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use lore_base::types::Address;

    use super::*;

    pub(crate) struct Directory(pub PathBuf);
    impl Directory {
        pub(crate) fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "lore-digest-test-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn persistent_records_are_isolated_and_corruption_is_a_miss() {
        let directory = Directory::new();
        let index = DigestIndex::new(&directory.0);
        let repository = RepositoryId::from([1; 16]);
        let content = ContentIdentity {
            address: Address::default(),
            size: 7,
        };
        assert_eq!(index.lookup(repository, content), None);
        index.publish(repository, content, [42; 32]).unwrap();
        let index = DigestIndex::new(&directory.0);
        assert_eq!(index.lookup(repository, content), Some([42; 32]));
        assert_eq!(index.lookup(RepositoryId::from([2; 16]), content), None);
        let mut changed = content;
        changed.size += 1;
        assert_eq!(index.lookup(repository, changed), None);
        changed = content;
        changed.address.context = [1; 16].into();
        assert_eq!(index.lookup(repository, changed), None);
        changed = content;
        changed.address.hash = [1; 32].into();
        assert_eq!(index.lookup(repository, changed), None);
        let path = index.path(&key(repository, content));
        let valid = fs::read(&path).unwrap();
        let changed_path = index.path(&key(repository, changed));
        fs::write(&changed_path, &valid).unwrap();
        assert_eq!(index.lookup(repository, changed), None);
        fs::remove_file(changed_path).unwrap();
        // Even a valid checksum cannot make a different algorithm compatible.
        let mut unsupported = valid.clone();
        unsupported[8 + KEY_SIZE - 1] = 2;
        let checksum = digest(&SHA256, &unsupported[..RECORD_SIZE - 32]);
        unsupported[RECORD_SIZE - 32..].copy_from_slice(checksum.as_ref());
        fs::write(&path, unsupported).unwrap();
        assert_eq!(index.lookup(repository, content), None);
        for offset in [0, 8, 8 + KEY_SIZE, RECORD_SIZE - 1] {
            let mut corrupt = valid.clone();
            corrupt[offset] ^= 1;
            fs::write(&path, corrupt).unwrap();
            assert_eq!(index.lookup(repository, content), None);
        }
        fs::write(&path, &valid[..RECORD_SIZE - 1]).unwrap();
        assert_eq!(index.lookup(repository, content), None);
        fs::write(&path, vec![0; RECORD_SIZE + 1]).unwrap();
        assert_eq!(index.lookup(repository, content), None);
        fs::remove_file(path).unwrap();
        assert_eq!(index.lookup(repository, content), None);
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_permissions_symlinks_and_nonregular_records_are_safe_misses() {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::fs::symlink;
        let directory = Directory::new();
        let index = DigestIndex::new(&directory.0);
        let repository = RepositoryId::from([1; 16]);
        let content = ContentIdentity {
            address: Address::default(),
            size: 7,
        };
        index.publish(repository, content, [42; 32]).unwrap();
        let path = index.path(&key(repository, content));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert_eq!(index.lookup(repository, content), None);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(index.lookup(repository, content), Some([42; 32]));
        let target = directory.0.join("valid-record");
        fs::rename(&path, &target).unwrap();
        symlink(&target, &path).unwrap();
        assert_eq!(index.lookup(repository, content), None);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert_eq!(index.lookup(repository, content), None);
        fs::remove_dir(&path).unwrap();
        let fifo = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: the CString is NUL-terminated and remains valid for the call.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert_eq!(index.lookup(repository, content), None);
    }

    #[test]
    fn concurrent_publishers_leave_a_complete_record() {
        let directory = Directory::new();
        let index = DigestIndex::new(&directory.0);
        let repository = RepositoryId::from([1; 16]);
        let content = ContentIdentity {
            address: Address::default(),
            size: 0,
        };
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let index = &index;
                scope.spawn(move || index.publish(repository, content, [3; 32]).unwrap());
            }
        });
        assert_eq!(index.lookup(repository, content), Some([3; 32]));
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
    }
}
