// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::fs::File;

/// Releases ownership even if a child temporarily retains the open file
/// description between fork and exec. Closing our descriptor alone would keep
/// an advisory lock held until the child's inherited descriptor is closed.
pub(crate) struct FileLock(File);

impl FileLock {
    #[cfg(unix)]
    pub(crate) fn try_lock(file: File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock()?;
        Ok(Self(file))
    }

    #[cfg(unix)]
    pub(crate) fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.0.metadata()
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // Closing the descriptor remains the fallback if explicit unlock fails.
        let _result = self.0.unlock();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn drop_releases_ownership_with_an_inherited_descriptor_alive() {
        let path = std::env::temp_dir().join(format!(
            "lore-file-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = File::create_new(&path).unwrap();
        let inherited = file.try_clone().unwrap();
        let competing = File::options().read(true).write(true).open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let owner = FileLock::try_lock(file).unwrap();
        assert!(owner.metadata().is_ok());
        assert!(matches!(
            FileLock::try_lock(competing.try_clone().unwrap()),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        assert!(matches!(
            competing.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(owner);
        competing.try_lock().unwrap();
        competing.unlock().unwrap();
        drop(inherited);
    }
}
