// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Live cache keys participate in the namespace's metadata witness. Existing
//! cells retain identity and may be filled while insertion is sealed.
use super::*;

impl Model {
    pub(super) async fn cache_cell(&self, inode: u64) -> Result<FileCell> {
        let mut files = self.files.lock().await;
        if let Some(cell) = files.get(&inode) {
            return Ok(cell.clone());
        }
        if files.len() >= self.namespace.retained_limits().max_inodes {
            return Err(core::Error::ResourceLimit.into());
        }
        // No callback or await while this guard and map lock are held. A failed
        // or cancelled later population retains this announced empty cell.
        let _mutation = self.namespace.file_cache_mutation()?;
        let cell = Arc::new(AsyncMutex::new(None));
        files.insert(inode, cell.clone());
        Ok(cell)
    }
}
