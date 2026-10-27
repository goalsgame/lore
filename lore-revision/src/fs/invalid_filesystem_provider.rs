use std::sync::Arc;

use async_trait::async_trait;

use crate::fs::filesystem_provider::FilesystemProvider;
use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperationImpl;

pub struct InvalidFilesystemProvider {
    revision_replacement: bool,
}

impl InvalidFilesystemProvider {
    pub fn new() -> Arc<Self> {
        Self::with_revision_replacement(true)
    }
    pub(crate) fn with_revision_replacement(revision_replacement: bool) -> Arc<Self> {
        Arc::new(Self {
            revision_replacement,
        })
    }
}

#[async_trait]
impl FilesystemProvider for InvalidFilesystemProvider {
    fn supports_revision_replacement(&self) -> bool {
        self.revision_replacement
    }
    async fn begin_operation(&self) -> Result<Arc<InstanceOperationImpl>, FsError> {
        Err(FsError::internal(
            "Attempting a filesystem operation on an invalid filesystem provider",
        ))
    }
}
