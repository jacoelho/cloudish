#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::unwrap_used,
        clippy::panic,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

mod backend;
mod error;
mod factory;
mod hybrid;
mod json_persistent;
mod memory;
mod scheduler;
mod scope;
mod snapshot;
mod wal;

use aws::RuntimeDefaults;
use std::path::{Path, PathBuf};

pub use backend::{
    ScanPredicate, StorageBackend, StorageHandle, StorageKey, StorageValue,
};
pub use error::StorageError;
pub use factory::{
    ServiceStorageConfig, StorageConfig, StorageFactory, StorageMode,
};
pub use hybrid::HybridStorage;
pub use json_persistent::PersistentStorage;
pub use memory::InMemoryStorage;
pub use scheduler::{
    ManualScheduler, ScheduleHandle, Scheduler, ThreadScheduler,
};
pub use scope::{
    ScopedStorage, ScopedStorageKey, StorageRepository, StorageScope,
    StorageScopeProvider,
};
pub use wal::{OP_DELETE, OP_PUT, WalStorage};

#[derive(Debug, Clone)]
pub struct StorageRuntime {
    state_directory: PathBuf,
}

impl StorageRuntime {
    pub fn new(defaults: &RuntimeDefaults) -> Self {
        Self { state_directory: defaults.state_directory().to_path_buf() }
    }

    pub fn state_directory(&self) -> &Path {
        &self.state_directory
    }
}
