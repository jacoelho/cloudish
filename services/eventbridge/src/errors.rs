use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventBridgeError {
    #[error("{message}")]
    ConcurrentModification { message: String },
    #[error("{message}")]
    InternalFailure { message: String },
    #[error("{message}")]
    ResourceAlreadyExists { message: String },
    #[error("{message}")]
    ResourceNotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    Validation { message: String },
}

pub(crate) fn storage_error(
    action: &str,
    source: StorageError,
) -> EventBridgeError {
    EventBridgeError::InternalFailure {
        message: format!("Failed while {action}: {source}"),
    }
}
