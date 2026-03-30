use aws::InfrastructureError;
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LambdaInitError {
    #[error("lambda metadata store failed: {0}")]
    Store(StorageError),
}

#[derive(Debug, Error)]
pub enum LambdaError {
    #[error("{message}")]
    Internal { message: String },
    #[error("{message}")]
    AccessDenied { message: String },
    #[error("{message}")]
    InvalidParameterValue { message: String },
    #[error("{message}")]
    InvalidRequestContent { message: String },
    #[error("lambda blob store failed: {0}")]
    Blob(InfrastructureError),
    #[error("lambda invoke backend failed: {0}")]
    InvokeBackend(InfrastructureError),
    #[error("lambda random source failed: {0}")]
    Random(InfrastructureError),
    #[error("{message}")]
    RequestTooLarge { message: String },
    #[error("{message}")]
    ResourceConflict { message: String },
    #[error("{message}")]
    ResourceNotFound { message: String },
    #[error("{message}")]
    PreconditionFailed { message: String },
    #[error("lambda metadata store failed: {0}")]
    Store(StorageError),
    #[error("{message}")]
    UnsupportedMediaType { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    Validation { message: String },
}
