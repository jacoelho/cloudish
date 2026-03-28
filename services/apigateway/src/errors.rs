use storage::StorageError;
use thiserror::Error;

pub use crate::execute_api::ExecuteApiError;

#[derive(Debug, Error)]
pub enum ApiGatewayError {
    #[error("{resource} already exists")]
    Conflict { resource: String },
    #[error("internal API Gateway failure: {message}")]
    Internal { message: String },
    #[error("{resource} was not found")]
    NotFound { resource: String },
    #[error("failed to persist API Gateway state: {0}")]
    Store(#[source] StorageError),
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("patch path {path:?} is not supported for {resource}")]
    UnsupportedPatchPath { path: String, resource: &'static str },
    #[error("{message}")]
    Validation { message: String },
}
