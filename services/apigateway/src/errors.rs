#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
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

impl ApiGatewayError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::Conflict { resource } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "ConflictException",
                format!("{resource} already exists."),
                409,
                true,
            )
            .expect("conflict error should build"),
            Self::Internal { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalFailure",
                message.clone(),
                500,
                false,
            )
            .expect("internal error should build"),
            Self::NotFound { resource } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NotFoundException",
                format!("{resource} was not found."),
                404,
                true,
            )
            .expect("not found error should build"),
            Self::Store(source) => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalFailure",
                source.to_string(),
                500,
                false,
            )
            .expect("storage error should build"),
            Self::UnsupportedOperation { message }
            | Self::UnsupportedPatchPath { path: message, .. }
            | Self::Validation { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "BadRequestException",
                message.clone(),
                400,
                true,
            )
            .expect("validation error should build"),
        }
    }
}
