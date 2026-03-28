use aws::InfrastructureError;
#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
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

impl LambdaError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::Blob(source)
            | Self::InvokeBackend(source)
            | Self::Random(source) => AwsError::custom(
                AwsErrorFamily::Internal,
                "ServiceException",
                source.to_string(),
                500,
                false,
            )
            .expect("lambda infrastructure error should build"),
            Self::Internal { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "ServiceException",
                message.clone(),
                500,
                false,
            )
            .expect("lambda internal error should build"),
            Self::Store(source) => AwsError::custom(
                AwsErrorFamily::Internal,
                "ServiceException",
                source.to_string(),
                500,
                false,
            )
            .expect("lambda storage error should build"),
            Self::AccessDenied { message } => AwsError::custom(
                AwsErrorFamily::AccessDenied,
                "AccessDeniedException",
                message.clone(),
                403,
                true,
            )
            .expect("lambda access denied error should build"),
            Self::InvalidParameterValue { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterValueException",
                message.clone(),
                400,
                true,
            )
            .expect("lambda invalid parameter error should build"),
            Self::InvalidRequestContent { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidRequestContentException",
                message.clone(),
                400,
                true,
            )
            .expect("lambda invalid request content error should build"),
            Self::RequestTooLarge { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "RequestTooLargeException",
                message.clone(),
                413,
                true,
            )
            .expect("lambda request too large error should build"),
            Self::ResourceConflict { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "ResourceConflictException",
                message.clone(),
                409,
                true,
            )
            .expect("lambda conflict error should build"),
            Self::ResourceNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                404,
                true,
            )
            .expect("lambda not found error should build"),
            Self::PreconditionFailed { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "PreconditionFailedException",
                message.clone(),
                412,
                true,
            )
            .expect("lambda precondition failed error should build"),
            Self::UnsupportedMediaType { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "UnsupportedMediaTypeException",
                message.clone(),
                415,
                true,
            )
            .expect("lambda unsupported media type error should build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperationException",
                message.clone(),
                400,
                true,
            )
            .expect("lambda unsupported operation error should build"),
            Self::Validation { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            )
            .expect("lambda validation error should build"),
        }
    }
}
