use storage::StorageError;
use thiserror::Error;

#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KinesisError {
    #[error("{message}")]
    ExpiredNextToken { message: String },
    #[error("{message}")]
    InternalFailure { message: String },
    #[error("{message}")]
    InvalidArgument { message: String },
    #[error("{message}")]
    ResourceInUse { message: String },
    #[error("{message}")]
    ResourceNotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
}

impl KinesisError {
    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ExpiredNextToken { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ExpiredNextTokenException",
                message,
                400,
                true,
            )
            .expect("Kinesis ExpiredNextTokenException must build"),
            Self::InternalFailure { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalFailureException",
                message,
                500,
                false,
            )
            .expect("Kinesis InternalFailureException must build"),
            Self::InvalidArgument { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidArgumentException",
                message,
                400,
                true,
            )
            .expect("Kinesis InvalidArgumentException must build"),
            Self::ResourceInUse { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "ResourceInUseException",
                message,
                400,
                true,
            )
            .expect("Kinesis ResourceInUseException must build"),
            Self::ResourceNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message,
                400,
                true,
            )
            .expect("Kinesis ResourceNotFoundException must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            )
            .expect("Kinesis UnsupportedOperation must build"),
        }
    }
}

pub(crate) fn storage_error(
    action: &str,
    source: StorageError,
) -> KinesisError {
    KinesisError::InternalFailure {
        message: format!("Failed while {action}: {source}"),
    }
}
