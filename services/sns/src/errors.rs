#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SnsError {
    #[error("{message}")]
    InvalidParameter { message: String },
    #[error("{message}")]
    MissingParameter { message: String },
    #[error("{message}")]
    NotFound { message: String },
    #[error("{message}")]
    ResourceNotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
}

impl SnsError {
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidParameter { .. } => "InvalidParameter",
            Self::MissingParameter { .. } => "MissingParameter",
            Self::NotFound { .. } => "NotFound",
            Self::ResourceNotFound { .. } => "ResourceNotFound",
            Self::UnsupportedOperation { .. } => "UnsupportedOperation",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidParameter { message }
            | Self::MissingParameter { message }
            | Self::NotFound { message }
            | Self::ResourceNotFound { message }
            | Self::UnsupportedOperation { message } => message,
        }
    }

    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InvalidParameter { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                self.code(),
                message,
                400,
                true,
            )
            .expect("SNS InvalidParameter must build"),
            Self::MissingParameter { message } => AwsError::custom(
                AwsErrorFamily::MissingParameter,
                self.code(),
                message,
                400,
                true,
            )
            .expect("SNS MissingParameter must build"),
            Self::NotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                self.code(),
                message,
                404,
                true,
            )
            .expect("SNS NotFound must build"),
            Self::ResourceNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                self.code(),
                message,
                404,
                true,
            )
            .expect("SNS ResourceNotFound must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                self.code(),
                message,
                400,
                true,
            )
            .expect("SNS UnsupportedOperation must build"),
        }
    }
}
