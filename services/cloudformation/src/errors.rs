use thiserror::Error;

#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CloudFormationError {
    #[error("{message}")]
    AlreadyExists { message: String },
    #[error("{message}")]
    ChangeSetNotFound { message: String },
    #[error("{message}")]
    InsufficientCapabilities { message: String },
    #[error("{message}")]
    NotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    Validation { message: String },
}

impl CloudFormationError {
    #[cfg(test)]
    #[allow(dead_code)]
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::AlreadyExists { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "AlreadyExists",
                message,
                400,
                true,
            )
            .expect("CloudFormation AlreadyExists must build"),
            Self::ChangeSetNotFound { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ChangeSetNotFound",
                message,
                400,
                true,
            )
            .expect("CloudFormation ChangeSetNotFound must build"),
            Self::InsufficientCapabilities { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InsufficientCapabilities",
                message,
                400,
                true,
            )
            .expect("CloudFormation InsufficientCapabilities must build"),
            Self::NotFound { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationError",
                message,
                400,
                true,
            )
            .expect("CloudFormation ValidationError must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            )
            .expect("CloudFormation UnsupportedOperation must build"),
            Self::Validation { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationError",
                message,
                400,
                true,
            )
            .expect("CloudFormation ValidationError must build"),
        }
    }
}
