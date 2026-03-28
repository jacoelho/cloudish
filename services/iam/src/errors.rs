use thiserror::Error;

#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IamError {
    #[error("{message}")]
    DeleteConflict { message: String },
    #[error("{message}")]
    EntityAlreadyExists { message: String },
    #[error("{message}")]
    InvalidInput { message: String },
    #[error("{message}")]
    LimitExceeded { message: String },
    #[error("{message}")]
    MalformedPolicyDocument { message: String },
    #[error("{message}")]
    NoSuchEntity { message: String },
}

impl IamError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::DeleteConflict { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "DeleteConflict",
                message,
                409,
                true,
            ),
            Self::EntityAlreadyExists { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "EntityAlreadyExists",
                message,
                409,
                true,
            ),
            Self::InvalidInput { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidInput",
                message,
                400,
                true,
            ),
            Self::LimitExceeded { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "LimitExceeded",
                message,
                409,
                true,
            ),
            Self::MalformedPolicyDocument { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "MalformedPolicyDocument",
                message,
                400,
                true,
            ),
            Self::NoSuchEntity { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchEntity",
                message,
                404,
                true,
            ),
        }
        .expect("IAM errors must map to valid AWS error shapes")
    }
}
