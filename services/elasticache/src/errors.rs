use aws::InfrastructureError;
use storage::StorageError;
use thiserror::Error;

#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ElastiCacheError {
    #[error("{message}")]
    DuplicateUserName { message: String },
    #[error("{message}")]
    InvalidParameterCombination { message: String },
    #[error("{message}")]
    InvalidParameterValue { message: String },
    #[error("{message}")]
    Internal { message: String },
    #[error("{message}")]
    ReplicationGroupAlreadyExists { message: String },
    #[error("{message}")]
    ReplicationGroupNotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    UserAlreadyExists { message: String },
    #[error("{message}")]
    UserNotFound { message: String },
}

impl ElastiCacheError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::DuplicateUserName { .. } => "DuplicateUserName",
            Self::InvalidParameterCombination { .. } => {
                "InvalidParameterCombination"
            }
            Self::InvalidParameterValue { .. } => "InvalidParameterValue",
            Self::Internal { .. } => "InternalFailure",
            Self::ReplicationGroupAlreadyExists { .. } => {
                "ReplicationGroupAlreadyExists"
            }
            Self::ReplicationGroupNotFound { .. } => {
                "ReplicationGroupNotFoundFault"
            }
            Self::UnsupportedOperation { .. } => "UnsupportedOperation",
            Self::UserAlreadyExists { .. } => "UserAlreadyExists",
            Self::UserNotFound { .. } => "UserNotFound",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::DuplicateUserName { message }
            | Self::InvalidParameterCombination { message }
            | Self::InvalidParameterValue { message }
            | Self::Internal { message }
            | Self::ReplicationGroupAlreadyExists { message }
            | Self::ReplicationGroupNotFound { message }
            | Self::UnsupportedOperation { message }
            | Self::UserAlreadyExists { message }
            | Self::UserNotFound { message } => message,
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::Internal { .. } => 500,
            Self::ReplicationGroupNotFound { .. }
            | Self::UserNotFound { .. } => 404,
            Self::DuplicateUserName { .. }
            | Self::InvalidParameterCombination { .. }
            | Self::InvalidParameterValue { .. }
            | Self::ReplicationGroupAlreadyExists { .. }
            | Self::UnsupportedOperation { .. }
            | Self::UserAlreadyExists { .. } => 400,
        }
    }

    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        let family = match self {
            Self::Internal { .. } => AwsErrorFamily::Internal,
            Self::UnsupportedOperation { .. } => {
                AwsErrorFamily::UnsupportedOperation
            }
            _ => AwsErrorFamily::Validation,
        };
        AwsError::custom(
            family,
            self.code(),
            self.message(),
            self.status_code(),
            true,
        )
        .expect("ElastiCache errors must build")
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal { message: message.into() }
    }
}

pub(crate) fn infrastructure_error(
    operation: &str,
    source: InfrastructureError,
) -> ElastiCacheError {
    ElastiCacheError::internal(format!("{operation} failed: {source}"))
}

pub(crate) fn storage_error(
    operation: &str,
    source: StorageError,
) -> ElastiCacheError {
    ElastiCacheError::internal(format!("{operation} failed: {source}"))
}
