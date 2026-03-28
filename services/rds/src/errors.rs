use aws::InfrastructureError;
#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RdsError {
    #[error("{message}")]
    DbClusterAlreadyExistsFault { message: String },
    #[error("{message}")]
    DbClusterNotFoundFault { message: String },
    #[error("{message}")]
    DbInstanceAlreadyExists { message: String },
    #[error("{message}")]
    DbInstanceNotFound { message: String },
    #[error("{message}")]
    DbParameterGroupAlreadyExists { message: String },
    #[error("{message}")]
    DbParameterGroupNotFound { message: String },
    #[error("{message}")]
    InternalFailure { message: String },
    #[error("{message}")]
    InvalidDbClusterStateFault { message: String },
    #[error("{message}")]
    InvalidDbInstanceState { message: String },
    #[error("{message}")]
    InvalidParameterCombination { message: String },
    #[error("{message}")]
    InvalidParameterValue { message: String },
}

impl RdsError {
    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::DbClusterAlreadyExistsFault { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "DBClusterAlreadyExistsFault",
                message,
                400,
                true,
            )
            .expect("RDS DBClusterAlreadyExistsFault must build"),
            Self::DbClusterNotFoundFault { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "DBClusterNotFoundFault",
                message,
                404,
                true,
            )
            .expect("RDS DBClusterNotFoundFault must build"),
            Self::DbInstanceAlreadyExists { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "DBInstanceAlreadyExists",
                message,
                400,
                true,
            )
            .expect("RDS DBInstanceAlreadyExists must build"),
            Self::DbInstanceNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "DBInstanceNotFound",
                message,
                404,
                true,
            )
            .expect("RDS DBInstanceNotFound must build"),
            Self::DbParameterGroupAlreadyExists { message } => {
                AwsError::custom(
                    AwsErrorFamily::AlreadyExists,
                    "DBParameterGroupAlreadyExists",
                    message,
                    400,
                    true,
                )
                .expect("RDS DBParameterGroupAlreadyExists must build")
            }
            Self::DbParameterGroupNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "DBParameterGroupNotFound",
                message,
                404,
                true,
            )
            .expect("RDS DBParameterGroupNotFound must build"),
            Self::InternalFailure { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalFailure",
                message,
                500,
                false,
            )
            .expect("RDS InternalFailure must build"),
            Self::InvalidDbClusterStateFault { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "InvalidDBClusterStateFault",
                message,
                400,
                true,
            )
            .expect("RDS InvalidDBClusterStateFault must build"),
            Self::InvalidDbInstanceState { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "InvalidDBInstanceState",
                message,
                400,
                true,
            )
            .expect("RDS InvalidDBInstanceState must build"),
            Self::InvalidParameterCombination { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterCombination",
                message,
                400,
                true,
            )
            .expect("RDS InvalidParameterCombination must build"),
            Self::InvalidParameterValue { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message,
                400,
                true,
            )
            .expect("RDS InvalidParameterValue must build"),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::InternalFailure { message: message.into() }
    }
}

pub(crate) fn storage_error(
    operation: &str,
    source: StorageError,
) -> RdsError {
    RdsError::internal(format!(
        "Unexpected storage error while {operation}: {source}"
    ))
}

pub(crate) fn infrastructure_error(
    operation: &str,
    source: InfrastructureError,
) -> RdsError {
    RdsError::internal(format!(
        "Unexpected infrastructure error while {operation}: {source}"
    ))
}
