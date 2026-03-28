#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetricNamespaceError {
    #[error("The namespace must not be blank.")]
    Blank,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CloudWatchLogsError {
    #[error("{message}")]
    InvalidNextToken { message: String },
    #[error("{message}")]
    InvalidParameterException { message: String },
    #[error("{message}")]
    InternalFailure { message: String },
    #[error("{message}")]
    ResourceAlreadyExistsException { message: String },
    #[error("{message}")]
    ResourceNotFoundException { message: String },
}

impl CloudWatchLogsError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InvalidNextToken { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidNextToken",
                message,
                400,
                true,
            )
            .expect("CloudWatch Logs InvalidNextToken must build"),
            Self::InvalidParameterException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterException",
                message,
                400,
                true,
            )
            .expect("CloudWatch Logs InvalidParameterException must build"),
            Self::InternalFailure { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalFailure",
                message,
                500,
                false,
            )
            .expect("CloudWatch Logs InternalFailure must build"),
            Self::ResourceAlreadyExistsException { message } => {
                AwsError::custom(
                    AwsErrorFamily::AlreadyExists,
                    "ResourceAlreadyExistsException",
                    message,
                    400,
                    true,
                )
                .expect(
                    "CloudWatch Logs ResourceAlreadyExistsException must build",
                )
            }
            Self::ResourceNotFoundException { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message,
                404,
                true,
            )
            .expect("CloudWatch Logs ResourceNotFoundException must build"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CloudWatchMetricsError {
    #[error("{message}")]
    InternalServiceError { message: String },
    #[error("{message}")]
    InvalidFormat { message: String },
    #[error("{message}")]
    InvalidNextToken { message: String },
    #[error("{message}")]
    InvalidParameterCombination { message: String },
    #[error("{message}")]
    InvalidParameterValue { message: String },
    #[error("{message}")]
    LimitExceeded { message: String },
    #[error("{message}")]
    MissingParameter { message: String },
    #[error("{message}")]
    ResourceNotFound { message: String },
}

impl CloudWatchMetricsError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self, query_mode: bool) -> AwsError {
        match self {
            Self::InternalServiceError { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalServiceError",
                message,
                500,
                false,
            )
            .expect("CloudWatch InternalServiceError must build"),
            Self::InvalidFormat { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidFormat",
                message,
                400,
                true,
            )
            .expect("CloudWatch InvalidFormat must build"),
            Self::InvalidNextToken { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidNextToken",
                message,
                400,
                true,
            )
            .expect("CloudWatch InvalidNextToken must build"),
            Self::InvalidParameterCombination { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterCombination",
                message,
                400,
                true,
            )
            .expect("CloudWatch InvalidParameterCombination must build"),
            Self::InvalidParameterValue { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message,
                400,
                true,
            )
            .expect("CloudWatch InvalidParameterValue must build"),
            Self::LimitExceeded { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "LimitExceeded",
                message,
                400,
                true,
            )
            .expect("CloudWatch LimitExceeded must build"),
            Self::MissingParameter { message } => AwsError::custom(
                AwsErrorFamily::MissingParameter,
                "MissingParameter",
                message,
                400,
                true,
            )
            .expect("CloudWatch MissingParameter must build"),
            Self::ResourceNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFound",
                message,
                if query_mode { 404 } else { 400 },
                true,
            )
            .expect("CloudWatch ResourceNotFound must build"),
        }
    }
}

impl From<StorageError> for CloudWatchLogsError {
    fn from(source: StorageError) -> Self {
        Self::InternalFailure {
            message: format!("CloudWatch Logs storage failure: {source}"),
        }
    }
}

impl From<StorageError> for CloudWatchMetricsError {
    fn from(source: StorageError) -> Self {
        Self::InternalServiceError {
            message: format!("CloudWatch storage failure: {source}"),
        }
    }
}
