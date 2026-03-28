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
