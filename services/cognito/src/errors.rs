use storage::StorageError;
use thiserror::Error;

#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};

#[derive(Debug, Error)]
pub enum CognitoError {
    #[error("{message}")]
    CodeMismatchException { message: String },
    #[error("{message}")]
    InvalidParameterException { message: String },
    #[error("{message}")]
    NotAuthorizedException { message: String },
    #[error("{message}")]
    ResourceNotFoundException { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    UserNotConfirmedException { message: String },
    #[error("{message}")]
    UserNotFoundException { message: String },
    #[error("{message}")]
    UsernameExistsException { message: String },
    #[error("failed to persist Cognito state: {0}")]
    Store(#[from] StorageError),
}

impl CognitoError {
    pub fn code_mismatch(message: impl Into<String>) -> Self {
        Self::CodeMismatchException { message: message.into() }
    }

    pub fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::InvalidParameterException { message: message.into() }
    }

    pub fn not_authorized(message: impl Into<String>) -> Self {
        Self::NotAuthorizedException { message: message.into() }
    }

    pub fn resource_not_found(message: impl Into<String>) -> Self {
        Self::ResourceNotFoundException { message: message.into() }
    }

    pub fn unsupported_operation(message: impl Into<String>) -> Self {
        Self::UnsupportedOperation { message: message.into() }
    }

    pub fn user_not_confirmed(message: impl Into<String>) -> Self {
        Self::UserNotConfirmedException { message: message.into() }
    }

    pub fn user_not_found(message: impl Into<String>) -> Self {
        Self::UserNotFoundException { message: message.into() }
    }

    pub fn username_exists(message: impl Into<String>) -> Self {
        Self::UsernameExistsException { message: message.into() }
    }

    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::CodeMismatchException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "CodeMismatchException",
                message,
                400,
                true,
            ),
            Self::InvalidParameterException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterException",
                message,
                400,
                true,
            ),
            Self::NotAuthorizedException { message } => AwsError::custom(
                AwsErrorFamily::AccessDenied,
                "NotAuthorizedException",
                message,
                400,
                true,
            ),
            Self::ResourceNotFoundException { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message,
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            ),
            Self::UserNotConfirmedException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "UserNotConfirmedException",
                message,
                400,
                true,
            ),
            Self::UserNotFoundException { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "UserNotFoundException",
                message,
                400,
                true,
            ),
            Self::UsernameExistsException { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "UsernameExistsException",
                message,
                400,
                true,
            ),
            Self::Store(error) => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalErrorException",
                format!("Failed to persist Cognito state: {error}"),
                500,
                false,
            ),
        }
        .expect("Cognito errors should map to valid AWS errors")
    }
}
