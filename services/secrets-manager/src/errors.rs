#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SecretNameError {
    #[error("Secret name must be between 1 and 256 characters long.")]
    Length,
    #[error("Secret name contains invalid characters.")]
    InvalidCharacter,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SecretReferenceError {
    #[error(transparent)]
    InvalidArn(#[from] aws::ArnError),
    #[error("Secret ARNs must include an AWS account and region.")]
    MissingScope,
    #[error("Secret ARNs must reference the secretsmanager service.")]
    InvalidService,
    #[error("Secret ARNs must reference a secret resource.")]
    InvalidResource,
    #[error(transparent)]
    InvalidSecretName(#[from] SecretNameError),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SecretsManagerError {
    #[error("{message}")]
    InternalServiceError { message: String },
    #[error("{message}")]
    InvalidNextTokenException { message: String },
    #[error("{message}")]
    InvalidParameterException { message: String },
    #[error("{message}")]
    InvalidRequestException { message: String },
    #[error("{message}")]
    ResourceExistsException { message: String },
    #[error("{message}")]
    ResourceNotFoundException { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
}

impl SecretsManagerError {
    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InternalServiceError { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalServiceError",
                message.clone(),
                500,
                false,
            )
            .expect("Secrets Manager InternalServiceError must build"),
            Self::InvalidNextTokenException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidNextTokenException",
                message.clone(),
                400,
                true,
            )
            .expect("Secrets Manager InvalidNextTokenException must build"),
            Self::InvalidParameterException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterException",
                message.clone(),
                400,
                true,
            )
            .expect("Secrets Manager InvalidParameterException must build"),
            Self::InvalidRequestException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidRequestException",
                message.clone(),
                400,
                true,
            )
            .expect("Secrets Manager InvalidRequestException must build"),
            Self::ResourceExistsException { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "ResourceExistsException",
                message.clone(),
                400,
                true,
            )
            .expect("Secrets Manager ResourceExistsException must build"),
            Self::ResourceNotFoundException { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                400,
                true,
            )
            .expect("Secrets Manager ResourceNotFoundException must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message.clone(),
                400,
                true,
            )
            .expect("Secrets Manager UnsupportedOperation must build"),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::InternalServiceError { message: message.into() }
    }

    pub(crate) fn invalid_next_token(message: impl Into<String>) -> Self {
        Self::InvalidNextTokenException { message: message.into() }
    }

    pub(crate) fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::InvalidParameterException { message: message.into() }
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequestException { message: message.into() }
    }

    pub(crate) fn resource_exists(message: impl Into<String>) -> Self {
        Self::ResourceExistsException { message: message.into() }
    }

    pub(crate) fn resource_not_found(message: impl Into<String>) -> Self {
        Self::ResourceNotFoundException { message: message.into() }
    }
}

impl From<StorageError> for SecretsManagerError {
    fn from(error: StorageError) -> Self {
        Self::internal(format!(
            "Secrets Manager state operation failed: {error}"
        ))
    }
}
