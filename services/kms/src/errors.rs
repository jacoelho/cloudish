#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KmsError {
    #[error("{message}")]
    AlreadyExistsException { message: String },
    #[error("{message}")]
    InternalFailure { message: String },
    #[error("{message}")]
    InvalidArnException { message: String },
    #[error("{message}")]
    InvalidCiphertextException { message: String },
    #[error("{message}")]
    InvalidKeyUsageException { message: String },
    #[error("{message}")]
    InvalidSignatureException { message: String },
    #[error("{message}")]
    IncorrectKeyException { message: String },
    #[error("{message}")]
    KmsInvalidStateException { message: String },
    #[error("{message}")]
    NotFoundException { message: String },
    #[error("{message}")]
    TagException { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    ValidationException { message: String },
}

impl KmsError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::AlreadyExistsException { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "AlreadyExistsException",
                message,
                400,
                true,
            )
            .expect("KMS AlreadyExistsException must build"),
            Self::InternalFailure { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "KMSInternalException",
                message,
                500,
                false,
            )
            .expect("KMSInternalException must build"),
            Self::InvalidArnException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidArnException",
                message,
                400,
                true,
            )
            .expect("KMS InvalidArnException must build"),
            Self::InvalidCiphertextException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidCiphertextException",
                message,
                400,
                true,
            )
            .expect("KMS InvalidCiphertextException must build"),
            Self::InvalidKeyUsageException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidKeyUsageException",
                message,
                400,
                true,
            )
            .expect("KMS InvalidKeyUsageException must build"),
            Self::InvalidSignatureException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "KMSInvalidSignatureException",
                message,
                400,
                true,
            )
            .expect("KMSInvalidSignatureException must build"),
            Self::IncorrectKeyException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "IncorrectKeyException",
                message,
                400,
                true,
            )
            .expect("KMS IncorrectKeyException must build"),
            Self::KmsInvalidStateException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "KMSInvalidStateException",
                message,
                400,
                true,
            )
            .expect("KMSInvalidStateException must build"),
            Self::NotFoundException { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NotFoundException",
                message,
                400,
                true,
            )
            .expect("KMS NotFoundException must build"),
            Self::TagException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "TagException",
                message,
                400,
                true,
            )
            .expect("KMS TagException must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            )
            .expect("KMS UnsupportedOperation must build"),
            Self::ValidationException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationException",
                message,
                400,
                true,
            )
            .expect("KMS ValidationException must build"),
        }
    }
}

impl From<StorageError> for KmsError {
    fn from(error: StorageError) -> Self {
        Self::InternalFailure { message: error.to_string() }
    }
}
