#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SsmError {
    #[error(
        "The parameter already exists. To overwrite this value, set the overwrite option in the request to true."
    )]
    ParameterAlreadyExists,
    #[error("The parameter couldn't be found. Verify the name and try again.")]
    ParameterNotFound,
    #[error(
        "The specified parameter version wasn't found. Verify the parameter name and version, and try again."
    )]
    ParameterVersionNotFound,
    #[error(
        "The resource ID isn't valid. Verify that you entered the correct ID and try again."
    )]
    InvalidResourceId,
    #[error(
        "The resource type isn't valid. For example, if you are attempting to tag an EC2 instance, the instance must be a registered managed node."
    )]
    InvalidResourceType,
    #[error("A parameter version can have a maximum of ten labels.")]
    ParameterVersionLabelLimitExceeded,
    #[error("The parameter type isn't supported.")]
    UnsupportedParameterType,
    #[error("{message}")]
    ValidationException { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    InternalFailure { message: String },
}

impl SsmError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ParameterAlreadyExists => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "ParameterAlreadyExists",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM ParameterAlreadyExists must build"),
            Self::ParameterNotFound => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ParameterNotFound",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM ParameterNotFound must build"),
            Self::ParameterVersionNotFound => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ParameterVersionNotFound",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM ParameterVersionNotFound must build"),
            Self::InvalidResourceId => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidResourceId",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM InvalidResourceId must build"),
            Self::InvalidResourceType => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidResourceType",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM InvalidResourceType must build"),
            Self::ParameterVersionLabelLimitExceeded => AwsError::custom(
                AwsErrorFamily::Validation,
                "ParameterVersionLabelLimitExceeded",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM ParameterVersionLabelLimitExceeded must build"),
            Self::UnsupportedParameterType => AwsError::custom(
                AwsErrorFamily::Validation,
                "UnsupportedParameterType",
                self.to_string(),
                400,
                true,
            )
            .expect("SSM UnsupportedParameterType must build"),
            Self::ValidationException { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            )
            .expect("SSM ValidationException must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message.clone(),
                400,
                true,
            )
            .expect("SSM UnsupportedOperation must build"),
            Self::InternalFailure { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalServerError",
                message.clone(),
                500,
                false,
            )
            .expect("SSM InternalServerError must build"),
        }
    }
}

impl From<StorageError> for SsmError {
    fn from(error: StorageError) -> Self {
        Self::InternalFailure {
            message: format!("SSM state operation failed: {error}"),
        }
    }
}
