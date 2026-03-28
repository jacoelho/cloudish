use aws::InfrastructureError;
use storage::StorageError;
use thiserror::Error;

#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};

#[derive(Debug, Error)]
pub enum StepFunctionsError {
    #[error("{message}")]
    ExecutionAlreadyExists { message: String },
    #[error("{message}")]
    ExecutionDoesNotExist { message: String },
    #[error("{message}")]
    InvalidArn { message: String },
    #[error("{message}")]
    InvalidDefinition { message: String },
    #[error("{message}")]
    InvalidExecutionInput { message: String },
    #[error("{message}")]
    InvalidName { message: String },
    #[error("{message}")]
    UnsupportedStateMachineType { message: String },
    #[error("{message}")]
    StateMachineAlreadyExists { message: String },
    #[error("{message}")]
    StateMachineDoesNotExist { message: String },
    #[error("{message}")]
    Validation { message: String },
    #[error(transparent)]
    Infrastructure(#[from] InfrastructureError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl StepFunctionsError {
    #[cfg(test)]
    #[allow(dead_code)]
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ExecutionAlreadyExists { message } => custom_error(
                AwsErrorFamily::AlreadyExists,
                "ExecutionAlreadyExists",
                message,
                400,
                true,
            ),
            Self::ExecutionDoesNotExist { message } => custom_error(
                AwsErrorFamily::NotFound,
                "ExecutionDoesNotExist",
                message,
                400,
                true,
            ),
            Self::InvalidArn { message } => custom_error(
                AwsErrorFamily::Validation,
                "InvalidArn",
                message,
                400,
                true,
            ),
            Self::InvalidDefinition { message } => custom_error(
                AwsErrorFamily::Validation,
                "InvalidDefinition",
                message,
                400,
                true,
            ),
            Self::InvalidExecutionInput { message } => custom_error(
                AwsErrorFamily::Validation,
                "InvalidExecutionInput",
                message,
                400,
                true,
            ),
            Self::InvalidName { message } => custom_error(
                AwsErrorFamily::Validation,
                "InvalidName",
                message,
                400,
                true,
            ),
            Self::UnsupportedStateMachineType { message }
            | Self::Validation { message } => custom_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message,
                400,
                true,
            ),
            Self::StateMachineAlreadyExists { message } => custom_error(
                AwsErrorFamily::AlreadyExists,
                "StateMachineAlreadyExists",
                message,
                400,
                true,
            ),
            Self::StateMachineDoesNotExist { message } => custom_error(
                AwsErrorFamily::NotFound,
                "StateMachineDoesNotExist",
                message,
                400,
                true,
            ),
            Self::Infrastructure(error) => custom_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                &error.to_string(),
                500,
                false,
            ),
            Self::Storage(error) => custom_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                &error.to_string(),
                500,
                false,
            ),
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn custom_error(
    family: AwsErrorFamily,
    code: &str,
    message: &str,
    status_code: u16,
    sender_fault: bool,
) -> AwsError {
    AwsError::custom(family, code, message, status_code, sender_fault)
        .expect("step functions errors must be valid")
}
