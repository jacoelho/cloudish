use aws::InfrastructureError;
use storage::StorageError;
use thiserror::Error;

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
