use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CloudFormationError {
    #[error("{message}")]
    AlreadyExists { message: String },
    #[error("{message}")]
    ChangeSetNotFound { message: String },
    #[error("{message}")]
    InsufficientCapabilities { message: String },
    #[error("{message}")]
    NotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
    #[error("{message}")]
    Validation { message: String },
}
