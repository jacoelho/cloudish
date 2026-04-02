#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SqsError {
    #[error("unsupported attribute {attribute_name}")]
    InvalidAttributeName { attribute_name: String },
    #[error("{message}")]
    InvalidAddress { message: String },
    #[error("{message}")]
    BatchEntryIdsNotDistinct { message: String },
    #[error("{message}")]
    BatchRequestTooLong { message: String },
    #[error("{message}")]
    EmptyBatchRequest { message: String },
    #[error("{message}")]
    InvalidBatchEntryId { message: String },
    #[error("{message}")]
    InvalidMessageContents { message: String },
    #[error("{message}")]
    InvalidParameterValue { message: String },
    #[error("{message}")]
    MissingParameter { message: String },
    #[error("{message}")]
    OverLimit { message: String },
    #[error("{message}")]
    PurgeQueueInProgress { message: String },
    #[error("{message}")]
    QueueAlreadyExists { message: String },
    #[error("The specified queue does not exist.")]
    QueueDoesNotExist,
    #[error(
        "The input receipt handle \"{receipt_handle}\" is not a valid receipt handle."
    )]
    ReceiptHandleIsInvalid { receipt_handle: String },
    #[error(
        "Value {receipt_handle} for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change."
    )]
    InvalidReceiptHandleForVisibility { receipt_handle: String },
    #[error("{message}")]
    MessageNotInflight { message: String },
    #[error("{message}")]
    ResourceNotFound { message: String },
    #[error("{message}")]
    UnsupportedOperation { message: String },
}

impl SqsError {
    #[cfg(test)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InvalidAttributeName { attribute_name } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidAttributeName",
                format!("Unknown Attribute {attribute_name}."),
                400,
                true,
            )
            .expect("SQS InvalidAttributeName must build"),
            Self::InvalidAddress { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidAddress",
                message.clone(),
                400,
                true,
            )
            .expect("SQS InvalidAddress must build"),
            Self::BatchEntryIdsNotDistinct { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                message.clone(),
                400,
                true,
            )
            .expect("SQS BatchEntryIdsNotDistinct must build"),
            Self::BatchRequestTooLong { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "BatchRequestTooLong",
                message.clone(),
                400,
                true,
            )
            .expect("SQS BatchRequestTooLong must build"),
            Self::EmptyBatchRequest { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                message.clone(),
                400,
                true,
            )
            .expect("SQS EmptyBatchRequest must build"),
            Self::InvalidBatchEntryId { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.InvalidBatchEntryId",
                message.clone(),
                400,
                true,
            )
            .expect("SQS InvalidBatchEntryId must build"),
            Self::InvalidMessageContents { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidMessageContents",
                message.clone(),
                400,
                true,
            )
            .expect("SQS InvalidMessageContents must build"),
            Self::InvalidParameterValue { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message.clone(),
                400,
                true,
            )
            .expect("SQS InvalidParameterValue must build"),
            Self::MissingParameter { message } => AwsError::custom(
                AwsErrorFamily::MissingParameter,
                "MissingParameter",
                message.clone(),
                400,
                true,
            )
            .expect("SQS MissingParameter must build"),
            Self::OverLimit { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "OverLimit",
                message.clone(),
                400,
                true,
            )
            .expect("SQS OverLimit must build"),
            Self::PurgeQueueInProgress { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.PurgeQueueInProgress",
                message.clone(),
                400,
                true,
            )
            .expect("SQS PurgeQueueInProgress must build"),
            Self::QueueAlreadyExists { message } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "QueueAlreadyExists",
                message.clone(),
                400,
                true,
            )
            .expect("SQS QueueAlreadyExists must build"),
            Self::QueueDoesNotExist => AwsError::custom(
                AwsErrorFamily::NotFound,
                "AWS.SimpleQueueService.NonExistentQueue",
                "The specified queue does not exist.",
                400,
                true,
            )
            .expect("SQS NonExistentQueue must build"),
            Self::ReceiptHandleIsInvalid { receipt_handle } => {
                AwsError::custom(
                    AwsErrorFamily::Validation,
                    "ReceiptHandleIsInvalid",
                    format!(
                        "The input receipt handle \"{receipt_handle}\" is not a valid receipt handle."
                    ),
                    400,
                    true,
                )
                .expect("SQS ReceiptHandleIsInvalid must build")
            }
            Self::InvalidReceiptHandleForVisibility { receipt_handle } => {
                AwsError::custom(
                    AwsErrorFamily::Validation,
                    "InvalidParameterValue",
                    format!(
                        "Value {receipt_handle} for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change."
                    ),
                    400,
                    true,
                )
                .expect("SQS invalid visibility receipt handle must build")
            }
            Self::ResourceNotFound { message } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                400,
                true,
            )
            .expect("SQS ResourceNotFoundException must build"),
            Self::MessageNotInflight { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "MessageNotInflight",
                message.clone(),
                400,
                true,
            )
            .expect("SQS MessageNotInflight must build"),
            Self::UnsupportedOperation { message } => AwsError::custom(
                AwsErrorFamily::UnsupportedOperation,
                "AWS.SimpleQueueService.UnsupportedOperation",
                message.clone(),
                400,
                true,
            )
            .expect("SQS UnsupportedOperation must build"),
        }
    }
}
