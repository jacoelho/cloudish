#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

use crate::{scope::TableName, state::CancellationReason};

#[derive(Debug, Error)]
pub enum DynamoDbInitError {
    #[error("failed to load persisted DynamoDB item state: {0}")]
    Items(StorageError),
    #[error("failed to load persisted DynamoDB stream records: {0}")]
    StreamRecords(StorageError),
    #[error("failed to load persisted DynamoDB table definitions: {0}")]
    Tables(StorageError),
}

#[derive(Debug, Error)]
pub enum DynamoDbError {
    #[error("The conditional request failed")]
    ConditionalCheckFailed,
    #[error("Invalid shard iterator")]
    InvalidShardIterator,
    #[error("DynamoDB storage failure while {context}: {source}")]
    Storage {
        context: &'static str,
        #[source]
        source: StorageError,
    },
    #[error("Requested resource not found: {resource}")]
    ResourceNotFound { resource: String },
    #[error("Requested resource not found: Table: {table_name} not found")]
    TableNotFound { table_name: TableName },
    #[error("Table already exists: {table_name}")]
    TableAlreadyExists { table_name: TableName },
    #[error("{message}")]
    TtlTransitionInProgress { message: String },
    #[error("{message}")]
    TransactionCanceled {
        cancellation_reasons: Vec<CancellationReason>,
        message: String,
    },
    #[error("{message}")]
    Validation { message: String },
}

impl DynamoDbError {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ConditionalCheckFailed => AwsError::custom(
                AwsErrorFamily::Validation,
                "ConditionalCheckFailedException",
                "The conditional request failed",
                400,
                true,
            )
            .expect("ConditionalCheckFailedException must build"),
            Self::InvalidShardIterator => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationException",
                "Invalid shard iterator",
                400,
                true,
            )
            .expect("ValidationException must build"),
            Self::Storage { .. } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalFailure",
                "Cloudish failed to persist DynamoDB state.",
                500,
                false,
            )
            .expect("InternalFailure must build"),
            Self::ResourceNotFound { resource } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                format!("Requested resource not found: {resource}"),
                400,
                true,
            )
            .expect("ResourceNotFoundException must build"),
            Self::TableNotFound { table_name } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                format!(
                    "Requested resource not found: Table: {table_name} not found"
                ),
                400,
                true,
            )
            .expect("ResourceNotFoundException must build"),
            Self::TableAlreadyExists { table_name } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "ResourceInUseException",
                format!("Table already exists: {table_name}"),
                400,
                true,
            )
            .expect("ResourceInUseException must build"),
            Self::TtlTransitionInProgress { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            )
            .expect("ValidationException must build"),
            Self::TransactionCanceled { message, .. } => AwsError::custom(
                AwsErrorFamily::Validation,
                "TransactionCanceledException",
                message.clone(),
                400,
                true,
            )
            .expect("TransactionCanceledException must build"),
            Self::Validation { message } => AwsError::custom(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            )
            .expect("ValidationException must build"),
        }
    }

    pub fn cancellation_reasons(&self) -> Option<&[CancellationReason]> {
        match self {
            Self::TransactionCanceled { cancellation_reasons, .. } => {
                Some(cancellation_reasons)
            }
            _ => None,
        }
    }
}
