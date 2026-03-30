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
    pub fn cancellation_reasons(&self) -> Option<&[CancellationReason]> {
        match self {
            Self::TransactionCanceled { cancellation_reasons, .. } => {
                Some(cancellation_reasons)
            }
            _ => None,
        }
    }
}
