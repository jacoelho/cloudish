use crate::ApiGatewayError;
use crate::CloudFormationError;
use crate::CognitoError;
use crate::DynamoDbError;
use crate::ElastiCacheError;
use crate::EventBridgeError;
use crate::IamError;
use crate::KinesisError;
use crate::KmsError;
use crate::LambdaError;
use crate::RdsError;
use crate::S3Error;
use crate::SecretsManagerError;
use crate::SnsError;
use crate::SqsError;
use crate::SsmError;
use crate::StepFunctionsError;
use crate::StsError;
use crate::{CloudWatchLogsError, CloudWatchMetricsError};
use aws::{AwsError, AwsErrorFamily};

pub(crate) trait AwsErrorShape {
    fn to_aws_error(&self) -> AwsError;
}

pub(crate) fn trusted_aws_error(
    family: AwsErrorFamily,
    code: impl Into<String>,
    message: impl Into<String>,
    status_code: u16,
    sender_fault: bool,
) -> AwsError {
    AwsError::trusted_custom(family, code, message, status_code, sender_fault)
}
pub(crate) trait QueryModeAwsErrorShape {
    fn to_aws_error(&self, query_mode: bool) -> AwsError;
}
impl AwsErrorShape for ApiGatewayError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::Conflict { resource } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "ConflictException",
                format!("{resource} already exists."),
                409,
                true,
            ),
            Self::Internal { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                message.clone(),
                500,
                false,
            ),
            Self::NotFound { resource } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NotFoundException",
                format!("{resource} was not found."),
                404,
                true,
            ),
            Self::Store(source) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                source.to_string(),
                500,
                false,
            ),
            Self::UnsupportedOperation { message }
            | Self::UnsupportedPatchPath { path: message, .. }
            | Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "BadRequestException",
                message.clone(),
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for CloudFormationError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::AlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "AlreadyExists",
                message,
                400,
                true,
            ),
            Self::ChangeSetNotFound { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ChangeSetNotFound",
                message,
                400,
                true,
            ),
            Self::InsufficientCapabilities { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InsufficientCapabilities",
                message,
                400,
                true,
            ),
            Self::NotFound { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationError",
                message,
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            ),
            Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationError",
                message,
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for EventBridgeError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ConcurrentModification { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "ConcurrentModificationException",
                message,
                400,
                true,
            ),
            Self::InternalFailure { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalException",
                message,
                500,
                false,
            ),
            Self::ResourceAlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "ResourceAlreadyExistsException",
                message,
                400,
                true,
            ),
            Self::ResourceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message,
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperationException",
                message,
                400,
                true,
            ),
            Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message,
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for CloudWatchLogsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InvalidNextToken { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidNextToken",
                message,
                400,
                true,
            ),
            Self::InvalidParameterException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterException",
                message,
                400,
                true,
            ),
            Self::InternalFailure { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                message,
                500,
                false,
            ),
            Self::ResourceAlreadyExistsException { message } => {
                trusted_aws_error(
                    AwsErrorFamily::AlreadyExists,
                    "ResourceAlreadyExistsException",
                    message,
                    400,
                    true,
                )
            }
            Self::ResourceNotFoundException { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message,
                404,
                true,
            ),
        }
    }
}
impl QueryModeAwsErrorShape for CloudWatchMetricsError {
    fn to_aws_error(&self, query_mode: bool) -> AwsError {
        match self {
            Self::InternalServiceError { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalServiceError",
                message,
                500,
                false,
            ),
            Self::InvalidFormat { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidFormat",
                message,
                400,
                true,
            ),
            Self::InvalidNextToken { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidNextToken",
                message,
                400,
                true,
            ),
            Self::InvalidParameterCombination { message } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    "InvalidParameterCombination",
                    message,
                    400,
                    true,
                )
            }
            Self::InvalidParameterValue { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message,
                400,
                true,
            ),
            Self::LimitExceeded { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "LimitExceeded",
                message,
                400,
                true,
            ),
            Self::MissingParameter { message } => trusted_aws_error(
                AwsErrorFamily::MissingParameter,
                "MissingParameter",
                message,
                400,
                true,
            ),
            Self::ResourceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFound",
                message,
                if query_mode { 404 } else { 400 },
                true,
            ),
        }
    }
}
impl AwsErrorShape for CognitoError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::CodeMismatchException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "CodeMismatchException",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidParameterException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterException",
                message.clone(),
                400,
                true,
            ),
            Self::NotAuthorizedException { message } => trusted_aws_error(
                AwsErrorFamily::AccessDenied,
                "NotAuthorizedException",
                message.clone(),
                400,
                true,
            ),
            Self::ResourceNotFoundException { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message.clone(),
                400,
                true,
            ),
            Self::UserNotConfirmedException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "UserNotConfirmedException",
                message.clone(),
                400,
                true,
            ),
            Self::UserNotFoundException { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "UserNotFoundException",
                message.clone(),
                400,
                true,
            ),
            Self::UsernameExistsException { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "UsernameExistsException",
                message.clone(),
                400,
                true,
            ),
            Self::Store(error) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalErrorException",
                format!("Failed to persist Cognito state: {error}"),
                500,
                false,
            ),
        }
    }
}
impl AwsErrorShape for DynamoDbError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ConditionalCheckFailed => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ConditionalCheckFailedException",
                "The conditional request failed",
                400,
                true,
            ),
            Self::InvalidShardIterator => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                "Invalid shard iterator",
                400,
                true,
            ),
            Self::Storage { .. } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                "Cloudish failed to persist DynamoDB state.",
                500,
                false,
            ),
            Self::ResourceNotFound { resource } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                format!("Requested resource not found: {resource}"),
                400,
                true,
            ),
            Self::TableNotFound { table_name } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                format!(
                    "Requested resource not found: Table: {table_name} not found"
                ),
                400,
                true,
            ),
            Self::TableAlreadyExists { table_name } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "ResourceInUseException",
                format!("Table already exists: {table_name}"),
                400,
                true,
            ),
            Self::TtlTransitionInProgress { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            ),
            Self::TransactionCanceled { message, .. } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "TransactionCanceledException",
                message.clone(),
                400,
                true,
            ),
            Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for ElastiCacheError {
    fn to_aws_error(&self) -> AwsError {
        let family = match self {
            Self::Internal { .. } => AwsErrorFamily::Internal,
            Self::UnsupportedOperation { .. } => {
                AwsErrorFamily::UnsupportedOperation
            }
            _ => AwsErrorFamily::Validation,
        };
        trusted_aws_error(
            family,
            self.code(),
            self.message(),
            self.status_code(),
            true,
        )
    }
}

impl AwsErrorShape for IamError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::DeleteConflict { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "DeleteConflict",
                message,
                409,
                true,
            ),
            Self::EntityAlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "EntityAlreadyExists",
                message,
                409,
                true,
            ),
            Self::InvalidInput { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidInput",
                message,
                400,
                true,
            ),
            Self::LimitExceeded { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "LimitExceeded",
                message,
                409,
                true,
            ),
            Self::MalformedPolicyDocument { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "MalformedPolicyDocument",
                message,
                400,
                true,
            ),
            Self::NoSuchEntity { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchEntity",
                message,
                404,
                true,
            ),
        }
    }
}
impl AwsErrorShape for KinesisError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ExpiredNextToken { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ExpiredNextTokenException",
                message,
                400,
                true,
            ),
            Self::InternalFailure { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailureException",
                message,
                500,
                false,
            ),
            Self::InvalidArgument { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidArgumentException",
                message,
                400,
                true,
            ),
            Self::ResourceInUse { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "ResourceInUseException",
                message,
                400,
                true,
            ),
            Self::ResourceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message,
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for KmsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::AlreadyExistsException { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "AlreadyExistsException",
                message,
                400,
                true,
            ),
            Self::InternalFailure { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "KMSInternalException",
                message,
                500,
                false,
            ),
            Self::InvalidArnException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidArnException",
                message,
                400,
                true,
            ),
            Self::InvalidCiphertextException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidCiphertextException",
                message,
                400,
                true,
            ),
            Self::InvalidKeyUsageException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidKeyUsageException",
                message,
                400,
                true,
            ),
            Self::InvalidSignatureException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "KMSInvalidSignatureException",
                message,
                400,
                true,
            ),
            Self::IncorrectKeyException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "IncorrectKeyException",
                message,
                400,
                true,
            ),
            Self::KmsInvalidStateException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "KMSInvalidStateException",
                message,
                400,
                true,
            ),
            Self::NotFoundException { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NotFoundException",
                message,
                400,
                true,
            ),
            Self::TagException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "TagException",
                message,
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message,
                400,
                true,
            ),
            Self::ValidationException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message,
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for LambdaError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::Blob(source)
            | Self::InvokeBackend(source)
            | Self::Random(source) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "ServiceException",
                source.to_string(),
                500,
                false,
            ),
            Self::Internal { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "ServiceException",
                message.clone(),
                500,
                false,
            ),
            Self::Store(source) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "ServiceException",
                source.to_string(),
                500,
                false,
            ),
            Self::AccessDenied { message } => trusted_aws_error(
                AwsErrorFamily::AccessDenied,
                "AccessDeniedException",
                message.clone(),
                403,
                true,
            ),
            Self::InvalidParameterValue { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterValueException",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidRequestContent { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidRequestContentException",
                message.clone(),
                400,
                true,
            ),
            Self::RequestTooLarge { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "RequestTooLargeException",
                message.clone(),
                413,
                true,
            ),
            Self::ResourceConflict { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "ResourceConflictException",
                message.clone(),
                409,
                true,
            ),
            Self::ResourceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                404,
                true,
            ),
            Self::PreconditionFailed { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "PreconditionFailedException",
                message.clone(),
                412,
                true,
            ),
            Self::UnsupportedMediaType { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "UnsupportedMediaTypeException",
                message.clone(),
                415,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperationException",
                message.clone(),
                400,
                true,
            ),
            Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for RdsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::DbClusterAlreadyExistsFault { message } => {
                trusted_aws_error(
                    AwsErrorFamily::AlreadyExists,
                    "DBClusterAlreadyExistsFault",
                    message,
                    400,
                    true,
                )
            }
            Self::DbClusterNotFoundFault { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "DBClusterNotFoundFault",
                message,
                404,
                true,
            ),
            Self::DbInstanceAlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "DBInstanceAlreadyExists",
                message,
                400,
                true,
            ),
            Self::DbInstanceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "DBInstanceNotFound",
                message,
                404,
                true,
            ),
            Self::DbParameterGroupAlreadyExists { message } => {
                trusted_aws_error(
                    AwsErrorFamily::AlreadyExists,
                    "DBParameterGroupAlreadyExists",
                    message,
                    400,
                    true,
                )
            }
            Self::DbParameterGroupNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "DBParameterGroupNotFound",
                message,
                404,
                true,
            ),
            Self::InternalFailure { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                message,
                500,
                false,
            ),
            Self::InvalidDbClusterStateFault { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "InvalidDBClusterStateFault",
                message,
                400,
                true,
            ),
            Self::InvalidDbInstanceState { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "InvalidDBInstanceState",
                message,
                400,
                true,
            ),
            Self::InvalidParameterCombination { message } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    "InvalidParameterCombination",
                    message,
                    400,
                    true,
                )
            }
            Self::InvalidParameterValue { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message,
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for S3Error {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::BucketAlreadyExists { bucket } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "BucketAlreadyExists",
                format!(
                    "The requested bucket name `{bucket}` is not available."
                ),
                409,
                true,
            ),
            Self::BucketAlreadyOwnedByYou { bucket } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "BucketAlreadyOwnedByYou",
                format!(
                    "Your previous request to create the named bucket `{bucket}` succeeded and you already own it."
                ),
                409,
                true,
            ),
            Self::BucketNotEmpty { bucket } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "BucketNotEmpty",
                format!(
                    "The bucket `{bucket}` you tried to delete is not empty."
                ),
                409,
                true,
            ),
            Self::AccessDenied { message } => trusted_aws_error(
                AwsErrorFamily::AccessDenied,
                "AccessDenied",
                message.clone(),
                403,
                true,
            ),
            Self::BucketObjectLockConfigurationNotFound { .. } => {
                trusted_aws_error(
                    AwsErrorFamily::NotFound,
                    "ObjectLockConfigurationNotFoundError",
                    "Object Lock configuration does not exist for this bucket",
                    404,
                    true,
                )
            }
            Self::InvalidArgument { code, message, status_code } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    *code,
                    message.clone(),
                    *status_code,
                    true,
                )
            }
            Self::InvalidBucketState { message } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "InvalidBucketState",
                message.clone(),
                409,
                true,
            ),
            Self::NoSuchBucket { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                404,
                true,
            ),
            Self::NoSuchBucketAcl { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchBucketAcl",
                "The specified bucket ACL does not exist.",
                404,
                true,
            ),
            Self::NoSuchBucketPolicy { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
                404,
                true,
            ),
            Self::NoSuchBucketEncryption { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
                404,
                true,
            ),
            Self::NoSuchCORSConfiguration { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
                404,
                true,
            ),
            Self::NoSuchLifecycleConfiguration { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
                404,
                true,
            ),
            Self::NoSuchKey { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchKey",
                "The specified key does not exist.",
                404,
                true,
            ),
            Self::CurrentVersionIsDeleteMarker { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchKey",
                "The specified key does not exist.",
                404,
                true,
            ),
            Self::RequestedVersionIsDeleteMarker { .. } => trusted_aws_error(
                AwsErrorFamily::Conflict,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
                405,
                true,
            ),
            Self::NoSuchObjectLockConfiguration { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
                404,
                true,
            ),
            Self::NoSuchUpload { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchUpload",
                "The specified multipart upload does not exist.",
                404,
                true,
            ),
            Self::NoSuchVersion { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchVersion",
                "The specified version does not exist.",
                404,
                true,
            ),
            Self::NoSuchTagSet { .. } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "NoSuchTagSet",
                "There is no tag set associated with the bucket.",
                404,
                true,
            ),
            Self::WrongRegion { .. } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "IncorrectEndpoint",
                "The specified bucket exists in another Region. Direct requests to the correct endpoint.",
                400,
                true,
            ),
            Self::Buckets(source)
            | Self::MultipartUploads(source)
            | Self::Objects(source)
            | Self::Sequences(source) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalError",
                format!("The S3 metadata store failed: {source}"),
                500,
                false,
            ),
            Self::Blob(source) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalError",
                format!("The S3 blob store failed: {source}"),
                500,
                false,
            ),
            Self::Internal { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalError",
                message.clone(),
                500,
                false,
            ),
        }
    }
}
impl AwsErrorShape for SecretsManagerError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InternalServiceError { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalServiceError",
                message.clone(),
                500,
                false,
            ),
            Self::InvalidNextTokenException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidNextTokenException",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidParameterException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterException",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidRequestException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidRequestException",
                message.clone(),
                400,
                true,
            ),
            Self::ResourceExistsException { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "ResourceExistsException",
                message.clone(),
                400,
                true,
            ),
            Self::ResourceNotFoundException { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message.clone(),
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for SnsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InvalidParameter { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                self.code(),
                message,
                400,
                true,
            ),
            Self::MissingParameter { message } => trusted_aws_error(
                AwsErrorFamily::MissingParameter,
                self.code(),
                message,
                400,
                true,
            ),
            Self::NotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                self.code(),
                message,
                404,
                true,
            ),
            Self::ResourceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                self.code(),
                message,
                404,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                self.code(),
                message,
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for SqsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::InvalidAttributeName { attribute_name } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    "InvalidAttributeName",
                    format!("Unknown Attribute {attribute_name}."),
                    400,
                    true,
                )
            }
            Self::InvalidAddress { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidAddress",
                message.clone(),
                400,
                true,
            ),
            Self::BatchEntryIdsNotDistinct { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                message.clone(),
                400,
                true,
            ),
            Self::BatchRequestTooLong { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "BatchRequestTooLong",
                message.clone(),
                400,
                true,
            ),
            Self::EmptyBatchRequest { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.EmptyBatchRequest",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidBatchEntryId { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.InvalidBatchEntryId",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidMessageContents { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidMessageContents",
                message.clone(),
                400,
                true,
            ),
            Self::InvalidParameterValue { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message.clone(),
                400,
                true,
            ),
            Self::MissingParameter { message } => trusted_aws_error(
                AwsErrorFamily::MissingParameter,
                "MissingParameter",
                message.clone(),
                400,
                true,
            ),
            Self::OverLimit { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "OverLimit",
                message.clone(),
                400,
                true,
            ),
            Self::PurgeQueueInProgress { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "AWS.SimpleQueueService.PurgeQueueInProgress",
                message.clone(),
                400,
                true,
            ),
            Self::QueueAlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "QueueAlreadyExists",
                message.clone(),
                400,
                true,
            ),
            Self::QueueDoesNotExist => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "AWS.SimpleQueueService.NonExistentQueue",
                "The specified queue does not exist.",
                400,
                true,
            ),
            Self::ReceiptHandleIsInvalid { receipt_handle } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    "ReceiptHandleIsInvalid",
                    format!(
                        "The input receipt handle \"{receipt_handle}\" is not a valid receipt handle."
                    ),
                    400,
                    true,
                )
            }
            Self::InvalidReceiptHandleForVisibility { receipt_handle } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    "InvalidParameterValue",
                    format!(
                        "Value {receipt_handle} for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change."
                    ),
                    400,
                    true,
                )
            }
            Self::ResourceNotFound { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ResourceNotFoundException",
                message.clone(),
                400,
                true,
            ),
            Self::MessageNotInflight { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "MessageNotInflight",
                message.clone(),
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "AWS.SimpleQueueService.UnsupportedOperation",
                message.clone(),
                400,
                true,
            ),
        }
    }
}
impl AwsErrorShape for SsmError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ParameterAlreadyExists => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "ParameterAlreadyExists",
                self.to_string(),
                400,
                true,
            ),
            Self::ParameterNotFound => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ParameterNotFound",
                self.to_string(),
                400,
                true,
            ),
            Self::ParameterVersionNotFound => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ParameterVersionNotFound",
                self.to_string(),
                400,
                true,
            ),
            Self::InvalidResourceId => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidResourceId",
                self.to_string(),
                400,
                true,
            ),
            Self::InvalidResourceType => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidResourceType",
                self.to_string(),
                400,
                true,
            ),
            Self::ParameterVersionLabelLimitExceeded => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ParameterVersionLabelLimitExceeded",
                self.to_string(),
                400,
                true,
            ),
            Self::UnsupportedParameterType => trusted_aws_error(
                AwsErrorFamily::Validation,
                "UnsupportedParameterType",
                self.to_string(),
                400,
                true,
            ),
            Self::ValidationException { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message.clone(),
                400,
                true,
            ),
            Self::UnsupportedOperation { message } => trusted_aws_error(
                AwsErrorFamily::UnsupportedOperation,
                "UnsupportedOperation",
                message.clone(),
                400,
                true,
            ),
            Self::InternalFailure { message } => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalServerError",
                message.clone(),
                500,
                false,
            ),
        }
    }
}
impl AwsErrorShape for StepFunctionsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::ExecutionAlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "ExecutionAlreadyExists",
                message,
                400,
                true,
            ),
            Self::ExecutionDoesNotExist { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "ExecutionDoesNotExist",
                message,
                400,
                true,
            ),
            Self::InvalidArn { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidArn",
                message,
                400,
                true,
            ),
            Self::InvalidDefinition { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidDefinition",
                message,
                400,
                true,
            ),
            Self::InvalidExecutionInput { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidExecutionInput",
                message,
                400,
                true,
            ),
            Self::InvalidName { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidName",
                message,
                400,
                true,
            ),
            Self::UnsupportedStateMachineType { message }
            | Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationException",
                message,
                400,
                true,
            ),
            Self::StateMachineAlreadyExists { message } => trusted_aws_error(
                AwsErrorFamily::AlreadyExists,
                "StateMachineAlreadyExists",
                message,
                400,
                true,
            ),
            Self::StateMachineDoesNotExist { message } => trusted_aws_error(
                AwsErrorFamily::NotFound,
                "StateMachineDoesNotExist",
                message,
                400,
                true,
            ),
            Self::Infrastructure(error) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                error.to_string(),
                500,
                false,
            ),
            Self::Storage(error) => trusted_aws_error(
                AwsErrorFamily::Internal,
                "InternalFailure",
                error.to_string(),
                500,
                false,
            ),
        }
    }
}

impl AwsErrorShape for StsError {
    fn to_aws_error(&self) -> AwsError {
        match self {
            Self::AccessDenied { message } => trusted_aws_error(
                AwsErrorFamily::AccessDenied,
                "AccessDenied",
                message,
                403,
                true,
            ),
            Self::InvalidAuthorizationMessage { message } => {
                trusted_aws_error(
                    AwsErrorFamily::Validation,
                    "InvalidAuthorizationMessageException",
                    message,
                    400,
                    true,
                )
            }
            Self::InvalidParameterValue { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "InvalidParameterValue",
                message,
                400,
                true,
            ),
            Self::Validation { message } => trusted_aws_error(
                AwsErrorFamily::Validation,
                "ValidationError",
                message,
                400,
                true,
            ),
        }
    }
}
