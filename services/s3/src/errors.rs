use aws::InfrastructureError;
#[cfg(test)]
use aws::{AwsError, AwsErrorFamily};
use storage::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum S3InitError {
    #[error("failed to load persisted S3 bucket metadata: {0}")]
    Buckets(#[source] StorageError),
    #[error("failed to load persisted S3 multipart state: {0}")]
    MultipartUploads(#[source] StorageError),
    #[error("failed to load persisted S3 object metadata: {0}")]
    Objects(#[source] StorageError),
    #[error("failed to load persisted S3 sequence state: {0}")]
    Sequences(#[source] StorageError),
}

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("bucket `{bucket}` already exists")]
    BucketAlreadyExists { bucket: String },
    #[error("bucket `{bucket}` already belongs to the caller")]
    BucketAlreadyOwnedByYou { bucket: String },
    #[error("bucket `{bucket}` is not empty")]
    BucketNotEmpty { bucket: String },
    #[error("bucket metadata store failed: {0}")]
    Buckets(#[source] StorageError),
    #[error("object blob store failed: {0}")]
    Blob(#[source] InfrastructureError),
    #[error("{message}")]
    AccessDenied { message: String },
    #[error("bucket `{bucket}` has no object lock configuration")]
    BucketObjectLockConfigurationNotFound { bucket: String },
    #[error("{message}")]
    InvalidArgument { code: &'static str, message: String, status_code: u16 },
    #[error("{message}")]
    InvalidBucketState { message: String },
    #[error("{message}")]
    Internal { message: String },
    #[error("bucket `{bucket}` does not exist")]
    NoSuchBucket { bucket: String },
    #[error("bucket `{bucket}` has no ACL")]
    NoSuchBucketAcl { bucket: String },
    #[error("bucket `{bucket}` has no CORS configuration")]
    NoSuchCORSConfiguration { bucket: String },
    #[error("bucket `{bucket}` has no encryption configuration")]
    NoSuchBucketEncryption { bucket: String },
    #[error("bucket `{bucket}` has no lifecycle configuration")]
    NoSuchLifecycleConfiguration { bucket: String },
    #[error("bucket `{bucket}` has no policy")]
    NoSuchBucketPolicy { bucket: String },
    #[error("bucket `{bucket}` has no tags")]
    NoSuchTagSet { bucket: String },
    #[error("key `{key}` does not exist")]
    NoSuchKey { key: String },
    #[error("object `{key}` has no object lock configuration")]
    NoSuchObjectLockConfiguration { key: String },
    #[error("multipart upload `{upload_id}` does not exist")]
    NoSuchUpload { upload_id: String },
    #[error("version `{version_id}` does not exist")]
    NoSuchVersion { version_id: String },
    #[error("multipart metadata store failed: {0}")]
    MultipartUploads(#[source] StorageError),
    #[error("object metadata store failed: {0}")]
    Objects(#[source] StorageError),
    #[error("sequence store failed: {0}")]
    Sequences(#[source] StorageError),
}

impl S3Error {
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn to_aws_error(&self) -> AwsError {
        match self {
            Self::BucketAlreadyExists { bucket } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "BucketAlreadyExists",
                format!("The requested bucket name `{bucket}` is not available."),
                409,
                true,
            ),
            Self::BucketAlreadyOwnedByYou { bucket } => AwsError::custom(
                AwsErrorFamily::AlreadyExists,
                "BucketAlreadyOwnedByYou",
                format!("Your previous request to create the named bucket `{bucket}` succeeded and you already own it."),
                409,
                true,
            ),
            Self::BucketNotEmpty { bucket } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "BucketNotEmpty",
                format!("The bucket `{bucket}` you tried to delete is not empty."),
                409,
                true,
            ),
            Self::AccessDenied { message } => AwsError::custom(
                AwsErrorFamily::AccessDenied,
                "AccessDenied",
                message.clone(),
                403,
                true,
            ),
            Self::BucketObjectLockConfigurationNotFound { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ObjectLockConfigurationNotFoundError",
                "Object Lock configuration does not exist for this bucket",
                404,
                true,
            ),
            Self::InvalidArgument {
                code,
                message,
                status_code,
            } => AwsError::custom(
                AwsErrorFamily::Validation,
                *code,
                message.clone(),
                *status_code,
                true,
            ),
            Self::InvalidBucketState { message } => AwsError::custom(
                AwsErrorFamily::Conflict,
                "InvalidBucketState",
                message.clone(),
                409,
                true,
            ),
            Self::NoSuchBucket { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchBucket",
                "The specified bucket does not exist.",
                404,
                true,
            ),
            Self::NoSuchBucketAcl { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchBucketAcl",
                "The specified bucket ACL does not exist.",
                404,
                true,
            ),
            Self::NoSuchBucketPolicy { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchBucketPolicy",
                "The bucket policy does not exist",
                404,
                true,
            ),
            Self::NoSuchBucketEncryption { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "ServerSideEncryptionConfigurationNotFoundError",
                "The server side encryption configuration was not found",
                404,
                true,
            ),
            Self::NoSuchCORSConfiguration { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchCORSConfiguration",
                "The CORS configuration does not exist",
                404,
                true,
            ),
            Self::NoSuchLifecycleConfiguration { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchLifecycleConfiguration",
                "The lifecycle configuration does not exist",
                404,
                true,
            ),
            Self::NoSuchKey { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchKey",
                "The specified key does not exist.",
                404,
                true,
            ),
            Self::NoSuchObjectLockConfiguration { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchObjectLockConfiguration",
                "The specified object does not have a ObjectLock configuration",
                404,
                true,
            ),
            Self::NoSuchUpload { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchUpload",
                "The specified multipart upload does not exist.",
                404,
                true,
            ),
            Self::NoSuchVersion { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchVersion",
                "The specified version does not exist.",
                404,
                true,
            ),
            Self::NoSuchTagSet { .. } => AwsError::custom(
                AwsErrorFamily::NotFound,
                "NoSuchTagSet",
                "There is no tag set associated with the bucket.",
                404,
                true,
            ),
            Self::Buckets(source)
            | Self::MultipartUploads(source)
            | Self::Objects(source)
            | Self::Sequences(source) => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalError",
                format!("The S3 metadata store failed: {source}"),
                500,
                false,
            ),
            Self::Blob(source) => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalError",
                format!("The S3 blob store failed: {source}"),
                500,
                false,
            ),
            Self::Internal { message } => AwsError::custom(
                AwsErrorFamily::Internal,
                "InternalError",
                message.clone(),
                500,
                false,
            ),
        }
        .expect("S3 error mappings must build valid AWS errors")
    }
}
