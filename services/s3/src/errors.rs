use aws::InfrastructureError;
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
