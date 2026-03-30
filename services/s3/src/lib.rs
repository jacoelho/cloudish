pub use aws::{BlobKey, BlobStore, Clock, InfrastructureError};

pub mod bucket;
pub mod errors;
pub mod multipart;
pub mod notifications;
pub mod object;
pub mod retention;
pub mod scope;
pub mod serialization;
mod state;

pub use bucket::{
    BucketTaggingOutput, BucketVersioningOutput, BucketVersioningStatus,
    CannedAcl, CreateBucketInput, GetBucketLocationOutput, ListBucketsOutput,
    ListBucketsPage, ListedBucket, StoredBucketAclInput, TaggingInput,
};
pub use errors::{S3Error, S3InitError};
pub use multipart::{
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CompletedMultipartPart, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, ListMultipartUploadsOutput,
    MultipartUploadSummary, UploadPartInput, UploadPartOutput,
};
pub use notifications::{
    BucketNotificationConfiguration, NotificationFilter,
    QueueNotificationConfiguration, S3EventNotification,
    S3NotificationTransport, TopicNotificationConfiguration,
};
pub use object::{
    CopyObjectInput, CopyObjectOutput, DeleteObjectInput, DeleteObjectOutput,
    GetObjectOutput, HeadObjectOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListedDeleteMarker, ListedObject,
    ListedObjectVersion, ListedVersionEntry, ObjectTaggingOutput,
    PutObjectInput, PutObjectOutput,
};
pub use retention::{
    BucketObjectLockConfiguration, DefaultObjectLockRetention,
    DefaultRetentionPeriod, LegalHoldStatus, ObjectLegalHoldOutput,
    ObjectLockMode, ObjectLockWriteOptions, ObjectRetention,
    PutObjectLegalHoldInput, PutObjectRetentionInput,
};
pub use scope::S3Scope;
pub use serialization::{
    CsvFileHeaderInfo, CsvInputSerialization, CsvOutputSerialization,
    SelectObjectContentInput, SelectObjectContentOutput, SelectObjectStats,
};
pub use state::S3Service;
