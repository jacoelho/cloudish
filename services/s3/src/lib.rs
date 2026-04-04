#![cfg_attr(
    test,
    allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)
)]

pub use aws::{BlobKey, BlobStore, Clock, InfrastructureError};

pub mod bucket;
mod bucket_subresources;
pub mod errors;
pub mod multipart;
pub mod notifications;
pub mod object;
pub mod object_listing;
mod object_ranges;
mod object_read_conditions;
mod object_read_model;
pub mod object_reads;
pub mod object_writes;
pub mod retention;
pub mod scope;
pub mod serialization;
mod state;
#[cfg(test)]
mod state_test_support;
pub mod tagging;

pub use bucket::{
    BucketVersioningOutput, BucketVersioningStatus, CreateBucketInput,
    GetBucketLocationOutput, HeadBucketInput, HeadBucketOutput,
    ListBucketsOutput, ListedBucket,
};
pub use bucket_subresources::{CannedAcl, StoredBucketAclInput};
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
    ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput,
    ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output,
    ListedDeleteMarker, ListedObject, ListedObjectVersion, ListedVersionEntry,
    ObjectTaggingOutput, PutObjectInput, PutObjectOutput,
};
pub use object_ranges::{IfRangeCondition, ObjectRange};
pub use object_read_conditions::{
    ObjectReadConditions, ObjectReadPreconditionOutcome,
};
pub use object_read_model::{
    GetObjectOutput, HeadObjectOutput, ObjectReadMetadata, ObjectReadRequest,
    ObjectReadResponseOverrides,
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
pub use tagging::{BucketTaggingOutput, TaggingInput};
