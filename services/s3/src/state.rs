use aws::{BlobKey, BlobStore, Clock};

use aws::{AccountId, Arn, ArnResource, Partition, RegionId, ServiceName};
use base64::Engine as _;
use csv::{ReaderBuilder, Terminator, WriterBuilder};
use regex_lite::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use storage::{StorageFactory, StorageHandle};

use crate::{
    errors::{S3Error, S3InitError},
    scope::S3Scope,
};

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const DEFAULT_MAX_KEYS: usize = 1_000;
const MAX_KEYS_LIMIT: usize = 1_000;
const STANDARD_STORAGE_CLASS: &str = "STANDARD";
const NULL_VERSION_ID: &str = "null";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBucketInput {
    pub name: String,
    pub object_lock_enabled: bool,
    pub region: RegionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketsOutput {
    pub buckets: Vec<ListedBucket>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketsPage {
    pub buckets: Vec<ListedBucket>,
    pub is_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedBucket {
    pub created_at_epoch_seconds: u64,
    pub name: String,
    pub owner_account_id: String,
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetBucketLocationOutput {
    pub name: String,
    pub region: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketVersioningStatus {
    Enabled,
    Suspended,
}

impl BucketVersioningStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Suspended => "Suspended",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketVersioningOutput {
    pub status: Option<BucketVersioningStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectLockMode {
    Compliance,
    Governance,
}

impl ObjectLockMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compliance => "COMPLIANCE",
            Self::Governance => "GOVERNANCE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegalHoldStatus {
    Off,
    On,
}

impl LegalHoldStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::On => "ON",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DefaultRetentionPeriod {
    Days(u32),
    Years(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultObjectLockRetention {
    pub mode: ObjectLockMode,
    pub period: DefaultRetentionPeriod,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketObjectLockConfiguration {
    pub object_lock_enabled: bool,
    pub default_retention: Option<DefaultObjectLockRetention>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRetention {
    pub mode: Option<ObjectLockMode>,
    pub retain_until_epoch_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLegalHoldOutput {
    pub status: LegalHoldStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectRetentionInput {
    pub bucket: String,
    pub bypass_governance: bool,
    pub bypass_governance_authorized: bool,
    pub key: String,
    pub retention: ObjectRetention,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectLegalHoldInput {
    pub bucket: String,
    pub key: String,
    pub status: LegalHoldStatus,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLockWriteOptions {
    pub legal_hold_status: Option<LegalHoldStatus>,
    pub mode: Option<ObjectLockMode>,
    pub retain_until_epoch_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BucketNotificationConfiguration {
    #[serde(default)]
    pub queue_configurations: Vec<QueueNotificationConfiguration>,
    #[serde(default)]
    pub topic_configurations: Vec<TopicNotificationConfiguration>,
}

impl BucketNotificationConfiguration {
    fn is_empty(&self) -> bool {
        self.queue_configurations.is_empty()
            && self.topic_configurations.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationFilter {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNotificationConfiguration {
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub filter: Option<NotificationFilter>,
    #[serde(default)]
    pub id: Option<String>,
    pub queue_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicNotificationConfiguration {
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub filter: Option<NotificationFilter>,
    #[serde(default)]
    pub id: Option<String>,
    pub topic_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3EventNotification {
    pub bucket_arn: Arn,
    pub bucket_name: String,
    pub bucket_owner_account_id: AccountId,
    pub configuration_id: Option<String>,
    pub destination_arn: Arn,
    pub etag: Option<String>,
    pub event_name: String,
    pub event_time_epoch_seconds: u64,
    pub key: String,
    pub region: RegionId,
    pub requester_account_id: AccountId,
    pub sequencer: String,
    pub size: u64,
    pub version_id: Option<String>,
}

struct ObjectNotificationDetails<'a> {
    event_name: &'a str,
    event_time_epoch_seconds: u64,
    etag: Option<&'a str>,
    key: &'a str,
    size: u64,
    version_id: Option<&'a str>,
}

pub trait S3NotificationTransport: Send + Sync {
    fn publish(&self, notification: &S3EventNotification);

    /// # Errors
    ///
    /// Returns an [`S3Error`] when the destination ARN is not acceptable for
    /// the current bucket notification configuration.
    fn validate_destination(
        &self,
        _scope: &S3Scope,
        _bucket_name: &str,
        _bucket_owner_account_id: &AccountId,
        _bucket_region: &RegionId,
        _destination_arn: &Arn,
    ) -> Result<(), S3Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvFileHeaderInfo {
    Ignore,
    None,
    Use,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvInputSerialization {
    pub comments: Option<char>,
    pub field_delimiter: char,
    pub file_header_info: CsvFileHeaderInfo,
    pub quote_character: char,
    pub quote_escape_character: char,
    pub record_delimiter: char,
}

impl Default for CsvInputSerialization {
    fn default() -> Self {
        Self {
            comments: Some('#'),
            field_delimiter: ',',
            file_header_info: CsvFileHeaderInfo::None,
            quote_character: '"',
            quote_escape_character: '"',
            record_delimiter: '\n',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvOutputSerialization {
    pub always_quote: bool,
    pub field_delimiter: char,
    pub quote_character: char,
    pub quote_escape_character: char,
    pub record_delimiter: char,
}

impl Default for CsvOutputSerialization {
    fn default() -> Self {
        Self {
            always_quote: false,
            field_delimiter: ',',
            quote_character: '"',
            quote_escape_character: '"',
            record_delimiter: '\n',
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectObjectContentInput {
    pub bucket: String,
    pub csv_input: CsvInputSerialization,
    pub csv_output: CsvOutputSerialization,
    pub expression: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectObjectStats {
    pub bytes_processed: u64,
    pub bytes_returned: u64,
    pub bytes_scanned: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectObjectContentOutput {
    pub records: String,
    pub stats: SelectObjectStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectInput {
    pub body: Vec<u8>,
    pub bucket: String,
    pub content_type: Option<String>,
    pub key: String,
    pub metadata: BTreeMap<String, String>,
    pub object_lock: Option<ObjectLockWriteOptions>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectOutput {
    pub etag: String,
    pub last_modified_epoch_seconds: u64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadObjectOutput {
    pub content_type: String,
    pub delete_marker: bool,
    pub etag: String,
    pub key: String,
    pub last_modified_epoch_seconds: u64,
    pub metadata: BTreeMap<String, String>,
    pub object_lock_legal_hold_status: Option<LegalHoldStatus>,
    pub object_lock_mode: Option<ObjectLockMode>,
    pub object_lock_retain_until_epoch_seconds: Option<u64>,
    pub size: u64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetObjectOutput {
    pub body: Vec<u8>,
    pub head: HeadObjectOutput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectInput {
    pub bypass_governance: bool,
    pub bypass_governance_authorized: bool,
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectOutput {
    pub delete_marker: bool,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyObjectInput {
    pub destination_bucket: String,
    pub destination_key: String,
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyObjectOutput {
    pub etag: String,
    pub last_modified_epoch_seconds: u64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsInput {
    pub bucket: String,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    pub max_keys: Option<usize>,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsOutput {
    pub bucket: String,
    pub common_prefixes: Vec<String>,
    pub contents: Vec<ListedObject>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub marker: Option<String>,
    pub max_keys: usize,
    pub next_marker: Option<String>,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Input {
    pub bucket: String,
    pub continuation_token: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<usize>,
    pub prefix: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Output {
    pub bucket: String,
    pub common_prefixes: Vec<String>,
    pub contents: Vec<ListedObject>,
    pub continuation_token: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub key_count: usize,
    pub max_keys: usize,
    pub next_continuation_token: Option<String>,
    pub prefix: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub etag: String,
    pub key: String,
    pub last_modified_epoch_seconds: u64,
    pub size: u64,
    pub storage_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectVersionsInput {
    pub bucket: String,
    pub max_keys: Option<usize>,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectVersionsOutput {
    pub bucket: String,
    pub entries: Vec<ListedVersionEntry>,
    pub is_truncated: bool,
    pub max_keys: usize,
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedVersionEntry {
    DeleteMarker(ListedDeleteMarker),
    Version(ListedObjectVersion),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedDeleteMarker {
    pub is_latest: bool,
    pub key: String,
    pub last_modified_epoch_seconds: u64,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObjectVersion {
    pub etag: String,
    pub is_latest: bool,
    pub key: String,
    pub last_modified_epoch_seconds: u64,
    pub size: u64,
    pub storage_class: String,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketTaggingOutput {
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTaggingOutput {
    pub tags: BTreeMap<String, String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggingInput {
    pub bucket: String,
    pub key: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMultipartUploadInput {
    pub bucket: String,
    pub content_type: Option<String>,
    pub key: String,
    pub metadata: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMultipartUploadOutput {
    pub upload_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadPartInput {
    pub body: Vec<u8>,
    pub bucket: String,
    pub key: String,
    pub part_number: u16,
    pub upload_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadPartOutput {
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedMultipartPart {
    pub etag: Option<String>,
    pub part_number: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub parts: Vec<CompletedMultipartPart>,
    pub upload_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteMultipartUploadOutput {
    pub etag: String,
    pub last_modified_epoch_seconds: u64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadSummary {
    pub initiated_epoch_seconds: u64,
    pub key: String,
    pub upload_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMultipartUploadsOutput {
    pub bucket: String,
    pub uploads: Vec<MultipartUploadSummary>,
}

#[derive(Clone)]
pub struct S3Service {
    blob_store: Arc<dyn BlobStore>,
    bucket_store: StorageHandle<String, BucketRecord>,
    clock: Arc<dyn Clock>,
    multipart_store: StorageHandle<String, MultipartUploadRecord>,
    mutation_lock: Arc<Mutex<()>>,
    notification_transport:
        Arc<Mutex<Arc<dyn S3NotificationTransport + Send + Sync>>>,
    object_store: StorageHandle<ObjectStorageKey, ObjectRecord>,
    sequence_store: StorageHandle<String, u64>,
}

impl S3Service {
    /// # Errors
    ///
    /// Returns [`S3InitError`] when persisted S3 metadata cannot be loaded
    /// from the configured stores.
    pub fn new(
        factory: &StorageFactory,
        blob_store: Arc<dyn BlobStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, S3InitError> {
        Self::with_stores(
            factory.create("s3", "buckets"),
            factory.create("s3", "objects"),
            factory.create("s3", "multipart_uploads"),
            factory.create("s3", "sequences"),
            blob_store,
            clock,
        )
    }

    fn with_stores(
        bucket_store: StorageHandle<String, BucketRecord>,
        object_store: StorageHandle<ObjectStorageKey, ObjectRecord>,
        multipart_store: StorageHandle<String, MultipartUploadRecord>,
        sequence_store: StorageHandle<String, u64>,
        blob_store: Arc<dyn BlobStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, S3InitError> {
        bucket_store.load().map_err(S3InitError::Buckets)?;
        object_store.load().map_err(S3InitError::Objects)?;
        multipart_store.load().map_err(S3InitError::MultipartUploads)?;
        sequence_store.load().map_err(S3InitError::Sequences)?;

        Ok(Self {
            blob_store,
            bucket_store,
            clock,
            multipart_store,
            mutation_lock: Arc::new(Mutex::new(())),
            notification_transport: Arc::new(Mutex::new(Arc::new(
                NoopS3NotificationTransport,
            ))),
            object_store,
            sequence_store,
        })
    }

    pub fn set_notification_transport(
        &self,
        transport: Arc<dyn S3NotificationTransport + Send + Sync>,
    ) {
        *self
            .notification_transport
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = transport;
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn create_bucket(
        &self,
        scope: &S3Scope,
        input: CreateBucketInput,
    ) -> Result<ListedBucket, S3Error> {
        validate_bucket_name(&input.name)?;
        let _guard = self.lock_state();

        if let Some(existing) = self.bucket_store.get(&input.name) {
            return if existing.owner_account_id == scope.account_id().as_str()
            {
                Err(S3Error::BucketAlreadyOwnedByYou { bucket: input.name })
            } else {
                Err(S3Error::BucketAlreadyExists { bucket: input.name })
            };
        }

        let record = BucketRecord {
            acl_configuration: None,
            cors_configuration: None,
            created_at_epoch_seconds: self.now_epoch_seconds()?,
            encryption_configuration: None,
            lifecycle_configuration: None,
            name: input.name.clone(),
            notification_configuration:
                BucketNotificationConfiguration::default(),
            object_lock_configuration: None,
            object_lock_enabled: input.object_lock_enabled,
            owner_account_id: scope.account_id().to_string(),
            policy: None,
            region: input.region.as_str().to_owned(),
            tags: BTreeMap::new(),
            versioning_status: input
                .object_lock_enabled
                .then_some(BucketVersioningStatus::Enabled),
        };

        self.bucket_store
            .put(input.name, record.clone())
            .map_err(S3Error::Buckets)?;

        Ok(record.into_bucket())
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, bucket)?;
        let bucket = bucket.to_owned();
        let scan_bucket = bucket.clone();

        if self
            .object_store
            .scan(&move |candidate| candidate.bucket == scan_bucket)
            .is_empty()
            && self
                .multipart_store
                .scan(&move |_| true)
                .into_iter()
                .all(|upload| upload.bucket != bucket)
        {
            self.bucket_store.delete(&bucket).map_err(S3Error::Buckets)?;
            return Ok(());
        }

        Err(S3Error::BucketNotEmpty { bucket })
    }

    pub fn list_buckets(&self, scope: &S3Scope) -> ListBucketsOutput {
        let buckets = self
            .bucket_store
            .scan(&|_| true)
            .into_iter()
            .filter(|bucket| {
                bucket.owner_account_id == scope.account_id().as_str()
            })
            .map(BucketRecord::into_bucket)
            .collect();

        ListBucketsOutput { buckets }
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_location(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<GetBucketLocationOutput, S3Error> {
        let bucket = self.ensure_bucket_owned(scope, bucket)?;
        Ok(GetBucketLocationOutput {
            name: bucket.name,
            region: bucket.region,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_versioning(
        &self,
        scope: &S3Scope,
        bucket: &str,
        status: BucketVersioningStatus,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        if bucket_record.object_lock_enabled
            && status == BucketVersioningStatus::Suspended
        {
            return Err(S3Error::InvalidBucketState {
                message: "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed."
                    .to_owned(),
            });
        }
        bucket_record.versioning_status = Some(status);
        self.bucket_store
            .put(bucket.to_owned(), bucket_record)
            .map_err(S3Error::Buckets)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_versioning(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketVersioningOutput, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        Ok(BucketVersioningOutput { status: bucket_record.versioning_status })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_notification_configuration(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: BucketNotificationConfiguration,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        validate_notification_configuration(&configuration)?;
        self.validate_notification_destinations(
            scope,
            &bucket_record,
            &configuration,
        )?;
        bucket_record.notification_configuration = configuration;
        self.bucket_store
            .put(bucket.to_owned(), bucket_record)
            .map_err(S3Error::Buckets)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_notification_configuration(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketNotificationConfiguration, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        Ok(bucket_record.notification_configuration)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_object_lock_configuration(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: BucketObjectLockConfiguration,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        if bucket_record.versioning_status
            != Some(BucketVersioningStatus::Enabled)
        {
            return Err(S3Error::InvalidBucketState {
                message: "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration"
                    .to_owned(),
            });
        }

        bucket_record.object_lock_enabled = configuration.object_lock_enabled;
        bucket_record.object_lock_configuration = configuration
            .default_retention
            .map(StoredObjectLockConfiguration::from);
        self.bucket_store
            .put(bucket.to_owned(), bucket_record)
            .map_err(S3Error::Buckets)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_object_lock_configuration(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketObjectLockConfiguration, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        if !bucket_record.object_lock_enabled {
            return Err(S3Error::BucketObjectLockConfigurationNotFound {
                bucket: bucket.to_owned(),
            });
        }

        Ok(BucketObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: bucket_record
                .object_lock_configuration
                .map(DefaultObjectLockRetention::from),
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_object(
        &self,
        scope: &S3Scope,
        input: PutObjectInput,
    ) -> Result<PutObjectOutput, S3Error> {
        let _guard = self.lock_state();
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        let stored = self.put_object_inner(scope, input, None)?;
        self.emit_object_created_notification(
            scope,
            "ObjectCreated:Put",
            &bucket,
            &key,
            stored.version_id.as_deref(),
        );
        Ok(stored)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn head_object(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<HeadObjectOutput, S3Error> {
        self.ensure_bucket_owned(scope, bucket)?;
        let object = self.resolve_object_record(bucket, key, version_id)?;
        if object.delete_marker {
            return Err(S3Error::NoSuchKey { key: key.to_owned() });
        }

        Ok(object.into_head())
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_object(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<GetObjectOutput, S3Error> {
        let object = self.resolve_object_record(bucket, key, version_id)?;
        self.ensure_bucket_owned(scope, bucket)?;
        if object.delete_marker {
            return Err(S3Error::NoSuchKey { key: key.to_owned() });
        }

        let body = self
            .blob_store
            .get(&object.blob_key())
            .map_err(S3Error::Blob)?
            .ok_or_else(|| S3Error::Internal {
                message: format!(
                    "object payload for `{bucket}/{key}` is missing from the blob store"
                ),
            })?;

        Ok(GetObjectOutput { body, head: object.into_head() })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_object(
        &self,
        scope: &S3Scope,
        input: DeleteObjectInput,
    ) -> Result<DeleteObjectOutput, S3Error> {
        let _guard = self.lock_state();
        let bucket_record = self.ensure_bucket_owned(scope, &input.bucket)?;

        if let Some(version_id) = input.version_id.as_deref() {
            let deleted = self.delete_object_version(
                &input.bucket,
                &input.key,
                version_id,
                input.bypass_governance,
                input.bypass_governance_authorized,
            )?;
            self.emit_object_removed_notification(
                scope,
                &bucket_record,
                &input.bucket,
                &input.key,
                deleted.version_id.as_deref(),
                deleted.delete_marker,
            );
            return Ok(deleted);
        }

        let Some(current) =
            self.current_object_record(&input.bucket, &input.key)
        else {
            return Ok(DeleteObjectOutput {
                delete_marker: false,
                version_id: None,
            });
        };

        match bucket_record.versioning_status {
            Some(BucketVersioningStatus::Enabled) => {
                let deleted = self.create_delete_marker(
                    &input.bucket,
                    &input.key,
                    Some(current),
                )?;
                self.emit_object_removed_notification(
                    scope,
                    &bucket_record,
                    &input.bucket,
                    &input.key,
                    deleted.version_id.as_deref(),
                    true,
                );
                Ok(deleted)
            }
            Some(BucketVersioningStatus::Suspended) => {
                self.delete_null_version_if_present(
                    &input.bucket,
                    &input.key,
                )?;
                let deleted = self.create_delete_marker(
                    &input.bucket,
                    &input.key,
                    Some(current),
                )?;
                self.emit_object_removed_notification(
                    scope,
                    &bucket_record,
                    &input.bucket,
                    &input.key,
                    deleted.version_id.as_deref(),
                    true,
                );
                Ok(deleted)
            }
            None => {
                self.remove_object_record(&current)?;
                self.emit_object_removed_notification(
                    scope,
                    &bucket_record,
                    &input.bucket,
                    &input.key,
                    None,
                    false,
                );
                Ok(DeleteObjectOutput {
                    delete_marker: false,
                    version_id: None,
                })
            }
        }
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn copy_object(
        &self,
        scope: &S3Scope,
        input: CopyObjectInput,
    ) -> Result<CopyObjectOutput, S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, &input.source_bucket)?;
        self.ensure_bucket_owned(scope, &input.destination_bucket)?;

        let source = self.resolve_object_record(
            &input.source_bucket,
            &input.source_key,
            input.source_version_id.as_deref(),
        )?;
        if source.delete_marker {
            return Err(S3Error::NoSuchKey { key: input.source_key });
        }

        let body = self
            .blob_store
            .get(&source.blob_key())
            .map_err(S3Error::Blob)?
            .ok_or_else(|| S3Error::Internal {
                message: format!(
                    "object payload for `{}/{}` is missing from the blob store",
                    input.source_bucket, input.source_key
                ),
            })?;
        let content_type = source.content_type;
        let metadata = source.metadata;
        let tags = source.tags;
        let copied = self.put_object_inner(
            scope,
            PutObjectInput {
                body,
                bucket: input.destination_bucket.clone(),
                content_type: Some(content_type),
                key: input.destination_key.clone(),
                metadata,
                object_lock: None,
                tags,
            },
            None,
        )?;
        self.emit_object_created_notification(
            scope,
            "ObjectCreated:Copy",
            &input.destination_bucket,
            &input.destination_key,
            copied.version_id.as_deref(),
        );

        Ok(CopyObjectOutput {
            etag: copied.etag,
            last_modified_epoch_seconds: copied.last_modified_epoch_seconds,
            version_id: copied.version_id,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_objects(
        &self,
        scope: &S3Scope,
        input: ListObjectsInput,
    ) -> Result<ListObjectsOutput, S3Error> {
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let page = self.collect_page(
            &input.bucket,
            input.prefix.clone(),
            input.delimiter.clone(),
            input.max_keys,
            input.marker.clone(),
        )?;

        Ok(ListObjectsOutput {
            bucket: input.bucket,
            common_prefixes: page.common_prefixes,
            contents: page.contents,
            delimiter: input.delimiter,
            is_truncated: page.is_truncated,
            marker: input.marker,
            max_keys: page.max_keys,
            next_marker: page.next_anchor,
            prefix: input.prefix,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_objects_v2(
        &self,
        scope: &S3Scope,
        input: ListObjectsV2Input,
    ) -> Result<ListObjectsV2Output, S3Error> {
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let anchor = match input.continuation_token.as_deref() {
            Some(token) => Some(decode_continuation_token(token)?),
            None => input.start_after.clone(),
        };
        let page = self.collect_page(
            &input.bucket,
            input.prefix.clone(),
            input.delimiter.clone(),
            input.max_keys,
            anchor,
        )?;

        Ok(ListObjectsV2Output {
            bucket: input.bucket,
            common_prefixes: page.common_prefixes,
            contents: page.contents,
            continuation_token: input.continuation_token,
            delimiter: input.delimiter,
            is_truncated: page.is_truncated,
            key_count: page.object_count,
            max_keys: page.max_keys,
            next_continuation_token: page
                .next_anchor
                .as_deref()
                .map(encode_continuation_token),
            prefix: input.prefix,
            start_after: input.start_after,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_object_versions(
        &self,
        scope: &S3Scope,
        input: ListObjectVersionsInput,
    ) -> Result<ListObjectVersionsOutput, S3Error> {
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let prefix = input.prefix.clone().unwrap_or_default();
        let max_keys = normalize_max_keys(input.max_keys);
        let bucket = input.bucket.clone();
        let mut entries = self
            .object_store
            .scan(&move |candidate| candidate.bucket == bucket)
            .into_iter()
            .filter(|object| object.key.starts_with(&prefix))
            .filter(|object| object.version_id.is_some())
            .map(|object| {
                if object.delete_marker {
                    ListedVersionEntry::DeleteMarker(
                        object.into_delete_marker(),
                    )
                } else {
                    ListedVersionEntry::Version(object.into_listed_version())
                }
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.sort_key().cmp(right.sort_key()).then_with(|| {
                right.version_sort_key().cmp(left.version_sort_key())
            })
        });

        let is_truncated = entries.len() > max_keys;
        entries.truncate(max_keys);

        Ok(ListObjectVersionsOutput {
            bucket: input.bucket,
            entries,
            is_truncated,
            max_keys,
            prefix: input.prefix,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_tagging(
        &self,
        scope: &S3Scope,
        bucket: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.tags = normalize_tags(tags)?;
        self.bucket_store
            .put(bucket.to_owned(), bucket_record)
            .map_err(S3Error::Buckets)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_tagging(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketTaggingOutput, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        if bucket_record.tags.is_empty() {
            return Err(S3Error::NoSuchTagSet { bucket: bucket.to_owned() });
        }

        Ok(BucketTaggingOutput { tags: bucket_record.tags })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_tagging(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.tags.clear();
        self.bucket_store
            .put(bucket.to_owned(), bucket_record)
            .map_err(S3Error::Buckets)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_object_tagging(
        &self,
        scope: &S3Scope,
        input: TaggingInput,
    ) -> Result<ObjectTaggingOutput, S3Error> {
        let _guard = self.lock_state();
        let key = input.key.ok_or_else(|| S3Error::InvalidArgument {
            code: "InvalidRequest",
            message: "Object tagging requires an object key.".to_owned(),
            status_code: 400,
        })?;
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let mut object = self.resolve_object_record(
            &input.bucket,
            &key,
            input.version_id.as_deref(),
        )?;
        if object.delete_marker {
            return Err(S3Error::NoSuchKey { key });
        }
        object.tags = normalize_tags(input.tags)?;
        self.persist_object_record(&object)?;
        self.emit_object_created_notification(
            scope,
            "ObjectTagging:Put",
            &input.bucket,
            &key,
            object.version_id.as_deref(),
        );

        Ok(ObjectTaggingOutput {
            tags: object.tags,
            version_id: object.version_id,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_object_tagging(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectTaggingOutput, S3Error> {
        self.ensure_bucket_owned(scope, bucket)?;
        let object = self.resolve_object_record(bucket, key, version_id)?;
        if object.delete_marker {
            return Err(S3Error::NoSuchKey { key: key.to_owned() });
        }

        Ok(ObjectTaggingOutput {
            tags: object.tags,
            version_id: object.version_id,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_object_tagging(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectTaggingOutput, S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, bucket)?;
        let mut object =
            self.resolve_object_record(bucket, key, version_id)?;
        if object.delete_marker {
            return Err(S3Error::NoSuchKey { key: key.to_owned() });
        }
        object.tags.clear();
        self.persist_object_record(&object)?;
        self.emit_object_created_notification(
            scope,
            "ObjectTagging:Delete",
            bucket,
            key,
            object.version_id.as_deref(),
        );

        Ok(ObjectTaggingOutput {
            tags: object.tags,
            version_id: object.version_id,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_object_retention(
        &self,
        scope: &S3Scope,
        input: PutObjectRetentionInput,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let bucket_record = self.ensure_bucket_owned(scope, &input.bucket)?;
        ensure_bucket_object_lock_enabled(&bucket_record)?;
        let mut object = self.resolve_object_record(
            &input.bucket,
            &input.key,
            input.version_id.as_deref(),
        )?;
        if object.delete_marker {
            return Err(S3Error::NoSuchVersion {
                version_id: input
                    .version_id
                    .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
            });
        }
        let now = self.now_epoch_seconds()?;
        apply_object_retention(
            &mut object,
            input.retention,
            now,
            input.bypass_governance && input.bypass_governance_authorized,
        )?;
        self.persist_object_record(&object)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_object_retention(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectRetention, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        ensure_bucket_object_lock_enabled(&bucket_record)?;
        let object = self.resolve_object_record(bucket, key, version_id)?;
        if object.delete_marker {
            return Err(S3Error::NoSuchVersion {
                version_id: version_id.unwrap_or(NULL_VERSION_ID).to_owned(),
            });
        }
        object.retention().ok_or_else(|| {
            S3Error::NoSuchObjectLockConfiguration { key: key.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_object_legal_hold(
        &self,
        scope: &S3Scope,
        input: PutObjectLegalHoldInput,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let bucket_record = self.ensure_bucket_owned(scope, &input.bucket)?;
        ensure_bucket_object_lock_enabled(&bucket_record)?;
        let mut object = self.resolve_object_record(
            &input.bucket,
            &input.key,
            input.version_id.as_deref(),
        )?;
        if object.delete_marker {
            return Err(S3Error::NoSuchVersion {
                version_id: input
                    .version_id
                    .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
            });
        }
        object.legal_hold_status = Some(input.status);
        self.persist_object_record(&object)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_object_legal_hold(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectLegalHoldOutput, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        ensure_bucket_object_lock_enabled(&bucket_record)?;
        let object = self.resolve_object_record(bucket, key, version_id)?;
        if object.delete_marker {
            return Err(S3Error::NoSuchVersion {
                version_id: version_id.unwrap_or(NULL_VERSION_ID).to_owned(),
            });
        }
        object
            .legal_hold_status
            .map(|status| ObjectLegalHoldOutput { status })
            .ok_or_else(|| S3Error::NoSuchObjectLockConfiguration {
                key: key.to_owned(),
            })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn select_object_content(
        &self,
        scope: &S3Scope,
        input: SelectObjectContentInput,
    ) -> Result<SelectObjectContentOutput, S3Error> {
        let object = self.get_object(
            scope,
            &input.bucket,
            &input.key,
            input.version_id.as_deref(),
        )?;
        evaluate_csv_select(&object.body, &input)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn restore_object(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), S3Error> {
        let _ = self.get_object(scope, bucket, key, version_id)?;
        Err(S3Error::InvalidArgument {
            code: "NotImplemented",
            message: "RestoreObject lifecycle is not implemented.".to_owned(),
            status_code: 501,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
        policy: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.policy = Some(policy);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.policy.ok_or_else(|| S3Error::NoSuchBucketPolicy {
            bucket: bucket.to_owned(),
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_policy(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.policy = None;
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_cors(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.cors_configuration = Some(configuration);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_cors(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.cors_configuration.ok_or_else(|| {
            S3Error::NoSuchCORSConfiguration { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_cors(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.cors_configuration = None;
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_lifecycle(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.lifecycle_configuration = Some(configuration);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_lifecycle(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.lifecycle_configuration.ok_or_else(|| {
            S3Error::NoSuchLifecycleConfiguration { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_lifecycle(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.lifecycle_configuration = None;
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_encryption(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: String,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.encryption_configuration = Some(configuration);
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_encryption(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        bucket_record.encryption_configuration.ok_or_else(|| {
            S3Error::NoSuchBucketEncryption { bucket: bucket.to_owned() }
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn delete_bucket_encryption(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.encryption_configuration = None;
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_acl(
        &self,
        scope: &S3Scope,
        bucket: &str,
        acl: StoredBucketAclInput,
    ) -> Result<(), S3Error> {
        self.put_bucket_subresource(scope, bucket, |record| {
            record.acl_configuration = Some(match acl {
                StoredBucketAclInput::Canned(value) => {
                    StoredBucketAcl::Canned(value)
                }
                StoredBucketAclInput::Xml(value) => {
                    StoredBucketAcl::Xml(value)
                }
            });
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_acl(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<String, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        Ok(match bucket_record.acl_configuration {
            Some(StoredBucketAcl::Xml(value)) => value,
            Some(StoredBucketAcl::Canned(value)) => {
                canned_acl_xml(&bucket_record.owner_account_id, value)
            }
            None => canned_acl_xml(
                &bucket_record.owner_account_id,
                CannedAcl::Private,
            ),
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn create_multipart_upload(
        &self,
        scope: &S3Scope,
        input: CreateMultipartUploadInput,
    ) -> Result<CreateMultipartUploadOutput, S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, &input.bucket)?;

        let upload_id =
            format!("upload-{:020}", self.next_sequence("multipart-upload")?);
        let record = MultipartUploadRecord {
            bucket: input.bucket,
            content_type: normalized_content_type(input.content_type),
            initiated_epoch_seconds: self.now_epoch_seconds()?,
            key: input.key,
            metadata: normalize_metadata(input.metadata),
            parts: BTreeMap::new(),
            tags: normalize_tags(input.tags)?,
            upload_id: upload_id.clone(),
        };
        self.multipart_store
            .put(upload_id.clone(), record)
            .map_err(S3Error::MultipartUploads)?;

        Ok(CreateMultipartUploadOutput { upload_id })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn upload_part(
        &self,
        scope: &S3Scope,
        input: UploadPartInput,
    ) -> Result<UploadPartOutput, S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let mut upload = self.multipart_record(
            &input.bucket,
            &input.key,
            &input.upload_id,
        )?;
        if input.part_number == 0 {
            return Err(S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "Part number must be between 1 and 10000.".to_owned(),
                status_code: 400,
            });
        }

        let etag = format!("\"{:x}\"", md5::compute(&input.body));
        let part_key =
            multipart_part_blob_key(&input.upload_id, input.part_number);
        self.blob_store.put(&part_key, &input.body).map_err(S3Error::Blob)?;
        upload.parts.insert(
            input.part_number,
            MultipartPartRecord {
                etag: etag.clone(),
                size: input.body.len() as u64,
            },
        );
        self.multipart_store
            .put(input.upload_id, upload)
            .map_err(S3Error::MultipartUploads)?;

        Ok(UploadPartOutput { etag })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn complete_multipart_upload(
        &self,
        scope: &S3Scope,
        input: CompleteMultipartUploadInput,
    ) -> Result<CompleteMultipartUploadOutput, S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, &input.bucket)?;
        if input.parts.is_empty() {
            return Err(S3Error::InvalidArgument {
                code: "MalformedXML",
                message: "The XML you provided was not well-formed or did not validate against our published schema."
                    .to_owned(),
                status_code: 400,
            });
        }

        for pair in input.parts.windows(2) {
            let [left, right] = pair else {
                return Err(S3Error::Internal {
                    message:
                        "multipart part validation encountered an unexpected window size."
                            .to_owned(),
                });
            };
            if left.part_number >= right.part_number {
                return Err(S3Error::InvalidArgument {
                    code: "InvalidPartOrder",
                    message: "The list of parts was not in ascending order."
                        .to_owned(),
                    status_code: 400,
                });
            }
        }

        let upload = self.multipart_record(
            &input.bucket,
            &input.key,
            &input.upload_id,
        )?;
        let mut body = Vec::new();
        let mut etag_input = Vec::new();

        for part in &input.parts {
            let uploaded = upload.parts.get(&part.part_number).ok_or_else(|| {
                S3Error::InvalidArgument {
                    code: "InvalidPart",
                    message: format!(
                        "One or more of the specified parts could not be found. Part {} is missing.",
                        part.part_number
                    ),
                    status_code: 400,
                }
            })?;
            if part.etag.as_deref().is_some_and(|etag| etag != uploaded.etag) {
                return Err(S3Error::InvalidArgument {
                    code: "InvalidPart",
                    message:
                        "One or more of the specified parts could not be found."
                            .to_owned(),
                    status_code: 400,
                });
            }

            let part_body = self
                .blob_store
                .get(&multipart_part_blob_key(
                    &input.upload_id,
                    part.part_number,
                ))
                .map_err(S3Error::Blob)?
                .ok_or_else(|| S3Error::InvalidArgument {
                    code: "InvalidPart",
                    message:
                        "One or more of the specified parts could not be found."
                            .to_owned(),
                    status_code: 400,
                })?;
            body.extend_from_slice(&part_body);
            etag_input.extend_from_slice(&md5_hex_to_bytes(&uploaded.etag)?);
        }

        let composite_etag = format!(
            "\"{:x}-{}\"",
            md5::compute(etag_input),
            input.parts.len()
        );
        let stored = self.put_object_inner(
            scope,
            PutObjectInput {
                body,
                bucket: upload.bucket.clone(),
                content_type: Some(upload.content_type.clone()),
                key: upload.key.clone(),
                metadata: upload.metadata.clone(),
                object_lock: None,
                tags: upload.tags.clone(),
            },
            Some(composite_etag.clone()),
        )?;
        self.cleanup_multipart_upload(&upload.upload_id, &upload)?;
        self.emit_object_created_notification(
            scope,
            "ObjectCreated:CompleteMultipartUpload",
            &upload.bucket,
            &upload.key,
            stored.version_id.as_deref(),
        );

        Ok(CompleteMultipartUploadOutput {
            etag: composite_etag,
            last_modified_epoch_seconds: stored.last_modified_epoch_seconds,
            version_id: stored.version_id,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn abort_multipart_upload(
        &self,
        scope: &S3Scope,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        self.ensure_bucket_owned(scope, bucket)?;
        let upload = self.multipart_record(bucket, key, upload_id)?;
        self.cleanup_multipart_upload(upload_id, &upload)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_multipart_uploads(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<ListMultipartUploadsOutput, S3Error> {
        self.ensure_bucket_owned(scope, bucket)?;
        let bucket = bucket.to_owned();
        let mut uploads = self
            .multipart_store
            .scan(&|_| true)
            .into_iter()
            .filter(|upload| upload.bucket == bucket)
            .map(|upload| MultipartUploadSummary {
                initiated_epoch_seconds: upload.initiated_epoch_seconds,
                key: upload.key,
                upload_id: upload.upload_id,
            })
            .collect::<Vec<_>>();
        uploads.sort_by(|left, right| {
            left.key.cmp(&right.key).then(left.upload_id.cmp(&right.upload_id))
        });

        Ok(ListMultipartUploadsOutput { bucket, uploads })
    }

    fn cleanup_multipart_upload(
        &self,
        upload_id: &str,
        upload: &MultipartUploadRecord,
    ) -> Result<(), S3Error> {
        for part_number in upload.parts.keys().copied().collect::<Vec<_>>() {
            self.blob_store
                .delete(&multipart_part_blob_key(upload_id, part_number))
                .map_err(S3Error::Blob)?;
        }
        self.multipart_store
            .delete(&upload_id.to_owned())
            .map_err(S3Error::MultipartUploads)
    }

    fn create_delete_marker(
        &self,
        bucket: &str,
        key: &str,
        current: Option<ObjectRecord>,
    ) -> Result<DeleteObjectOutput, S3Error> {
        if let Some(mut current) = current {
            current.is_latest = false;
            self.persist_object_record(&current)?;
        }

        let version_id = format!(
            "{:020}",
            self.next_sequence(&format!("object-version:{bucket}:{key}"))?
        );
        let record = ObjectRecord {
            bucket: bucket.to_owned(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            delete_marker: true,
            etag: String::new(),
            is_latest: true,
            key: key.to_owned(),
            last_modified_epoch_seconds: self.now_epoch_seconds()?,
            legal_hold_status: None,
            metadata: BTreeMap::new(),
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            size: 0,
            tags: BTreeMap::new(),
            version_id: Some(version_id.clone()),
        };
        self.persist_object_record(&record)?;

        Ok(DeleteObjectOutput {
            delete_marker: true,
            version_id: Some(version_id),
        })
    }

    fn delete_null_version_if_present(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), S3Error> {
        if let Some(object) =
            self.resolve_nullable_version(bucket, key, NULL_VERSION_ID)
        {
            self.remove_object_record(&object)?;
        }

        Ok(())
    }

    fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        bypass_governance: bool,
        bypass_governance_authorized: bool,
    ) -> Result<DeleteObjectOutput, S3Error> {
        let object = self
            .resolve_nullable_version(bucket, key, version_id)
            .ok_or_else(|| S3Error::NoSuchVersion {
                version_id: version_id.to_owned(),
            })?;
        self.ensure_object_version_deletable(
            &object,
            bypass_governance && bypass_governance_authorized,
            self.now_epoch_seconds()?,
        )?;
        let was_latest = object.is_latest;
        let delete_marker = object.delete_marker;
        let returned_version_id = object
            .version_id
            .clone()
            .unwrap_or_else(|| NULL_VERSION_ID.to_owned());
        self.remove_object_record(&object)?;

        if was_latest {
            self.promote_latest_version(bucket, key)?;
        }

        Ok(DeleteObjectOutput {
            delete_marker,
            version_id: Some(returned_version_id),
        })
    }

    fn promote_latest_version(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), S3Error> {
        let Some(mut latest) = self
            .all_versions(bucket, key)
            .into_iter()
            .max_by(|left, right| {
                left.last_modified_epoch_seconds
                    .cmp(&right.last_modified_epoch_seconds)
                    .then_with(|| {
                        left.version_sort_key().cmp(&right.version_sort_key())
                    })
            })
        else {
            return Ok(());
        };
        latest.is_latest = true;
        self.persist_object_record(&latest)
    }

    fn put_object_inner(
        &self,
        scope: &S3Scope,
        input: PutObjectInput,
        etag_override: Option<String>,
    ) -> Result<PutObjectOutput, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, &input.bucket)?;
        let last_modified_epoch_seconds = self.now_epoch_seconds()?;
        let object_lock = resolve_object_lock_write(
            &bucket_record,
            input.object_lock.clone(),
            last_modified_epoch_seconds,
        )?;
        let metadata = normalize_metadata(input.metadata);
        let tags = normalize_tags(input.tags)?;
        let etag = etag_override
            .unwrap_or_else(|| format!("\"{:x}\"", md5::compute(&input.body)));
        let content_type = normalized_content_type(input.content_type);
        let mut version_id = None;

        match bucket_record.versioning_status {
            Some(BucketVersioningStatus::Enabled) => {
                if let Some(mut current) =
                    self.current_object_record(&input.bucket, &input.key)
                {
                    current.is_latest = false;
                    self.persist_object_record(&current)?;
                }
                let generated = format!(
                    "{:020}",
                    self.next_sequence(&format!(
                        "object-version:{}:{}",
                        input.bucket, input.key
                    ))?
                );
                let generated_version_id = Some(generated);
                version_id = generated_version_id.clone();
                let record = ObjectRecord {
                    bucket: input.bucket.clone(),
                    content_type,
                    delete_marker: false,
                    etag: etag.clone(),
                    is_latest: true,
                    key: input.key.clone(),
                    last_modified_epoch_seconds,
                    legal_hold_status: object_lock.legal_hold_status,
                    metadata,
                    object_lock_mode: object_lock.mode,
                    object_lock_retain_until_epoch_seconds: object_lock
                        .retain_until_epoch_seconds,
                    size: input.body.len() as u64,
                    tags,
                    version_id: generated_version_id,
                };
                self.blob_store
                    .put(&record.blob_key(), &input.body)
                    .map_err(S3Error::Blob)?;
                self.persist_object_record(&record)?;
            }
            Some(BucketVersioningStatus::Suspended) => {
                if let Some(mut current) =
                    self.current_object_record(&input.bucket, &input.key)
                    && current.version_id.as_deref() != Some(NULL_VERSION_ID)
                {
                    current.is_latest = false;
                    self.persist_object_record(&current)?;
                }

                let null_version_id = Some(NULL_VERSION_ID.to_owned());
                let record = ObjectRecord {
                    bucket: input.bucket.clone(),
                    content_type,
                    delete_marker: false,
                    etag: etag.clone(),
                    is_latest: true,
                    key: input.key.clone(),
                    last_modified_epoch_seconds,
                    legal_hold_status: object_lock.legal_hold_status,
                    metadata,
                    object_lock_mode: object_lock.mode,
                    object_lock_retain_until_epoch_seconds: object_lock
                        .retain_until_epoch_seconds,
                    size: input.body.len() as u64,
                    tags,
                    version_id: null_version_id.clone(),
                };
                if let Some(previous) = self.resolve_nullable_version(
                    &input.bucket,
                    &input.key,
                    NULL_VERSION_ID,
                ) {
                    self.remove_object_payload(&previous)?;
                }
                self.blob_store
                    .put(&record.blob_key(), &input.body)
                    .map_err(S3Error::Blob)?;
                self.persist_object_record(&record)?;
                version_id = null_version_id;
            }
            None => {
                if let Some(previous) =
                    self.current_object_record(&input.bucket, &input.key)
                {
                    self.remove_object_record(&previous)?;
                }
                let record = ObjectRecord {
                    bucket: input.bucket.clone(),
                    content_type,
                    delete_marker: false,
                    etag: etag.clone(),
                    is_latest: true,
                    key: input.key.clone(),
                    last_modified_epoch_seconds,
                    legal_hold_status: object_lock.legal_hold_status,
                    metadata,
                    object_lock_mode: object_lock.mode,
                    object_lock_retain_until_epoch_seconds: object_lock
                        .retain_until_epoch_seconds,
                    size: input.body.len() as u64,
                    tags,
                    version_id: None,
                };
                self.blob_store
                    .put(&record.blob_key(), &input.body)
                    .map_err(S3Error::Blob)?;
                self.persist_object_record(&record)?;
            }
        }

        Ok(PutObjectOutput { etag, last_modified_epoch_seconds, version_id })
    }

    fn put_bucket_subresource<F>(
        &self,
        scope: &S3Scope,
        bucket: &str,
        mutate: F,
    ) -> Result<(), S3Error>
    where
        F: FnOnce(&mut BucketRecord),
    {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        mutate(&mut bucket_record);
        self.bucket_store
            .put(bucket.to_owned(), bucket_record)
            .map_err(S3Error::Buckets)
    }

    fn collect_page(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        max_keys: Option<usize>,
        anchor: Option<String>,
    ) -> Result<ListPage, S3Error> {
        let prefix = prefix.unwrap_or_default();
        let max_keys = normalize_max_keys(max_keys);
        let mut entries = Vec::new();
        let mut common_prefixes = BTreeMap::new();

        for object in self.list_current_objects(bucket) {
            if !object.key.starts_with(&prefix) {
                continue;
            }

            if anchor.as_ref().is_some_and(|anchor| object.key <= *anchor) {
                continue;
            }

            if let Some(delimiter) = delimiter.as_deref() {
                let remainder = &object.key[prefix.len()..];
                if let Some(index) = remainder.find(delimiter) {
                    let common_prefix = format!(
                        "{prefix}{}",
                        &remainder[..index + delimiter.len()]
                    );
                    common_prefixes
                        .entry(common_prefix.clone())
                        .or_insert_with(|| ListedEntry::prefix(common_prefix));
                    continue;
                }
            }

            entries.push(ListedEntry::object(object.into_listed()));
        }

        entries.extend(common_prefixes.into_values());
        entries.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));

        let is_truncated = entries.len() > max_keys;
        let page_entries =
            entries.into_iter().take(max_keys).collect::<Vec<_>>();
        let next_anchor = if is_truncated {
            page_entries.last().map(|entry| entry.sort_key.clone())
        } else {
            None
        };
        let mut contents = Vec::new();
        let mut prefixes = Vec::new();

        for entry in page_entries {
            match entry.kind {
                ListedEntryKind::CommonPrefix(value) => prefixes.push(value),
                ListedEntryKind::Object(value) => contents.push(value),
            }
        }
        let object_count = contents.len();

        Ok(ListPage {
            common_prefixes: prefixes,
            contents,
            is_truncated,
            max_keys,
            next_anchor,
            object_count,
        })
    }

    fn list_current_objects(&self, bucket: &str) -> Vec<ObjectRecord> {
        let bucket = bucket.to_owned();
        let mut objects = self
            .object_store
            .scan(&move |candidate| candidate.bucket == bucket)
            .into_iter()
            .filter(|object| object.is_latest && !object.delete_marker)
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        objects
    }

    fn all_versions(&self, bucket: &str, key: &str) -> Vec<ObjectRecord> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        self.object_store.scan(&move |candidate| {
            candidate.bucket == bucket && candidate.key == key
        })
    }

    fn resolve_object_record(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectRecord, S3Error> {
        match version_id {
            Some(version_id) => self
                .resolve_nullable_version(bucket, key, version_id)
                .ok_or_else(|| S3Error::NoSuchVersion {
                    version_id: version_id.to_owned(),
                }),
            None => self
                .current_object_record(bucket, key)
                .ok_or_else(|| S3Error::NoSuchKey { key: key.to_owned() }),
        }
    }

    fn resolve_nullable_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Option<ObjectRecord> {
        if version_id == NULL_VERSION_ID {
            self.object_store.get(&ObjectStorageKey::new(
                bucket,
                key,
                Some(NULL_VERSION_ID.to_owned()),
            ))
        } else {
            self.object_store.get(&ObjectStorageKey::new(
                bucket,
                key,
                Some(version_id.to_owned()),
            ))
        }
    }

    fn current_object_record(
        &self,
        bucket: &str,
        key: &str,
    ) -> Option<ObjectRecord> {
        self.all_versions(bucket, key)
            .into_iter()
            .find(|object| object.is_latest)
    }

    fn multipart_record(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<MultipartUploadRecord, S3Error> {
        let upload =
            self.multipart_store.get(&upload_id.to_owned()).ok_or_else(
                || S3Error::NoSuchUpload { upload_id: upload_id.to_owned() },
            )?;
        if upload.bucket != bucket || upload.key != key {
            return Err(S3Error::NoSuchUpload {
                upload_id: upload_id.to_owned(),
            });
        }

        Ok(upload)
    }

    fn ensure_bucket_owned(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketRecord, S3Error> {
        let bucket =
            self.bucket_store.get(&bucket.to_owned()).ok_or_else(|| {
                S3Error::NoSuchBucket { bucket: bucket.to_owned() }
            })?;

        if bucket.owner_account_id != scope.account_id().as_str() {
            return Err(S3Error::AccessDenied {
                message: format!(
                    "Bucket `{}` is not owned by account {}.",
                    bucket.name,
                    scope.account_id()
                ),
            });
        }

        if bucket.region != scope.region().as_str() {
            return Err(S3Error::WrongRegion {
                bucket: bucket.name,
                region: bucket.region,
            });
        }

        Ok(bucket)
    }

    fn persist_object_record(
        &self,
        record: &ObjectRecord,
    ) -> Result<(), S3Error> {
        self.object_store
            .put(record.storage_key(), record.clone())
            .map_err(S3Error::Objects)
    }

    fn remove_object_record(
        &self,
        record: &ObjectRecord,
    ) -> Result<(), S3Error> {
        self.remove_object_payload(record)?;
        self.object_store
            .delete(&record.storage_key())
            .map_err(S3Error::Objects)
    }

    fn remove_object_payload(
        &self,
        record: &ObjectRecord,
    ) -> Result<(), S3Error> {
        if record.delete_marker {
            return Ok(());
        }

        self.blob_store.delete(&record.blob_key()).map_err(S3Error::Blob)
    }

    fn next_sequence(&self, name: &str) -> Result<u64, S3Error> {
        let key = name.to_owned();
        let next = self.sequence_store.get(&key).unwrap_or(0) + 1;
        self.sequence_store.put(key, next).map_err(S3Error::Sequences)?;
        Ok(next)
    }

    fn notification_transport(
        &self,
    ) -> Arc<dyn S3NotificationTransport + Send + Sync> {
        self.notification_transport
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn validate_notification_destinations(
        &self,
        scope: &S3Scope,
        bucket_record: &BucketRecord,
        configuration: &BucketNotificationConfiguration,
    ) -> Result<(), S3Error> {
        let transport = self.notification_transport();
        let (bucket_owner_account_id, bucket_region) =
            bucket_scope(bucket_record)?;

        for queue in &configuration.queue_configurations {
            transport.validate_destination(
                scope,
                &bucket_record.name,
                &bucket_owner_account_id,
                &bucket_region,
                &queue.queue_arn,
            )?;
        }
        for topic in &configuration.topic_configurations {
            transport.validate_destination(
                scope,
                &bucket_record.name,
                &bucket_owner_account_id,
                &bucket_region,
                &topic.topic_arn,
            )?;
        }

        Ok(())
    }

    fn emit_object_created_notification(
        &self,
        scope: &S3Scope,
        event_name: &str,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) {
        let Ok(bucket_record) = self.ensure_bucket_owned(scope, bucket) else {
            return;
        };
        if bucket_record.notification_configuration.is_empty() {
            return;
        }
        let object = version_id
            .and_then(|version_id| {
                self.resolve_nullable_version(bucket, key, version_id)
            })
            .or_else(|| self.current_object_record(bucket, key));
        let Some(object) = object else {
            return;
        };
        self.publish_matching_notifications(
            scope,
            &bucket_record,
            ObjectNotificationDetails {
                etag: (!object.etag.is_empty())
                    .then_some(object.etag.as_str()),
                event_name,
                event_time_epoch_seconds: object.last_modified_epoch_seconds,
                key,
                size: object.size,
                version_id: object.version_id.as_deref(),
            },
        );
    }

    fn emit_object_removed_notification(
        &self,
        scope: &S3Scope,
        bucket_record: &BucketRecord,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        delete_marker: bool,
    ) {
        if bucket_record.notification_configuration.is_empty() {
            return;
        }
        let sequencer = version_id
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{:016X}", key.len()));
        let Ok((bucket_owner_account_id, bucket_region)) =
            bucket_scope(bucket_record)
        else {
            return;
        };
        let transport = self.notification_transport();

        for queue in
            &bucket_record.notification_configuration.queue_configurations
        {
            if !notification_matches(
                &queue.events,
                "ObjectRemoved:Delete",
                key,
                queue.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn(bucket),
                bucket_name: bucket.to_owned(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: queue.id.clone(),
                destination_arn: queue.queue_arn.clone(),
                etag: None,
                event_name: "ObjectRemoved:Delete".to_owned(),
                event_time_epoch_seconds: bucket_record
                    .created_at_epoch_seconds,
                key: key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: 0,
                version_id: version_id
                    .map(str::to_owned)
                    .filter(|_| !delete_marker),
            });
        }

        for topic in
            &bucket_record.notification_configuration.topic_configurations
        {
            if !notification_matches(
                &topic.events,
                "ObjectRemoved:Delete",
                key,
                topic.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn(bucket),
                bucket_name: bucket.to_owned(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: topic.id.clone(),
                destination_arn: topic.topic_arn.clone(),
                etag: None,
                event_name: "ObjectRemoved:Delete".to_owned(),
                event_time_epoch_seconds: bucket_record
                    .created_at_epoch_seconds,
                key: key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: 0,
                version_id: version_id
                    .map(str::to_owned)
                    .filter(|_| !delete_marker),
            });
        }
    }

    fn publish_matching_notifications(
        &self,
        scope: &S3Scope,
        bucket_record: &BucketRecord,
        details: ObjectNotificationDetails<'_>,
    ) {
        let transport = self.notification_transport();
        let bucket_arn = bucket_arn(&bucket_record.name);
        let Ok((bucket_owner_account_id, bucket_region)) =
            bucket_scope(bucket_record)
        else {
            return;
        };
        let sequencer = notification_sequencer(
            details.version_id,
            details.event_time_epoch_seconds,
            details.key,
        );

        for queue in
            &bucket_record.notification_configuration.queue_configurations
        {
            if !notification_matches(
                &queue.events,
                details.event_name,
                details.key,
                queue.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn.clone(),
                bucket_name: bucket_record.name.clone(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: queue.id.clone(),
                destination_arn: queue.queue_arn.clone(),
                etag: details.etag.map(str::to_owned),
                event_name: details.event_name.to_owned(),
                event_time_epoch_seconds: details.event_time_epoch_seconds,
                key: details.key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: details.size,
                version_id: details.version_id.map(str::to_owned),
            });
        }

        for topic in
            &bucket_record.notification_configuration.topic_configurations
        {
            if !notification_matches(
                &topic.events,
                details.event_name,
                details.key,
                topic.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn.clone(),
                bucket_name: bucket_record.name.clone(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: topic.id.clone(),
                destination_arn: topic.topic_arn.clone(),
                etag: details.etag.map(str::to_owned),
                event_name: details.event_name.to_owned(),
                event_time_epoch_seconds: details.event_time_epoch_seconds,
                key: details.key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: details.size,
                version_id: details.version_id.map(str::to_owned),
            });
        }
    }

    fn ensure_object_version_deletable(
        &self,
        object: &ObjectRecord,
        bypass_governance: bool,
        now_epoch_seconds: u64,
    ) -> Result<(), S3Error> {
        if object.delete_marker {
            return Ok(());
        }
        if object.legal_hold_status == Some(LegalHoldStatus::On) {
            return Err(S3Error::AccessDenied {
                message:
                    "Access Denied because object protected by object lock."
                        .to_owned(),
            });
        }
        let Some(retain_until) = object.object_lock_retain_until_epoch_seconds
        else {
            return Ok(());
        };
        if retain_until <= now_epoch_seconds {
            return Ok(());
        }

        match object.object_lock_mode {
            Some(ObjectLockMode::Compliance) => Err(S3Error::AccessDenied {
                message:
                    "Access Denied because object protected by object lock."
                        .to_owned(),
            }),
            Some(ObjectLockMode::Governance) if !bypass_governance => {
                Err(S3Error::AccessDenied {
                    message:
                        "Access Denied because object protected by object lock."
                            .to_owned(),
                })
            }
            _ => Ok(()),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ()> {
        self.mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn now_epoch_seconds(&self) -> Result<u64, S3Error> {
        system_time_to_epoch_seconds(self.clock.now())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredBucketAclInput {
    Canned(CannedAcl),
    Xml(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CannedAcl {
    Private,
    PublicRead,
    PublicReadWrite,
}

impl std::str::FromStr for CannedAcl {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "private" => Ok(Self::Private),
            "public-read" => Ok(Self::PublicRead),
            "public-read-write" => Ok(Self::PublicReadWrite),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListPage {
    common_prefixes: Vec<String>,
    contents: Vec<ListedObject>,
    is_truncated: bool,
    max_keys: usize,
    next_anchor: Option<String>,
    object_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListedEntry {
    kind: ListedEntryKind,
    sort_key: String,
}

impl ListedEntry {
    fn object(value: ListedObject) -> Self {
        Self {
            sort_key: value.key.clone(),
            kind: ListedEntryKind::Object(value),
        }
    }

    fn prefix(value: String) -> Self {
        Self {
            sort_key: value.clone(),
            kind: ListedEntryKind::CommonPrefix(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ListedEntryKind {
    CommonPrefix(String),
    Object(ListedObject),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BucketRecord {
    acl_configuration: Option<StoredBucketAcl>,
    cors_configuration: Option<String>,
    created_at_epoch_seconds: u64,
    encryption_configuration: Option<String>,
    lifecycle_configuration: Option<String>,
    name: String,
    #[serde(default)]
    notification_configuration: BucketNotificationConfiguration,
    #[serde(default)]
    object_lock_configuration: Option<StoredObjectLockConfiguration>,
    #[serde(default)]
    object_lock_enabled: bool,
    owner_account_id: String,
    policy: Option<String>,
    region: String,
    tags: BTreeMap<String, String>,
    versioning_status: Option<BucketVersioningStatus>,
}

impl BucketRecord {
    fn into_bucket(self) -> ListedBucket {
        ListedBucket {
            created_at_epoch_seconds: self.created_at_epoch_seconds,
            name: self.name,
            owner_account_id: self.owner_account_id,
            region: self.region,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum StoredBucketAcl {
    Canned(CannedAcl),
    Xml(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredObjectLockConfiguration {
    mode: ObjectLockMode,
    period: DefaultRetentionPeriod,
}

impl From<DefaultObjectLockRetention> for StoredObjectLockConfiguration {
    fn from(value: DefaultObjectLockRetention) -> Self {
        Self { mode: value.mode, period: value.period }
    }
}

impl From<StoredObjectLockConfiguration> for DefaultObjectLockRetention {
    fn from(value: StoredObjectLockConfiguration) -> Self {
        Self { mode: value.mode, period: value.period }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct ObjectStorageKey {
    bucket: String,
    key: String,
    version_id: Option<String>,
}

impl ObjectStorageKey {
    fn new(bucket: &str, key: &str, version_id: Option<String>) -> Self {
        Self { bucket: bucket.to_owned(), key: key.to_owned(), version_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ObjectRecord {
    bucket: String,
    content_type: String,
    delete_marker: bool,
    etag: String,
    is_latest: bool,
    key: String,
    last_modified_epoch_seconds: u64,
    #[serde(default)]
    legal_hold_status: Option<LegalHoldStatus>,
    metadata: BTreeMap<String, String>,
    #[serde(default)]
    object_lock_mode: Option<ObjectLockMode>,
    #[serde(default)]
    object_lock_retain_until_epoch_seconds: Option<u64>,
    size: u64,
    tags: BTreeMap<String, String>,
    version_id: Option<String>,
}

impl ObjectRecord {
    fn storage_key(&self) -> ObjectStorageKey {
        ObjectStorageKey::new(&self.bucket, &self.key, self.version_id.clone())
    }

    fn blob_key(&self) -> BlobKey {
        object_blob_key(&self.bucket, &self.key, self.version_id.as_deref())
    }

    fn into_head(self) -> HeadObjectOutput {
        HeadObjectOutput {
            content_type: self.content_type,
            delete_marker: self.delete_marker,
            etag: self.etag,
            key: self.key,
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            metadata: self.metadata,
            object_lock_legal_hold_status: self.legal_hold_status,
            object_lock_mode: self.object_lock_mode,
            object_lock_retain_until_epoch_seconds: self
                .object_lock_retain_until_epoch_seconds,
            size: self.size,
            version_id: self.version_id,
        }
    }

    fn into_listed(self) -> ListedObject {
        ListedObject {
            etag: self.etag,
            key: self.key,
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            size: self.size,
            storage_class: STANDARD_STORAGE_CLASS.to_owned(),
        }
    }

    fn into_delete_marker(self) -> ListedDeleteMarker {
        ListedDeleteMarker {
            is_latest: self.is_latest,
            key: self.key,
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            version_id: self
                .version_id
                .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
        }
    }

    fn into_listed_version(self) -> ListedObjectVersion {
        ListedObjectVersion {
            etag: self.etag,
            is_latest: self.is_latest,
            key: self.key,
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            size: self.size,
            storage_class: STANDARD_STORAGE_CLASS.to_owned(),
            version_id: self
                .version_id
                .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
        }
    }

    fn version_sort_key(&self) -> String {
        self.version_id.clone().unwrap_or_else(|| NULL_VERSION_ID.to_owned())
    }

    fn retention(&self) -> Option<ObjectRetention> {
        self.object_lock_mode
            .zip(self.object_lock_retain_until_epoch_seconds)
            .map(|(mode, retain_until_epoch_seconds)| ObjectRetention {
                mode: Some(mode),
                retain_until_epoch_seconds: Some(retain_until_epoch_seconds),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MultipartUploadRecord {
    bucket: String,
    content_type: String,
    initiated_epoch_seconds: u64,
    key: String,
    metadata: BTreeMap<String, String>,
    parts: BTreeMap<u16, MultipartPartRecord>,
    tags: BTreeMap<String, String>,
    upload_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MultipartPartRecord {
    etag: String,
    size: u64,
}

#[derive(Debug)]
struct NoopS3NotificationTransport;

impl S3NotificationTransport for NoopS3NotificationTransport {
    fn publish(&self, _notification: &S3EventNotification) {}
}

impl ListedVersionEntry {
    fn sort_key(&self) -> &str {
        match self {
            Self::DeleteMarker(value) => &value.key,
            Self::Version(value) => &value.key,
        }
    }

    fn version_sort_key(&self) -> &str {
        match self {
            Self::DeleteMarker(value) => &value.version_id,
            Self::Version(value) => &value.version_id,
        }
    }
}

fn bucket_arn(bucket: &str) -> Arn {
    Arn::trusted_new(
        Partition::aws(),
        ServiceName::S3,
        None,
        None,
        ArnResource::Generic(bucket.to_owned()),
    )
}

fn bucket_scope(
    bucket_record: &BucketRecord,
) -> Result<(AccountId, RegionId), S3Error> {
    let owner_account_id =
        bucket_record.owner_account_id.parse().map_err(|error| {
            S3Error::Internal {
                message: format!(
                    "bucket {} stored an invalid owner account id {}: {error}",
                    bucket_record.name, bucket_record.owner_account_id
                ),
            }
        })?;
    let region =
        bucket_record.region.parse().map_err(|error| S3Error::Internal {
            message: format!(
                "bucket {} stored an invalid region {}: {error}",
                bucket_record.name, bucket_record.region
            ),
        })?;

    Ok((owner_account_id, region))
}

fn notification_matches(
    configured_events: &[String],
    event_name: &str,
    key: &str,
    filter: Option<&NotificationFilter>,
) -> bool {
    configured_events
        .iter()
        .any(|configured| notification_event_matches(configured, event_name))
        && notification_filter_matches(filter, key)
}

fn notification_event_matches(configured: &str, event_name: &str) -> bool {
    let configured = configured.strip_prefix("s3:").unwrap_or(configured);
    if let Some(prefix) = configured.strip_suffix('*') {
        event_name.starts_with(prefix)
    } else {
        configured == event_name
    }
}

fn notification_filter_matches(
    filter: Option<&NotificationFilter>,
    key: &str,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter.prefix.as_deref().is_some_and(|prefix| !key.starts_with(prefix))
    {
        return false;
    }
    if filter.suffix.as_deref().is_some_and(|suffix| !key.ends_with(suffix)) {
        return false;
    }
    true
}

fn notification_sequencer(
    version_id: Option<&str>,
    event_time_epoch_seconds: u64,
    key: &str,
) -> String {
    let seed = format!(
        "{}:{event_time_epoch_seconds}:{key}",
        version_id.unwrap_or(NULL_VERSION_ID)
    );
    format!("{:x}", md5::compute(seed)).to_uppercase()
}

fn validate_notification_configuration(
    configuration: &BucketNotificationConfiguration,
) -> Result<(), S3Error> {
    for queue in &configuration.queue_configurations {
        if queue.events.is_empty() {
            return Err(malformed_xml_s3_error());
        }
        validate_notification_events(&queue.events)?;
        validate_notification_filter(queue.filter.as_ref())?;
    }
    for topic in &configuration.topic_configurations {
        if topic.events.is_empty() {
            return Err(malformed_xml_s3_error());
        }
        validate_notification_events(&topic.events)?;
        validate_notification_filter(topic.filter.as_ref())?;
    }

    Ok(())
}

fn validate_notification_events(events: &[String]) -> Result<(), S3Error> {
    for event in events {
        if !event.starts_with("s3:") {
            return Err(S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: format!("Unsupported notification event `{event}`."),
                status_code: 400,
            });
        }
    }
    Ok(())
}

fn validate_notification_filter(
    filter: Option<&NotificationFilter>,
) -> Result<(), S3Error> {
    let Some(filter) = filter else {
        return Ok(());
    };
    if filter.prefix.as_deref().is_some_and(|prefix| prefix.is_empty()) {
        return Err(malformed_xml_s3_error());
    }
    if filter.suffix.as_deref().is_some_and(|suffix| suffix.is_empty()) {
        return Err(malformed_xml_s3_error());
    }
    Ok(())
}

fn ensure_bucket_object_lock_enabled(
    bucket: &BucketRecord,
) -> Result<(), S3Error> {
    if bucket.object_lock_enabled {
        Ok(())
    } else {
        Err(S3Error::InvalidArgument {
            code: "InvalidRequest",
            message: "Bucket is missing Object Lock Configuration".to_owned(),
            status_code: 400,
        })
    }
}

fn resolve_object_lock_write(
    bucket: &BucketRecord,
    requested: Option<ObjectLockWriteOptions>,
    last_modified_epoch_seconds: u64,
) -> Result<ObjectLockWriteOptions, S3Error> {
    let mut resolved = requested.unwrap_or(ObjectLockWriteOptions {
        legal_hold_status: None,
        mode: None,
        retain_until_epoch_seconds: None,
    });
    let has_retention = resolved.mode.is_some()
        || resolved.retain_until_epoch_seconds.is_some();
    let has_requested_lock =
        has_retention || resolved.legal_hold_status.is_some();
    if has_requested_lock && !bucket.object_lock_enabled {
        return Err(S3Error::InvalidArgument {
            code: "InvalidRequest",
            message: "Bucket is missing Object Lock Configuration".to_owned(),
            status_code: 400,
        });
    }
    if resolved.mode.is_some() ^ resolved.retain_until_epoch_seconds.is_some()
    {
        return Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied"
                .to_owned(),
            status_code: 400,
        });
    }
    if let Some(retain_until_epoch_seconds) =
        resolved.retain_until_epoch_seconds
        && retain_until_epoch_seconds <= last_modified_epoch_seconds
    {
        return Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The retain until date must be in the future.".to_owned(),
            status_code: 400,
        });
    }
    if bucket.object_lock_enabled
        && !has_retention
        && let Some(default_retention) =
            bucket.object_lock_configuration.as_ref()
    {
        resolved.mode = Some(default_retention.mode);
        resolved.retain_until_epoch_seconds =
            Some(default_retention_epoch_seconds(
                &default_retention.period,
                last_modified_epoch_seconds,
            ));
    }

    Ok(resolved)
}

fn default_retention_epoch_seconds(
    period: &DefaultRetentionPeriod,
    created_at_epoch_seconds: u64,
) -> u64 {
    match period {
        DefaultRetentionPeriod::Days(days) => created_at_epoch_seconds
            .saturating_add(u64::from(*days).saturating_mul(24 * 60 * 60)),
        DefaultRetentionPeriod::Years(years) => created_at_epoch_seconds
            .saturating_add(
                u64::from(*years).saturating_mul(365 * 24 * 60 * 60),
            ),
    }
}

fn apply_object_retention(
    object: &mut ObjectRecord,
    retention: ObjectRetention,
    now_epoch_seconds: u64,
    bypass_governance: bool,
) -> Result<(), S3Error> {
    if retention.mode.is_some()
        ^ retention.retain_until_epoch_seconds.is_some()
    {
        return Err(malformed_xml_s3_error());
    }
    let current_mode = object.object_lock_mode;
    let current_retain_until = object.object_lock_retain_until_epoch_seconds;
    let current_active = current_retain_until
        .is_some_and(|retain_until| retain_until > now_epoch_seconds);

    match current_mode {
        Some(ObjectLockMode::Compliance) if current_active => {
            if retention.mode != Some(ObjectLockMode::Compliance)
                || retention.retain_until_epoch_seconds.unwrap_or(0)
                    < current_retain_until.unwrap_or(0)
            {
                return Err(S3Error::AccessDenied {
                    message:
                        "Access Denied because object protected by object lock."
                            .to_owned(),
                });
            }
        }
        Some(ObjectLockMode::Governance)
            if current_active && !bypass_governance =>
        {
            if retention.retain_until_epoch_seconds.unwrap_or(0)
                < current_retain_until.unwrap_or(0)
                || retention.mode.is_none()
            {
                return Err(S3Error::AccessDenied {
                    message:
                        "Access Denied because object protected by object lock."
                            .to_owned(),
                });
            }
        }
        _ => {}
    }

    if let Some(retain_until_epoch_seconds) =
        retention.retain_until_epoch_seconds
        && retain_until_epoch_seconds <= now_epoch_seconds
    {
        return Err(S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The retain until date must be in the future.".to_owned(),
            status_code: 400,
        });
    }

    object.object_lock_mode = retention.mode;
    object.object_lock_retain_until_epoch_seconds =
        retention.retain_until_epoch_seconds;
    Ok(())
}

fn malformed_xml_s3_error() -> S3Error {
    S3Error::InvalidArgument {
        code: "MalformedXML",
        message: "The XML you provided was not well-formed or did not validate against our published schema."
            .to_owned(),
        status_code: 400,
    }
}

fn evaluate_csv_select(
    body: &[u8],
    input: &SelectObjectContentInput,
) -> Result<SelectObjectContentOutput, S3Error> {
    let query = parse_select_query(&input.expression)?;
    let mut reader = ReaderBuilder::new();
    let has_headers = matches!(
        input.csv_input.file_header_info,
        CsvFileHeaderInfo::Ignore | CsvFileHeaderInfo::Use
    );
    reader
        .has_headers(has_headers)
        .delimiter(csv_byte(
            input.csv_input.field_delimiter,
            "FieldDelimiter",
        )?)
        .quote(csv_byte(input.csv_input.quote_character, "QuoteCharacter")?)
        .escape(Some(csv_byte(
            input.csv_input.quote_escape_character,
            "QuoteEscapeCharacter",
        )?))
        .terminator(Terminator::Any(csv_byte(
            input.csv_input.record_delimiter,
            "RecordDelimiter",
        )?));
    if let Some(comment) = input.csv_input.comments {
        reader.comment(Some(csv_byte(comment, "Comments")?));
    }

    let mut reader = reader.from_reader(body);
    let all_headers = if has_headers {
        Some(
            reader
                .headers()
                .map_err(|error| {
                    invalid_select_input_error(error.to_string())
                })?
                .iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let visible_headers =
        matches!(input.csv_input.file_header_info, CsvFileHeaderInfo::Use)
            .then(|| all_headers.clone().unwrap_or_default());

    let mut writer = WriterBuilder::new();
    writer
        .delimiter(csv_byte(
            input.csv_output.field_delimiter,
            "FieldDelimiter",
        )?)
        .quote(csv_byte(input.csv_output.quote_character, "QuoteCharacter")?)
        .escape(csv_byte(
            input.csv_output.quote_escape_character,
            "QuoteEscapeCharacter",
        )?)
        .quote_style(if input.csv_output.always_quote {
            csv::QuoteStyle::Always
        } else {
            csv::QuoteStyle::Necessary
        })
        .terminator(Terminator::Any(csv_byte(
            input.csv_output.record_delimiter,
            "RecordDelimiter",
        )?));
    let mut writer = writer.from_writer(Vec::new());

    let mut matched = 0usize;
    for record in reader.records() {
        let record = record
            .map_err(|error| invalid_select_input_error(error.to_string()))?;
        if !query.matches(&record, visible_headers.as_deref())? {
            continue;
        }
        query.write_projection(
            &record,
            visible_headers.as_deref(),
            &mut writer,
        )?;
        matched += 1;
        if query.limit.is_some_and(|limit| matched >= limit) {
            break;
        }
    }

    let records = String::from_utf8(
        writer
            .into_inner()
            .map_err(|error| invalid_select_input_error(error.to_string()))?,
    )
    .map_err(|error| invalid_select_input_error(error.to_string()))?;

    Ok(SelectObjectContentOutput {
        stats: SelectObjectStats {
            bytes_processed: body.len() as u64,
            bytes_returned: records.len() as u64,
            bytes_scanned: body.len() as u64,
        },
        records,
    })
}

fn csv_byte(value: char, name: &str) -> Result<u8, S3Error> {
    if !value.is_ascii() {
        return Err(invalid_select_input_error(format!(
            "{name} must be a single-byte ASCII character."
        )));
    }
    Ok(value as u8)
}

#[derive(Debug, Clone, PartialEq)]
struct SelectQuery {
    condition: Option<SelectCondition>,
    limit: Option<usize>,
    projection: SelectProjection,
}

impl SelectQuery {
    fn matches(
        &self,
        record: &csv::StringRecord,
        headers: Option<&[String]>,
    ) -> Result<bool, S3Error> {
        let Some(condition) = &self.condition else {
            return Ok(true);
        };
        let lhs = select_record_value(record, headers, &condition.column)?;
        Ok(condition.matches(lhs))
    }

    fn write_projection(
        &self,
        record: &csv::StringRecord,
        headers: Option<&[String]>,
        writer: &mut csv::Writer<Vec<u8>>,
    ) -> Result<(), S3Error> {
        match &self.projection {
            SelectProjection::All => {
                writer.write_record(record.iter()).map_err(|error| {
                    invalid_select_input_error(error.to_string())
                })
            }
            SelectProjection::Columns(columns) => {
                let values = columns
                    .iter()
                    .map(|column| {
                        select_record_value(record, headers, column)
                            .map(str::to_owned)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                writer.write_record(values).map_err(|error| {
                    invalid_select_input_error(error.to_string())
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SelectProjection {
    All,
    Columns(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
struct SelectCondition {
    column: String,
    operator: SelectOperator,
    value: SelectValue,
}

impl SelectCondition {
    fn matches(&self, lhs: &str) -> bool {
        match (&self.operator, &self.value) {
            (SelectOperator::Eq, SelectValue::String(rhs)) => lhs == rhs,
            (SelectOperator::Eq, SelectValue::Number(rhs)) => lhs
                .parse::<f64>()
                .ok()
                .is_some_and(|lhs| (lhs - rhs).abs() < f64::EPSILON),
            (SelectOperator::Gt, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs > *rhs)
            }
            (SelectOperator::Ge, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs >= *rhs)
            }
            (SelectOperator::Lt, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs < *rhs)
            }
            (SelectOperator::Le, SelectValue::Number(rhs)) => {
                lhs.parse::<f64>().ok().is_some_and(|lhs| lhs <= *rhs)
            }
            (SelectOperator::Gt, SelectValue::String(rhs)) => lhs > rhs,
            (SelectOperator::Ge, SelectValue::String(rhs)) => lhs >= rhs,
            (SelectOperator::Lt, SelectValue::String(rhs)) => lhs < rhs,
            (SelectOperator::Le, SelectValue::String(rhs)) => lhs <= rhs,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SelectOperator {
    Eq,
    Ge,
    Gt,
    Le,
    Lt,
}

#[derive(Debug, Clone, PartialEq)]
enum SelectValue {
    Number(f64),
    String(String),
}

fn parse_select_query(expression: &str) -> Result<SelectQuery, S3Error> {
    static SELECT_REGEX: std::sync::OnceLock<Result<Regex, String>> =
        std::sync::OnceLock::new();
    static WHERE_REGEX: std::sync::OnceLock<Result<Regex, String>> =
        std::sync::OnceLock::new();

    let select_regex = SELECT_REGEX
        .get_or_init(|| {
            Regex::new(
                "(?is)^select\\s+(.+?)\\s+from\\s+s3object(?:\\s+([A-Za-z_][A-Za-z0-9_]*))?(?:\\s+where\\s+(.+?))?(?:\\s+limit\\s+(\\d+))?\\s*$",
            )
            .map_err(|error| {
                format!("select regex failed to compile: {error}")
            })
        })
        .as_ref()
        .map_err(|message| S3Error::Internal {
            message: message.clone(),
        })?;
    let where_regex = WHERE_REGEX
        .get_or_init(|| {
            Regex::new("(?is)^(.+?)\\s*(<=|>=|=|<|>)\\s*(.+?)\\s*$").map_err(
                |error| format!("where regex failed to compile: {error}"),
            )
        })
        .as_ref()
        .map_err(|message| S3Error::Internal { message: message.clone() })?;

    let captures =
        select_regex.captures(expression.trim()).ok_or_else(|| {
            invalid_select_input_error(
                "Unsupported SELECT expression.".to_owned(),
            )
        })?;

    let alias = captures.get(2).map(|capture| capture.as_str().to_owned());
    let projection = captures
        .get(1)
        .ok_or_else(|| {
            invalid_select_input_error(
                "Unsupported SELECT projection.".to_owned(),
            )
        })?
        .as_str()
        .trim();
    let projection = if projection == "*" {
        SelectProjection::All
    } else {
        SelectProjection::Columns(
            projection
                .split(',')
                .map(|column| {
                    normalize_select_identifier(
                        column.trim(),
                        alias.as_deref(),
                    )
                })
                .collect(),
        )
    };
    let condition = captures
        .get(3)
        .map(|capture| {
            let where_captures = where_regex
                .captures(capture.as_str().trim())
                .ok_or_else(|| {
                    invalid_select_input_error(
                        "Unsupported WHERE clause.".to_owned(),
                    )
                })?;
            let column = normalize_select_identifier(
                where_captures
                    .get(1)
                    .ok_or_else(|| {
                        invalid_select_input_error(
                            "Unsupported WHERE left-hand side.".to_owned(),
                        )
                    })?
                    .as_str(),
                alias.as_deref(),
            );
            let operator = match where_captures
                .get(2)
                .ok_or_else(|| {
                    invalid_select_input_error(
                        "Unsupported WHERE operator.".to_owned(),
                    )
                })?
                .as_str()
            {
                "=" => SelectOperator::Eq,
                ">" => SelectOperator::Gt,
                ">=" => SelectOperator::Ge,
                "<" => SelectOperator::Lt,
                "<=" => SelectOperator::Le,
                _ => {
                    return Err(invalid_select_input_error(
                        "Unsupported WHERE operator.".to_owned(),
                    ));
                }
            };
            let raw_value = where_captures
                .get(3)
                .ok_or_else(|| {
                    invalid_select_input_error(
                        "Unsupported WHERE right-hand side.".to_owned(),
                    )
                })?
                .as_str()
                .trim();
            let value = if raw_value.starts_with('\'')
                && raw_value.ends_with('\'')
                && raw_value.len() >= 2
            {
                SelectValue::String(
                    raw_value[1..raw_value.len() - 1].replace("''", "'"),
                )
            } else {
                SelectValue::Number(raw_value.parse::<f64>().map_err(
                    |_| {
                        invalid_select_input_error(
                            "Unsupported WHERE value.".to_owned(),
                        )
                    },
                )?)
            };

            Ok(SelectCondition { column, operator, value })
        })
        .transpose()?;
    let limit = captures
        .get(4)
        .map(|capture| {
            capture.as_str().parse::<usize>().map_err(|_| {
                invalid_select_input_error(
                    "LIMIT must be a positive integer.".to_owned(),
                )
            })
        })
        .transpose()?;

    Ok(SelectQuery { condition, limit, projection })
}

fn normalize_select_identifier(value: &str, alias: Option<&str>) -> String {
    let value = value.trim();
    let value = alias
        .and_then(|alias| value.strip_prefix(&format!("{alias}.")))
        .unwrap_or(value);
    value.trim_matches('"').to_owned()
}

fn select_record_value<'a>(
    record: &'a csv::StringRecord,
    headers: Option<&[String]>,
    column: &str,
) -> Result<&'a str, S3Error> {
    if let Some(position) = column.strip_prefix('_') {
        let index = position.parse::<usize>().map_err(|_| {
            invalid_select_input_error(format!(
                "Unsupported column `{column}`."
            ))
        })?;
        return record.get(index.saturating_sub(1)).ok_or_else(|| {
            invalid_select_input_error(format!(
                "Column `{column}` is out of bounds."
            ))
        });
    }

    let headers = headers.ok_or_else(|| {
        invalid_select_input_error(format!(
            "Column `{column}` requires CSV headers."
        ))
    })?;
    let index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            invalid_select_input_error(format!(
                "Column `{column}` does not exist."
            ))
        })?;
    record.get(index).ok_or_else(|| {
        invalid_select_input_error(format!(
            "Column `{column}` does not exist."
        ))
    })
}

fn invalid_select_input_error(message: String) -> S3Error {
    S3Error::InvalidArgument {
        code: "InvalidArgument",
        message,
        status_code: 400,
    }
}

fn object_blob_key(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> BlobKey {
    match version_id {
        Some(version_id) => BlobKey::new(
            format!("{bucket}-versions"),
            format!("{version_id}:{key}"),
        ),
        None => BlobKey::new(bucket, key),
    }
}

fn multipart_part_blob_key(upload_id: &str, part_number: u16) -> BlobKey {
    BlobKey::new(format!("multipart-{upload_id}"), format!("{part_number:05}"))
}

fn md5_hex_to_bytes(etag: &str) -> Result<[u8; 16], S3Error> {
    let value = etag.trim_matches('"');
    if value.len() != 32 {
        return Err(S3Error::Internal {
            message: format!("unexpected multipart ETag shape: {etag}"),
        });
    }

    let mut bytes = [0; 16];
    for (slot, chunk) in bytes.iter_mut().zip(value.as_bytes().chunks(2)) {
        let [high, low] = chunk else {
            return Err(S3Error::Internal {
                message: format!("unexpected multipart ETag shape: {etag}"),
            });
        };
        *slot = (hex_value(*high)? << 4) | hex_value(*low)?;
    }
    Ok(bytes)
}

fn hex_value(value: u8) -> Result<u8, S3Error> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(S3Error::Internal {
            message: format!("invalid hex digit `{}` in ETag", value as char),
        }),
    }
}

fn decode_continuation_token(token: &str) -> Result<String, S3Error> {
    base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_| S3Error::InvalidArgument {
            code: "InvalidArgument",
            message: "The continuation token is not valid.".to_owned(),
            status_code: 400,
        })
        .and_then(|bytes| {
            String::from_utf8(bytes).map_err(|_| S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "The continuation token is not valid.".to_owned(),
                status_code: 400,
            })
        })
}

fn encode_continuation_token(value: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

fn normalize_max_keys(max_keys: Option<usize>) -> usize {
    max_keys.unwrap_or(DEFAULT_MAX_KEYS).clamp(1, MAX_KEYS_LIMIT)
}

fn normalize_metadata(
    metadata: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    metadata
        .into_iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value))
        .collect()
}

fn normalize_tags(
    tags: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, S3Error> {
    if tags.len() > 10 {
        return Err(S3Error::InvalidArgument {
            code: "InvalidTag",
            message: "The TagSet may contain at most 10 tags.".to_owned(),
            status_code: 400,
        });
    }
    if tags.keys().any(|key| key.trim().is_empty()) {
        return Err(S3Error::InvalidArgument {
            code: "InvalidTag",
            message: "Tag keys must not be empty.".to_owned(),
            status_code: 400,
        });
    }

    Ok(tags)
}

fn normalized_content_type(content_type: Option<String>) -> String {
    content_type
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned())
}

fn canned_acl_xml(owner_account_id: &str, acl: CannedAcl) -> String {
    let mut xml = String::from(
        "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner>",
    );
    xml.push_str(&format!(
        "<ID>{owner_account_id}</ID><DisplayName>{owner_account_id}</DisplayName></Owner><AccessControlList>"
    ));
    xml.push_str(&format!(
        "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{owner_account_id}</ID><DisplayName>{owner_account_id}</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant>"
    ));

    match acl {
        CannedAcl::Private => {}
        CannedAcl::PublicRead => {
            xml.push_str("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>");
        }
        CannedAcl::PublicReadWrite => {
            xml.push_str("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>");
            xml.push_str("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>WRITE</Permission></Grant>");
        }
    }

    xml.push_str("</AccessControlList></AccessControlPolicy>");
    xml
}

fn system_time_to_epoch_seconds(time: SystemTime) -> Result<u64, S3Error> {
    time.duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).map_err(
        |_| S3Error::Internal {
            message: "S3 clock produced a timestamp before the Unix epoch."
                .to_owned(),
        },
    )
}

fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    if name.trim().is_empty() {
        return Err(S3Error::InvalidArgument {
            code: "InvalidBucketName",
            message: "The specified bucket is not valid.".to_owned(),
            status_code: 400,
        });
    }

    if name.chars().any(char::is_control) {
        return Err(S3Error::InvalidArgument {
            code: "InvalidBucketName",
            message: "The specified bucket is not valid.".to_owned(),
            status_code: 400,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BucketNotificationConfiguration, BucketObjectLockConfiguration,
        BucketVersioningStatus, CompleteMultipartUploadInput,
        CompletedMultipartPart, CopyObjectInput, CreateBucketInput,
        CreateMultipartUploadInput, CsvFileHeaderInfo, CsvInputSerialization,
        CsvOutputSerialization, DefaultObjectLockRetention,
        DefaultRetentionPeriod, DeleteObjectInput, HeadObjectOutput,
        LegalHoldStatus, ListObjectVersionsInput, ListObjectsInput,
        ListObjectsV2Input, NotificationFilter, ObjectLockMode,
        ObjectRetention, PutObjectInput, PutObjectLegalHoldInput,
        PutObjectRetentionInput, QueueNotificationConfiguration, S3Error,
        S3EventNotification, S3NotificationTransport, S3Scope, S3Service,
        SelectObjectContentInput, StoredBucketAclInput, TaggingInput,
        TopicNotificationConfiguration,
    };
    use crate::{BlobKey, BlobStore, Clock, InfrastructureError};
    use aws::{AccountId, BlobMetadata, RegionId};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug, Default)]
    struct TestBlobStore {
        blobs: Mutex<BTreeMap<BlobKey, Vec<u8>>>,
    }

    impl BlobStore for TestBlobStore {
        fn put(
            &self,
            key: &BlobKey,
            body: &[u8],
        ) -> Result<BlobMetadata, InfrastructureError> {
            self.blobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key.clone(), body.to_vec());
            Ok(BlobMetadata::new(key.clone(), body.len()))
        }

        fn get(
            &self,
            key: &BlobKey,
        ) -> Result<Option<Vec<u8>>, InfrastructureError> {
            Ok(self
                .blobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .cloned())
        }

        fn delete(&self, key: &BlobKey) -> Result<(), InfrastructureError> {
            self.blobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(key);
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct TestClock {
        now: SystemTime,
    }

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            self.now
        }
    }

    #[derive(Debug, Default)]
    struct RecordingNotificationTransport {
        published: Mutex<Vec<S3EventNotification>>,
    }

    impl RecordingNotificationTransport {
        fn published(&self) -> Vec<S3EventNotification> {
            self.published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl S3NotificationTransport for RecordingNotificationTransport {
        fn publish(&self, notification: &S3EventNotification) {
            self.published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(notification.clone());
        }
    }

    fn scope() -> S3Scope {
        scope_in_region("eu-west-2")
    }

    fn scope_in_region(region: &str) -> S3Scope {
        S3Scope::new(
            "000000000000".parse::<AccountId>().expect("account should parse"),
            region.parse::<RegionId>().expect("region should parse"),
        )
    }

    fn service(now: u64) -> S3Service {
        let factory = StorageFactory::new(StorageConfig::new(
            std::env::temp_dir().join("cloudish-s3-service-tests"),
            StorageMode::Memory,
        ));

        S3Service::new(
            &factory,
            Arc::new(TestBlobStore::default()),
            Arc::new(TestClock { now: UNIX_EPOCH + Duration::from_secs(now) }),
        )
        .expect("S3 service should build")
    }

    fn create_bucket(service: &S3Service, scope: &S3Scope, name: &str) {
        service
            .create_bucket(
                scope,
                CreateBucketInput {
                    name: name.to_owned(),
                    object_lock_enabled: false,
                    region: scope.region().clone(),
                },
            )
            .expect("bucket should be created");
    }

    #[test]
    fn s3_core_get_and_head_share_metadata_but_get_returns_payload() {
        let service = service(1_710_000_000);
        let scope = scope();
        create_bucket(&service, &scope, "demo");

        let put = service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"payload".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: Some("text/plain".to_owned()),
                    key: "notes.txt".to_owned(),
                    metadata: BTreeMap::from([(
                        "Trace".to_owned(),
                        "abc123".to_owned(),
                    )]),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("object put should succeed");
        let head = service
            .head_object(&scope, "demo", "notes.txt", None)
            .expect("head should succeed");
        let get = service
            .get_object(&scope, "demo", "notes.txt", None)
            .expect("get should succeed");

        assert_eq!(put.etag, head.etag);
        assert_eq!(head, get.head);
        assert_eq!(head.content_type, "text/plain");
        assert_eq!(head.version_id, None);
        assert_eq!(
            head.metadata,
            BTreeMap::from([("trace".to_owned(), "abc123".to_owned())])
        );
        assert_eq!(get.body, b"payload".to_vec());
    }

    #[test]
    fn s3_core_rejects_duplicate_buckets_and_non_empty_bucket_delete() {
        let service = service(1_710_000_001);
        let scope = scope();
        create_bucket(&service, &scope, "demo");
        let duplicate = service
            .create_bucket(
                &scope,
                CreateBucketInput {
                    name: "demo".to_owned(),
                    object_lock_enabled: false,
                    region: scope.region().clone(),
                },
            )
            .expect_err("duplicate bucket should fail");
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"payload".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "notes.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("object put should succeed");
        let delete = service
            .delete_bucket(&scope, "demo")
            .expect_err("non-empty bucket should fail");

        assert!(matches!(duplicate, S3Error::BucketAlreadyOwnedByYou { .. }));
        assert!(matches!(delete, S3Error::BucketNotEmpty { .. }));
    }

    #[test]
    fn s3_core_rejects_bucket_access_from_the_wrong_region() {
        let service = service(1_710_000_001);
        let bucket_scope = scope();
        let wrong_region_scope = scope_in_region("us-east-1");
        create_bucket(&service, &bucket_scope, "demo");

        let error = service
            .get_bucket_location(&wrong_region_scope, "demo")
            .expect_err("wrong-region bucket access should fail");

        assert!(matches!(
            error,
            S3Error::WrongRegion { bucket, region }
                if bucket == "demo" && region == "eu-west-2"
        ));
    }

    #[test]
    fn s3_core_listing_supports_prefix_delimiter_and_continuation_tokens() {
        let service = service(1_710_000_002);
        let scope = scope();
        create_bucket(&service, &scope, "demo");
        for key in [
            "logs/2026/a.txt",
            "logs/2026/b.txt",
            "logs/2026/c.txt",
            "logs/2027/d.txt",
        ] {
            service
                .put_object(
                    &scope,
                    PutObjectInput {
                        body: key.as_bytes().to_vec(),
                        bucket: "demo".to_owned(),
                        content_type: None,
                        key: key.to_owned(),
                        metadata: BTreeMap::new(),
                        object_lock: None,
                        tags: BTreeMap::new(),
                    },
                )
                .expect("object put should succeed");
        }

        let page = service
            .list_objects(
                &scope,
                ListObjectsInput {
                    bucket: "demo".to_owned(),
                    delimiter: Some("/".to_owned()),
                    marker: None,
                    max_keys: Some(10),
                    prefix: Some("logs/".to_owned()),
                },
            )
            .expect("list objects should succeed");
        let v2 = service
            .list_objects_v2(
                &scope,
                ListObjectsV2Input {
                    bucket: "demo".to_owned(),
                    continuation_token: None,
                    delimiter: None,
                    max_keys: Some(2),
                    prefix: Some("logs/2026/".to_owned()),
                    start_after: None,
                },
            )
            .expect("list objects v2 should succeed");
        let continued = service
            .list_objects_v2(
                &scope,
                ListObjectsV2Input {
                    bucket: "demo".to_owned(),
                    continuation_token: v2.next_continuation_token.clone(),
                    delimiter: None,
                    max_keys: Some(2),
                    prefix: Some("logs/2026/".to_owned()),
                    start_after: None,
                },
            )
            .expect("continuation token should succeed");

        assert_eq!(
            page.common_prefixes,
            vec!["logs/2026/".to_owned(), "logs/2027/".to_owned()]
        );
        assert!(page.contents.is_empty());
        assert_eq!(
            v2.contents
                .iter()
                .map(|item| item.key.as_str())
                .collect::<Vec<_>>(),
            vec!["logs/2026/a.txt", "logs/2026/b.txt"]
        );
        assert!(v2.next_continuation_token.is_some());
        assert_eq!(
            continued
                .contents
                .iter()
                .map(|item| item.key.as_str())
                .collect::<Vec<_>>(),
            vec!["logs/2026/c.txt"]
        );
    }

    #[test]
    fn s3_core_copy_and_delete_object_cleanup_round_trips() {
        let service = service(1_710_000_003);
        let scope = scope();
        create_bucket(&service, &scope, "demo");
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"payload".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "src.txt".to_owned(),
                    metadata: BTreeMap::from([(
                        "env".to_owned(),
                        "dev".to_owned(),
                    )]),
                    object_lock: None,
                    tags: BTreeMap::from([(
                        "team".to_owned(),
                        "platform".to_owned(),
                    )]),
                },
            )
            .expect("source object should be written");

        let copied = service
            .copy_object(
                &scope,
                CopyObjectInput {
                    destination_bucket: "demo".to_owned(),
                    destination_key: "dst.txt".to_owned(),
                    source_bucket: "demo".to_owned(),
                    source_key: "src.txt".to_owned(),
                    source_version_id: None,
                },
            )
            .expect("copy should succeed");
        let head = service
            .head_object(&scope, "demo", "dst.txt", None)
            .expect("copied object should exist");
        let tags = service
            .get_object_tagging(&scope, "demo", "dst.txt", None)
            .expect("copied tags should exist");

        service
            .delete_object(
                &scope,
                DeleteObjectInput {
                    bypass_governance: false,
                    bypass_governance_authorized: false,
                    bucket: "demo".to_owned(),
                    key: "src.txt".to_owned(),
                    version_id: None,
                },
            )
            .expect("delete should succeed");
        let missing = service
            .get_object(&scope, "demo", "src.txt", None)
            .expect_err("deleted object should be missing");

        assert_eq!(copied.etag, head.etag);
        assert_eq!(head.metadata.get("env"), Some(&"dev".to_owned()));
        assert_eq!(tags.tags.get("team"), Some(&"platform".to_owned()));
        assert!(matches!(missing, S3Error::NoSuchKey { .. }));
    }

    #[test]
    fn s3_advanced_copy_object_can_target_a_noncurrent_version() {
        let service = service(1_710_000_000);
        let scope = scope();

        create_bucket(&service, &scope, "demo");
        service
            .put_bucket_versioning(
                &scope,
                "demo",
                BucketVersioningStatus::Enabled,
            )
            .expect("versioning should enable");

        let first = service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"first".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: Some("text/plain".to_owned()),
                    key: "src.txt".to_owned(),
                    metadata: BTreeMap::from([(
                        "env".to_owned(),
                        "dev".to_owned(),
                    )]),
                    object_lock: None,
                    tags: BTreeMap::from([(
                        "team".to_owned(),
                        "platform".to_owned(),
                    )]),
                },
            )
            .expect("first version should write");
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"second".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: Some("text/plain".to_owned()),
                    key: "src.txt".to_owned(),
                    metadata: BTreeMap::from([(
                        "env".to_owned(),
                        "prod".to_owned(),
                    )]),
                    object_lock: None,
                    tags: BTreeMap::from([(
                        "team".to_owned(),
                        "data".to_owned(),
                    )]),
                },
            )
            .expect("second version should write");

        service
            .copy_object(
                &scope,
                CopyObjectInput {
                    destination_bucket: "demo".to_owned(),
                    destination_key: "dst.txt".to_owned(),
                    source_bucket: "demo".to_owned(),
                    source_key: "src.txt".to_owned(),
                    source_version_id: first.version_id,
                },
            )
            .expect("historical version copy should succeed");

        let copied = service
            .get_object(&scope, "demo", "dst.txt", None)
            .expect("copied object should exist");
        let copied_tags = service
            .get_object_tagging(&scope, "demo", "dst.txt", None)
            .expect("copied tags should exist");

        assert_eq!(copied.body, b"first");
        assert_eq!(copied.head.metadata.get("env"), Some(&"dev".to_owned()));
        assert_eq!(copied_tags.tags.get("team"), Some(&"platform".to_owned()));
    }

    #[test]
    fn s3_core_head_output_matches_expected_shape() {
        let head = HeadObjectOutput {
            content_type: "text/plain".to_owned(),
            delete_marker: false,
            etag: "\"abc\"".to_owned(),
            key: "demo.txt".to_owned(),
            last_modified_epoch_seconds: 1,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            size: 7,
            version_id: Some("1".to_owned()),
        };

        assert_eq!(head.content_type, "text/plain");
        assert_eq!(head.etag, "\"abc\"");
        assert_eq!(head.size, 7);
        assert_eq!(head.version_id.as_deref(), Some("1"));
    }

    #[test]
    fn s3_advanced_versioning_delete_markers_and_historical_reads() {
        let service = service(1_710_000_010);
        let scope = scope();
        create_bucket(&service, &scope, "demo");
        service
            .put_bucket_versioning(
                &scope,
                "demo",
                BucketVersioningStatus::Enabled,
            )
            .expect("versioning should enable");

        let first = service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"first".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "doc.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("first version should write");
        let second = service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"second".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "doc.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("second version should write");
        let delete = service
            .delete_object(
                &scope,
                DeleteObjectInput {
                    bypass_governance: false,
                    bypass_governance_authorized: false,
                    bucket: "demo".to_owned(),
                    key: "doc.txt".to_owned(),
                    version_id: None,
                },
            )
            .expect("delete should create a marker");
        let current = service
            .get_object(&scope, "demo", "doc.txt", None)
            .expect_err("latest delete marker should hide the object");
        let historical = service
            .get_object(&scope, "demo", "doc.txt", first.version_id.as_deref())
            .expect("first version should remain readable");
        let newer = service
            .get_object(
                &scope,
                "demo",
                "doc.txt",
                second.version_id.as_deref(),
            )
            .expect("second version should remain readable");
        let versions = service
            .list_object_versions(
                &scope,
                ListObjectVersionsInput {
                    bucket: "demo".to_owned(),
                    max_keys: Some(10),
                    prefix: Some("doc".to_owned()),
                },
            )
            .expect("version list should succeed");

        assert!(matches!(current, S3Error::NoSuchKey { .. }));
        assert_eq!(historical.body, b"first".to_vec());
        assert_eq!(newer.body, b"second".to_vec());
        assert!(delete.delete_marker);
        assert_eq!(versions.entries.len(), 3);
        assert!(matches!(
            versions.entries.first(),
            Some(super::ListedVersionEntry::DeleteMarker(_))
        ));
    }

    #[test]
    fn s3_advanced_multipart_completion_and_abort_cleanup() {
        let service = service(1_710_000_011);
        let scope = scope();
        create_bucket(&service, &scope, "demo");

        let upload = service
            .create_multipart_upload(
                &scope,
                CreateMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    content_type: Some("text/plain".to_owned()),
                    key: "large.txt".to_owned(),
                    metadata: BTreeMap::from([(
                        "trace".to_owned(),
                        "abc123".to_owned(),
                    )]),
                    tags: BTreeMap::new(),
                },
            )
            .expect("multipart upload should start");
        let part = service
            .upload_part(
                &scope,
                super::UploadPartInput {
                    body: b"payload".to_vec(),
                    bucket: "demo".to_owned(),
                    key: "large.txt".to_owned(),
                    part_number: 1,
                    upload_id: upload.upload_id.clone(),
                },
            )
            .expect("part should upload");
        let complete = service
            .complete_multipart_upload(
                &scope,
                CompleteMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    key: "large.txt".to_owned(),
                    parts: vec![CompletedMultipartPart {
                        etag: Some(part.etag),
                        part_number: 1,
                    }],
                    upload_id: upload.upload_id,
                },
            )
            .expect("multipart upload should complete");
        let head = service
            .head_object(&scope, "demo", "large.txt", None)
            .expect("completed object should exist");
        let uploads = service
            .list_multipart_uploads(&scope, "demo")
            .expect("multipart listing should succeed");

        assert_eq!(head.etag, complete.etag);
        assert!(uploads.uploads.is_empty());

        let aborted = service
            .create_multipart_upload(
                &scope,
                CreateMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "aborted.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("abortable upload should start");
        service
            .upload_part(
                &scope,
                super::UploadPartInput {
                    body: b"chunk".to_vec(),
                    bucket: "demo".to_owned(),
                    key: "aborted.txt".to_owned(),
                    part_number: 1,
                    upload_id: aborted.upload_id.clone(),
                },
            )
            .expect("abortable part should upload");
        service
            .abort_multipart_upload(
                &scope,
                "demo",
                "aborted.txt",
                &aborted.upload_id,
            )
            .expect("abort should succeed");
        let after_abort = service
            .list_multipart_uploads(&scope, "demo")
            .expect("multipart listing after abort should succeed");
        let complete_after_abort = service
            .complete_multipart_upload(
                &scope,
                CompleteMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    key: "aborted.txt".to_owned(),
                    parts: vec![CompletedMultipartPart {
                        etag: None,
                        part_number: 1,
                    }],
                    upload_id: aborted.upload_id,
                },
            )
            .expect_err("aborted upload should be gone");

        assert!(after_abort.uploads.is_empty());
        assert!(matches!(complete_after_abort, S3Error::NoSuchUpload { .. }));
    }

    #[test]
    fn s3_advanced_multipart_rejects_invalid_completion_shapes() {
        let service = service(1_710_000_012);
        let scope = scope();
        create_bucket(&service, &scope, "demo");

        let upload = service
            .create_multipart_upload(
                &scope,
                CreateMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "invalid.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    tags: BTreeMap::new(),
                },
            )
            .expect("upload should start");
        let first = service
            .upload_part(
                &scope,
                super::UploadPartInput {
                    body: b"first".to_vec(),
                    bucket: "demo".to_owned(),
                    key: "invalid.txt".to_owned(),
                    part_number: 1,
                    upload_id: upload.upload_id.clone(),
                },
            )
            .expect("first part should upload");
        let _second = service
            .upload_part(
                &scope,
                super::UploadPartInput {
                    body: b"second".to_vec(),
                    bucket: "demo".to_owned(),
                    key: "invalid.txt".to_owned(),
                    part_number: 2,
                    upload_id: upload.upload_id.clone(),
                },
            )
            .expect("second part should upload");

        let upload_id = upload.upload_id;
        let wrong_part = service
            .complete_multipart_upload(
                &scope,
                CompleteMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    key: "invalid.txt".to_owned(),
                    parts: vec![CompletedMultipartPart {
                        etag: Some(first.etag),
                        part_number: 3,
                    }],
                    upload_id: upload_id.clone(),
                },
            )
            .expect_err("missing part should fail");
        let unordered = service
            .complete_multipart_upload(
                &scope,
                CompleteMultipartUploadInput {
                    bucket: "demo".to_owned(),
                    key: "invalid.txt".to_owned(),
                    parts: vec![
                        CompletedMultipartPart { etag: None, part_number: 2 },
                        CompletedMultipartPart { etag: None, part_number: 1 },
                    ],
                    upload_id,
                },
            )
            .expect_err("unordered parts should fail");

        assert!(matches!(
            wrong_part,
            S3Error::InvalidArgument { code: "InvalidPart", .. }
        ));
        assert!(matches!(
            unordered,
            S3Error::InvalidArgument { code: "InvalidPartOrder", .. }
        ));
    }

    #[test]
    fn s3_advanced_tagging_and_bucket_subresource_storage_round_trip() {
        let service = service(1_710_000_013);
        let scope = scope();
        create_bucket(&service, &scope, "demo");

        service
            .put_bucket_tagging(
                &scope,
                "demo",
                BTreeMap::from([("env".to_owned(), "dev".to_owned())]),
            )
            .expect("bucket tags should store");
        let bucket_tags = service
            .get_bucket_tagging(&scope, "demo")
            .expect("bucket tags should round-trip");

        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"payload".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "object.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::from([(
                        "seed".to_owned(),
                        "true".to_owned(),
                    )]),
                },
            )
            .expect("object should store");
        let object_tags = service
            .put_object_tagging(
                &scope,
                TaggingInput {
                    bucket: "demo".to_owned(),
                    key: Some("object.txt".to_owned()),
                    tags: BTreeMap::from([(
                        "team".to_owned(),
                        "storage".to_owned(),
                    )]),
                    version_id: None,
                },
            )
            .expect("object tags should store");
        let stored_tags = service
            .get_object_tagging(&scope, "demo", "object.txt", None)
            .expect("object tags should round-trip");

        service
            .put_bucket_policy(
                &scope,
                "demo",
                "{\"Version\":\"2012-10-17\"}".to_owned(),
            )
            .expect("bucket policy should store");
        service
            .put_bucket_cors(
                &scope,
                "demo",
                "<CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>".to_owned(),
            )
            .expect("bucket cors should store");
        service
            .put_bucket_lifecycle(
                &scope,
                "demo",
                "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>expire</ID><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>".to_owned(),
            )
            .expect("bucket lifecycle should store");
        service
            .put_bucket_encryption(
                &scope,
                "demo",
                "<ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_owned(),
            )
            .expect("bucket encryption should store");
        service
            .put_bucket_acl(
                &scope,
                "demo",
                StoredBucketAclInput::Canned(super::CannedAcl::PublicRead),
            )
            .expect("bucket ACL should store");

        assert_eq!(bucket_tags.tags.get("env"), Some(&"dev".to_owned()));
        assert_eq!(object_tags.tags.get("team"), Some(&"storage".to_owned()));
        assert_eq!(stored_tags.tags.get("team"), Some(&"storage".to_owned()));
        assert!(
            service
                .get_bucket_policy(&scope, "demo")
                .expect("stored policy should read")
                .contains("\"Version\"")
        );
        assert!(
            service
                .get_bucket_cors(&scope, "demo")
                .expect("stored cors should read")
                .contains("CORSConfiguration")
        );
        assert!(
            service
                .get_bucket_lifecycle(&scope, "demo")
                .expect("stored lifecycle should read")
                .contains("LifecycleConfiguration")
        );
        assert!(
            service
                .get_bucket_encryption(&scope, "demo")
                .expect("stored encryption should read")
                .contains("ServerSideEncryptionConfiguration")
        );
        assert!(
            service
                .get_bucket_acl(&scope, "demo")
                .expect("stored ACL should read")
                .contains("AllUsers")
        );
    }

    #[test]
    fn s3_notifications_emit_matching_events_via_explicit_transport() {
        let service = service(1_710_000_020);
        let scope = scope();
        let transport = Arc::new(RecordingNotificationTransport::default());
        service.set_notification_transport(transport.clone());
        create_bucket(&service, &scope, "demo");
        service
            .put_bucket_notification_configuration(
                &scope,
                "demo",
                BucketNotificationConfiguration {
                    queue_configurations: vec![
                        QueueNotificationConfiguration {
                            events: vec!["s3:ObjectCreated:*".to_owned()],
                            filter: Some(NotificationFilter {
                                prefix: Some("reports/".to_owned()),
                                suffix: Some(".csv".to_owned()),
                            }),
                            id: Some("queue-config".to_owned()),
                            queue_arn:
                                "arn:aws:sqs:eu-west-2:000000000000:demo"
                                    .parse()
                                    .expect("queue ARN should parse"),
                        },
                    ],
                    topic_configurations: vec![
                        TopicNotificationConfiguration {
                            events: vec!["s3:ObjectCreated:Put".to_owned()],
                            filter: None,
                            id: Some("topic-config".to_owned()),
                            topic_arn:
                                "arn:aws:sns:eu-west-2:000000000000:demo"
                                    .parse()
                                    .expect("topic ARN should parse"),
                        },
                    ],
                },
            )
            .expect("notification config should store");

        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"name,age\njane,30\n".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: Some("text/csv".to_owned()),
                    key: "reports/data.csv".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("matching object write should succeed");
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"ignored".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "notes.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("non-matching object write should succeed");

        let published = transport.published();
        assert_eq!(published.len(), 3);
        assert_eq!(published[0].event_name, "ObjectCreated:Put");
        assert_eq!(
            published[0].configuration_id.as_deref(),
            Some("queue-config")
        );
        assert_eq!(
            published[0].destination_arn,
            "arn:aws:sqs:eu-west-2:000000000000:demo"
                .parse()
                .expect("queue ARN should parse")
        );
        assert_eq!(published[0].key, "reports/data.csv");
        assert_eq!(
            published[1].configuration_id.as_deref(),
            Some("topic-config")
        );
        assert_eq!(
            published[1].destination_arn,
            "arn:aws:sns:eu-west-2:000000000000:demo"
                .parse()
                .expect("topic ARN should parse")
        );
        assert_eq!(published[2].event_name, "ObjectCreated:Put");
        assert_eq!(
            published[2].configuration_id.as_deref(),
            Some("topic-config")
        );
        assert_eq!(
            published[2].destination_arn,
            "arn:aws:sns:eu-west-2:000000000000:demo"
                .parse()
                .expect("topic ARN should parse")
        );
        assert_eq!(published[2].key, "notes.txt");
    }

    #[test]
    fn s3_notifications_object_lock_rejects_unauthorized_governance_bypass() {
        let service = service(1_710_000_021);
        let scope = scope();
        service
            .create_bucket(
                &scope,
                CreateBucketInput {
                    name: "locked".to_owned(),
                    object_lock_enabled: true,
                    region: scope.region().clone(),
                },
            )
            .expect("object lock bucket should be created");
        service
            .put_object_lock_configuration(
                &scope,
                "locked",
                BucketObjectLockConfiguration {
                    object_lock_enabled: true,
                    default_retention: Some(DefaultObjectLockRetention {
                        mode: ObjectLockMode::Governance,
                        period: DefaultRetentionPeriod::Days(1),
                    }),
                },
            )
            .expect("object lock configuration should store");

        let put = service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"payload".to_vec(),
                    bucket: "locked".to_owned(),
                    content_type: None,
                    key: "doc.txt".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("locked object should store");

        let retention_error = service
            .put_object_retention(
                &scope,
                PutObjectRetentionInput {
                    bucket: "locked".to_owned(),
                    bypass_governance: true,
                    bypass_governance_authorized: false,
                    key: "doc.txt".to_owned(),
                    retention: ObjectRetention {
                        mode: Some(ObjectLockMode::Governance),
                        retain_until_epoch_seconds: Some(1_710_000_100),
                    },
                    version_id: put.version_id.clone(),
                },
            )
            .expect_err("unauthorized governance bypass should fail");
        service
            .put_object_legal_hold(
                &scope,
                PutObjectLegalHoldInput {
                    bucket: "locked".to_owned(),
                    key: "doc.txt".to_owned(),
                    status: LegalHoldStatus::On,
                    version_id: put.version_id.clone(),
                },
            )
            .expect("legal hold should store");
        let legal_hold = service
            .get_object_legal_hold(
                &scope,
                "locked",
                "doc.txt",
                put.version_id.as_deref(),
            )
            .expect("legal hold should read");
        let delete_error = service
            .delete_object(
                &scope,
                DeleteObjectInput {
                    bucket: "locked".to_owned(),
                    bypass_governance: false,
                    bypass_governance_authorized: false,
                    key: "doc.txt".to_owned(),
                    version_id: put.version_id,
                },
            )
            .expect_err("legal hold should block deletion");

        assert!(matches!(retention_error, S3Error::AccessDenied { .. }));
        assert_eq!(legal_hold.status, LegalHoldStatus::On);
        assert!(matches!(delete_error, S3Error::AccessDenied { .. }));
    }

    #[test]
    fn s3_notifications_select_filters_csv_rows() {
        let service = service(1_710_000_022);
        let scope = scope();
        create_bucket(&service, &scope, "demo");
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"Name,Age\nJane,30\nBob,20\n".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: Some("text/csv".to_owned()),
                    key: "records.csv".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("CSV object should store");

        let output = service
            .select_object_content(
                &scope,
                SelectObjectContentInput {
                    bucket: "demo".to_owned(),
                    csv_input: CsvInputSerialization {
                        file_header_info: CsvFileHeaderInfo::Use,
                        ..CsvInputSerialization::default()
                    },
                    csv_output: CsvOutputSerialization::default(),
                    expression:
                        "SELECT s.\"Name\" FROM s3object s WHERE s.\"Age\" > 21"
                            .to_owned(),
                    key: "records.csv".to_owned(),
                    version_id: None,
                },
            )
            .expect("CSV select should succeed");

        assert_eq!(output.records, "Jane\n");
        assert!(output.stats.bytes_processed > 0);
        assert!(output.stats.bytes_returned > 0);
    }

    #[test]
    fn s3_notifications_restore_reports_explicit_not_implemented() {
        let service = service(1_710_000_023);
        let scope = scope();
        create_bucket(&service, &scope, "demo");
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: b"payload".to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: "archive.bin".to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("archive object should store");

        let error = service
            .restore_object(&scope, "demo", "archive.bin", None)
            .expect_err("restore should fail explicitly");

        assert!(matches!(
            error,
            S3Error::InvalidArgument {
                code: "NotImplemented",
                status_code: 501,
                ..
            }
        ));
    }
}
