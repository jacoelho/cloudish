use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub use crate::object_read_model::{
    GetObjectOutput, HeadObjectOutput, ObjectReadMetadata,
};
use crate::{
    errors::S3Error,
    object_ranges::{ObjectRange, ResolvedObjectRange, resolve_object_range},
    retention::{
        LegalHoldStatus, ObjectLockMode, ObjectLockWriteOptions,
        ObjectRetention,
    },
    state::S3Service,
};

pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
pub(crate) const NULL_VERSION_ID: &str = "null";
pub(crate) const STANDARD_STORAGE_CLASS: &str = "STANDARD";

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
pub(crate) struct NewStoredObject {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) content_type: String,
    pub(crate) etag: String,
    pub(crate) last_modified_epoch_seconds: u64,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) tags: BTreeMap<String, String>,
    pub(crate) version_id: Option<String>,
    pub(crate) object_lock: ObjectLockWriteOptions,
    pub(crate) size: u64,
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
pub struct ObjectTaggingOutput {
    pub tags: BTreeMap<String, String>,
    pub version_id: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct ObjectStorageKey {
    bucket: String,
    key: String,
    version_id: Option<String>,
}

impl ObjectStorageKey {
    pub(crate) fn new(
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> Self {
        Self { bucket: bucket.to_owned(), key: key.to_owned(), version_id }
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObjectRecord {
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

impl ListedVersionEntry {
    pub(crate) fn sort_key(&self) -> &str {
        match self {
            Self::DeleteMarker(value) => &value.key,
            Self::Version(value) => &value.key,
        }
    }

    pub(crate) fn version_sort_key(&self) -> &str {
        match self {
            Self::DeleteMarker(value) => &value.version_id,
            Self::Version(value) => &value.version_id,
        }
    }
}

impl ObjectRecord {
    pub(crate) fn new_stored_object(input: NewStoredObject) -> Self {
        Self {
            bucket: input.bucket,
            content_type: input.content_type,
            delete_marker: false,
            etag: input.etag,
            is_latest: true,
            key: input.key,
            last_modified_epoch_seconds: input.last_modified_epoch_seconds,
            legal_hold_status: input.object_lock.legal_hold_status,
            metadata: input.metadata,
            object_lock_mode: input.object_lock.mode,
            object_lock_retain_until_epoch_seconds: input
                .object_lock
                .retain_until_epoch_seconds,
            size: input.size,
            tags: input.tags,
            version_id: input.version_id,
        }
    }

    pub(crate) fn new_delete_marker(
        bucket: String,
        key: String,
        last_modified_epoch_seconds: u64,
        version_id: String,
    ) -> Self {
        Self {
            bucket,
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            delete_marker: true,
            etag: String::new(),
            is_latest: true,
            key,
            last_modified_epoch_seconds,
            legal_hold_status: None,
            metadata: BTreeMap::new(),
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            size: 0,
            tags: BTreeMap::new(),
            version_id: Some(version_id),
        }
    }

    pub(crate) fn is_delete_marker(&self) -> bool {
        self.delete_marker
    }

    pub(crate) fn storage_key(&self) -> ObjectStorageKey {
        ObjectStorageKey::new(&self.bucket, &self.key, self.version_id.clone())
    }

    pub(crate) fn blob_key(&self) -> crate::BlobKey {
        match self.version_id.as_deref() {
            Some(version_id) => crate::BlobKey::new(
                format!("{}-versions", self.bucket),
                format!("{version_id}:{}", self.key),
            ),
            None => crate::BlobKey::new(&self.bucket, &self.key),
        }
    }

    pub(crate) fn delete_marker_error(
        &self,
        requested_version_id: Option<&str>,
    ) -> Option<S3Error> {
        if !self.delete_marker {
            return None;
        }

        Some(match requested_version_id {
            Some(version_id) => S3Error::RequestedVersionIsDeleteMarker {
                last_modified_epoch_seconds: self.last_modified_epoch_seconds,
                version_id: version_id.to_owned(),
            },
            None => S3Error::CurrentVersionIsDeleteMarker {
                version_id: self.version_id.clone(),
            },
        })
    }

    pub(crate) fn into_head(
        self,
        range: Option<&ObjectRange>,
    ) -> Result<HeadObjectOutput, S3Error> {
        let size = self.size;
        let range = match range {
            Some(range) => Some(resolve_object_range(range, size)?),
            None => None,
        };
        let content_length = range
            .as_ref()
            .map(ResolvedObjectRange::content_length)
            .unwrap_or(size);

        Ok(self.into_head_with_range(content_length))
    }

    pub(crate) fn into_head_with_range(
        self,
        content_length: u64,
    ) -> HeadObjectOutput {
        HeadObjectOutput { content_length, metadata: self.into_metadata() }
    }

    pub(crate) fn into_metadata(self) -> ObjectReadMetadata {
        ObjectReadMetadata {
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
            version_id: self.version_id,
        }
    }

    pub(crate) fn into_copy_input(
        self,
        bucket: String,
        key: String,
        body: Vec<u8>,
    ) -> PutObjectInput {
        PutObjectInput {
            body,
            bucket,
            content_type: Some(self.content_type),
            key,
            metadata: self.metadata,
            object_lock: None,
            tags: self.tags,
        }
    }

    pub(crate) fn into_tagging_output(self) -> ObjectTaggingOutput {
        ObjectTaggingOutput { tags: self.tags, version_id: self.version_id }
    }

    pub(crate) fn put_output(&self) -> PutObjectOutput {
        PutObjectOutput {
            etag: self.etag.clone(),
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            version_id: self.version_id.clone(),
        }
    }

    pub(crate) fn copy_output(&self) -> CopyObjectOutput {
        CopyObjectOutput {
            etag: self.etag.clone(),
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            version_id: self.version_id.clone(),
        }
    }

    pub(crate) fn into_listed(self) -> ListedObject {
        ListedObject {
            etag: self.etag,
            key: self.key,
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            size: self.size,
            storage_class: STANDARD_STORAGE_CLASS.to_owned(),
        }
    }

    pub(crate) fn into_delete_marker(self) -> ListedDeleteMarker {
        ListedDeleteMarker {
            is_latest: self.is_latest,
            key: self.key,
            last_modified_epoch_seconds: self.last_modified_epoch_seconds,
            version_id: self
                .version_id
                .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
        }
    }

    pub(crate) fn into_listed_version(self) -> ListedObjectVersion {
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

    pub(crate) fn version_sort_key(&self) -> String {
        self.version_id.clone().unwrap_or_else(|| NULL_VERSION_ID.to_owned())
    }

    pub(crate) fn is_current_visible(&self) -> bool {
        self.is_latest && !self.delete_marker
    }

    pub(crate) fn is_latest(&self) -> bool {
        self.is_latest
    }

    pub(crate) fn mark_not_latest(&mut self) {
        self.is_latest = false;
    }

    pub(crate) fn mark_latest(&mut self) {
        self.is_latest = true;
    }

    pub(crate) fn has_version_id(&self) -> bool {
        self.version_id.is_some()
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn set_tags(&mut self, tags: BTreeMap<String, String>) {
        self.tags = tags;
    }

    pub(crate) fn clear_tags(&mut self) {
        self.tags.clear();
    }

    pub(crate) fn version_id(&self) -> &Option<String> {
        &self.version_id
    }

    pub(crate) fn etag(&self) -> &str {
        &self.etag
    }

    pub(crate) fn last_modified_epoch_seconds(&self) -> u64 {
        self.last_modified_epoch_seconds
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn retention(&self) -> Option<ObjectRetention> {
        self.object_lock_mode
            .zip(self.object_lock_retain_until_epoch_seconds)
            .map(|(mode, retain_until_epoch_seconds)| ObjectRetention {
                mode: Some(mode),
                retain_until_epoch_seconds: Some(retain_until_epoch_seconds),
            })
    }

    pub(crate) fn object_lock_mode(&self) -> Option<ObjectLockMode> {
        self.object_lock_mode
    }

    pub(crate) fn object_lock_retain_until_epoch_seconds(
        &self,
    ) -> Option<u64> {
        self.object_lock_retain_until_epoch_seconds
    }

    pub(crate) fn set_retention(
        &mut self,
        mode: Option<ObjectLockMode>,
        retain_until_epoch_seconds: Option<u64>,
    ) {
        self.object_lock_mode = mode;
        self.object_lock_retain_until_epoch_seconds =
            retain_until_epoch_seconds;
    }

    pub(crate) fn legal_hold_status(&self) -> Option<LegalHoldStatus> {
        self.legal_hold_status
    }

    pub(crate) fn set_legal_hold_status(&mut self, status: LegalHoldStatus) {
        self.legal_hold_status = Some(status);
    }

    pub(crate) fn version_id_or_null(&self) -> String {
        self.version_id.clone().unwrap_or_else(|| NULL_VERSION_ID.to_owned())
    }
}

pub(crate) fn normalize_metadata(
    metadata: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    metadata
        .into_iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value))
        .collect()
}

pub(crate) fn normalized_content_type(content_type: Option<String>) -> String {
    content_type
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned())
}

impl S3Service {
    pub(crate) fn all_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Vec<ObjectRecord> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        self.object_store().scan(&move |candidate| {
            candidate.bucket() == bucket && candidate.key() == key
        })
    }

    pub(crate) fn resolve_object_record(
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

    pub(crate) fn resolve_nullable_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Option<ObjectRecord> {
        let version_id = (version_id != NULL_VERSION_ID)
            .then(|| version_id.to_owned())
            .or_else(|| Some(NULL_VERSION_ID.to_owned()));
        self.object_store()
            .get(&ObjectStorageKey::new(bucket, key, version_id))
    }

    pub(crate) fn current_object_record(
        &self,
        bucket: &str,
        key: &str,
    ) -> Option<ObjectRecord> {
        self.all_versions(bucket, key)
            .into_iter()
            .find(ObjectRecord::is_latest)
    }

    pub(crate) fn bucket_object_records(
        &self,
        bucket: &str,
    ) -> Vec<ObjectRecord> {
        let bucket = bucket.to_owned();
        self.object_store()
            .scan(&move |candidate| candidate.bucket() == bucket)
    }

    pub(crate) fn object_body(
        &self,
        object: &ObjectRecord,
    ) -> Result<Vec<u8>, S3Error> {
        self.blob_store()
            .get(&object.blob_key())
            .map_err(S3Error::Blob)?
            .ok_or_else(|| S3Error::Internal {
                message: format!(
                    "object payload for `{}/{}` is missing from the blob store",
                    object.bucket(),
                    object.key()
                ),
            })
    }
}
