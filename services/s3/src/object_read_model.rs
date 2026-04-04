use crate::{
    object_ranges::{IfRangeCondition, ObjectRange},
    object_read_conditions::ObjectReadConditions,
    retention::{LegalHoldStatus, ObjectLockMode},
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectReadResponseOverrides {
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectReadRequest {
    pub bucket: String,
    pub conditions: ObjectReadConditions,
    pub expected_bucket_owner: Option<String>,
    pub if_range: Option<IfRangeCondition>,
    pub key: String,
    pub range: Option<ObjectRange>,
    pub response_overrides: ObjectReadResponseOverrides,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectReadMetadata {
    pub content_type: String,
    pub delete_marker: bool,
    pub etag: String,
    pub key: String,
    pub last_modified_epoch_seconds: u64,
    pub metadata: BTreeMap<String, String>,
    pub object_lock_legal_hold_status: Option<LegalHoldStatus>,
    pub object_lock_mode: Option<ObjectLockMode>,
    pub object_lock_retain_until_epoch_seconds: Option<u64>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadObjectOutput {
    pub content_length: u64,
    pub metadata: ObjectReadMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetObjectOutput {
    pub body: Vec<u8>,
    pub content_length: u64,
    pub content_range: Option<String>,
    pub is_partial: bool,
    pub metadata: ObjectReadMetadata,
}
