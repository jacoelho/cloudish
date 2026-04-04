use aws::RegionId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    bucket_subresources::{
        StoredBucketAcl, StoredBucketAclInput as StoredBucketAclInputValue,
        canned_acl_xml,
    },
    errors::S3Error,
    notifications::BucketNotificationConfiguration,
    retention::{
        BucketObjectLockConfiguration, DefaultObjectLockRetention,
        StoredObjectLockConfiguration,
    },
    scope::S3Scope,
    state::S3Service,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadBucketOutput {
    Forbidden { region: String },
    Found { region: String },
    Missing,
    WrongRegion { region: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadBucketInput {
    pub expected_bucket_owner: Option<String>,
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

pub use crate::bucket_subresources::{CannedAcl, StoredBucketAclInput};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BucketRecord {
    acl_configuration: Option<StoredBucketAcl>,
    cors_configuration: Option<String>,
    created_at_epoch_seconds: u64,
    encryption_configuration: Option<String>,
    lifecycle_configuration: Option<String>,
    pub(crate) name: String,
    #[serde(default)]
    notification_configuration: BucketNotificationConfiguration,
    #[serde(default)]
    object_lock_configuration: Option<StoredObjectLockConfiguration>,
    #[serde(default)]
    pub(crate) object_lock_enabled: bool,
    pub(crate) owner_account_id: String,
    policy: Option<String>,
    pub(crate) region: String,
    tags: BTreeMap<String, String>,
    pub(crate) versioning_status: Option<BucketVersioningStatus>,
}

impl BucketRecord {
    pub(crate) fn new(
        created_at_epoch_seconds: u64,
        name: String,
        object_lock_enabled: bool,
        owner_account_id: String,
        region: String,
    ) -> Self {
        Self {
            acl_configuration: None,
            cors_configuration: None,
            created_at_epoch_seconds,
            encryption_configuration: None,
            lifecycle_configuration: None,
            name,
            notification_configuration:
                BucketNotificationConfiguration::default(),
            object_lock_configuration: None,
            object_lock_enabled,
            owner_account_id,
            policy: None,
            region,
            tags: BTreeMap::new(),
            versioning_status: object_lock_enabled
                .then_some(BucketVersioningStatus::Enabled),
        }
    }

    pub(crate) fn into_bucket(self) -> ListedBucket {
        ListedBucket {
            created_at_epoch_seconds: self.created_at_epoch_seconds,
            name: self.name,
            owner_account_id: self.owner_account_id,
            region: self.region,
        }
    }

    pub(crate) fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    pub(crate) fn into_tags(self) -> BTreeMap<String, String> {
        self.tags
    }

    pub(crate) fn set_tags(&mut self, tags: BTreeMap<String, String>) {
        self.tags = tags;
    }

    pub(crate) fn clear_tags(&mut self) {
        self.tags.clear();
    }

    pub(crate) fn into_policy(self) -> Option<String> {
        self.policy
    }

    pub(crate) fn set_policy(&mut self, policy: Option<String>) {
        self.policy = policy;
    }

    pub(crate) fn into_cors_configuration(self) -> Option<String> {
        self.cors_configuration
    }

    pub(crate) fn set_cors_configuration(
        &mut self,
        configuration: Option<String>,
    ) {
        self.cors_configuration = configuration;
    }

    pub(crate) fn into_lifecycle_configuration(self) -> Option<String> {
        self.lifecycle_configuration
    }

    pub(crate) fn set_lifecycle_configuration(
        &mut self,
        configuration: Option<String>,
    ) {
        self.lifecycle_configuration = configuration;
    }

    pub(crate) fn into_encryption_configuration(self) -> Option<String> {
        self.encryption_configuration
    }

    pub(crate) fn set_encryption_configuration(
        &mut self,
        configuration: Option<String>,
    ) {
        self.encryption_configuration = configuration;
    }

    pub(crate) fn set_acl_configuration(
        &mut self,
        acl: StoredBucketAclInputValue,
    ) {
        self.acl_configuration = Some(match acl {
            StoredBucketAclInputValue::Canned(value) => {
                StoredBucketAcl::Canned(value)
            }
            StoredBucketAclInputValue::Xml(value) => {
                StoredBucketAcl::Xml(value)
            }
        });
    }

    pub(crate) fn acl_xml(self) -> String {
        match self.acl_configuration {
            Some(StoredBucketAcl::Xml(value)) => value,
            Some(StoredBucketAcl::Canned(value)) => {
                canned_acl_xml(&self.owner_account_id, value)
            }
            None => canned_acl_xml(&self.owner_account_id, CannedAcl::Private),
        }
    }

    pub(crate) fn notification_configuration(
        &self,
    ) -> &BucketNotificationConfiguration {
        &self.notification_configuration
    }

    pub(crate) fn into_notification_configuration(
        self,
    ) -> BucketNotificationConfiguration {
        self.notification_configuration
    }

    pub(crate) fn set_notification_configuration(
        &mut self,
        configuration: BucketNotificationConfiguration,
    ) {
        self.notification_configuration = configuration;
    }

    pub(crate) fn created_at_epoch_seconds(&self) -> u64 {
        self.created_at_epoch_seconds
    }

    pub(crate) fn apply_object_lock_configuration(
        &mut self,
        configuration: BucketObjectLockConfiguration,
    ) {
        self.object_lock_enabled = configuration.object_lock_enabled;
        self.object_lock_configuration = configuration
            .default_retention
            .map(StoredObjectLockConfiguration::from);
    }

    pub(crate) fn object_lock_configuration_output(
        self,
    ) -> Option<BucketObjectLockConfiguration> {
        self.object_lock_enabled.then(|| BucketObjectLockConfiguration {
            object_lock_enabled: true,
            default_retention: self
                .object_lock_configuration
                .map(DefaultObjectLockRetention::from),
        })
    }

    pub(crate) fn default_object_lock_retention(
        &self,
    ) -> Option<DefaultObjectLockRetention> {
        self.object_lock_configuration
            .clone()
            .map(DefaultObjectLockRetention::from)
    }
}

impl S3Service {
    pub(crate) fn bucket_record(&self, bucket: &str) -> Option<BucketRecord> {
        self.bucket_store().get(&bucket.to_owned())
    }

    fn bucket_records(&self) -> Vec<BucketRecord> {
        self.bucket_store().scan(&|_| true)
    }

    pub(crate) fn ensure_bucket_owned(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketRecord, S3Error> {
        let bucket = self.bucket_record(bucket).ok_or_else(|| {
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

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn create_bucket(
        &self,
        scope: &S3Scope,
        input: CreateBucketInput,
    ) -> Result<ListedBucket, S3Error> {
        let CreateBucketInput { name, object_lock_enabled, region } = input;
        validate_bucket_name(&name)?;
        let _guard = self.lock_state();

        if let Some(existing) = self.bucket_record(&name) {
            return if existing.owner_account_id == scope.account_id().as_str()
            {
                Err(S3Error::BucketAlreadyOwnedByYou { bucket: name })
            } else {
                Err(S3Error::BucketAlreadyExists { bucket: name })
            };
        }

        let created_at_epoch_seconds = self.now_epoch_seconds()?;
        let owner_account_id = scope.account_id().to_string();
        let region = region.as_str().to_owned();
        let record = BucketRecord::new(
            created_at_epoch_seconds,
            name.clone(),
            object_lock_enabled,
            owner_account_id.clone(),
            region.clone(),
        );
        self.persist_bucket_record(&name, record)?;

        Ok(ListedBucket {
            created_at_epoch_seconds,
            name,
            owner_account_id,
            region,
        })
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
        if self.bucket_is_empty(bucket) {
            self.delete_bucket_record(bucket)?;
            return Ok(());
        }

        Err(S3Error::BucketNotEmpty { bucket: bucket.to_owned() })
    }

    pub fn list_buckets(&self, scope: &S3Scope) -> ListBucketsOutput {
        let buckets = self
            .bucket_records()
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
        self.persist_bucket_record(bucket, bucket_record)
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

    pub fn head_bucket(
        &self,
        scope: &S3Scope,
        bucket: &str,
        input: &HeadBucketInput,
    ) -> HeadBucketOutput {
        let Some(bucket) = self.bucket_record(bucket) else {
            return HeadBucketOutput::Missing;
        };
        if input
            .expected_bucket_owner
            .as_deref()
            .is_some_and(|owner| owner != bucket.owner_account_id)
        {
            return HeadBucketOutput::Forbidden { region: bucket.region };
        }
        if bucket.owner_account_id != scope.account_id().as_str() {
            return HeadBucketOutput::Forbidden { region: bucket.region };
        }
        if bucket.region != scope.region().as_str() {
            return HeadBucketOutput::WrongRegion { region: bucket.region };
        }

        HeadBucketOutput::Found { region: bucket.region }
    }
}

pub(crate) fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
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

impl S3Service {
    fn bucket_is_empty(&self, bucket: &str) -> bool {
        self.bucket_object_records(bucket).is_empty()
            && self.bucket_multipart_uploads(bucket).is_empty()
    }
}
