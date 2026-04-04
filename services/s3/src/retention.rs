use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredObjectLockConfiguration {
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

use crate::{
    bucket::BucketRecord, errors::S3Error, scope::S3Scope, state::S3Service,
};

impl S3Service {
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
            != Some(crate::bucket::BucketVersioningStatus::Enabled)
        {
            return Err(S3Error::InvalidBucketState {
                message: "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration"
                    .to_owned(),
            });
        }

        bucket_record.apply_object_lock_configuration(configuration);
        self.persist_bucket_record(bucket, bucket_record)
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
        bucket_record.object_lock_configuration_output().ok_or_else(|| {
            S3Error::BucketObjectLockConfigurationNotFound {
                bucket: bucket.to_owned(),
            }
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
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchVersion {
                version_id: input
                    .version_id
                    .unwrap_or_else(|| object.version_id_or_null()),
            });
        }
        apply_object_retention(
            &mut object,
            input.retention,
            self.now_epoch_seconds()?,
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
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchVersion {
                version_id: version_id
                    .map(str::to_owned)
                    .unwrap_or_else(|| object.version_id_or_null()),
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
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchVersion {
                version_id: input
                    .version_id
                    .unwrap_or_else(|| object.version_id_or_null()),
            });
        }
        object.set_legal_hold_status(input.status);
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
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchVersion {
                version_id: version_id
                    .map(str::to_owned)
                    .unwrap_or_else(|| object.version_id_or_null()),
            });
        }
        object
            .legal_hold_status()
            .map(|status| ObjectLegalHoldOutput { status })
            .ok_or_else(|| S3Error::NoSuchObjectLockConfiguration {
                key: key.to_owned(),
            })
    }
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

fn apply_object_retention(
    object: &mut crate::object::ObjectRecord,
    retention: ObjectRetention,
    now_epoch_seconds: u64,
    bypass_governance: bool,
) -> Result<(), S3Error> {
    if retention.mode.is_some()
        ^ retention.retain_until_epoch_seconds.is_some()
    {
        return Err(malformed_xml_s3_error());
    }
    let current_mode = object.object_lock_mode();
    let current_retain_until = object.object_lock_retain_until_epoch_seconds();
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

    object.set_retention(retention.mode, retention.retain_until_epoch_seconds);
    Ok(())
}

pub(crate) fn malformed_xml_s3_error() -> S3Error {
    S3Error::InvalidArgument {
        code: "MalformedXML",
        message: "The XML you provided was not well-formed or did not validate against our published schema."
            .to_owned(),
        status_code: 400,
    }
}

pub(crate) fn ensure_object_version_deletable(
    object: &crate::object::ObjectRecord,
    bypass_governance: bool,
    now_epoch_seconds: u64,
) -> Result<(), S3Error> {
    if object.is_delete_marker() {
        return Ok(());
    }
    if object.legal_hold_status() == Some(LegalHoldStatus::On) {
        return Err(S3Error::AccessDenied {
            message: "Access Denied because object protected by object lock."
                .to_owned(),
        });
    }
    let Some(retain_until) = object.object_lock_retain_until_epoch_seconds()
    else {
        return Ok(());
    };
    if retain_until <= now_epoch_seconds {
        return Ok(());
    }

    match object.object_lock_mode() {
        Some(ObjectLockMode::Compliance) => Err(S3Error::AccessDenied {
            message: "Access Denied because object protected by object lock."
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

pub(crate) fn resolve_object_lock_write(
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
        && let Some(default_retention) = bucket.default_object_lock_retention()
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
