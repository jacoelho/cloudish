use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketTaggingOutput {
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggingInput {
    pub bucket: String,
    pub key: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub version_id: Option<String>,
}

use crate::{
    errors::S3Error, object::ObjectTaggingOutput, scope::S3Scope,
    state::S3Service,
};

impl S3Service {
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
        bucket_record.set_tags(normalize_tags(tags)?);
        self.persist_bucket_record(bucket, bucket_record)
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
        if bucket_record.tags().is_empty() {
            return Err(S3Error::NoSuchTagSet { bucket: bucket.to_owned() });
        }

        Ok(BucketTaggingOutput { tags: bucket_record.into_tags() })
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
        bucket_record.clear_tags();
        self.persist_bucket_record(bucket, bucket_record)
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
        let key = required_tagging_key(input.key)?;
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let mut object = self.resolve_object_record(
            &input.bucket,
            &key,
            input.version_id.as_deref(),
        )?;
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchKey { key });
        }
        object.set_tags(normalize_tags(input.tags)?);
        self.persist_object_record(&object)?;
        self.emit_object_created_notification(
            scope,
            "ObjectTagging:Put",
            &input.bucket,
            &key,
            object.version_id().as_deref(),
        );

        Ok(object.into_tagging_output())
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
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchKey { key: key.to_owned() });
        }

        Ok(object.into_tagging_output())
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
        if object.is_delete_marker() {
            return Err(S3Error::NoSuchKey { key: key.to_owned() });
        }
        object.clear_tags();
        self.persist_object_record(&object)?;
        self.emit_object_created_notification(
            scope,
            "ObjectTagging:Delete",
            bucket,
            key,
            object.version_id().as_deref(),
        );

        Ok(object.into_tagging_output())
    }
}

fn required_tagging_key(key: Option<String>) -> Result<String, S3Error> {
    key.ok_or_else(|| S3Error::InvalidArgument {
        code: "InvalidRequest",
        message: "Object tagging requires an object key.".to_owned(),
        status_code: 400,
    })
}

pub(crate) fn normalize_tags(
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
