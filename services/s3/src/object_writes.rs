use crate::{
    bucket::BucketVersioningStatus,
    errors::S3Error,
    object::{
        CopyObjectInput, CopyObjectOutput, DeleteObjectInput,
        DeleteObjectOutput, NULL_VERSION_ID, ObjectRecord, PutObjectInput,
        PutObjectOutput, normalize_metadata, normalized_content_type,
    },
    retention::{ensure_object_version_deletable, resolve_object_lock_write},
    scope::S3Scope,
    state::S3Service,
    tagging::normalize_tags,
};

pub(crate) struct StoredPutObject {
    pub(crate) record: ObjectRecord,
}

impl S3Service {
    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_object(
        &self,
        scope: &S3Scope,
        input: PutObjectInput,
    ) -> Result<PutObjectOutput, S3Error> {
        let _guard = self.lock_state();
        let stored = self.put_object_inner(scope, input, None)?;
        self.emit_object_created_notification(
            scope,
            "ObjectCreated:Put",
            stored.record.bucket(),
            stored.record.key(),
            stored.record.version_id().as_deref(),
        );
        Ok(stored.record.put_output())
    }

    pub(crate) fn put_object_inner(
        &self,
        scope: &S3Scope,
        input: PutObjectInput,
        etag_override: Option<String>,
    ) -> Result<StoredPutObject, S3Error> {
        let PutObjectInput {
            body,
            bucket,
            content_type,
            key,
            metadata,
            object_lock,
            tags,
        } = input;
        let bucket_record = self.ensure_bucket_owned(scope, &bucket)?;
        let last_modified_epoch_seconds = self.now_epoch_seconds()?;
        let object_lock = resolve_object_lock_write(
            &bucket_record,
            object_lock,
            last_modified_epoch_seconds,
        )?;
        let metadata = normalize_metadata(metadata);
        let tags = normalize_tags(tags)?;
        let etag = etag_override
            .unwrap_or_else(|| format!("\"{:x}\"", md5::compute(&body)));
        let content_type = normalized_content_type(content_type);
        let body_len = body.len() as u64;
        let record = match bucket_record.versioning_status {
            Some(BucketVersioningStatus::Enabled) => {
                if let Some(mut current) =
                    self.current_object_record(&bucket, &key)
                {
                    current.mark_not_latest();
                    self.persist_object_record(&current)?;
                }
                let generated = format!(
                    "{:020}",
                    self.next_sequence(&format!(
                        "object-version:{bucket}:{key}"
                    ))?
                );
                ObjectRecord::new_stored_object(
                    bucket,
                    key,
                    content_type,
                    etag,
                    last_modified_epoch_seconds,
                    metadata,
                    tags,
                    Some(generated),
                    object_lock,
                    body_len,
                )
            }
            Some(BucketVersioningStatus::Suspended) => {
                if let Some(mut current) =
                    self.current_object_record(&bucket, &key)
                    && current.version_id().as_deref() != Some(NULL_VERSION_ID)
                {
                    current.mark_not_latest();
                    self.persist_object_record(&current)?;
                }
                let previous = self.resolve_nullable_version(
                    &bucket,
                    &key,
                    NULL_VERSION_ID,
                );

                let record = ObjectRecord::new_stored_object(
                    bucket,
                    key,
                    content_type,
                    etag,
                    last_modified_epoch_seconds,
                    metadata,
                    tags,
                    Some(NULL_VERSION_ID.to_owned()),
                    object_lock,
                    body_len,
                );
                if let Some(previous) = previous {
                    self.remove_object_payload(&previous)?;
                }
                record
            }
            None => {
                if let Some(previous) =
                    self.current_object_record(&bucket, &key)
                {
                    self.remove_object_record(&previous)?;
                }
                ObjectRecord::new_stored_object(
                    bucket,
                    key,
                    content_type,
                    etag,
                    last_modified_epoch_seconds,
                    metadata,
                    tags,
                    None,
                    object_lock,
                    body_len,
                )
            }
        };

        self.blob_store()
            .put(&record.blob_key(), &body)
            .map_err(S3Error::Blob)?;
        self.persist_object_record(&record)?;

        Ok(StoredPutObject { record })
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
        if source.is_delete_marker() {
            return Err(S3Error::NoSuchKey { key: input.source_key });
        }
        let body = self.object_body(&source)?;

        let copied = self.put_object_inner(
            scope,
            source.into_copy_input(
                input.destination_bucket,
                input.destination_key,
                body,
            ),
            None,
        )?;
        self.emit_object_created_notification(
            scope,
            "ObjectCreated:Copy",
            copied.record.bucket(),
            copied.record.key(),
            copied.record.version_id().as_deref(),
        );

        Ok(copied.record.copy_output())
    }

    pub(crate) fn create_delete_marker(
        &self,
        bucket: &str,
        key: &str,
        current: Option<ObjectRecord>,
    ) -> Result<DeleteObjectOutput, S3Error> {
        if let Some(mut current) = current {
            current.mark_not_latest();
            self.persist_object_record(&current)?;
        }

        let version_id = format!(
            "{:020}",
            self.next_sequence(&format!("object-version:{bucket}:{key}"))?
        );
        let record = ObjectRecord::new_delete_marker(
            bucket.to_owned(),
            key.to_owned(),
            self.now_epoch_seconds()?,
            version_id.clone(),
        );
        self.persist_object_record(&record)?;

        Ok(DeleteObjectOutput {
            delete_marker: true,
            version_id: Some(version_id),
        })
    }

    pub(crate) fn delete_null_version_if_present(
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

    pub(crate) fn delete_object_version(
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
        ensure_object_version_deletable(
            &object,
            bypass_governance && bypass_governance_authorized,
            self.now_epoch_seconds()?,
        )?;
        let was_latest = object.is_latest();
        let delete_marker = object.is_delete_marker();
        let returned_version_id = object.version_id_or_null();
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
                left.last_modified_epoch_seconds()
                    .cmp(&right.last_modified_epoch_seconds())
                    .then_with(|| {
                        left.version_sort_key().cmp(&right.version_sort_key())
                    })
            })
        else {
            return Ok(());
        };
        latest.mark_latest();
        self.persist_object_record(&latest)
    }
}
