use aws::BlobKey;
use std::collections::BTreeMap;

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

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct MultipartUploadRecord {
    pub(crate) bucket: String,
    pub(crate) content_type: String,
    pub(crate) initiated_epoch_seconds: u64,
    pub(crate) key: String,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) parts: BTreeMap<u16, MultipartPartRecord>,
    pub(crate) tags: BTreeMap<String, String>,
    pub(crate) upload_id: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
pub(crate) struct MultipartPartRecord {
    pub(crate) etag: String,
    pub(crate) size: u64,
}

use crate::{
    errors::S3Error,
    object::{PutObjectInput, normalize_metadata, normalized_content_type},
    scope::S3Scope,
    state::S3Service,
    tagging::normalize_tags,
};

impl S3Service {
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
        self.persist_multipart_record(&upload_id, record)?;

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
        if !(1..=10_000).contains(&input.part_number) {
            return Err(S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: "Part number must be between 1 and 10000.".to_owned(),
                status_code: 400,
            });
        }

        let etag = format!("\"{:x}\"", md5::compute(&input.body));
        self.put_multipart_part_body(
            &input.upload_id,
            input.part_number,
            &input.body,
        )?;
        upload.parts.insert(
            input.part_number,
            MultipartPartRecord {
                etag: etag.clone(),
                size: input.body.len() as u64,
            },
        );
        self.persist_multipart_record(&input.upload_id, upload)?;

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
            return Err(malformed_completion_error());
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
        let completed = self.completed_multipart_body(&input, &upload)?;
        let composite_etag = format!(
            "\"{:x}-{}\"",
            md5::compute(&completed.etag_input),
            input.parts.len()
        );
        let stored = self.put_object_inner(
            scope,
            PutObjectInput {
                body: completed.body,
                bucket: upload.bucket.clone(),
                content_type: Some(upload.content_type.clone()),
                key: upload.key.clone(),
                metadata: upload.metadata.clone(),
                object_lock: None,
                tags: upload.tags.clone(),
            },
            Some(composite_etag),
        )?;
        self.cleanup_multipart_upload(&upload.upload_id, &upload)?;
        self.emit_object_created_notification(
            scope,
            "ObjectCreated:CompleteMultipartUpload",
            &upload.bucket,
            &upload.key,
            stored.record.version_id().as_deref(),
        );

        Ok(CompleteMultipartUploadOutput {
            etag: stored.record.etag().to_owned(),
            last_modified_epoch_seconds: stored
                .record
                .last_modified_epoch_seconds(),
            version_id: stored.record.version_id().clone(),
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
        let mut uploads = self
            .bucket_multipart_uploads(bucket)
            .into_iter()
            .map(|upload| MultipartUploadSummary {
                initiated_epoch_seconds: upload.initiated_epoch_seconds,
                key: upload.key,
                upload_id: upload.upload_id,
            })
            .collect::<Vec<_>>();
        uploads.sort_by(|left, right| {
            left.key.cmp(&right.key).then(left.upload_id.cmp(&right.upload_id))
        });

        Ok(ListMultipartUploadsOutput { bucket: bucket.to_owned(), uploads })
    }

    pub(crate) fn multipart_record(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<MultipartUploadRecord, S3Error> {
        let upload =
            self.multipart_upload_record(upload_id).ok_or_else(|| {
                S3Error::NoSuchUpload { upload_id: upload_id.to_owned() }
            })?;
        if upload.bucket != bucket || upload.key != key {
            return Err(S3Error::NoSuchUpload {
                upload_id: upload_id.to_owned(),
            });
        }

        Ok(upload)
    }

    pub(crate) fn multipart_upload_record(
        &self,
        upload_id: &str,
    ) -> Option<MultipartUploadRecord> {
        self.multipart_store().get(&upload_id.to_owned())
    }

    pub(crate) fn bucket_multipart_uploads(
        &self,
        bucket: &str,
    ) -> Vec<MultipartUploadRecord> {
        let bucket = bucket.to_owned();
        self.multipart_store()
            .scan(&|_| true)
            .into_iter()
            .filter(|upload| upload.bucket == bucket)
            .collect()
    }

    pub(crate) fn put_multipart_part_body(
        &self,
        upload_id: &str,
        part_number: u16,
        body: &[u8],
    ) -> Result<(), S3Error> {
        self.blob_store()
            .put(&multipart_part_blob_key(upload_id, part_number), body)
            .map(|_| ())
            .map_err(S3Error::Blob)
    }

    pub(crate) fn multipart_part_body(
        &self,
        upload_id: &str,
        part_number: u16,
    ) -> Result<Option<Vec<u8>>, S3Error> {
        self.blob_store()
            .get(&multipart_part_blob_key(upload_id, part_number))
            .map_err(S3Error::Blob)
    }

    pub(crate) fn delete_multipart_part_body(
        &self,
        upload_id: &str,
        part_number: u16,
    ) -> Result<(), S3Error> {
        self.blob_store()
            .delete(&multipart_part_blob_key(upload_id, part_number))
            .map_err(S3Error::Blob)
    }

    fn completed_multipart_body(
        &self,
        input: &CompleteMultipartUploadInput,
        upload: &MultipartUploadRecord,
    ) -> Result<CompletedMultipartBody, S3Error> {
        let mut body = Vec::new();
        let mut etag_input = Vec::new();

        for requested_part in &input.parts {
            let stored_part = upload
                .parts
                .get(&requested_part.part_number)
                .ok_or_else(invalid_part_error)?;
            if requested_part
                .etag
                .as_deref()
                .is_some_and(|etag| etag != stored_part.etag)
            {
                return Err(invalid_part_error());
            }

            let part_body = self
                .multipart_part_body(
                    &input.upload_id,
                    requested_part.part_number,
                )?
                .ok_or_else(invalid_part_error)?;
            body.extend_from_slice(&part_body);
            etag_input.extend_from_slice(stored_part.etag.as_bytes());
        }

        Ok(CompletedMultipartBody { body, etag_input })
    }

    fn cleanup_multipart_upload(
        &self,
        upload_id: &str,
        upload: &MultipartUploadRecord,
    ) -> Result<(), S3Error> {
        for part_number in upload.parts.keys() {
            self.delete_multipart_part_body(upload_id, *part_number)?;
        }
        self.delete_multipart_upload_record(upload_id)
    }
}

struct CompletedMultipartBody {
    body: Vec<u8>,
    etag_input: Vec<u8>,
}

fn multipart_part_blob_key(upload_id: &str, part_number: u16) -> BlobKey {
    BlobKey::new(format!("multipart-{upload_id}"), format!("{part_number:05}"))
}

fn malformed_completion_error() -> S3Error {
    S3Error::InvalidArgument {
        code: "MalformedXML",
        message: "The XML you provided was not well-formed or did not validate against our published schema."
            .to_owned(),
        status_code: 400,
    }
}

fn invalid_part_error() -> S3Error {
    S3Error::InvalidArgument {
        code: "InvalidPart",
        message: "One or more of the specified parts could not be found."
            .to_owned(),
        status_code: 400,
    }
}
