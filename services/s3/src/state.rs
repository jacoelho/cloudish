use aws::{BlobStore, Clock};

use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

use crate::{
    bucket::BucketRecord,
    errors::{S3Error, S3InitError},
    multipart::MultipartUploadRecord,
    notifications::{S3NotificationTransport, default_notification_transport},
    object::{ObjectRecord, ObjectStorageKey},
};
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
            notification_transport: Arc::new(Mutex::new(
                default_notification_transport(),
            )),
            object_store,
            sequence_store,
        })
    }

    pub(crate) fn blob_store(&self) -> &Arc<dyn BlobStore> {
        &self.blob_store
    }

    pub(crate) fn bucket_store(&self) -> &StorageHandle<String, BucketRecord> {
        &self.bucket_store
    }

    pub(crate) fn multipart_store(
        &self,
    ) -> &StorageHandle<String, MultipartUploadRecord> {
        &self.multipart_store
    }

    pub(crate) fn object_store(
        &self,
    ) -> &StorageHandle<ObjectStorageKey, ObjectRecord> {
        &self.object_store
    }

    pub(crate) fn notification_transport_cell(
        &self,
    ) -> &Mutex<Arc<dyn S3NotificationTransport + Send + Sync>> {
        &self.notification_transport
    }

    pub(crate) fn persist_bucket_record(
        &self,
        bucket: &str,
        record: BucketRecord,
    ) -> Result<(), S3Error> {
        self.bucket_store
            .put(bucket.to_owned(), record)
            .map_err(S3Error::Buckets)
    }

    pub(crate) fn delete_bucket_record(
        &self,
        bucket: &str,
    ) -> Result<(), S3Error> {
        self.bucket_store.delete(&bucket.to_owned()).map_err(S3Error::Buckets)
    }

    pub(crate) fn persist_object_record(
        &self,
        record: &ObjectRecord,
    ) -> Result<(), S3Error> {
        self.object_store
            .put(record.storage_key(), record.clone())
            .map_err(S3Error::Objects)
    }

    pub(crate) fn remove_object_record(
        &self,
        record: &ObjectRecord,
    ) -> Result<(), S3Error> {
        self.remove_object_payload(record)?;
        self.object_store
            .delete(&record.storage_key())
            .map_err(S3Error::Objects)
    }

    pub(crate) fn remove_object_payload(
        &self,
        record: &ObjectRecord,
    ) -> Result<(), S3Error> {
        if record.is_delete_marker() {
            return Ok(());
        }

        self.blob_store.delete(&record.blob_key()).map_err(S3Error::Blob)
    }

    pub(crate) fn persist_multipart_record(
        &self,
        upload_id: &str,
        upload: MultipartUploadRecord,
    ) -> Result<(), S3Error> {
        self.multipart_store
            .put(upload_id.to_owned(), upload)
            .map_err(S3Error::MultipartUploads)
    }

    pub(crate) fn delete_multipart_upload_record(
        &self,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        self.multipart_store
            .delete(&upload_id.to_owned())
            .map_err(S3Error::MultipartUploads)
    }

    pub(crate) fn next_sequence(&self, name: &str) -> Result<u64, S3Error> {
        let key = name.to_owned();
        let next = self
            .sequence_store
            .get(&key)
            .unwrap_or(0)
            .saturating_add(1);
        self.sequence_store.put(key, next).map_err(S3Error::Sequences)?;
        Ok(next)
    }

    pub(crate) fn lock_state(&self) -> std::sync::MutexGuard<'_, ()> {
        self.mutation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn now_epoch_seconds(&self) -> Result<u64, S3Error> {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .map_err(|_| S3Error::Internal {
                message:
                    "S3 clock produced a timestamp before the Unix epoch."
                        .to_owned(),
            })
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
