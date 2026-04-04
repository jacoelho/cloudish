use crate::{
    BlobKey, BlobStore, Clock, CreateBucketInput, InfrastructureError,
    ObjectRange, S3EventNotification, S3NotificationTransport, S3Scope,
    S3Service, object_read_model::ObjectReadRequest,
};
use aws::{AccountId, BlobMetadata, RegionId};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::{StorageConfig, StorageFactory, StorageMode};

#[derive(Debug, Default)]
pub(crate) struct TestBlobStore {
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

#[derive(Debug, Default)]
pub(crate) struct CountingBlobStore {
    blobs: Mutex<BTreeMap<BlobKey, Vec<u8>>>,
    get_calls: AtomicUsize,
}

impl CountingBlobStore {
    pub(crate) fn get_calls(&self) -> usize {
        self.get_calls.load(Ordering::SeqCst)
    }
}

impl BlobStore for CountingBlobStore {
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
        self.get_calls.fetch_add(1, Ordering::SeqCst);
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
pub(crate) struct RecordingNotificationTransport {
    published: Mutex<Vec<S3EventNotification>>,
}

impl RecordingNotificationTransport {
    pub(crate) fn published(&self) -> Vec<S3EventNotification> {
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

pub(crate) fn scope() -> S3Scope {
    scope_in_region("eu-west-2")
}

pub(crate) fn scope_in_region(region: &str) -> S3Scope {
    scope_in_account_region("000000000000", region)
}

pub(crate) fn scope_in_account_region(
    account_id: &str,
    region: &str,
) -> S3Scope {
    S3Scope::new(
        account_id.parse::<AccountId>().expect("account should parse"),
        region.parse::<RegionId>().expect("region should parse"),
    )
}

pub(crate) fn service(now: u64) -> S3Service {
    service_with_blob_store(now, Arc::new(TestBlobStore::default()))
}

pub(crate) fn service_with_blob_store(
    now: u64,
    blob_store: Arc<dyn BlobStore>,
) -> S3Service {
    let factory = StorageFactory::new(StorageConfig::new(
        std::env::temp_dir().join("cloudish-s3-service-tests"),
        StorageMode::Memory,
    ));

    S3Service::new(
        &factory,
        blob_store,
        Arc::new(TestClock { now: UNIX_EPOCH + Duration::from_secs(now) }),
    )
    .expect("S3 service should build")
}

pub(crate) fn create_bucket(service: &S3Service, scope: &S3Scope, name: &str) {
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

pub(crate) fn object_read(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    range: Option<&ObjectRange>,
) -> ObjectReadRequest {
    ObjectReadRequest {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        version_id: version_id.map(str::to_owned),
        range: range.cloned(),
        ..ObjectReadRequest::default()
    }
}
