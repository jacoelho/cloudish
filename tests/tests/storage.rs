#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Duration;
use storage::{
    HybridStorage, InMemoryStorage, ManualScheduler, PersistentStorage,
    ServiceStorageConfig, StorageBackend, StorageConfig, StorageFactory,
    StorageHandle, StorageMode, WalStorage,
};
use test_support::temporary_directory;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct StoredRecord {
    payload: String,
}

fn assert_crud_contract(backend: StorageHandle<String, StoredRecord>) {
    backend.load().expect("load should succeed");
    backend
        .put("alpha".to_owned(), StoredRecord { payload: "one".to_owned() })
        .expect("first put should succeed");
    backend
        .put("beta".to_owned(), StoredRecord { payload: "two".to_owned() })
        .expect("second put should succeed");
    backend
        .put("alpha".to_owned(), StoredRecord { payload: "three".to_owned() })
        .expect("overwrite should succeed");

    assert_eq!(
        backend.get(&"alpha".to_owned()),
        Some(StoredRecord { payload: "three".to_owned() })
    );
    assert_eq!(
        backend.scan(&|key| key.starts_with('a')),
        vec![StoredRecord { payload: "three".to_owned() }]
    );
    assert_eq!(backend.keys(), vec!["alpha".to_owned(), "beta".to_owned()]);

    backend.delete(&"beta".to_owned()).expect("delete should succeed");
    assert_eq!(backend.get(&"beta".to_owned()), None);

    backend.clear().expect("clear should succeed");
    assert!(backend.keys().is_empty());
    backend.shutdown().expect("shutdown should succeed");
}

#[test]
fn crud_contract_holds_across_backends() {
    let directory = temporary_directory("tests-storage-crud");
    let scheduler = Arc::new(ManualScheduler::new());
    let backends = vec![
        Arc::new(InMemoryStorage::<String, StoredRecord>::new())
            as StorageHandle<String, StoredRecord>,
        Arc::new(PersistentStorage::<String, StoredRecord>::new(
            directory.join("persistent.json"),
        )),
        Arc::new(HybridStorage::<String, StoredRecord>::with_scheduler(
            directory.join("hybrid.json"),
            Duration::from_secs(1),
            scheduler.clone(),
        )),
        Arc::new(WalStorage::<String, StoredRecord>::with_scheduler(
            directory.join("wal-snapshot.json"),
            directory.join("wal.wal"),
            Duration::from_secs(1),
            scheduler,
        )),
    ];

    for backend in backends {
        assert_crud_contract(backend);
    }
}

#[test]
fn factory_applies_service_overrides() {
    let directory = temporary_directory("tests-storage-factory");
    let factory = StorageFactory::with_scheduler(
        StorageConfig::new(&directory, StorageMode::Memory)
            .with_service_override(
                "s3",
                ServiceStorageConfig::default().with_mode(StorageMode::Wal),
            )
            .with_service_override(
                "sqs",
                ServiceStorageConfig::default()
                    .with_mode(StorageMode::Persistent),
            ),
        Arc::new(ManualScheduler::new()),
    );

    assert_eq!(
        factory.create::<String, StoredRecord>("sns", "topics").mode(),
        StorageMode::Memory
    );
    assert_eq!(
        factory.create::<String, StoredRecord>("sqs", "queues").mode(),
        StorageMode::Persistent
    );
    assert_eq!(
        factory.create::<String, StoredRecord>("s3", "objects").mode(),
        StorageMode::Wal
    );
}

#[test]
fn wal_truncation_preserves_prior_state() {
    let directory = temporary_directory("tests-storage-wal-truncation");
    let snapshot = directory.join("snapshot.json");
    let wal = directory.join("state.wal");
    let backend = WalStorage::<String, StoredRecord>::new(
        &snapshot,
        &wal,
        Duration::from_secs(30),
    );
    backend.load().expect("load should succeed");
    backend
        .put("good".to_owned(), StoredRecord { payload: "value".to_owned() })
        .expect("put should succeed");
    backend
        .put("partial".to_owned(), StoredRecord { payload: "tail".to_owned() })
        .expect("put should succeed");
    let wal_len = std::fs::metadata(&wal).expect("wal metadata").len();
    let file = OpenOptions::new()
        .write(true)
        .open(&wal)
        .expect("wal should be writable");
    file.set_len(wal_len - 2).expect("wal should be truncatable");

    let reloaded = WalStorage::<String, StoredRecord>::new(
        &snapshot,
        &wal,
        Duration::from_secs(30),
    );
    reloaded.load().expect("load should stop safely");

    assert_eq!(
        reloaded.get(&"good".to_owned()),
        Some(StoredRecord { payload: "value".to_owned() })
    );
}
