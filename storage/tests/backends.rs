#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use aws::{AccountId, RegionId};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use storage::{
    HybridStorage, InMemoryStorage, ManualScheduler, PersistentStorage,
    ServiceStorageConfig, StorageConfig, StorageFactory, StorageHandle,
    StorageMode, StorageScope, WalStorage,
};
use test_support::temporary_directory;

macro_rules! assert_ok {
    ($expr:expr, $($arg:tt)*) => {{
        match $expr {
            Ok(value) => value,
            Err(error) => {
                assert!(false, "{}: {error}", format!($($arg)*));
                return;
            }
        }
    }};
}

fn concurrent_backend_cases()
-> Vec<(&'static str, StorageHandle<String, String>)> {
    let directory = temporary_directory("storage-concurrency");
    let scheduler = Arc::new(ManualScheduler::new());

    vec![
        (
            "memory",
            Arc::new(InMemoryStorage::<String, String>::new())
                as StorageHandle<String, String>,
        ),
        (
            "persistent",
            Arc::new(PersistentStorage::<String, String>::new(
                directory.join("persistent.json"),
            )),
        ),
        (
            "hybrid",
            Arc::new(HybridStorage::<String, String>::with_scheduler(
                directory.join("hybrid.json"),
                Duration::from_secs(1),
                scheduler.clone(),
            )),
        ),
        (
            "wal",
            Arc::new(WalStorage::<String, String>::with_scheduler(
                directory.join("wal-snapshot.json"),
                directory.join("wal.wal"),
                Duration::from_secs(1),
                scheduler,
            )),
        ),
    ]
}

#[test]
fn concurrent_access_keeps_all_backends_consistent() {
    for (name, backend) in concurrent_backend_cases() {
        assert_ok!(backend.load(), "{name} backend should load");
        let mut workers = Vec::new();

        for worker in 0..4 {
            let backend = Arc::clone(&backend);
            workers.push(thread::spawn(move || {
                for item in 0..10 {
                    let result = backend.put(
                        format!("{worker}-{item}"),
                        format!("value-{worker}-{item}"),
                    );
                    assert!(
                        result.is_ok(),
                        "put should succeed for {worker}-{item}: {result:?}"
                    );
                }
            }));
        }

        for worker in workers {
            assert!(worker.join().is_ok(), "worker thread should finish");
        }

        assert_eq!(
            backend.keys().len(),
            40,
            "{name} should retain all concurrent writes"
        );
        assert_ok!(backend.shutdown(), "{name} backend should shut down");
    }
}

#[test]
fn factory_lifecycle_restores_persistent_and_wal_backends() {
    let directory = temporary_directory("storage-factory-restart");
    let scheduler = Arc::new(ManualScheduler::new());
    let config = StorageConfig::new(&directory, StorageMode::Persistent)
        .with_service_override(
            "s3",
            ServiceStorageConfig::default().with_mode(StorageMode::Wal),
        );

    let factory =
        StorageFactory::with_scheduler(config.clone(), scheduler.clone());
    let persistent = factory.create::<String, String>("sqs", "queues");
    let wal = factory.create::<String, String>("s3", "objects");
    assert_ok!(persistent.load(), "persistent backend should load");
    assert_ok!(wal.load(), "wal backend should load");
    assert_ok!(
        persistent.put("queue".to_owned(), "state".to_owned()),
        "persistent put should succeed"
    );
    assert_ok!(
        wal.put("bucket".to_owned(), "state".to_owned()),
        "wal put should succeed"
    );
    assert_ok!(factory.shutdown_all(), "factory shutdown should succeed");

    let reloaded_factory = StorageFactory::with_scheduler(config, scheduler);
    let reloaded_persistent =
        reloaded_factory.create::<String, String>("sqs", "queues");
    let reloaded_wal =
        reloaded_factory.create::<String, String>("s3", "objects");
    assert_ok!(reloaded_factory.load_all(), "factory load_all should succeed");

    assert_eq!(
        reloaded_persistent.get(&"queue".to_owned()),
        Some("state".to_owned())
    );
    assert_eq!(
        reloaded_wal.get(&"bucket".to_owned()),
        Some("state".to_owned())
    );
}

fn scope(account_id: &str, region: &str) -> Result<StorageScope, String> {
    let account_id = account_id
        .parse::<AccountId>()
        .map_err(|error| format!("account id should parse: {error}"))?;
    let region = region
        .parse::<RegionId>()
        .map_err(|error| format!("region should parse: {error}"))?;

    Ok(StorageScope::new(account_id, region))
}

#[test]
fn scoped_repositories_persist_same_names_without_scope_collisions() {
    let directory = temporary_directory("storage-scoped-restart");
    let scheduler = Arc::new(ManualScheduler::new());
    let config = StorageConfig::new(&directory, StorageMode::Persistent)
        .with_service_override(
            "lambda",
            ServiceStorageConfig::default().with_mode(StorageMode::Wal),
        );

    let factory =
        StorageFactory::with_scheduler(config.clone(), scheduler.clone());
    let persistent =
        factory.create_scoped::<String, String>("ssm", "parameters");
    let wal = factory.create_scoped::<String, String>("lambda", "functions");
    let eu_scope = assert_ok!(
        scope("000000000000", "eu-west-2"),
        "EU scope should build"
    );
    let us_scope = assert_ok!(
        scope("111111111111", "us-east-1"),
        "US scope should build"
    );

    assert_ok!(persistent.load(), "persistent scoped storage should load");
    assert_ok!(wal.load(), "wal scoped storage should load");
    assert_ok!(
        persistent.put(
            &eu_scope,
            "shared".to_owned(),
            "eu-parameter".to_owned()
        ),
        "EU persistent put should succeed"
    );
    assert_ok!(
        persistent.put(
            &us_scope,
            "shared".to_owned(),
            "us-parameter".to_owned()
        ),
        "US persistent put should succeed"
    );
    assert_ok!(
        wal.put(&eu_scope, "shared".to_owned(), "eu-function".to_owned()),
        "EU wal put should succeed"
    );
    assert_ok!(
        wal.put(&us_scope, "shared".to_owned(), "us-function".to_owned()),
        "US wal put should succeed"
    );
    assert_ok!(factory.shutdown_all(), "factory shutdown should succeed");

    let reloaded_factory = StorageFactory::with_scheduler(config, scheduler);
    let reloaded_persistent =
        reloaded_factory.create_scoped::<String, String>("ssm", "parameters");
    let reloaded_wal = reloaded_factory
        .create_scoped::<String, String>("lambda", "functions");
    assert_ok!(reloaded_factory.load_all(), "factory load_all should succeed");

    assert_eq!(
        reloaded_persistent.get(&eu_scope, &"shared".to_owned()),
        Some("eu-parameter".to_owned())
    );
    assert_eq!(
        reloaded_persistent.get(&us_scope, &"shared".to_owned()),
        Some("us-parameter".to_owned())
    );
    assert_eq!(
        reloaded_wal.get(&eu_scope, &"shared".to_owned()),
        Some("eu-function".to_owned())
    );
    assert_eq!(
        reloaded_wal.get(&us_scope, &"shared".to_owned()),
        Some("us-function".to_owned())
    );
}
