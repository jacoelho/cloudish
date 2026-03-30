use crate::{
    HybridStorage, InMemoryStorage, PersistentStorage, Scheduler,
    ScopedStorage, ScopedStorageKey, StorageHandle, StorageKey, StorageValue,
    ThreadScheduler, WalStorage,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LockResult, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    Memory,
    Persistent,
    Hybrid,
    Wal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    base_path: PathBuf,
    default_flush_interval: Duration,
    default_mode: StorageMode,
    service_overrides: BTreeMap<String, ServiceStorageConfig>,
    wal_compaction_interval: Duration,
}

impl StorageConfig {
    pub fn new(
        base_path: impl Into<PathBuf>,
        default_mode: StorageMode,
    ) -> Self {
        Self {
            base_path: base_path.into(),
            default_flush_interval: Duration::from_secs(5),
            default_mode,
            service_overrides: BTreeMap::new(),
            wal_compaction_interval: Duration::from_secs(30),
        }
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    pub fn with_default_flush_interval(
        mut self,
        flush_interval: Duration,
    ) -> Self {
        self.default_flush_interval = flush_interval;
        self
    }

    pub fn with_service_override(
        mut self,
        service: impl Into<String>,
        override_config: ServiceStorageConfig,
    ) -> Self {
        self.service_overrides.insert(service.into(), override_config);
        self
    }

    pub fn with_wal_compaction_interval(
        mut self,
        compaction_interval: Duration,
    ) -> Self {
        self.wal_compaction_interval = compaction_interval;
        self
    }

    pub fn mode_for(&self, service: &str) -> StorageMode {
        self.service_overrides
            .get(service)
            .and_then(|config| config.mode)
            .unwrap_or(self.default_mode)
    }

    pub fn flush_interval_for(&self, service: &str) -> Duration {
        self.service_overrides
            .get(service)
            .and_then(|config| config.flush_interval)
            .unwrap_or(self.default_flush_interval)
    }

    pub fn wal_compaction_interval(&self) -> Duration {
        self.wal_compaction_interval
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceStorageConfig {
    flush_interval: Option<Duration>,
    mode: Option<StorageMode>,
}

impl ServiceStorageConfig {
    pub fn with_flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = Some(flush_interval);
        self
    }

    pub fn with_mode(mut self, mode: StorageMode) -> Self {
        self.mode = Some(mode);
        self
    }
}

pub struct StorageFactory {
    config: StorageConfig,
    lifecycle_hooks: Mutex<Vec<LifecycleHooks>>,
    scheduler: Arc<dyn Scheduler>,
}

impl StorageFactory {
    pub fn new(config: StorageConfig) -> Self {
        Self::with_scheduler(config, Arc::new(ThreadScheduler))
    }

    pub fn with_scheduler(
        config: StorageConfig,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        Self { config, lifecycle_hooks: Mutex::new(Vec::new()), scheduler }
    }

    pub fn create<K, V>(
        &self,
        service: &str,
        store_name: &str,
    ) -> StorageHandle<K, V>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let backend: StorageHandle<K, V> = match self.config.mode_for(service)
        {
            StorageMode::Memory => Arc::new(InMemoryStorage::<K, V>::new()),
            StorageMode::Persistent => {
                Arc::new(PersistentStorage::<K, V>::new(
                    self.persistent_snapshot_path(service, store_name),
                ))
            }
            StorageMode::Hybrid => {
                Arc::new(HybridStorage::<K, V>::with_scheduler(
                    self.persistent_snapshot_path(service, store_name),
                    self.config.flush_interval_for(service),
                    Arc::clone(&self.scheduler),
                ))
            }
            StorageMode::Wal => {
                let (snapshot_path, wal_path) =
                    self.wal_paths(service, store_name);
                Arc::new(WalStorage::<K, V>::with_scheduler(
                    snapshot_path,
                    wal_path,
                    self.config.wal_compaction_interval(),
                    Arc::clone(&self.scheduler),
                ))
            }
        };

        self.register_backend(Arc::clone(&backend));
        backend
    }

    pub fn create_scoped<K, V>(
        &self,
        service: &str,
        store_name: &str,
    ) -> ScopedStorage<K, V>
    where
        K: StorageKey,
        V: StorageValue,
    {
        ScopedStorage::new(
            self.create::<ScopedStorageKey<K>, V>(service, store_name),
        )
    }

    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Flushes every backend created by this factory.
    ///
    /// # Errors
    ///
    /// Returns the first backend flush error encountered.
    pub fn flush_all(&self) -> Result<(), crate::StorageError> {
        for hooks in recover(self.lifecycle_hooks.lock()).iter() {
            (hooks.flush)()?;
        }
        Ok(())
    }

    /// Loads persisted state for every backend created by this factory.
    ///
    /// # Errors
    ///
    /// Returns the first backend load error encountered.
    pub fn load_all(&self) -> Result<(), crate::StorageError> {
        for hooks in recover(self.lifecycle_hooks.lock()).iter() {
            (hooks.load)()?;
        }
        Ok(())
    }

    /// Shuts down every backend created by this factory.
    ///
    /// # Errors
    ///
    /// Returns the first backend shutdown error encountered.
    pub fn shutdown_all(&self) -> Result<(), crate::StorageError> {
        for hooks in recover(self.lifecycle_hooks.lock()).iter() {
            (hooks.shutdown)()?;
        }
        Ok(())
    }

    pub fn persistent_snapshot_path(
        &self,
        service: &str,
        store_name: &str,
    ) -> PathBuf {
        self.service_root(service).join(format!("{store_name}.json"))
    }

    pub fn wal_paths(
        &self,
        service: &str,
        store_name: &str,
    ) -> (PathBuf, PathBuf) {
        let root = self.service_root(service);
        (
            root.join(format!("{store_name}-snapshot.json")),
            root.join(format!("{store_name}.wal")),
        )
    }

    fn register_backend<K, V>(&self, backend: StorageHandle<K, V>)
    where
        K: StorageKey,
        V: StorageValue,
    {
        recover(self.lifecycle_hooks.lock()).push(LifecycleHooks {
            flush: Box::new({
                let backend = Arc::clone(&backend);
                move || backend.flush()
            }),
            load: Box::new({
                let backend = Arc::clone(&backend);
                move || backend.load()
            }),
            shutdown: Box::new(move || backend.shutdown()),
        });
    }

    fn service_root(&self, service: &str) -> PathBuf {
        self.config.base_path.join(service)
    }
}

struct LifecycleHooks {
    flush: Box<dyn Fn() -> Result<(), crate::StorageError> + Send + Sync>,
    load: Box<dyn Fn() -> Result<(), crate::StorageError> + Send + Sync>,
    shutdown: Box<dyn Fn() -> Result<(), crate::StorageError> + Send + Sync>,
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{
        ServiceStorageConfig, StorageConfig, StorageFactory, StorageMode,
    };
    use crate::ManualScheduler;
    use crate::StorageScope;
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;
    use test_support::temporary_directory;

    #[test]
    fn config_resolves_global_and_service_specific_modes() {
        let config = StorageConfig::new("/tmp/cloudish", StorageMode::Memory)
            .with_default_flush_interval(Duration::from_secs(9))
            .with_service_override(
                "s3",
                ServiceStorageConfig::default()
                    .with_mode(StorageMode::Wal)
                    .with_flush_interval(Duration::from_secs(3)),
            );

        assert_eq!(config.mode_for("sqs"), StorageMode::Memory);
        assert_eq!(config.mode_for("s3"), StorageMode::Wal);
        assert_eq!(config.flush_interval_for("sqs"), Duration::from_secs(9));
        assert_eq!(config.flush_interval_for("s3"), Duration::from_secs(3));
    }

    #[test]
    fn factory_creates_the_expected_backend_modes() {
        let directory = temporary_directory("factory-modes");
        let scheduler = Arc::new(ManualScheduler::new());
        let factory = StorageFactory::with_scheduler(
            StorageConfig::new(&directory, StorageMode::Hybrid)
                .with_service_override(
                    "s3",
                    ServiceStorageConfig::default()
                        .with_mode(StorageMode::Wal),
                )
                .with_service_override(
                    "sqs",
                    ServiceStorageConfig::default()
                        .with_mode(StorageMode::Persistent),
                ),
            scheduler,
        );

        let hybrid = factory.create::<String, String>("sns", "topics");
        let wal = factory.create::<String, String>("s3", "objects");
        let persistent = factory.create::<String, String>("sqs", "queues");

        assert_eq!(hybrid.mode(), StorageMode::Hybrid);
        assert_eq!(wal.mode(), StorageMode::Wal);
        assert_eq!(persistent.mode(), StorageMode::Persistent);
    }

    #[test]
    fn factory_loads_and_shuts_down_all_backends() {
        let directory = temporary_directory("factory-lifecycle");
        let scheduler = Arc::new(ManualScheduler::new());
        let factory = StorageFactory::with_scheduler(
            StorageConfig::new(&directory, StorageMode::Wal)
                .with_wal_compaction_interval(Duration::from_secs(10)),
            scheduler.clone(),
        );
        let backend = factory.create::<String, String>("dynamodb", "tables");
        backend
            .put("table".to_owned(), "state".to_owned())
            .expect("put should succeed");
        factory.shutdown_all().expect("shutdown should succeed");
        assert_eq!(scheduler.active_task_count(), 0);

        let reloaded =
            factory.create::<String, String>("dynamodb", "tables-reloaded");
        reloaded
            .put("placeholder".to_owned(), "value".to_owned())
            .expect("create after shutdown should still work");

        let (snapshot_path, wal_path) =
            factory.wal_paths("dynamodb", "tables");
        assert!(snapshot_path.exists());
        assert!(
            !wal_path.exists()
                || fs::metadata(wal_path).expect("wal metadata").len() == 0
        );
    }

    #[test]
    fn factory_creates_scoped_repositories_with_typed_scope() {
        let directory = temporary_directory("factory-scoped");
        let factory = StorageFactory::new(StorageConfig::new(
            &directory,
            StorageMode::Wal,
        ));
        let storage =
            factory.create_scoped::<String, String>("ssm", "parameters");
        let eu_scope = StorageScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let us_scope = StorageScope::new(
            "111111111111".parse().expect("account id should parse"),
            "us-east-1".parse().expect("region should parse"),
        );

        storage.load().expect("load should succeed");
        storage
            .put(&eu_scope, "name".to_owned(), "eu".to_owned())
            .expect("put should succeed");
        storage
            .put(&us_scope, "name".to_owned(), "us".to_owned())
            .expect("put should succeed");

        assert_eq!(
            storage.get(&eu_scope, &"name".to_owned()),
            Some("eu".to_owned())
        );
        assert_eq!(
            storage.get(&us_scope, &"name".to_owned()),
            Some("us".to_owned())
        );
    }
}
