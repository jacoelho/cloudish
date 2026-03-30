use crate::snapshot::{load_snapshot, write_snapshot_atomic};
use crate::{
    ScanPredicate, ScheduleHandle, Scheduler, StorageBackend, StorageError,
    StorageKey, StorageMode, StorageValue, ThreadScheduler,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LockResult, Mutex, RwLock};
use std::time::Duration;

pub struct HybridStorage<K: StorageKey, V: StorageValue> {
    inner: Arc<HybridInner<K, V>>,
    schedule: Mutex<Option<Box<dyn ScheduleHandle>>>,
}

#[derive(Debug)]
struct HybridInner<K: StorageKey, V: StorageValue> {
    current_version: AtomicU64,
    persisted_version: AtomicU64,
    flush_lock: Mutex<()>,
    snapshot_path: PathBuf,
    state: RwLock<BTreeMap<K, V>>,
}

impl<K: StorageKey, V: StorageValue> HybridStorage<K, V> {
    pub fn new(
        snapshot_path: impl Into<PathBuf>,
        flush_interval: Duration,
    ) -> Self {
        Self::with_scheduler(
            snapshot_path,
            flush_interval,
            Arc::new(ThreadScheduler),
        )
    }

    pub fn with_scheduler(
        snapshot_path: impl Into<PathBuf>,
        flush_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        let inner = Arc::new(HybridInner {
            current_version: AtomicU64::new(0),
            persisted_version: AtomicU64::new(0),
            flush_lock: Mutex::new(()),
            snapshot_path: snapshot_path.into(),
            state: RwLock::new(BTreeMap::new()),
        });
        let scheduled_inner = Arc::clone(&inner);
        let schedule = scheduler.schedule_repeating(
            "hybrid-storage-flush",
            flush_interval,
            Arc::new(move || {
                let _ = scheduled_inner.flush_if_dirty();
            }),
        );

        Self { inner, schedule: Mutex::new(Some(schedule)) }
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.inner.snapshot_path
    }

    fn cancel_schedule(&self) {
        if let Some(handle) = recover(self.schedule.lock()).take() {
            handle.cancel();
        }
    }

    fn mark_mutated(&self) {
        self.inner.current_version.fetch_add(1, Ordering::SeqCst);
    }
}

impl<K: StorageKey, V: StorageValue> HybridInner<K, V> {
    fn flush_if_dirty(&self) -> Result<(), StorageError> {
        if self.current_version.load(Ordering::SeqCst)
            == self.persisted_version.load(Ordering::SeqCst)
        {
            return Ok(());
        }

        let _flush_guard = recover(self.flush_lock.lock());
        if self.current_version.load(Ordering::SeqCst)
            == self.persisted_version.load(Ordering::SeqCst)
        {
            return Ok(());
        }

        let (version, snapshot) = {
            let state = recover(self.state.read());
            let version = self.current_version.load(Ordering::SeqCst);
            (version, state.clone())
        };
        write_snapshot_atomic(&self.snapshot_path, &snapshot)?;
        self.persisted_version.store(version, Ordering::SeqCst);
        Ok(())
    }
}

impl<K: StorageKey, V: StorageValue> StorageBackend<K, V>
    for HybridStorage<K, V>
{
    fn mode(&self) -> StorageMode {
        StorageMode::Hybrid
    }

    fn put(&self, key: K, value: V) -> Result<(), StorageError> {
        recover(self.inner.state.write()).insert(key, value);
        self.mark_mutated();
        Ok(())
    }

    fn get(&self, key: &K) -> Option<V> {
        recover(self.inner.state.read()).get(key).cloned()
    }

    fn delete(&self, key: &K) -> Result<(), StorageError> {
        recover(self.inner.state.write()).remove(key);
        self.mark_mutated();
        Ok(())
    }

    fn scan(&self, key_filter: &ScanPredicate<K>) -> Vec<V> {
        recover(self.inner.state.read())
            .iter()
            .filter(|(key, _)| key_filter(key))
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn keys(&self) -> Vec<K> {
        recover(self.inner.state.read()).keys().cloned().collect()
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.inner.flush_if_dirty()
    }

    fn load(&self) -> Result<(), StorageError> {
        let _flush_guard = recover(self.inner.flush_lock.lock());
        let mut state = recover(self.inner.state.write());
        let snapshot = load_snapshot(&self.inner.snapshot_path)?;
        *state = snapshot;
        self.inner.current_version.store(0, Ordering::SeqCst);
        self.inner.persisted_version.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn clear(&self) -> Result<(), StorageError> {
        let _flush_guard = recover(self.inner.flush_lock.lock());
        let mut state = recover(self.inner.state.write());
        write_snapshot_atomic(
            &self.inner.snapshot_path,
            &BTreeMap::<K, V>::new(),
        )?;
        state.clear();
        self.inner.current_version.store(0, Ordering::SeqCst);
        self.inner.persisted_version.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn shutdown(&self) -> Result<(), StorageError> {
        self.cancel_schedule();
        self.flush()
    }
}

impl<K: StorageKey, V: StorageValue> Drop for HybridStorage<K, V> {
    fn drop(&mut self) {
        self.cancel_schedule();
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::HybridStorage;
    use crate::{ManualScheduler, StorageBackend, StorageError};
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;
    use test_support::temporary_directory;

    #[test]
    fn hybrid_flushes_only_after_a_mutation() {
        let directory = temporary_directory("hybrid-dirty");
        let scheduler = Arc::new(ManualScheduler::new());
        let storage = HybridStorage::<String, String>::with_scheduler(
            directory.join("store.json"),
            Duration::from_secs(1),
            scheduler.clone(),
        );

        scheduler.run_pending();
        assert!(
            !storage.snapshot_path().exists(),
            "clean backends should not flush"
        );

        storage
            .put("key".to_owned(), "value".to_owned())
            .expect("put should succeed");
        scheduler.run_pending();
        assert!(storage.snapshot_path().exists());
    }

    #[test]
    fn hybrid_shutdown_forces_a_final_flush() {
        let directory = temporary_directory("hybrid-shutdown");
        let scheduler = Arc::new(ManualScheduler::new());
        let path = directory.join("store.json");
        let storage = HybridStorage::<String, String>::with_scheduler(
            &path,
            Duration::from_secs(1),
            scheduler,
        );
        storage
            .put("key".to_owned(), "value".to_owned())
            .expect("put should succeed");
        storage.shutdown().expect("shutdown should flush");

        let reloaded = HybridStorage::<String, String>::new(
            &path,
            Duration::from_secs(1),
        );
        reloaded.load().expect("load should succeed");
        assert_eq!(reloaded.get(&"key".to_owned()), Some("value".to_owned()));
    }

    #[test]
    fn hybrid_clear_is_restart_safe_without_an_extra_flush() {
        let directory = temporary_directory("hybrid-clear");
        let scheduler = Arc::new(ManualScheduler::new());
        let path = directory.join("store.json");
        let storage = HybridStorage::<String, String>::with_scheduler(
            &path,
            Duration::from_secs(1),
            scheduler,
        );
        storage.load().expect("load should succeed");
        storage
            .put("key".to_owned(), "value".to_owned())
            .expect("put should succeed");

        storage.clear().expect("clear should persist an empty snapshot");

        let reloaded = HybridStorage::<String, String>::new(
            &path,
            Duration::from_secs(1),
        );
        reloaded.load().expect("load should succeed");
        assert!(reloaded.keys().is_empty());
    }

    #[test]
    fn failed_hybrid_flush_keeps_data_dirty_until_retry() {
        let directory = temporary_directory("hybrid-retry");
        let scheduler = Arc::new(ManualScheduler::new());
        let path = directory.join("store.json");
        let storage = HybridStorage::<String, String>::with_scheduler(
            &path,
            Duration::from_secs(1),
            scheduler.clone(),
        );
        storage
            .put("stable".to_owned(), "value".to_owned())
            .expect("put should succeed");
        scheduler.run_pending();
        fs::create_dir_all(path.with_file_name("store.json.tmp"))
            .expect("temp blocker should exist");

        storage
            .put("retry".to_owned(), "value".to_owned())
            .expect("put should succeed");
        scheduler.run_pending();
        assert_eq!(storage.get(&"retry".to_owned()), Some("value".to_owned()));

        fs::remove_dir(path.with_file_name("store.json.tmp"))
            .expect("temp blocker should be removable");
        storage.flush().expect("retry flush should succeed");

        let reloaded = HybridStorage::<String, String>::new(
            &path,
            Duration::from_secs(1),
        );
        reloaded.load().expect("load should succeed");
        assert_eq!(
            reloaded.get(&"retry".to_owned()),
            Some("value".to_owned())
        );
    }

    #[test]
    fn hybrid_exposes_snapshot_write_failures_on_explicit_flush() {
        let directory = temporary_directory("hybrid-flush-error");
        let scheduler = Arc::new(ManualScheduler::new());
        let path = directory.join("store.json");
        let storage = HybridStorage::<String, String>::with_scheduler(
            &path,
            Duration::from_secs(1),
            scheduler,
        );
        storage
            .put("stable".to_owned(), "value".to_owned())
            .expect("put should succeed");
        fs::create_dir_all(path.with_file_name("store.json.tmp"))
            .expect("temp blocker should exist");

        let error = storage.flush().expect_err("flush should fail");
        match error {
            StorageError::WriteSnapshot { .. } => {}
            other => panic!("unexpected error: {other}"),
        }
    }
}
