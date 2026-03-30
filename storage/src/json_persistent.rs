use crate::snapshot::{load_snapshot, write_snapshot_atomic};
use crate::{
    ScanPredicate, StorageBackend, StorageError, StorageKey, StorageMode,
    StorageValue,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{LockResult, RwLock};

#[derive(Debug)]
pub struct PersistentStorage<K: StorageKey, V: StorageValue> {
    snapshot_path: PathBuf,
    state: RwLock<BTreeMap<K, V>>,
}

impl<K: StorageKey, V: StorageValue> PersistentStorage<K, V> {
    pub fn new(snapshot_path: impl Into<PathBuf>) -> Self {
        Self {
            snapshot_path: snapshot_path.into(),
            state: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    fn apply_durable_change<F>(&self, change: F) -> Result<(), StorageError>
    where
        F: FnOnce(&mut BTreeMap<K, V>),
    {
        let mut state = recover(self.state.write());
        let mut next = state.clone();
        change(&mut next);
        write_snapshot_atomic(&self.snapshot_path, &next)?;
        *state = next;
        Ok(())
    }
}

impl<K: StorageKey, V: StorageValue> StorageBackend<K, V>
    for PersistentStorage<K, V>
{
    fn mode(&self) -> StorageMode {
        StorageMode::Persistent
    }

    fn put(&self, key: K, value: V) -> Result<(), StorageError> {
        self.apply_durable_change(|state| {
            state.insert(key, value);
        })
    }

    fn get(&self, key: &K) -> Option<V> {
        recover(self.state.read()).get(key).cloned()
    }

    fn delete(&self, key: &K) -> Result<(), StorageError> {
        self.apply_durable_change(|state| {
            state.remove(key);
        })
    }

    fn scan(&self, key_filter: &ScanPredicate<K>) -> Vec<V> {
        recover(self.state.read())
            .iter()
            .filter(|(key, _)| key_filter(key))
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn keys(&self) -> Vec<K> {
        recover(self.state.read()).keys().cloned().collect()
    }

    fn flush(&self) -> Result<(), StorageError> {
        let snapshot = recover(self.state.read()).clone();
        write_snapshot_atomic(&self.snapshot_path, &snapshot)
    }

    fn load(&self) -> Result<(), StorageError> {
        let snapshot = load_snapshot(&self.snapshot_path)?;
        *recover(self.state.write()) = snapshot;
        Ok(())
    }

    fn clear(&self) -> Result<(), StorageError> {
        self.apply_durable_change(BTreeMap::clear)
    }

    fn shutdown(&self) -> Result<(), StorageError> {
        self.flush()
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::PersistentStorage;
    use crate::{StorageBackend, StorageError};
    use std::fs;
    use test_support::temporary_directory;

    #[test]
    fn persistent_storage_survives_restart() {
        let directory = temporary_directory("persistent-restart");
        let path = directory.join("store.json");
        let storage = PersistentStorage::<String, String>::new(&path);
        storage.load().expect("load should succeed");
        storage
            .put("key".to_owned(), "value".to_owned())
            .expect("put should persist");

        let reloaded = PersistentStorage::<String, String>::new(&path);
        reloaded.load().expect("load should succeed");

        assert_eq!(reloaded.get(&"key".to_owned()), Some("value".to_owned()));
    }

    #[test]
    fn failed_persistent_write_leaves_last_good_snapshot() {
        let directory = temporary_directory("persistent-failure");
        let path = directory.join("store.json");
        let storage = PersistentStorage::<String, String>::new(&path);
        storage
            .put("stable".to_owned(), "value".to_owned())
            .expect("initial write should succeed");
        fs::create_dir_all(path.with_file_name("store.json.tmp"))
            .expect("temp blocker should be creatable");

        let error = storage
            .put("new".to_owned(), "value".to_owned())
            .expect_err("blocked temp file should fail");

        match error {
            StorageError::WriteSnapshot { .. } => {}
            other => panic!("unexpected error: {other}"),
        }

        let reloaded = PersistentStorage::<String, String>::new(&path);
        reloaded.load().expect("reloaded snapshot should decode");
        assert_eq!(
            reloaded.get(&"stable".to_owned()),
            Some("value".to_owned())
        );
        assert_eq!(
            storage.get(&"new".to_owned()),
            None,
            "failed writes must not become visible in memory"
        );
    }
}
