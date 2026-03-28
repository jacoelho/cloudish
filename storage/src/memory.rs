use crate::{
    ScanPredicate, StorageBackend, StorageError, StorageKey, StorageMode,
    StorageValue,
};
use std::collections::BTreeMap;
use std::sync::{LockResult, RwLock};

#[derive(Debug, Default)]
pub struct InMemoryStorage<K: StorageKey, V: StorageValue> {
    state: RwLock<BTreeMap<K, V>>,
}

impl<K: StorageKey, V: StorageValue> InMemoryStorage<K, V> {
    pub fn new() -> Self {
        Self { state: RwLock::new(BTreeMap::new()) }
    }
}

impl<K: StorageKey, V: StorageValue> StorageBackend<K, V>
    for InMemoryStorage<K, V>
{
    fn mode(&self) -> StorageMode {
        StorageMode::Memory
    }

    fn put(&self, key: K, value: V) -> Result<(), StorageError> {
        recover(self.state.write()).insert(key, value);
        Ok(())
    }

    fn get(&self, key: &K) -> Option<V> {
        recover(self.state.read()).get(key).cloned()
    }

    fn delete(&self, key: &K) -> Result<(), StorageError> {
        recover(self.state.write()).remove(key);
        Ok(())
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
        Ok(())
    }

    fn load(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn clear(&self) -> Result<(), StorageError> {
        recover(self.state.write()).clear();
        Ok(())
    }

    fn shutdown(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::InMemoryStorage;
    use crate::StorageBackend;

    #[test]
    fn in_memory_storage_supports_crud_and_scan() {
        let storage = InMemoryStorage::<String, String>::new();
        storage
            .put("key1".to_owned(), "value1".to_owned())
            .expect("put should succeed");
        storage
            .put("key2".to_owned(), "value2".to_owned())
            .expect("put should succeed");

        assert_eq!(storage.get(&"key1".to_owned()), Some("value1".to_owned()));
        assert_eq!(storage.scan(&|key| key.starts_with("key")).len(), 2);
        assert_eq!(storage.keys(), vec!["key1".to_owned(), "key2".to_owned()]);

        storage.delete(&"key1".to_owned()).expect("delete should succeed");
        assert_eq!(storage.get(&"key1".to_owned()), None);

        storage.clear().expect("clear should succeed");
        assert!(storage.keys().is_empty());
        storage.flush().expect("flush is a no-op");
        storage.load().expect("load is a no-op");
        storage.shutdown().expect("shutdown is a no-op");
    }
}
