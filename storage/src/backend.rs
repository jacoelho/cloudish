use crate::{StorageError, StorageMode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;

pub trait StorageKey:
    Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl<T> StorageKey for T where
    T: Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

pub trait StorageValue:
    Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl<T> StorageValue for T where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

pub type ScanPredicate<K> = dyn Fn(&K) -> bool + Send + Sync;
pub type StorageHandle<K, V> = Arc<dyn StorageBackend<K, V>>;

pub trait StorageBackend<K: StorageKey, V: StorageValue>: Send + Sync {
    fn mode(&self) -> StorageMode;

    /// Persists a value under the provided key.
    ///
    /// # Errors
    ///
    /// Returns a storage I/O or serialization error when the write cannot be
    /// recorded durably.
    fn put(&self, key: K, value: V) -> Result<(), StorageError>;

    fn get(&self, key: &K) -> Option<V>;

    /// Removes the value stored for the provided key.
    ///
    /// # Errors
    ///
    /// Returns a storage I/O or serialization error when the delete cannot be
    /// recorded durably.
    fn delete(&self, key: &K) -> Result<(), StorageError>;

    fn scan(&self, key_filter: &ScanPredicate<K>) -> Vec<V>;

    fn keys(&self) -> Vec<K>;

    /// Flushes any buffered mutations to the durable backend.
    ///
    /// # Errors
    ///
    /// Returns a storage I/O or serialization error when the backend cannot
    /// persist its current state.
    fn flush(&self) -> Result<(), StorageError>;

    /// Loads persisted state into memory.
    ///
    /// # Errors
    ///
    /// Returns a storage I/O or serialization error when the persisted state
    /// cannot be read or decoded.
    fn load(&self) -> Result<(), StorageError>;

    /// Clears all persisted and in-memory state for the backend.
    ///
    /// # Errors
    ///
    /// Returns a storage I/O or serialization error when the backend cannot be
    /// reset.
    fn clear(&self) -> Result<(), StorageError>;

    /// Stops background work and flushes any remaining state.
    ///
    /// # Errors
    ///
    /// Returns a storage I/O or serialization error when shutdown flushing
    /// fails.
    fn shutdown(&self) -> Result<(), StorageError>;
}
