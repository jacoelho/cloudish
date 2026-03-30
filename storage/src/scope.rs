use crate::{StorageError, StorageHandle, StorageKey, StorageValue};
use aws::{AccountId, RegionId};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct StorageScope {
    account_id: AccountId,
    region: RegionId,
}

impl StorageScope {
    pub fn new(account_id: AccountId, region: RegionId) -> Self {
        Self { account_id, region }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }
}

pub trait StorageScopeProvider {
    fn storage_scope(&self) -> StorageScope;
}

impl StorageScopeProvider for StorageScope {
    fn storage_scope(&self) -> StorageScope {
        self.clone()
    }
}

impl<T> StorageScopeProvider for &T
where
    T: StorageScopeProvider + ?Sized,
{
    fn storage_scope(&self) -> StorageScope {
        (*self).storage_scope()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ScopedStorageKey<K> {
    key: K,
    scope: StorageScope,
}

impl<K: StorageKey> ScopedStorageKey<K> {
    fn new(scope: StorageScope, key: K) -> Self {
        Self { key, scope }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn scope(&self) -> &StorageScope {
        &self.scope
    }
}

#[derive(Clone)]
pub struct ScopedStorage<K: StorageKey, V: StorageValue> {
    backend: StorageHandle<ScopedStorageKey<K>, V>,
}

impl<K: StorageKey, V: StorageValue> ScopedStorage<K, V> {
    pub fn new(backend: StorageHandle<ScopedStorageKey<K>, V>) -> Self {
        Self { backend }
    }

    pub fn repository<S>(&self, scope: S) -> StorageRepository<K, V>
    where
        S: StorageScopeProvider,
    {
        StorageRepository {
            backend: self.backend.clone(),
            scope: scope.storage_scope(),
        }
    }

    /// Stores a scoped value in the underlying backend.
    ///
    /// # Errors
    ///
    /// Returns any backend persistence error raised while recording the
    /// mutation.
    pub fn put<S>(
        &self,
        scope: &S,
        key: K,
        value: V,
    ) -> Result<(), StorageError>
    where
        S: StorageScopeProvider + ?Sized,
    {
        self.repository(scope).put(key, value)
    }

    pub fn get<S>(&self, scope: &S, key: &K) -> Option<V>
    where
        S: StorageScopeProvider + ?Sized,
    {
        self.repository(scope).get(key)
    }

    /// Deletes a scoped key from the underlying backend.
    ///
    /// # Errors
    ///
    /// Returns any backend persistence error raised while recording the
    /// deletion.
    pub fn delete<S>(&self, scope: &S, key: &K) -> Result<(), StorageError>
    where
        S: StorageScopeProvider + ?Sized,
    {
        self.repository(scope).delete(key)
    }

    pub fn scan<S, F>(&self, scope: &S, key_filter: F) -> Vec<V>
    where
        S: StorageScopeProvider + ?Sized,
        F: Fn(&K) -> bool + Send + Sync + 'static,
    {
        self.repository(scope).scan(key_filter)
    }

    pub fn keys<S>(&self, scope: &S) -> Vec<K>
    where
        S: StorageScopeProvider + ?Sized,
    {
        self.repository(scope).keys()
    }

    /// Clears all keys stored for the provided scope.
    ///
    /// # Errors
    ///
    /// Returns any backend persistence error raised while rewriting the scoped
    /// state.
    pub fn clear<S>(&self, scope: &S) -> Result<(), StorageError>
    where
        S: StorageScopeProvider + ?Sized,
    {
        self.repository(scope).clear()
    }

    pub fn scan_all<F>(&self, key_filter: F) -> Vec<(StorageScope, V)>
    where
        F: Fn(&K) -> bool,
    {
        self.backend
            .keys()
            .into_iter()
            .filter(|candidate| key_filter(candidate.key()))
            .filter_map(|candidate| {
                self.backend
                    .get(&candidate)
                    .map(|value| (candidate.scope().clone(), value))
            })
            .collect()
    }

    /// Flushes the underlying backend.
    ///
    /// # Errors
    ///
    /// Returns any backend error raised while persisting buffered mutations.
    pub fn flush(&self) -> Result<(), StorageError> {
        self.backend.flush()
    }

    /// Loads the underlying backend state.
    ///
    /// # Errors
    ///
    /// Returns any backend error raised while reading or decoding persisted
    /// state.
    pub fn load(&self) -> Result<(), StorageError> {
        self.backend.load()
    }

    /// Shuts down the underlying backend.
    ///
    /// # Errors
    ///
    /// Returns any backend error raised while flushing final state.
    pub fn shutdown(&self) -> Result<(), StorageError> {
        self.backend.shutdown()
    }
}

#[derive(Clone)]
pub struct StorageRepository<K: StorageKey, V: StorageValue> {
    backend: StorageHandle<ScopedStorageKey<K>, V>,
    scope: StorageScope,
}

impl<K: StorageKey, V: StorageValue> StorageRepository<K, V> {
    pub fn scope(&self) -> &StorageScope {
        &self.scope
    }

    /// Stores a value inside this repository scope.
    ///
    /// # Errors
    ///
    /// Returns any backend persistence error raised while recording the
    /// mutation.
    pub fn put(&self, key: K, value: V) -> Result<(), StorageError> {
        self.backend.put(ScopedStorageKey::new(self.scope.clone(), key), value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.backend
            .get(&ScopedStorageKey::new(self.scope.clone(), key.clone()))
    }

    /// Deletes a value inside this repository scope.
    ///
    /// # Errors
    ///
    /// Returns any backend persistence error raised while recording the
    /// deletion.
    pub fn delete(&self, key: &K) -> Result<(), StorageError> {
        self.backend
            .delete(&ScopedStorageKey::new(self.scope.clone(), key.clone()))
    }

    pub fn scan<F>(&self, key_filter: F) -> Vec<V>
    where
        F: Fn(&K) -> bool + Send + Sync + 'static,
    {
        let scope = self.scope.clone();

        self.backend.scan(&move |candidate| {
            candidate.scope == scope && key_filter(candidate.key())
        })
    }

    pub fn keys(&self) -> Vec<K> {
        self.backend
            .keys()
            .into_iter()
            .filter(|candidate| candidate.scope == self.scope)
            .map(|candidate| candidate.key)
            .collect()
    }

    /// Clears every key stored in this repository scope.
    ///
    /// # Errors
    ///
    /// Returns any backend persistence error raised while rewriting the scoped
    /// state.
    pub fn clear(&self) -> Result<(), StorageError> {
        for key in self.keys() {
            self.delete(&key)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ScopedStorage, StorageScope};
    use crate::InMemoryStorage;
    use std::sync::Arc;

    fn scope(account_id: &str, region: &str) -> StorageScope {
        StorageScope::new(
            account_id.parse().expect("account id should parse"),
            region.parse().expect("region should parse"),
        )
    }

    #[test]
    fn scoped_storage_keeps_identical_keys_isolated_per_scope() {
        let storage = ScopedStorage::new(Arc::new(InMemoryStorage::<
            super::ScopedStorageKey<String>,
            String,
        >::new()));
        let eu_scope = scope("000000000000", "eu-west-2");
        let us_scope = scope("111111111111", "us-east-1");

        storage
            .put(&eu_scope, "demo".to_owned(), "eu".to_owned())
            .expect("put should succeed");
        storage
            .put(&us_scope, "demo".to_owned(), "us".to_owned())
            .expect("put should succeed");

        assert_eq!(
            storage.get(&eu_scope, &"demo".to_owned()),
            Some("eu".to_owned())
        );
        assert_eq!(
            storage.get(&us_scope, &"demo".to_owned()),
            Some("us".to_owned())
        );
        assert_eq!(storage.keys(&eu_scope), vec!["demo".to_owned()]);
        assert_eq!(storage.keys(&us_scope), vec!["demo".to_owned()]);
    }

    #[test]
    fn scoped_repository_clear_removes_only_one_scope() {
        let storage = ScopedStorage::new(Arc::new(InMemoryStorage::<
            super::ScopedStorageKey<String>,
            String,
        >::new()));
        let eu_scope = scope("000000000000", "eu-west-2");
        let us_scope = scope("111111111111", "us-east-1");

        storage
            .put(&eu_scope, "demo".to_owned(), "eu".to_owned())
            .expect("put should succeed");
        storage
            .put(&us_scope, "demo".to_owned(), "us".to_owned())
            .expect("put should succeed");

        storage.repository(&eu_scope).clear().expect("clear should succeed");

        assert_eq!(storage.get(&eu_scope, &"demo".to_owned()), None);
        assert_eq!(
            storage.get(&us_scope, &"demo".to_owned()),
            Some("us".to_owned())
        );
    }
}
