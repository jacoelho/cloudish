use crate::snapshot::{load_snapshot, write_snapshot_atomic};
use crate::{
    ScanPredicate, ScheduleHandle, Scheduler, StorageBackend, StorageError,
    StorageKey, StorageMode, StorageValue, ThreadScheduler,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LockResult, Mutex, RwLock};
use std::time::Duration;

pub const OP_PUT: u8 = 0x01;
pub const OP_DELETE: u8 = 0x02;

pub struct WalStorage<K: StorageKey, V: StorageValue> {
    inner: Arc<WalInner<K, V>>,
    schedule: Mutex<Option<Box<dyn ScheduleHandle>>>,
}

#[derive(Debug)]
struct WalInner<K: StorageKey, V: StorageValue> {
    compacted_version: AtomicU64,
    current_version: AtomicU64,
    mutation_lock: Mutex<()>,
    snapshot_path: PathBuf,
    state: RwLock<BTreeMap<K, V>>,
    wal_path: PathBuf,
    wal_writer: Mutex<Option<File>>,
}

impl<K: StorageKey, V: StorageValue> WalStorage<K, V> {
    pub fn new(
        snapshot_path: impl Into<PathBuf>,
        wal_path: impl Into<PathBuf>,
        compaction_interval: Duration,
    ) -> Self {
        Self::with_scheduler(
            snapshot_path,
            wal_path,
            compaction_interval,
            Arc::new(ThreadScheduler),
        )
    }

    pub fn with_scheduler(
        snapshot_path: impl Into<PathBuf>,
        wal_path: impl Into<PathBuf>,
        compaction_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        let inner = Arc::new(WalInner {
            compacted_version: AtomicU64::new(0),
            current_version: AtomicU64::new(0),
            mutation_lock: Mutex::new(()),
            snapshot_path: snapshot_path.into(),
            state: RwLock::new(BTreeMap::new()),
            wal_path: wal_path.into(),
            wal_writer: Mutex::new(None),
        });
        let scheduled_inner = Arc::clone(&inner);
        let schedule = scheduler.schedule_repeating(
            "wal-storage-compaction",
            compaction_interval,
            Arc::new(move || {
                let _ = scheduled_inner.compact_if_dirty();
            }),
        );

        Self { inner, schedule: Mutex::new(Some(schedule)) }
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.inner.snapshot_path
    }

    pub fn wal_path(&self) -> &Path {
        &self.inner.wal_path
    }

    fn cancel_schedule(&self) {
        if let Some(handle) = recover(self.schedule.lock()).take() {
            handle.cancel();
        }
    }
}

impl<K: StorageKey, V: StorageValue> WalInner<K, V> {
    fn compact_if_dirty(&self) -> Result<(), StorageError> {
        if self.current_version.load(Ordering::SeqCst)
            == self.compacted_version.load(Ordering::SeqCst)
        {
            return Ok(());
        }

        let _mutation_guard = recover(self.mutation_lock.lock());
        if self.current_version.load(Ordering::SeqCst)
            == self.compacted_version.load(Ordering::SeqCst)
        {
            return Ok(());
        }

        let snapshot = recover(self.state.read()).clone();
        write_snapshot_atomic(&self.snapshot_path, &snapshot)?;
        self.close_writer();
        let _ = fs::remove_file(&self.wal_path);
        self.compacted_version.store(
            self.current_version.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        Ok(())
    }

    fn ensure_writer(&self) -> Result<(), StorageError> {
        let mut writer = recover(self.wal_writer.lock());
        if writer.is_some() {
            return Ok(());
        }

        let directory =
            self.wal_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(directory).map_err(|source| {
            StorageError::CreateDirectory {
                path: directory.to_path_buf(),
                source,
            }
        })?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)
            .map_err(|source| StorageError::OpenWal {
                path: self.wal_path.clone(),
                source,
            })?;
        *writer = Some(file);
        Ok(())
    }

    fn close_writer(&self) {
        let _ = recover(self.wal_writer.lock()).take();
    }

    fn append_put(&self, key: &K, value: &V) -> Result<(), StorageError> {
        let key_bytes = encode_cbor(&self.wal_path, key)?;
        let value_bytes = encode_cbor(&self.wal_path, value)?;
        self.ensure_writer()?;

        let mut writer = recover(self.wal_writer.lock());
        let writer = writer.as_mut().ok_or_else(|| StorageError::OpenWal {
            path: self.wal_path.clone(),
            source: io::Error::other(
                "WAL writer missing after initialization",
            ),
        })?;
        writer
            .write_all(&[OP_PUT])
            .and_then(|_| {
                writer.write_all(&(key_bytes.len() as u32).to_be_bytes())
            })
            .and_then(|_| writer.write_all(&key_bytes))
            .and_then(|_| {
                writer.write_all(&(value_bytes.len() as u32).to_be_bytes())
            })
            .and_then(|_| writer.write_all(&value_bytes))
            .and_then(|_| writer.sync_data())
            .map_err(|source| StorageError::WriteWal {
                path: self.wal_path.clone(),
                source,
            })
    }

    fn append_delete(&self, key: &K) -> Result<(), StorageError> {
        let key_bytes = encode_cbor(&self.wal_path, key)?;
        self.ensure_writer()?;

        let mut writer = recover(self.wal_writer.lock());
        let writer = writer.as_mut().ok_or_else(|| StorageError::OpenWal {
            path: self.wal_path.clone(),
            source: io::Error::other(
                "WAL writer missing after initialization",
            ),
        })?;
        writer
            .write_all(&[OP_DELETE])
            .and_then(|_| {
                writer.write_all(&(key_bytes.len() as u32).to_be_bytes())
            })
            .and_then(|_| writer.write_all(&key_bytes))
            .and_then(|_| writer.sync_data())
            .map_err(|source| StorageError::WriteWal {
                path: self.wal_path.clone(),
                source,
            })
    }

    fn replay_wal(
        &self,
        state: &mut BTreeMap<K, V>,
    ) -> Result<bool, StorageError> {
        if !self.wal_path.exists() {
            return Ok(false);
        }

        let file = File::open(&self.wal_path).map_err(|source| {
            StorageError::ReadWal { path: self.wal_path.clone(), source }
        })?;
        let mut reader = BufReader::new(file);
        let mut replayed = false;

        while let Some(opcode) = read_opcode(&mut reader, &self.wal_path)? {
            if opcode != OP_PUT && opcode != OP_DELETE {
                return Err(StorageError::UnknownWalOp {
                    path: self.wal_path.clone(),
                    op: opcode,
                });
            }
            let key_len = match read_be_u32(&mut reader, &self.wal_path)? {
                Some(length) => length as usize,
                None => break,
            };
            let key_bytes =
                match read_bytes(&mut reader, &self.wal_path, key_len)? {
                    Some(bytes) => bytes,
                    None => break,
                };
            let key = decode_cbor(&self.wal_path, &key_bytes)?;

            match opcode {
                OP_PUT => {
                    let value_len =
                        match read_be_u32(&mut reader, &self.wal_path)? {
                            Some(length) => length as usize,
                            None => break,
                        };
                    let value_bytes = match read_bytes(
                        &mut reader,
                        &self.wal_path,
                        value_len,
                    )? {
                        Some(bytes) => bytes,
                        None => break,
                    };
                    let value = decode_cbor(&self.wal_path, &value_bytes)?;
                    state.insert(key, value);
                    replayed = true;
                }
                OP_DELETE => {
                    state.remove(&key);
                    replayed = true;
                }
                _ => {
                    return Err(StorageError::UnknownWalOp {
                        path: self.wal_path.clone(),
                        op: opcode,
                    });
                }
            }
        }

        Ok(replayed)
    }
}

impl<K: StorageKey, V: StorageValue> StorageBackend<K, V>
    for WalStorage<K, V>
{
    fn mode(&self) -> StorageMode {
        StorageMode::Wal
    }

    fn put(&self, key: K, value: V) -> Result<(), StorageError> {
        let _mutation_guard = recover(self.inner.mutation_lock.lock());
        self.inner.append_put(&key, &value)?;
        recover(self.inner.state.write()).insert(key, value);
        self.inner.current_version.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn get(&self, key: &K) -> Option<V> {
        recover(self.inner.state.read()).get(key).cloned()
    }

    fn delete(&self, key: &K) -> Result<(), StorageError> {
        let _mutation_guard = recover(self.inner.mutation_lock.lock());
        self.inner.append_delete(key)?;
        recover(self.inner.state.write()).remove(key);
        self.inner.current_version.fetch_add(1, Ordering::SeqCst);
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
        self.inner.compact_if_dirty()
    }

    fn load(&self) -> Result<(), StorageError> {
        let _mutation_guard = recover(self.inner.mutation_lock.lock());
        self.inner.close_writer();

        let mut snapshot = load_snapshot(&self.inner.snapshot_path)?;
        let replayed = self.inner.replay_wal(&mut snapshot)?;
        *recover(self.inner.state.write()) = snapshot;
        self.inner
            .current_version
            .store(u64::from(replayed), Ordering::SeqCst);
        self.inner.compacted_version.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn clear(&self) -> Result<(), StorageError> {
        let _mutation_guard = recover(self.inner.mutation_lock.lock());
        write_snapshot_atomic(
            &self.inner.snapshot_path,
            &BTreeMap::<K, V>::new(),
        )?;
        self.inner.close_writer();
        let _ = fs::remove_file(&self.inner.wal_path);
        recover(self.inner.state.write()).clear();
        self.inner.current_version.store(0, Ordering::SeqCst);
        self.inner.compacted_version.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn shutdown(&self) -> Result<(), StorageError> {
        self.cancel_schedule();
        self.flush()?;
        self.inner.close_writer();
        Ok(())
    }
}

impl<K: StorageKey, V: StorageValue> Drop for WalStorage<K, V> {
    fn drop(&mut self) {
        self.cancel_schedule();
        self.inner.close_writer();
    }
}

fn encode_cbor<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).map_err(|source| {
        StorageError::EncodeWal {
            path: path.to_path_buf(),
            details: source.to_string(),
        }
    })?;
    Ok(bytes)
}

fn decode_cbor<T: serde::de::DeserializeOwned>(
    path: &Path,
    bytes: &[u8],
) -> Result<T, StorageError> {
    ciborium::from_reader(bytes).map_err(|source| StorageError::DecodeWal {
        path: path.to_path_buf(),
        details: source.to_string(),
    })
}

fn read_opcode<R: Read>(
    reader: &mut R,
    path: &Path,
) -> Result<Option<u8>, StorageError> {
    let mut opcode = [0_u8; 1];
    match reader.read_exact(&mut opcode) {
        Ok(()) => Ok(Some(opcode[0])),
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            Ok(None)
        }
        Err(source) => {
            Err(StorageError::ReadWal { path: path.to_path_buf(), source })
        }
    }
}

fn read_be_u32<R: Read>(
    reader: &mut R,
    path: &Path,
) -> Result<Option<u32>, StorageError> {
    let mut bytes = [0_u8; 4];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(Some(u32::from_be_bytes(bytes))),
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            Ok(None)
        }
        Err(source) => {
            Err(StorageError::ReadWal { path: path.to_path_buf(), source })
        }
    }
}

fn read_bytes<R: Read>(
    reader: &mut R,
    path: &Path,
    length: usize,
) -> Result<Option<Vec<u8>>, StorageError> {
    let mut bytes = vec![0_u8; length];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(Some(bytes)),
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            Ok(None)
        }
        Err(source) => {
            Err(StorageError::ReadWal { path: path.to_path_buf(), source })
        }
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{OP_DELETE, OP_PUT, WalStorage};
    use crate::{ManualScheduler, StorageBackend, StorageError};
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Cursor;
    use std::io::Read;
    use std::sync::Arc;
    use std::time::Duration;
    use test_support::temporary_directory;

    #[test]
    fn wal_replays_uncompacted_mutations_after_restart() {
        let directory = temporary_directory("wal-replay");
        let scheduler = Arc::new(ManualScheduler::new());
        let snapshot = directory.join("state-snapshot.json");
        let wal = directory.join("state.wal");
        let storage = WalStorage::<String, String>::with_scheduler(
            &snapshot,
            &wal,
            Duration::from_secs(30),
            scheduler,
        );

        storage.load().expect("load should succeed");
        storage
            .put("a".to_owned(), "one".to_owned())
            .expect("put should succeed");
        storage
            .put("b".to_owned(), "two".to_owned())
            .expect("put should succeed");
        storage.delete(&"a".to_owned()).expect("delete should succeed");

        let reloaded = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );
        reloaded.load().expect("load should replay");

        assert_eq!(reloaded.get(&"a".to_owned()), None);
        assert_eq!(reloaded.get(&"b".to_owned()), Some("two".to_owned()));
    }

    #[test]
    fn wal_skips_truncated_tails_safely() {
        let directory = temporary_directory("wal-truncated");
        let snapshot = directory.join("state-snapshot.json");
        let wal = directory.join("state.wal");
        let storage = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );
        storage.load().expect("load should succeed");
        storage
            .put("good".to_owned(), "entry".to_owned())
            .expect("put should succeed");
        storage
            .put("partial".to_owned(), "entry".to_owned())
            .expect("put should succeed");
        let wal_len = fs::metadata(&wal).expect("wal should exist").len();
        let file = OpenOptions::new()
            .write(true)
            .open(&wal)
            .expect("wal should be writable");
        file.set_len(wal_len - 2).expect("wal should be truncatable");

        let reloaded = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );
        reloaded.load().expect("load should stop safely");

        assert_eq!(reloaded.get(&"good".to_owned()), Some("entry".to_owned()));
    }

    #[test]
    fn wal_flush_compacts_snapshot_and_truncates_log() {
        let directory = temporary_directory("wal-flush");
        let scheduler = Arc::new(ManualScheduler::new());
        let snapshot = directory.join("state-snapshot.json");
        let wal = directory.join("state.wal");
        let storage = WalStorage::<String, String>::with_scheduler(
            &snapshot,
            &wal,
            Duration::from_secs(1),
            scheduler,
        );
        storage
            .put("key".to_owned(), "value".to_owned())
            .expect("put should succeed");
        storage.flush().expect("flush should compact");

        assert!(snapshot.exists());
        assert!(
            !wal.exists()
                || fs::metadata(&wal).expect("wal metadata").len() == 0
        );
    }

    #[test]
    fn wal_shutdown_flushes_pending_state() {
        let directory = temporary_directory("wal-shutdown");
        let snapshot = directory.join("state-snapshot.json");
        let wal = directory.join("state.wal");
        let storage = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );
        storage
            .put("key".to_owned(), "value".to_owned())
            .expect("put should succeed");
        storage.shutdown().expect("shutdown should compact");

        let reloaded = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );
        reloaded.load().expect("load should succeed");
        assert_eq!(reloaded.get(&"key".to_owned()), Some("value".to_owned()));
    }

    #[test]
    fn wal_writes_expected_binary_format() {
        let directory = temporary_directory("wal-format");
        let snapshot = directory.join("state-snapshot.json");
        let wal = directory.join("state.wal");
        let storage = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );
        storage.load().expect("load should succeed");
        storage
            .put("k".to_owned(), "v".to_owned())
            .expect("put should succeed");
        storage.delete(&"d".to_owned()).expect("delete should succeed");

        let bytes = fs::read(&wal).expect("wal should exist");
        let mut cursor = Cursor::new(bytes);
        let mut opcode = [0_u8; 1];
        cursor.read_exact(&mut opcode).expect("put op should exist");
        assert_eq!(opcode[0], OP_PUT);

        let mut length = [0_u8; 4];
        cursor.read_exact(&mut length).expect("key length should exist");
        let key_len = u32::from_be_bytes(length) as usize;
        let mut key_bytes = vec![0_u8; key_len];
        cursor.read_exact(&mut key_bytes).expect("key bytes should exist");
        assert_eq!(key_bytes, vec![0x61, b'k']);

        cursor.read_exact(&mut length).expect("value length should exist");
        let value_len = u32::from_be_bytes(length) as usize;
        let mut value_bytes = vec![0_u8; value_len];
        cursor.read_exact(&mut value_bytes).expect("value bytes should exist");
        assert_eq!(value_bytes, vec![0x61, b'v']);

        cursor.read_exact(&mut opcode).expect("delete op should exist");
        assert_eq!(opcode[0], OP_DELETE);
    }

    #[test]
    fn wal_surfaces_unknown_opcodes() {
        let directory = temporary_directory("wal-unknown-op");
        let snapshot = directory.join("state-snapshot.json");
        let wal = directory.join("state.wal");
        fs::write(&wal, [0x7f, 0, 0, 0, 0]).expect("wal bytes should write");
        let storage = WalStorage::<String, String>::new(
            &snapshot,
            &wal,
            Duration::from_secs(30),
        );

        let error = storage.load().expect_err("unknown op should fail");

        match error {
            StorageError::UnknownWalOp { op, .. } => assert_eq!(op, 0x7f),
            other => panic!("unexpected error: {other}"),
        }
    }
}
