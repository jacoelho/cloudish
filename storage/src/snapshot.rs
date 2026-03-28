use crate::{StorageError, StorageKey, StorageValue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEntry<K, V> {
    key: K,
    value: V,
}

pub(crate) fn load_snapshot<K, V>(
    path: &Path,
) -> Result<BTreeMap<K, V>, StorageError>
where
    K: StorageKey,
    V: StorageValue,
{
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let file = File::open(path).map_err(|source| {
        StorageError::ReadSnapshot { path: path.to_path_buf(), source }
    })?;
    let entries: Vec<SnapshotEntry<K, V>> =
        serde_json::from_reader(BufReader::new(file)).map_err(|source| {
            StorageError::DecodeSnapshot {
                path: path.to_path_buf(),
                details: source.to_string(),
            }
        })?;

    Ok(entries.into_iter().fold(BTreeMap::new(), |mut state, entry| {
        state.insert(entry.key, entry.value);
        state
    }))
}

pub(crate) fn write_snapshot_atomic<K, V>(
    path: &Path,
    state: &BTreeMap<K, V>,
) -> Result<(), StorageError>
where
    K: StorageKey,
    V: StorageValue,
{
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(directory).map_err(|source| {
        StorageError::CreateDirectory { path: directory.to_path_buf(), source }
    })?;

    let temp_path = temp_snapshot_path(path);
    let entries = state
        .iter()
        .map(|(key, value)| SnapshotEntry {
            key: key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();

    let result = (|| {
        let mut file = File::create(&temp_path).map_err(|source| {
            StorageError::WriteSnapshot { path: temp_path.clone(), source }
        })?;
        serde_json::to_writer_pretty(&mut file, &entries).map_err(
            |source| StorageError::EncodeSnapshot {
                path: temp_path.clone(),
                details: source.to_string(),
            },
        )?;
        file.write_all(b"\n").map_err(|source| {
            StorageError::WriteSnapshot { path: temp_path.clone(), source }
        })?;
        file.sync_all().map_err(|source| StorageError::WriteSnapshot {
            path: temp_path.clone(),
            source,
        })?;
        fs::rename(&temp_path, path).map_err(|source| {
            StorageError::ReplaceSnapshot { path: path.to_path_buf(), source }
        })?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    result
}

pub(crate) fn temp_snapshot_path(path: &Path) -> PathBuf {
    let file_name =
        path.file_name().and_then(|name| name.to_str()).unwrap_or("snapshot");
    path.with_file_name(format!("{file_name}.tmp"))
}

#[cfg(test)]
mod tests {
    use super::{load_snapshot, temp_snapshot_path, write_snapshot_atomic};
    use crate::StorageError;
    use std::collections::BTreeMap;
    use test_support::temporary_directory;

    #[test]
    fn snapshot_round_trips_entries() {
        let directory = temporary_directory("snapshot-round-trip");
        let path = directory.join("state.json");
        let mut state = BTreeMap::new();
        state.insert("a".to_owned(), "one".to_owned());
        state.insert("b".to_owned(), "two".to_owned());

        write_snapshot_atomic(&path, &state)
            .expect("snapshot writes should succeed");
        let loaded =
            load_snapshot::<String, String>(&path).expect("snapshot load");

        assert_eq!(loaded, state);
    }

    #[test]
    fn missing_snapshot_loads_as_empty_state() {
        let directory = temporary_directory("snapshot-missing");
        let path = directory.join("missing.json");

        let loaded =
            load_snapshot::<String, String>(&path).expect("missing snapshot");

        assert!(loaded.is_empty());
    }

    #[test]
    fn temp_snapshot_path_uses_tmp_suffix() {
        let path = std::path::Path::new("/tmp/example.json");

        assert_eq!(
            temp_snapshot_path(path).to_string_lossy(),
            "/tmp/example.json.tmp"
        );
    }

    #[test]
    fn failed_snapshot_write_preserves_previous_snapshot() {
        let directory = temporary_directory("snapshot-failure");
        let path = directory.join("state.json");
        let mut initial = BTreeMap::new();
        initial.insert("stable".to_owned(), "value".to_owned());
        write_snapshot_atomic(&path, &initial)
            .expect("initial snapshot should write");
        std::fs::create_dir_all(temp_snapshot_path(&path))
            .expect("temp path blocker should be creatable");

        let mut changed = initial.clone();
        changed.insert("new".to_owned(), "value".to_owned());
        let error = write_snapshot_atomic(&path, &changed)
            .expect_err("blocked temp path should fail");

        match error {
            StorageError::WriteSnapshot { .. } => {}
            other => panic!("unexpected error: {other}"),
        }

        let loaded =
            load_snapshot::<String, String>(&path).expect("previous snapshot");
        assert_eq!(loaded, initial);
    }
}
