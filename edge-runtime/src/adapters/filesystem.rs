use aws::{BlobKey, BlobMetadata, BlobStore, InfrastructureError};
#[cfg(test)]
use aws::{LogRecord, LogSink, PayloadId, PayloadStore, StoredPayload};
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DirectoryBlobStore {
    root: PathBuf,
    temp_ids: AtomicUsize,
}

impl DirectoryBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into(), temp_ids: AtomicUsize::new(0) }
    }

    fn blob_path(&self, key: &BlobKey) -> PathBuf {
        self.root
            .join(encode_component(key.namespace()))
            .join(format!("{}.blob", encode_component(key.name())))
    }
}

impl BlobStore for DirectoryBlobStore {
    fn put(
        &self,
        key: &BlobKey,
        body: &[u8],
    ) -> Result<BlobMetadata, InfrastructureError> {
        let path = self.blob_path(key);
        write_atomically(&path, body, &self.temp_ids).map_err(|source| {
            InfrastructureError::blob_store("put", key, source)
        })?;

        Ok(BlobMetadata::new(key.clone(), body.len()))
    }

    fn get(
        &self,
        key: &BlobKey,
    ) -> Result<Option<Vec<u8>>, InfrastructureError> {
        let path = self.blob_path(key);

        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(source) => {
                Err(InfrastructureError::blob_store("get", key, source))
            }
        }
    }

    fn delete(&self, key: &BlobKey) -> Result<(), InfrastructureError> {
        let path = self.blob_path(key);

        match fs::remove_file(&path) {
            Ok(()) => {
                remove_empty_parent(path.parent());
                Ok(())
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => {
                Err(InfrastructureError::blob_store("delete", key, source))
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct FilesystemPayloadStore {
    next_payload_id: AtomicUsize,
    root: PathBuf,
    temp_ids: AtomicUsize,
}

#[cfg(test)]
impl FilesystemPayloadStore {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            next_payload_id: AtomicUsize::new(0),
            root: root.into(),
            temp_ids: AtomicUsize::new(0),
        }
    }

    fn next_id(&self, name_hint: &str) -> PayloadId {
        let id = self.next_payload_id.fetch_add(1, Ordering::Relaxed);
        let name = if name_hint.trim().is_empty() {
            "payload".to_owned()
        } else {
            encode_component(name_hint)
        };

        PayloadId::new(format!("{name}-{id}"))
    }

    fn payload_path(&self, id: &PayloadId) -> PathBuf {
        self.root.join(format!("{}.bin", encode_component(id.as_str())))
    }
}

#[cfg(test)]
impl PayloadStore for FilesystemPayloadStore {
    fn persist(
        &self,
        name_hint: &str,
        body: &[u8],
    ) -> Result<StoredPayload, InfrastructureError> {
        let id = self.next_id(name_hint);
        let path = self.payload_path(&id);
        write_atomically(&path, body, &self.temp_ids).map_err(|source| {
            InfrastructureError::payload_store(
                "persist",
                id.to_string(),
                source,
            )
        })?;

        Ok(StoredPayload::new(id, body.len()))
    }

    fn read(
        &self,
        id: &PayloadId,
    ) -> Result<Option<Vec<u8>>, InfrastructureError> {
        let path = self.payload_path(id);

        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(source) => Err(InfrastructureError::payload_store(
                "read",
                id.to_string(),
                source,
            )),
        }
    }

    fn delete(&self, id: &PayloadId) -> Result<(), InfrastructureError> {
        let path = self.payload_path(id);

        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(InfrastructureError::payload_store(
                "delete",
                id.to_string(),
                source,
            )),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct FileLogSink {
    path: PathBuf,
}

#[cfg(test)]
impl FileLogSink {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[cfg(test)]
impl LogSink for FileLogSink {
    fn emit(&self, record: &LogRecord) -> Result<(), InfrastructureError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| {
                InfrastructureError::log_sink(
                    self.path.display().to_string(),
                    source,
                )
            })?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| {
                InfrastructureError::log_sink(
                    self.path.display().to_string(),
                    source,
                )
            })?;

        writeln!(
            file,
            "{}\t{}\t{}",
            format_timestamp(record.emitted_at()),
            record.stream(),
            escaped_message(record.message()),
        )
        .map_err(|source| {
            InfrastructureError::log_sink(
                self.path.display().to_string(),
                source,
            )
        })
    }
}

fn encode_component(component: &str) -> String {
    let mut encoded =
        String::with_capacity(component.len().saturating_mul(2));
    for byte in component.bytes() {
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
    }
    encoded
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0'.saturating_add(value)),
        10..=15 => {
            char::from(b'a'.saturating_add(value.saturating_sub(10)))
        }
        _ => '0',
    }
}

#[cfg(test)]
fn escaped_message(message: &str) -> String {
    message.replace('\n', "\\n").replace('\t', "\\t")
}

#[cfg(test)]
fn format_timestamp(timestamp: SystemTime) -> String {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            format!("{}.{:09}", duration.as_secs(), duration.subsec_nanos())
        }
        Err(_) => "0.000000000".to_owned(),
    }
}

fn temporary_path(final_path: &Path, suffix: usize) -> PathBuf {
    let file_name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("payload");
    let parent = final_path.parent().unwrap_or_else(|| Path::new("."));

    parent.join(format!(".{file_name}.tmp-{suffix}"))
}

fn write_atomically(
    final_path: &Path,
    body: &[u8],
    temp_ids: &AtomicUsize,
) -> io::Result<()> {
    let parent = final_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no parent directory", final_path.display()),
        )
    })?;
    fs::create_dir_all(parent)?;

    let temp_path =
        temporary_path(final_path, temp_ids.fetch_add(1, Ordering::Relaxed));
    let result = (|| -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(body)?;
        file.sync_all()?;
        fs::rename(&temp_path, final_path)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    result
}

fn remove_empty_parent(path: Option<&Path>) {
    let Some(path) = path else {
        return;
    };

    match fs::remove_dir(path) {
        Ok(()) => {}
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::DirectoryNotEmpty | io::ErrorKind::NotFound
            ) => {}
        Err(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DirectoryBlobStore, FileLogSink, FilesystemPayloadStore,
        format_timestamp,
    };
    use aws::{
        BlobKey, BlobStore, InfrastructureError, LogRecord, LogSink,
        PayloadStore,
    };
    use std::time::{Duration, UNIX_EPOCH};
    use test_support::temporary_directory;

    #[test]
    fn blob_store_round_trips_payloads() {
        let root = temporary_directory("hosting-blob");
        let store = DirectoryBlobStore::new(root.join("blobs"));
        let key = BlobKey::new("s3", "bucket/object.txt");

        let metadata = store
            .put(&key, b"blob-payload")
            .expect("blob write should succeed");

        assert_eq!(metadata.key(), &key);
        assert_eq!(metadata.byte_len(), 12);
        assert_eq!(
            store.get(&key).expect("blob read should succeed"),
            Some(b"blob-payload".to_vec())
        );

        store.delete(&key).expect("blob delete should succeed");

        assert_eq!(store.get(&key).expect("blob lookup should succeed"), None);
    }

    #[test]
    fn blob_store_delete_removes_empty_namespace_directories() {
        let root = temporary_directory("hosting-blob-cleanup");
        let blob_root = root.join("blobs");
        let store = DirectoryBlobStore::new(&blob_root);
        let key = BlobKey::new("s3", "bucket/object.txt");

        store.put(&key, b"payload").expect("blob write should succeed");
        store.delete(&key).expect("blob delete should succeed");

        let namespace = blob_root.join("7333");
        assert!(
            !namespace.exists(),
            "expected the empty namespace directory to be removed"
        );
    }

    #[test]
    fn blob_store_surfaces_filesystem_failures() {
        let root = temporary_directory("hosting-blob-error");
        let blocking = root.join("blocking");
        std::fs::write(&blocking, "blocking file")
            .expect("blocking file should be writable");
        let store = DirectoryBlobStore::new(blocking);
        let key = BlobKey::new("s3", "denied");

        let error = store
            .put(&key, b"payload")
            .expect_err("filesystem failure should surface");

        assert!(matches!(
            error,
            InfrastructureError::BlobStore { operation: "put", .. }
        ));
    }

    #[test]
    fn payload_store_round_trips_generated_ids() {
        let root = temporary_directory("hosting-payload");
        let store = FilesystemPayloadStore::new(root.join("payloads"));

        let stored = store
            .persist("lambda-code", b"zip-bytes")
            .expect("payload write should succeed");

        assert_eq!(stored.byte_len(), 9);
        assert!(stored.id().as_str().starts_with("6c616d6264612d636f6465-"));
        assert_eq!(
            store.read(stored.id()).expect("payload read should succeed"),
            Some(b"zip-bytes".to_vec())
        );

        store.delete(stored.id()).expect("payload delete should succeed");

        assert_eq!(
            store.read(stored.id()).expect("payload lookup should succeed"),
            None
        );
    }

    #[test]
    fn payload_store_surfaces_filesystem_failures() {
        let root = temporary_directory("hosting-payload-error");
        let blocking = root.join("blocking");
        std::fs::write(&blocking, "blocking file")
            .expect("blocking file should be writable");
        let store = FilesystemPayloadStore::new(blocking);

        let error = store
            .persist("lambda", b"payload")
            .expect_err("filesystem failure should surface");

        assert!(matches!(
            error,
            InfrastructureError::PayloadStore { operation: "persist", .. }
        ));
    }

    #[test]
    fn file_log_sink_appends_escaped_records() {
        let root = temporary_directory("hosting-log");
        let path = root.join("logs").join("runtime.log");
        let sink = FileLogSink::new(&path);
        let emitted_at = UNIX_EPOCH + Duration::from_secs(42);

        sink.emit(&LogRecord::new("lambda", "line 1\nline 2", emitted_at))
            .expect("log write should succeed");
        sink.emit(&LogRecord::new("lambda", "done", emitted_at))
            .expect("second log write should succeed");

        let contents = std::fs::read_to_string(path)
            .expect("log file should be readable");
        assert!(contents.contains("42.000000000\tlambda\tline 1\\nline 2"));
        assert!(contents.contains("42.000000000\tlambda\tdone"));
    }

    #[test]
    fn timestamps_before_epoch_format_as_zero() {
        assert_eq!(
            format_timestamp(UNIX_EPOCH - Duration::from_secs(1)),
            "0.000000000"
        );
    }
}
