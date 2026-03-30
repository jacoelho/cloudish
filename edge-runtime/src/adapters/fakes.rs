use aws::{
    BackgroundScheduler, BlobKey, BlobMetadata, BlobStore, Clock,
    InfrastructureError, LogRecord, LogSink, PayloadId, PayloadStore,
    RandomSource, ScheduledTaskHandle, StoredPayload,
};
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LockResult, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Debug, Default)]
pub struct MemoryBlobStore {
    blobs: Mutex<BTreeMap<BlobKey, Vec<u8>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BlobStore for MemoryBlobStore {
    fn put(
        &self,
        key: &BlobKey,
        body: &[u8],
    ) -> Result<BlobMetadata, InfrastructureError> {
        recover(self.blobs.lock()).insert(key.clone(), body.to_vec());
        Ok(BlobMetadata::new(key.clone(), body.len()))
    }

    fn get(
        &self,
        key: &BlobKey,
    ) -> Result<Option<Vec<u8>>, InfrastructureError> {
        Ok(recover(self.blobs.lock()).get(key).cloned())
    }

    fn delete(&self, key: &BlobKey) -> Result<(), InfrastructureError> {
        recover(self.blobs.lock()).remove(key);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct MemoryPayloadStore {
    next_id: AtomicUsize,
    payloads: Mutex<BTreeMap<PayloadId, Vec<u8>>>,
}

impl MemoryPayloadStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PayloadStore for MemoryPayloadStore {
    fn persist(
        &self,
        _name_hint: &str,
        body: &[u8],
    ) -> Result<StoredPayload, InfrastructureError> {
        let id = PayloadId::new(format!(
            "payload-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        ));
        recover(self.payloads.lock()).insert(id.clone(), body.to_vec());
        Ok(StoredPayload::new(id, body.len()))
    }

    fn read(
        &self,
        id: &PayloadId,
    ) -> Result<Option<Vec<u8>>, InfrastructureError> {
        Ok(recover(self.payloads.lock()).get(id).cloned())
    }

    fn delete(&self, id: &PayloadId) -> Result<(), InfrastructureError> {
        recover(self.payloads.lock()).remove(id);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FixedClock {
    now: SystemTime,
}

impl FixedClock {
    pub fn new(now: SystemTime) -> Self {
        Self { now }
    }
}

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.now
    }
}

#[derive(Debug, Clone)]
pub struct SequenceRandomSource {
    cursor: Arc<Mutex<usize>>,
    pattern: Arc<Vec<u8>>,
}

impl SequenceRandomSource {
    pub fn new(pattern: Vec<u8>) -> Self {
        let pattern = if pattern.is_empty() { vec![0] } else { pattern };

        Self { cursor: Arc::new(Mutex::new(0)), pattern: Arc::new(pattern) }
    }
}

impl RandomSource for SequenceRandomSource {
    fn fill_bytes(&self, bytes: &mut [u8]) -> Result<(), InfrastructureError> {
        let mut cursor = recover(self.cursor.lock());
        let pattern_len = self.pattern.len();

        for byte in bytes {
            *byte =
                self.pattern.get(*cursor % pattern_len).copied().ok_or_else(
                    || {
                        InfrastructureError::random_source(
                            "sequence-random",
                            io::Error::other(
                                "sequence random pattern must not be empty",
                            ),
                        )
                    },
                )?;
            *cursor += 1;
        }

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ManualBackgroundScheduler {
    inner: Arc<Mutex<ManualSchedulerState>>,
}

impl ManualBackgroundScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn active_task_count(&self) -> usize {
        recover(self.inner.lock())
            .tasks
            .values()
            .filter(|task| !task.cancelled)
            .count()
    }

    pub fn run_pending(&self) {
        let tasks = {
            let state = recover(self.inner.lock());
            state
                .tasks
                .values()
                .filter(|task| !task.cancelled)
                .map(|task| Arc::clone(&task.task))
                .collect::<Vec<_>>()
        };

        for task in tasks {
            task();
        }
    }
}

impl BackgroundScheduler for ManualBackgroundScheduler {
    fn schedule_repeating(
        &self,
        _task_name: String,
        _interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Box<dyn ScheduledTaskHandle>, InfrastructureError> {
        let id = {
            let mut state = recover(self.inner.lock());
            let id = state.next_id;
            state.next_id += 1;
            state.tasks.insert(id, ManualTask { cancelled: false, task });
            id
        };

        Ok(Box::new(ManualTaskHandle { id, inner: Arc::clone(&self.inner) }))
    }
}

#[derive(Default)]
struct ManualSchedulerState {
    next_id: usize,
    tasks: BTreeMap<usize, ManualTask>,
}

struct ManualTask {
    cancelled: bool,
    task: Arc<dyn Fn() + Send + Sync>,
}

struct ManualTaskHandle {
    id: usize,
    inner: Arc<Mutex<ManualSchedulerState>>,
}

impl ScheduledTaskHandle for ManualTaskHandle {
    fn cancel(&self) -> Result<(), InfrastructureError> {
        if let Some(task) = recover(self.inner.lock()).tasks.get_mut(&self.id)
        {
            task.cancelled = true;
        }

        Ok(())
    }
}

impl Drop for ManualTaskHandle {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

#[derive(Debug, Default)]
pub struct RecordingLogSink {
    entries: Mutex<Vec<LogRecord>>,
}

impl RecordingLogSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entries(&self) -> Vec<LogRecord> {
        recover(self.entries.lock()).clone()
    }
}

impl LogSink for RecordingLogSink {
    fn emit(&self, record: &LogRecord) -> Result<(), InfrastructureError> {
        recover(self.entries.lock()).push(record.clone());
        Ok(())
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{
        FixedClock, ManualBackgroundScheduler, MemoryBlobStore,
        MemoryPayloadStore, RecordingLogSink, SequenceRandomSource,
    };
    use aws::{
        BackgroundScheduler, BlobKey, BlobStore, Clock, LogRecord, LogSink,
        PayloadStore, RandomSource,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn memory_blob_store_round_trips() {
        let store = MemoryBlobStore::new();
        let key = BlobKey::new("s3", "bucket/key");

        store.put(&key, b"hello").expect("write should succeed");

        assert_eq!(
            store.get(&key).expect("read should succeed"),
            Some(b"hello".to_vec())
        );

        store.delete(&key).expect("delete should succeed");
        assert_eq!(store.get(&key).expect("lookup should succeed"), None);
    }

    #[test]
    fn memory_payload_store_generates_ids() {
        let store = MemoryPayloadStore::new();
        let first = store
            .persist("lambda", b"first")
            .expect("first payload should persist");
        let second = store
            .persist("lambda", b"second")
            .expect("second payload should persist");

        assert_ne!(first.id(), second.id());
        assert_eq!(
            store.read(second.id()).expect("read should succeed"),
            Some(b"second".to_vec())
        );
    }

    #[test]
    fn fixed_clock_returns_the_same_instant() {
        let clock = FixedClock::new(UNIX_EPOCH + Duration::from_secs(5));
        assert_eq!(clock.now(), UNIX_EPOCH + Duration::from_secs(5));
    }

    #[test]
    fn sequence_random_source_repeats_its_pattern() {
        let source = SequenceRandomSource::new(vec![1, 2, 3]);
        let mut buffer = [0_u8; 5];
        source.fill_bytes(&mut buffer).expect("random fill should succeed");

        assert_eq!(buffer, [1, 2, 3, 1, 2]);
    }

    #[test]
    fn manual_scheduler_runs_pending_tasks() {
        let scheduler = ManualBackgroundScheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let task_counter = Arc::clone(&counter);
        let handle = scheduler
            .schedule_repeating(
                "manual".to_owned(),
                Duration::from_secs(1),
                Arc::new(move || {
                    task_counter.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .expect("task should schedule");

        scheduler.run_pending();
        scheduler.run_pending();
        handle.cancel().expect("task should cancel");
        scheduler.run_pending();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(scheduler.active_task_count(), 0);
    }

    #[test]
    fn recording_log_sink_collects_entries() {
        let sink = RecordingLogSink::new();
        let entry = LogRecord::new(
            "lambda",
            "invoked",
            UNIX_EPOCH + Duration::from_secs(9),
        );

        sink.emit(&entry).expect("log append should succeed");

        assert_eq!(sink.entries(), vec![entry]);
    }
}
