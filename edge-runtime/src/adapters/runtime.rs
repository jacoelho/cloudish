use aws::{
    BackgroundScheduler, Clock, InfrastructureError, RandomSource,
    ScheduledTaskHandle,
};
use getrandom::getrandom;
use std::io;
#[cfg(any(feature = "eventbridge", test))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};
#[cfg(feature = "step-functions")]
use step_functions::{
    StepFunctionsExecutionSpawner, StepFunctionsSleeper,
    StepFunctionsSpawnHandle,
};

pub struct ManagedBackgroundTasks {
    handles: Vec<Box<dyn ScheduledTaskHandle>>,
}

impl ManagedBackgroundTasks {
    pub fn new(handles: Vec<Box<dyn ScheduledTaskHandle>>) -> Self {
        Self { handles }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Cancels every registered background task handle owned by this set.
    ///
    /// # Errors
    ///
    /// Returns the first infrastructure error reported by a task handle while
    /// cancelling it.
    pub fn cancel(&self) -> Result<(), InfrastructureError> {
        for handle in &self.handles {
            handle.cancel()?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn task_count(&self) -> usize {
        self.handles.len()
    }
}

impl Drop for ManagedBackgroundTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            let _ = handle.cancel();
        }
    }
}

#[cfg(any(feature = "eventbridge", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadWorkQueueShutdownOutcome {
    Drained,
    TimedOut,
}

#[cfg(any(feature = "eventbridge", test))]
#[derive(Debug, Default)]
pub(crate) struct ThreadWorkQueueStopToken {
    stop_requested: AtomicBool,
}

#[cfg(any(feature = "eventbridge", test))]
impl ThreadWorkQueueStopToken {
    pub(crate) fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::SeqCst)
    }

    pub(crate) fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::SeqCst);
    }
}

#[cfg(any(feature = "eventbridge", test))]
pub(crate) struct ThreadWorkQueue<T: Send + 'static> {
    completion: Mutex<mpsc::Receiver<()>>,
    completed: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    sender: Mutex<Option<mpsc::Sender<T>>>,
    stop_token: Arc<ThreadWorkQueueStopToken>,
    task_name: String,
}

#[cfg(any(feature = "eventbridge", test))]
impl<T: Send + 'static> ThreadWorkQueue<T> {
    pub(crate) fn spawn(
        task_name: String,
        handler: impl Fn(T, &ThreadWorkQueueStopToken) + Send + 'static,
    ) -> Result<Self, InfrastructureError> {
        let (completion_tx, completion_rx) = mpsc::channel();
        let (sender, receiver) = mpsc::channel();
        let stop_token = Arc::new(ThreadWorkQueueStopToken::default());
        let worker_stop_token = Arc::clone(&stop_token);
        let join = thread::Builder::new()
            .name(task_name.clone())
            .spawn(move || {
                while let Ok(item) = receiver.recv() {
                    if worker_stop_token.is_stop_requested() {
                        break;
                    }
                    handler(item, worker_stop_token.as_ref());
                }
                let _ = completion_tx.send(());
            })
            .map_err(|source| {
                InfrastructureError::scheduler(
                    "spawn",
                    task_name.clone(),
                    source,
                )
            })?;

        Ok(Self {
            completion: Mutex::new(completion_rx),
            completed: AtomicBool::new(false),
            join: Mutex::new(Some(join)),
            sender: Mutex::new(Some(sender)),
            stop_token,
            task_name,
        })
    }

    pub(crate) fn enqueue(&self, item: T) -> Result<(), InfrastructureError> {
        let sender = recover(self.sender.lock());
        let Some(sender) = sender.as_ref() else {
            return Err(InfrastructureError::scheduler(
                "enqueue",
                self.task_name.clone(),
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "worker queue is shut down",
                ),
            ));
        };
        sender.send(item).map_err(|_| {
            InfrastructureError::scheduler(
                "enqueue",
                self.task_name.clone(),
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "worker queue receiver is unavailable",
                ),
            )
        })
    }

    pub(crate) fn begin_shutdown(&self) {
        let _ = recover(self.sender.lock()).take();
    }

    pub(crate) fn request_hard_stop(&self) {
        self.begin_shutdown();
        self.stop_token.request_stop();
    }

    pub(crate) fn finish_shutdown(
        &self,
        timeout: Option<Duration>,
    ) -> Result<ThreadWorkQueueShutdownOutcome, InfrastructureError> {
        match self.wait_for_completion(timeout)? {
            ThreadWorkQueueShutdownOutcome::Drained => {
                self.join_worker()?;
                Ok(ThreadWorkQueueShutdownOutcome::Drained)
            }
            ThreadWorkQueueShutdownOutcome::TimedOut => {
                Ok(ThreadWorkQueueShutdownOutcome::TimedOut)
            }
        }
    }

    fn wait_for_completion(
        &self,
        timeout: Option<Duration>,
    ) -> Result<ThreadWorkQueueShutdownOutcome, InfrastructureError> {
        if self.completed.load(Ordering::SeqCst) {
            return Ok(ThreadWorkQueueShutdownOutcome::Drained);
        }

        let completion = recover(self.completion.lock());
        let completed = match timeout {
            Some(timeout) => match completion.recv_timeout(timeout) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => true,
                Err(mpsc::RecvTimeoutError::Timeout) => false,
            },
            None => {
                let _ = completion.recv();
                true
            }
        };

        if completed {
            self.completed.store(true, Ordering::SeqCst);
            Ok(ThreadWorkQueueShutdownOutcome::Drained)
        } else {
            Ok(ThreadWorkQueueShutdownOutcome::TimedOut)
        }
    }

    fn join_worker(&self) -> Result<(), InfrastructureError> {
        if let Some(join) = recover(self.join.lock()).take() {
            join.join().map_err(|_| {
                InfrastructureError::scheduler(
                    "shutdown",
                    self.task_name.clone(),
                    io::Error::other("worker queue panicked"),
                )
            })?;
        }

        Ok(())
    }
}

#[cfg(any(feature = "eventbridge", test))]
impl<T: Send + 'static> Drop for ThreadWorkQueue<T> {
    fn drop(&mut self) {
        self.begin_shutdown();
        self.stop_token.request_stop();
        if matches!(
            self.finish_shutdown(Some(Duration::ZERO)),
            Ok(ThreadWorkQueueShutdownOutcome::Drained)
        ) {
            return;
        }
        let _ = recover(self.join.lock()).take();
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct OsRandomSource;

impl RandomSource for OsRandomSource {
    fn fill_bytes(&self, bytes: &mut [u8]) -> Result<(), InfrastructureError> {
        getrandom(bytes).map_err(|source| {
            InfrastructureError::random_source(
                "os-random",
                io::Error::other(source.to_string()),
            )
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadBackgroundScheduler;

impl ThreadBackgroundScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl BackgroundScheduler for ThreadBackgroundScheduler {
    fn schedule_repeating(
        &self,
        task_name: String,
        interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Box<dyn ScheduledTaskHandle>, InfrastructureError> {
        let interval = normalized_interval(interval);
        let (stop_tx, stop_rx) = mpsc::channel();
        let join = thread::Builder::new()
            .name(task_name.clone())
            .spawn(move || {
                loop {
                    match stop_rx.recv_timeout(interval) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => task(),
                    }
                }
            })
            .map_err(|source| {
                InfrastructureError::scheduler("schedule", task_name, source)
            })?;

        Ok(Box::new(ThreadScheduledTask {
            join: Mutex::new(Some(join)),
            stop: Mutex::new(Some(stop_tx)),
        }))
    }
}

struct ThreadScheduledTask {
    join: Mutex<Option<JoinHandle<()>>>,
    stop: Mutex<Option<mpsc::Sender<()>>>,
}

impl ScheduledTaskHandle for ThreadScheduledTask {
    fn cancel(&self) -> Result<(), InfrastructureError> {
        if let Some(stop) = recover(self.stop.lock()).take() {
            let _ = stop.send(());
        }

        if let Some(join) = recover(self.join.lock()).take() {
            join.join().map_err(|_| {
                InfrastructureError::scheduler(
                    "cancel",
                    "thread-scheduler",
                    io::Error::other("scheduler worker panicked"),
                )
            })?;
        }

        Ok(())
    }
}

impl Drop for ThreadScheduledTask {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

#[cfg(feature = "step-functions")]
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadStepFunctionsExecutionSpawner;

#[cfg(feature = "step-functions")]
impl StepFunctionsExecutionSpawner for ThreadStepFunctionsExecutionSpawner {
    fn spawn_paused(
        &self,
        task_name: &str,
        task: Box<dyn FnOnce() + Send>,
    ) -> Result<Box<dyn StepFunctionsSpawnHandle>, InfrastructureError> {
        let (start_tx, start_rx) = mpsc::channel();
        thread::Builder::new()
            .name(task_name.to_owned())
            .spawn(move || {
                if matches!(start_rx.recv(), Ok(true)) {
                    task();
                }
            })
            .map(|_| {
                Box::new(ThreadStepFunctionsSpawnHandle {
                    start_tx: Some(start_tx),
                }) as Box<dyn StepFunctionsSpawnHandle>
            })
            .map_err(|source| {
                InfrastructureError::scheduler(
                    "spawn",
                    task_name.to_owned(),
                    source,
                )
            })
    }
}

#[cfg(feature = "step-functions")]
#[derive(Debug)]
struct ThreadStepFunctionsSpawnHandle {
    start_tx: Option<mpsc::Sender<bool>>,
}

#[cfg(feature = "step-functions")]
impl StepFunctionsSpawnHandle for ThreadStepFunctionsSpawnHandle {
    fn start(mut self: Box<Self>) {
        if let Some(start_tx) = self.start_tx.take() {
            let _ = start_tx.send(true);
        }
    }
}

#[cfg(feature = "step-functions")]
impl Drop for ThreadStepFunctionsSpawnHandle {
    fn drop(&mut self) {
        if let Some(start_tx) = self.start_tx.take() {
            let _ = start_tx.send(false);
        }
    }
}

#[cfg(feature = "step-functions")]
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadStepFunctionsSleeper;

#[cfg(feature = "step-functions")]
impl StepFunctionsSleeper for ThreadStepFunctionsSleeper {
    fn sleep(&self, duration: Duration) -> Result<(), InfrastructureError> {
        thread::sleep(duration);
        Ok(())
    }
}

fn normalized_interval(interval: Duration) -> Duration {
    if interval.is_zero() { Duration::from_millis(1) } else { interval }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{
        ManagedBackgroundTasks, OsRandomSource, SystemClock,
        ThreadBackgroundScheduler, ThreadWorkQueue,
        ThreadWorkQueueShutdownOutcome, recover,
    };
    use aws::{
        BackgroundScheduler, Clock, InfrastructureError, RandomSource,
        ScheduledTaskHandle,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn system_clock_tracks_current_time() {
        let before = std::time::SystemTime::now();
        let observed = SystemClock.now();
        let after = std::time::SystemTime::now();

        assert!(observed >= before);
        assert!(observed <= after);
    }

    #[test]
    fn os_random_source_fills_requested_bytes() {
        let mut bytes = [0_u8; 32];
        OsRandomSource
            .fill_bytes(&mut bytes)
            .expect("random bytes should be available");

        assert!(bytes.iter().any(|byte| *byte != 0));
    }

    #[test]
    fn thread_scheduler_runs_tasks() {
        let scheduler = ThreadBackgroundScheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let task_counter = Arc::clone(&counter);
        let handle = scheduler
            .schedule_repeating(
                "hosting-scheduler".to_owned(),
                Duration::from_millis(5),
                Arc::new(move || {
                    task_counter.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .expect("task should schedule");

        thread::sleep(Duration::from_millis(25));
        handle.cancel().expect("task should cancel");

        assert!(counter.load(Ordering::Relaxed) > 0);
    }

    struct RecordingHandle {
        cancelled: Arc<AtomicUsize>,
    }

    impl ScheduledTaskHandle for RecordingHandle {
        fn cancel(&self) -> Result<(), InfrastructureError> {
            self.cancelled.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn managed_background_tasks_cancel_all_handles() {
        let cancelled = Arc::new(AtomicUsize::new(0));
        let tasks = ManagedBackgroundTasks::new(vec![
            Box::new(RecordingHandle { cancelled: Arc::clone(&cancelled) }),
            Box::new(RecordingHandle { cancelled: Arc::clone(&cancelled) }),
        ]);

        tasks.cancel().expect("all handles should cancel");

        assert_eq!(cancelled.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn thread_work_queue_shutdown_drains_pending_items() {
        let processed = Arc::new(Mutex::new(Vec::new()));
        let processed_items = Arc::clone(&processed);
        let queue = ThreadWorkQueue::spawn(
            "thread-work-queue-drain".to_owned(),
            move |item, _stop_token| {
                recover(processed_items.lock()).push(item)
            },
        )
        .expect("work queue should spawn");

        queue.enqueue(1).expect("first item should enqueue");
        queue.enqueue(2).expect("second item should enqueue");
        queue.begin_shutdown();
        assert_eq!(
            queue
                .finish_shutdown(None)
                .expect("queue should shut down cleanly"),
            ThreadWorkQueueShutdownOutcome::Drained
        );

        assert_eq!(*recover(processed.lock()), vec![1, 2]);
    }

    #[test]
    fn thread_work_queue_rejects_enqueue_after_shutdown() {
        let queue = ThreadWorkQueue::spawn(
            "thread-work-queue-shutdown".to_owned(),
            |_, _stop_token| {},
        )
        .expect("work queue should spawn");

        queue.begin_shutdown();
        assert!(queue.enqueue(1).is_err());
        assert_eq!(
            queue
                .finish_shutdown(None)
                .expect("queue should shut down cleanly"),
            ThreadWorkQueueShutdownOutcome::Drained
        );
    }

    #[test]
    fn thread_work_queue_enqueue_does_not_wait_for_a_busy_worker() {
        let processed = Arc::new(Mutex::new(Vec::new()));
        let processed_items = Arc::clone(&processed);
        let worker_started = Arc::new(AtomicBool::new(false));
        let worker_started_for_handler = Arc::clone(&worker_started);
        let (release_tx, release_rx) = mpsc::channel();
        let queue = ThreadWorkQueue::spawn(
            "thread-work-queue-busy-worker".to_owned(),
            move |item, _stop_token| {
                if !worker_started_for_handler.swap(true, Ordering::SeqCst) {
                    release_rx
                        .recv()
                        .expect("first item should wait for release");
                }
                recover(processed_items.lock()).push(item);
            },
        )
        .expect("work queue should spawn");

        queue.enqueue(1).expect("first item should enqueue");
        while !worker_started.load(Ordering::SeqCst) {
            thread::yield_now();
        }
        for item in 2..=128 {
            queue.enqueue(item).expect("busy worker should not block enqueue");
        }
        release_tx.send(()).expect("first item should release cleanly");
        queue.begin_shutdown();
        assert_eq!(
            queue
                .finish_shutdown(None)
                .expect("queue should shut down cleanly"),
            ThreadWorkQueueShutdownOutcome::Drained
        );

        let processed = recover(processed.lock());
        assert_eq!(processed.len(), 128);
        assert_eq!(processed[0], 1);
        assert_eq!(processed[127], 128);
    }

    #[test]
    fn thread_work_queue_shutdown_with_timeout_returns_promptly_for_busy_worker()
     {
        let worker_started = Arc::new(AtomicBool::new(false));
        let worker_started_for_handler = Arc::clone(&worker_started);
        let (release_tx, release_rx) = mpsc::channel();
        let queue = ThreadWorkQueue::spawn(
            "thread-work-queue-timeout".to_owned(),
            move |_, _stop_token| {
                worker_started_for_handler.store(true, Ordering::SeqCst);
                release_rx
                    .recv()
                    .expect("worker should wait for explicit release");
            },
        )
        .expect("work queue should spawn");

        queue.enqueue(1).expect("item should enqueue");
        while !worker_started.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        let started_at = std::time::Instant::now();
        queue.begin_shutdown();
        let outcome = queue
            .finish_shutdown(Some(Duration::from_millis(20)))
            .expect("shutdown should not fail");

        assert_eq!(outcome, ThreadWorkQueueShutdownOutcome::TimedOut);
        assert!(started_at.elapsed() < Duration::from_millis(200));

        release_tx.send(()).expect("worker should release cleanly");
    }

    #[test]
    fn thread_work_queue_hard_stop_stops_processing_between_batch_items() {
        let processed = Arc::new(Mutex::new(Vec::new()));
        let processed_items = Arc::clone(&processed);
        let worker_started = Arc::new(AtomicBool::new(false));
        let worker_started_for_handler = Arc::clone(&worker_started);
        let (release_tx, release_rx) = mpsc::channel();
        let queue = ThreadWorkQueue::spawn(
            "thread-work-queue-hard-stop".to_owned(),
            move |items: Vec<u32>, stop_token| {
                for item in items {
                    recover(processed_items.lock()).push(item);
                    if item == 1 {
                        worker_started_for_handler
                            .store(true, Ordering::SeqCst);
                        release_rx
                            .recv()
                            .expect("first item should wait for release");
                    }
                    if stop_token.is_stop_requested() {
                        break;
                    }
                }
            },
        )
        .expect("work queue should spawn");

        queue.enqueue(vec![1, 2]).expect("batch should enqueue");
        while !worker_started.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        queue.begin_shutdown();
        assert_eq!(
            queue
                .finish_shutdown(Some(Duration::from_millis(20)))
                .expect("grace drain should not fail"),
            ThreadWorkQueueShutdownOutcome::TimedOut
        );

        queue.request_hard_stop();
        release_tx.send(()).expect("first item should release cleanly");
        assert_eq!(
            queue
                .finish_shutdown(Some(Duration::from_millis(200)))
                .expect("hard stop should finish"),
            ThreadWorkQueueShutdownOutcome::Drained
        );

        assert_eq!(*recover(processed.lock()), vec![1]);
    }
}
