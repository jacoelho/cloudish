use aws::{
    BackgroundScheduler, Clock, InfrastructureError, RandomSource,
    ScheduledTaskHandle,
};
use getrandom::getrandom;
use std::io;
use std::sync::mpsc;
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};
#[cfg(feature = "step-functions")]
use step_functions::{StepFunctionsExecutionSpawner, StepFunctionsSleeper};

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

    #[cfg(test)]
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
}

impl Drop for ManagedBackgroundTasks {
    fn drop(&mut self) {
        for handle in &self.handles {
            let _ = handle.cancel();
        }
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
    fn spawn(
        &self,
        task_name: &str,
        task: Box<dyn FnOnce() + Send>,
    ) -> Result<(), InfrastructureError> {
        thread::Builder::new()
            .name(task_name.to_owned())
            .spawn(task)
            .map(|_| ())
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
        ThreadBackgroundScheduler,
    };
    use aws::{
        BackgroundScheduler, Clock, InfrastructureError, RandomSource,
        ScheduledTaskHandle,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
}
