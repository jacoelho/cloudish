use std::collections::BTreeMap;
use std::sync::mpsc;
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub trait ScheduleHandle: Send + Sync {
    fn cancel(&self);
}

pub trait Scheduler: Send + Sync {
    fn schedule_repeating(
        &self,
        task_name: &'static str,
        interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> Box<dyn ScheduleHandle>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadScheduler;

impl Scheduler for ThreadScheduler {
    fn schedule_repeating(
        &self,
        task_name: &'static str,
        interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> Box<dyn ScheduleHandle> {
        let interval = normalized_interval(interval);
        let (stop_tx, stop_rx) = mpsc::channel();
        let _ = task_name;
        let join = thread::spawn(move || {
            loop {
                match stop_rx.recv_timeout(interval) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => task(),
                }
            }
        });

        Box::new(ThreadScheduleHandle {
            join: Mutex::new(Some(join)),
            stop: Mutex::new(Some(stop_tx)),
        })
    }
}

struct ThreadScheduleHandle {
    join: Mutex<Option<JoinHandle<()>>>,
    stop: Mutex<Option<mpsc::Sender<()>>>,
}

impl ScheduleHandle for ThreadScheduleHandle {
    fn cancel(&self) {
        if let Some(stop) = recover(self.stop.lock()).take() {
            let _ = stop.send(());
        }

        if let Some(join) = recover(self.join.lock()).take() {
            let _ = join.join();
        }
    }
}

impl Drop for ThreadScheduleHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Clone, Default)]
pub struct ManualScheduler {
    inner: Arc<Mutex<ManualSchedulerState>>,
}

impl ManualScheduler {
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

impl Scheduler for ManualScheduler {
    fn schedule_repeating(
        &self,
        _task_name: &'static str,
        _interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> Box<dyn ScheduleHandle> {
        let id = {
            let mut state = recover(self.inner.lock());
            let id = state.next_id;
            state.next_id = state.next_id.saturating_add(1);
            state.tasks.insert(id, ManualTask { cancelled: false, task });
            id
        };

        Box::new(ManualScheduleHandle { id, inner: Arc::clone(&self.inner) })
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

struct ManualScheduleHandle {
    id: usize,
    inner: Arc<Mutex<ManualSchedulerState>>,
}

impl ScheduleHandle for ManualScheduleHandle {
    fn cancel(&self) {
        if let Some(task) = recover(self.inner.lock()).tasks.get_mut(&self.id)
        {
            task.cancelled = true;
        }
    }
}

impl Drop for ManualScheduleHandle {
    fn drop(&mut self) {
        self.cancel();
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
    use super::{ManualScheduler, Scheduler, ThreadScheduler};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn manual_scheduler_runs_pending_tasks() {
        let scheduler = ManualScheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let task_counter = Arc::clone(&counter);
        let handle = scheduler.schedule_repeating(
            "manual-task",
            Duration::from_secs(1),
            Arc::new(move || {
                task_counter.fetch_add(1, Ordering::Relaxed);
            }),
        );

        assert_eq!(scheduler.active_task_count(), 1);
        scheduler.run_pending();
        scheduler.run_pending();
        handle.cancel();
        scheduler.run_pending();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(scheduler.active_task_count(), 0);
    }

    #[test]
    fn thread_scheduler_treats_zero_interval_as_non_zero() {
        let scheduler = ThreadScheduler;
        let counter = Arc::new(AtomicUsize::new(0));
        let task_counter = Arc::clone(&counter);
        let handle = scheduler.schedule_repeating(
            "thread-task",
            Duration::ZERO,
            Arc::new(move || {
                task_counter.fetch_add(1, Ordering::Relaxed);
            }),
        );

        thread::sleep(Duration::from_millis(20));
        handle.cancel();

        assert!(counter.load(Ordering::Relaxed) > 0);
    }
}
