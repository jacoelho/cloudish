use aws::{
    ContainerCommand, ContainerRuntime, InfrastructureError, RunningContainer,
};
use std::io;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LockResult, Mutex};

#[derive(Debug, Default)]
pub struct ProcessContainerRuntime {
    next_id: AtomicUsize,
}

impl ProcessContainerRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ContainerRuntime for ProcessContainerRuntime {
    fn start(
        &self,
        command: &ContainerCommand,
    ) -> Result<Box<dyn RunningContainer>, InfrastructureError> {
        let mut process = Command::new(command.program());
        for arg in command.args().iter().skip(1) {
            process.arg(arg);
        }
        process.stdin(Stdio::null());
        process.stdout(Stdio::null());
        process.stderr(Stdio::null());
        process.envs(command.environment());

        let child = process.spawn().map_err(|source| {
            InfrastructureError::container("start", command.name(), source)
        })?;
        let id = format!(
            "process-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        );

        Ok(Box::new(ProcessContainerHandle {
            child: Mutex::new(Some(child)),
            id,
            name: command.name().to_owned(),
        }))
    }
}

struct ProcessContainerHandle {
    child: Mutex<Option<Child>>,
    id: String,
    name: String,
}

impl RunningContainer for ProcessContainerHandle {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn stop(&self) -> Result<(), InfrastructureError> {
        let mut child_slot = recover(self.child.lock());
        let Some(child) = child_slot.as_mut() else {
            return Ok(());
        };

        if child
            .try_wait()
            .map_err(|source| {
                InfrastructureError::container("stop", &self.name, source)
            })?
            .is_none()
        {
            match child.kill() {
                Ok(()) => {}
                Err(source)
                    if source.kind() == io::ErrorKind::InvalidInput => {}
                Err(source) => {
                    return Err(InfrastructureError::container(
                        "stop", &self.name, source,
                    ));
                }
            }

            child.wait().map_err(|source| {
                InfrastructureError::container("stop", &self.name, source)
            })?;
        }

        child_slot.take();
        Ok(())
    }
}

impl Drop for ProcessContainerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::ProcessContainerRuntime;
    use aws::{ContainerCommand, ContainerRuntime, InfrastructureError};

    #[test]
    fn process_runtime_reports_spawn_failures() {
        let runtime = ProcessContainerRuntime::new();
        let error = match runtime
            .start(&ContainerCommand::new("lambda", "/definitely-missing"))
        {
            Ok(_) => panic!("missing executable should fail"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            InfrastructureError::Container { operation: "start", .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn process_runtime_starts_and_stops_a_process() {
        let runtime = ProcessContainerRuntime::new();
        let handle = runtime
            .start(
                &ContainerCommand::new("lambda", "/bin/sh")
                    .with_args(["-c", "sleep 5"]),
            )
            .expect("process should start");

        assert_eq!(handle.name(), "lambda");
        assert!(handle.id().starts_with("process-"));
        handle.stop().expect("process should stop");
    }
}
