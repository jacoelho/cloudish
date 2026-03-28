use aws::{Clock, InfrastructureError};
use std::sync::Arc;
use std::time::Duration;

use crate::scope::StepFunctionsScope;

pub use crate::state::{
    StepFunctionsService, TaskInvocationFailure, TaskInvocationRequest,
    TaskInvocationResult,
};

pub trait StepFunctionsExecutionSpawner: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the underlying runtime cannot schedule the task.
    fn spawn(
        &self,
        task_name: &str,
        task: Box<dyn FnOnce() + Send>,
    ) -> Result<(), InfrastructureError>;
}

pub trait StepFunctionsSleeper: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the underlying runtime cannot wait for the
    /// requested duration.
    fn sleep(&self, duration: Duration) -> Result<(), InfrastructureError>;
}

pub trait StepFunctionsTaskAdapter: Send + Sync {
    /// # Errors
    ///
    /// Returns an error when the task adapter cannot invoke the requested
    /// downstream resource.
    fn invoke(
        &self,
        scope: &StepFunctionsScope,
        request: &TaskInvocationRequest,
    ) -> Result<TaskInvocationResult, TaskInvocationFailure>;
}

#[derive(Clone)]
pub struct StepFunctionsServiceDependencies {
    pub clock: Arc<dyn Clock>,
    pub execution_spawner: Arc<dyn StepFunctionsExecutionSpawner>,
    pub sleeper: Arc<dyn StepFunctionsSleeper>,
    pub task_adapter: Arc<dyn StepFunctionsTaskAdapter>,
}
