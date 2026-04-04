#![cfg_attr(
    test,
    allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)
)]

pub mod choice_eval;
pub mod definitions;
pub mod errors;
pub mod executions;
pub mod history;
pub mod scope;
mod state;
pub mod tasks;

pub use definitions::{
    CreateStateMachineInput, CreateStateMachineOutput,
    DescribeStateMachineOutput, ListStateMachinesInput,
    ListStateMachinesOutput, StateMachineListItem, StateMachineStatus,
    StateMachineType,
};
pub use errors::StepFunctionsError;
pub use executions::{
    DescribeExecutionInput, DescribeExecutionOutput, ExecutionListItem,
    ExecutionStatus, ListExecutionsInput, ListExecutionsOutput,
    StartExecutionInput, StartExecutionOutput, StopExecutionInput,
    StopExecutionOutput,
};
pub use history::{
    ExecutionAbortedEventDetails, ExecutionFailedEventDetails,
    ExecutionStartedEventDetails, ExecutionSucceededEventDetails,
    GetExecutionHistoryInput, GetExecutionHistoryOutput, HistoryEvent,
    StateEnteredEventDetails, StateExitedEventDetails, TaskFailedEventDetails,
    TaskScheduledEventDetails, TaskSucceededEventDetails,
};
pub use scope::StepFunctionsScope;
pub use tasks::{
    StepFunctionsExecutionSpawner, StepFunctionsService,
    StepFunctionsServiceDependencies, StepFunctionsSleeper,
    StepFunctionsSpawnHandle, StepFunctionsTaskAdapter, TaskInvocationFailure,
    TaskInvocationRequest, TaskInvocationResult,
};
