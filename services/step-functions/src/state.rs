use crate::{
    choice_eval::{ChoiceRule, evaluate_choice_rules, parse_choice_rule},
    definitions::{
        CreateStateMachineInput, CreateStateMachineOutput,
        DescribeStateMachineOutput, ListStateMachinesInput,
        ListStateMachinesOutput, StateMachineListItem, StateMachineStatus,
        StateMachineType,
    },
    errors::StepFunctionsError,
    executions::{
        DescribeExecutionInput, DescribeExecutionOutput, ExecutionListItem,
        ExecutionStatus, ListExecutionsInput, ListExecutionsOutput,
        StartExecutionInput, StartExecutionOutput, StopExecutionInput,
        StopExecutionOutput,
    },
    history::{
        ExecutionAbortedEventDetails, ExecutionFailedEventDetails,
        ExecutionStartedEventDetails, ExecutionSucceededEventDetails,
        GetExecutionHistoryInput, GetExecutionHistoryOutput, HistoryEvent,
        StateEnteredEventDetails, StateExitedEventDetails,
        TaskFailedEventDetails, TaskScheduledEventDetails,
        TaskSucceededEventDetails,
    },
    scope::StepFunctionsScope,
    tasks::{
        StepFunctionsExecutionSpawner, StepFunctionsServiceDependencies,
        StepFunctionsSleeper, StepFunctionsTaskAdapter,
    },
};
use aws::{
    AccountId, Arn, ArnResource, Clock, InfrastructureError,
    LambdaFunctionTarget, RegionId, ServiceName,
};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, LockResult, Mutex, MutexGuard};
use std::time::{Duration, UNIX_EPOCH};
use storage::{StorageFactory, StorageHandle};

const STEP_FUNCTIONS_SERVICE: &str = "step-functions";
const DEFAULT_INPUT: &str = "{}";

#[derive(Clone)]
pub struct StepFunctionsService {
    clock: Arc<dyn Clock>,
    execution_spawner: Arc<dyn StepFunctionsExecutionSpawner>,
    sleeper: Arc<dyn StepFunctionsSleeper>,
    state_lock: Arc<Mutex<()>>,
    state_machine_store:
        StorageHandle<StateMachineStorageKey, StoredStateMachine>,
    task_adapter: Arc<dyn StepFunctionsTaskAdapter>,
    execution_store: StorageHandle<ExecutionStorageKey, StoredExecution>,
}

impl StepFunctionsService {
    pub fn new(
        factory: &StorageFactory,
        dependencies: StepFunctionsServiceDependencies,
    ) -> Self {
        Self {
            clock: dependencies.clock,
            execution_spawner: dependencies.execution_spawner,
            sleeper: dependencies.sleeper,
            state_lock: Arc::new(Mutex::new(())),
            state_machine_store: factory
                .create(STEP_FUNCTIONS_SERVICE, "state-machines"),
            task_adapter: dependencies.task_adapter,
            execution_store: factory
                .create(STEP_FUNCTIONS_SERVICE, "executions"),
        }
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the definition is invalid, or
    /// the new state machine cannot be persisted.
    pub fn create_state_machine(
        &self,
        scope: &StepFunctionsScope,
        input: CreateStateMachineInput,
    ) -> Result<CreateStateMachineOutput, StepFunctionsError> {
        validate_name(&input.name, "State machine name")?;
        if input.role_arn.trim().is_empty() {
            return Err(StepFunctionsError::Validation {
                message: "roleArn is required.".to_owned(),
            });
        }
        let state_machine_type =
            StateMachineType::parse(input.state_machine_type.as_deref())?;
        parse_state_machine_definition(&input.definition)?;

        let key = StateMachineStorageKey::new(scope, &input.name);
        let _guard = self.lock_state();
        if self.state_machine_store.get(&key).is_some() {
            return Err(StepFunctionsError::StateMachineAlreadyExists {
                message: format!(
                    "State Machine Already Exists: {}",
                    state_machine_arn(scope, &input.name)
                ),
            });
        }

        let creation_date = self.now_epoch_seconds()?;
        let state_machine_arn = state_machine_arn(scope, &input.name);
        self.state_machine_store.put(
            key,
            StoredStateMachine {
                creation_date,
                definition: input.definition,
                name: input.name,
                role_arn: input.role_arn,
                state_machine_arn: state_machine_arn.clone(),
                state_machine_type,
            },
        )?;

        Ok(CreateStateMachineOutput { creation_date, state_machine_arn })
    }

    /// # Errors
    ///
    /// Returns an error when the requested state machine ARN is invalid or no
    /// matching stored state machine exists.
    pub fn describe_state_machine(
        &self,
        scope: &StepFunctionsScope,
        state_machine_arn: &str,
    ) -> Result<DescribeStateMachineOutput, StepFunctionsError> {
        let record = self.lookup_state_machine(scope, state_machine_arn)?;

        Ok(DescribeStateMachineOutput {
            creation_date: record.creation_date,
            definition: record.definition.clone(),
            name: record.name.clone(),
            role_arn: record.role_arn.clone(),
            state_machine_arn: record.state_machine_arn.clone(),
            state_machine_type: record.state_machine_type,
            status: StateMachineStatus::Active,
        })
    }

    /// # Errors
    ///
    /// Returns an error when pagination inputs are unsupported.
    pub fn list_state_machines(
        &self,
        scope: &StepFunctionsScope,
        input: ListStateMachinesInput,
    ) -> Result<ListStateMachinesOutput, StepFunctionsError> {
        reject_pagination(input.max_results, input.next_token.as_deref())?;
        let account_id = scope.account_id().to_owned();
        let region = scope.region().to_owned();
        let mut state_machines = self
            .state_machine_store
            .scan(&move |key| {
                key.account_id == account_id && key.region == region
            })
            .into_iter()
            .map(|record| StateMachineListItem {
                creation_date: record.creation_date,
                name: record.name,
                state_machine_arn: record.state_machine_arn,
                state_machine_type: record.state_machine_type,
            })
            .collect::<Vec<_>>();
        state_machines.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(ListStateMachinesOutput { state_machines })
    }

    /// # Errors
    ///
    /// Returns an error when the requested state machine ARN is invalid, no
    /// matching state machine exists, or deletion fails.
    pub fn delete_state_machine(
        &self,
        scope: &StepFunctionsScope,
        state_machine_arn: &str,
    ) -> Result<(), StepFunctionsError> {
        let key = parse_state_machine_arn(scope, state_machine_arn)?;
        let _guard = self.lock_state();
        if self.state_machine_store.get(&key).is_none() {
            return Err(StepFunctionsError::StateMachineDoesNotExist {
                message: format!(
                    "State Machine Does Not Exist: `{state_machine_arn}`"
                ),
            });
        }
        self.state_machine_store.delete(&key)?;

        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when validation fails, the definition or input is
    /// invalid, or the new execution cannot be persisted or scheduled.
    pub fn start_execution(
        &self,
        scope: &StepFunctionsScope,
        input: StartExecutionInput,
    ) -> Result<StartExecutionOutput, StepFunctionsError> {
        let state_machine =
            self.lookup_state_machine(scope, &input.state_machine_arn)?;
        let definition =
            parse_state_machine_definition(state_machine.definition.as_str())?;
        let input_value = parse_execution_input(input.input.as_deref())?;
        let input_string = canonical_json(&input_value)?;
        let execution_name = match input.name {
            Some(name) => {
                validate_name(&name, "Execution name")?;
                name
            }
            None => self.next_execution_name(scope, &state_machine.name),
        };
        let key = ExecutionStorageKey::new(
            scope,
            &state_machine.name,
            &execution_name,
        );
        let execution_arn =
            execution_arn(scope, &state_machine.name, &execution_name);
        let start_date = self.now_epoch_seconds()?;
        let execution = StoredExecution {
            cause: None,
            error: None,
            execution_arn: execution_arn.clone(),
            history: Vec::new(),
            input: input_string.clone(),
            name: execution_name.clone(),
            output: None,
            start_date,
            state_machine_arn: state_machine.state_machine_arn.clone(),
            status: ExecutionStatus::Running,
            stop_date: None,
        };

        {
            let _guard = self.lock_state();
            if self.execution_store.get(&key).is_some() {
                return Err(StepFunctionsError::ExecutionAlreadyExists {
                    message: format!(
                        "Execution Already Exists: {execution_arn}"
                    ),
                });
            }
            self.execution_store.put(key.clone(), execution)?;
        }

        self.append_history_event(
            &key,
            HistoryEvent {
                execution_aborted_event_details: None,
                execution_failed_event_details: None,
                execution_started_event_details: Some(
                    ExecutionStartedEventDetails {
                        input: input_string,
                        role_arn: state_machine.role_arn,
                    },
                ),
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: None,
                state_exited_event_details: None,
                task_failed_event_details: None,
                task_scheduled_event_details: None,
                task_succeeded_event_details: None,
                timestamp: start_date,
                event_type: "ExecutionStarted".to_owned(),
            },
        )?;

        let runner = self.clone();
        let task_name = format!("step-functions-{execution_name}");
        let state_machine_scope = scope.clone();
        self.execution_spawner.spawn(
            &task_name,
            Box::new(move || {
                runner.run_execution(
                    &state_machine_scope,
                    &key,
                    &definition,
                    input_value,
                );
            }),
        )?;

        Ok(StartExecutionOutput { execution_arn, start_date })
    }

    /// # Errors
    ///
    /// Returns an error when the requested execution ARN is invalid or no
    /// matching execution exists.
    pub fn describe_execution(
        &self,
        scope: &StepFunctionsScope,
        input: DescribeExecutionInput,
    ) -> Result<DescribeExecutionOutput, StepFunctionsError> {
        let record = self.lookup_execution(scope, &input.execution_arn)?;

        Ok(DescribeExecutionOutput {
            cause: record.cause.clone(),
            execution_arn: record.execution_arn.clone(),
            error: record.error.clone(),
            input: record.input.clone(),
            name: record.name.clone(),
            output: record.output.clone(),
            start_date: record.start_date,
            state_machine_arn: record.state_machine_arn.clone(),
            status: record.status,
            stop_date: record.stop_date,
        })
    }

    /// # Errors
    ///
    /// Returns an error when pagination inputs are unsupported or the target
    /// state machine ARN cannot be resolved.
    pub fn list_executions(
        &self,
        scope: &StepFunctionsScope,
        input: ListExecutionsInput,
    ) -> Result<ListExecutionsOutput, StepFunctionsError> {
        reject_pagination(input.max_results, input.next_token.as_deref())?;
        let state_machine =
            self.lookup_state_machine(scope, &input.state_machine_arn)?;
        let status_filter = input
            .status_filter
            .as_deref()
            .map(|value| ExecutionStatus::parse(Some(value)))
            .transpose()?;
        let account_id = scope.account_id().to_owned();
        let region = scope.region().to_owned();
        let state_machine_arn = state_machine.state_machine_arn.clone();
        let mut executions = self
            .execution_store
            .scan(&move |key| {
                key.account_id == account_id
                    && key.region == region
                    && key.state_machine_name == state_machine.name
            })
            .into_iter()
            .filter(|record| {
                record.state_machine_arn == state_machine_arn
                    && status_filter
                        .is_none_or(|status| record.status == status)
            })
            .map(|record| ExecutionListItem {
                execution_arn: record.execution_arn,
                name: record.name,
                start_date: record.start_date,
                state_machine_arn: record.state_machine_arn,
                status: record.status,
                stop_date: record.stop_date,
            })
            .collect::<Vec<_>>();
        executions.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(ListExecutionsOutput { executions })
    }

    /// # Errors
    ///
    /// Returns an error when the requested execution ARN is invalid, the
    /// execution is not running, or the updated state cannot be persisted.
    pub fn stop_execution(
        &self,
        scope: &StepFunctionsScope,
        input: StopExecutionInput,
    ) -> Result<StopExecutionOutput, StepFunctionsError> {
        let key = parse_execution_arn(scope, &input.execution_arn)?;
        let stop_date = self.now_epoch_seconds()?;
        self.with_execution_mut(&key, |execution| {
            if execution.status != ExecutionStatus::Running {
                return Err(StepFunctionsError::Validation {
                    message: "Execution is not running.".to_owned(),
                });
            }
            execution.status = ExecutionStatus::Aborted;
            execution.stop_date = Some(stop_date);
            execution.cause = input.cause.clone();
            execution.error = input.error.clone();

            Ok(())
        })?;
        self.append_history_event(
            &key,
            HistoryEvent {
                execution_aborted_event_details: Some(
                    ExecutionAbortedEventDetails {
                        cause: input.cause,
                        error: input.error,
                    },
                ),
                execution_failed_event_details: None,
                execution_started_event_details: None,
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: None,
                state_exited_event_details: None,
                task_failed_event_details: None,
                task_scheduled_event_details: None,
                task_succeeded_event_details: None,
                timestamp: stop_date,
                event_type: "ExecutionAborted".to_owned(),
            },
        )?;

        Ok(StopExecutionOutput { stop_date })
    }

    /// # Errors
    ///
    /// Returns an error when pagination inputs are unsupported or the target
    /// execution ARN cannot be resolved.
    pub fn get_execution_history(
        &self,
        scope: &StepFunctionsScope,
        input: GetExecutionHistoryInput,
    ) -> Result<GetExecutionHistoryOutput, StepFunctionsError> {
        reject_pagination(input.max_results, input.next_token.as_deref())?;
        let mut execution =
            self.lookup_execution(scope, &input.execution_arn)?;
        if !input.include_execution_data.unwrap_or(true) {
            strip_execution_data(&mut execution.history);
        }
        if input.reverse_order.unwrap_or(false) {
            execution.history.reverse();
        }

        Ok(GetExecutionHistoryOutput { events: execution.history })
    }

    fn run_execution(
        &self,
        scope: &StepFunctionsScope,
        key: &ExecutionStorageKey,
        definition: &StateMachineDefinition,
        input: Value,
    ) {
        let outcome = self.execute_definition(scope, key, definition, input);
        match outcome {
            Ok(ExecutionOutcome::Succeeded(output)) => {
                let stop_date = match self.now_epoch_seconds() {
                    Ok(timestamp) => timestamp,
                    Err(_) => return,
                };
                let output_string = match canonical_json(&output) {
                    Ok(output_string) => output_string,
                    Err(_) => return,
                };
                if self
                    .with_execution_mut(key, |execution| {
                        if execution.status != ExecutionStatus::Running {
                            return Ok(());
                        }
                        execution.output = Some(output_string.clone());
                        execution.status = ExecutionStatus::Succeeded;
                        execution.stop_date = Some(stop_date);

                        Ok(())
                    })
                    .is_err()
                {
                    return;
                }
                let _ = self.append_history_event(
                    key,
                    HistoryEvent {
                        execution_aborted_event_details: None,
                        execution_failed_event_details: None,
                        execution_started_event_details: None,
                        execution_succeeded_event_details: Some(
                            ExecutionSucceededEventDetails {
                                output: output_string,
                            },
                        ),
                        id: 0,
                        previous_event_id: None,
                        state_entered_event_details: None,
                        state_exited_event_details: None,
                        task_failed_event_details: None,
                        task_scheduled_event_details: None,
                        task_succeeded_event_details: None,
                        timestamp: stop_date,
                        event_type: "ExecutionSucceeded".to_owned(),
                    },
                );
            }
            Ok(ExecutionOutcome::Failed { error, cause }) => {
                let stop_date = match self.now_epoch_seconds() {
                    Ok(timestamp) => timestamp,
                    Err(_) => return,
                };
                if self
                    .with_execution_mut(key, |execution| {
                        if execution.status != ExecutionStatus::Running {
                            return Ok(());
                        }
                        execution.error = Some(error.clone());
                        execution.cause = Some(cause.clone());
                        execution.status = ExecutionStatus::Failed;
                        execution.stop_date = Some(stop_date);

                        Ok(())
                    })
                    .is_err()
                {
                    return;
                }
                let _ = self.append_history_event(
                    key,
                    HistoryEvent {
                        execution_aborted_event_details: None,
                        execution_failed_event_details: Some(
                            ExecutionFailedEventDetails { cause, error },
                        ),
                        execution_started_event_details: None,
                        execution_succeeded_event_details: None,
                        id: 0,
                        previous_event_id: None,
                        state_entered_event_details: None,
                        state_exited_event_details: None,
                        task_failed_event_details: None,
                        task_scheduled_event_details: None,
                        task_succeeded_event_details: None,
                        timestamp: stop_date,
                        event_type: "ExecutionFailed".to_owned(),
                    },
                );
            }
            Ok(ExecutionOutcome::Aborted) => {}
            Err(error) => {
                let stop_date = match self.now_epoch_seconds() {
                    Ok(timestamp) => timestamp,
                    Err(_) => return,
                };
                let cause = error.to_string();
                let _ = self.with_execution_mut(key, |execution| {
                    if execution.status != ExecutionStatus::Running {
                        return Ok(());
                    }
                    execution.error = Some("States.Runtime".to_owned());
                    execution.cause = Some(cause.clone());
                    execution.status = ExecutionStatus::Failed;
                    execution.stop_date = Some(stop_date);

                    Ok(())
                });
                let _ = self.append_history_event(
                    key,
                    HistoryEvent {
                        execution_aborted_event_details: None,
                        execution_failed_event_details: Some(
                            ExecutionFailedEventDetails {
                                cause,
                                error: "States.Runtime".to_owned(),
                            },
                        ),
                        execution_started_event_details: None,
                        execution_succeeded_event_details: None,
                        id: 0,
                        previous_event_id: None,
                        state_entered_event_details: None,
                        state_exited_event_details: None,
                        task_failed_event_details: None,
                        task_scheduled_event_details: None,
                        task_succeeded_event_details: None,
                        timestamp: stop_date,
                        event_type: "ExecutionFailed".to_owned(),
                    },
                );
            }
        }
    }

    fn definition_state<'a>(
        &self,
        definition: &'a StateMachineDefinition,
        state_name: &str,
    ) -> Result<&'a State, StepFunctionsError> {
        definition.states.get(state_name).ok_or_else(|| {
            StepFunctionsError::Validation {
                message: format!(
                    "State `{state_name}` was not found in its definition."
                ),
            }
        })
    }

    fn execute_definition(
        &self,
        scope: &StepFunctionsScope,
        key: &ExecutionStorageKey,
        definition: &StateMachineDefinition,
        input: Value,
    ) -> Result<ExecutionOutcome, StepFunctionsError> {
        let mut current_state = definition.start_at.clone();
        let mut current_data = input;

        loop {
            if !self.execution_running(key)? {
                return Ok(ExecutionOutcome::Aborted);
            }
            let state =
                self.definition_state(definition, current_state.as_str())?;
            let entered_data =
                apply_input_path(state.input_path(), &current_data)?;
            self.record_state_entered(
                key,
                state.entered_event_type(),
                &current_state,
                &entered_data,
            )?;

            let transition = match state {
                State::Pass(state) => {
                    let result = if let Some(parameters) = &state.parameters {
                        resolve_parameters(parameters, &entered_data)?
                    } else if let Some(result) = &state.result {
                        result.clone()
                    } else {
                        entered_data.clone()
                    };
                    let merged = apply_result_path(
                        state.result_path.as_ref(),
                        &entered_data,
                        result,
                    )?;
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &merged,
                    )?;
                    if state.end {
                        StateTransition::Succeeded(output)
                    } else {
                        StateTransition::Next(
                            required_next_state(
                                state.next.as_ref(),
                                &current_state,
                                "Pass",
                            )?,
                            output,
                        )
                    }
                }
                State::Task(state) => {
                    let request = build_task_request(state, &entered_data)?;
                    self.record_task_scheduled(key, &request)?;
                    let invocation =
                        match self.task_adapter.invoke(scope, &request) {
                            Ok(invocation) => invocation,
                            Err(error) => {
                                self.record_task_failed(key, &error)?;
                                return Ok(ExecutionOutcome::Failed {
                                    error: error.error,
                                    cause: error.cause,
                                });
                            }
                        };
                    if !self.execution_running(key)? {
                        return Ok(ExecutionOutcome::Aborted);
                    }
                    let task_result = task_result_value(state, &invocation);
                    self.record_task_succeeded(key, &request, &task_result)?;
                    let merged = apply_result_path(
                        state.result_path.as_ref(),
                        &entered_data,
                        task_result,
                    )?;
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &merged,
                    )?;
                    if state.end {
                        StateTransition::Succeeded(output)
                    } else {
                        StateTransition::Next(
                            required_next_state(
                                state.next.as_ref(),
                                &current_state,
                                "Task",
                            )?,
                            output,
                        )
                    }
                }
                State::Choice(state) => {
                    let Some(next) =
                        evaluate_choice_state(state, &entered_data)?
                    else {
                        return Ok(ExecutionOutcome::Failed {
                            error: "States.NoChoiceMatched".to_owned(),
                            cause: "No Choice state matched and no Default \
                                    state was provided."
                                .to_owned(),
                        });
                    };
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &entered_data,
                    )?;
                    StateTransition::Next(next, output)
                }
                State::Wait(state) => {
                    let seconds = wait_seconds(state, &entered_data)?;
                    if seconds > 0 {
                        self.sleeper.sleep(Duration::from_secs(seconds))?;
                    }
                    if !self.execution_running(key)? {
                        return Ok(ExecutionOutcome::Aborted);
                    }
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &entered_data,
                    )?;
                    if state.end {
                        StateTransition::Succeeded(output)
                    } else {
                        StateTransition::Next(
                            required_next_state(
                                state.next.as_ref(),
                                &current_state,
                                "Wait",
                            )?,
                            output,
                        )
                    }
                }
                State::Succeed(state) => {
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &entered_data,
                    )?;
                    StateTransition::Succeeded(output)
                }
                State::Fail(state) => {
                    return Ok(ExecutionOutcome::Failed {
                        cause: state.cause.clone().unwrap_or_default(),
                        error: state
                            .error
                            .clone()
                            .unwrap_or_else(|| "States.Fail".to_owned()),
                    });
                }
                State::Parallel(state) => {
                    let branch_input =
                        if let Some(parameters) = &state.parameters {
                            resolve_parameters(parameters, &entered_data)?
                        } else {
                            entered_data.clone()
                        };
                    let mut results = Vec::with_capacity(state.branches.len());
                    for branch in &state.branches {
                        match self.execute_definition(
                            scope,
                            key,
                            branch,
                            branch_input.clone(),
                        )? {
                            ExecutionOutcome::Succeeded(output) => {
                                results.push(output);
                            }
                            ExecutionOutcome::Failed { error, cause } => {
                                return Ok(ExecutionOutcome::Failed {
                                    error,
                                    cause,
                                });
                            }
                            ExecutionOutcome::Aborted => {
                                return Ok(ExecutionOutcome::Aborted);
                            }
                        }
                    }
                    let merged = apply_result_path(
                        state.result_path.as_ref(),
                        &entered_data,
                        Value::Array(results),
                    )?;
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &merged,
                    )?;
                    if state.end {
                        StateTransition::Succeeded(output)
                    } else {
                        StateTransition::Next(
                            required_next_state(
                                state.next.as_ref(),
                                &current_state,
                                "Parallel",
                            )?,
                            output,
                        )
                    }
                }
                State::Map(state) => {
                    let map_input = if let Some(parameters) = &state.parameters
                    {
                        resolve_parameters(parameters, &entered_data)?
                    } else {
                        entered_data.clone()
                    };
                    let items =
                        map_items(state.items_path.as_ref(), &map_input)?;
                    let mut results = Vec::with_capacity(items.len());
                    for item in items {
                        match self.execute_definition(
                            scope,
                            key,
                            &state.iterator,
                            item,
                        )? {
                            ExecutionOutcome::Succeeded(output) => {
                                results.push(output);
                            }
                            ExecutionOutcome::Failed { error, cause } => {
                                return Ok(ExecutionOutcome::Failed {
                                    error,
                                    cause,
                                });
                            }
                            ExecutionOutcome::Aborted => {
                                return Ok(ExecutionOutcome::Aborted);
                            }
                        }
                    }
                    let merged = apply_result_path(
                        state.result_path.as_ref(),
                        &entered_data,
                        Value::Array(results),
                    )?;
                    let output = apply_output_path(
                        state.output_path.as_ref(),
                        &merged,
                    )?;
                    if state.end {
                        StateTransition::Succeeded(output)
                    } else {
                        StateTransition::Next(
                            required_next_state(
                                state.next.as_ref(),
                                &current_state,
                                "Map",
                            )?,
                            output,
                        )
                    }
                }
            };

            match transition {
                StateTransition::Next(next, output) => {
                    self.record_state_exited(
                        key,
                        state.exited_event_type(),
                        &current_state,
                        &output,
                    )?;
                    current_state = next;
                    current_data = output;
                }
                StateTransition::Succeeded(output) => {
                    self.record_state_exited(
                        key,
                        state.exited_event_type(),
                        &current_state,
                        &output,
                    )?;
                    return Ok(ExecutionOutcome::Succeeded(output));
                }
            }
        }
    }

    fn record_state_entered(
        &self,
        key: &ExecutionStorageKey,
        event_type: &str,
        name: &str,
        input: &Value,
    ) -> Result<(), StepFunctionsError> {
        let timestamp = self.now_epoch_seconds()?;
        self.append_history_event(
            key,
            HistoryEvent {
                execution_aborted_event_details: None,
                execution_failed_event_details: None,
                execution_started_event_details: None,
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: Some(StateEnteredEventDetails {
                    input: canonical_json(input)?,
                    name: name.to_owned(),
                }),
                state_exited_event_details: None,
                task_failed_event_details: None,
                task_scheduled_event_details: None,
                task_succeeded_event_details: None,
                timestamp,
                event_type: event_type.to_owned(),
            },
        )
    }

    fn record_state_exited(
        &self,
        key: &ExecutionStorageKey,
        event_type: &str,
        name: &str,
        output: &Value,
    ) -> Result<(), StepFunctionsError> {
        let timestamp = self.now_epoch_seconds()?;
        self.append_history_event(
            key,
            HistoryEvent {
                execution_aborted_event_details: None,
                execution_failed_event_details: None,
                execution_started_event_details: None,
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: None,
                state_exited_event_details: Some(StateExitedEventDetails {
                    name: name.to_owned(),
                    output: canonical_json(output)?,
                }),
                task_failed_event_details: None,
                task_scheduled_event_details: None,
                task_succeeded_event_details: None,
                timestamp,
                event_type: event_type.to_owned(),
            },
        )
    }

    fn record_task_scheduled(
        &self,
        key: &ExecutionStorageKey,
        request: &TaskInvocationRequest,
    ) -> Result<(), StepFunctionsError> {
        let timestamp = self.now_epoch_seconds()?;
        self.append_history_event(
            key,
            HistoryEvent {
                execution_aborted_event_details: None,
                execution_failed_event_details: None,
                execution_started_event_details: None,
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: None,
                state_exited_event_details: None,
                task_failed_event_details: None,
                task_scheduled_event_details: Some(
                    TaskScheduledEventDetails {
                        resource: request.resource.clone(),
                        resource_type: request.resource_type.clone(),
                    },
                ),
                task_succeeded_event_details: None,
                timestamp,
                event_type: "TaskScheduled".to_owned(),
            },
        )
    }

    fn record_task_succeeded(
        &self,
        key: &ExecutionStorageKey,
        request: &TaskInvocationRequest,
        output: &Value,
    ) -> Result<(), StepFunctionsError> {
        let timestamp = self.now_epoch_seconds()?;
        self.append_history_event(
            key,
            HistoryEvent {
                execution_aborted_event_details: None,
                execution_failed_event_details: None,
                execution_started_event_details: None,
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: None,
                state_exited_event_details: None,
                task_failed_event_details: None,
                task_scheduled_event_details: None,
                task_succeeded_event_details: Some(
                    TaskSucceededEventDetails {
                        output: canonical_json(output)?,
                        resource: request.resource.clone(),
                        resource_type: request.resource_type.clone(),
                    },
                ),
                timestamp,
                event_type: "TaskSucceeded".to_owned(),
            },
        )
    }

    fn record_task_failed(
        &self,
        key: &ExecutionStorageKey,
        error: &TaskInvocationFailure,
    ) -> Result<(), StepFunctionsError> {
        let timestamp = self.now_epoch_seconds()?;
        self.append_history_event(
            key,
            HistoryEvent {
                execution_aborted_event_details: None,
                execution_failed_event_details: None,
                execution_started_event_details: None,
                execution_succeeded_event_details: None,
                id: 0,
                previous_event_id: None,
                state_entered_event_details: None,
                state_exited_event_details: None,
                task_failed_event_details: Some(TaskFailedEventDetails {
                    cause: error.cause.clone(),
                    error: error.error.clone(),
                    resource: error.resource.clone(),
                    resource_type: error.resource_type.clone(),
                }),
                task_scheduled_event_details: None,
                task_succeeded_event_details: None,
                timestamp,
                event_type: "TaskFailed".to_owned(),
            },
        )
    }

    fn append_history_event(
        &self,
        key: &ExecutionStorageKey,
        mut event: HistoryEvent,
    ) -> Result<(), StepFunctionsError> {
        self.with_execution_mut(key, |execution| {
            let next_id = execution.history.len() as u64 + 1;
            event.id = next_id;
            event.previous_event_id = next_id.checked_sub(1);
            execution.history.push(event);
            Ok(())
        })
    }

    fn execution_running(
        &self,
        key: &ExecutionStorageKey,
    ) -> Result<bool, StepFunctionsError> {
        Ok(self
            .execution_store
            .get(key)
            .ok_or_else(|| StepFunctionsError::ExecutionDoesNotExist {
                message: format!(
                    "Execution Does Not Exist: {}",
                    key.execution_arn()
                ),
            })?
            .status
            == ExecutionStatus::Running)
    }

    fn next_execution_name(
        &self,
        scope: &StepFunctionsScope,
        state_machine_name: &str,
    ) -> String {
        let account_id = scope.account_id().to_owned();
        let region = scope.region().to_owned();
        let state_machine_name = state_machine_name.to_owned();
        let count = self
            .execution_store
            .scan(&move |key| {
                key.account_id == account_id
                    && key.region == region
                    && key.state_machine_name == state_machine_name
            })
            .len();

        format!("execution-{}", count + 1)
    }

    fn lookup_state_machine(
        &self,
        scope: &StepFunctionsScope,
        state_machine_arn: &str,
    ) -> Result<StoredStateMachine, StepFunctionsError> {
        let key = parse_state_machine_arn(scope, state_machine_arn)?;
        self.state_machine_store.get(&key).ok_or_else(|| {
            StepFunctionsError::StateMachineDoesNotExist {
                message: format!(
                    "State Machine Does Not Exist: `{state_machine_arn}`"
                ),
            }
        })
    }

    fn lookup_execution(
        &self,
        scope: &StepFunctionsScope,
        execution_arn: &str,
    ) -> Result<StoredExecution, StepFunctionsError> {
        let key = parse_execution_arn(scope, execution_arn)?;
        self.execution_store.get(&key).ok_or_else(|| {
            StepFunctionsError::ExecutionDoesNotExist {
                message: format!(
                    "Execution Does Not Exist: `{execution_arn}`"
                ),
            }
        })
    }

    fn with_execution_mut<T, F>(
        &self,
        key: &ExecutionStorageKey,
        update: F,
    ) -> Result<T, StepFunctionsError>
    where
        F: FnOnce(&mut StoredExecution) -> Result<T, StepFunctionsError>,
    {
        let _guard = self.lock_state();
        let mut execution =
            self.execution_store.get(key).ok_or_else(|| {
                StepFunctionsError::ExecutionDoesNotExist {
                    message: format!(
                        "Execution Does Not Exist: `{}`",
                        key.execution_arn()
                    ),
                }
            })?;
        let value = update(&mut execution)?;
        self.execution_store.put(key.clone(), execution)?;

        Ok(value)
    }

    fn now_epoch_seconds(&self) -> Result<u64, StepFunctionsError> {
        self.clock
            .now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .map_err(|error| {
                StepFunctionsError::Infrastructure(
                    InfrastructureError::scheduler(
                        "clock",
                        "step-functions-clock",
                        std::io::Error::other(error.to_string()),
                    ),
                )
            })
    }

    fn lock_state(&self) -> MutexGuard<'_, ()> {
        recover(self.state_lock.lock())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct StateMachineStorageKey {
    account_id: AccountId,
    region: RegionId,
    name: String,
}

impl StateMachineStorageKey {
    fn new(scope: &StepFunctionsScope, name: &str) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            name: name.to_owned(),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
struct ExecutionStorageKey {
    account_id: AccountId,
    region: RegionId,
    state_machine_name: String,
    name: String,
}

impl ExecutionStorageKey {
    fn new(
        scope: &StepFunctionsScope,
        state_machine_name: &str,
        name: &str,
    ) -> Self {
        Self {
            account_id: scope.account_id().clone(),
            region: scope.region().clone(),
            state_machine_name: state_machine_name.to_owned(),
            name: name.to_owned(),
        }
    }

    fn execution_arn(&self) -> String {
        format!(
            "arn:aws:states:{}:{}:execution:{}:{}",
            self.region.as_str(),
            self.account_id.as_str(),
            self.state_machine_name,
            self.name
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredStateMachine {
    creation_date: u64,
    definition: String,
    name: String,
    role_arn: String,
    state_machine_arn: String,
    state_machine_type: StateMachineType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredExecution {
    cause: Option<String>,
    error: Option<String>,
    execution_arn: String,
    history: Vec<HistoryEvent>,
    input: String,
    name: String,
    output: Option<String>,
    start_date: u64,
    state_machine_arn: String,
    status: ExecutionStatus,
    stop_date: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
enum ExecutionOutcome {
    Succeeded(Value),
    Failed { error: String, cause: String },
    Aborted,
}

#[derive(Debug, Clone, PartialEq)]
enum StateTransition {
    Next(String, Value),
    Succeeded(Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInvocationRequest {
    function_name: String,
    payload: Value,
    resource: String,
    resource_type: String,
    target: LambdaFunctionTarget,
}

impl TaskInvocationRequest {
    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn resource(&self) -> &str {
        &self.resource
    }

    pub fn resource_type(&self) -> &str {
        &self.resource_type
    }

    pub fn target(&self) -> &LambdaFunctionTarget {
        &self.target
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInvocationResult {
    executed_version: String,
    payload: Value,
    status_code: u16,
}

impl TaskInvocationResult {
    pub fn new(
        executed_version: impl Into<String>,
        payload: Value,
        status_code: u16,
    ) -> Self {
        Self {
            executed_version: executed_version.into(),
            payload,
            status_code,
        }
    }

    pub fn executed_version(&self) -> &str {
        &self.executed_version
    }

    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInvocationFailure {
    cause: String,
    error: String,
    resource: String,
    resource_type: String,
}

impl TaskInvocationFailure {
    pub fn new(
        error: impl Into<String>,
        cause: impl Into<String>,
        resource: impl Into<String>,
        resource_type: impl Into<String>,
    ) -> Self {
        Self {
            cause: cause.into(),
            error: error.into(),
            resource: resource.into(),
            resource_type: resource_type.into(),
        }
    }

    pub fn with_error_override(mut self, error: impl Into<String>) -> Self {
        self.error = error.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateMachineDefinition {
    start_at: String,
    states: BTreeMap<String, State>,
}

#[derive(Debug, Clone, PartialEq)]
enum State {
    Pass(PassState),
    Task(TaskState),
    Choice(ChoiceState),
    Wait(WaitState),
    Succeed(SucceedState),
    Fail(FailState),
    Parallel(ParallelState),
    Map(MapState),
}

impl State {
    fn input_path(&self) -> Option<&PathSelection> {
        match self {
            Self::Pass(state) => state.input_path.as_ref(),
            Self::Task(state) => state.input_path.as_ref(),
            Self::Choice(state) => state.input_path.as_ref(),
            Self::Wait(state) => state.input_path.as_ref(),
            Self::Succeed(state) => state.input_path.as_ref(),
            Self::Fail(_) => None,
            Self::Parallel(state) => state.input_path.as_ref(),
            Self::Map(state) => state.input_path.as_ref(),
        }
    }

    fn entered_event_type(&self) -> &'static str {
        match self {
            Self::Pass(_) => "PassStateEntered",
            Self::Task(_) => "TaskStateEntered",
            Self::Choice(_) => "ChoiceStateEntered",
            Self::Wait(_) => "WaitStateEntered",
            Self::Succeed(_) => "SucceedStateEntered",
            Self::Fail(_) => "FailStateEntered",
            Self::Parallel(_) => "ParallelStateEntered",
            Self::Map(_) => "MapStateEntered",
        }
    }

    fn exited_event_type(&self) -> &'static str {
        match self {
            Self::Pass(_) => "PassStateExited",
            Self::Task(_) => "TaskStateExited",
            Self::Choice(_) => "ChoiceStateExited",
            Self::Wait(_) => "WaitStateExited",
            Self::Succeed(_) => "SucceedStateExited",
            Self::Fail(_) => "FailStateExited",
            Self::Parallel(_) => "ParallelStateExited",
            Self::Map(_) => "MapStateExited",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PassState {
    end: bool,
    input_path: Option<PathSelection>,
    next: Option<String>,
    output_path: Option<PathSelection>,
    parameters: Option<Value>,
    result: Option<Value>,
    result_path: Option<ResultPathSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskState {
    end: bool,
    input_path: Option<PathSelection>,
    next: Option<String>,
    output_path: Option<PathSelection>,
    parameters: Option<Value>,
    resource: TaskResource,
    result_path: Option<ResultPathSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskResource {
    DirectLambdaArn(String),
    OptimizedLambdaInvoke,
}

#[derive(Debug, Clone, PartialEq)]
struct ChoiceState {
    choices: Vec<ChoiceRule>,
    default: Option<String>,
    input_path: Option<PathSelection>,
    output_path: Option<PathSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WaitState {
    end: bool,
    input_path: Option<PathSelection>,
    next: Option<String>,
    output_path: Option<PathSelection>,
    seconds: WaitSeconds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WaitSeconds {
    Literal(u64),
    Path(JsonPath),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SucceedState {
    input_path: Option<PathSelection>,
    output_path: Option<PathSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailState {
    cause: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct ParallelState {
    branches: Vec<StateMachineDefinition>,
    end: bool,
    input_path: Option<PathSelection>,
    next: Option<String>,
    output_path: Option<PathSelection>,
    parameters: Option<Value>,
    result_path: Option<ResultPathSelection>,
}

#[derive(Debug, Clone, PartialEq)]
struct MapState {
    end: bool,
    input_path: Option<PathSelection>,
    items_path: Option<JsonPath>,
    iterator: StateMachineDefinition,
    next: Option<String>,
    output_path: Option<PathSelection>,
    parameters: Option<Value>,
    result_path: Option<ResultPathSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSelection {
    Discard,
    Path(JsonPath),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResultPathSelection {
    Discard,
    Path(JsonPath),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JsonPath {
    tokens: Vec<PathToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathToken {
    Field(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathLookup<'a> {
    Missing,
    Present(&'a Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawStateMachineDefinition {
    #[serde(rename = "Comment")]
    _comment: Option<String>,
    start_at: String,
    states: BTreeMap<String, Value>,
    #[serde(rename = "Version")]
    _version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawPassState {
    #[serde(rename = "Type")]
    _state_type: String,
    end: Option<bool>,
    input_path: Option<NullablePath>,
    next: Option<String>,
    output_path: Option<NullablePath>,
    parameters: Option<Value>,
    result: Option<Value>,
    result_path: Option<NullablePath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawTaskState {
    #[serde(rename = "Type")]
    _state_type: String,
    end: Option<bool>,
    input_path: Option<NullablePath>,
    next: Option<String>,
    output_path: Option<NullablePath>,
    parameters: Option<Value>,
    resource: String,
    result_path: Option<NullablePath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawChoiceState {
    #[serde(rename = "Type")]
    _state_type: String,
    choices: Vec<Value>,
    default: Option<String>,
    input_path: Option<NullablePath>,
    output_path: Option<NullablePath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawWaitState {
    #[serde(rename = "Type")]
    _state_type: String,
    end: Option<bool>,
    input_path: Option<NullablePath>,
    next: Option<String>,
    output_path: Option<NullablePath>,
    seconds: Option<u64>,
    seconds_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawSucceedState {
    #[serde(rename = "Type")]
    _state_type: String,
    input_path: Option<NullablePath>,
    output_path: Option<NullablePath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawFailState {
    #[serde(rename = "Type")]
    _state_type: String,
    cause: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawParallelState {
    #[serde(rename = "Type")]
    _state_type: String,
    branches: Vec<Value>,
    end: Option<bool>,
    input_path: Option<NullablePath>,
    next: Option<String>,
    output_path: Option<NullablePath>,
    parameters: Option<Value>,
    result_path: Option<NullablePath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct RawMapState {
    #[serde(rename = "Type")]
    _state_type: String,
    end: Option<bool>,
    input_path: Option<NullablePath>,
    items_path: Option<String>,
    iterator: Value,
    next: Option<String>,
    output_path: Option<NullablePath>,
    parameters: Option<Value>,
    result_path: Option<NullablePath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NullablePath {
    Null,
    Path(String),
}

impl<'de> Deserialize<'de> for NullablePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::Null => Ok(Self::Null),
            Value::String(value) => Ok(Self::Path(value)),
            other => Err(D::Error::custom(format!(
                "expected a JSONPath string or null, got {other}"
            ))),
        }
    }
}

fn parse_state_machine_definition(
    definition: &str,
) -> Result<StateMachineDefinition, StepFunctionsError> {
    let raw: RawStateMachineDefinition = serde_json::from_str(definition)
        .map_err(|error| StepFunctionsError::InvalidDefinition {
            message: format!("State Machine Definition is invalid: {error}"),
        })?;
    let mut states = BTreeMap::new();
    for (name, raw_state) in raw.states {
        validate_name(&name, "State name")?;
        states.insert(name.clone(), parse_state(&name, raw_state)?);
    }
    if !states.contains_key(&raw.start_at) {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "StartAt references unknown state `{}`.",
                raw.start_at
            ),
        });
    }
    let definition = StateMachineDefinition { start_at: raw.start_at, states };
    validate_state_targets(&definition)?;

    Ok(definition)
}

fn parse_state(
    state_name: &str,
    state: Value,
) -> Result<State, StepFunctionsError> {
    let state_type =
        state.get("Type").and_then(Value::as_str).ok_or_else(|| {
            StepFunctionsError::InvalidDefinition {
                message: format!("State `{state_name}` must declare a Type."),
            }
        })?;

    match state_type {
        "Pass" => {
            let raw: RawPassState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Pass state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            if raw.result.is_some() && raw.parameters.is_some() {
                return Err(StepFunctionsError::InvalidDefinition {
                    message: format!(
                        "Pass state `{state_name}` cannot declare both \
                         Result and Parameters."
                    ),
                });
            }
            let (next, end) =
                parse_next_or_end(state_name, "Pass", raw.next, raw.end)?;
            Ok(State::Pass(PassState {
                end,
                input_path: parse_path_selection(raw.input_path)?,
                next,
                output_path: parse_path_selection(raw.output_path)?,
                parameters: raw.parameters,
                result: raw.result,
                result_path: parse_result_path_selection(raw.result_path)?,
            }))
        }
        "Task" => {
            let raw: RawTaskState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Task state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            let resource = parse_task_resource(
                state_name,
                &raw.resource,
                raw.parameters.as_ref(),
            )?;
            let (next, end) =
                parse_next_or_end(state_name, "Task", raw.next, raw.end)?;
            Ok(State::Task(TaskState {
                end,
                input_path: parse_path_selection(raw.input_path)?,
                next,
                output_path: parse_path_selection(raw.output_path)?,
                parameters: raw.parameters,
                resource,
                result_path: parse_result_path_selection(raw.result_path)?,
            }))
        }
        "Choice" => {
            let raw: RawChoiceState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Choice state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            let mut choices = Vec::with_capacity(raw.choices.len());
            for raw_choice in raw.choices {
                choices.push(parse_choice_rule(raw_choice)?);
            }
            Ok(State::Choice(ChoiceState {
                choices,
                default: raw.default,
                input_path: parse_path_selection(raw.input_path)?,
                output_path: parse_path_selection(raw.output_path)?,
            }))
        }
        "Wait" => {
            let raw: RawWaitState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Wait state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            let (next, end) =
                parse_next_or_end(state_name, "Wait", raw.next, raw.end)?;
            let seconds = match (raw.seconds, raw.seconds_path.as_deref()) {
                (Some(seconds), None) => WaitSeconds::Literal(seconds),
                (None, Some(path)) => {
                    WaitSeconds::Path(parse_json_path(path)?)
                }
                (Some(_), Some(_)) => {
                    return Err(StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Wait state `{state_name}` must declare exactly \
                             one of Seconds or SecondsPath."
                        ),
                    });
                }
                (None, None) => {
                    return Err(StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Wait state `{state_name}` must declare Seconds \
                             or SecondsPath."
                        ),
                    });
                }
            };
            Ok(State::Wait(WaitState {
                end,
                input_path: parse_path_selection(raw.input_path)?,
                next,
                output_path: parse_path_selection(raw.output_path)?,
                seconds,
            }))
        }
        "Succeed" => {
            let raw: RawSucceedState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Succeed state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            Ok(State::Succeed(SucceedState {
                input_path: parse_path_selection(raw.input_path)?,
                output_path: parse_path_selection(raw.output_path)?,
            }))
        }
        "Fail" => {
            let raw: RawFailState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Fail state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            Ok(State::Fail(FailState { cause: raw.cause, error: raw.error }))
        }
        "Parallel" => {
            let raw: RawParallelState = serde_json::from_value(state)
                .map_err(|error| StepFunctionsError::InvalidDefinition {
                    message: format!(
                        "Parallel state `{state_name}` is invalid: {error}"
                    ),
                })?;
            let (next, end) =
                parse_next_or_end(state_name, "Parallel", raw.next, raw.end)?;
            let mut branches = Vec::with_capacity(raw.branches.len());
            for branch in raw.branches {
                branches.push(parse_nested_definition(branch)?);
            }
            Ok(State::Parallel(ParallelState {
                branches,
                end,
                input_path: parse_path_selection(raw.input_path)?,
                next,
                output_path: parse_path_selection(raw.output_path)?,
                parameters: raw.parameters,
                result_path: parse_result_path_selection(raw.result_path)?,
            }))
        }
        "Map" => {
            let raw: RawMapState =
                serde_json::from_value(state).map_err(|error| {
                    StepFunctionsError::InvalidDefinition {
                        message: format!(
                            "Map state `{state_name}` is invalid: {error}"
                        ),
                    }
                })?;
            let (next, end) =
                parse_next_or_end(state_name, "Map", raw.next, raw.end)?;
            Ok(State::Map(MapState {
                end,
                input_path: parse_path_selection(raw.input_path)?,
                items_path: raw
                    .items_path
                    .as_deref()
                    .map(parse_json_path)
                    .transpose()?,
                iterator: parse_nested_definition(raw.iterator)?,
                next,
                output_path: parse_path_selection(raw.output_path)?,
                parameters: raw.parameters,
                result_path: parse_result_path_selection(raw.result_path)?,
            }))
        }
        other => Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "State `{state_name}` uses unsupported type `{other}`."
            ),
        }),
    }
}

fn parse_nested_definition(
    value: Value,
) -> Result<StateMachineDefinition, StepFunctionsError> {
    let definition = serde_json::to_string(&value).map_err(|error| {
        StepFunctionsError::InvalidDefinition {
            message: format!(
                "Nested state machine definition is invalid: {error}"
            ),
        }
    })?;
    parse_state_machine_definition(&definition)
}

fn parse_next_or_end(
    state_name: &str,
    state_type: &str,
    next: Option<String>,
    end: Option<bool>,
) -> Result<(Option<String>, bool), StepFunctionsError> {
    let end = end.unwrap_or(false);
    if next.is_some() == end {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "{state_type} state `{state_name}` must declare exactly one of \
                 Next or End."
            ),
        });
    }

    Ok((next, end))
}

fn parse_task_resource(
    state_name: &str,
    resource: &str,
    parameters: Option<&Value>,
) -> Result<TaskResource, StepFunctionsError> {
    if resource == "arn:aws:states:::lambda:invoke" {
        validate_optimized_lambda_parameters(state_name, parameters)?;
        return Ok(TaskResource::OptimizedLambdaInvoke);
    }
    if resource.ends_with(".waitForTaskToken") || resource.ends_with(".sync") {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` uses unsupported resource \
                 `{resource}`."
            ),
        });
    }
    if resource.starts_with("arn:aws:states:::") {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` uses unsupported integration \
                 `{resource}`."
            ),
        });
    }
    let parsed = resource.parse::<Arn>().map_err(|error| {
        StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` uses an invalid resource ARN: {error}"
            ),
        }
    })?;
    if parsed.service() != ServiceName::Lambda {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` only supports Lambda resources."
            ),
        });
    }
    let ArnResource::Generic(resource_id) = parsed.resource() else {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` requires a Lambda function ARN."
            ),
        });
    };
    if !resource_id.starts_with("function:") {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` requires a Lambda function ARN."
            ),
        });
    }

    Ok(TaskResource::DirectLambdaArn(resource.to_owned()))
}

fn validate_optimized_lambda_parameters(
    state_name: &str,
    parameters: Option<&Value>,
) -> Result<(), StepFunctionsError> {
    let Some(parameters) = parameters else {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` requires Parameters for \
                 arn:aws:states:::lambda:invoke."
            ),
        });
    };
    let Some(object) = parameters.as_object() else {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` requires object Parameters for \
                 arn:aws:states:::lambda:invoke."
            ),
        });
    };
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "FunctionName" | "FunctionName.$" | "Payload" | "Payload.$"
        ) {
            return Err(StepFunctionsError::InvalidDefinition {
                message: format!(
                    "Task state `{state_name}` only supports FunctionName and \
                     Payload Parameters for arn:aws:states:::lambda:invoke."
                ),
            });
        }
    }
    if !object.contains_key("FunctionName")
        && !object.contains_key("FunctionName.$")
    {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!(
                "Task state `{state_name}` requires FunctionName for \
                 arn:aws:states:::lambda:invoke."
            ),
        });
    }

    Ok(())
}

fn validate_state_targets(
    definition: &StateMachineDefinition,
) -> Result<(), StepFunctionsError> {
    for state in definition.states.values() {
        match state {
            State::Pass(state) => validate_target(
                definition,
                state.next.as_deref(),
                "Pass state next",
            )?,
            State::Task(state) => validate_target(
                definition,
                state.next.as_deref(),
                "Task state next",
            )?,
            State::Choice(state) => {
                validate_target(
                    definition,
                    state.default.as_deref(),
                    "Choice state default",
                )?;
                for choice in &state.choices {
                    validate_target(
                        definition,
                        Some(choice.next()),
                        "Choice rule next",
                    )?;
                }
            }
            State::Wait(state) => validate_target(
                definition,
                state.next.as_deref(),
                "Wait state next",
            )?,
            State::Succeed(_) | State::Fail(_) => {}
            State::Parallel(state) => {
                validate_target(
                    definition,
                    state.next.as_deref(),
                    "Parallel state next",
                )?;
            }
            State::Map(state) => validate_target(
                definition,
                state.next.as_deref(),
                "Map state next",
            )?,
        }
    }

    Ok(())
}

fn validate_target(
    definition: &StateMachineDefinition,
    target: Option<&str>,
    label: &str,
) -> Result<(), StepFunctionsError> {
    if let Some(target) = target
        && !definition.states.contains_key(target)
    {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!("{label} references unknown state `{target}`."),
        });
    }

    Ok(())
}

fn evaluate_choice_state(
    state: &ChoiceState,
    input: &Value,
) -> Result<Option<String>, StepFunctionsError> {
    evaluate_choice_rules(&state.choices, state.default.as_deref(), input)
}

fn wait_seconds(
    state: &WaitState,
    input: &Value,
) -> Result<u64, StepFunctionsError> {
    match &state.seconds {
        WaitSeconds::Literal(seconds) => Ok(*seconds),
        WaitSeconds::Path(path) => {
            let value = resolve_required_path(input, path)?;
            value.as_u64().ok_or_else(|| StepFunctionsError::Validation {
                message: format!(
                    "SecondsPath `{path}` did not resolve to a non-negative \
                     integer."
                ),
            })
        }
    }
}

fn map_items(
    items_path: Option<&JsonPath>,
    input: &Value,
) -> Result<Vec<Value>, StepFunctionsError> {
    let value = match items_path {
        Some(path) => resolve_required_path(input, path)?,
        None => input,
    };
    let Some(items) = value.as_array() else {
        return Err(StepFunctionsError::Validation {
            message: "ItemsPath must resolve to an array.".to_owned(),
        });
    };

    Ok(items.clone())
}

fn build_task_request(
    state: &TaskState,
    input: &Value,
) -> Result<TaskInvocationRequest, StepFunctionsError> {
    match &state.resource {
        TaskResource::DirectLambdaArn(resource) => {
            let payload = if let Some(parameters) = &state.parameters {
                resolve_parameters(parameters, input)?
            } else {
                input.clone()
            };
            let target =
                LambdaFunctionTarget::parse(resource).map_err(|error| {
                    StepFunctionsError::Validation {
                        message: format!(
                            "Task resource `{resource}` is not a valid Lambda \
                         target: {error}"
                        ),
                    }
                })?;

            Ok(TaskInvocationRequest {
                function_name: target.to_string(),
                payload,
                resource: resource.clone(),
                resource_type: "lambda".to_owned(),
                target,
            })
        }
        TaskResource::OptimizedLambdaInvoke => {
            let parameters = resolve_parameters(
                optimized_lambda_parameters(state)?,
                input,
            )?;
            let Some(object) = parameters.as_object() else {
                return Err(StepFunctionsError::Validation {
                    message: "Task Parameters must resolve to an object for \
                         arn:aws:states:::lambda:invoke."
                        .to_owned(),
                });
            };
            let function_name = object
                .get("FunctionName")
                .and_then(Value::as_str)
                .ok_or_else(|| StepFunctionsError::Validation {
                    message: "FunctionName must resolve to a string for \
                         arn:aws:states:::lambda:invoke."
                        .to_owned(),
                })?;
            let payload =
                object.get("Payload").cloned().unwrap_or(Value::Null);
            let target = LambdaFunctionTarget::parse(function_name).map_err(
                |error| StepFunctionsError::Validation {
                    message: format!(
                        "FunctionName must resolve to a valid Lambda \
                             target for arn:aws:states:::lambda:invoke: \
                             {error}"
                    ),
                },
            )?;

            Ok(TaskInvocationRequest {
                function_name: target.to_string(),
                payload,
                resource: "arn:aws:states:::lambda:invoke".to_owned(),
                resource_type: "lambda".to_owned(),
                target,
            })
        }
    }
}

fn required_next_state(
    next: Option<&String>,
    state_name: &str,
    state_type: &str,
) -> Result<String, StepFunctionsError> {
    next.cloned().ok_or_else(|| StepFunctionsError::Validation {
        message: format!(
            "{state_type} state `{state_name}` must declare a Next state when End is false."
        ),
    })
}

fn optimized_lambda_parameters(
    state: &TaskState,
) -> Result<&Value, StepFunctionsError> {
    state.parameters.as_ref().ok_or_else(|| StepFunctionsError::Validation {
        message:
            "Task Parameters are required for arn:aws:states:::lambda:invoke."
                .to_owned(),
    })
}

fn task_result_value(
    state: &TaskState,
    invocation: &TaskInvocationResult,
) -> Value {
    match state.resource {
        TaskResource::DirectLambdaArn(_) => invocation.payload.clone(),
        TaskResource::OptimizedLambdaInvoke => {
            let mut result = Map::new();
            result.insert(
                "ExecutedVersion".to_owned(),
                Value::String(invocation.executed_version.clone()),
            );
            result.insert("Payload".to_owned(), invocation.payload.clone());
            result.insert(
                "StatusCode".to_owned(),
                Value::Number(invocation.status_code.into()),
            );
            Value::Object(result)
        }
    }
}

fn apply_input_path(
    input_path: Option<&PathSelection>,
    input: &Value,
) -> Result<Value, StepFunctionsError> {
    match input_path {
        None => Ok(input.clone()),
        Some(PathSelection::Discard) => Ok(Value::Object(Map::new())),
        Some(PathSelection::Path(path)) => {
            Ok(resolve_required_path(input, path)?.clone())
        }
    }
}

fn apply_output_path(
    output_path: Option<&PathSelection>,
    output: &Value,
) -> Result<Value, StepFunctionsError> {
    match output_path {
        None => Ok(output.clone()),
        Some(PathSelection::Discard) => Ok(Value::Object(Map::new())),
        Some(PathSelection::Path(path)) => {
            Ok(resolve_required_path(output, path)?.clone())
        }
    }
}

fn apply_result_path(
    result_path: Option<&ResultPathSelection>,
    input: &Value,
    result: Value,
) -> Result<Value, StepFunctionsError> {
    match result_path {
        None => Ok(result),
        Some(ResultPathSelection::Discard) => Ok(input.clone()),
        Some(ResultPathSelection::Path(path)) => {
            if path.tokens.is_empty() {
                return Ok(result);
            }
            let mut updated = input.clone();
            set_path_value(&mut updated, path, result)?;
            Ok(updated)
        }
    }
}

fn resolve_parameters(
    template: &Value,
    input: &Value,
) -> Result<Value, StepFunctionsError> {
    match template {
        Value::Object(object) => {
            let mut rendered = Map::new();
            for (key, value) in object {
                if let Some(stripped) = key.strip_suffix(".$") {
                    let path = value.as_str().ok_or_else(|| {
                        StepFunctionsError::Validation {
                            message: format!(
                                "Parameter template `{key}` must use a string \
                                 JSONPath value."
                            ),
                        }
                    })?;
                    rendered.insert(
                        stripped.to_owned(),
                        resolve_required_path(input, &parse_json_path(path)?)?
                            .clone(),
                    );
                } else {
                    rendered.insert(
                        key.clone(),
                        resolve_parameters(value, input)?,
                    );
                }
            }
            Ok(Value::Object(rendered))
        }
        Value::Array(array) => Ok(Value::Array(
            array
                .iter()
                .map(|value| resolve_parameters(value, input))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Ok(template.clone()),
    }
}

fn resolve_required_path<'a>(
    input: &'a Value,
    path: &JsonPath,
) -> Result<&'a Value, StepFunctionsError> {
    match resolve_path(input, path) {
        PathLookup::Missing => Err(StepFunctionsError::Validation {
            message: format!("JSONPath `{path}` did not resolve to a value."),
        }),
        PathLookup::Present(value) => Ok(value),
    }
}

pub(crate) fn resolve_path<'a>(
    input: &'a Value,
    path: &JsonPath,
) -> PathLookup<'a> {
    let mut current = input;
    for token in &path.tokens {
        match token {
            PathToken::Field(field) => {
                let Some(next) = current.get(field) else {
                    return PathLookup::Missing;
                };
                current = next;
            }
            PathToken::Index(index) => {
                let Some(array) = current.as_array() else {
                    return PathLookup::Missing;
                };
                let Some(next) = array.get(*index) else {
                    return PathLookup::Missing;
                };
                current = next;
            }
        }
    }

    PathLookup::Present(current)
}

fn set_path_value(
    input: &mut Value,
    path: &JsonPath,
    result: Value,
) -> Result<(), StepFunctionsError> {
    fn set_recursive(
        current: &mut Value,
        tokens: &[PathToken],
        result: Value,
    ) -> Result<(), StepFunctionsError> {
        let Some((head, tail)) = tokens.split_first() else {
            *current = result;
            return Ok(());
        };
        match head {
            PathToken::Field(field) => {
                if !current.is_object() {
                    *current = Value::Object(Map::new());
                }
                let Some(object) = current.as_object_mut() else {
                    return Err(StepFunctionsError::Validation {
                        message:
                            "ResultPath could not initialize an object path."
                                .to_owned(),
                    });
                };
                let entry = object.entry(field.clone()).or_insert(Value::Null);
                set_recursive(entry, tail, result)
            }
            PathToken::Index(index) => {
                let Some(array) = current.as_array_mut() else {
                    return Err(StepFunctionsError::Validation {
                        message:
                            "ResultPath cannot index into a non-array value."
                                .to_owned(),
                    });
                };
                let Some(entry) = array.get_mut(*index) else {
                    return Err(StepFunctionsError::Validation {
                        message: format!(
                            "ResultPath referenced missing array index \
                             {index}."
                        ),
                    });
                };
                set_recursive(entry, tail, result)
            }
        }
    }

    set_recursive(input, &path.tokens, result)
}

fn parse_path_selection(
    value: Option<NullablePath>,
) -> Result<Option<PathSelection>, StepFunctionsError> {
    value
        .map(|value| match value {
            NullablePath::Null => Ok(PathSelection::Discard),
            NullablePath::Path(path) => {
                Ok(PathSelection::Path(parse_json_path(path.as_str())?))
            }
        })
        .transpose()
}

fn parse_result_path_selection(
    value: Option<NullablePath>,
) -> Result<Option<ResultPathSelection>, StepFunctionsError> {
    value
        .map(|value| match value {
            NullablePath::Null => Ok(ResultPathSelection::Discard),
            NullablePath::Path(path) => {
                Ok(ResultPathSelection::Path(parse_json_path(path.as_str())?))
            }
        })
        .transpose()
}

pub(crate) fn parse_json_path(
    path: &str,
) -> Result<JsonPath, StepFunctionsError> {
    let Some(mut rest) = path.strip_prefix('$') else {
        return Err(StepFunctionsError::InvalidDefinition {
            message: format!("Unsupported JSONPath `{path}`."),
        });
    };
    let mut tokens = Vec::new();
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix('.') {
            let end = next.find(['.', '[']).unwrap_or(next.len());
            if end == 0 {
                return Err(StepFunctionsError::InvalidDefinition {
                    message: format!("Unsupported JSONPath `{path}`."),
                });
            }
            let field = &next[..end];
            tokens.push(PathToken::Field(field.to_owned()));
            rest = &next[end..];
            continue;
        }
        if let Some(next) = rest.strip_prefix('[') {
            if let Some(next) = next.strip_prefix('\'') {
                let Some(end) = next.find("']") else {
                    return Err(StepFunctionsError::InvalidDefinition {
                        message: format!("Unsupported JSONPath `{path}`."),
                    });
                };
                tokens.push(PathToken::Field(next[..end].to_owned()));
                rest = &next[end + 2..];
                continue;
            }
            if let Some(next) = next.strip_prefix('"') {
                let Some(end) = next.find("\"]") else {
                    return Err(StepFunctionsError::InvalidDefinition {
                        message: format!("Unsupported JSONPath `{path}`."),
                    });
                };
                tokens.push(PathToken::Field(next[..end].to_owned()));
                rest = &next[end + 2..];
                continue;
            }
            let Some(end) = next.find(']') else {
                return Err(StepFunctionsError::InvalidDefinition {
                    message: format!("Unsupported JSONPath `{path}`."),
                });
            };
            let index = next[..end].parse::<usize>().map_err(|_| {
                StepFunctionsError::InvalidDefinition {
                    message: format!("Unsupported JSONPath `{path}`."),
                }
            })?;
            tokens.push(PathToken::Index(index));
            rest = &next[end + 1..];
            continue;
        }

        return Err(StepFunctionsError::InvalidDefinition {
            message: format!("Unsupported JSONPath `{path}`."),
        });
    }

    Ok(JsonPath { tokens })
}

impl std::fmt::Display for JsonPath {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("$")?;
        for token in &self.tokens {
            match token {
                PathToken::Field(field) => write!(formatter, ".{field}")?,
                PathToken::Index(index) => write!(formatter, "[{index}]")?,
            }
        }

        Ok(())
    }
}

fn validate_name(value: &str, label: &str) -> Result<(), StepFunctionsError> {
    if value.trim().is_empty() {
        return Err(StepFunctionsError::InvalidName {
            message: format!("{label} must not be blank."),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(StepFunctionsError::InvalidName {
            message: format!("{label} must not contain control characters."),
        });
    }

    Ok(())
}

fn parse_execution_input(
    input: Option<&str>,
) -> Result<Value, StepFunctionsError> {
    serde_json::from_str(input.unwrap_or(DEFAULT_INPUT)).map_err(|error| {
        StepFunctionsError::InvalidExecutionInput {
            message: format!("Execution input must be valid JSON: {error}"),
        }
    })
}

fn canonical_json(value: &Value) -> Result<String, StepFunctionsError> {
    serde_json::to_string(value).map_err(|error| {
        StepFunctionsError::Validation {
            message: format!("State data must serialize as JSON: {error}"),
        }
    })
}

fn state_machine_arn(scope: &StepFunctionsScope, name: &str) -> String {
    format!(
        "arn:aws:states:{}:{}:stateMachine:{name}",
        scope.region().as_str(),
        scope.account_id().as_str(),
    )
}

fn execution_arn(
    scope: &StepFunctionsScope,
    state_machine_name: &str,
    name: &str,
) -> String {
    format!(
        "arn:aws:states:{}:{}:execution:{state_machine_name}:{name}",
        scope.region().as_str(),
        scope.account_id().as_str(),
    )
}

fn parse_state_machine_arn(
    scope: &StepFunctionsScope,
    value: &str,
) -> Result<StateMachineStorageKey, StepFunctionsError> {
    let arn = value.parse::<Arn>().map_err(|error| {
        StepFunctionsError::InvalidArn {
            message: format!("Invalid state machine ARN `{value}`: {error}"),
        }
    })?;
    if arn.service() != ServiceName::StepFunctions {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid state machine ARN `{value}`."),
        });
    }
    if arn.region() != Some(scope.region())
        || arn.account_id() != Some(scope.account_id())
    {
        return Err(StepFunctionsError::StateMachineDoesNotExist {
            message: format!("State Machine Does Not Exist: `{value}`"),
        });
    }
    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid state machine ARN `{value}`."),
        });
    };
    let Some(name) = resource.strip_prefix("stateMachine:") else {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid state machine ARN `{value}`."),
        });
    };

    Ok(StateMachineStorageKey::new(scope, name))
}

fn parse_execution_arn(
    scope: &StepFunctionsScope,
    value: &str,
) -> Result<ExecutionStorageKey, StepFunctionsError> {
    let arn = value.parse::<Arn>().map_err(|error| {
        StepFunctionsError::InvalidArn {
            message: format!("Invalid execution ARN `{value}`: {error}"),
        }
    })?;
    if arn.service() != ServiceName::StepFunctions {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid execution ARN `{value}`."),
        });
    }
    if arn.region() != Some(scope.region())
        || arn.account_id() != Some(scope.account_id())
    {
        return Err(StepFunctionsError::ExecutionDoesNotExist {
            message: format!("Execution Does Not Exist: `{value}`"),
        });
    }
    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid execution ARN `{value}`."),
        });
    };
    let Some(resource) = resource.strip_prefix("execution:") else {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid execution ARN `{value}`."),
        });
    };
    let Some((state_machine_name, name)) = resource.split_once(':') else {
        return Err(StepFunctionsError::InvalidArn {
            message: format!("Invalid execution ARN `{value}`."),
        });
    };

    Ok(ExecutionStorageKey::new(scope, state_machine_name, name))
}

fn reject_pagination(
    max_results: Option<u32>,
    next_token: Option<&str>,
) -> Result<(), StepFunctionsError> {
    if max_results.is_some() || next_token.is_some() {
        return Err(StepFunctionsError::Validation {
            message: "Pagination tokens and explicit page sizes are not \
                      supported by the Cloudish Step Functions subset."
                .to_owned(),
        });
    }

    Ok(())
}

fn strip_execution_data(history: &mut [HistoryEvent]) {
    for event in history {
        if let Some(details) = &mut event.execution_started_event_details {
            details.input.clear();
        }
        if let Some(details) = &mut event.execution_succeeded_event_details {
            details.output.clear();
        }
        if let Some(details) = &mut event.state_entered_event_details {
            details.input.clear();
        }
        if let Some(details) = &mut event.state_exited_event_details {
            details.output.clear();
        }
        if let Some(details) = &mut event.task_succeeded_event_details {
            details.output.clear();
        }
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::SystemTime;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug, Default)]
    struct InlineSpawner;

    #[derive(Clone)]
    struct FixedClock(SystemTime);

    impl FixedClock {
        fn new(now: SystemTime) -> Self {
            Self(now)
        }
    }

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    impl StepFunctionsExecutionSpawner for InlineSpawner {
        fn spawn(
            &self,
            _task_name: &str,
            task: Box<dyn FnOnce() + Send>,
        ) -> Result<(), InfrastructureError> {
            task();
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct ThreadSpawner;

    impl StepFunctionsExecutionSpawner for ThreadSpawner {
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

    #[derive(Debug, Default)]
    struct RecordingSleeper {
        sleeps: Mutex<Vec<Duration>>,
    }

    impl RecordingSleeper {
        fn sleeps(&self) -> Vec<Duration> {
            recover(self.sleeps.lock()).clone()
        }
    }

    impl StepFunctionsSleeper for RecordingSleeper {
        fn sleep(
            &self,
            duration: Duration,
        ) -> Result<(), InfrastructureError> {
            recover(self.sleeps.lock()).push(duration);
            Ok(())
        }
    }

    #[derive(Debug, Default, Clone)]
    struct StaticTaskAdapter {
        failures: Arc<Mutex<BTreeMap<String, TaskInvocationFailure>>>,
        invocations: Arc<Mutex<Vec<TaskInvocationRequest>>>,
        responses: Arc<Mutex<BTreeMap<String, TaskInvocationResult>>>,
    }

    impl StaticTaskAdapter {
        fn with_response(self, function_name: &str, payload: Value) -> Self {
            recover(self.responses.lock()).insert(
                function_name.to_owned(),
                TaskInvocationResult {
                    executed_version: "$LATEST".to_owned(),
                    payload,
                    status_code: 200,
                },
            );
            self
        }

        fn with_failure(
            self,
            function_name: &str,
            error: &str,
            cause: &str,
        ) -> Self {
            recover(self.failures.lock()).insert(
                function_name.to_owned(),
                TaskInvocationFailure::new(
                    error,
                    cause,
                    function_name,
                    "lambda",
                ),
            );
            self
        }

        fn invocations(&self) -> Vec<TaskInvocationRequest> {
            recover(self.invocations.lock()).clone()
        }
    }

    impl StepFunctionsTaskAdapter for StaticTaskAdapter {
        fn invoke(
            &self,
            _scope: &StepFunctionsScope,
            request: &TaskInvocationRequest,
        ) -> Result<TaskInvocationResult, TaskInvocationFailure> {
            recover(self.invocations.lock()).push(request.clone());
            if let Some(error) = recover(self.failures.lock())
                .get(&request.function_name)
                .cloned()
            {
                return Err(error);
            }

            recover(self.responses.lock())
                .get(&request.function_name)
                .cloned()
                .ok_or_else(|| {
                    TaskInvocationFailure::new(
                        "Lambda.ResourceNotFoundException",
                        format!(
                            "Lambda function {} was not found.",
                            request.function_name
                        ),
                        request.resource.clone(),
                        request.resource_type.clone(),
                    )
                })
        }
    }

    fn scope() -> StepFunctionsScope {
        StepFunctionsScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn service(
        task_adapter: StaticTaskAdapter,
        sleeper: Arc<RecordingSleeper>,
    ) -> StepFunctionsService {
        let root = std::env::temp_dir().join(format!(
            "cloudish-step-functions-{}",
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let factory =
            StorageFactory::new(StorageConfig::new(root, StorageMode::Memory));

        StepFunctionsService::new(
            &factory,
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(10),
                )),
                execution_spawner: Arc::new(InlineSpawner),
                sleeper,
                task_adapter: Arc::new(task_adapter),
            },
        )
    }

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn step_functions_pass_choice_and_task_states_complete() {
        let sleeper = Arc::new(RecordingSleeper::default());
        let task_adapter = StaticTaskAdapter::default().with_response(
            "arn:aws:lambda:eu-west-2:000000000000:function:echo",
            json!({"handled": true}),
        );
        let service = service(task_adapter.clone(), Arc::clone(&sleeper));
        let definition = json!({
            "StartAt": "Seed",
            "States": {
                "Seed": {
                    "Type": "Pass",
                    "Result": {"invoke": true},
                    "ResultPath": "$.route",
                    "Next": "Route"
                },
                "Route": {
                    "Type": "Choice",
                    "Choices": [{
                        "Variable": "$.route.invoke",
                        "BooleanEquals": true,
                        "Next": "Invoke"
                    }],
                    "Default": "Done"
                },
                "Invoke": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:eu-west-2:000000000000:function:echo",
                    "ResultPath": "$.task",
                    "Next": "Pause"
                },
                "Pause": {
                    "Type": "Wait",
                    "Seconds": 1,
                    "Next": "Done"
                },
                "Done": {
                    "Type": "Succeed"
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &scope(),
                CreateStateMachineInput {
                    definition,
                    name: "demo".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &scope(),
                StartExecutionInput {
                    input: Some(r#"{"hello":"world"}"#.to_owned()),
                    name: Some("run-1".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &scope(),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn.clone(),
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Succeeded);
        assert_eq!(
            described.output,
            Some(
                json!({
                    "hello": "world",
                    "route": {"invoke": true},
                    "task": {"handled": true}
                })
                .to_string()
            )
        );
        let history = service
            .get_execution_history(
                &scope(),
                GetExecutionHistoryInput {
                    execution_arn: started.execution_arn,
                    include_execution_data: Some(true),
                    max_results: None,
                    next_token: None,
                    reverse_order: Some(false),
                },
            )
            .expect("history should load");
        let event_types = history
            .events
            .into_iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>();
        assert_eq!(
            event_types,
            vec![
                "ExecutionStarted",
                "PassStateEntered",
                "PassStateExited",
                "ChoiceStateEntered",
                "ChoiceStateExited",
                "TaskStateEntered",
                "TaskScheduled",
                "TaskSucceeded",
                "TaskStateExited",
                "WaitStateEntered",
                "WaitStateExited",
                "SucceedStateEntered",
                "SucceedStateExited",
                "ExecutionSucceeded",
            ]
        );
        assert_eq!(sleeper.sleeps(), vec![Duration::from_secs(1)]);
        assert_eq!(task_adapter.invocations().len(), 1);
    }

    #[test]
    fn step_functions_parallel_and_map_preserve_result_order() {
        let sleeper = Arc::new(RecordingSleeper::default());
        let task_adapter = StaticTaskAdapter::default()
            .with_response(
                "arn:aws:lambda:eu-west-2:000000000000:function:first",
                json!("first"),
            )
            .with_response(
                "arn:aws:lambda:eu-west-2:000000000000:function:second",
                json!("second"),
            )
            .with_response(
                "arn:aws:lambda:eu-west-2:000000000000:function:item",
                json!({"ok": true}),
            );
        let service = service(task_adapter, sleeper);
        let definition = json!({
            "StartAt": "Parallel",
            "States": {
                "Parallel": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "First",
                            "States": {
                                "First": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:eu-west-2:000000000000:function:first",
                                    "End": true
                                }
                            }
                        },
                        {
                            "StartAt": "Second",
                            "States": {
                                "Second": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:eu-west-2:000000000000:function:second",
                                    "End": true
                                }
                            }
                        }
                    ],
                    "ResultPath": "$.parallel",
                    "Next": "Map"
                },
                "Map": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "Invoke",
                        "States": {
                            "Invoke": {
                                "Type": "Task",
                                "Resource": "arn:aws:lambda:eu-west-2:000000000000:function:item",
                                "End": true
                            }
                        }
                    },
                    "ResultPath": "$.mapped",
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &scope(),
                CreateStateMachineInput {
                    definition,
                    name: "parallel-map".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &scope(),
                StartExecutionInput {
                    input: Some(r#"{"items":[1,2,3]}"#.to_owned()),
                    name: Some("run-2".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &scope(),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");
        assert_eq!(described.status, ExecutionStatus::Succeeded);
        assert_eq!(
            described.output,
            Some(
                json!({
                    "items": [1, 2, 3],
                    "parallel": ["first", "second"],
                    "mapped": [
                        {"ok": true},
                        {"ok": true},
                        {"ok": true}
                    ]
                })
                .to_string()
            )
        );
    }

    #[test]
    fn step_functions_unsupported_fields_fail_validation() {
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": "demo"
                    },
                    "Retry": [{
                        "ErrorEquals": ["States.ALL"]
                    }],
                    "End": true
                }
            }
        })
        .to_string();

        let error = parse_state_machine_definition(&definition)
            .expect_err("unsupported retry should fail");
        assert!(matches!(error, StepFunctionsError::InvalidDefinition { .. }));
    }

    #[test]
    fn step_functions_unsupported_resource_fails_validation() {
        let definition = json!({
            "StartAt": "Send",
            "States": {
                "Send": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "End": true
                }
            }
        })
        .to_string();

        let error = parse_state_machine_definition(&definition)
            .expect_err("unsupported integration should fail");
        assert!(matches!(error, StepFunctionsError::InvalidDefinition { .. }));
    }

    #[test]
    fn step_functions_task_failures_surface_failed_execution() {
        let sleeper = Arc::new(RecordingSleeper::default());
        let task_adapter = StaticTaskAdapter::default().with_failure(
            "arn:aws:lambda:eu-west-2:000000000000:function:explode",
            "Lambda.Unknown",
            "boom",
        );
        let service = service(task_adapter, sleeper);
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:eu-west-2:000000000000:function:explode",
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &scope(),
                CreateStateMachineInput {
                    definition,
                    name: "failing".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &scope(),
                StartExecutionInput {
                    input: None,
                    name: Some("run-3".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &scope(),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Failed);
        assert_eq!(described.error.as_deref(), Some("Lambda.Unknown"));
        assert_eq!(described.cause.as_deref(), Some("boom"));
    }

    #[test]
    fn step_functions_stop_execution_aborts_running_wait() {
        let sleeper = Arc::new(RecordingSleeper::default());
        let service = StepFunctionsService::new(
            &StorageFactory::new(StorageConfig::new(
                std::env::temp_dir().join("cloudish-step-functions-stop"),
                StorageMode::Memory,
            )),
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(20),
                )),
                execution_spawner: Arc::new(ThreadSpawner),
                sleeper,
                task_adapter: Arc::new(StaticTaskAdapter::default()),
            },
        );
        let definition = json!({
            "StartAt": "Wait",
            "States": {
                "Wait": {
                    "Type": "Wait",
                    "Seconds": 1,
                    "End": true
                }
            }
        })
        .to_string();
        let created = service
            .create_state_machine(
                &scope(),
                CreateStateMachineInput {
                    definition,
                    name: "waiter".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &scope(),
                StartExecutionInput {
                    input: None,
                    name: Some("run-4".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let stopped = service
            .stop_execution(
                &scope(),
                StopExecutionInput {
                    cause: Some("manual".to_owned()),
                    error: Some("Abort".to_owned()),
                    execution_arn: started.execution_arn.clone(),
                },
            )
            .expect("stop should succeed");
        assert_eq!(stopped.stop_date, 20);
        thread::sleep(Duration::from_millis(25));
        let described = service
            .describe_execution(
                &scope(),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");
        assert_eq!(described.status, ExecutionStatus::Aborted);
    }
}
