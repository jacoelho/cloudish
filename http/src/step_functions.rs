pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, RequestContext};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use services::{
    CreateStateMachineInput, DescribeExecutionInput, GetExecutionHistoryInput,
    ListExecutionsInput, ListStateMachinesInput, StartExecutionInput,
    StepFunctionsError, StepFunctionsScope, StepFunctionsService,
    StopExecutionInput,
};

pub(crate) fn handle_json(
    step_functions: &StepFunctionsService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let scope = StepFunctionsScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match operation {
        StepFunctionsOperation::CreateStateMachine => json_response(
            &step_functions
                .create_state_machine(
                    &scope,
                    parse_json_body::<CreateStateMachineRequest>(
                        request.body(),
                    )?
                    .into_input(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::DescribeStateMachine => json_response(
            &step_functions
                .describe_state_machine(
                    &scope,
                    parse_json_body::<StateMachineArnRequest>(request.body())?
                        .state_machine_arn
                        .as_str(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::ListStateMachines => json_response(
            &step_functions
                .list_state_machines(
                    &scope,
                    parse_json_body::<ListStateMachinesRequest>(
                        request.body(),
                    )?
                    .into_input(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::DeleteStateMachine => {
            step_functions
                .delete_state_machine(
                    &scope,
                    parse_json_body::<StateMachineArnRequest>(request.body())?
                        .state_machine_arn
                        .as_str(),
                )
                .map_err(|error| error.to_aws_error())?;
            json_response(&serde_json::json!({}))
        }
        StepFunctionsOperation::StartExecution => json_response(
            &step_functions
                .start_execution(
                    &scope,
                    parse_json_body::<StartExecutionRequest>(request.body())?
                        .into_input(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::DescribeExecution => json_response(
            &step_functions
                .describe_execution(
                    &scope,
                    DescribeExecutionInput {
                        execution_arn: parse_json_body::<ExecutionArnRequest>(
                            request.body(),
                        )?
                        .execution_arn,
                    },
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::ListExecutions => json_response(
            &step_functions
                .list_executions(
                    &scope,
                    parse_json_body::<ListExecutionsRequest>(request.body())?
                        .into_input(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::StopExecution => json_response(
            &step_functions
                .stop_execution(
                    &scope,
                    parse_json_body::<StopExecutionRequest>(request.body())?
                        .into_input(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        StepFunctionsOperation::GetExecutionHistory => json_response(
            &step_functions
                .get_execution_history(
                    &scope,
                    parse_json_body::<GetExecutionHistoryRequest>(
                        request.body(),
                    )?
                    .into_input(),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepFunctionsOperation {
    CreateStateMachine,
    DeleteStateMachine,
    DescribeExecution,
    DescribeStateMachine,
    GetExecutionHistory,
    ListExecutions,
    ListStateMachines,
    StartExecution,
    StopExecution,
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<StepFunctionsOperation, StepFunctionsError> {
    let target = target.ok_or_else(|| StepFunctionsError::Validation {
        message: "missing X-Amz-Target header".to_owned(),
    })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(StepFunctionsError::Validation {
            message: format!("unsupported Step Functions target `{target}`."),
        });
    };
    if prefix != "AWSStepFunctions" {
        return Err(StepFunctionsError::Validation {
            message: format!("unsupported Step Functions target `{target}`."),
        });
    }

    match operation {
        "CreateStateMachine" => Ok(StepFunctionsOperation::CreateStateMachine),
        "DeleteStateMachine" => Ok(StepFunctionsOperation::DeleteStateMachine),
        "DescribeExecution" => Ok(StepFunctionsOperation::DescribeExecution),
        "DescribeStateMachine" => {
            Ok(StepFunctionsOperation::DescribeStateMachine)
        }
        "GetExecutionHistory" => {
            Ok(StepFunctionsOperation::GetExecutionHistory)
        }
        "ListExecutions" => Ok(StepFunctionsOperation::ListExecutions),
        "ListStateMachines" => Ok(StepFunctionsOperation::ListStateMachines),
        "StartExecution" => Ok(StepFunctionsOperation::StartExecution),
        "StopExecution" => Ok(StepFunctionsOperation::StopExecution),
        _ => Err(StepFunctionsError::Validation {
            message: format!(
                "unsupported Step Functions operation `{operation}`."
            ),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(body).map_err(|error| {
        StepFunctionsError::Validation {
            message: format!("request body is not valid JSON: {error}"),
        }
        .to_aws_error()
    })
}

fn json_response<T>(value: &T) -> Result<Vec<u8>, AwsError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|error| {
        StepFunctionsError::Validation {
            message: format!(
                "failed to serialize Step Functions response: {error}"
            ),
        }
        .to_aws_error()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateStateMachineRequest {
    definition: String,
    name: String,
    role_arn: String,
    #[serde(rename = "type")]
    state_machine_type: Option<String>,
}

impl CreateStateMachineRequest {
    fn into_input(self) -> CreateStateMachineInput {
        CreateStateMachineInput {
            definition: self.definition,
            name: self.name,
            role_arn: self.role_arn,
            state_machine_type: self.state_machine_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListStateMachinesRequest {
    max_results: Option<u32>,
    next_token: Option<String>,
}

impl ListStateMachinesRequest {
    fn into_input(self) -> ListStateMachinesInput {
        ListStateMachinesInput {
            max_results: self.max_results,
            next_token: self.next_token,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateMachineArnRequest {
    state_machine_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartExecutionRequest {
    input: Option<String>,
    name: Option<String>,
    state_machine_arn: String,
    trace_header: Option<String>,
}

impl StartExecutionRequest {
    fn into_input(self) -> StartExecutionInput {
        StartExecutionInput {
            input: self.input,
            name: self.name,
            state_machine_arn: self.state_machine_arn,
            trace_header: self.trace_header,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecutionArnRequest {
    execution_arn: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListExecutionsRequest {
    max_results: Option<u32>,
    next_token: Option<String>,
    state_machine_arn: String,
    status_filter: Option<String>,
}

impl ListExecutionsRequest {
    fn into_input(self) -> ListExecutionsInput {
        ListExecutionsInput {
            max_results: self.max_results,
            next_token: self.next_token,
            state_machine_arn: self.state_machine_arn,
            status_filter: self.status_filter,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StopExecutionRequest {
    cause: Option<String>,
    error: Option<String>,
    execution_arn: String,
}

impl StopExecutionRequest {
    fn into_input(self) -> StopExecutionInput {
        StopExecutionInput {
            cause: self.cause,
            error: self.error,
            execution_arn: self.execution_arn,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GetExecutionHistoryRequest {
    execution_arn: String,
    include_execution_data: Option<bool>,
    max_results: Option<u32>,
    next_token: Option<String>,
    reverse_order: Option<bool>,
}

impl GetExecutionHistoryRequest {
    fn into_input(self) -> GetExecutionHistoryInput {
        GetExecutionHistoryInput {
            execution_arn: self.execution_arn,
            include_execution_data: self.include_execution_data,
            max_results: self.max_results,
            next_token: self.next_token,
            reverse_order: self.reverse_order,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_runtime::FixedClock;
    use aws::{
        AccountId, ProtocolFamily, RegionId, RequestContext, ServiceName,
    };
    use serde_json::json;
    use services::{
        StepFunctionsServiceDependencies, ThreadStepFunctionsSleeper,
    };
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};
    use step_functions::StepFunctionsSpawnHandle;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug, Default)]
    struct InlineSpawner;

    impl services::StepFunctionsExecutionSpawner for InlineSpawner {
        fn spawn_paused(
            &self,
            _task_name: &str,
            task: Box<dyn FnOnce() + Send>,
        ) -> Result<
            Box<dyn StepFunctionsSpawnHandle>,
            services::InfrastructureError,
        > {
            Ok(Box::new(InlineSpawnHandle { task: Some(task) }))
        }
    }

    #[derive(Default)]
    struct InlineSpawnHandle {
        task: Option<Box<dyn FnOnce() + Send>>,
    }

    impl StepFunctionsSpawnHandle for InlineSpawnHandle {
        fn start(mut self: Box<Self>) {
            if let Some(task) = self.task.take() {
                task();
            }
        }
    }

    #[derive(Debug, Default, Clone)]
    struct NullTaskAdapter;

    impl services::StepFunctionsTaskAdapter for NullTaskAdapter {
        fn invoke(
            &self,
            _scope: &StepFunctionsScope,
            request: &services::TaskInvocationRequest,
        ) -> Result<
            services::TaskInvocationResult,
            services::TaskInvocationFailure,
        > {
            Ok(services::TaskInvocationResult::new(
                "$LATEST",
                serde_json::json!({ "echo": request.payload() }),
                200,
            ))
        }
    }

    fn context() -> RequestContext {
        RequestContext::try_new(
            "000000000000"
                .parse::<AccountId>()
                .expect("account id should parse"),
            "eu-west-2".parse::<RegionId>().expect("region should parse"),
            ServiceName::StepFunctions,
            ProtocolFamily::AwsJson10,
            "CreateStateMachine",
            None,
            false,
        )
        .expect("context should build")
    }

    fn service() -> StepFunctionsService {
        let factory = StorageFactory::new(StorageConfig::new(
            std::env::temp_dir().join("cloudish-http-step-functions"),
            StorageMode::Memory,
        ));

        StepFunctionsService::new(
            &factory,
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(30),
                )),
                execution_spawner: Arc::new(InlineSpawner),
                sleeper: Arc::new(ThreadStepFunctionsSleeper),
                task_adapter: Arc::new(NullTaskAdapter),
            },
        )
    }

    #[test]
    fn step_functions_http_round_trips_control_plane_and_history() {
        let service = service();
        let definition = json!({
            "StartAt": "Pass",
            "States": {
                "Pass": {
                    "Type": "Pass",
                    "Result": {"ok": true},
                    "End": true
                }
            }
        })
        .to_string();
        let create_body = json!({
            "definition": definition,
            "name": "demo",
            "roleArn": "arn:aws:iam::000000000000:role/demo"
        })
        .to_string();
        let create_request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: \
             AWSStepFunctions.CreateStateMachine\r\nContent-Type: \
             application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{}",
            create_body.len(),
            create_body
        );
        let create_http = HttpRequest::parse(create_request.as_bytes())
            .expect("request should parse");
        let created = handle_json(&service, &create_http, &context())
            .expect("create should succeed");
        let created: serde_json::Value =
            serde_json::from_slice(&created).expect("response should decode");
        let state_machine_arn = created["stateMachineArn"]
            .as_str()
            .expect("create should include arn");

        let start_body = json!({
            "name": "run-1",
            "stateMachineArn": state_machine_arn
        })
        .to_string();
        let start_request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: \
             AWSStepFunctions.StartExecution\r\nContent-Type: \
             application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{}",
            start_body.len(),
            start_body
        );
        let start_http = HttpRequest::parse(start_request.as_bytes())
            .expect("request should parse");
        let started = handle_json(&service, &start_http, &context())
            .expect("start should succeed");
        let started: serde_json::Value =
            serde_json::from_slice(&started).expect("response should decode");
        let execution_arn = started["executionArn"]
            .as_str()
            .expect("start should include arn");

        let history_body =
            json!({ "executionArn": execution_arn }).to_string();
        let history_request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: \
             AWSStepFunctions.GetExecutionHistory\r\nContent-Type: \
             application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{}",
            history_body.len(),
            history_body
        );
        let history_http = HttpRequest::parse(history_request.as_bytes())
            .expect("request should parse");
        let history = handle_json(&service, &history_http, &context())
            .expect("history should succeed");
        let history: serde_json::Value =
            serde_json::from_slice(&history).expect("response should decode");
        assert_eq!(
            history["events"]
                .as_array()
                .expect("events should be an array")
                .iter()
                .map(|event| {
                    event["type"]
                        .as_str()
                        .expect("history events should include a type")
                })
                .collect::<Vec<_>>(),
            vec![
                "ExecutionStarted",
                "PassStateEntered",
                "PassStateExited",
                "ExecutionSucceeded",
            ]
        );
    }

    #[test]
    fn step_functions_http_rejects_unknown_target() {
        let error = operation_from_target(Some("AWSStepFunctions.Unknown"))
            .expect_err("unknown target should fail");
        assert!(matches!(error, StepFunctionsError::Validation { .. }));
    }
}
