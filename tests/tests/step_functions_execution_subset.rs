#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use tests::common::lambda as lambda_fixture;
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_iam::Client as IamClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime};
use aws_sdk_sfn::Client as SfnClient;
use aws_sdk_sfn::error::ProvideErrorMetadata;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("step_functions_execution_subset");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::time::Duration;

async fn wait_for_execution_completion(
    sfn: &SfnClient,
    execution_arn: &str,
) -> aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput {
    let mut last_described = None;

    for _ in 0..40 {
        let described = sfn
            .describe_execution()
            .execution_arn(execution_arn)
            .send()
            .await
            .expect("execution should describe");
        if described.status().as_str() != "RUNNING" {
            return described;
        }
        last_described = Some(described);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let described = last_described.expect("execution should describe");
    assert_ne!(
        described.status().as_str(),
        "RUNNING",
        "execution did not complete in time",
    );
    std::process::abort();
}

#[tokio::test]
async fn step_functions_execution_subset_lifecycle_history_and_lambda_task_round_trip()
 {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let lambda = LambdaClient::new(&config);
    let sfn = SfnClient::new(&config);

    let role_name = lambda_fixture::unique_name("step-functions-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;
    let function_name = lambda_fixture::unique_name("step-functions-echo");
    let created_function = lambda
        .create_function()
        .function_name(&function_name)
        .role(&role_arn)
        .runtime(Runtime::from("provided.al2"))
        .handler("bootstrap.handler")
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(lambda_fixture::provided_bootstrap_zip()))
                .build(),
        )
        .send()
        .await
        .expect("lambda function should create");
    let function_arn = created_function
        .function_arn()
        .expect("lambda function ARN should be returned")
        .to_owned();

    let definition = serde_json::json!({
        "StartAt": "Seed",
        "States": {
            "Seed": {
                "Type": "Pass",
                "Result": {
                    "invoke": true,
                    "payload": {
                        "message": "hello"
                    }
                },
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
                "Default": "Rejected"
            },
            "Invoke": {
                "Type": "Task",
                "Resource": function_arn,
                "Parameters": {
                    "message.$": "$.route.payload.message"
                },
                "ResultPath": "$.lambda",
                "End": true
            },
            "Rejected": {
                "Type": "Fail",
                "Error": "UnexpectedRoute",
                "Cause": "Choice default selected unexpectedly."
            }
        }
    })
    .to_string();

    let state_machine_name =
        lambda_fixture::unique_name("step-functions-machine");
    let created_state_machine = sfn
        .create_state_machine()
        .name(&state_machine_name)
        .definition(definition)
        .role_arn(&role_arn)
        .send()
        .await
        .expect("state machine should create");
    let state_machine_arn =
        created_state_machine.state_machine_arn().to_owned();

    let described_state_machine = sfn
        .describe_state_machine()
        .state_machine_arn(&state_machine_arn)
        .send()
        .await
        .expect("state machine should describe");
    assert_eq!(described_state_machine.name(), state_machine_name.as_str());

    let listed_state_machines = sfn
        .list_state_machines()
        .send()
        .await
        .expect("state machines should list");
    assert!(listed_state_machines.state_machines().iter().any(
        |state_machine| {
            state_machine.name() == state_machine_name.as_str()
        }
    ));

    let started_execution = sfn
        .start_execution()
        .state_machine_arn(&state_machine_arn)
        .name("run-1")
        .input("{}")
        .send()
        .await
        .expect("execution should start");
    let execution_arn = started_execution.execution_arn().to_owned();

    let described_execution =
        wait_for_execution_completion(&sfn, &execution_arn).await;
    let expected_output = serde_json::json!({
        "route": {
            "invoke": true,
            "payload": {
                "message": "hello"
            }
        },
        "lambda": {
            "message": "hello"
        }
    })
    .to_string();
    assert_eq!(described_execution.status().as_str(), "SUCCEEDED");
    assert_eq!(described_execution.output(), Some(expected_output.as_str()));

    let listed_executions = sfn
        .list_executions()
        .state_machine_arn(&state_machine_arn)
        .send()
        .await
        .expect("executions should list");
    assert!(listed_executions.executions().iter().any(|execution| {
        execution.execution_arn() == execution_arn.as_str()
    }));

    let history = sfn
        .get_execution_history()
        .execution_arn(&execution_arn)
        .include_execution_data(true)
        .send()
        .await
        .expect("execution history should load");
    let event_types = history
        .events()
        .iter()
        .map(|event| event.r#type().as_str())
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
            "ExecutionSucceeded",
        ]
    );

    sfn.delete_state_machine()
        .state_machine_arn(&state_machine_arn)
        .send()
        .await
        .expect("state machine should delete");
}

#[tokio::test]
async fn step_functions_execution_subset_rejects_retry_during_validation() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let sfn = SfnClient::new(&config);

    let role_name = lambda_fixture::unique_name("step-functions-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;
    let definition = serde_json::json!({
        "StartAt": "Invoke",
        "States": {
            "Invoke": {
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

    let error = sfn
        .create_state_machine()
        .name(lambda_fixture::unique_name("step-functions-invalid"))
        .definition(definition)
        .role_arn(role_arn)
        .send()
        .await
        .expect_err("unsupported Retry should fail");

    assert_eq!(error.code(), Some("InvalidDefinition"));
    assert!(error.message().is_some_and(|message| message.contains("Retry")));
}
