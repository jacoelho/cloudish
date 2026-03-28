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
#[path = "common/lambda.rs"]
mod lambda_fixture;
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_iam::Client as IamClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::error::ProvideErrorMetadata;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, InvocationType, Runtime};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

#[tokio::test]
async fn lambda_core_function_lifecycle_and_invoke_modes_round_trip() {
    let runtime = RuntimeServer::spawn("sdk-lambda-core").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let lambda = LambdaClient::new(&config);
    let role_name = lambda_fixture::unique_name("lambda-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;
    let function_name = lambda_fixture::unique_name("demo");
    let archive = lambda_fixture::provided_bootstrap_zip();

    let created = lambda
        .create_function()
        .function_name(&function_name)
        .role(&role_arn)
        .runtime(Runtime::from("provided.al2"))
        .handler("bootstrap.handler")
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(archive.clone()))
                .build(),
        )
        .send()
        .await
        .expect("function should be created");
    assert_eq!(created.function_name(), Some(function_name.as_str()));
    assert_eq!(created.version(), Some("$LATEST"));

    let fetched = lambda
        .get_function()
        .function_name(&function_name)
        .send()
        .await
        .expect("function should fetch");
    assert_eq!(
        fetched
            .configuration()
            .and_then(|configuration| configuration.function_name()),
        Some(function_name.as_str())
    );

    let listed =
        lambda.list_functions().send().await.expect("functions should list");
    assert!(listed.functions().iter().any(|configuration| {
        configuration.function_name() == Some(function_name.as_str())
    }));

    let updated = lambda
        .update_function_code()
        .function_name(&function_name)
        .publish(true)
        .zip_file(Blob::new(archive))
        .send()
        .await
        .expect("function code should update");
    assert_eq!(updated.version(), Some("1"));

    let alias = lambda
        .create_alias()
        .function_name(&function_name)
        .name("live")
        .function_version("1")
        .send()
        .await
        .expect("alias should be created");
    assert_eq!(alias.function_version(), Some("1"));

    let versions = lambda
        .list_versions_by_function()
        .function_name(&function_name)
        .send()
        .await
        .expect("versions should list");
    assert!(
        versions
            .versions()
            .iter()
            .any(|configuration| configuration.version() == Some("$LATEST"))
    );
    assert!(
        versions
            .versions()
            .iter()
            .any(|configuration| configuration.version() == Some("1"))
    );

    let aliases = lambda
        .list_aliases()
        .function_name(&function_name)
        .send()
        .await
        .expect("aliases should list");
    assert!(
        aliases
            .aliases()
            .iter()
            .any(|configuration| configuration.name() == Some("live"))
    );

    let dry_run = lambda
        .invoke()
        .function_name(&function_name)
        .qualifier("live")
        .invocation_type(InvocationType::from("DryRun"))
        .payload(Blob::new(br#"{"noop":true}"#.to_vec()))
        .send()
        .await
        .expect("dry run should succeed");
    assert_eq!(dry_run.status_code(), 204);
    assert_eq!(dry_run.executed_version(), Some("1"));
    assert!(
        dry_run
            .payload()
            .map(|payload| payload.as_ref().is_empty())
            .unwrap_or(true)
    );

    let response = lambda
        .invoke()
        .function_name(&function_name)
        .qualifier("live")
        .invocation_type(InvocationType::from("RequestResponse"))
        .payload(Blob::new(br#"{"hello":"world"}"#.to_vec()))
        .send()
        .await
        .expect("request-response invoke should succeed");
    assert_eq!(response.status_code(), 200);
    assert_eq!(response.executed_version(), Some("1"));
    assert_eq!(
        response.payload().map(|payload| payload.as_ref().to_vec()),
        Some(br#"{"hello":"world"}"#.to_vec())
    );

    lambda
        .delete_function()
        .function_name(&function_name)
        .send()
        .await
        .expect("function should delete");
    let missing = lambda
        .get_function()
        .function_name(&function_name)
        .send()
        .await
        .expect_err("deleted function should fail to fetch");
    assert_eq!(missing.code(), Some("ResourceNotFoundException"));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

#[tokio::test]
async fn lambda_core_missing_qualifier_surfaces_explicit_error() {
    let runtime = RuntimeServer::spawn("sdk-lambda-core-missing").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let lambda = LambdaClient::new(&config);
    let role_name = lambda_fixture::unique_name("lambda-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;
    let function_name = lambda_fixture::unique_name("demo");

    lambda
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
        .expect("function should be created");

    let error = lambda
        .invoke()
        .function_name(&function_name)
        .qualifier("missing")
        .invocation_type(InvocationType::from("RequestResponse"))
        .payload(Blob::new(br#"{"hello":"world"}"#.to_vec()))
        .send()
        .await
        .expect_err("missing qualifier should fail");
    assert_eq!(error.code(), Some("ResourceNotFoundException"));
    assert!(
        error
            .message()
            .is_some_and(|message| message.contains("Function not found"))
    );

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}
