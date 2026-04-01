#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::lambda as lambda_fixture;
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_iam::Client as IamClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    DeadLetterConfig, DestinationConfig, FunctionCode, FunctionUrlAuthType,
    InvocationType, OnFailure, Runtime,
};
use aws_sdk_sqs::Client as SqsClient;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::time::Duration;

#[tokio::test]
async fn lambda_url_control_plane_async_destinations_and_sqs_mapping_round_trip()
 {
    let runtime = RuntimeServer::spawn("sdk-lambda-url").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let lambda = LambdaClient::new(&config);
    let sqs = SqsClient::new(&config);
    let role_name = lambda_fixture::unique_name("lambda-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;

    let url_function_name = lambda_fixture::unique_name("lambda-url");
    let created = lambda
        .create_function()
        .function_name(&url_function_name)
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
        .expect("URL function should be created");
    assert_eq!(created.function_name(), Some(url_function_name.as_str()));

    let url_config = lambda
        .create_function_url_config()
        .function_name(&url_function_name)
        .auth_type(FunctionUrlAuthType::None)
        .send()
        .await
        .expect("function URL config should be created");
    assert!(
        url_config.function_url().contains("/__aws/lambda-url/eu-west-2/")
    );
    assert_eq!(url_config.auth_type().as_str(), "NONE");
    assert_eq!(
        url_config
            .invoke_mode()
            .expect("invoke mode should be present")
            .as_str(),
        "BUFFERED"
    );

    lambda
        .add_permission()
        .function_name(&url_function_name)
        .action("lambda:InvokeFunctionUrl")
        .function_url_auth_type(FunctionUrlAuthType::None)
        .principal("*")
        .statement_id("allow-public")
        .send()
        .await
        .expect("function URL permission should be created");
    let listed_urls = lambda
        .list_function_url_configs()
        .function_name(&url_function_name)
        .send()
        .await
        .expect("function URLs should list");
    assert_eq!(listed_urls.function_url_configs().len(), 1);
    let fetched_url = lambda
        .get_function_url_config()
        .function_name(&url_function_name)
        .send()
        .await
        .expect("function URL config should fetch");
    assert_eq!(fetched_url.function_url(), url_config.function_url());

    let http_response = reqwest::Client::new()
        .post(url_config.function_url())
        .query(&[("mode", "test")])
        .header("Cookie", "theme=light")
        .body(r#"{"hello":"world"}"#)
        .send()
        .await
        .expect("function URL invoke should succeed");
    assert_eq!(http_response.status(), reqwest::StatusCode::OK);
    let event: serde_json::Value = serde_json::from_str(
        &http_response.text().await.expect("response body should be readable"),
    )
    .expect("response should be JSON");
    assert_eq!(event["version"], "2.0");
    assert_eq!(event["rawQueryString"], "mode=test");
    assert_eq!(event["cookies"][0], "theme=light");
    assert_eq!(event["requestContext"]["http"]["sourceIp"], "127.0.0.1");

    let failure_queue_name = lambda_fixture::unique_name("lambda-failure");
    let failure_queue = sqs
        .create_queue()
        .queue_name(&failure_queue_name)
        .send()
        .await
        .expect("failure queue should be created");
    let failure_queue_url = failure_queue
        .queue_url()
        .expect("failure queue URL should be returned")
        .to_owned();
    let failure_queue_arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{failure_queue_name}");

    let dlq_name = lambda_fixture::unique_name("lambda-dlq");
    let dlq = sqs
        .create_queue()
        .queue_name(&dlq_name)
        .send()
        .await
        .expect("dead-letter queue should be created");
    let dlq_url = dlq
        .queue_url()
        .expect("dead-letter queue URL should be returned")
        .to_owned();
    let dlq_arn = format!("arn:aws:sqs:eu-west-2:000000000000:{dlq_name}");

    let failing_function_name = lambda_fixture::unique_name("lambda-failing");
    lambda
        .create_function()
        .function_name(&failing_function_name)
        .role(&role_arn)
        .runtime(Runtime::from("provided.al2"))
        .handler("bootstrap.handler")
        .dead_letter_config(
            DeadLetterConfig::builder().target_arn(&dlq_arn).build(),
        )
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(lambda_fixture::failing_bootstrap_zip()))
                .build(),
        )
        .send()
        .await
        .expect("failing function should be created");
    lambda
        .put_function_event_invoke_config()
        .function_name(&failing_function_name)
        .destination_config(
            DestinationConfig::builder()
                .on_failure(
                    OnFailure::builder()
                        .destination(&failure_queue_arn)
                        .build(),
                )
                .build(),
        )
        .maximum_retry_attempts(0)
        .send()
        .await
        .expect("event invoke config should be stored");
    lambda
        .invoke()
        .function_name(&failing_function_name)
        .invocation_type(InvocationType::from("Event"))
        .payload(Blob::new(br#"{"job":"fail"}"#.to_vec()))
        .send()
        .await
        .expect("async invoke should be accepted");

    let failure_message =
        wait_for_message(&sqs, &failure_queue_url, "failure destination")
            .await;
    let failure_body = failure_message
        .body()
        .expect("failure destination should include a body");
    let failure_event: serde_json::Value = serde_json::from_str(failure_body)
        .expect("failure body should be JSON");
    assert_eq!(
        failure_event["requestContext"]["condition"],
        "RetriesExhausted"
    );
    let dead_letter_message =
        wait_for_message(&sqs, &dlq_url, "dead-letter queue").await;
    assert_eq!(
        dead_letter_message
            .body()
            .expect("dead-letter queue should include a body"),
        r#"{"job":"fail"}"#
    );

    let source_queue_name = lambda_fixture::unique_name("lambda-source");
    let source_queue = sqs
        .create_queue()
        .queue_name(&source_queue_name)
        .send()
        .await
        .expect("source queue should be created");
    let source_queue_url = source_queue
        .queue_url()
        .expect("source queue URL should be returned")
        .to_owned();
    let source_queue_arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{source_queue_name}");
    let mapping = lambda
        .create_event_source_mapping()
        .batch_size(5)
        .enabled(true)
        .event_source_arn(&source_queue_arn)
        .function_name(&url_function_name)
        .maximum_batching_window_in_seconds(0)
        .send()
        .await
        .expect("event source mapping should be created");
    assert_eq!(mapping.state(), Some("Creating"));

    sqs.send_message()
        .queue_url(&source_queue_url)
        .message_body(r#"{"job":"process"}"#)
        .send()
        .await
        .expect("source queue message should be sent");
    wait_for_queue_empty(&sqs, &source_queue_url).await;

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

async fn wait_for_message(
    sqs: &SqsClient,
    queue_url: &str,
    label: &str,
) -> aws_sdk_sqs::types::Message {
    for _ in 0..40 {
        let response = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(0)
            .visibility_timeout(0)
            .send()
            .await
            .expect("receive message request should succeed");
        if let Some(message) = response.messages().first() {
            return message.clone();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let response = sqs
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(0)
        .visibility_timeout(0)
        .send()
        .await
        .expect("receive message request should succeed");
    assert!(
        !response.messages().is_empty(),
        "{label} should receive a message"
    );
    response
        .messages()
        .first()
        .cloned()
        .expect("message should exist after the final receive")
}

async fn wait_for_queue_empty(sqs: &SqsClient, queue_url: &str) {
    for _ in 0..40 {
        let response = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(0)
            .visibility_timeout(0)
            .send()
            .await
            .expect("receive message request should succeed");
        if response.messages().is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let response = sqs
        .receive_message()
        .queue_url(queue_url)
        .max_number_of_messages(1)
        .wait_time_seconds(0)
        .visibility_timeout(0)
        .send()
        .await
        .expect("receive message request should succeed");
    assert!(
        response.messages().is_empty(),
        "source queue should be drained by the event source mapping",
    );
}
