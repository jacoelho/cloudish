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
use aws_sdk_lambda::types::{
    DestinationConfig, FunctionCode, OnSuccess, Runtime,
};
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sns::types::{
    MessageAttributeValue as SnsMessageAttributeValue,
    PublishBatchRequestEntry,
};
use aws_sdk_sqs::Client as SqsClient;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("sns_delivery");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

struct CapturedHttpRequest {
    body: String,
    headers: Vec<(String, String)>,
}

struct CaptureHttpServer {
    address: SocketAddr,
    handle: Option<JoinHandle<()>>,
    request_rx: mpsc::Receiver<Vec<u8>>,
}

impl CaptureHttpServer {
    fn spawn(expected_requests: usize) -> Self {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("fixture should bind");
        let address =
            listener.local_addr().expect("fixture should expose its address");
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener
                    .accept()
                    .expect("fixture should accept an HTTP request");
                let mut request = Vec::new();
                stream
                    .read_to_end(&mut request)
                    .expect("fixture request should be readable");
                request_tx
                    .send(request)
                    .expect("fixture should forward the request");
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .expect("fixture response should be writable");
            }
        });

        Self { address, handle: Some(handle), request_rx }
    }

    fn endpoint_url(&self) -> String {
        format!("http://{}/subscription", self.address)
    }

    fn next_request(&self, label: &str) -> CapturedHttpRequest {
        let request =
            self.request_rx.recv_timeout(Duration::from_secs(2)).expect(label);
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("fixture request should contain headers");
        let headers = std::str::from_utf8(
            request
                .get(..header_end)
                .expect("fixture request should contain header bytes"),
        )
        .expect("fixture request headers should be UTF-8");
        let mut lines = headers.split("\r\n");
        let _request_line =
            lines.next().expect("fixture request line should exist");
        let parsed_headers = lines
            .map(|line| {
                let (name, value) =
                    line.split_once(':').expect("header should contain ':'");
                (name.to_owned(), value.trim().to_owned())
            })
            .collect();
        let body_start = request
            .get(header_end + 4..)
            .expect("fixture request should contain a body");

        CapturedHttpRequest {
            body: String::from_utf8(body_start.to_vec())
                .expect("fixture body should be UTF-8"),
            headers: parsed_headers,
        }
    }

    fn assert_no_more_requests(&self) {
        assert!(
            self.request_rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "fixture should not receive another request"
        );
    }

    fn join(mut self) {
        self.handle
            .take()
            .expect("fixture should retain its worker")
            .join()
            .expect("fixture worker should finish");
    }
}

#[tokio::test]
async fn sns_delivery_fanout_and_filter_policies_across_sqs_lambda_and_http() {
    let runtime = shared_runtime().await;
    let callback = CaptureHttpServer::spawn(2);
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let iam = IamClient::new(&config);
    let lambda = LambdaClient::new(&config);
    let sns = SnsClient::new(&config);
    let sqs = SqsClient::new(&config);
    let role_name = lambda_fixture::unique_name("lambda-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;

    let success_queue_name = lambda_fixture::unique_name("lambda-success");
    let success_queue = sqs
        .create_queue()
        .queue_name(&success_queue_name)
        .send()
        .await
        .expect("success queue should be created");
    let success_queue_url = success_queue
        .queue_url()
        .expect("success queue URL should be returned")
        .to_owned();
    let success_queue_arn =
        format!("arn:aws:sqs:eu-west-2:000000000000:{success_queue_name}");

    let function_name = lambda_fixture::unique_name("sns-processor");
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
        .expect("lambda function should be created");
    lambda
        .put_function_event_invoke_config()
        .function_name(&function_name)
        .destination_config(
            DestinationConfig::builder()
                .on_success(
                    OnSuccess::builder()
                        .destination(&success_queue_arn)
                        .build(),
                )
                .build(),
        )
        .maximum_retry_attempts(0)
        .send()
        .await
        .expect("lambda success destination should be stored");

    let created = sns
        .create_topic()
        .name("orders")
        .send()
        .await
        .expect("topic should be created");
    let topic_arn =
        created.topic_arn().expect("topic ARN should be returned").to_owned();

    let queue_name = lambda_fixture::unique_name("orders-queue");
    let queue = sqs
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("sqs subscription queue should be created");
    let queue_url =
        queue.queue_url().expect("queue URL should be returned").to_owned();
    let queue_arn = format!("arn:aws:sqs:eu-west-2:000000000000:{queue_name}");

    let queue_subscription = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .return_subscription_arn(true)
        .attributes("RawMessageDelivery", "true")
        .attributes("FilterPolicy", r#"{"store":["eu-west"]}"#)
        .send()
        .await
        .expect("sqs subscription should be created");
    let queue_subscription_arn = queue_subscription
        .subscription_arn()
        .expect("queue subscription ARN should be returned")
        .to_owned();
    let queue_attributes = sns
        .get_subscription_attributes()
        .subscription_arn(&queue_subscription_arn)
        .send()
        .await
        .expect("queue subscription attributes should load");
    assert_eq!(
        queue_attributes
            .attributes()
            .expect("queue subscription attributes should exist")
            .get("RawMessageDelivery"),
        Some(&"true".to_owned())
    );

    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("lambda")
        .endpoint(format!(
            "arn:aws:lambda:eu-west-2:000000000000:function:{function_name}"
        ))
        .attributes(
            "FilterPolicy",
            r#"{"detail":{"kind":[{"prefix":"order-"}]}}"#,
        )
        .attributes("FilterPolicyScope", "MessageBody")
        .send()
        .await
        .expect("lambda subscription should be created");

    let http_subscription = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("http")
        .endpoint(callback.endpoint_url())
        .return_subscription_arn(true)
        .attributes("FilterPolicy", r#"{"store":["eu-west"]}"#)
        .send()
        .await
        .expect("http subscription should be created");
    let pending_http_subscription_arn = http_subscription
        .subscription_arn()
        .expect("pending http subscription ARN should be returned")
        .to_owned();
    let confirmation_request =
        callback.next_request("subscription confirmation");
    let confirmation: serde_json::Value =
        serde_json::from_str(&confirmation_request.body)
            .expect("confirmation should be JSON");
    let token = confirmation
        .get("Token")
        .and_then(serde_json::Value::as_str)
        .expect("confirmation token should exist");
    let confirmed = sns
        .confirm_subscription()
        .topic_arn(&topic_arn)
        .token(token)
        .send()
        .await
        .expect("http subscription should confirm");
    assert_eq!(
        confirmed.subscription_arn(),
        Some(pending_http_subscription_arn.as_str())
    );
    assert!(confirmation_request.headers.contains(&(
        "x-amz-sns-message-type".to_owned(),
        "SubscriptionConfirmation".to_owned(),
    )));
    assert!(
        confirmation_request
            .headers
            .contains(&("x-amz-sns-topic-arn".to_owned(), topic_arn.clone(),))
    );
    assert!(confirmation_request.headers.contains(&(
        "Content-Type".to_owned(),
        "text/plain; charset=UTF-8".to_owned(),
    )));

    let matching_message = r#"{"detail":{"kind":"order-created"}}"#;
    sns.publish()
        .topic_arn(&topic_arn)
        .message(matching_message)
        .message_attributes(
            "store",
            SnsMessageAttributeValue::builder()
                .data_type("String")
                .string_value("eu-west")
                .build()
                .expect("message attribute should build"),
        )
        .send()
        .await
        .expect("matching publish should succeed");

    let queue_message =
        wait_for_message(&sqs, &queue_url, "sns sqs delivery").await;
    assert_eq!(
        queue_message.body().expect("queue delivery should contain a body"),
        matching_message
    );

    let lambda_success = wait_for_message(
        &sqs,
        &success_queue_url,
        "lambda success destination",
    )
    .await;
    let lambda_event: serde_json::Value = serde_json::from_str(
        lambda_success
            .body()
            .expect("lambda success destination should contain a body"),
    )
    .expect("lambda success payload should be JSON");
    assert_eq!(lambda_event["requestContext"]["condition"], "Success");
    assert_eq!(
        lambda_event["requestPayload"]["Records"][0]["Sns"]["Message"],
        matching_message
    );

    let notification_request = callback.next_request("notification delivery");
    let notification: serde_json::Value =
        serde_json::from_str(&notification_request.body)
            .expect("notification should be JSON");
    assert_eq!(notification["Type"], "Notification");
    assert_eq!(notification["Message"], matching_message);
    assert_eq!(notification["MessageAttributes"]["store"]["Value"], "eu-west");
    assert!(notification_request.headers.contains(&(
        "x-amz-sns-message-type".to_owned(),
        "Notification".to_owned(),
    )));
    assert!(
        notification_request.headers.contains(&(
            "x-amz-sns-message-id".to_owned(),
            notification["MessageId"]
                .as_str()
                .expect("notification message id should exist")
                .to_owned(),
        ))
    );
    assert!(
        notification_request
            .headers
            .contains(&("x-amz-sns-topic-arn".to_owned(), topic_arn.clone(),))
    );
    assert!(notification_request.headers.iter().any(|(name, value)| {
        name == "x-amz-sns-subscription-arn"
            && value.starts_with("arn:aws:sns:eu-west-2:000000000000:orders:")
    }));
    assert!(notification_request.headers.contains(&(
        "Content-Type".to_owned(),
        "text/plain; charset=UTF-8".to_owned(),
    )));

    sns.publish()
        .topic_arn(&topic_arn)
        .message(r#"{"detail":{"kind":"invoice"}}"#)
        .message_attributes(
            "store",
            SnsMessageAttributeValue::builder()
                .data_type("String")
                .string_value("us-east")
                .build()
                .expect("message attribute should build"),
        )
        .send()
        .await
        .expect("non-matching publish should still succeed");

    assert_no_message(&sqs, &queue_url).await;
    assert_no_message(&sqs, &success_queue_url).await;
    callback.assert_no_more_requests();

    assert!(runtime.state_directory().exists());
    callback.join();
}

#[tokio::test]
async fn sns_delivery_publish_batch_reports_partial_failure() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let sns = SnsClient::new(&config);

    let created = sns
        .create_topic()
        .name("orders")
        .send()
        .await
        .expect("topic should be created");
    let topic_arn =
        created.topic_arn().expect("topic ARN should be returned").to_owned();

    let response = sns
        .publish_batch()
        .topic_arn(&topic_arn)
        .publish_batch_request_entries(
            PublishBatchRequestEntry::builder()
                .id("ok")
                .message("payload")
                .build()
                .expect("batch entry should build"),
        )
        .publish_batch_request_entries(
            PublishBatchRequestEntry::builder()
                .id("bad")
                .message("")
                .build()
                .expect("batch entry should build"),
        )
        .send()
        .await
        .expect("publish batch should succeed");

    assert_eq!(response.successful().len(), 1);
    assert_eq!(response.successful()[0].id(), Some("ok"));
    assert_eq!(response.failed().len(), 1);
    assert_eq!(response.failed()[0].id(), "bad");
    assert_eq!(response.failed()[0].code(), "InvalidParameter");
    assert_eq!(
        response.failed()[0].message(),
        Some("Invalid parameter: Message")
    );

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn sns_delivery_non_raw_sqs_envelope_uses_signed_fields() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let sns = SnsClient::new(&config);
    let sqs = SqsClient::new(&config);

    let queue_name = lambda_fixture::unique_name("sns-signed-queue");
    let queue_url = sqs
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .expect("queue should create")
        .queue_url()
        .expect("queue url should exist")
        .to_owned();
    let queue_arn = format!("arn:aws:sqs:eu-west-2:000000000000:{queue_name}");
    let topic_arn = sns
        .create_topic()
        .name(lambda_fixture::unique_name("sns-signed-topic"))
        .send()
        .await
        .expect("topic should create")
        .topic_arn()
        .expect("topic arn should exist")
        .to_owned();

    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("sqs")
        .endpoint(&queue_arn)
        .send()
        .await
        .expect("queue subscription should create");
    sns.publish()
        .topic_arn(&topic_arn)
        .message(r#"{"detail":{"kind":"invoice"}}"#)
        .send()
        .await
        .expect("publish should succeed");

    let queue_message =
        wait_for_message(&sqs, &queue_url, "signed sns sqs delivery").await;
    let envelope: serde_json::Value = serde_json::from_str(
        queue_message.body().expect("queue delivery should contain a body"),
    )
    .expect("queue delivery should be a JSON SNS envelope");

    assert_eq!(envelope["Type"], "Notification");
    assert_eq!(envelope["SignatureVersion"], "1");
    assert_ne!(envelope["Signature"].as_str(), Some("CLOUDISH"),);
    assert!(
        envelope["SigningCertURL"]
            .as_str()
            .expect("signing cert url should exist")
            .contains("/__aws/sns/signing-cert.pem")
    );
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
            .visibility_timeout(5)
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
        .visibility_timeout(5)
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

async fn assert_no_message(sqs: &SqsClient, queue_url: &str) {
    for _ in 0..5 {
        let response = sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(1)
            .wait_time_seconds(0)
            .visibility_timeout(0)
            .send()
            .await
            .expect("receive message request should succeed");
        if !response.messages().is_empty() {
            assert!(
                response.messages().is_empty(),
                "queue {queue_url} should stay empty"
            );
            std::process::abort();
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
        "queue {queue_url} should stay empty"
    );
}
