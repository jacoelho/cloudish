#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_sns::Client;
use aws_sdk_sns::error::ProvideErrorMetadata;
use aws_sdk_sns::types::Tag;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("sns_core");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

struct CaptureHttpServer {
    address: SocketAddr,
    handle: Option<JoinHandle<()>>,
    request_rx: mpsc::Receiver<Vec<u8>>,
}

impl CaptureHttpServer {
    fn spawn() -> Self {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("fixture should bind");
        let address =
            listener.local_addr().expect("fixture should expose its address");
        let (request_tx, request_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) =
                listener.accept().expect("fixture should accept one request");
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
        });

        Self { address, handle: Some(handle), request_rx }
    }

    fn endpoint_url(&self) -> String {
        format!("http://{}/subscription", self.address)
    }

    fn next_body(&self) -> String {
        let request = self
            .request_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("fixture should receive a confirmation request");
        let body_start = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .expect("fixture request should contain headers");

        String::from_utf8(
            request
                .get(body_start..)
                .expect("fixture request should contain a body")
                .to_vec(),
        )
        .expect("fixture body should be UTF-8")
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
async fn sns_core_topic_and_subscription_lifecycle_round_trips() {
    let runtime = shared_runtime().await;
    let callback = CaptureHttpServer::spawn();
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let created = client
        .create_topic()
        .name("orders")
        .send()
        .await
        .expect("topic should be created");
    let topic_arn =
        created.topic_arn().expect("topic ARN should be returned").to_owned();

    let listed =
        client.list_topics().send().await.expect("topics should list");
    assert!(
        listed
            .topics()
            .iter()
            .any(|topic| topic.topic_arn() == Some(topic_arn.as_str()))
    );

    client
        .set_topic_attributes()
        .topic_arn(&topic_arn)
        .attribute_name("DisplayName")
        .attribute_value("Orders")
        .send()
        .await
        .expect("topic attributes should update");
    let attributes = client
        .get_topic_attributes()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("topic attributes should load");
    let attributes = attributes.attributes().expect("attributes should exist");
    assert_eq!(
        attributes.get("DisplayName").map(String::as_str),
        Some("Orders")
    );

    client
        .tag_resource()
        .resource_arn(&topic_arn)
        .tags(
            Tag::builder()
                .key("env")
                .value("dev")
                .build()
                .expect("tag should build"),
        )
        .tags(
            Tag::builder()
                .key("team")
                .value("platform")
                .build()
                .expect("tag should build"),
        )
        .send()
        .await
        .expect("topic tags should apply");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&topic_arn)
        .send()
        .await
        .expect("topic tags should list");
    assert!(
        tags.tags()
            .iter()
            .any(|tag| tag.key() == "env" && tag.value() == "dev")
    );

    let subscribed = client
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("http")
        .endpoint(callback.endpoint_url())
        .return_subscription_arn(true)
        .send()
        .await
        .expect("http subscription should be created");
    let pending_subscription_arn = subscribed
        .subscription_arn()
        .expect("subscription ARN should be returned")
        .to_owned();
    let body = callback.next_body();
    let confirmation: serde_json::Value =
        serde_json::from_str(&body).expect("confirmation should be JSON");
    let token = confirmation
        .get("Token")
        .and_then(serde_json::Value::as_str)
        .expect("confirmation token should exist");

    let pending = client
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("subscriptions should list");
    assert!(pending.subscriptions().iter().any(|subscription| {
        subscription.subscription_arn() == Some("PendingConfirmation")
    }));

    let confirmed = client
        .confirm_subscription()
        .topic_arn(&topic_arn)
        .token(token)
        .send()
        .await
        .expect("subscription should confirm");
    assert_eq!(
        confirmed.subscription_arn(),
        Some(pending_subscription_arn.as_str())
    );

    let listed = client
        .list_subscriptions_by_topic()
        .topic_arn(&topic_arn)
        .send()
        .await
        .expect("confirmed subscriptions should list");
    assert!(listed.subscriptions().iter().any(|subscription| {
        subscription.subscription_arn()
            == Some(pending_subscription_arn.as_str())
    }));

    let published = client
        .publish()
        .topic_arn(&topic_arn)
        .message("payload")
        .send()
        .await
        .expect("publish should succeed");
    assert!(published.message_id().is_some());

    assert!(runtime.state_directory().exists());
    callback.join();
}

#[tokio::test]
async fn sns_core_missing_topic_publish_surfaces_explicit_error() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = Client::new(&config);

    let error = client
        .publish()
        .topic_arn("arn:aws:sns:eu-west-2:000000000000:missing")
        .message("payload")
        .send()
        .await
        .expect_err("publishing to a missing topic should fail");

    assert_eq!(error.code(), Some("NotFound"));
    assert!(error.message().is_some_and(|message| message.contains("Topic")));
    assert!(runtime.state_directory().exists());
}
