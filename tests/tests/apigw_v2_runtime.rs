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
use tests::common::http_fixture;
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_apigatewayv2::Client as ApiGatewayV2Client;
use aws_sdk_apigatewayv2::types::{IntegrationType, ProtocolType};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk-apigw-v2-runtime-{id}")
}

#[tokio::test]
async fn apigw_v2_runtime_exact_greedy_and_default_routes_resolve_in_order() {
    let runtime = RuntimeServer::spawn("sdk-apigw-v2-runtime").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ApiGatewayV2Client::new(&config);

    let exact_backend = http_fixture::OneShotHttpServer::spawn(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: exact\r\nContent-Length: 5\r\nConnection: close\r\n\r\nexact".to_vec(),
    );
    let greedy_backend = http_fixture::OneShotHttpServer::spawn(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: greedy\r\nContent-Length: 6\r\nConnection: close\r\n\r\ngreedy".to_vec(),
    );
    let default_backend = http_fixture::OneShotHttpServer::spawn(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Upstream: default\r\nContent-Length: 7\r\nConnection: close\r\n\r\ndefault".to_vec(),
    );

    let root = unique_root();
    let api = client
        .create_api()
        .name(format!("{root}-api"))
        .protocol_type(ProtocolType::Http)
        .send()
        .await
        .expect("HTTP API should create");
    let api_id = api.api_id().expect("api id should be present").to_owned();

    let exact = client
        .create_integration()
        .api_id(&api_id)
        .integration_type(IntegrationType::HttpProxy)
        .integration_uri(format!(
            "http://127.0.0.1:{}/exact",
            exact_backend.address().port()
        ))
        .send()
        .await
        .expect("exact integration should create");
    let greedy = client
        .create_integration()
        .api_id(&api_id)
        .integration_type(IntegrationType::HttpProxy)
        .integration_uri(format!(
            "http://127.0.0.1:{}/greedy/{{proxy}}",
            greedy_backend.address().port()
        ))
        .send()
        .await
        .expect("greedy integration should create");
    let default_integration = client
        .create_integration()
        .api_id(&api_id)
        .integration_type(IntegrationType::HttpProxy)
        .integration_uri(format!(
            "http://127.0.0.1:{}",
            default_backend.address().port()
        ))
        .send()
        .await
        .expect("default integration should create");

    client
        .create_route()
        .api_id(&api_id)
        .route_key("GET /pets/dog/1")
        .target(format!(
            "integrations/{}",
            exact
                .integration_id()
                .expect("exact integration id should be present")
        ))
        .send()
        .await
        .expect("exact route should create");
    client
        .create_route()
        .api_id(&api_id)
        .route_key("GET /pets/{proxy+}")
        .target(format!(
            "integrations/{}",
            greedy
                .integration_id()
                .expect("greedy integration id should be present")
        ))
        .send()
        .await
        .expect("greedy route should create");
    client
        .create_route()
        .api_id(&api_id)
        .route_key("$default")
        .target(format!(
            "integrations/{}",
            default_integration
                .integration_id()
                .expect("default integration id should be present")
        ))
        .send()
        .await
        .expect("default route should create");
    client
        .create_stage()
        .api_id(&api_id)
        .stage_name("$default")
        .auto_deploy(true)
        .send()
        .await
        .expect("default stage should create");

    let exact_response = reqwest::Client::new()
        .get(format!(
            "http://{}/__aws/execute-api/{api_id}/pets/dog/1",
            runtime.address()
        ))
        .send()
        .await
        .expect("exact execute-api request should succeed");
    let greedy_response = reqwest::Client::new()
        .get(format!(
            "http://{}/__aws/execute-api/{api_id}/pets/cat/2?view=full",
            runtime.address()
        ))
        .send()
        .await
        .expect("greedy execute-api request should succeed");
    let default_response = reqwest::Client::new()
        .post(format!(
            "http://{}/__aws/execute-api/{api_id}/orders/5",
            runtime.address()
        ))
        .body(r#"{"ok":true}"#)
        .send()
        .await
        .expect("default execute-api request should succeed");

    assert_eq!(exact_response.status(), 200);
    assert_eq!(
        exact_response
            .headers()
            .get("x-upstream")
            .and_then(|value| value.to_str().ok()),
        Some("exact")
    );
    assert_eq!(
        exact_response.text().await.expect("exact body should decode"),
        "exact"
    );

    assert_eq!(greedy_response.status(), 200);
    assert_eq!(
        greedy_response
            .headers()
            .get("x-upstream")
            .and_then(|value| value.to_str().ok()),
        Some("greedy")
    );
    assert_eq!(
        greedy_response.text().await.expect("greedy body should decode"),
        "greedy"
    );

    assert_eq!(default_response.status(), 200);
    assert_eq!(
        default_response
            .headers()
            .get("x-upstream")
            .and_then(|value| value.to_str().ok()),
        Some("default")
    );
    assert_eq!(
        default_response.text().await.expect("default body should decode"),
        "default"
    );

    exact_backend.join();
    greedy_backend.join();
    default_backend.join();
    runtime.shutdown().await;
}
