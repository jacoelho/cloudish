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
use tests::common::runtime;

use ciborium::from_reader;
use httpdate::parse_http_date;
use runtime::SharedRuntimeLease;
use serde::Deserialize;
use serde_json::Value;
use test_support::{send_http_request, send_http_request_bytes};

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("core_runtime");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

fn split_response(response: &[u8]) -> (&str, Vec<(&str, &str)>, &[u8]) {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response should contain a header terminator");
    let headers = std::str::from_utf8(
        response
            .get(..header_end)
            .expect("response should contain header bytes"),
    )
    .expect("response headers should be valid UTF-8");
    let mut lines = headers.split("\r\n");
    let status = lines.next().expect("response should contain a status line");
    let mut parsed_headers = Vec::new();

    for line in lines {
        let (name, value) =
            line.split_once(':').expect("header should contain ':'");
        parsed_headers.push((name, value.trim()));
    }

    (
        status,
        parsed_headers,
        response
            .get(header_end + 4..)
            .expect("response should contain a body slice"),
    )
}

fn header_value<'a>(
    headers: &'a [(&'a str, &'a str)],
    name: &str,
) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| *value)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct AwsJsonErrorBody {
    #[serde(rename = "__type")]
    error_type: String,
    message: String,
}

#[tokio::test]
async fn health_and_status_endpoints_return_internal_json() {
    let runtime = shared_runtime().await;
    let address = runtime.address();

    let health = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
    })
    .await
    .expect("health request task should complete")
    .expect("health request should succeed");
    let status = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            "GET /__cloudish/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
    })
    .await
    .expect("status request task should complete")
    .expect("status request should succeed");

    let (health_status, health_headers, health_body) =
        split_response(health.as_bytes());
    let (status_status, _, status_body) = split_response(status.as_bytes());

    assert_eq!(health_status, "HTTP/1.1 200 OK");
    let health_date = header_value(&health_headers, "date")
        .expect("health response should expose a Date header");
    parse_http_date(health_date)
        .expect("health Date header should use HTTP-date format");
    assert_eq!(status_status, "HTTP/1.1 200 OK");

    let health_json: Value = serde_json::from_slice(health_body)
        .expect("health response body should be valid JSON");
    let status_json: Value = serde_json::from_slice(status_body)
        .expect("status response body should be valid JSON");
    let enabled_services = status_json
        .get("enabledServices")
        .expect("enabledServices should exist")
        .as_array()
        .expect("enabledServices should be an array");

    assert_eq!(
        health_json.get("status").expect("health status should exist"),
        "ok"
    );
    assert_eq!(
        health_json
            .get("enabledServices")
            .expect("health enabledServices should exist")
            .as_u64(),
        Some(enabled_services.len() as u64)
    );
    assert_eq!(status_json.get("ready").expect("ready should exist"), true);
    assert_eq!(
        status_json
            .get("serviceCount")
            .expect("serviceCount should exist")
            .as_u64(),
        Some(enabled_services.len() as u64)
    );
    assert!(
        status_json
            .get("stateDirectory")
            .expect("stateDirectory should exist")
            .is_string()
    );
    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn unknown_json_and_query_requests_return_protocol_shaped_errors() {
    let runtime = shared_runtime().await;
    let address = runtime.address();

    let json_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 2\r\n\r\n{}",
        )
    })
    .await
    .expect("JSON request task should complete")
    .expect("JSON request should succeed");
    let query_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 20\r\n\r\nAction=UnknownAction",
        )
    })
    .await
    .expect("query request task should complete")
    .expect("query request should succeed");

    let (json_status, json_headers, json_body) =
        split_response(json_response.as_bytes());
    let (query_status, query_headers, query_body) =
        split_response(query_response.as_bytes());
    let json_body: AwsJsonErrorBody =
        serde_json::from_slice(json_body).expect("JSON body should decode");
    let query_body =
        std::str::from_utf8(query_body).expect("query body should be UTF-8");

    assert_eq!(json_status, "HTTP/1.1 400 Bad Request");
    assert_eq!(
        header_value(&json_headers, "content-type"),
        Some("application/x-amz-json-1.0")
    );
    assert_eq!(json_body.error_type, "UnknownOperationException");
    assert_eq!(
        json_body.message,
        "Unknown operation: missing X-Amz-Target header."
    );
    assert_eq!(query_status, "HTTP/1.1 400 Bad Request");
    assert_eq!(header_value(&query_headers, "content-type"), Some("text/xml"));
    assert!(query_body.contains("<Code>InvalidAction</Code>"));
    assert!(
        query_body
            .contains("<Message>Unknown action UnknownAction.</Message>")
    );
}

#[tokio::test]
async fn smithy_cbor_requests_return_cbor_errors() {
    let runtime = shared_runtime().await;
    let address = runtime.address();

    let response = tokio::task::spawn_blocking(move || {
        send_http_request_bytes(
            address,
            b"POST /service/GraniteServiceVersion20100801/operation/GetMetricData HTTP/1.1\r\nHost: localhost\r\nSmithy-Protocol: rpc-v2-cbor\r\nContent-Type: application/cbor\r\nContent-Length: 1\r\n\r\n\xa0",
        )
    })
    .await
    .expect("CBOR request task should complete")
    .expect("CBOR request should succeed");

    let (status, headers, body) = split_response(&response);
    let body: AwsJsonErrorBody =
        from_reader(body).expect("CBOR error body should decode");

    assert_eq!(status, "HTTP/1.1 400 Bad Request");
    assert_eq!(
        header_value(&headers, "content-type"),
        Some("application/cbor")
    );
    assert_eq!(header_value(&headers, "smithy-protocol"), Some("rpc-v2-cbor"));
    parse_http_date(
        header_value(&headers, "date").expect("Date header should be present"),
    )
    .expect("CBOR Date header should use HTTP-date format");
    assert_eq!(body.error_type, "MissingParameter");
    assert!(body.message.contains("EndTime"));
}
