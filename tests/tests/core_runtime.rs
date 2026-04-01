#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
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

    assert_eq!(
        health_json.get("status").expect("health status should exist"),
        "ok"
    );
    assert_eq!(
        status_json
            .get("defaultAccount")
            .expect("defaultAccount should exist"),
        "000000000000"
    );
    assert_eq!(
        status_json.get("defaultRegion").expect("defaultRegion should exist"),
        "eu-west-2"
    );
    assert!(
        status_json
            .get("stateDirectory")
            .expect("stateDirectory should exist")
            .is_string()
    );
    assert_eq!(health_json.as_object().map(|object| object.len()), Some(1));
    assert_eq!(status_json.as_object().map(|object| object.len()), Some(3));
    assert!(health_json.get("defaultAccount").is_none());
    assert!(health_json.get("defaultRegion").is_none());
    assert!(status_json.get("status").is_none());
    assert!(status_json.get("ready").is_none());
    assert!(status_json.get("enabledServices").is_none());
    assert!(status_json.get("serviceCount").is_none());
    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn unknown_json_and_query_requests_return_protocol_shaped_errors() {
    let runtime = shared_runtime().await;
    let address = runtime.address();
    let query_body = "Action=UnknownAction&Version=2011-06-15";

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
            &format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{query_body}",
                query_body.len()
            ),
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
async fn query_get_requests_route_to_sts_and_missing_version_fails_before_mutation()
 {
    let runtime = shared_runtime().await;
    let address = runtime.address();

    let get_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            "GET /?Action=GetCallerIdentity&Version=2011-06-15 HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
    })
    .await
    .expect("GET query request task should complete")
    .expect("GET query request should succeed");
    let missing_version_body = "Action=CreateQueue&QueueName=demo";
    let missing_version_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            &format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{missing_version_body}",
                missing_version_body.len()
            ),
        )
    })
    .await
    .expect("missing-version request task should complete")
    .expect("missing-version request should succeed");
    let list_queues_body = "Action=ListQueues&Version=2012-11-05";
    let list_queues_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            &format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{list_queues_body}",
                list_queues_body.len()
            ),
        )
    })
    .await
    .expect("list queues request task should complete")
    .expect("list queues request should succeed");

    let (get_status, get_headers, get_body) =
        split_response(get_response.as_bytes());
    let get_body =
        std::str::from_utf8(get_body).expect("GET query body should be UTF-8");
    let (
        missing_version_status,
        missing_version_headers,
        missing_version_body,
    ) = split_response(missing_version_response.as_bytes());
    let missing_version_body = std::str::from_utf8(missing_version_body)
        .expect("missing-version body should be UTF-8");
    let (list_status, list_headers, list_body) =
        split_response(list_queues_response.as_bytes());
    let list_body = std::str::from_utf8(list_body)
        .expect("list queues body should be UTF-8");

    assert_eq!(get_status, "HTTP/1.1 403 Forbidden");
    assert_eq!(header_value(&get_headers, "content-type"), Some("text/xml"));
    assert!(get_body.contains("<Code>MissingAuthenticationToken</Code>"));
    assert!(!get_body.contains("<ListAllMyBucketsResult"));

    assert_eq!(missing_version_status, "HTTP/1.1 400 Bad Request");
    assert_eq!(
        header_value(&missing_version_headers, "content-type"),
        Some("text/xml")
    );
    assert!(missing_version_body.contains("<Code>MissingParameter</Code>"));
    assert!(missing_version_body.contains(
        "<Message>The request must contain the parameter Version.</Message>"
    ));

    assert_eq!(list_status, "HTTP/1.1 200 OK");
    assert_eq!(header_value(&list_headers, "content-type"), Some("text/xml"));
    assert!(list_body.contains("<ListQueuesResult"));
    assert!(!list_body.contains("demo"));
}

#[tokio::test]
async fn query_wrong_version_and_json_markers_fail_with_the_expected_protocol()
{
    let runtime = shared_runtime().await;
    let address = runtime.address();
    let wrong_version_body =
        "Action=CreateQueue&QueueName=demo&Version=2011-06-15";

    let wrong_version_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            &format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{wrong_version_body}",
                wrong_version_body.len()
            ),
        )
    })
    .await
    .expect("wrong-version request task should complete")
    .expect("wrong-version request should succeed");
    let list_queues_body = "Action=ListQueues&Version=2012-11-05";
    let list_queues_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            &format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{list_queues_body}",
                list_queues_body.len()
            ),
        )
    })
    .await
    .expect("list queues request task should complete")
    .expect("list queues request should succeed");
    let json_like_body = "Action=GetCallerIdentity&Version=2011-06-15";
    let json_like_response = tokio::task::spawn_blocking(move || {
        send_http_request(
            address,
            &format!(
                "POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: AmazonSSM.GetParameter\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\n\r\n{json_like_body}",
                json_like_body.len()
            ),
        )
    })
    .await
    .expect("JSON-marker request task should complete")
    .expect("JSON-marker request should succeed");

    let (wrong_version_status, wrong_version_headers, wrong_version_body) =
        split_response(wrong_version_response.as_bytes());
    let wrong_version_body = std::str::from_utf8(wrong_version_body)
        .expect("wrong-version body should be UTF-8");
    let (list_status, list_headers, list_body) =
        split_response(list_queues_response.as_bytes());
    let list_body = std::str::from_utf8(list_body)
        .expect("list queues body should be UTF-8");
    let (json_status, json_headers, json_body) =
        split_response(json_like_response.as_bytes());
    let json_body: AwsJsonErrorBody =
        serde_json::from_slice(json_body).expect("JSON body should decode");

    assert_eq!(wrong_version_status, "HTTP/1.1 400 Bad Request");
    assert_eq!(
        header_value(&wrong_version_headers, "content-type"),
        Some("text/xml")
    );
    assert!(wrong_version_body.contains("<Code>InvalidAction</Code>"));
    assert!(wrong_version_body.contains(
        "<Message>Could not find operation CreateQueue for version 2011-06-15.</Message>"
    ));

    assert_eq!(list_status, "HTTP/1.1 200 OK");
    assert_eq!(header_value(&list_headers, "content-type"), Some("text/xml"));
    assert!(list_body.contains("<ListQueuesResult"));
    assert!(!list_body.contains("demo"));

    assert_eq!(json_status, "HTTP/1.1 400 Bad Request");
    assert_eq!(
        header_value(&json_headers, "content-type"),
        Some("application/x-amz-json-1.1")
    );
    assert_eq!(json_body.error_type, "ValidationException");
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
