use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use http::EdgeResponse;
use http_body_util::Full;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Response, StatusCode};
use serde_json::{Value, json};
use std::error::Error;
use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

const EDGE_WIRE_LOG_TARGET: &str = "cloudish.edge_wire";

pub(crate) fn try_init_tracing(
    wire_log_enabled: bool,
) -> Result<(), WireLogInitError> {
    static TRACING_INIT: OnceLock<Result<(), WireLogInitError>> =
        OnceLock::new();

    ensure_dispatch_installed(&TRACING_INIT, || {
        install_global_dispatch(edge_wire_dispatch(wire_log_enabled))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireLogInitError {
    GlobalSubscriberAlreadySet,
}

impl fmt::Display for WireLogInitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalSubscriberAlreadySet => write!(
                formatter,
                "tracing subscriber requested but Cloudish could not install its JSON tracing subscriber because a global tracing subscriber is already installed"
            ),
        }
    }
}

impl Error for WireLogInitError {}

fn ensure_dispatch_installed<F>(
    init: &OnceLock<Result<(), WireLogInitError>>,
    install: F,
) -> Result<(), WireLogInitError>
where
    F: FnOnce() -> Result<(), WireLogInitError>,
{
    init.get_or_init(install).clone()
}

fn edge_wire_dispatch(wire_log_enabled: bool) -> tracing::Dispatch {
    let max_level = if wire_log_enabled {
        tracing::Level::INFO
    } else {
        tracing::Level::WARN
    };
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_target(true)
        .with_writer(std::io::stderr)
        .with_max_level(max_level)
        .finish();
    tracing::Dispatch::new(subscriber)
}

fn install_global_dispatch(
    dispatch: tracing::Dispatch,
) -> Result<(), WireLogInitError> {
    tracing::dispatcher::set_global_default(dispatch)
        .map_err(|_| WireLogInitError::GlobalSubscriberAlreadySet)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggedRequest {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    method: String,
    path: String,
    source_ip: Option<String>,
}

impl LoggedRequest {
    pub(crate) fn from_parts(
        parts: &hyper::http::request::Parts,
        body: &[u8],
        source_ip: Option<&str>,
    ) -> Self {
        let headers = parts
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let path = parts
            .uri
            .path_and_query()
            .map(hyper::http::uri::PathAndQuery::as_str)
            .unwrap_or_else(|| parts.uri.path())
            .to_owned();

        Self {
            body: body.to_vec(),
            headers,
            method: parts.method.as_str().to_owned(),
            path,
            source_ip: source_ip.map(str::to_owned),
        }
    }

    fn path_without_query(&self) -> &str {
        self.path.split('?').next().unwrap_or(&self.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggedResponse {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    status_code: u16,
}

impl LoggedResponse {
    pub(crate) fn from_edge_response(response: EdgeResponse) -> Self {
        let (status_code, headers, body) = response.into_parts();
        Self { body, headers, status_code }
    }

    pub(crate) fn json(status: StatusCode, body: &Value) -> Self {
        let bytes = match serde_json::to_vec(body) {
            Ok(bytes) => bytes,
            Err(_) => {
                b"{\"message\":\"failed to serialize response\"}".to_vec()
            }
        };
        let content_length = bytes.len();

        Self {
            body: bytes,
            headers: vec![
                ("Content-Type".to_owned(), "application/json".to_owned()),
                ("Connection".to_owned(), "close".to_owned()),
                ("Content-Length".to_owned(), content_length.to_string()),
            ],
            status_code: status.as_u16(),
        }
    }

    pub(crate) fn into_hyper(self) -> Response<Full<Bytes>> {
        let mut response = Response::new(Full::new(Bytes::from(self.body)));
        *response.status_mut() = match StatusCode::from_u16(self.status_code) {
            Ok(status_code) => status_code,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let response_headers = response.headers_mut();
        for (name, value) in self.headers {
            if let Ok(name) = HeaderName::from_bytes(name.as_bytes())
                && let Ok(value) = HeaderValue::from_str(&value)
            {
                response_headers.append(name, value);
            }
        }

        response
    }
}

pub(crate) fn log_exchange(
    request: &LoggedRequest,
    response: &LoggedResponse,
    duration: Duration,
) {
    if should_skip_path(request.path_without_query()) {
        return;
    }

    let (request_body, request_body_encoding) = encode_body(&request.body);
    let (response_body, response_body_encoding) = encode_body(&response.body);
    let request_headers_json =
        serialize_headers(&request.headers, HeaderCategory::Request);
    let response_headers_json =
        serialize_headers(&response.headers, HeaderCategory::Response);
    let source_ip = request.source_ip.as_deref().unwrap_or("");

    tracing::info!(
        target: EDGE_WIRE_LOG_TARGET,
        duration_ms = duration_ms(duration),
        method = request.method.as_str(),
        path = sanitize_path(&request.path),
        request_body = request_body.as_str(),
        request_body_bytes = request.body.len(),
        request_body_encoding = request_body_encoding,
        request_headers_json = request_headers_json.as_str(),
        response_body = response_body.as_str(),
        response_body_bytes = response.body.len(),
        response_body_encoding = response_body_encoding,
        response_headers_json = response_headers_json.as_str(),
        source_ip,
        status_code = response.status_code,
        message = "edge wire exchange",
    );
}

fn duration_ms(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        return u64::MAX;
    }

    millis as u64
}

fn should_skip_path(path: &str) -> bool {
    matches!(path, "/__cloudish/health" | "/__cloudish/status")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderCategory {
    Request,
    Response,
}

fn serialize_headers(
    headers: &[(String, String)],
    category: HeaderCategory,
) -> String {
    let serialized = Value::Array(
        headers
            .iter()
            .map(|(name, value)| {
                json!({
                    "name": name,
                    "value": redact_header(name, value, category),
                })
            })
            .collect(),
    );

    match serde_json::to_string(&serialized) {
        Ok(serialized) => serialized,
        Err(error) => format!(
            "[{{\"name\":\"serialization_error\",\"value\":\"{error}\"}}]"
        ),
    }
}

fn redact_header(name: &str, value: &str, category: HeaderCategory) -> String {
    let redact = match category {
        HeaderCategory::Request => matches_ignore_ascii_case(
            name,
            &["authorization", "cookie", "x-amz-security-token"],
        ),
        HeaderCategory::Response => {
            matches_ignore_ascii_case(name, &["set-cookie"])
        }
    };

    if redact { "[REDACTED]".to_owned() } else { value.to_owned() }
}

fn matches_ignore_ascii_case(name: &str, candidates: &[&str]) -> bool {
    candidates.iter().any(|candidate| name.eq_ignore_ascii_case(candidate))
}

fn sanitize_path(path: &str) -> String {
    let Some((route, query)) = path.split_once('?') else {
        return path.to_owned();
    };

    let sanitized = query
        .split('&')
        .map(sanitize_query_pair)
        .collect::<Vec<_>>()
        .join("&");

    format!("{route}?{sanitized}")
}

fn sanitize_query_pair(pair: &str) -> String {
    let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
    let decoded_key = urlencoding::decode(raw_key)
        .map_or_else(|_| raw_key.into(), |key| key);
    if matches_ignore_ascii_case(
        decoded_key.as_ref(),
        &["X-Amz-Credential", "X-Amz-Security-Token", "X-Amz-Signature"],
    ) {
        return format!("{raw_key}=[REDACTED]");
    }

    if raw_value.is_empty() { raw_key.to_owned() } else { pair.to_owned() }
}

fn encode_body(body: &[u8]) -> (String, &'static str) {
    match std::str::from_utf8(body) {
        Ok(body) => (body.to_owned(), "utf8"),
        Err(_) => (BASE64_STANDARD.encode(body), "base64"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EDGE_WIRE_LOG_TARGET, HeaderCategory, LoggedRequest, LoggedResponse,
        WireLogInitError, ensure_dispatch_installed, log_exchange,
        redact_header, sanitize_path,
    };
    use serde_json::Value;
    use std::io;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct SharedWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn contents(&self) -> Vec<u8> {
            self.bytes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        }
    }

    struct SharedGuard {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for SharedGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedWriter {
        type Writer = SharedGuard;

        fn make_writer(&'a self) -> Self::Writer {
            SharedGuard { bytes: self.bytes.clone() }
        }
    }

    #[test]
    fn sanitize_path_redacts_presigned_query_secrets() {
        assert_eq!(
            sanitize_path(
                "/queues?Action=CreateQueue&X-Amz-Credential=abc&X-Amz-Signature=sig&QueueName=orders"
            ),
            "/queues?Action=CreateQueue&X-Amz-Credential=[REDACTED]&X-Amz-Signature=[REDACTED]&QueueName=orders"
        );
    }

    #[test]
    fn redact_header_only_hides_sensitive_values() {
        assert_eq!(
            redact_header("Authorization", "secret", HeaderCategory::Request,),
            "[REDACTED]"
        );
        assert_eq!(
            redact_header(
                "X-Amz-Target",
                "AmazonSQS.CreateQueue",
                HeaderCategory::Request
            ),
            "AmazonSQS.CreateQueue"
        );
        assert_eq!(
            redact_header(
                "Set-Cookie",
                "session=abc",
                HeaderCategory::Response
            ),
            "[REDACTED]"
        );
    }

    #[test]
    fn log_exchange_emits_json_event_with_redacted_fields() {
        let request = hyper::Request::builder()
            .method("POST")
            .uri("/?Action=CreateQueue&X-Amz-Signature=secret")
            .header("Authorization", "AWS4-HMAC-SHA256 ...")
            .header("X-Amz-Target", "AmazonSQS.CreateQueue")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();
        let logged_request = LoggedRequest::from_parts(
            &parts,
            br#"{"QueueName":"orders"}"#,
            Some("127.0.0.1"),
        );
        let response = LoggedResponse::json(
            hyper::StatusCode::BAD_REQUEST,
            &serde_json::json!({
                "__type": "QueueAlreadyExists",
                "message": "A queue already exists with the same name and a different value for attribute ReceiveMessageWaitTimeSeconds",
            }),
        );
        let writer = SharedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_target(true)
            .with_writer(writer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_exchange(
                &logged_request,
                &response,
                Duration::from_millis(12),
            );
        });

        let output =
            String::from_utf8(writer.contents()).expect("logs should be utf8");
        let event: Value = serde_json::from_str(
            output.lines().next().expect("one log line should be emitted"),
        )
        .expect("log line should be json");

        assert_eq!(event["target"], EDGE_WIRE_LOG_TARGET);
        assert_eq!(event["message"], "edge wire exchange");
        assert_eq!(
            event["path"],
            "/?Action=CreateQueue&X-Amz-Signature=[REDACTED]"
        );
        assert_eq!(event["status_code"], 400);
        assert_eq!(event["source_ip"], "127.0.0.1");

        let request_headers: Value = serde_json::from_str(
            event["request_headers_json"]
                .as_str()
                .expect("request headers should be serialized"),
        )
        .expect("request headers json should parse");
        assert_eq!(request_headers[0]["value"], "[REDACTED]");
        assert_eq!(request_headers[1]["value"], "AmazonSQS.CreateQueue");
        assert_eq!(event["request_body"], "{\"QueueName\":\"orders\"}");
    }

    #[test]
    fn log_exchange_base64_encodes_binary_bodies() {
        let request = hyper::Request::builder()
            .method("POST")
            .uri("/binary")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();
        let logged_request =
            LoggedRequest::from_parts(&parts, &[0_u8, 159], None);
        let response = LoggedResponse::json(
            hyper::StatusCode::OK,
            &serde_json::json!({ "ok": true }),
        );
        let writer = SharedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(writer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_exchange(&logged_request, &response, Duration::from_millis(1));
        });

        let output =
            String::from_utf8(writer.contents()).expect("logs should be utf8");
        let event: Value = serde_json::from_str(
            output.lines().next().expect("one log line should be emitted"),
        )
        .expect("log line should be json");

        assert_eq!(event["request_body_encoding"], "base64");
        assert_eq!(event["request_body"], "AJ8=");
    }

    #[test]
    fn log_exchange_skips_internal_health_routes() {
        let request = hyper::Request::builder()
            .method("GET")
            .uri("/__cloudish/health")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();
        let logged_request = LoggedRequest::from_parts(&parts, &[], None);
        let response = LoggedResponse::json(
            hyper::StatusCode::OK,
            &serde_json::json!({ "status": "ok" }),
        );
        let writer = SharedWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(writer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_exchange(&logged_request, &response, Duration::from_millis(1));
        });

        assert!(writer.contents().is_empty());
    }

    #[test]
    fn ensure_dispatch_installed_is_idempotent_after_success() {
        let init = OnceLock::new();
        let calls = AtomicUsize::new(0);

        let first = ensure_dispatch_installed(&init, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let second = ensure_dispatch_installed(&init, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        assert_eq!(first, Ok(()));
        assert_eq!(second, Ok(()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ensure_dispatch_installed_caches_conflict_errors() {
        let init = OnceLock::new();
        let calls = AtomicUsize::new(0);

        let first = ensure_dispatch_installed(&init, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(WireLogInitError::GlobalSubscriberAlreadySet)
        });
        let second = ensure_dispatch_installed(&init, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        assert_eq!(first, Err(WireLogInitError::GlobalSubscriberAlreadySet));
        assert_eq!(second, Err(WireLogInitError::GlobalSubscriberAlreadySet));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
