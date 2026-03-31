use crate::StartupError;
use crate::edge_wire_log::{LoggedRequest, LoggedResponse, log_exchange};
use bytes::Bytes;
use http::{EdgeRequest, EdgeRequestExecutor, EdgeRouter};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Incoming};
use hyper::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::json;
use std::convert::Infallible;
use std::future::Future;
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LockResult, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinSet};

const BLOCKING_REQUEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

pub(crate) async fn serve_listener_with_shutdown<F>(
    listener: TcpListener,
    router: EdgeRouter,
    max_request_bytes: usize,
    wire_log_enabled: bool,
    shutdown: F,
) -> Result<(), StartupError>
where
    F: Future<Output = ()>,
{
    let blocking_requests = BlockingRequestSet::default();
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => {
                blocking_requests.begin_shutdown();
                router.begin_shutdown();
                connections.abort_all();
                let shutdown_outcome =
                    wait_for_blocking_requests(blocking_requests.clone()).await;
                drain_connection_tasks(&mut connections).await;
                return finish_server_shutdown(shutdown_outcome, || {
                    let warnings = router.shutdown();
                    for warning in &warnings {
                        tracing::warn!(%warning, "service shutdown warning");
                    }
                });
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(source) => {
                        let warnings = router.shutdown();
                        for warning in &warnings {
                            tracing::warn!(%warning, "service shutdown warning");
                        }
                        return Err(StartupError::Accept { source });
                    }
                };
                let router = router.clone();
                let blocking_requests = blocking_requests.clone();
                connections.spawn(async move {
                    serve_connection(
                        stream,
                        router,
                        max_request_bytes,
                        blocking_requests.clone(),
                        wire_log_enabled,
                    )
                    .await;
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                handle_connection_task_result(result);
            }
        }
    }
}

async fn drain_connection_tasks(connections: &mut JoinSet<()>) {
    while let Some(result) = connections.join_next().await {
        handle_connection_task_result(result);
    }
}

fn handle_connection_task_result(result: Result<(), JoinError>) {
    if let Err(error) = result
        && error.is_panic()
    {
        panic::resume_unwind(error.into_panic());
    }
}

async fn serve_connection(
    stream: TcpStream,
    router: EdgeRouter,
    max_request_bytes: usize,
    blocking_requests: BlockingRequestSet,
    wire_log_enabled: bool,
) {
    let source_ip =
        stream.peer_addr().ok().map(|address| address.ip().to_string());
    let request_executor = router.request_executor();
    let io = TokioIo::new(stream);
    let service = service_fn(move |request| {
        let request_executor = request_executor.clone();
        let source_ip = source_ip.clone();
        let blocking_requests = blocking_requests.clone();
        async move {
            Ok::<_, Infallible>(
                handle_request(
                    request_executor,
                    request,
                    max_request_bytes,
                    source_ip,
                    blocking_requests,
                    wire_log_enabled,
                )
                .await,
            )
        }
    });

    if let Err(error) = http1::Builder::new()
        .half_close(true)
        .serve_connection(io, service)
        .await
    {
        tracing::warn!(%error, "http connection error");
    }
}

async fn handle_request(
    request_executor: EdgeRequestExecutor,
    request: Request<Incoming>,
    max_request_bytes: usize,
    source_ip: Option<String>,
    blocking_requests: BlockingRequestSet,
    wire_log_enabled: bool,
) -> Response<Full<Bytes>> {
    if wire_log_enabled {
        return handle_request_with_wire_log(
            request_executor,
            request,
            max_request_bytes,
            source_ip,
            blocking_requests,
        )
        .await;
    }

    handle_request_without_wire_log(
        request_executor,
        request,
        max_request_bytes,
        source_ip,
        blocking_requests,
    )
    .await
}

async fn handle_request_without_wire_log(
    request_executor: EdgeRequestExecutor,
    request: Request<Incoming>,
    max_request_bytes: usize,
    source_ip: Option<String>,
    blocking_requests: BlockingRequestSet,
) -> Response<Full<Bytes>> {
    let (parts, body) = request.into_parts();
    let body = match collect_request_body(
        body,
        max_request_bytes,
        BodyCaptureMode::None,
    )
    .await
    {
        Ok(body) => body,
        Err(RequestBodyError::PayloadTooLarge { .. }) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &json!({
                    "message": format!(
                        "request body exceeds configured limit of {max_request_bytes} bytes"
                    )
                }),
            );
        }
        Err(RequestBodyError::Read { .. }) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({ "message": "failed to read request body" }),
            );
        }
    };

    let request = match edge_request(parts, body, source_ip) {
        Ok(request) => request,
        Err(message) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({ "message": message }),
            );
        }
    };
    let Some(blocking_request) = blocking_requests.begin_request() else {
        return shutting_down_response();
    };

    await_blocking_request(blocking_request, move |request_cancellation| {
        edge_response(request_executor.execute(request, request_cancellation))
    })
    .await
}

async fn handle_request_with_wire_log(
    request_executor: EdgeRequestExecutor,
    request: Request<Incoming>,
    max_request_bytes: usize,
    source_ip: Option<String>,
    blocking_requests: BlockingRequestSet,
) -> Response<Full<Bytes>> {
    let started_at = Instant::now();
    let (parts, body) = request.into_parts();
    let body = match collect_request_body(
        body,
        max_request_bytes,
        BodyCaptureMode::PartialForWireLog,
    )
    .await
    {
        Ok(body) => body,
        Err(error @ RequestBodyError::PayloadTooLarge { .. }) => {
            let request = LoggedRequest::from_parts(
                &parts,
                error.partial_body().unwrap_or(&[]),
                source_ip.as_deref(),
            );
            let response = LoggedResponse::json(
                StatusCode::PAYLOAD_TOO_LARGE,
                &json!({
                    "message": format!(
                        "request body exceeds configured limit of {max_request_bytes} bytes"
                    )
                }),
            );
            log_exchange(&request, &response, started_at.elapsed());
            return response.into_hyper();
        }
        Err(error @ RequestBodyError::Read { .. }) => {
            let request = LoggedRequest::from_parts(
                &parts,
                error.partial_body().unwrap_or(&[]),
                source_ip.as_deref(),
            );
            let response = LoggedResponse::json(
                StatusCode::BAD_REQUEST,
                &json!({ "message": "failed to read request body" }),
            );
            log_exchange(&request, &response, started_at.elapsed());
            return response.into_hyper();
        }
    };
    let request_log =
        LoggedRequest::from_parts(&parts, &body, source_ip.as_deref());
    let Some(blocking_request) = blocking_requests.begin_request() else {
        let response = LoggedResponse::json(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "message": "server shutting down" }),
        );
        log_exchange(&request_log, &response, started_at.elapsed());
        return response.into_hyper();
    };

    await_blocking_request(blocking_request, move |request_cancellation| {
        let request = match edge_request(parts, body, source_ip) {
            Ok(request) => request,
            Err(message) => {
                let response = LoggedResponse::json(
                    StatusCode::BAD_REQUEST,
                    &json!({ "message": message }),
                );
                log_exchange(&request_log, &response, started_at.elapsed());
                return response.into_hyper();
            }
        };

        let response = LoggedResponse::from_edge_response(
            request_executor.execute(request, request_cancellation),
        );
        log_exchange(&request_log, &response, started_at.elapsed());
        response.into_hyper()
    })
    .await
}

async fn await_blocking_request<F>(
    blocking_request: ActiveBlockingRequest,
    request: F,
) -> Response<Full<Bytes>>
where
    F: FnOnce(&AtomicBool) -> Response<Full<Bytes>> + Send + 'static,
{
    let request_cancellation = blocking_request.cancellation_signal();
    match tokio::task::spawn_blocking(move || {
        let response = request(request_cancellation.as_ref());
        drop(blocking_request);
        response
    })
    .await
    {
        Ok(response) => response,
        Err(error) => join_error_response(error),
    }
}

async fn wait_for_blocking_requests(
    blocking_requests: BlockingRequestSet,
) -> BlockingRequestShutdownOutcome {
    let wait_set = blocking_requests.clone();
    match tokio::task::spawn_blocking(move || {
        wait_set.wait_for_idle(BLOCKING_REQUEST_SHUTDOWN_TIMEOUT)
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            if error.is_panic() {
                panic::resume_unwind(error.into_panic());
            }
            BlockingRequestShutdownOutcome::TimedOut {
                active_requests: blocking_requests.active_request_count(),
            }
        }
    }
}

fn finish_server_shutdown<F>(
    shutdown_outcome: BlockingRequestShutdownOutcome,
    shutdown_runtime: F,
) -> Result<(), StartupError>
where
    F: FnOnce(),
{
    match shutdown_outcome {
        BlockingRequestShutdownOutcome::Idle => {
            shutdown_runtime();
            Ok(())
        }
        BlockingRequestShutdownOutcome::TimedOut { active_requests } => {
            tracing::warn!(
                active_requests,
                "blocking requests did not drain, proceeding with teardown"
            );
            shutdown_runtime();
            Err(StartupError::ShutdownTimeout { active_requests })
        }
    }
}

fn join_error_response(error: JoinError) -> Response<Full<Bytes>> {
    if error.is_panic() {
        panic::resume_unwind(error.into_panic());
    }

    json_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        &json!({ "message": "internal server error" }),
    )
}

async fn collect_request_body<B>(
    mut body: B,
    max_request_bytes: usize,
    capture_mode: BodyCaptureMode,
) -> Result<Vec<u8>, RequestBodyError>
where
    B: Body<Data = Bytes> + Unpin,
{
    let mut decoded = Vec::new();

    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => {
                return Err(RequestBodyError::read(capture_mode, decoded));
            }
        };
        if let Some(data) = frame.data_ref() {
            let Some(next_len) = decoded.len().checked_add(data.len()) else {
                return Err(RequestBodyError::payload_too_large(
                    capture_mode,
                    decoded,
                ));
            };
            if next_len > max_request_bytes {
                return Err(RequestBodyError::payload_too_large(
                    capture_mode,
                    decoded,
                ));
            }
            decoded.extend_from_slice(data);
        }
    }

    Ok(decoded)
}

fn edge_request(
    parts: hyper::http::request::Parts,
    body: Vec<u8>,
    source_ip: Option<String>,
) -> Result<EdgeRequest, &'static str> {
    let headers = parts
        .headers
        .iter()
        .map(|(name, value)| {
            Ok::<_, &'static str>((
                name.as_str().to_owned(),
                value
                    .to_str()
                    .map_err(|_| "request headers are not valid UTF-8")?
                    .to_owned(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let path = parts
        .uri
        .path_and_query()
        .map(hyper::http::uri::PathAndQuery::as_str)
        .unwrap_or_else(|| parts.uri.path())
        .to_owned();

    let mut request =
        EdgeRequest::new(parts.method.as_str(), path, headers, body);
    request.set_source_ip(source_ip);
    Ok(request)
}

fn edge_response(response: http::EdgeResponse) -> Response<Full<Bytes>> {
    let (status_code, headers, body) = response.into_parts();
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = match StatusCode::from_u16(status_code) {
        Ok(status_code) => status_code,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };

    let response_headers = response.headers_mut();
    for (name, value) in headers {
        if let Ok(name) = HeaderName::from_bytes(name.as_bytes())
            && let Ok(value) = HeaderValue::from_str(&value)
        {
            response_headers.append(name, value);
        }
    }

    response
}

#[derive(Clone, Default)]
struct BlockingRequestSet {
    inner: Arc<BlockingRequestSetInner>,
}

struct BlockingRequestSetInner {
    idle: Condvar,
    state: Mutex<BlockingRequestState>,
}

#[derive(Default)]
struct BlockingRequestState {
    admission_open: bool,
    requests: Vec<Arc<AtomicBool>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockingRequestShutdownOutcome {
    Idle,
    TimedOut { active_requests: usize },
}

impl BlockingRequestSet {
    fn begin_request(&self) -> Option<ActiveBlockingRequest> {
        let mut state = recover(self.inner.state.lock());
        if !state.admission_open {
            return None;
        }
        let cancellation = Arc::new(AtomicBool::new(false));
        state.requests.push(Arc::clone(&cancellation));

        Some(ActiveBlockingRequest { cancellation, requests: self.clone() })
    }

    fn begin_shutdown(&self) {
        let mut state = recover(self.inner.state.lock());
        state.admission_open = false;
        for cancellation in &state.requests {
            cancellation.store(true, Ordering::SeqCst);
        }
        if state.requests.is_empty() {
            self.inner.idle.notify_all();
        }
    }

    fn finish_request(&self, cancellation: &Arc<AtomicBool>) {
        let mut state = recover(self.inner.state.lock());
        state.requests.retain(|active| !Arc::ptr_eq(active, cancellation));
        if state.requests.is_empty() {
            self.inner.idle.notify_all();
        }
    }

    fn wait_for_idle(
        &self,
        timeout: Duration,
    ) -> BlockingRequestShutdownOutcome {
        let state = recover(self.inner.state.lock());
        let (state, _) = self
            .inner
            .idle
            .wait_timeout_while(state, timeout, |state| {
                !state.requests.is_empty()
            })
            .unwrap_or_else(|poison| poison.into_inner());
        if state.requests.is_empty() {
            BlockingRequestShutdownOutcome::Idle
        } else {
            BlockingRequestShutdownOutcome::TimedOut {
                active_requests: state.requests.len(),
            }
        }
    }

    fn active_request_count(&self) -> usize {
        recover(self.inner.state.lock()).requests.len()
    }
}

impl Default for BlockingRequestSetInner {
    fn default() -> Self {
        Self {
            idle: Condvar::new(),
            state: Mutex::new(BlockingRequestState {
                admission_open: true,
                requests: Vec::new(),
            }),
        }
    }
}

struct ActiveBlockingRequest {
    cancellation: Arc<AtomicBool>,
    requests: BlockingRequestSet,
}

impl ActiveBlockingRequest {
    fn cancellation_signal(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancellation)
    }
}

impl Drop for ActiveBlockingRequest {
    fn drop(&mut self) {
        self.requests.finish_request(&self.cancellation);
    }
}

fn recover<T>(result: LockResult<T>) -> T {
    match result {
        Ok(value) => value,
        Err(poisoned) => {
            tracing::warn!("recovered from poisoned mutex");
            poisoned.into_inner()
        }
    }
}

fn json_response(
    status: StatusCode,
    body: &serde_json::Value,
) -> Response<Full<Bytes>> {
    let bytes = match serde_json::to_vec(body) {
        Ok(bytes) => bytes,
        Err(_) => b"{\"message\":\"failed to serialize response\"}".to_vec(),
    };

    fixed_response(status, "application/json", bytes)
}

fn shutting_down_response() -> Response<Full<Bytes>> {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({ "message": "server shutting down" }),
    )
}

fn fixed_response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let content_length = body.len();
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(CONNECTION, HeaderValue::from_static("close"));
    if let Ok(content_length) =
        HeaderValue::from_str(&content_length.to_string())
    {
        headers.insert(CONTENT_LENGTH, content_length);
    }
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyCaptureMode {
    None,
    PartialForWireLog,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RequestBodyError {
    PayloadTooLarge { partial_body: Option<Vec<u8>> },
    Read { partial_body: Option<Vec<u8>> },
}

impl RequestBodyError {
    fn payload_too_large(
        capture_mode: BodyCaptureMode,
        partial_body: Vec<u8>,
    ) -> Self {
        Self::PayloadTooLarge {
            partial_body: capture_mode.capture(partial_body),
        }
    }

    fn read(capture_mode: BodyCaptureMode, partial_body: Vec<u8>) -> Self {
        Self::Read { partial_body: capture_mode.capture(partial_body) }
    }

    fn partial_body(&self) -> Option<&[u8]> {
        match self {
            Self::PayloadTooLarge { partial_body }
            | Self::Read { partial_body } => partial_body.as_deref(),
        }
    }
}

impl BodyCaptureMode {
    fn capture(self, body: Vec<u8>) -> Option<Vec<u8>> {
        match self {
            Self::None => None,
            Self::PartialForWireLog => Some(body),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockingRequestSet, BlockingRequestShutdownOutcome, BodyCaptureMode,
        RequestBodyError, collect_request_body, finish_server_shutdown,
    };
    use crate::StartupError;
    use bytes::Bytes;
    use hyper::body::{Body, Frame, SizeHint};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct TestBody {
        frames: VecDeque<Result<Frame<Bytes>, ()>>,
    }

    impl TestBody {
        fn from_results(
            results: impl IntoIterator<Item = Result<&'static [u8], ()>>,
        ) -> Self {
            let frames = results
                .into_iter()
                .map(|result| {
                    result.map(|bytes| Frame::data(Bytes::from_static(bytes)))
                })
                .collect();
            Self { frames }
        }
    }

    impl Body for TestBody {
        type Data = Bytes;
        type Error = ();

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.frames.pop_front())
        }

        fn is_end_stream(&self) -> bool {
            self.frames.is_empty()
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    #[tokio::test]
    async fn collect_request_body_skips_partial_capture_when_wire_log_is_disabled()
     {
        let error = collect_request_body(
            TestBody::from_results([
                Ok(&b"1234"[..]),
                Ok(&b"5678"[..]),
                Ok(&b"9"[..]),
            ]),
            8,
            BodyCaptureMode::None,
        )
        .await
        .expect_err("oversized body should fail");

        assert_eq!(
            error,
            RequestBodyError::PayloadTooLarge { partial_body: None }
        );
    }

    #[tokio::test]
    async fn collect_request_body_retains_partial_capture_for_wire_log() {
        let error = collect_request_body(
            TestBody::from_results([
                Ok(&b"1234"[..]),
                Ok(&b"5678"[..]),
                Ok(&b"9"[..]),
            ]),
            8,
            BodyCaptureMode::PartialForWireLog,
        )
        .await
        .expect_err("oversized body should fail");

        assert_eq!(
            error,
            RequestBodyError::PayloadTooLarge {
                partial_body: Some(b"12345678".to_vec()),
            }
        );
    }

    #[tokio::test]
    async fn collect_request_body_retains_partial_reads_only_for_wire_log() {
        let without_logging = collect_request_body(
            TestBody::from_results([Ok(&b"1234"[..]), Err(())]),
            8,
            BodyCaptureMode::None,
        )
        .await
        .expect_err("read failure should surface");
        let with_logging = collect_request_body(
            TestBody::from_results([Ok(&b"1234"[..]), Err(())]),
            8,
            BodyCaptureMode::PartialForWireLog,
        )
        .await
        .expect_err("read failure should surface");

        assert_eq!(
            without_logging,
            RequestBodyError::Read { partial_body: None }
        );
        assert_eq!(
            with_logging,
            RequestBodyError::Read { partial_body: Some(b"1234".to_vec()) }
        );
    }

    #[test]
    fn blocking_request_set_rejects_new_requests_after_shutdown_starts() {
        let blocking_requests = BlockingRequestSet::default();
        let active_request = blocking_requests
            .begin_request()
            .expect("first request should be admitted");

        blocking_requests.begin_shutdown();

        assert!(
            blocking_requests.begin_request().is_none(),
            "shutdown should close request admission",
        );
        assert!(
            active_request.cancellation_signal().load(Ordering::SeqCst),
            "shutdown should cancel active requests",
        );
    }

    #[test]
    fn blocking_request_set_reports_shutdown_timeout_with_active_request_count()
     {
        let blocking_requests = BlockingRequestSet::default();
        let _first = blocking_requests
            .begin_request()
            .expect("first request should be admitted");
        let _second = blocking_requests
            .begin_request()
            .expect("second request should be admitted");

        assert_eq!(
            blocking_requests.wait_for_idle(Duration::from_millis(10)),
            BlockingRequestShutdownOutcome::TimedOut { active_requests: 2 }
        );
    }

    #[test]
    fn finish_server_shutdown_tears_down_and_returns_error_after_timeout() {
        let shutdown_called = AtomicBool::new(false);

        let error = finish_server_shutdown(
            BlockingRequestShutdownOutcome::TimedOut { active_requests: 1 },
            || shutdown_called.store(true, Ordering::SeqCst),
        )
        .expect_err("shutdown timeout should return error");

        assert!(matches!(
            error,
            StartupError::ShutdownTimeout { active_requests: 1 }
        ));
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "runtime teardown should run even after timeout",
        );
    }

    #[test]
    fn finish_server_shutdown_runs_runtime_teardown_after_requests_finish() {
        let shutdown_called = AtomicBool::new(false);

        let result = finish_server_shutdown(
            BlockingRequestShutdownOutcome::Idle,
            || shutdown_called.store(true, Ordering::SeqCst),
        );

        assert!(result.is_ok(), "idle shutdown should succeed cleanly");
        assert!(
            shutdown_called.load(Ordering::SeqCst),
            "runtime teardown should run after blocking work is idle",
        );
    }
}
