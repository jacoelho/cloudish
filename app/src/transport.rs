use crate::StartupError;
use bytes::Bytes;
use http::{EdgeRequest, EdgeResponse, EdgeRouter};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
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
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinError, JoinSet};

pub(crate) async fn serve_listener_with_shutdown<F>(
    listener: TcpListener,
    router: EdgeRouter,
    max_request_bytes: usize,
    shutdown: F,
) -> Result<(), StartupError>
where
    F: Future<Output = ()>,
{
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            () = &mut shutdown => {
                connections.abort_all();
                drain_connection_tasks(&mut connections).await;
                router.shutdown();
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(source) => {
                        router.shutdown();
                        return Err(StartupError::Accept { source });
                    }
                };
                let router = router.clone();
                connections.spawn(async move {
                    serve_connection(stream, router, max_request_bytes).await;
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
        std::panic::resume_unwind(error.into_panic());
    }
}

async fn serve_connection(
    stream: TcpStream,
    router: EdgeRouter,
    max_request_bytes: usize,
) {
    let source_ip =
        stream.peer_addr().ok().map(|address| address.ip().to_string());
    let io = TokioIo::new(stream);
    let service = service_fn(move |request| {
        let router = router.clone();
        let source_ip = source_ip.clone();
        async move {
            Ok::<_, Infallible>(
                handle_request(router, request, max_request_bytes, source_ip)
                    .await,
            )
        }
    });

    let _ = http1::Builder::new()
        .half_close(true)
        .serve_connection(io, service)
        .await;
}

async fn handle_request(
    router: EdgeRouter,
    request: Request<Incoming>,
    max_request_bytes: usize,
    source_ip: Option<String>,
) -> Response<Full<Bytes>> {
    let (parts, body) = request.into_parts();
    let body = match collect_request_body(body, max_request_bytes).await {
        Ok(body) => body,
        Err(RequestBodyError::PayloadTooLarge) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &json!({
                    "message": format!(
                        "request body exceeds configured limit of {max_request_bytes} bytes"
                    )
                }),
            );
        }
        Err(RequestBodyError::Read) => {
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

    edge_response(router.handle_request(request))
}

async fn collect_request_body(
    mut body: Incoming,
    max_request_bytes: usize,
) -> Result<Vec<u8>, RequestBodyError> {
    let mut decoded = Vec::new();

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| RequestBodyError::Read)?;
        if let Some(data) = frame.data_ref() {
            let next_len = decoded
                .len()
                .checked_add(data.len())
                .ok_or(RequestBodyError::PayloadTooLarge)?;
            if next_len > max_request_bytes {
                return Err(RequestBodyError::PayloadTooLarge);
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

fn edge_response(response: EdgeResponse) -> Response<Full<Bytes>> {
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

fn fixed_response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body.clone())));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(CONNECTION, HeaderValue::from_static("close"));
    if let Ok(content_length) = HeaderValue::from_str(&body.len().to_string())
    {
        headers.insert(CONTENT_LENGTH, content_length);
    }
    response
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestBodyError {
    PayloadTooLarge,
    Read,
}
