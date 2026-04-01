use aws::{
    Endpoint, HttpForwardRequest, HttpForwardResponse, HttpForwarder,
    InfrastructureError, RunningTcpProxy, TcpProxyRuntime, TcpProxySpec,
};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadTcpProxyRuntime;

impl ThreadTcpProxyRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl TcpProxyRuntime for ThreadTcpProxyRuntime {
    fn start(
        &self,
        spec: &TcpProxySpec,
    ) -> Result<Box<dyn RunningTcpProxy>, InfrastructureError> {
        let listener = TcpListener::bind(endpoint_address(spec.listen()))
            .map_err(|source| {
                InfrastructureError::tcp_proxy("bind", spec.listen(), source)
            })?;
        listener.set_nonblocking(true).map_err(|source| {
            InfrastructureError::tcp_proxy("configure", spec.listen(), source)
        })?;
        let local_address = listener.local_addr().map_err(|source| {
            InfrastructureError::tcp_proxy(
                "discover-address",
                spec.listen(),
                source,
            )
        })?;
        let listen = Endpoint::new(
            local_address.ip().to_string(),
            local_address.port(),
        );
        let upstream = spec.upstream().clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let thread_name = format!("tcp-proxy-{}", listen.port());
        let join = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((downstream, _)) => {
                            let upstream = upstream.clone();
                            thread::spawn(move || {
                                let Ok(()) = downstream.set_nonblocking(false)
                                else {
                                    return;
                                };
                                if let Ok(upstream) = TcpStream::connect(
                                    endpoint_address(&upstream),
                                ) {
                                    let _ = upstream.set_nonblocking(false);
                                    let _ = relay_bidirectional(
                                        downstream, upstream,
                                    );
                                }
                            });
                        }
                        Err(source)
                            if source.kind() == io::ErrorKind::WouldBlock =>
                        {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(source)
                            if source.kind() == io::ErrorKind::Interrupted => {
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|source| {
                InfrastructureError::tcp_proxy("spawn", &listen, source)
            })?;

        Ok(Box::new(ThreadTcpProxyHandle {
            join: Mutex::new(Some(join)),
            listen,
            stop,
        }))
    }
}

struct ThreadTcpProxyHandle {
    join: Mutex<Option<JoinHandle<()>>>,
    listen: Endpoint,
    stop: Arc<AtomicBool>,
}

impl RunningTcpProxy for ThreadTcpProxyHandle {
    fn listen_endpoint(&self) -> Endpoint {
        self.listen.clone()
    }

    fn stop(&self) -> Result<(), InfrastructureError> {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(endpoint_address(&self.listen));

        if let Some(join) = recover(self.join.lock()).take() {
            join.join().map_err(|_| {
                InfrastructureError::tcp_proxy(
                    "stop",
                    &self.listen,
                    io::Error::other("proxy worker panicked"),
                )
            })?;
        }

        Ok(())
    }
}

impl Drop for ThreadTcpProxyHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TcpHttpForwarder;

const HTTP_FORWARD_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_FORWARD_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_FORWARD_IO_POLL_INTERVAL: Duration = Duration::from_millis(50);

impl TcpHttpForwarder {
    pub fn new() -> Self {
        Self
    }
}

impl HttpForwarder for TcpHttpForwarder {
    fn forward(
        &self,
        request: &HttpForwardRequest,
    ) -> Result<HttpForwardResponse, InfrastructureError> {
        self.forward_with_cancellation(request, &|| false)
    }

    fn forward_with_cancellation(
        &self,
        request: &HttpForwardRequest,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<HttpForwardResponse, InfrastructureError> {
        let mut stream = connect_stream(request.endpoint(), is_cancelled)?;
        configure_forward_stream(&stream, request.endpoint())?;
        write_request(&mut stream, request, request.endpoint(), is_cancelled)?;
        read_response(&mut stream, request.endpoint(), is_cancelled)
    }
}

fn configure_forward_stream(
    stream: &TcpStream,
    endpoint: &Endpoint,
) -> Result<(), InfrastructureError> {
    stream.set_read_timeout(Some(HTTP_FORWARD_IO_POLL_INTERVAL)).map_err(
        |source| {
            InfrastructureError::http_forward(
                "set-read-timeout",
                endpoint,
                source,
            )
        },
    )?;
    stream.set_write_timeout(Some(HTTP_FORWARD_IO_POLL_INTERVAL)).map_err(
        |source| {
            InfrastructureError::http_forward(
                "set-write-timeout",
                endpoint,
                source,
            )
        },
    )?;

    Ok(())
}

fn connect_stream(
    endpoint: &Endpoint,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<TcpStream, InfrastructureError> {
    let addresses = endpoint_address(endpoint)
        .to_socket_addrs()
        .map_err(|source| {
            InfrastructureError::http_forward("resolve", endpoint, source)
        })?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(InfrastructureError::http_forward(
            "resolve",
            endpoint,
            io::Error::new(
                io::ErrorKind::NotFound,
                "endpoint did not resolve to any socket addresses",
            ),
        ));
    }
    let deadline = Instant::now() + HTTP_FORWARD_CONNECT_TIMEOUT;
    let mut last_retryable_error = None;

    while Instant::now() < deadline {
        if is_cancelled() {
            return Err(cancelled_forward_error(endpoint));
        }
        for address in &addresses {
            match TcpStream::connect_timeout(
                address,
                HTTP_FORWARD_IO_POLL_INTERVAL,
            ) {
                Ok(stream) => return Ok(stream),
                Err(source) if is_retryable_forward_error(&source) => {
                    last_retryable_error = Some(source);
                }
                Err(source) => {
                    return Err(InfrastructureError::http_forward(
                        "connect", endpoint, source,
                    ));
                }
            }
        }
    }

    Err(InfrastructureError::http_forward(
        "connect",
        endpoint,
        last_retryable_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP forward connect timed out",
            )
        }),
    ))
}

fn write_request(
    stream: &mut TcpStream,
    request: &HttpForwardRequest,
    endpoint: &Endpoint,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<(), InfrastructureError> {
    let request_bytes = build_request_bytes(request);
    write_with_cancellation(stream, endpoint, &request_bytes, is_cancelled)?;
    if is_cancelled() {
        cancel_stream(stream);
        return Err(cancelled_forward_error(endpoint));
    }
    stream.shutdown(Shutdown::Write).map_err(|source| {
        InfrastructureError::http_forward("shutdown-write", endpoint, source)
    })?;

    Ok(())
}

fn write_with_cancellation(
    stream: &mut TcpStream,
    endpoint: &Endpoint,
    bytes: &[u8],
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<(), InfrastructureError> {
    let mut idle_deadline = Instant::now() + HTTP_FORWARD_IDLE_TIMEOUT;
    let mut written = 0;
    while written < bytes.len() {
        let remaining = bytes.get(written..).ok_or_else(|| {
            InfrastructureError::http_forward(
                "write",
                endpoint,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP forward write offset exceeded request length",
                ),
            )
        })?;
        if is_cancelled() {
            cancel_stream(stream);
            return Err(cancelled_forward_error(endpoint));
        }
        match stream.write(remaining) {
            Ok(0) => {
                return Err(InfrastructureError::http_forward(
                    "write",
                    endpoint,
                    io::Error::new(
                        io::ErrorKind::WriteZero,
                        "HTTP forward write returned zero bytes",
                    ),
                ));
            }
            Ok(count) => {
                written += count;
                idle_deadline = Instant::now() + HTTP_FORWARD_IDLE_TIMEOUT;
            }
            Err(source) if is_retryable_forward_error(&source) => {
                if Instant::now() >= idle_deadline {
                    return Err(InfrastructureError::http_forward(
                        "write",
                        endpoint,
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "HTTP forward write timed out waiting for progress",
                        ),
                    ));
                }
            }
            Err(source) => {
                return Err(InfrastructureError::http_forward(
                    "write", endpoint, source,
                ));
            }
        }
    }

    Ok(())
}

fn read_response(
    stream: &mut TcpStream,
    endpoint: &Endpoint,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<HttpForwardResponse, InfrastructureError> {
    let mut idle_deadline = Instant::now() + HTTP_FORWARD_IDLE_TIMEOUT;
    let mut response = Vec::new();
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        if is_cancelled() {
            cancel_stream(stream);
            return Err(cancelled_forward_error(endpoint));
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => {
                let bytes = chunk.get(..count).ok_or_else(|| {
                    InfrastructureError::http_forward(
                        "read",
                        endpoint,
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "HTTP forward read exceeded buffer length",
                        ),
                    )
                })?;
                response.extend_from_slice(bytes);
                idle_deadline = Instant::now() + HTTP_FORWARD_IDLE_TIMEOUT;
            }
            Err(source) if is_retryable_forward_error(&source) => {
                if Instant::now() >= idle_deadline {
                    return Err(InfrastructureError::http_forward(
                        "read",
                        endpoint,
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "HTTP forward read timed out waiting for progress",
                        ),
                    ));
                }
            }
            Err(source) => {
                return Err(InfrastructureError::http_forward(
                    "read", endpoint, source,
                ));
            }
        }
    }

    parse_response(&response, endpoint)
}

fn is_retryable_forward_error(source: &io::Error) -> bool {
    matches!(
        source.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::WouldBlock
    )
}

fn cancelled_forward_error(endpoint: &Endpoint) -> InfrastructureError {
    InfrastructureError::http_forward(
        "cancel",
        endpoint,
        io::Error::other("HTTP forward cancelled"),
    )
}

fn cancel_stream(stream: &TcpStream) {
    let _ = stream.shutdown(Shutdown::Both);
}

fn build_request_bytes(request: &HttpForwardRequest) -> Vec<u8> {
    let path = if request.path().starts_with('/') {
        request.path().to_owned()
    } else {
        format!("/{}", request.path())
    };
    let mut bytes = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        request.method(),
        path,
        request.endpoint(),
        request.body().len()
    )
    .into_bytes();

    for (name, value) in request.headers() {
        if name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }

        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(b": ");
        bytes.extend_from_slice(value.as_bytes());
        bytes.extend_from_slice(b"\r\n");
    }

    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(request.body());
    bytes
}

fn copy_stream(
    mut reader: TcpStream,
    mut writer: TcpStream,
) -> io::Result<()> {
    io::copy(&mut reader, &mut writer)?;
    let _ = writer.shutdown(Shutdown::Write);
    Ok(())
}

fn endpoint_address(endpoint: &Endpoint) -> String {
    format!("{}:{}", endpoint.host(), endpoint.port())
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn join_copy_thread(join: JoinHandle<io::Result<()>>) -> io::Result<()> {
    join.join().map_err(|_| io::Error::other("copy worker panicked"))?
}

fn parse_response(
    response: &[u8],
    endpoint: &Endpoint,
) -> Result<HttpForwardResponse, InfrastructureError> {
    let headers_end = header_end(response).ok_or_else(|| {
        InfrastructureError::http_forward(
            "parse-response",
            endpoint,
            io::Error::new(
                io::ErrorKind::InvalidData,
                "response is missing the header terminator",
            ),
        )
    })?;
    let headers =
        std::str::from_utf8(response.get(..headers_end).ok_or_else(|| {
            InfrastructureError::http_forward(
                "parse-response",
                endpoint,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response headers exceed buffer length",
                ),
            )
        })?)
        .map_err(|source| {
            InfrastructureError::http_forward(
                "parse-response",
                endpoint,
                io::Error::new(io::ErrorKind::InvalidData, source),
            )
        })?;
    let mut lines = headers.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let mut status_parts = status_line.splitn(3, ' ');
    let protocol = status_parts.next().unwrap_or_default();
    let status_code = status_parts.next().unwrap_or_default();
    let reason = status_parts.next().unwrap_or_default();

    if !protocol.starts_with("HTTP/") || status_code.is_empty() {
        return Err(InfrastructureError::http_forward(
            "parse-response",
            endpoint,
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid HTTP status line `{status_line}`"),
            ),
        ));
    }

    let status_code = status_code.parse::<u16>().map_err(|source| {
        InfrastructureError::http_forward(
            "parse-response",
            endpoint,
            io::Error::new(io::ErrorKind::InvalidData, source),
        )
    })?;
    let parsed_headers = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid header `{line}`"),
                )
            })?;
            Ok((name.trim().to_owned(), value.trim().to_owned()))
        })
        .collect::<io::Result<Vec<_>>>()
        .map_err(|source| {
            InfrastructureError::http_forward(
                "parse-response",
                endpoint,
                source,
            )
        })?;

    Ok(HttpForwardResponse::new(
        status_code,
        reason,
        parsed_headers,
        response
            .get(headers_end..)
            .ok_or_else(|| {
                InfrastructureError::http_forward(
                    "parse-response",
                    endpoint,
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "response body offset exceeds buffer length",
                    ),
                )
            })?
            .to_vec(),
    ))
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn relay_bidirectional(
    downstream: TcpStream,
    upstream: TcpStream,
) -> io::Result<()> {
    let downstream_reader = downstream.try_clone()?;
    let upstream_writer = upstream.try_clone()?;
    let forward =
        thread::spawn(move || copy_stream(downstream_reader, upstream_writer));
    let backward = thread::spawn(move || copy_stream(upstream, downstream));

    join_copy_thread(forward)?;
    join_copy_thread(backward)
}

#[cfg(test)]
mod tests {
    use super::TcpHttpForwarder;
    use aws::{Endpoint, HttpForwardRequest, HttpForwarder};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn tcp_http_forwarder_cancels_stalled_response_reads() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let endpoint = Endpoint::localhost(
            listener
                .local_addr()
                .expect("listener should expose its address")
                .port(),
        );
        let (received_tx, received_rx) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let release_server = Arc::clone(&release);
        let server = thread::spawn(move || {
            let (mut stream, _) =
                listener.accept().expect("forwarded request should connect");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .expect("server should read the forwarded request");
            received_tx
                .send(String::from_utf8_lossy(&request).into_owned())
                .expect("test should observe the request");
            while !release_server.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
            }
        });
        let forwarder = TcpHttpForwarder::new();
        let cancelled = Arc::new(AtomicBool::new(false));
        let trigger = Arc::clone(&cancelled);
        let cancel_on_request = thread::spawn(move || {
            let _ = received_rx
                .recv()
                .expect("test should wait for the forwarded request");
            thread::sleep(Duration::from_millis(50));
            trigger.store(true, Ordering::SeqCst);
        });
        let request = HttpForwardRequest::new(endpoint, "POST", "/upstream")
            .with_body("payload");

        let started_at = Instant::now();
        let error = forwarder
            .forward_with_cancellation(&request, &|| {
                cancelled.load(Ordering::SeqCst)
            })
            .expect_err("cancelled forward should fail");

        release.store(true, Ordering::SeqCst);
        cancel_on_request
            .join()
            .expect("cancellation thread should finish cleanly");
        server.join().expect("server thread should finish cleanly");

        assert!(
            started_at.elapsed() < Duration::from_secs(1),
            "cancellation should stop the forward promptly",
        );
        assert!(
            error.to_string().contains("HTTP forward cancelled"),
            "unexpected cancellation error: {error}",
        );
    }

    #[test]
    fn tcp_http_forwarder_allows_slow_responses_that_keep_making_progress() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let endpoint = Endpoint::localhost(
            listener
                .local_addr()
                .expect("listener should expose its address")
                .port(),
        );
        let server = thread::spawn(move || {
            let (mut stream, _) =
                listener.accept().expect("forwarded request should connect");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .expect("server should read the forwarded request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
                )
                .expect("server should write response headers");
            thread::sleep(Duration::from_millis(1200));
            stream
                .write_all(b"da")
                .expect("server should write the first response chunk");
            thread::sleep(Duration::from_millis(1200));
            stream
                .write_all(b"ta")
                .expect("server should write the second response chunk");
        });
        let forwarder = TcpHttpForwarder::new();
        let request = HttpForwardRequest::new(endpoint, "POST", "/upstream")
            .with_body("payload");

        let started_at = Instant::now();
        let response = forwarder
            .forward(&request)
            .expect("slow-but-progressing forward should succeed");

        server.join().expect("server thread should finish cleanly");

        assert!(
            started_at.elapsed() > Duration::from_secs(2),
            "the regression needs total elapsed time to exceed the idle budget",
        );
        assert_eq!(response.status_code(), 200);
        assert_eq!(response.body(), b"data");
    }
}
