use aws::{
    Endpoint, HttpForwardRequest, HttpForwardResponse, HttpForwarder,
    InfrastructureError, RunningTcpProxy, TcpProxyRuntime, TcpProxySpec,
};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
        let mut stream = TcpStream::connect(endpoint_address(
            request.endpoint(),
        ))
        .map_err(|source| {
            InfrastructureError::http_forward(
                "connect",
                request.endpoint(),
                source,
            )
        })?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).map_err(
            |source| {
                InfrastructureError::http_forward(
                    "set-read-timeout",
                    request.endpoint(),
                    source,
                )
            },
        )?;
        stream.set_write_timeout(Some(Duration::from_secs(2))).map_err(
            |source| {
                InfrastructureError::http_forward(
                    "set-write-timeout",
                    request.endpoint(),
                    source,
                )
            },
        )?;

        let request_bytes = build_request_bytes(request);
        stream.write_all(&request_bytes).map_err(|source| {
            InfrastructureError::http_forward(
                "write",
                request.endpoint(),
                source,
            )
        })?;
        stream.shutdown(Shutdown::Write).map_err(|source| {
            InfrastructureError::http_forward(
                "shutdown-write",
                request.endpoint(),
                source,
            )
        })?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response).map_err(|source| {
            InfrastructureError::http_forward(
                "read",
                request.endpoint(),
                source,
            )
        })?;

        parse_response(&response, request.endpoint())
    }
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
