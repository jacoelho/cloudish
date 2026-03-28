#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::unwrap_used,
        clippy::panic,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT_DIRECTORY_ID: AtomicUsize = AtomicUsize::new(0);

/// Creates a unique temporary directory path for test data.
///
/// The directory creation is best-effort; callers typically fail naturally
/// later if the directory cannot be created in the current environment.
pub fn temporary_directory(label: &str) -> PathBuf {
    let id = NEXT_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir()
        .join(format!("cloudish-{label}-{}-{id}", std::process::id()));

    if path.exists() {
        let _ = std::fs::remove_dir_all(&path);
    }

    let _ = std::fs::create_dir_all(&path);
    path
}

/// Sends a UTF-8 HTTP request and collects the UTF-8 response body.
///
/// # Errors
///
/// Returns `std::io::Error` when the request cannot be sent or the response is
/// not valid UTF-8.
pub fn send_http_request(
    address: SocketAddr,
    request: &str,
) -> std::io::Result<String> {
    let response = send_http_request_bytes(address, request.as_bytes())?;
    String::from_utf8(response).map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
    })
}

/// Sends raw HTTP request bytes and collects the full response bytes.
///
/// # Errors
///
/// Returns `std::io::Error` when the connection cannot be established, the
/// request cannot be written, or the response framing is invalid.
pub fn send_http_request_bytes(
    address: SocketAddr,
    request: &[u8],
) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(request)?;
    stream.shutdown(Shutdown::Write)?;

    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                let Some(chunk) = buffer.get(..read) else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "read past buffer bounds",
                    ));
                };
                response.extend_from_slice(chunk);

                if response_complete(&response)? {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                ) && response_complete(&response)? =>
            {
                break;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(response)
}

fn response_complete(response: &[u8]) -> std::io::Result<bool> {
    let Some(header_end) = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
    else {
        return Ok(false);
    };
    let Some(header_bytes) = response.get(..header_end) else {
        return Ok(false);
    };
    let headers = std::str::from_utf8(header_bytes).map_err(|error| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
    })?;
    let content_length = headers
        .split("\r\n")
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then_some(value.trim())
        })
        .map(|value| {
            value.parse::<usize>().map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, error)
            })
        })
        .transpose()?;
    if let Some(content_length) = content_length {
        return Ok(response.len() >= header_end + content_length);
    }

    let transfer_encoding = headers.split("\r\n").find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("transfer-encoding").then_some(value.trim())
    });
    if transfer_encoding
        .map(|value| value.eq_ignore_ascii_case("chunked"))
        .unwrap_or(false)
    {
        let Some(body) = response.get(header_end..) else {
            return Ok(false);
        };
        return chunked_body_complete(body);
    }

    Ok(false)
}

fn chunked_body_complete(body: &[u8]) -> std::io::Result<bool> {
    let mut offset = 0;

    loop {
        let Some(remaining) = body.get(offset..) else {
            return Ok(false);
        };
        let Some(size_end) =
            remaining.windows(2).position(|window| window == b"\r\n")
        else {
            return Ok(false);
        };
        let Some(size_bytes) = body.get(offset..offset + size_end) else {
            return Ok(false);
        };
        let size_line = std::str::from_utf8(size_bytes).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error)
        })?;
        let size = usize::from_str_radix(
            size_line.split(';').next().unwrap_or_default().trim(),
            16,
        )
        .map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error)
        })?;
        offset += size_end + 2;

        if body.len() < offset + size + 2 {
            return Ok(false);
        }
        let Some(chunk_suffix) = body.get(offset + size..offset + size + 2)
        else {
            return Ok(false);
        };
        if chunk_suffix != b"\r\n" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked response chunk missing CRLF terminator",
            ));
        }

        offset += size + 2;
        if size == 0 {
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        send_http_request, send_http_request_bytes, temporary_directory,
    };
    use std::io::Read;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::atomic::Ordering;
    use std::sync::{Mutex, OnceLock};
    use std::thread;

    fn directory_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn temporary_directory_creates_a_unique_directory() {
        let _guard =
            directory_test_lock().lock().expect("directory tests should lock");
        let path_a = temporary_directory("support-a");
        let path_b = temporary_directory("support-b");

        assert!(path_a.exists());
        assert!(path_b.exists());
        assert_ne!(path_a, path_b);

        let _ = std::fs::remove_dir_all(path_a);
        let _ = std::fs::remove_dir_all(path_b);
    }

    #[test]
    fn temporary_directory_replaces_a_stale_directory() {
        let _guard =
            directory_test_lock().lock().expect("directory tests should lock");
        let id = super::NEXT_DIRECTORY_ID.load(Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cloudish-support-stale-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)
            .expect("stale directory should be creatable");
        std::fs::write(path.join("stale.txt"), "stale")
            .expect("stale file should be writable");

        let recreated = temporary_directory("support-stale");

        assert_eq!(recreated, path);
        assert!(recreated.exists());
        assert!(!recreated.join("stale.txt").exists());

        let _ = std::fs::remove_dir_all(recreated);
    }

    #[test]
    fn send_http_request_reads_the_full_response() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("request should connect to the listener");
            let mut request = String::new();
            stream
                .read_to_string(&mut request)
                .expect("request should be readable");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .expect("response should be written");
        });

        let response = send_http_request(
            address,
            "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should succeed");

        server.join().expect("server thread should finish");

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.ends_with("ok"));
    }

    #[test]
    fn send_http_request_bytes_preserves_binary_payloads() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("request should connect to the listener");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .expect("request should be readable");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n\xff\x00",
                )
                .expect("response should be written");
        });

        let response = send_http_request_bytes(
            address,
            b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should succeed");

        server.join().expect("server thread should finish");

        assert!(response.ends_with(&[0xff, 0x00]));
    }

    #[test]
    fn send_http_request_bytes_stops_at_content_length_without_socket_close() {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("request should connect to the listener");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .expect("request should be readable");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .expect("response should be written");
            thread::sleep(std::time::Duration::from_secs(3));
        });

        let response = send_http_request_bytes(
            address,
            b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should succeed");

        server.join().expect("server thread should finish");

        assert!(response.ends_with(b"ok"));
    }
}
