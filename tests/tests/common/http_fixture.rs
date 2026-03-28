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
use serde_json::Value;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::thread::{self, JoinHandle};

pub(crate) struct OneShotHttpServer {
    address: SocketAddr,
    handle: Option<JoinHandle<()>>,
}

impl OneShotHttpServer {
    pub(crate) fn spawn(response: Vec<u8>) -> Self {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("fixture should bind");
        let address =
            listener.local_addr().expect("fixture should expose an address");
        let handle = thread::spawn(move || {
            let (mut stream, _) =
                listener.accept().expect("fixture should accept one request");
            let mut request = Vec::new();
            stream
                .read_to_end(&mut request)
                .expect("fixture request should be readable");
            stream
                .write_all(&response)
                .expect("fixture response should be writable");
        });

        Self { address, handle: Some(handle) }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn join(mut self) {
        self.handle
            .take()
            .expect("fixture should retain its worker")
            .join()
            .expect("fixture worker should finish");
    }
}

#[allow(dead_code)]
pub(crate) fn json_response(
    status: &str,
    date: &str,
    body: &Value,
) -> Vec<u8> {
    let body = serde_json::to_vec(body)
        .expect("fixture JSON responses should serialize");

    format!(
        "{status}\r\nDate: {date}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body)
    .collect()
}
