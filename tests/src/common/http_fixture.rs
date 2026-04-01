#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::thread::{self, JoinHandle};

pub struct OneShotHttpServer {
    address: SocketAddr,
    handle: Option<JoinHandle<()>>,
}

impl OneShotHttpServer {
    pub fn spawn(response: Vec<u8>) -> Self {
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

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn join(mut self) {
        self.handle
            .take()
            .expect("fixture should retain its worker")
            .join()
            .expect("fixture worker should finish");
    }
}
