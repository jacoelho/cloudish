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
use app::CloudishApp;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use test_support::temporary_directory;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub(crate) struct RuntimeServer {
    address: SocketAddr,
    handle: Option<tokio::task::JoinHandle<Result<(), app::StartupError>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    #[allow(dead_code)]
    state_directory: PathBuf,
}

impl RuntimeServer {
    pub(crate) async fn spawn(label: &str) -> Self {
        let (defaults, state_directory) = runtime_defaults(label);
        let server = CloudishApp::from_runtime_defaults(defaults)
            .expect("app bootstrap should succeed");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_listener_with_shutdown(listener, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            address,
            handle: Some(handle),
            shutdown_tx: Some(shutdown_tx),
            state_directory,
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    #[allow(dead_code)]
    pub(crate) fn state_directory(&self) -> &Path {
        &self.state_directory
    }

    pub(crate) async fn shutdown(mut self) {
        self.shutdown_tx
            .take()
            .expect("runtime app should expose a shutdown channel")
            .send(())
            .expect("runtime app should still be running");
        self.handle
            .take()
            .expect("runtime app should expose a join handle")
            .await
            .expect("runtime app task should complete")
            .expect("runtime app should stop cleanly");
    }
}

fn runtime_defaults(label: &str) -> (aws::RuntimeDefaults, PathBuf) {
    let state_directory = temporary_directory(label).join("state");
    let defaults = aws::RuntimeDefaults::try_new(
        Some("000000000000".to_owned()),
        Some("eu-west-2".to_owned()),
        Some(state_directory.to_string_lossy().into_owned()),
    )
    .expect("test defaults should be valid");

    (defaults, state_directory)
}
