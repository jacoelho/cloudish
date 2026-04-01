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
use reqwest::Client;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use test_support::temporary_directory;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, MutexGuard, oneshot};

pub struct RuntimeServer {
    address: SocketAddr,
    handle: Option<tokio::task::JoinHandle<Result<(), app::StartupError>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    state_directory: PathBuf,
}

impl RuntimeServer {
    pub async fn spawn(label: &str) -> Self {
        let (defaults, state_directory) = runtime_defaults(label);
        Self::spawn_with_defaults(defaults, state_directory).await
    }

    pub async fn spawn_with_runtime_defaults(
        defaults: aws::RuntimeDefaults,
    ) -> Self {
        let state_directory = defaults.state_directory().to_path_buf();
        Self::spawn_with_defaults(defaults, state_directory).await
    }

    pub async fn spawn_with_state_directory(state_directory: PathBuf) -> Self {
        let (defaults, state_directory) =
            runtime_defaults_for_state_directory(state_directory);
        Self::spawn_with_defaults(defaults, state_directory).await
    }

    async fn spawn_with_defaults(
        defaults: aws::RuntimeDefaults,
        state_directory: PathBuf,
    ) -> Self {
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

        wait_until_ready(address).await;

        Self {
            address,
            handle: Some(handle),
            shutdown_tx: Some(shutdown_tx),
            state_directory,
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn localhost_endpoint_url(&self) -> String {
        format!("http://localhost:{}", self.address.port())
    }

    pub fn state_directory(&self) -> &Path {
        &self.state_directory
    }

    pub async fn is_healthy(&self) -> bool {
        health_check(self.address).await
    }

    pub async fn shutdown(mut self) {
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

    async fn shutdown_if_running(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

pub struct SharedRuntime {
    label: &'static str,
    runtime: Mutex<Option<RuntimeServer>>,
}

impl SharedRuntime {
    pub const fn new(label: &'static str) -> Self {
        Self { label, runtime: Mutex::const_new(None) }
    }

    pub async fn acquire(&'static self) -> SharedRuntimeLease<'static> {
        let mut guard = self.runtime.lock().await;
        let needs_restart = match guard.as_ref() {
            Some(runtime) => !runtime.is_healthy().await,
            None => true,
        };
        if needs_restart {
            if let Some(runtime) = guard.take() {
                runtime.shutdown_if_running().await;
            }
            *guard = Some(RuntimeServer::spawn(self.label).await);
        }

        SharedRuntimeLease { guard }
    }
}

pub struct SharedRuntimeLease<'a> {
    guard: MutexGuard<'a, Option<RuntimeServer>>,
}

impl Deref for SharedRuntimeLease<'_> {
    type Target = RuntimeServer;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("shared runtime lease should hold a runtime server")
    }
}

fn runtime_defaults(label: &str) -> (aws::RuntimeDefaults, PathBuf) {
    let state_directory = temporary_directory(label).join("state");
    runtime_defaults_for_state_directory(state_directory)
}

fn runtime_defaults_for_state_directory(
    state_directory: PathBuf,
) -> (aws::RuntimeDefaults, PathBuf) {
    let defaults = aws::RuntimeDefaults::try_new(
        Some("000000000000".to_owned()),
        Some("eu-west-2".to_owned()),
        Some(state_directory.to_string_lossy().into_owned()),
    )
    .expect("test defaults should be valid");

    (defaults, state_directory)
}

async fn wait_until_ready(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);

    while Instant::now() < deadline {
        if health_check(address).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("runtime app at {address} did not become healthy within 5 seconds");
}

async fn health_check(address: SocketAddr) -> bool {
    let client = Client::new();
    let url = format!("http://{address}/__cloudish/health");

    matches!(
        client.get(&url).send().await,
        Ok(response) if response.status().is_success()
    )
}
