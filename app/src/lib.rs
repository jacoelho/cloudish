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

mod hosting;

use auth::Authenticator;
use aws::{
    DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, RuntimeDefaults,
    RuntimeDefaultsError, STATE_DIRECTORY_ENV,
};
#[cfg(feature = "dynamodb")]
use edge_runtime::DynamoDbInitError;
#[cfg(feature = "eventbridge")]
use edge_runtime::EventBridgeError;
#[cfg(feature = "s3")]
use edge_runtime::S3InitError;
#[cfg(feature = "lambda")]
use edge_runtime::{InfrastructureError, LambdaInitError};
use edge_runtime::{
    LocalRuntimeBuilder, ManagedBackgroundTasks, RuntimeBuildError,
    supported_services,
};
use hosting::HostingPlan;
use http::EdgeRouter;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use storage::StorageRuntime;
use tokio::net::{TcpListener, TcpStream};

const MAX_REQUEST_BYTES: usize = 64 * 1024;

pub use hosting::{EDGE_HOST_ENV, EDGE_PORT_ENV, HostingPlanError};

#[derive(Clone)]
pub struct CloudishApp {
    address: SocketAddr,
    #[cfg(feature = "lambda")]
    _lambda_background_tasks: Arc<ManagedBackgroundTasks>,
    router: EdgeRouter,
}

impl fmt::Debug for CloudishApp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CloudishApp")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl CloudishApp {
    /// Build a Cloudish application from process environment.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError`] when required runtime or hosting configuration
    /// is missing or invalid, the state directory cannot be created, or the
    /// runtime graph cannot be assembled.
    pub fn from_env() -> Result<Self, StartupError> {
        let defaults = load_runtime_defaults(|name| std::env::var(name).ok())?;
        let hosting = load_hosting_plan(|name| std::env::var(name).ok())?;
        Self::from_runtime_defaults_with_hosting(defaults, hosting)
    }

    /// Build a Cloudish application from explicit runtime defaults.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError`] when the state directory cannot be created or
    /// the runtime graph cannot be assembled.
    pub fn from_runtime_defaults(
        defaults: RuntimeDefaults,
    ) -> Result<Self, StartupError> {
        Self::from_runtime_defaults_with_hosting(
            defaults,
            HostingPlan::local(),
        )
    }

    fn from_runtime_defaults_with_hosting(
        defaults: RuntimeDefaults,
        hosting: HostingPlan,
    ) -> Result<Self, StartupError> {
        let storage = StorageRuntime::new(&defaults);
        std::fs::create_dir_all(storage.state_directory()).map_err(
            |source| StartupError::StateDirectory {
                path: storage.state_directory().to_path_buf(),
                source,
            },
        )?;

        let authenticator = Authenticator::new(defaults.clone());
        let (services, runtime, background_tasks) =
            LocalRuntimeBuilder::new(defaults.clone(), authenticator.clone())
                .build()
                .map_err(StartupError::from)
                .map(|assembly| assembly.into_parts())?;
        #[cfg(not(feature = "lambda"))]
        let _ = background_tasks;
        let router =
            EdgeRouter::new(defaults, authenticator, services, runtime);

        Ok(Self {
            address: hosting.edge_address(),
            #[cfg(feature = "lambda")]
            _lambda_background_tasks: Arc::new(background_tasks),
            router,
        })
    }

    pub fn edge_address(&self) -> SocketAddr {
        self.address
    }

    /// Bind the configured edge address and serve until shutdown resolves.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError`] when the edge listener cannot bind, a
    /// connection cannot be accepted, or an accepted connection cannot be
    /// served.
    pub async fn serve_with_shutdown<F>(
        self,
        shutdown: F,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
    {
        let listener = TcpListener::bind(self.edge_address()).await.map_err(
            |source| StartupError::Bind {
                address: self.edge_address(),
                source,
            },
        )?;

        self.serve_listener_with_shutdown(listener, shutdown).await
    }

    /// Serve on an existing listener until shutdown resolves.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError`] when the listener cannot accept a connection or
    /// an accepted connection cannot be served.
    pub async fn serve_listener_with_shutdown<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
    {
        let router = self.router;
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let (stream, _) = accepted
                        .map_err(|source| StartupError::Accept { source })?;
                    serve_connection(&router, stream).await?;
                }
            }
        }
    }
}

pub fn supported_service_count() -> usize {
    supported_services().len()
}

/// Start Cloudish from process environment and wait for `ctrl-c`.
///
/// # Errors
///
/// Returns [`StartupError`] when configuration is invalid, runtime startup
/// fails, the edge listener cannot bind, or serving a connection fails.
pub async fn run_from_env() -> Result<(), StartupError> {
    let app = CloudishApp::from_env()?;
    app.serve_with_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

/// Load runtime defaults from an environment reader.
///
/// # Errors
///
/// Returns [`StartupError`] when any required runtime default is missing or
/// invalid.
pub fn load_runtime_defaults<F>(
    mut read_env: F,
) -> Result<RuntimeDefaults, StartupError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    RuntimeDefaults::try_new(
        read_env(DEFAULT_ACCOUNT_ENV),
        read_env(DEFAULT_REGION_ENV),
        read_env(STATE_DIRECTORY_ENV),
    )
    .map_err(StartupError::Config)
}

pub(crate) fn load_hosting_plan<F>(
    read_env: F,
) -> Result<HostingPlan, StartupError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    HostingPlan::from_env(read_env).map_err(StartupError::HostingConfig)
}

async fn serve_connection(
    router: &EdgeRouter,
    stream: TcpStream,
) -> Result<(), StartupError> {
    let request = read_request(&stream).await?;
    let response = router.handle_bytes(&request).to_http_bytes();
    write_response(&stream, &response).await
}

async fn read_request(stream: &TcpStream) -> Result<Vec<u8>, StartupError> {
    let mut buffer = vec![0; MAX_REQUEST_BYTES];
    let mut used = 0;
    let mut expected_total = None;

    loop {
        stream
            .readable()
            .await
            .map_err(|source| StartupError::ConnectionRead { source })?;

        let Some(read_buffer) = buffer.get_mut(used..) else {
            return Err(StartupError::ConnectionRead {
                source: io::Error::other(
                    "request buffer cursor exceeded the maximum request size",
                ),
            });
        };

        match stream.try_read(read_buffer) {
            Ok(0) => break,
            Ok(read) => {
                used += read;

                let Some(received) = buffer.get(..used) else {
                    return Err(StartupError::ConnectionRead {
                        source: io::Error::other(
                            "request length exceeded the available buffer",
                        ),
                    });
                };

                if expected_total.is_none()
                    && let Some(header_end) = header_end(received)
                    && let Some(headers) = received.get(..header_end)
                {
                    expected_total = Some(
                        header_end + content_length(headers).unwrap_or(0),
                    );
                }

                if expected_total.is_some_and(|expected| used >= expected)
                    || used == buffer.len()
                {
                    break;
                }
            }
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => {
                return Err(StartupError::ConnectionRead { source });
            }
        }
    }

    buffer.truncate(used);
    Ok(buffer)
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let headers = std::str::from_utf8(headers).ok()?;

    headers.split("\r\n").skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;

        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse().ok()
        } else {
            None
        }
    })
}

async fn write_response(
    stream: &TcpStream,
    response: &[u8],
) -> Result<(), StartupError> {
    let mut written = 0;

    while written < response.len() {
        stream
            .writable()
            .await
            .map_err(|source| StartupError::ConnectionWrite { source })?;

        let Some(remaining) = response.get(written..) else {
            return Err(StartupError::ConnectionWrite {
                source: io::Error::other(
                    "response write cursor exceeded the response length",
                ),
            });
        };

        match stream.try_write(remaining) {
            Ok(0) => {
                return Err(StartupError::ConnectionWrite {
                    source: io::Error::new(
                        io::ErrorKind::WriteZero,
                        "connection closed before the response completed",
                    ),
                });
            }
            Ok(bytes) => written += bytes,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {}
            Err(source) => {
                return Err(StartupError::ConnectionWrite { source });
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum StartupError {
    Accept {
        source: io::Error,
    },
    Bind {
        address: SocketAddr,
        source: io::Error,
    },
    BuildConfiguration(String),
    Config(RuntimeDefaultsError),
    HostingConfig(HostingPlanError),
    ConnectionRead {
        source: io::Error,
    },
    ConnectionWrite {
        source: io::Error,
    },
    #[cfg(feature = "dynamodb")]
    DynamoDbState(DynamoDbInitError),
    #[cfg(feature = "eventbridge")]
    EventBridgeState(EventBridgeError),
    #[cfg(feature = "lambda")]
    LambdaRuntime(InfrastructureError),
    #[cfg(feature = "lambda")]
    LambdaState(LambdaInitError),
    #[cfg(feature = "s3")]
    S3State(S3InitError),
    StateDirectory {
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for StartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accept { source } => {
                write!(formatter, "failed to accept edge connection: {source}")
            }
            Self::Bind { address, source } => write!(
                formatter,
                "failed to bind edge listener on {address}: {source}"
            ),
            Self::BuildConfiguration(message) => {
                write!(
                    formatter,
                    "runtime build configuration error: {message}"
                )
            }
            Self::Config(source) => {
                write!(formatter, "startup configuration error: {source}")
            }
            Self::HostingConfig(source) => {
                write!(formatter, "startup configuration error: {source}")
            }
            Self::ConnectionRead { source } => {
                write!(formatter, "failed to read edge request: {source}")
            }
            Self::ConnectionWrite { source } => {
                write!(formatter, "failed to write edge response: {source}")
            }
            #[cfg(feature = "dynamodb")]
            Self::DynamoDbState(source) => {
                write!(formatter, "failed to load DynamoDB state: {source}")
            }
            #[cfg(feature = "eventbridge")]
            Self::EventBridgeState(source) => write!(
                formatter,
                "failed to initialize EventBridge runtime: {source}"
            ),
            #[cfg(feature = "lambda")]
            Self::LambdaRuntime(source) => write!(
                formatter,
                "failed to start Lambda background runtime: {source}"
            ),
            #[cfg(feature = "lambda")]
            Self::LambdaState(source) => {
                write!(formatter, "failed to load Lambda state: {source}")
            }
            #[cfg(feature = "s3")]
            Self::S3State(source) => {
                write!(formatter, "failed to load S3 state: {source}")
            }
            Self::StateDirectory { path, source } => write!(
                formatter,
                "failed to create state directory `{}`: {source}",
                path.display()
            ),
        }
    }
}

impl Error for StartupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Accept { source }
            | Self::Bind { source, .. }
            | Self::ConnectionRead { source }
            | Self::ConnectionWrite { source }
            | Self::StateDirectory { source, .. } => Some(source),
            #[cfg(feature = "dynamodb")]
            Self::DynamoDbState(source) => Some(source),
            #[cfg(feature = "eventbridge")]
            Self::EventBridgeState(source) => Some(source),
            #[cfg(feature = "lambda")]
            Self::LambdaRuntime(source) => Some(source),
            #[cfg(feature = "lambda")]
            Self::LambdaState(source) => Some(source),
            #[cfg(feature = "s3")]
            Self::S3State(source) => Some(source),
            Self::BuildConfiguration(_) => None,
            Self::Config(source) => Some(source),
            Self::HostingConfig(source) => Some(source),
        }
    }
}

impl From<RuntimeBuildError> for StartupError {
    fn from(error: RuntimeBuildError) -> Self {
        match error {
            RuntimeBuildError::Configuration(message) => {
                Self::BuildConfiguration(message)
            }
            #[cfg(feature = "dynamodb")]
            RuntimeBuildError::DynamoDbState(source) => {
                Self::DynamoDbState(source)
            }
            #[cfg(feature = "eventbridge")]
            RuntimeBuildError::EventBridgeState(source) => {
                Self::EventBridgeState(source)
            }
            #[cfg(feature = "lambda")]
            RuntimeBuildError::LambdaRuntime(source) => {
                Self::LambdaRuntime(source)
            }
            #[cfg(feature = "lambda")]
            RuntimeBuildError::LambdaState(source) => {
                Self::LambdaState(source)
            }
            #[cfg(feature = "s3")]
            RuntimeBuildError::S3State(source) => Self::S3State(source),
        }
    }
}

#[cfg(all(test, feature = "all-services"))]
mod tests {
    use super::{
        CloudishApp, EDGE_HOST_ENV, EDGE_PORT_ENV, load_hosting_plan,
        load_runtime_defaults,
    };
    use aws::{
        DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, RuntimeDefaults,
        STATE_DIRECTORY_ENV,
    };
    use httpdate::parse_http_date;
    use serde_json::Value;
    use std::error::Error;
    use std::io;
    use std::io::Write;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::OnceLock;
    use std::thread;
    use std::time::Duration;
    use test_support::{send_http_request, temporary_directory};
    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, oneshot};

    fn default_edge_port_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn runtime_defaults(label: &str) -> RuntimeDefaults {
        let state_directory = temporary_directory(label).join("state");

        RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_directory.to_string_lossy().into_owned()),
        )
        .expect("test defaults should be valid")
    }

    #[test]
    fn load_runtime_defaults_reports_missing_fields() {
        let error = load_runtime_defaults(|name| match name {
            DEFAULT_ACCOUNT_ENV => None,
            DEFAULT_REGION_ENV => Some(String::new()),
            _ => None,
        })
        .expect_err("missing config must fail");

        assert_eq!(
            error.to_string(),
            format!(
                "startup configuration error: missing required config values: {DEFAULT_ACCOUNT_ENV}, {DEFAULT_REGION_ENV}, {STATE_DIRECTORY_ENV}"
            )
        );
        assert_eq!(
            Error::source(&error)
                .map(ToString::to_string)
                .as_deref(),
            Some(
                format!(
                    "missing required config values: {DEFAULT_ACCOUNT_ENV}, {DEFAULT_REGION_ENV}, {STATE_DIRECTORY_ENV}"
                )
                .as_str()
            )
        );
    }

    #[test]
    fn load_hosting_plan_reports_invalid_edge_host() {
        let error = load_hosting_plan(|name| match name {
            EDGE_HOST_ENV => Some("localhost".to_owned()),
            _ => None,
        })
        .expect_err("invalid edge host must fail");

        assert_eq!(
            error.to_string(),
            "startup configuration error: invalid edge host in CLOUDISH_EDGE_HOST: invalid IP address syntax"
        );
        assert_eq!(
            Error::source(&error).map(ToString::to_string).as_deref(),
            Some(
                "invalid edge host in CLOUDISH_EDGE_HOST: invalid IP address syntax"
            )
        );
    }

    #[test]
    fn load_hosting_plan_accepts_edge_host_and_port_overrides() {
        let plan = load_hosting_plan(|name| match name {
            EDGE_HOST_ENV => Some("0.0.0.0".to_owned()),
            EDGE_PORT_ENV => Some("4570".to_owned()),
            _ => None,
        })
        .expect("valid hosting overrides should build");

        assert_eq!(plan.edge_address().to_string(), "0.0.0.0:4570");
    }

    #[test]
    fn bootstrap_server_exposes_the_default_edge_address() {
        let state_directory =
            temporary_directory("server-address").join("state");
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_directory.to_string_lossy().into_owned()),
        )
        .expect("test defaults should be valid");
        let server = CloudishApp::from_runtime_defaults(defaults)
            .expect("server bootstrap should succeed");

        assert_eq!(server.edge_address().to_string(), "127.0.0.1:4566");
        assert!(state_directory.exists());

        let root = state_directory
            .parent()
            .expect("state directory should have a test root");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn startup_error_messages_are_explicit() {
        let bind_error = super::StartupError::Bind {
            address: "127.0.0.1:4566"
                .parse()
                .expect("socket address should parse"),
            source: io::Error::other("bind failure"),
        };
        let accept_error = super::StartupError::Accept {
            source: io::Error::other("accept failure"),
        };
        let read_error = super::StartupError::ConnectionRead {
            source: io::Error::other("read failure"),
        };
        let write_error = super::StartupError::ConnectionWrite {
            source: io::Error::other("write failure"),
        };
        let hosting_config_error = super::StartupError::HostingConfig(
            super::HostingPlanError::BlankPort,
        );
        let lambda_state_error =
            super::StartupError::LambdaState(lambda::LambdaInitError::Store(
                storage::StorageError::CreateDirectory {
                    path: "/tmp/cloudish-lambda".into(),
                    source: io::Error::other("lambda state failure"),
                },
            ));
        let state_directory_error = super::StartupError::StateDirectory {
            path: "/tmp/cloudish-state".into(),
            source: io::Error::other("mkdir failure"),
        };

        assert_eq!(
            bind_error.to_string(),
            "failed to bind edge listener on 127.0.0.1:4566: bind failure"
        );
        assert_eq!(
            accept_error.to_string(),
            "failed to accept edge connection: accept failure"
        );
        assert_eq!(
            read_error.to_string(),
            "failed to read edge request: read failure"
        );
        assert_eq!(
            write_error.to_string(),
            "failed to write edge response: write failure"
        );
        assert_eq!(
            hosting_config_error.to_string(),
            "startup configuration error: invalid edge port in CLOUDISH_EDGE_PORT: value must not be blank"
        );
        assert_eq!(
            lambda_state_error.to_string(),
            "failed to load Lambda state: lambda metadata store failed: failed to create storage directory `/tmp/cloudish-lambda`: lambda state failure"
        );
        assert_eq!(
            state_directory_error.to_string(),
            "failed to create state directory `/tmp/cloudish-state`: mkdir failure"
        );
        assert_eq!(
            Error::source(&state_directory_error)
                .map(ToString::to_string)
                .as_deref(),
            Some("mkdir failure")
        );
    }

    #[test]
    fn bootstrap_server_reports_state_directory_errors() {
        let root = temporary_directory("server-state-error");
        let blocking_path = root.join("blocking");
        std::fs::write(&blocking_path, "blocking")
            .expect("blocking file should be creatable");
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(blocking_path.join("state").to_string_lossy().into_owned()),
        )
        .expect("test defaults should be valid");

        let error = CloudishApp::from_runtime_defaults(defaults)
            .expect_err("state directory creation should fail");

        assert!(matches!(error, super::StartupError::StateDirectory { .. }));
        assert!(
            error
                .to_string()
                .starts_with("failed to create state directory `")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn serve_with_shutdown_binds_and_stops_cleanly() {
        let _guard = default_edge_port_lock().lock().await;
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-bind-success",
        ))
        .expect("server bootstrap should succeed");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            server
                .serve_with_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(()).expect("server should still be running");
        let server_result = handle.await.expect("server task should complete");
        server_result.expect("server should stop cleanly");
    }

    #[tokio::test]
    async fn serve_with_shutdown_reports_bind_errors() {
        let _guard = default_edge_port_lock().lock().await;
        let occupied =
            StdTcpListener::bind("127.0.0.1:4566").expect("port should bind");
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-bind-error",
        ))
        .expect("server bootstrap should succeed");

        let error = server
            .serve_with_shutdown(async {})
            .await
            .expect_err("bind failure should surface");

        assert!(matches!(error, super::StartupError::Bind { .. }));
        drop(occupied);
    }

    #[tokio::test]
    async fn read_request_returns_empty_when_client_closes() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");
        let client = std::net::TcpStream::connect(address)
            .expect("client should connect");
        let (server_stream, _) = listener
            .accept()
            .await
            .expect("server should accept a connection");
        drop(client);

        let request = super::read_request(&server_stream)
            .await
            .expect("closed connections should read as empty");

        assert!(request.is_empty());
    }

    #[tokio::test]
    async fn write_response_reports_closed_connections() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");
        let client = std::net::TcpStream::connect(address)
            .expect("client should connect");
        let (server_stream, _) = listener
            .accept()
            .await
            .expect("server should accept a connection");
        drop(client);

        let response = vec![b'x'; 1024 * 1024];
        let error = super::write_response(&server_stream, &response)
            .await
            .expect_err("closed connections should fail writes");

        assert!(matches!(error, super::StartupError::ConnectionWrite { .. }));
    }

    #[tokio::test]
    async fn read_request_reads_body_without_waiting_for_socket_close() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address =
            listener.local_addr().expect("listener should expose its address");
        let client = thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(address)
                .expect("client should connect");
            stream
                .write_all(
                    b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}",
                )
                .expect("request should be written");
            thread::sleep(Duration::from_millis(250));
        });
        let (server_stream, _) = listener
            .accept()
            .await
            .expect("server should accept a connection");

        let request = tokio::time::timeout(
            Duration::from_millis(100),
            super::read_request(&server_stream),
        )
        .await
        .expect("body read should finish before the client closes")
        .expect("request should be readable");

        client.join().expect("client thread should finish");

        assert!(request.ends_with(b"{}"));
    }

    #[tokio::test]
    async fn serve_internal_runtime_endpoints_on_a_listener() {
        let state_directory =
            temporary_directory("server-health").join("nested").join("state");
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_directory.to_string_lossy().into_owned()),
        )
        .expect("test defaults should be valid");
        let server = CloudishApp::from_runtime_defaults(defaults)
            .expect("server bootstrap should succeed");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should expose its address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            server
                .serve_listener_with_shutdown(listener, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let health_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("request task should complete")
        .expect("request should succeed");

        let status_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "GET /__cloudish/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("request task should complete")
        .expect("request should succeed");

        shutdown_tx.send(()).expect("test server should still be waiting");
        let server_result = handle.await.expect("server task should complete");
        server_result.expect("server should shut down cleanly");

        assert!(state_directory.exists());
        assert!(health_response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(health_response.contains("\"status\":\"ok\""));
        let health_date = health_response
            .lines()
            .find_map(|line| line.strip_prefix("Date: "))
            .expect("health response should include a Date header");
        parse_http_date(health_date)
            .expect("health Date header should use HTTP-date format");
        assert!(status_response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(status_response.contains("\"ready\":true"));
        assert!(status_response.contains("\"stateDirectory\":\""));
        let status_body = status_response
            .split("\r\n\r\n")
            .nth(1)
            .expect("status response should include a body");
        let status_json: Value = serde_json::from_str(status_body)
            .expect("status response body should be valid JSON");
        let expected_service_count = crate::supported_service_count();
        assert_eq!(
            status_json
                .get("serviceCount")
                .expect("serviceCount should exist")
                .as_u64(),
            Some(expected_service_count as u64)
        );
        assert_eq!(
            status_json
                .get("enabledServices")
                .expect("enabledServices should exist")
                .as_array()
                .expect("enabledServices should be an array")
                .len(),
            expected_service_count
        );

        let root = state_directory
            .parent()
            .and_then(|path| path.parent())
            .expect("nested state directory should have a test root");
        let _ = std::fs::remove_dir_all(root);
    }
}
