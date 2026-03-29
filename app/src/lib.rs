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
mod transport;

use auth::Authenticator;
use aws::{
    DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, RuntimeDefaults,
    RuntimeDefaultsError, STATE_DIRECTORY_ENV, SharedAdvertisedEdge,
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
use tokio::net::TcpListener;

pub use hosting::{
    EDGE_HOST_ENV, EDGE_MAX_REQUEST_BYTES_ENV, EDGE_PORT_ENV,
    EDGE_PUBLIC_HOST_ENV, EDGE_PUBLIC_PORT_ENV, EDGE_PUBLIC_SCHEME_ENV,
    EDGE_READY_FILE_ENV, HostingPlanError,
};

#[derive(Clone)]
pub struct CloudishApp {
    address: SocketAddr,
    advertised_edge: SharedAdvertisedEdge,
    advertised_edge_template: aws::AdvertisedEdgeTemplate,
    max_request_bytes: usize,
    #[cfg(feature = "lambda")]
    _lambda_background_tasks: Arc<ManagedBackgroundTasks>,
    ready_file: Option<PathBuf>,
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
        let advertised_edge = SharedAdvertisedEdge::new(
            hosting.advertised_edge().resolve(hosting.edge_address().port()),
        );
        let (services, runtime, background_tasks) =
            LocalRuntimeBuilder::new(defaults.clone(), authenticator.clone())
                .with_advertised_edge(advertised_edge.clone())
                .build()
                .map_err(StartupError::from)
                .map(|assembly| assembly.into_parts())?;
        #[cfg(not(feature = "lambda"))]
        let _ = background_tasks;
        let router = EdgeRouter::new(
            defaults,
            advertised_edge.clone(),
            authenticator,
            services,
            runtime,
        );

        Ok(Self {
            address: hosting.edge_address(),
            advertised_edge,
            advertised_edge_template: hosting.advertised_edge().clone(),
            max_request_bytes: hosting.max_request_bytes(),
            #[cfg(feature = "lambda")]
            _lambda_background_tasks: Arc::new(background_tasks),
            ready_file: hosting.ready_file().cloned(),
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
    /// Returns [`StartupError`] when the edge listener cannot bind or a
    /// connection cannot be accepted.
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
    /// Returns [`StartupError`] when the listener cannot accept a connection.
    pub async fn serve_listener_with_shutdown<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
    {
        let local_address = listener
            .local_addr()
            .map_err(|source| StartupError::ListenerAddress { source })?;
        self.advertised_edge.update(
            self.advertised_edge_template.resolve(local_address.port()),
        );
        if let Some(path) = self.ready_file.as_ref() {
            write_ready_file(path, local_address)?;
        }
        transport::serve_listener_with_shutdown(
            listener,
            self.router,
            self.max_request_bytes,
            shutdown,
        )
        .await
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
/// fails, or the edge listener cannot bind.
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

fn write_ready_file(
    path: &PathBuf,
    address: SocketAddr,
) -> Result<(), StartupError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            StartupError::StateDirectory { path: parent.to_path_buf(), source }
        })?;
    }
    std::fs::write(path, address.to_string()).map_err(|source| {
        StartupError::StateDirectory { path: path.clone(), source }
    })
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
    ListenerAddress {
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
            Self::ListenerAddress { source } => {
                write!(formatter, "failed to inspect edge listener: {source}")
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
            | Self::ListenerAddress { source }
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
        CloudishApp, EDGE_HOST_ENV, EDGE_MAX_REQUEST_BYTES_ENV, EDGE_PORT_ENV,
        load_hosting_plan, load_runtime_defaults,
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
    use std::net::Shutdown;
    use std::net::TcpListener as StdTcpListener;
    use std::thread;
    use std::time::Duration;
    use test_support::{
        send_http_request, send_http_request_bytes, temporary_directory,
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

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
    fn load_hosting_plan_accepts_edge_host_port_and_request_limit() {
        let plan = load_hosting_plan(|name| match name {
            EDGE_HOST_ENV => Some("0.0.0.0".to_owned()),
            EDGE_PORT_ENV => Some("4570".to_owned()),
            EDGE_MAX_REQUEST_BYTES_ENV => Some("1048576".to_owned()),
            _ => None,
        })
        .expect("valid hosting overrides should build");

        assert_eq!(plan.edge_address().to_string(), "0.0.0.0:4570");
        assert_eq!(plan.max_request_bytes(), 1_048_576);
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
        let port = available_port();
        let hosting = load_hosting_plan(|name| match name {
            EDGE_PORT_ENV => Some(port.to_string()),
            _ => None,
        })
        .expect("ephemeral hosting plan should build");
        let server = CloudishApp::from_runtime_defaults_with_hosting(
            runtime_defaults("server-bind-success"),
            hosting,
        )
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
        let port = available_port();
        let occupied = StdTcpListener::bind(("127.0.0.1", port))
            .expect("port should bind");
        let hosting = load_hosting_plan(|name| match name {
            EDGE_PORT_ENV => Some(port.to_string()),
            _ => None,
        })
        .expect("ephemeral hosting plan should build");
        let server = CloudishApp::from_runtime_defaults_with_hosting(
            runtime_defaults("server-bind-error"),
            hosting,
        )
        .expect("server bootstrap should succeed");

        let error = server
            .serve_with_shutdown(async {})
            .await
            .expect_err("bind failure should surface");

        assert!(matches!(error, super::StartupError::Bind { .. }));
        drop(occupied);
    }

    #[tokio::test]
    async fn serve_listener_keeps_accepting_while_a_client_stalls() {
        let state_directory =
            temporary_directory("server-stalled-client").join("state");
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

        let mut stalled_client = std::net::TcpStream::connect(address)
            .expect("stalled client should connect");
        stalled_client
            .write_all(
                b"PUT /stalled-bucket/object.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nabc",
            )
            .expect("stalled request should be written");

        let health_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("health request task should complete")
        .expect("health request should succeed");

        stalled_client
            .shutdown(Shutdown::Both)
            .expect("stalled client should close");
        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");

        assert!(health_response.starts_with("HTTP/1.1 200 OK\r\n"));
    }

    #[tokio::test]
    async fn serve_listener_survives_a_client_disconnect_mid_body() {
        let state_directory =
            temporary_directory("server-disconnect-mid-body").join("state");
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

        let mut broken_client = std::net::TcpStream::connect(address)
            .expect("broken client should connect");
        broken_client
            .write_all(
                b"PUT /broken-bucket/object.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 6\r\n\r\nabc",
            )
            .expect("partial request should be written");
        broken_client
            .shutdown(Shutdown::Both)
            .expect("broken client should close");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let health_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("health request task should complete")
        .expect("health request should succeed");

        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");

        assert!(health_response.starts_with("HTTP/1.1 200 OK\r\n"));
    }

    #[tokio::test]
    async fn serve_listener_reads_content_length_bodies_without_waiting_for_close()
     {
        let state_directory =
            temporary_directory("server-content-length").join("state");
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

        let create_bucket = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "PUT /transport-content-length HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
            )
        })
        .await
        .expect("bucket creation task should complete")
        .expect("bucket creation should succeed");
        assert!(create_bucket.starts_with("HTTP/1.1 200 OK\r\n"));

        let writer = thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(address)
                .expect("writer should connect");
            stream
                .write_all(
                    b"PUT /transport-content-length/object.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 7\r\n\r\npayload",
                )
                .expect("request should be written");
            thread::sleep(Duration::from_millis(250));
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let get_response = tokio::task::spawn_blocking(move || {
            send_http_request_bytes(
                address,
                b"GET /transport-content-length/object.txt HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("get task should complete")
        .expect("get request should succeed");

        writer.join().expect("writer thread should finish");
        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");

        assert_eq!(response_body(&get_response), b"payload");
    }

    #[tokio::test]
    async fn serve_listener_reads_http_chunked_bodies() {
        let state_directory =
            temporary_directory("server-http-chunked").join("state");
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

        let create_bucket = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "PUT /transport-chunked HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
            )
        })
        .await
        .expect("bucket creation task should complete")
        .expect("bucket creation should succeed");
        assert!(create_bucket.starts_with("HTTP/1.1 200 OK\r\n"));

        let put_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "PUT /transport-chunked/object.txt HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ndata\r\n3\r\n123\r\n0\r\n\r\n",
            )
        })
        .await
        .expect("put task should complete")
        .expect("put request should succeed");
        let get_response = tokio::task::spawn_blocking(move || {
            send_http_request_bytes(
                address,
                b"GET /transport-chunked/object.txt HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("get task should complete")
        .expect("get request should succeed");

        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");

        assert!(put_response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert_eq!(response_body(&get_response), b"data123");
    }

    #[tokio::test]
    async fn serve_listener_rejects_oversized_request_bodies() {
        let state_directory =
            temporary_directory("server-request-limit").join("state");
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_directory.to_string_lossy().into_owned()),
        )
        .expect("test defaults should be valid");
        let hosting = load_hosting_plan(|name| match name {
            EDGE_MAX_REQUEST_BYTES_ENV => Some("8".to_owned()),
            _ => None,
        })
        .expect("request size override should build");
        let server =
            CloudishApp::from_runtime_defaults_with_hosting(defaults, hosting)
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

        let oversized_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "POST /__cloudish/health HTTP/1.1\r\nHost: localhost\r\nContent-Length: 9\r\n\r\n123456789",
            )
        })
        .await
        .expect("oversized request task should complete")
        .expect("oversized request should return a response");
        let health_response = tokio::task::spawn_blocking(move || {
            send_http_request(
                address,
                "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
        })
        .await
        .expect("health request task should complete")
        .expect("health request should succeed");

        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");

        assert!(
            oversized_response
                .starts_with("HTTP/1.1 413 Payload Too Large\r\n")
        );
        assert!(oversized_response.contains(
            "\"message\":\"request body exceeds configured limit of 8 bytes\""
        ));
        assert!(health_response.starts_with("HTTP/1.1 200 OK\r\n"));
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
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("date").then_some(value.trim())
            })
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

    fn response_body(response: &[u8]) -> &[u8] {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .expect("response should contain a header terminator");

        &response[header_end..]
    }

    fn available_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0")
            .expect("ephemeral port should bind")
            .local_addr()
            .expect("ephemeral listener should expose its address")
            .port()
    }
}
