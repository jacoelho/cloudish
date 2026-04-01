#![cfg_attr(
    test,
    allow(
        clippy::unreachable,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

mod edge_wire_log;
mod hosting;
mod transport;

use auth::Authenticator;
use aws::{
    BootstrapSignatureVerificationMode, DEFAULT_ACCOUNT_ENV,
    DEFAULT_REGION_ENV, RuntimeDefaults, RuntimeDefaultsError,
    STATE_DIRECTORY_ENV, SharedAdvertisedEdge, UNSAFE_BOOTSTRAP_AUTH_ENV,
};
use edge_runtime::DynamoDbInitError;
use edge_runtime::ElastiCacheError;
use edge_runtime::EventBridgeError;
use edge_runtime::InfrastructureError;
use edge_runtime::LambdaInitError;
use edge_runtime::RdsError;
use edge_runtime::S3InitError;
use edge_runtime::{LocalRuntimeBuilder, RuntimeBuildError};
use hosting::HostingPlan;
use http::EdgeRouter;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, LockResult, Mutex, MutexGuard};
use storage::{StorageError, StorageRuntime};
use tokio::net::TcpListener;

pub use edge_wire_log::WireLogInitError;
pub use hosting::{
    EDGE_HOST_ENV, EDGE_MAX_REQUEST_BYTES_ENV, EDGE_PORT_ENV,
    EDGE_PUBLIC_HOST_ENV, EDGE_PUBLIC_PORT_ENV, EDGE_PUBLIC_SCHEME_ENV,
    EDGE_READY_FILE_ENV, EDGE_WIRE_LOG_ENV, HostingPlanError,
};

#[derive(Clone)]
pub struct CloudishApp {
    address: SocketAddr,
    advertised_edge: SharedAdvertisedEdge,
    advertised_edge_template: aws::AdvertisedEdgeTemplate,
    max_request_bytes: usize,
    ready_file: Option<PathBuf>,
    router: EdgeRouter,
    serve_state: Arc<Mutex<ServeState>>,
    wire_log_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServeState {
    Ready,
    Serving,
    Closed,
}

struct ServeGuard {
    state: Arc<Mutex<ServeState>>,
    released: bool,
}

impl ServeGuard {
    fn begin(state: &Arc<Mutex<ServeState>>) -> Result<Self, StartupError> {
        let mut current = recover(state.lock());
        match *current {
            ServeState::Ready => {
                *current = ServeState::Serving;
                Ok(Self { state: Arc::clone(state), released: false })
            }
            ServeState::Serving => Err(StartupError::AlreadyServing),
            ServeState::Closed => Err(StartupError::AlreadyServed),
        }
    }

    fn release_to_ready(&mut self) {
        if self.released {
            return;
        }
        *recover(self.state.lock()) = ServeState::Ready;
        self.released = true;
    }

    fn release_to_closed(&mut self) {
        if self.released {
            return;
        }
        *recover(self.state.lock()) = ServeState::Closed;
        self.released = true;
    }
}

impl Drop for ServeGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        *recover(self.state.lock()) = ServeState::Closed;
    }
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
        let runtime =
            LocalRuntimeBuilder::new(defaults.clone(), authenticator.clone())
                .with_advertised_edge(advertised_edge.clone())
                .build()
                .map_err(StartupError::from)
                .map(|assembly| assembly.into_parts())?;
        let router = EdgeRouter::new(
            defaults,
            advertised_edge.clone(),
            authenticator,
            runtime,
        );

        Ok(Self {
            address: hosting.edge_address(),
            advertised_edge,
            advertised_edge_template: hosting.advertised_edge().clone(),
            max_request_bytes: hosting.max_request_bytes(),
            ready_file: hosting.ready_file().cloned(),
            router,
            serve_state: Arc::new(Mutex::new(ServeState::Ready)),
            wire_log_enabled: hosting.wire_log_enabled(),
        })
    }

    pub fn edge_address(&self) -> SocketAddr {
        self.address
    }

    /// Bind the configured edge address and serve until shutdown resolves.
    /// The caller retains ownership of the app while serving.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError`] when the edge listener cannot bind or a
    /// connection cannot be accepted.
    pub async fn serve_with_shutdown<F>(
        &self,
        shutdown: F,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
    {
        self.serve_with_shutdown_with_wire_log_init(
            shutdown,
            edge_wire_log::try_init_tracing,
        )
        .await
    }

    async fn serve_with_shutdown_with_wire_log_init<F, G>(
        &self,
        shutdown: F,
        init_tracing: G,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
        G: FnOnce(bool) -> Result<(), WireLogInitError>,
    {
        let mut serve_guard = self.begin_serve()?;
        if let Err(error) = self.prepare_for_serve_with(init_tracing) {
            serve_guard.release_to_ready();
            return Err(error);
        }
        let listener =
            TcpListener::bind(self.edge_address()).await.map_err(|source| {
                StartupError::Bind { address: self.edge_address(), source }
            });
        let listener = match listener {
            Ok(listener) => listener,
            Err(error) => {
                serve_guard.release_to_ready();
                return Err(error);
            }
        };

        let result =
            self.serve_listener_after_prepare(listener, shutdown).await;
        serve_guard.release_to_closed();
        result
    }

    /// Serve on an existing listener until shutdown resolves.
    /// The caller retains ownership of the app while serving.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError`] when the listener cannot accept a connection.
    pub async fn serve_listener_with_shutdown<F>(
        &self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
    {
        self.serve_listener_with_shutdown_with_wire_log_init(
            listener,
            shutdown,
            edge_wire_log::try_init_tracing,
        )
        .await
    }

    async fn serve_listener_with_shutdown_with_wire_log_init<F, G>(
        &self,
        listener: TcpListener,
        shutdown: F,
        init_tracing: G,
    ) -> Result<(), StartupError>
    where
        F: Future<Output = ()>,
        G: FnOnce(bool) -> Result<(), WireLogInitError>,
    {
        let mut serve_guard = self.begin_serve()?;
        if let Err(error) = self.prepare_for_serve_with(init_tracing) {
            serve_guard.release_to_ready();
            return Err(error);
        }
        let result =
            self.serve_listener_after_prepare(listener, shutdown).await;
        serve_guard.release_to_closed();
        result
    }

    async fn serve_listener_after_prepare<F>(
        &self,
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
            self.router.clone(),
            self.max_request_bytes,
            self.wire_log_enabled,
            shutdown,
        )
        .await
    }

    fn prepare_for_serve_with<F>(
        &self,
        init_tracing: F,
    ) -> Result<(), StartupError>
    where
        F: FnOnce(bool) -> Result<(), WireLogInitError>,
    {
        let should_log_wire = self.wire_log_enabled;
        init_tracing(should_log_wire).map_err(StartupError::WireLogInit).or_else(
            |error| {
                if !should_log_wire
                    && matches!(
                        error,
                        StartupError::WireLogInit(
                            WireLogInitError::GlobalSubscriberAlreadySet
                        )
                    )
                {
                    tracing::warn!("wire log initialization skipped because a global tracing subscriber is already installed");
                    Ok(())
                } else {
                    Err(error)
                }
            },
        )
    }

    fn begin_serve(&self) -> Result<ServeGuard, StartupError> {
        ServeGuard::begin(&self.serve_state)
    }
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
    let defaults = RuntimeDefaults::try_new(
        read_env(DEFAULT_ACCOUNT_ENV),
        read_env(DEFAULT_REGION_ENV),
        read_env(STATE_DIRECTORY_ENV),
    )
    .map_err(StartupError::Config)?;
    let bootstrap_signature_verification = read_env(UNSAFE_BOOTSTRAP_AUTH_ENV)
        .map(BootstrapSignatureVerificationMode::parse_env_value)
        .transpose()
        .map_err(StartupError::Config)?
        .unwrap_or(BootstrapSignatureVerificationMode::Enforce);

    Ok(defaults.with_bootstrap_signature_verification(
        bootstrap_signature_verification,
    ))
}

pub(crate) fn load_hosting_plan<F>(
    read_env: F,
) -> Result<HostingPlan, StartupError>
where
    F: FnMut(&'static str) -> Option<String>,
{
    HostingPlan::from_env(read_env).map_err(StartupError::HostingConfig)
}

fn recover<T>(result: LockResult<MutexGuard<'_, T>>) -> MutexGuard<'_, T> {
    match result {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("recovered from poisoned mutex");
            poisoned.into_inner()
        }
    }
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
    Accept { source: io::Error },
    AlreadyServed,
    AlreadyServing,
    Bind { address: SocketAddr, source: io::Error },
    BuildConfiguration(String),
    Config(RuntimeDefaultsError),
    HostingConfig(HostingPlanError),
    ListenerAddress { source: io::Error },
    ShutdownTimeout { active_requests: usize },
    WireLogInit(WireLogInitError),
    DynamoDbState(DynamoDbInitError),
    ElastiCacheRestore(ElastiCacheError),
    EventBridgeRuntime(InfrastructureError),
    EventBridgeRestore(EventBridgeError),
    LambdaRuntime(InfrastructureError),
    LambdaState(LambdaInitError),
    RdsRestore(RdsError),
    S3State(S3InitError),
    StorageLoad(StorageError),
    StateDirectory { path: PathBuf, source: io::Error },
}

impl fmt::Display for StartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accept { source } => {
                write!(formatter, "failed to accept edge connection: {source}")
            }
            Self::AlreadyServed => {
                write!(
                    formatter,
                    "CloudishApp cannot be served again after it has stopped"
                )
            }
            Self::AlreadyServing => {
                write!(
                    formatter,
                    "CloudishApp is already serving on another listener"
                )
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
            Self::ShutdownTimeout { active_requests } => write!(
                formatter,
                "edge shutdown timed out with {active_requests} blocking requests still active"
            ),
            Self::WireLogInit(source) => {
                write!(
                    formatter,
                    "failed to initialize edge wire logging: {source}"
                )
            }
            Self::DynamoDbState(source) => {
                write!(formatter, "failed to load DynamoDB state: {source}")
            }
            Self::ElastiCacheRestore(source) => write!(
                formatter,
                "failed to restore ElastiCache runtimes: {source}"
            ),
            Self::EventBridgeRuntime(source) => write!(
                formatter,
                "failed to start EventBridge delivery runtime: {source}"
            ),
            Self::EventBridgeRestore(source) => write!(
                formatter,
                "failed to restore EventBridge schedules: {source}"
            ),
            Self::LambdaRuntime(source) => write!(
                formatter,
                "failed to start Lambda background runtime: {source}"
            ),
            Self::LambdaState(source) => {
                write!(formatter, "failed to load Lambda state: {source}")
            }
            Self::RdsRestore(source) => {
                write!(formatter, "failed to restore RDS runtimes: {source}")
            }
            Self::S3State(source) => {
                write!(formatter, "failed to load S3 state: {source}")
            }
            Self::StorageLoad(source) => write!(
                formatter,
                "failed to load persistent runtime state: {source}"
            ),
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
            Self::WireLogInit(source) => Some(source),
            Self::DynamoDbState(source) => Some(source),
            Self::EventBridgeRuntime(source) => Some(source),
            Self::EventBridgeRestore(source) => Some(source),
            Self::ElastiCacheRestore(source) => Some(source),
            Self::LambdaRuntime(source) => Some(source),
            Self::LambdaState(source) => Some(source),
            Self::RdsRestore(source) => Some(source),
            Self::S3State(source) => Some(source),
            Self::StorageLoad(source) => Some(source),
            Self::AlreadyServed | Self::AlreadyServing => None,
            Self::BuildConfiguration(_) => None,
            Self::Config(source) => Some(source),
            Self::HostingConfig(source) => Some(source),
            Self::ShutdownTimeout { .. } => None,
        }
    }
}

impl From<RuntimeBuildError> for StartupError {
    fn from(error: RuntimeBuildError) -> Self {
        match error {
            RuntimeBuildError::Configuration(message) => {
                Self::BuildConfiguration(message)
            }
            RuntimeBuildError::DynamoDbState(source) => {
                Self::DynamoDbState(source)
            }
            RuntimeBuildError::ElastiCacheRestore(source) => {
                Self::ElastiCacheRestore(source)
            }
            RuntimeBuildError::EventBridgeRuntime(source) => {
                Self::EventBridgeRuntime(source)
            }
            RuntimeBuildError::EventBridgeRestore(source) => {
                Self::EventBridgeRestore(source)
            }
            RuntimeBuildError::LambdaRuntime(source) => {
                Self::LambdaRuntime(source)
            }
            RuntimeBuildError::LambdaState(source) => {
                Self::LambdaState(source)
            }
            RuntimeBuildError::RdsRestore(source) => Self::RdsRestore(source),
            RuntimeBuildError::S3State(source) => Self::S3State(source),
            RuntimeBuildError::StorageLoad(source) => {
                Self::StorageLoad(source)
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::{
        CloudishApp, EDGE_HOST_ENV, EDGE_MAX_REQUEST_BYTES_ENV, EDGE_PORT_ENV,
        EDGE_READY_FILE_ENV, EDGE_WIRE_LOG_ENV, WireLogInitError,
        load_hosting_plan, load_runtime_defaults,
    };
    use aws::{
        BootstrapSignatureVerificationMode, DEFAULT_ACCOUNT_ENV,
        DEFAULT_REGION_ENV, RuntimeDefaults, STATE_DIRECTORY_ENV,
        UNSAFE_BOOTSTRAP_AUTH_ENV,
    };
    #[cfg(unix)]
    use base64::Engine as _;
    #[cfg(unix)]
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use httpdate::parse_http_date;
    use serde_json::Value;
    use std::error::Error;
    use std::io;
    use std::io::Write;
    use std::net::Shutdown;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;
    use test_support::{
        send_http_request, send_http_request_bytes, temporary_directory,
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    #[cfg(unix)]
    use zip::write::FileOptions;

    fn runtime_defaults(label: &str) -> RuntimeDefaults {
        let state_directory = temporary_directory(label).join("state");

        RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_directory.to_string_lossy().into_owned()),
        )
        .expect("test defaults should be valid")
    }

    fn create_bucket_request(path: &str) -> String {
        let body = concat!(
            "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
            "<LocationConstraint>eu-west-2</LocationConstraint>",
            "</CreateBucketConfiguration>",
        );

        format!(
            "PUT {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    fn query_form_request(path: &str, body: &str) -> String {
        format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }
    fn create_iam_role_request(role_name: &str) -> String {
        let trust_policy = urlencoding::encode(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#,
        );

        query_form_request(
            "/",
            &format!(
                "Action=CreateRole&Version=2010-05-08&RoleName={role_name}&AssumeRolePolicyDocument={trust_policy}",
            ),
        )
    }

    #[cfg(unix)]
    fn lambda_archive_base64(entries: &[(&str, &str)]) -> String {
        let mut cursor = io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut cursor);
            for (path, body) in entries {
                let options = if *path == "bootstrap" {
                    FileOptions::default().unix_permissions(0o755)
                } else {
                    FileOptions::default().unix_permissions(0o644)
                };
                writer
                    .start_file(*path, options)
                    .expect("zip entry should start");
                writer
                    .write_all(body.as_bytes())
                    .expect("zip entry should write");
            }
            writer.finish().expect("zip should finish");
        }

        BASE64_STANDARD.encode(cursor.into_inner())
    }

    #[cfg(unix)]
    fn create_lambda_request(
        function_name: &str,
        archive_base64: &str,
    ) -> String {
        let body = serde_json::json!({
            "Code": {
                "ZipFile": archive_base64,
            },
            "FunctionName": function_name,
            "Handler": "bootstrap.handler",
            "Role": "arn:aws:iam::000000000000:role/service-role/lambda-role",
            "Runtime": "provided.al2",
        })
        .to_string();

        format!(
            "POST /2015-03-31/functions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    #[cfg(unix)]
    fn invoke_lambda_request(function_name: &str, body: &str) -> String {
        format!(
            "POST /2015-03-31/functions/{function_name}/invocations HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nX-Amz-Invocation-Type: RequestResponse\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
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
    fn load_runtime_defaults_accepts_unsafe_bootstrap_auth_flag() {
        let defaults = load_runtime_defaults(|name| match name {
            DEFAULT_ACCOUNT_ENV => Some("000000000000".to_owned()),
            DEFAULT_REGION_ENV => Some("eu-west-2".to_owned()),
            STATE_DIRECTORY_ENV => Some("/tmp/cloudish".to_owned()),
            UNSAFE_BOOTSTRAP_AUTH_ENV => Some("on".to_owned()),
            _ => None,
        })
        .expect("valid bootstrap auth override should build");

        assert_eq!(
            defaults.bootstrap_signature_verification(),
            BootstrapSignatureVerificationMode::Skip
        );
    }

    #[test]
    fn load_runtime_defaults_rejects_invalid_unsafe_bootstrap_auth_flag() {
        let error = load_runtime_defaults(|name| match name {
            DEFAULT_ACCOUNT_ENV => Some("000000000000".to_owned()),
            DEFAULT_REGION_ENV => Some("eu-west-2".to_owned()),
            STATE_DIRECTORY_ENV => Some("/tmp/cloudish".to_owned()),
            UNSAFE_BOOTSTRAP_AUTH_ENV => Some("verbose".to_owned()),
            _ => None,
        })
        .expect_err("invalid bootstrap auth override must fail");

        assert_eq!(
            error.to_string(),
            "startup configuration error: invalid bootstrap auth mode in CLOUDISH_UNSAFE_BOOTSTRAP_AUTH: unsupported value `verbose`"
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
            EDGE_WIRE_LOG_ENV => Some("on".to_owned()),
            _ => None,
        })
        .expect("valid hosting overrides should build");

        assert_eq!(plan.edge_address().to_string(), "0.0.0.0:4570");
        assert_eq!(plan.max_request_bytes(), 1_048_576);
        assert!(plan.wire_log_enabled());
    }

    #[test]
    fn prepare_for_serve_always_initializes_tracing() {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "wire-log-disabled",
        ))
        .expect("server bootstrap should succeed");
        let init_calls = AtomicUsize::new(0);
        let received_wire_log_enabled = AtomicBool::new(true);

        server
            .prepare_for_serve_with(|wire_log_enabled| {
                init_calls.fetch_add(1, Ordering::SeqCst);
                received_wire_log_enabled
                    .store(wire_log_enabled, Ordering::SeqCst);
                Ok(())
            })
            .expect("tracing initialization should succeed");

        assert_eq!(
            init_calls.load(Ordering::SeqCst),
            1,
            "tracing init should be called even when wire log is disabled",
        );
        assert!(
            !received_wire_log_enabled.load(Ordering::SeqCst),
            "wire_log_enabled should be false when wire log is disabled",
        );
    }

    #[test]
    fn prepare_for_serve_ignores_existing_tracing_subscriber_when_wire_log_disabled()
     {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "wire-log-disabled-existing-subscriber",
        ))
        .expect("server bootstrap should succeed");
        let init_calls = AtomicUsize::new(0);
        let received_wire_log_enabled = AtomicBool::new(true);

        server
            .prepare_for_serve_with(|wire_log_enabled| {
                init_calls.fetch_add(1, Ordering::SeqCst);
                received_wire_log_enabled
                    .store(wire_log_enabled, Ordering::SeqCst);
                Err(WireLogInitError::GlobalSubscriberAlreadySet)
            })
            .expect(
                "existing tracing subscriber should be tolerated when wire log is disabled",
            );

        assert_eq!(
            init_calls.load(Ordering::SeqCst),
            1,
            "tracing init should be attempted even when wire log is disabled",
        );
        assert!(
            !received_wire_log_enabled.load(Ordering::SeqCst),
            "wire_log_enabled should be false when wire log is disabled",
        );
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
        let shutdown_timeout_error =
            super::StartupError::ShutdownTimeout { active_requests: 2 };
        let already_serving_error = super::StartupError::AlreadyServing;
        let already_served_error = super::StartupError::AlreadyServed;
        let wire_log_error = super::StartupError::WireLogInit(
            WireLogInitError::GlobalSubscriberAlreadySet,
        );

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
            shutdown_timeout_error.to_string(),
            "edge shutdown timed out with 2 blocking requests still active"
        );
        assert_eq!(
            already_serving_error.to_string(),
            "CloudishApp is already serving on another listener"
        );
        assert_eq!(
            already_served_error.to_string(),
            "CloudishApp cannot be served again after it has stopped"
        );
        assert_eq!(
            wire_log_error.to_string(),
            "failed to initialize edge wire logging: tracing subscriber requested but Cloudish could not install its JSON tracing subscriber because a global tracing subscriber is already installed"
        );
        assert_eq!(
            Error::source(&state_directory_error)
                .map(ToString::to_string)
                .as_deref(),
            Some("mkdir failure")
        );
        assert_eq!(
            Error::source(&wire_log_error).map(ToString::to_string).as_deref(),
            Some(
                "tracing subscriber requested but Cloudish could not install its JSON tracing subscriber because a global tracing subscriber is already installed"
            )
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
    async fn serve_listener_with_shutdown_rejects_second_serve_after_clean_stop()
     {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-borrowed-serve",
        ))
        .expect("server bootstrap should succeed");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");

        server
            .serve_listener_with_shutdown(listener, async {})
            .await
            .expect("server should stop cleanly");

        let second_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("second listener should bind");
        let error = server
            .serve_listener_with_shutdown(second_listener, async {})
            .await
            .expect_err("server should reject repeated serve");

        assert!(matches!(error, super::StartupError::AlreadyServed));
    }

    #[tokio::test]
    async fn serve_listener_with_shutdown_rejects_concurrent_serve_attempts() {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-concurrent-serve",
        ))
        .expect("server bootstrap should succeed");
        let first_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("first listener should bind");
        let second_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("second listener should bind");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let serving_server = server.clone();

        let first_handle = tokio::spawn(async move {
            serving_server
                .serve_listener_with_shutdown(first_listener, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let error = server
            .serve_listener_with_shutdown(second_listener, async {})
            .await
            .expect_err("server should reject concurrent serve");

        assert!(matches!(error, super::StartupError::AlreadyServing));
        shutdown_tx.send(()).expect("first server should still be running");
        first_handle
            .await
            .expect("first server task should complete")
            .expect("first server should stop cleanly");
    }

    #[tokio::test]
    async fn serve_with_shutdown_allows_retry_after_bind_failure() {
        let port = available_port();
        let occupied = StdTcpListener::bind(("127.0.0.1", port))
            .expect("port should bind");
        let hosting = load_hosting_plan(|name| match name {
            EDGE_PORT_ENV => Some(port.to_string()),
            _ => None,
        })
        .expect("ephemeral hosting plan should build");
        let server = CloudishApp::from_runtime_defaults_with_hosting(
            runtime_defaults("server-bind-retry"),
            hosting,
        )
        .expect("server bootstrap should succeed");

        let error = server
            .serve_with_shutdown(async {})
            .await
            .expect_err("bind failure should surface");
        assert!(matches!(error, super::StartupError::Bind { .. }));
        drop(occupied);

        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("retry listener should bind");
        server
            .serve_listener_with_shutdown(listener, async {})
            .await
            .expect("server should remain reusable after bind failure");
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
    async fn serve_with_shutdown_reports_wire_log_init_errors_before_binding()
    {
        let port = available_port();
        let hosting = load_hosting_plan(|name| match name {
            EDGE_PORT_ENV => Some(port.to_string()),
            EDGE_WIRE_LOG_ENV => Some("on".to_owned()),
            _ => None,
        })
        .expect("wire log hosting plan should build");
        let server = CloudishApp::from_runtime_defaults_with_hosting(
            runtime_defaults("wire-log-bind-error"),
            hosting,
        )
        .expect("server bootstrap should succeed");

        let error = server
            .serve_with_shutdown_with_wire_log_init(async {}, |_| {
                Err(WireLogInitError::GlobalSubscriberAlreadySet)
            })
            .await
            .expect_err("wire log init failure should surface");

        assert!(matches!(
            error,
            super::StartupError::WireLogInit(
                WireLogInitError::GlobalSubscriberAlreadySet
            )
        ));
        let listener = StdTcpListener::bind(("127.0.0.1", port))
            .expect("wire log init failure should happen before bind");
        drop(listener);
    }

    #[tokio::test]
    async fn serve_listener_with_wire_log_disabled_ignores_existing_tracing_subscriber()
     {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "wire-log-disabled-existing-subscriber-serving",
        ))
        .expect("server bootstrap should succeed");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            server
                .serve_listener_with_shutdown_with_wire_log_init(
                    listener,
                    async {
                        let _ = shutdown_rx.await;
                    },
                    |_| Err(WireLogInitError::GlobalSubscriberAlreadySet),
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");
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
            let request = create_bucket_request("/transport-content-length");
            send_http_request(address, &request)
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
            let request = create_bucket_request("/transport-chunked");
            send_http_request(address, &request)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn serve_listener_handles_health_checks_while_sqs_long_poll_is_waiting()
     {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-long-poll-health",
        ))
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

        let create_queue_request = query_form_request(
            "/",
            "Action=CreateQueue&Version=2012-11-05&QueueName=orders",
        );
        let create_queue_response = tokio::task::spawn_blocking(move || {
            send_http_request(address, &create_queue_request)
        })
        .await
        .expect("queue creation task should complete")
        .expect("queue creation should succeed");
        assert!(create_queue_response.starts_with("HTTP/1.1 200 OK\r\n"));

        let long_poll_request = query_form_request(
            "/000000000000/orders",
            "Action=ReceiveMessage&Version=2012-11-05&WaitTimeSeconds=1",
        );
        let long_poll = thread::spawn(move || {
            send_http_request(address, &long_poll_request)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let health_response = tokio::time::timeout(
            Duration::from_millis(300),
            tokio::task::spawn_blocking(move || {
                send_http_request(
                    address,
                    "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
                )
            }),
        )
        .await
        .expect("health request should not wait behind long polling")
        .expect("health request task should complete")
        .expect("health request should succeed");
        let long_poll_response = long_poll
            .join()
            .expect("long poll thread should finish cleanly")
            .expect("long poll should succeed");

        shutdown_tx.send(()).expect("server should still be running");
        let result = handle.await.expect("server task should complete");
        result.expect("server should stop cleanly");

        assert!(health_response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(long_poll_response.starts_with("HTTP/1.1 200 OK\r\n"));
    }

    #[tokio::test]
    async fn serve_listener_shutdown_cancels_in_flight_sqs_long_polls() {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-long-poll-shutdown",
        ))
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

        let create_queue_request = query_form_request(
            "/",
            "Action=CreateQueue&Version=2012-11-05&QueueName=orders",
        );
        let create_queue_response = tokio::task::spawn_blocking(move || {
            send_http_request(address, &create_queue_request)
        })
        .await
        .expect("queue creation task should complete")
        .expect("queue creation should succeed");
        assert!(create_queue_response.starts_with("HTTP/1.1 200 OK\r\n"));

        let long_poll_request = query_form_request(
            "/000000000000/orders",
            "Action=ReceiveMessage&Version=2012-11-05&WaitTimeSeconds=20",
        );
        let long_poll = tokio::task::spawn_blocking(move || {
            send_http_request(address, &long_poll_request)
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        shutdown_tx.send(()).expect("server should still be running");
        let server_result =
            tokio::time::timeout(Duration::from_millis(500), handle)
                .await
                .expect(
                    "shutdown should not wait for the full long-poll window",
                )
                .expect("server task should complete");
        server_result.expect("server should stop cleanly");

        let _ = tokio::time::timeout(Duration::from_secs(1), long_poll)
            .await
            .expect("client should unblock after shutdown")
            .expect("client task should complete");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serve_listener_shutdown_cancels_in_flight_lambda_invokes() {
        let server = CloudishApp::from_runtime_defaults(runtime_defaults(
            "server-lambda-shutdown",
        ))
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

        let create_role_request = create_iam_role_request("lambda-role");
        let create_role_response = tokio::task::spawn_blocking(move || {
            send_http_request(address, &create_role_request)
        })
        .await
        .expect("role creation task should complete")
        .expect("role creation should succeed");
        assert!(create_role_response.starts_with("HTTP/1.1 200 OK\r\n"));

        let archive = lambda_archive_base64(&[(
            "bootstrap",
            "#!/bin/sh\nsleep 20\ncat\n",
        )]);
        let create_request = create_lambda_request("slow", &archive);
        let create_response = tokio::task::spawn_blocking(move || {
            send_http_request(address, &create_request)
        })
        .await
        .expect("function creation task should complete")
        .expect("function creation should succeed");
        if !create_response.starts_with("HTTP/1.1 201 Created\r\n") {
            panic!("unexpected function create response:\n{create_response}");
        }

        let invoke_request = invoke_lambda_request("slow", r#"{"ok":true}"#);
        let invoke = tokio::task::spawn_blocking(move || {
            send_http_request(address, &invoke_request)
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        shutdown_tx.send(()).expect("server should still be running");
        let server_result = tokio::time::timeout(
            Duration::from_millis(500),
            handle,
        )
        .await
        .expect("shutdown should not wait for the full Lambda invoke window")
        .expect("server task should complete");
        server_result.expect("server should stop cleanly");

        let _ = tokio::time::timeout(Duration::from_secs(1), invoke)
            .await
            .expect("invoke client should unblock after shutdown")
            .expect("invoke client task should complete");
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
    async fn serve_listener_reports_wire_log_init_errors_before_writing_ready_file()
     {
        let ready_root = temporary_directory("wire-log-ready-file");
        let ready_file = ready_root.join("nested").join("cloudish-ready");
        let hosting = load_hosting_plan(|name| match name {
            EDGE_READY_FILE_ENV => {
                Some(ready_file.to_string_lossy().into_owned())
            }
            EDGE_WIRE_LOG_ENV => Some("on".to_owned()),
            _ => None,
        })
        .expect("wire log hosting plan should build");
        let server = CloudishApp::from_runtime_defaults_with_hosting(
            runtime_defaults("wire-log-ready-file"),
            hosting,
        )
        .expect("server bootstrap should succeed");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");

        let error = server
            .serve_listener_with_shutdown_with_wire_log_init(
                listener,
                async {},
                |_| Err(WireLogInitError::GlobalSubscriberAlreadySet),
            )
            .await
            .expect_err("wire log init failure should surface");

        assert!(matches!(
            error,
            super::StartupError::WireLogInit(
                WireLogInitError::GlobalSubscriberAlreadySet
            )
        ));
        assert!(!ready_file.exists());
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
        assert!(status_response.contains("\"stateDirectory\":\""));
        assert!(!health_response.contains("\"defaultAccount\""));
        assert!(!health_response.contains("\"defaultRegion\""));
        assert!(!status_response.contains("\"status\":\"ok\""));
        assert!(!status_response.contains("\"ready\":true"));
        let status_body = status_response
            .split("\r\n\r\n")
            .nth(1)
            .expect("status response should include a body");
        let status_json: Value = serde_json::from_str(status_body)
            .expect("status response body should be valid JSON");
        assert_eq!(
            status_json.as_object().map(|object| object.len()),
            Some(3)
        );
        assert!(
            status_json
                .get("defaultAccount")
                .expect("defaultAccount should exist")
                .is_string()
        );
        assert!(
            status_json
                .get("defaultRegion")
                .expect("defaultRegion should exist")
                .is_string()
        );
        assert!(status_json.get("serviceCount").is_none());
        assert!(status_json.get("enabledServices").is_none());
        assert!(status_json.get("status").is_none());
        assert!(status_json.get("ready").is_none());

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
