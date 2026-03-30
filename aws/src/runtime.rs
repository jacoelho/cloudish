use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;

/// Pure hosting contracts that keep service logic free of filesystem, socket,
/// and process implementation types.

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct BlobKey {
    namespace: String,
    name: String,
}

impl BlobKey {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self { namespace: namespace.into(), name: name.into() }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for BlobKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobMetadata {
    key: BlobKey,
    byte_len: usize,
}

impl BlobMetadata {
    pub fn new(key: BlobKey, byte_len: usize) -> Self {
        Self { key, byte_len }
    }

    pub fn key(&self) -> &BlobKey {
        &self.key
    }

    pub fn byte_len(&self) -> usize {
        self.byte_len
    }
}

pub trait BlobStore: Send + Sync {
    /// Stores a blob by key.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the underlying blob store cannot
    /// persist the body.
    fn put(
        &self,
        key: &BlobKey,
        body: &[u8],
    ) -> Result<BlobMetadata, InfrastructureError>;

    /// Loads a blob by key.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the underlying blob store cannot be
    /// queried.
    fn get(
        &self,
        key: &BlobKey,
    ) -> Result<Option<Vec<u8>>, InfrastructureError>;

    /// Deletes a blob by key.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the underlying blob store cannot
    /// delete the blob.
    fn delete(&self, key: &BlobKey) -> Result<(), InfrastructureError>;
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct PayloadId(String);

impl PayloadId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PayloadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPayload {
    id: PayloadId,
    byte_len: usize,
}

impl StoredPayload {
    pub fn new(id: PayloadId, byte_len: usize) -> Self {
        Self { id, byte_len }
    }

    pub fn id(&self) -> &PayloadId {
        &self.id
    }

    pub fn byte_len(&self) -> usize {
        self.byte_len
    }
}

pub trait PayloadStore: Send + Sync {
    /// Persists a payload and returns its stable identifier.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the payload store cannot persist the
    /// body.
    fn persist(
        &self,
        name_hint: &str,
        body: &[u8],
    ) -> Result<StoredPayload, InfrastructureError>;

    /// Loads a previously persisted payload.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the payload store cannot be read.
    fn read(
        &self,
        id: &PayloadId,
    ) -> Result<Option<Vec<u8>>, InfrastructureError>;

    /// Deletes a previously persisted payload.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the payload store cannot delete the
    /// requested payload.
    fn delete(&self, id: &PayloadId) -> Result<(), InfrastructureError>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self { host: host.into(), port }
    }

    pub fn localhost(port: u16) -> Self {
        Self::new("127.0.0.1", port)
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerCommand {
    args: Vec<String>,
    environment: BTreeMap<String, String>,
    name: String,
}

impl ContainerCommand {
    pub fn new(name: impl Into<String>, program: impl Into<String>) -> Self {
        Self {
            args: vec![program.into()],
            environment: BTreeMap::new(),
            name: name.into(),
        }
    }

    pub fn with_arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn with_env(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.environment.insert(name.into(), value.into());
        self
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }

    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn program(&self) -> &str {
        self.args.first().map(String::as_str).unwrap_or_default()
    }
}

pub trait RunningContainer: Send + Sync {
    fn id(&self) -> &str;

    fn name(&self) -> &str;

    /// Stops the running container.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the container cannot be stopped.
    fn stop(&self) -> Result<(), InfrastructureError>;
}

pub trait ContainerRuntime: Send + Sync {
    /// Starts a container from the supplied command specification.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the runtime cannot start the
    /// requested container.
    fn start(
        &self,
        command: &ContainerCommand,
    ) -> Result<Box<dyn RunningContainer>, InfrastructureError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpProxySpec {
    listen: Endpoint,
    upstream: Endpoint,
}

impl TcpProxySpec {
    pub fn new(listen: Endpoint, upstream: Endpoint) -> Self {
        Self { listen, upstream }
    }

    pub fn listen(&self) -> &Endpoint {
        &self.listen
    }

    pub fn upstream(&self) -> &Endpoint {
        &self.upstream
    }
}

pub trait RunningTcpProxy: Send + Sync {
    fn listen_endpoint(&self) -> Endpoint;

    /// Stops the running TCP proxy.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the proxy cannot be stopped.
    fn stop(&self) -> Result<(), InfrastructureError>;
}

pub trait TcpProxyRuntime: Send + Sync {
    /// Starts a TCP proxy from the supplied specification.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the proxy cannot be started.
    fn start(
        &self,
        spec: &TcpProxySpec,
    ) -> Result<Box<dyn RunningTcpProxy>, InfrastructureError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpForwardRequest {
    body: Vec<u8>,
    endpoint: Endpoint,
    headers: Vec<(String, String)>,
    method: String,
    path: String,
}

impl HttpForwardRequest {
    pub fn new(
        endpoint: Endpoint,
        method: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            body: Vec::new(),
            endpoint,
            headers: Vec::new(),
            method: method.into(),
            path: path.into(),
        }
    }

    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    pub fn with_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpForwardResponse {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    reason_phrase: String,
    status_code: u16,
}

impl HttpForwardResponse {
    pub fn new(
        status_code: u16,
        reason_phrase: impl Into<String>,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            body,
            headers,
            reason_phrase: reason_phrase.into(),
            status_code,
        }
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    pub fn reason_phrase(&self) -> &str {
        &self.reason_phrase
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }
}

pub trait HttpForwarder: Send + Sync {
    /// Forwards an HTTP request to a remote endpoint.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the request cannot be forwarded or
    /// the response cannot be read.
    fn forward(
        &self,
        request: &HttpForwardRequest,
    ) -> Result<HttpForwardResponse, InfrastructureError>;

    /// Forwards an HTTP request and allows request-scoped callers to stop the
    /// operation when their own shutdown boundary is reached.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the request cannot be forwarded,
    /// the response cannot be read, or the caller cancels the request.
    fn forward_with_cancellation(
        &self,
        request: &HttpForwardRequest,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<HttpForwardResponse, InfrastructureError> {
        let _ = is_cancelled;
        self.forward(request)
    }
}

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub trait RandomSource: Send + Sync {
    /// Fills the provided slice with random bytes.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when random data cannot be produced.
    fn fill_bytes(&self, bytes: &mut [u8]) -> Result<(), InfrastructureError>;
}

pub trait ScheduledTaskHandle: Send + Sync {
    /// Cancels the scheduled task.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when cancellation cannot be completed.
    fn cancel(&self) -> Result<(), InfrastructureError>;
}

pub trait BackgroundScheduler: Send + Sync {
    /// Schedules a repeating background task.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the scheduler cannot register the
    /// requested task.
    fn schedule_repeating(
        &self,
        task_name: String,
        interval: Duration,
        task: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Box<dyn ScheduledTaskHandle>, InfrastructureError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    emitted_at: SystemTime,
    message: String,
    stream: String,
}

impl LogRecord {
    pub fn new(
        stream: impl Into<String>,
        message: impl Into<String>,
        emitted_at: SystemTime,
    ) -> Self {
        Self { emitted_at, message: message.into(), stream: stream.into() }
    }

    pub fn emitted_at(&self) -> SystemTime {
        self.emitted_at
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn stream(&self) -> &str {
        &self.stream
    }
}

pub trait LogSink: Send + Sync {
    /// Emits a structured log record.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the sink cannot persist or forward
    /// the record.
    fn emit(&self, record: &LogRecord) -> Result<(), InfrastructureError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LambdaExecutionPackage {
    ImageUri(String),
    ZipArchive(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LambdaInvocationRequest {
    environment: BTreeMap<String, String>,
    function_name: String,
    handler: String,
    package: LambdaExecutionPackage,
    payload: Vec<u8>,
    runtime: String,
    version: String,
}

impl LambdaInvocationRequest {
    pub fn new(
        function_name: impl Into<String>,
        version: impl Into<String>,
        runtime: impl Into<String>,
        handler: impl Into<String>,
        package: LambdaExecutionPackage,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            environment: BTreeMap::new(),
            function_name: function_name.into(),
            handler: handler.into(),
            package,
            payload,
            runtime: runtime.into(),
            version: version.into(),
        }
    }

    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn handler(&self) -> &str {
        &self.handler
    }

    pub fn package(&self) -> &LambdaExecutionPackage {
        &self.package
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn runtime(&self) -> &str {
        &self.runtime
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn with_environment(
        mut self,
        environment: BTreeMap<String, String>,
    ) -> Self {
        self.environment = environment;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LambdaInvocationResult {
    function_error: Option<String>,
    payload: Vec<u8>,
}

impl LambdaInvocationResult {
    pub fn new(
        payload: Vec<u8>,
        function_error: Option<impl Into<String>>,
    ) -> Self {
        Self { payload, function_error: function_error.map(Into::into) }
    }

    pub fn function_error(&self) -> Option<&str> {
        self.function_error.as_deref()
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

pub trait LambdaExecutor: Send + Sync {
    /// Executes a Lambda invocation synchronously.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the Lambda runtime cannot execute
    /// the request.
    fn invoke(
        &self,
        request: &LambdaInvocationRequest,
    ) -> Result<LambdaInvocationResult, InfrastructureError>;

    /// Executes a Lambda invocation synchronously with an optional
    /// request-scoped cancellation boundary.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the Lambda runtime cannot execute
    /// the request.
    fn invoke_with_cancellation(
        &self,
        request: &LambdaInvocationRequest,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<LambdaInvocationResult, InfrastructureError> {
        let _ = is_cancelled;
        self.invoke(request)
    }

    /// Executes a Lambda invocation asynchronously.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the runtime cannot enqueue or
    /// launch the invocation.
    fn invoke_async(
        &self,
        request: LambdaInvocationRequest,
    ) -> Result<(), InfrastructureError>;

    /// Validates a Lambda zip archive for the selected runtime and handler.
    ///
    /// # Errors
    ///
    /// Returns `InfrastructureError` when the archive is invalid or cannot be
    /// inspected.
    fn validate_zip(
        &self,
        runtime: &str,
        handler: &str,
        archive: &[u8],
    ) -> Result<(), InfrastructureError>;
}

#[derive(Debug, Error)]
pub enum InfrastructureError {
    #[error("blob store operation `{operation}` failed for `{key}`: {source}")]
    BlobStore {
        operation: &'static str,
        key: BlobKey,
        #[source]
        source: io::Error,
    },
    #[error(
        "container operation `{operation}` failed for `{container}`: {source}"
    )]
    Container {
        container: String,
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error(
        "HTTP forwarding operation `{operation}` failed for `{endpoint}`: {source}"
    )]
    HttpForward {
        endpoint: Endpoint,
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("log sink `{sink}` failed: {source}")]
    LogSink {
        sink: String,
        #[source]
        source: io::Error,
    },
    #[error(
        "payload store operation `{operation}` failed for `{payload}`: {source}"
    )]
    PayloadStore {
        operation: &'static str,
        payload: String,
        #[source]
        source: io::Error,
    },
    #[error("random source `{adapter}` failed: {source}")]
    RandomSource {
        adapter: &'static str,
        #[source]
        source: io::Error,
    },
    #[error(
        "scheduler operation `{operation}` failed for `{task_name}`: {source}"
    )]
    Scheduler {
        operation: &'static str,
        task_name: String,
        #[source]
        source: io::Error,
    },
    #[error(
        "tcp proxy operation `{operation}` failed for `{endpoint}`: {source}"
    )]
    TcpProxy {
        endpoint: Endpoint,
        operation: &'static str,
        #[source]
        source: io::Error,
    },
}

impl InfrastructureError {
    pub fn blob_store(
        operation: &'static str,
        key: &BlobKey,
        source: io::Error,
    ) -> Self {
        Self::BlobStore { operation, key: key.clone(), source }
    }

    pub fn container(
        operation: &'static str,
        container: impl Into<String>,
        source: io::Error,
    ) -> Self {
        Self::Container { container: container.into(), operation, source }
    }

    pub fn http_forward(
        operation: &'static str,
        endpoint: &Endpoint,
        source: io::Error,
    ) -> Self {
        Self::HttpForward { endpoint: endpoint.clone(), operation, source }
    }

    pub fn log_sink(sink: impl Into<String>, source: io::Error) -> Self {
        Self::LogSink { sink: sink.into(), source }
    }

    pub fn payload_store(
        operation: &'static str,
        payload: impl Into<String>,
        source: io::Error,
    ) -> Self {
        Self::PayloadStore { operation, payload: payload.into(), source }
    }

    pub fn random_source(adapter: &'static str, source: io::Error) -> Self {
        Self::RandomSource { adapter, source }
    }

    pub fn scheduler(
        operation: &'static str,
        task_name: impl Into<String>,
        source: io::Error,
    ) -> Self {
        Self::Scheduler { operation, task_name: task_name.into(), source }
    }

    pub fn tcp_proxy(
        operation: &'static str,
        endpoint: &Endpoint,
        source: io::Error,
    ) -> Self {
        Self::TcpProxy { endpoint: endpoint.clone(), operation, source }
    }
}
