use async_trait::async_trait;
use aws::{Endpoint, InfrastructureError};
use futures::StreamExt;
use futures::stream;
use futures::{Sink, SinkExt};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType,
    InitWriter, OkResponse, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};
use pgwire::api::auth::{
    DefaultServerParameterProvider, StartupHandler, finish_authentication,
    protocol_negotiation, save_startup_parameters_to_metadata,
};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::{
    ClientInfo, PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use rds::{
    RdsAuthEndpointSource, RdsBackendRuntime, RdsBackendSpec, RdsEngine,
    RdsIamTokenValidator, RunningRdsBackend,
};
use sha1::{Digest as Sha1Digest, Sha1};
use std::fmt;
use std::io;
use std::net::{TcpListener as StdTcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;

#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadRdsBackendRuntime;

impl ThreadRdsBackendRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl RdsBackendRuntime for ThreadRdsBackendRuntime {
    fn start(
        &self,
        spec: &RdsBackendSpec,
    ) -> Result<Box<dyn RunningRdsBackend>, InfrastructureError> {
        let listener =
            StdTcpListener::bind(endpoint_address(&spec.listen_endpoint))
                .map_err(|source| {
                    InfrastructureError::tcp_proxy(
                        "bind-rds-backend",
                        &spec.listen_endpoint,
                        source,
                    )
                })?;
        listener.set_nonblocking(true).map_err(|source| {
            InfrastructureError::tcp_proxy(
                "configure-rds-backend",
                &spec.listen_endpoint,
                source,
            )
        })?;
        let local_address = listener.local_addr().map_err(|source| {
            InfrastructureError::tcp_proxy(
                "discover-rds-backend-address",
                &spec.listen_endpoint,
                source,
            )
        })?;
        let listen = Endpoint::new(
            local_address.ip().to_string(),
            local_address.port(),
        );
        let spec = spec.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let thread_name = match protocol_family(spec.engine) {
            ProtocolFamily::Postgres => {
                format!("rds-postgres-{}", listen.port())
            }
            ProtocolFamily::Mysql => format!("rds-mysql-{}", listen.port()),
        };
        let join = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build();
                let Ok(runtime) = runtime else {
                    return;
                };
                runtime.block_on(async move {
                    let listener = TcpListener::from_std(listener);
                    let Ok(listener) = listener else {
                        return;
                    };
                    match protocol_family(spec.engine) {
                        ProtocolFamily::Postgres => {
                            run_postgres_backend(listener, spec, stop_flag)
                                .await;
                        }
                        ProtocolFamily::Mysql => {
                            run_mysql_backend(listener, spec, stop_flag).await;
                        }
                    }
                });
            })
            .map_err(|source| {
                InfrastructureError::tcp_proxy(
                    "spawn-rds-backend",
                    &listen,
                    source,
                )
            })?;

        Ok(Box::new(ThreadRdsBackendHandle {
            join: Mutex::new(Some(join)),
            listen,
            stop,
        }))
    }
}

struct ThreadRdsBackendHandle {
    join: Mutex<Option<JoinHandle<()>>>,
    listen: Endpoint,
    stop: Arc<AtomicBool>,
}

impl RunningRdsBackend for ThreadRdsBackendHandle {
    fn listen_endpoint(&self) -> Endpoint {
        self.listen.clone()
    }

    fn stop(&self) -> Result<(), InfrastructureError> {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(endpoint_address(&self.listen));

        if let Some(join) = recover(self.join.lock()).take() {
            join.join().map_err(|_| {
                InfrastructureError::tcp_proxy(
                    "stop-rds-backend",
                    &self.listen,
                    io::Error::other("RDS backend worker panicked"),
                )
            })?;
        }

        Ok(())
    }
}

impl Drop for ThreadRdsBackendHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

async fn run_postgres_backend(
    listener: TcpListener,
    spec: RdsBackendSpec,
    stop: Arc<AtomicBool>,
) {
    let handlers = Arc::new(PostgresHandlers::new(spec));

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let accepted = listener.accept().await;
        let Ok((socket, _)) = accepted else {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            continue;
        };

        let handlers = Arc::clone(&handlers);
        tokio::spawn(async move {
            let _ = process_socket(socket, None, handlers).await;
        });
    }
}

async fn run_mysql_backend(
    listener: TcpListener,
    spec: RdsBackendSpec,
    stop: Arc<AtomicBool>,
) {
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let accepted = listener.accept().await;
        let Ok((socket, _)) = accepted else {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            continue;
        };

        let session = MysqlSession::new(&spec);
        tokio::spawn(async move {
            let (reader, writer) = socket.into_split();
            let _ =
                AsyncMysqlIntermediary::run_on(session, reader, writer).await;
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolFamily {
    Mysql,
    Postgres,
}

fn protocol_family(engine: RdsEngine) -> ProtocolFamily {
    match engine {
        RdsEngine::Postgres | RdsEngine::AuroraPostgresql => {
            ProtocolFamily::Postgres
        }
        RdsEngine::Mysql
        | RdsEngine::Aurora
        | RdsEngine::AuroraMysql
        | RdsEngine::MariaDb => ProtocolFamily::Mysql,
    }
}

#[derive(Debug)]
struct PostgresHandlers {
    handler: Arc<PostgresHandler>,
}

impl PostgresHandlers {
    fn new(spec: RdsBackendSpec) -> Self {
        Self {
            handler: Arc::new(PostgresHandler {
                auth_endpoint_source: spec.auth_endpoint_source,
                database_name: spec.database_name,
                iam_token_validator: spec.iam_token_validator,
                master_password: spec.master_password,
                master_username: spec.master_username,
            }),
        }
    }
}

impl PgWireServerHandlers for PostgresHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.handler)
    }
}

#[derive(Debug)]
struct PostgresHandler {
    auth_endpoint_source: Arc<dyn RdsAuthEndpointSource + Send + Sync>,
    database_name: Option<String>,
    iam_token_validator: Option<Arc<dyn RdsIamTokenValidator + Send + Sync>>,
    master_password: String,
    master_username: String,
}

impl PostgresHandler {
    fn server_parameters(&self) -> DefaultServerParameterProvider {
        let mut provider = DefaultServerParameterProvider::default();
        provider.session_authorization = Some(self.master_username.clone());
        provider
    }

    fn validate_password(&self, username: &str, password: &str) -> bool {
        if username != self.master_username {
            return false;
        }
        if password == self.master_password {
            return true;
        }

        self.iam_token_validator.as_ref().is_some_and(|validator| {
            self.auth_endpoint_source.endpoints().into_iter().any(|endpoint| {
                validator.validate(&endpoint, username, password).is_ok()
            })
        })
    }
}

#[async_trait]
impl StartupHandler for PostgresHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(
                    PgWireConnectionState::AuthenticationInProgress,
                );
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(password) => {
                let password = password.into_password()?;
                let username = client
                    .metadata()
                    .get("user")
                    .map(String::as_str)
                    .unwrap_or_default();
                let password = password.password.as_str();
                if !self.validate_password(username, password) {
                    return Err(PgWireError::InvalidPassword(
                        username.to_owned(),
                    ));
                }
                finish_authentication(client, &self.server_parameters())
                    .await?;
            }
            _ => {}
        }

        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for PostgresHandler {
    async fn do_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        C::Error: fmt::Debug,
    {
        let database_name = client
            .metadata()
            .get("database")
            .map(String::as_str)
            .or(self.database_name.as_deref());
        let username = client
            .metadata()
            .get("user")
            .map(String::as_str)
            .unwrap_or(self.master_username.as_str());

        postgres_query_response(query, database_name, username)
    }
}

#[derive(Debug, Clone)]
struct MysqlSession {
    current_database: Option<String>,
    master_password: String,
    master_username: String,
    version: String,
}

impl MysqlSession {
    fn new(spec: &RdsBackendSpec) -> Self {
        Self {
            current_database: spec.database_name.clone(),
            master_password: spec.master_password.clone(),
            master_username: spec.master_username.clone(),
            version: mysql_version(spec.engine).to_owned(),
        }
    }
}

#[async_trait]
impl<W> AsyncMysqlShim<W> for MysqlSession
where
    W: AsyncWrite + Send + Unpin,
{
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(1, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        match mysql_query_response(
            query,
            self.current_database.as_deref(),
            self.master_username.as_str(),
            self.version.as_str(),
        ) {
            LocalQueryResponse::Command => {
                results.completed(OkResponse::default()).await
            }
            LocalQueryResponse::RowSet { columns, row } => {
                let columns = columns
                    .iter()
                    .map(|column| Column {
                        table: String::new(),
                        column: column.clone(),
                        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                        colflags: ColumnFlags::empty(),
                    })
                    .collect::<Vec<_>>();
                let mut writer = results.start(&columns).await?;
                for value in &row {
                    writer.write_col(value.as_str())?;
                }
                writer.end_row().await?;
                writer.finish().await
            }
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        self.current_database = Some(database.to_owned());
        writer.ok().await
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        auth_plugin == "mysql_native_password"
            && username == self.master_username.as_bytes()
            && auth_data
                == mysql_native_password_response(
                    self.master_password.as_bytes(),
                    salt,
                )
    }

    fn version(&self) -> String {
        self.version.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalQueryResponse {
    Command,
    RowSet { columns: Vec<String>, row: Vec<String> },
}

fn postgres_query_response(
    query: &str,
    database_name: Option<&str>,
    username: &str,
) -> PgWireResult<Vec<Response>> {
    match local_query_response(
        query,
        database_name,
        username,
        "PostgreSQL 16.3-cloudish",
    ) {
        LocalQueryResponse::Command => {
            let tag = command_tag(query);
            Ok(vec![Response::Execution(Tag::new(&tag).with_rows(0))])
        }
        LocalQueryResponse::RowSet { columns, row } => {
            let schema = Arc::new(
                columns
                    .into_iter()
                    .map(|column| {
                        FieldInfo::new(
                            column,
                            None,
                            None,
                            Type::VARCHAR,
                            FieldFormat::Text,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            let schema_ref = Arc::clone(&schema);
            let mut encoder = DataRowEncoder::new(Arc::clone(&schema_ref));
            let rows = stream::iter([row]).map(move |row| {
                for value in &row {
                    encoder.encode_field(&Some(value.as_str()))?;
                }
                Ok(encoder.take_row())
            });

            Ok(vec![Response::Query(QueryResponse::new(schema_ref, rows))])
        }
    }
}

fn mysql_query_response(
    query: &str,
    database_name: Option<&str>,
    username: &str,
    version: &str,
) -> LocalQueryResponse {
    local_query_response(query, database_name, username, version)
}

fn local_query_response(
    query: &str,
    database_name: Option<&str>,
    username: &str,
    version: &str,
) -> LocalQueryResponse {
    let statement = trim_statement(query);
    let lowered = statement.to_ascii_lowercase();

    if lowered.is_empty() {
        return LocalQueryResponse::Command;
    }

    if let Some(name) = lowered.strip_prefix("show ") {
        return LocalQueryResponse::RowSet {
            columns: vec![name.to_owned()],
            row: vec![show_value(name.trim(), version).to_owned()],
        };
    }

    if lowered.starts_with("select ") {
        let projections = split_select_list(&statement["select ".len()..]);
        let mut columns = Vec::with_capacity(projections.len());
        let mut row = Vec::with_capacity(projections.len());

        for projection in projections {
            let column = projection_column_name(&projection);
            let value = projection_value(
                &projection,
                database_name,
                username,
                version,
            );
            columns.push(column);
            row.push(value);
        }

        return LocalQueryResponse::RowSet { columns, row };
    }

    LocalQueryResponse::Command
}

fn show_value<'a>(name: &'a str, version: &'a str) -> &'a str {
    match name {
        "server_version" => version,
        "transaction_read_only" | "default_transaction_read_only" => "off",
        _ => "on",
    }
}

fn projection_value(
    projection: &str,
    database_name: Option<&str>,
    username: &str,
    version: &str,
) -> String {
    let expression = projection_expression(projection);
    let normalized = expression.to_ascii_lowercase();

    match normalized.as_str() {
        "1" => "1".to_owned(),
        "current_database()" | "database()" | "schema()" => {
            database_name.unwrap_or("postgres").to_owned()
        }
        "current_user" | "current_user()" | "session_user" | "user()" => {
            username.to_owned()
        }
        "version()" => version.to_owned(),
        _ if normalized.starts_with("@@") => {
            mysql_variable_value(&normalized).to_owned()
        }
        _ if normalized.starts_with('\'') && normalized.ends_with('\'') => {
            expression.trim_matches('\'').to_owned()
        }
        _ => "1".to_owned(),
    }
}

fn mysql_variable_value(name: &str) -> &'static str {
    match name {
        "@@version_comment" => "cloudish",
        "@@system_time_zone" | "@@time_zone" => "UTC",
        "@@transaction_isolation" | "@@tx_isolation" => "REPEATABLE-READ",
        "@@auto_increment_increment" => "1",
        "@@max_allowed_packet" => "67108864",
        _ => "1",
    }
}

fn projection_column_name(projection: &str) -> String {
    let trimmed = projection.trim();
    let lowered = trimmed.to_ascii_lowercase();

    if let Some((_, alias)) = lowered.rsplit_once(" as ") {
        return alias.trim_matches('`').trim_matches('"').to_owned();
    }

    trimmed.to_owned()
}

fn projection_expression(projection: &str) -> &str {
    let trimmed = projection.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if let Some((expression, _)) = lowered.rsplit_once(" as ") {
        let len = expression.len();
        return &trimmed[..len];
    }
    trimmed
}

fn split_select_list(select: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_single_quote = false;
    let bytes = select.as_bytes();

    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b'\'' => in_single_quote = !in_single_quote,
            b',' if !in_single_quote => {
                parts.push(select[start..index].trim().to_owned());
                start = index.saturating_add(1);
            }
            _ => {}
        }
    }

    parts.push(select[start..].trim().to_owned());
    parts
}

fn trim_statement(query: &str) -> &str {
    query.trim().trim_end_matches(';').trim()
}

fn command_tag(query: &str) -> String {
    trim_statement(query)
        .split_whitespace()
        .next()
        .unwrap_or("OK")
        .to_ascii_uppercase()
}

fn mysql_version(engine: RdsEngine) -> &'static str {
    match engine {
        RdsEngine::MariaDb => "11.4.0-MariaDB-cloudish",
        _ => "8.0.36-cloudish",
    }
}

fn mysql_native_password_response(password: &[u8], salt: &[u8]) -> Vec<u8> {
    let stage_1 = Sha1::digest(password);
    let stage_2 = Sha1::digest(stage_1);
    let stage_3 = Sha1::digest([salt, stage_2.as_slice()].concat());

    stage_1
        .iter()
        .zip(stage_3.iter())
        .map(|(left, right)| left ^ right)
        .collect()
}

fn endpoint_address(endpoint: &Endpoint) -> String {
    format!("{}:{}", endpoint.host(), endpoint.port())
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{ThreadRdsBackendRuntime, endpoint_address};
    use crate::ThreadTcpProxyRuntime;
    use aws::{Endpoint, TcpProxyRuntime, TcpProxySpec};
    use rds::{
        RdsAuthEndpointSource, RdsBackendRuntime, RdsBackendSpec, RdsEngine,
    };
    use std::io::{self, Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct EmptyAuthEndpointSource;

    impl RdsAuthEndpointSource for EmptyAuthEndpointSource {
        fn endpoints(&self) -> Vec<Endpoint> {
            Vec::new()
        }
    }

    #[test]
    fn postgres_backend_emits_cleartext_password_challenge() {
        let runtime = ThreadRdsBackendRuntime::new();
        let backend = runtime
            .start(&backend_spec())
            .expect("postgres backend should start");

        let result = run_postgres_startup(backend.listen_endpoint());

        backend.stop().expect("postgres backend should stop");

        result.expect("backend should emit the password challenge");
    }

    #[test]
    fn postgres_proxy_relays_startup_and_authentication() {
        let backend_runtime = ThreadRdsBackendRuntime::new();
        let backend = backend_runtime
            .start(&backend_spec())
            .expect("postgres backend should start");
        let proxy_runtime = ThreadTcpProxyRuntime::new();
        let proxy = proxy_runtime
            .start(&TcpProxySpec::new(
                Endpoint::localhost(0),
                backend.listen_endpoint(),
            ))
            .expect("proxy should start");

        let result = run_postgres_startup(proxy.listen_endpoint());

        proxy.stop().expect("proxy should stop");
        backend.stop().expect("postgres backend should stop");

        result.expect("proxy should relay startup and authentication");
    }

    fn backend_spec() -> RdsBackendSpec {
        RdsBackendSpec {
            auth_endpoint_source: Arc::new(EmptyAuthEndpointSource),
            database_name: Some("app".to_owned()),
            engine: RdsEngine::Postgres,
            iam_token_validator: None,
            listen_endpoint: Endpoint::localhost(0),
            master_password: "secret123!".to_owned(),
            master_username: "postgres".to_owned(),
        }
    }

    fn run_postgres_startup(endpoint: Endpoint) -> io::Result<()> {
        let mut stream = TcpStream::connect(endpoint_address(&endpoint))?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        stream.set_write_timeout(Some(Duration::from_secs(1)))?;

        stream.write_all(&startup_message("postgres", "app"))?;
        assert_authentication_code(&mut stream, 3)?;

        stream.write_all(&password_message("secret123!"))?;
        assert_authentication_code(&mut stream, 0)?;

        let mut saw_ready = false;
        for _ in 0..16 {
            let (tag, _) = read_backend_message(&mut stream)?;
            if tag == b'Z' {
                saw_ready = true;
                break;
            }
        }
        if !saw_ready {
            return Err(io::Error::other(
                "backend should complete startup with ReadyForQuery",
            ));
        }

        Ok(())
    }

    fn startup_message(user: &str, database: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196608u32.to_be_bytes());
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        body.extend_from_slice(b"database\0");
        body.extend_from_slice(database.as_bytes());
        body.push(0);
        body.push(0);

        let mut message = Vec::new();
        message.extend_from_slice(
            &(body.len().saturating_add(4) as u32).to_be_bytes(),
        );
        message.extend_from_slice(&body);
        message
    }

    fn password_message(password: &str) -> Vec<u8> {
        let mut message = Vec::new();
        message.push(b'p');
        message
            .extend_from_slice(
                &(password.len().saturating_add(5) as u32).to_be_bytes(),
            );
        message.extend_from_slice(password.as_bytes());
        message.push(0);
        message
    }

    fn assert_authentication_code(
        stream: &mut TcpStream,
        expected: u32,
    ) -> io::Result<()> {
        let (tag, body) = read_backend_message(stream)?;
        if tag != b'R' {
            return Err(io::Error::other(
                "backend should send an authentication message",
            ));
        }
        let body_bytes: [u8; 4] =
            body.as_slice().try_into().map_err(|_| {
                io::Error::other("authentication body should be four bytes")
            })?;
        let actual = u32::from_be_bytes(body_bytes);
        if actual != expected {
            return Err(io::Error::other(format!(
                "unexpected authentication code {actual}, expected {expected}"
            )));
        }
        Ok(())
    }

    fn read_backend_message(
        stream: &mut TcpStream,
    ) -> io::Result<(u8, Vec<u8>)> {
        let mut tag = [0; 1];
        stream.read_exact(&mut tag)?;

        let mut length = [0; 4];
        stream.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as usize;
        let mut body = vec![0; length.saturating_sub(4)];
        stream.read_exact(&mut body)?;

        Ok((tag[0], body))
    }
}
