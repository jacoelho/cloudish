use aws::{Endpoint, InfrastructureError};
use elasticache::{
    ElastiCacheAuthenticationType, ElastiCacheNodeRuntime,
    ElastiCacheNodeSpec, ElastiCacheProxyRuntime, ElastiCacheProxySpec,
    RunningElastiCacheNode, RunningElastiCacheProxy,
};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LockResult, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const INVALID_AUTH_RESPONSE: &[u8] =
    b"-ERR invalid username-password pair or user is disabled.\r\n";
const NOAUTH_RESPONSE: &[u8] = b"-NOAUTH Authentication required.\r\n";
const OK_RESPONSE: &[u8] = b"+OK\r\n";
const WRONG_HELLO_ARGS_RESPONSE: &[u8] =
    b"-ERR wrong number of arguments for 'hello' command\r\n";
const UNSUPPORTED_HELLO_RESPONSE: &[u8] =
    b"-ERR only HELLO 3 is supported by Cloudish.\r\n";
const RESP3_HELLO_RESPONSE: &[u8] = b"%7\r\n+server\r\n+redis\r\n+version\r\n+7.0.0\r\n+proto\r\n:3\r\n+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n";

#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadElastiCacheNodeRuntime;

impl ThreadElastiCacheNodeRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl ElastiCacheNodeRuntime for ThreadElastiCacheNodeRuntime {
    fn start(
        &self,
        spec: &ElastiCacheNodeSpec,
    ) -> Result<Box<dyn RunningElastiCacheNode>, InfrastructureError> {
        let listener =
            TcpListener::bind(endpoint_address(spec.listen_endpoint()))
                .map_err(|source| {
                    InfrastructureError::tcp_proxy(
                        "bind-elasticache-node",
                        spec.listen_endpoint(),
                        source,
                    )
                })?;
        listener.set_nonblocking(true).map_err(|source| {
            InfrastructureError::tcp_proxy(
                "configure-elasticache-node",
                spec.listen_endpoint(),
                source,
            )
        })?;
        let local_address = listener.local_addr().map_err(|source| {
            InfrastructureError::tcp_proxy(
                "discover-elasticache-node-address",
                spec.listen_endpoint(),
                source,
            )
        })?;
        let listen = Endpoint::new(
            local_address.ip().to_string(),
            local_address.port(),
        );
        let state = Arc::new(Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let state_for_thread = Arc::clone(&state);
        let thread_name = format!("elasticache-node-{}", spec.group_id());
        let join = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let state = Arc::clone(&state_for_thread);
                            thread::spawn(move || {
                                let _ = stream.set_nonblocking(false);
                                handle_backend_connection(stream, state);
                            });
                        }
                        Err(source)
                            if source.kind() == io::ErrorKind::WouldBlock =>
                        {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(source)
                            if source.kind() == io::ErrorKind::Interrupted => {
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|source| {
                InfrastructureError::tcp_proxy(
                    "spawn-elasticache-node",
                    &listen,
                    source,
                )
            })?;

        Ok(Box::new(ThreadElastiCacheNodeHandle {
            join: Mutex::new(Some(join)),
            listen,
            stop,
            _state: state,
        }))
    }
}

struct ThreadElastiCacheNodeHandle {
    join: Mutex<Option<JoinHandle<()>>>,
    listen: Endpoint,
    stop: Arc<AtomicBool>,
    _state: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl RunningElastiCacheNode for ThreadElastiCacheNodeHandle {
    fn listen_endpoint(&self) -> Endpoint {
        self.listen.clone()
    }

    fn stop(&self) -> Result<(), InfrastructureError> {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(endpoint_address(&self.listen));

        if let Some(join) = recover(self.join.lock()).take() {
            join.join().map_err(|_| {
                InfrastructureError::tcp_proxy(
                    "stop-elasticache-node",
                    &self.listen,
                    io::Error::other("ElastiCache node worker panicked"),
                )
            })?;
        }

        Ok(())
    }
}

impl Drop for ThreadElastiCacheNodeHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadElastiCacheProxyRuntime;

impl ThreadElastiCacheProxyRuntime {
    pub fn new() -> Self {
        Self
    }
}

impl ElastiCacheProxyRuntime for ThreadElastiCacheProxyRuntime {
    fn start(
        &self,
        spec: &ElastiCacheProxySpec,
    ) -> Result<Box<dyn RunningElastiCacheProxy>, InfrastructureError> {
        let listener =
            TcpListener::bind(endpoint_address(spec.listen_endpoint()))
                .map_err(|source| {
                    InfrastructureError::tcp_proxy(
                        "bind-elasticache-proxy",
                        spec.listen_endpoint(),
                        source,
                    )
                })?;
        listener.set_nonblocking(true).map_err(|source| {
            InfrastructureError::tcp_proxy(
                "configure-elasticache-proxy",
                spec.listen_endpoint(),
                source,
            )
        })?;
        let local_address = listener.local_addr().map_err(|source| {
            InfrastructureError::tcp_proxy(
                "discover-elasticache-proxy-address",
                spec.listen_endpoint(),
                source,
            )
        })?;
        let listen = Endpoint::new(
            local_address.ip().to_string(),
            local_address.port(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let spec = spec.clone();
        let thread_name =
            format!("elasticache-proxy-{}", spec.replication_group_id());
        let join = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let spec = spec.clone();
                            thread::spawn(move || {
                                let _ = stream.set_nonblocking(false);
                                let _ = stream.set_nodelay(true);
                                let _ = handle_proxy_connection(stream, &spec);
                            });
                        }
                        Err(source)
                            if source.kind() == io::ErrorKind::WouldBlock =>
                        {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(source)
                            if source.kind() == io::ErrorKind::Interrupted => {
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|source| {
                InfrastructureError::tcp_proxy(
                    "spawn-elasticache-proxy",
                    &listen,
                    source,
                )
            })?;

        Ok(Box::new(ThreadElastiCacheProxyHandle {
            join: Mutex::new(Some(join)),
            listen,
            stop,
        }))
    }
}

struct ThreadElastiCacheProxyHandle {
    join: Mutex<Option<JoinHandle<()>>>,
    listen: Endpoint,
    stop: Arc<AtomicBool>,
}

impl RunningElastiCacheProxy for ThreadElastiCacheProxyHandle {
    fn listen_endpoint(&self) -> Endpoint {
        self.listen.clone()
    }

    fn stop(&self) -> Result<(), InfrastructureError> {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(endpoint_address(&self.listen));

        if let Some(join) = recover(self.join.lock()).take() {
            join.join().map_err(|_| {
                InfrastructureError::tcp_proxy(
                    "stop-elasticache-proxy",
                    &self.listen,
                    io::Error::other("ElastiCache proxy worker panicked"),
                )
            })?;
        }

        Ok(())
    }
}

impl Drop for ThreadElastiCacheProxyHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn handle_backend_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
) {
    while let Ok(Some(command)) = RespCommandReader::new(&mut stream).read() {
        let Some(command_name) = command_name(&command) else {
            let _ = stream.write_all(
                error_response("ERR malformed RESP command").as_slice(),
            );
            break;
        };
        let lower = command_name.to_ascii_lowercase();
        let result = match lower.as_str() {
            "ping" => handle_ping(&command),
            "echo" => handle_echo(&command),
            "set" => handle_set(&command, &state),
            "get" => handle_get(&command, &state),
            "del" => handle_del(&command, &state),
            "select" => handle_select(&command),
            "client" => handle_client(&command),
            "command" => Ok(empty_array_response()),
            "acl" => Ok(error_response(
                "ERR ACL command support is not implemented by Cloudish",
            )),
            "cluster" => Ok(error_response(
                "ERR cluster mode is not supported by Cloudish",
            )),
            "quit" => {
                let _ = stream.write_all(OK_RESPONSE);
                let _ = stream.flush();
                break;
            }
            other => {
                Ok(error_response(&format!("ERR unknown command `{other}`")))
            }
        };

        match result {
            Ok(response) => {
                if stream.write_all(&response).is_err() {
                    break;
                }
                if stream.flush().is_err() {
                    break;
                }
            }
            Err(response) => {
                let _ = stream.write_all(&response);
                let _ = stream.flush();
                break;
            }
        }
    }
}

fn handle_proxy_connection(
    mut client: TcpStream,
    spec: &ElastiCacheProxySpec,
) -> io::Result<()> {
    let Some(command) = RespCommandReader::new(&mut client).read()? else {
        return Ok(());
    };
    let Some(command_name) = command_name(&command) else {
        client.write_all(
            error_response("ERR malformed RESP command").as_slice(),
        )?;
        client.flush()?;
        return Ok(());
    };

    match command_name.to_ascii_lowercase().as_str() {
        "auth" => {
            if !validate_auth_command(spec, &command)? {
                client.write_all(INVALID_AUTH_RESPONSE)?;
                client.flush()?;
                return Ok(());
            }
            client.write_all(OK_RESPONSE)?;
            client.flush()?;
            let backend = connect_backend(spec)?;
            bridge(client, backend);
            Ok(())
        }
        "hello" => {
            if !validate_hello_command(spec, &command, &mut client)? {
                return Ok(());
            }
            let backend = connect_backend(spec)?;
            bridge(client, backend);
            Ok(())
        }
        _ if spec.auth_mode() == ElastiCacheAuthenticationType::NoPassword => {
            let mut backend = connect_backend(spec)?;
            backend.write_all(&serialize_command(&command))?;
            backend.flush()?;
            bridge(client, backend);
            Ok(())
        }
        _ => {
            client.write_all(NOAUTH_RESPONSE)?;
            client.flush()?;
            Ok(())
        }
    }
}

fn validate_auth_command(
    spec: &ElastiCacheProxySpec,
    command: &[Vec<u8>],
) -> io::Result<bool> {
    let (username, password) = match command {
        [_, password] => (None, bytes_to_string(password)?),
        [_, username, password] => {
            (Some(bytes_to_string(username)?), bytes_to_string(password)?)
        }
        _ => return Err(io::Error::other("wrong auth arity")),
    };

    validate_credentials(spec, username.as_deref(), &password)
}

fn validate_hello_command(
    spec: &ElastiCacheProxySpec,
    command: &[Vec<u8>],
    client: &mut TcpStream,
) -> io::Result<bool> {
    if command.len() < 2 {
        client.write_all(WRONG_HELLO_ARGS_RESPONSE)?;
        client.flush()?;
        return Ok(false);
    }
    if bytes_to_string(command_part(command, 1)?)? != "3" {
        client.write_all(UNSUPPORTED_HELLO_RESPONSE)?;
        client.flush()?;
        return Ok(false);
    }

    let mut username = None;
    let mut password = None;
    let mut index = 2;
    while index < command.len() {
        match bytes_to_string(command_part(command, index)?)?
            .to_ascii_lowercase()
            .as_str()
        {
            "auth" => {
                if index.saturating_add(2) >= command.len() {
                    client.write_all(WRONG_HELLO_ARGS_RESPONSE)?;
                    client.flush()?;
                    return Ok(false);
                }
                username =
                    Some(bytes_to_string(command_part(
                        command,
                        index.saturating_add(1),
                    )?)?);
                password =
                    Some(bytes_to_string(command_part(
                        command,
                        index.saturating_add(2),
                    )?)?);
                index = index.saturating_add(3);
            }
            "setname" => {
                if index.saturating_add(1) >= command.len() {
                    client.write_all(WRONG_HELLO_ARGS_RESPONSE)?;
                    client.flush()?;
                    return Ok(false);
                }
                index = index.saturating_add(2);
            }
            _ => {
                index = index.saturating_add(1);
            }
        }
    }

    if spec.auth_mode() != ElastiCacheAuthenticationType::NoPassword {
        let Some(password) = password else {
            client.write_all(NOAUTH_RESPONSE)?;
            client.flush()?;
            return Ok(false);
        };
        if !validate_credentials(spec, username.as_deref(), &password)? {
            client.write_all(INVALID_AUTH_RESPONSE)?;
            client.flush()?;
            return Ok(false);
        }
    }

    client.write_all(RESP3_HELLO_RESPONSE)?;
    client.flush()?;
    Ok(true)
}

fn validate_credentials(
    spec: &ElastiCacheProxySpec,
    username: Option<&str>,
    password: &str,
) -> io::Result<bool> {
    match spec.auth_mode() {
        ElastiCacheAuthenticationType::NoPassword => Ok(true),
        ElastiCacheAuthenticationType::Password => Ok(spec
            .authenticator()
            .validate_password(
                spec.scope(),
                spec.replication_group_id(),
                username,
                password,
            )
            .is_ok()),
        ElastiCacheAuthenticationType::Iam => Ok(spec
            .authenticator()
            .validate_iam_token(
                spec.scope(),
                spec.replication_group_id(),
                password,
            )
            .is_ok()),
    }
}

fn connect_backend(spec: &ElastiCacheProxySpec) -> io::Result<TcpStream> {
    let backend = TcpStream::connect(endpoint_address(spec.upstream()))?;
    backend.set_nodelay(true)?;
    Ok(backend)
}

fn handle_ping(command: &[Vec<u8>]) -> Result<Vec<u8>, Vec<u8>> {
    match command {
        [_] => Ok(b"+PONG\r\n".to_vec()),
        [_, value] => Ok(bulk_string_response(value.as_slice())),
        _ => Err(error_response(
            "ERR wrong number of arguments for 'ping' command",
        )),
    }
}

fn handle_echo(command: &[Vec<u8>]) -> Result<Vec<u8>, Vec<u8>> {
    match command {
        [_, value] => Ok(bulk_string_response(value.as_slice())),
        _ => Err(error_response(
            "ERR wrong number of arguments for 'echo' command",
        )),
    }
}

fn handle_set(
    command: &[Vec<u8>],
    state: &Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
) -> Result<Vec<u8>, Vec<u8>> {
    if command.len() < 3 {
        return Err(error_response(
            "ERR wrong number of arguments for 'set' command",
        ));
    }
    let key = command_part_response(command, 1)?.clone();
    let value = command_part_response(command, 2)?.clone();
    recover(state.lock()).insert(key, value);
    Ok(OK_RESPONSE.to_vec())
}

fn handle_get(
    command: &[Vec<u8>],
    state: &Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
) -> Result<Vec<u8>, Vec<u8>> {
    if command.len() != 2 {
        return Err(error_response(
            "ERR wrong number of arguments for 'get' command",
        ));
    }

    let key = command_part_response(command, 1)?;

    Ok(recover(state.lock())
        .get(key)
        .map(|value| bulk_string_response(value))
        .unwrap_or_else(null_bulk_string_response))
}

fn handle_del(
    command: &[Vec<u8>],
    state: &Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
) -> Result<Vec<u8>, Vec<u8>> {
    if command.len() < 2 {
        return Err(error_response(
            "ERR wrong number of arguments for 'del' command",
        ));
    }
    let mut deleted = 0_i64;
    let mut values = recover(state.lock());
    for key in command.iter().skip(1) {
        if values.remove(key).is_some() {
            deleted = deleted.saturating_add(1);
        }
    }
    Ok(integer_response(deleted))
}

fn handle_select(command: &[Vec<u8>]) -> Result<Vec<u8>, Vec<u8>> {
    if command.len() != 2 {
        return Err(error_response(
            "ERR wrong number of arguments for 'select' command",
        ));
    }
    if command_part_response(command, 1)?.as_slice() == b"0" {
        Ok(OK_RESPONSE.to_vec())
    } else {
        Err(error_response(
            "ERR DB index is out of range or unsupported by Cloudish",
        ))
    }
}

fn handle_client(command: &[Vec<u8>]) -> Result<Vec<u8>, Vec<u8>> {
    if command
        .get(1)
        .is_some_and(|argument| argument.eq_ignore_ascii_case(b"setinfo"))
        && command.len() >= 3
    {
        return Ok(OK_RESPONSE.to_vec());
    }
    if matches!(
        command,
        [_, argument] if argument.eq_ignore_ascii_case(b"id")
    ) {
        return Ok(integer_response(1));
    }

    Err(error_response("ERR CLIENT subcommand is not supported by Cloudish"))
}

fn bridge(client: TcpStream, backend: TcpStream) {
    let client_to_backend = thread::spawn({
        let client = client.try_clone();
        let backend = backend.try_clone();
        move || match (client, backend) {
            (Ok(client), Ok(backend)) => relay(client, backend),
            _ => Ok(()),
        }
    });
    let backend_to_client = thread::spawn(move || relay(backend, client));

    let _ = join_copy_thread(client_to_backend);
    let _ = join_copy_thread(backend_to_client);
}

fn relay(mut from: TcpStream, mut to: TcpStream) -> io::Result<()> {
    io::copy(&mut from, &mut to)?;
    let _ = to.shutdown(Shutdown::Write);
    Ok(())
}

fn join_copy_thread(join: JoinHandle<io::Result<()>>) -> io::Result<()> {
    join.join().map_err(|_| io::Error::other("copy worker panicked"))?
}

fn endpoint_address(endpoint: &Endpoint) -> String {
    format!("{}:{}", endpoint.host(), endpoint.port())
}

fn bytes_to_string(value: &[u8]) -> io::Result<String> {
    String::from_utf8(value.to_vec()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8")
    })
}

fn command_name(command: &[Vec<u8>]) -> Option<String> {
    command.first().and_then(|value| bytes_to_string(value).ok())
}

fn error_response(message: &str) -> Vec<u8> {
    let mut response = Vec::with_capacity(message.len().saturating_add(3));
    response.push(b'-');
    response.extend_from_slice(message.as_bytes());
    response.extend_from_slice(b"\r\n");
    response
}

fn bulk_string_response(value: &[u8]) -> Vec<u8> {
    let mut response = format!("${}\r\n", value.len()).into_bytes();
    response.extend_from_slice(value);
    response.extend_from_slice(b"\r\n");
    response
}

fn null_bulk_string_response() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

fn integer_response(value: i64) -> Vec<u8> {
    format!(":{value}\r\n").into_bytes()
}

fn empty_array_response() -> Vec<u8> {
    b"*0\r\n".to_vec()
}

fn serialize_command(command: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", command.len()).into_bytes();
    for part in command {
        bytes.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        bytes.extend_from_slice(part);
        bytes.extend_from_slice(b"\r\n");
    }
    bytes
}

struct RespCommandReader<'a, R> {
    reader: &'a mut R,
}

impl<'a, R> RespCommandReader<'a, R>
where
    R: Read,
{
    fn new(reader: &'a mut R) -> Self {
        Self { reader }
    }

    fn read(&mut self) -> io::Result<Option<Vec<Vec<u8>>>> {
        let Some(first_line) = self.read_line()? else {
            return Ok(None);
        };
        if !first_line.starts_with(b"*") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected RESP array",
            ));
        }
        let argument_count =
            parse_decimal(first_line.get(1..).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected RESP array",
                )
            })?)?;
        let mut command = Vec::with_capacity(argument_count);
        for _ in 0..argument_count {
            let Some(header) = self.read_line()? else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF in RESP bulk string header",
                ));
            };
            if !header.starts_with(b"$") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected RESP bulk string",
                ));
            }
            let length = parse_decimal(header.get(1..).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected RESP bulk string",
                )
            })?)?;
            let mut data = vec![0_u8; length];
            self.reader.read_exact(&mut data)?;
            let mut trailer = [0_u8; 2];
            self.reader.read_exact(&mut trailer)?;
            if trailer != *b"\r\n" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP bulk string missing CRLF terminator",
                ));
            }
            command.push(data);
        }

        Ok(Some(command))
    }

    fn read_line(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut line = Vec::new();
        let mut byte = [0_u8; 1];
        loop {
            match self.reader.read(&mut byte) {
                Ok(0) if line.is_empty() => return Ok(None),
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected EOF reading RESP line",
                    ));
                }
                Ok(_) if byte[0] == b'\r' => {
                    self.reader.read_exact(&mut byte)?;
                    if byte[0] != b'\n' {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "expected LF after CR",
                        ));
                    }
                    return Ok(Some(line));
                }
                Ok(_) => line.push(byte[0]),
                Err(error) => return Err(error),
            }
        }
    }
}

fn parse_decimal(bytes: &[u8]) -> io::Result<usize> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "invalid integer")
    })?;
    text.parse::<usize>().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "invalid integer")
    })
}

fn command_part(command: &[Vec<u8>], index: usize) -> io::Result<&[u8]> {
    command
        .get(index)
        .map(Vec::as_slice)
        .ok_or_else(|| io::Error::other("command argument missing"))
}

fn command_part_response(
    command: &[Vec<u8>],
    index: usize,
) -> Result<&Vec<u8>, Vec<u8>> {
    command.get(index).ok_or_else(|| {
        error_response("ERR wrong number of arguments for command")
    })
}

fn recover<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{
        INVALID_AUTH_RESPONSE, NOAUTH_RESPONSE, RESP3_HELLO_RESPONSE,
        RespCommandReader, ThreadElastiCacheNodeRuntime,
        ThreadElastiCacheProxyRuntime, WRONG_HELLO_ARGS_RESPONSE,
        bulk_string_response, bytes_to_string, command_name, connect_backend,
        empty_array_response, error_response, handle_backend_connection,
        handle_client, handle_del, handle_echo, handle_get, handle_ping,
        handle_proxy_connection, handle_select, handle_set, integer_response,
        join_copy_thread, null_bulk_string_response, parse_decimal,
        serialize_command, validate_auth_command, validate_credentials,
        validate_hello_command,
    };
    use aws::Endpoint;
    use elasticache::{
        ElastiCacheAuthenticationType, ElastiCacheConnectionAuthenticator,
        ElastiCacheEngine, ElastiCacheNodeRuntime, ElastiCacheNodeSpec,
        ElastiCacheProxyRuntime, ElastiCacheProxySpec,
        ElastiCacheReplicationGroupId, ElastiCacheScope,
    };
    use std::io::Cursor;
    use std::io::{self, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct TestAuthenticator;

    impl ElastiCacheConnectionAuthenticator for TestAuthenticator {
        fn validate_iam_token(
            &self,
            _scope: &ElastiCacheScope,
            _replication_group_id: &ElastiCacheReplicationGroupId,
            token: &str,
        ) -> Result<(), String> {
            if token == "iam-token" {
                Ok(())
            } else {
                Err("bad token".to_owned())
            }
        }

        fn validate_password(
            &self,
            _scope: &ElastiCacheScope,
            _replication_group_id: &ElastiCacheReplicationGroupId,
            username: Option<&str>,
            password: &str,
        ) -> Result<(), String> {
            match (username, password) {
                (None, "group-secret") | (Some("alice"), "user-secret") => {
                    Ok(())
                }
                _ => Err("bad password".to_owned()),
            }
        }
    }

    fn scope() -> ElastiCacheScope {
        ElastiCacheScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn proxy_spec(
        auth_mode: ElastiCacheAuthenticationType,
        upstream: Endpoint,
    ) -> ElastiCacheProxySpec {
        ElastiCacheProxySpec::new(
            scope(),
            ElastiCacheReplicationGroupId::new("cache-a")
                .expect("test replication group id should parse"),
            auth_mode,
            Endpoint::localhost(0),
            upstream,
            Arc::new(TestAuthenticator),
        )
    }

    fn socket_pair() -> (TcpStream, TcpStream) {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener address");
        let client =
            TcpStream::connect(address).expect("client stream should connect");
        let (server, _) =
            listener.accept().expect("server stream should accept");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("client timeout should set");
        server
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("server timeout should set");
        (client, server)
    }

    fn read_once(stream: &mut TcpStream) -> Vec<u8> {
        let mut buffer = [0_u8; 512];
        let read = stream.read(&mut buffer).expect("socket should read");
        buffer[..read].to_vec()
    }

    fn send_command(stream: &mut TcpStream, command: &[Vec<u8>]) -> Vec<u8> {
        stream
            .write_all(&serialize_command(command))
            .expect("command should write");
        stream.flush().expect("command should flush");
        read_once(stream)
    }

    fn spawn_backend_once(
        response: &'static [u8],
    ) -> (Endpoint, thread::JoinHandle<Vec<Vec<u8>>>) {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("backend should bind");
        let address = listener.local_addr().expect("backend address");
        let endpoint = Endpoint::new(address.ip().to_string(), address.port());
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("backend accept");
            let command = RespCommandReader::new(&mut stream)
                .read()
                .expect("backend command should parse")
                .expect("backend command should exist");
            stream.write_all(response).expect("backend response should write");
            stream.flush().expect("backend response should flush");
            command
        });
        (endpoint, join)
    }

    fn hello_exchange(
        auth_mode: ElastiCacheAuthenticationType,
        command: Vec<Vec<u8>>,
    ) -> (bool, Vec<u8>) {
        let (mut client, mut server) = socket_pair();
        client
            .write_all(&serialize_command(&command))
            .expect("HELLO command should write");
        client.flush().expect("HELLO command should flush");
        let result = validate_hello_command(
            &proxy_spec(auth_mode, Endpoint::localhost(9)),
            &command,
            &mut server,
        )
        .expect("HELLO validation should run");
        let response = read_once(&mut client);
        (result, response)
    }

    #[test]
    fn elasticache_resp_command_reader_parses_bulk_arrays() {
        let mut bytes =
            Cursor::new(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n".to_vec());
        let command = RespCommandReader::new(&mut bytes)
            .read()
            .expect("RESP command should parse")
            .expect("RESP command should exist");

        assert_eq!(command, vec![b"AUTH".to_vec(), b"secret".to_vec()]);
        assert_eq!(
            serialize_command(&command),
            b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n"
        );
    }

    #[test]
    fn elasticache_resp_response_helpers_cover_expected_shapes() {
        assert_eq!(bulk_string_response(b"ok"), b"$2\r\nok\r\n");
        assert_eq!(null_bulk_string_response(), b"$-1\r\n");
        assert_eq!(integer_response(2), b":2\r\n");
        assert_eq!(empty_array_response(), b"*0\r\n");
        assert_eq!(error_response("ERR bad"), b"-ERR bad\r\n");
        assert!(RESP3_HELLO_RESPONSE.starts_with(b"%7\r\n"));
    }

    #[test]
    fn elasticache_resp_backend_ping_and_echo_commands() {
        assert_eq!(
            handle_ping(&[b"PING".to_vec()]).expect("PING"),
            b"+PONG\r\n"
        );
        assert_eq!(
            handle_ping(&[b"PING".to_vec(), b"hi".to_vec()])
                .expect("PING with message"),
            b"$2\r\nhi\r\n"
        );
        assert_eq!(
            handle_ping(&[b"PING".to_vec(), b"a".to_vec(), b"b".to_vec()])
                .expect_err("extra PING args should fail"),
            b"-ERR wrong number of arguments for 'ping' command\r\n"
        );

        assert_eq!(
            handle_echo(&[b"ECHO".to_vec(), b"hello".to_vec()])
                .expect("ECHO should succeed"),
            b"$5\r\nhello\r\n"
        );
        assert_eq!(
            handle_echo(&[b"ECHO".to_vec()]).expect_err("missing ECHO args"),
            b"-ERR wrong number of arguments for 'echo' command\r\n"
        );
    }

    #[test]
    fn elasticache_resp_backend_key_value_commands() {
        let state = Arc::new(Mutex::new(std::collections::BTreeMap::new()));

        assert_eq!(
            handle_set(
                &[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()],
                &state
            )
            .expect("SET should succeed"),
            b"+OK\r\n"
        );
        assert_eq!(
            handle_set(&[b"SET".to_vec(), b"k".to_vec()], &state)
                .expect_err("short SET should fail"),
            b"-ERR wrong number of arguments for 'set' command\r\n"
        );
        assert_eq!(
            handle_get(&[b"GET".to_vec(), b"k".to_vec()], &state)
                .expect("GET should succeed"),
            b"$1\r\nv\r\n"
        );
        assert_eq!(
            handle_get(&[b"GET".to_vec(), b"missing".to_vec()], &state)
                .expect("GET should return null"),
            b"$-1\r\n"
        );
        assert_eq!(
            handle_get(&[b"GET".to_vec()], &state)
                .expect_err("short GET should fail"),
            b"-ERR wrong number of arguments for 'get' command\r\n"
        );

        assert_eq!(
            handle_del(
                &[b"DEL".to_vec(), b"k".to_vec(), b"missing".to_vec()],
                &state
            )
            .expect("DEL should succeed"),
            b":1\r\n"
        );
        assert_eq!(
            handle_del(&[b"DEL".to_vec()], &state)
                .expect_err("short DEL should fail"),
            b"-ERR wrong number of arguments for 'del' command\r\n"
        );
    }

    #[test]
    fn elasticache_resp_backend_select_and_client_commands() {
        assert_eq!(
            handle_select(&[b"SELECT".to_vec(), b"0".to_vec()])
                .expect("SELECT 0 should succeed"),
            b"+OK\r\n"
        );
        assert_eq!(
            handle_select(&[b"SELECT".to_vec(), b"1".to_vec()])
                .expect_err("unsupported DBs should fail"),
            b"-ERR DB index is out of range or unsupported by Cloudish\r\n"
        );
        assert_eq!(
            handle_select(&[b"SELECT".to_vec()])
                .expect_err("short SELECT should fail"),
            b"-ERR wrong number of arguments for 'select' command\r\n"
        );

        assert_eq!(
            handle_client(&[
                b"CLIENT".to_vec(),
                b"SETINFO".to_vec(),
                b"LIB-NAME".to_vec(),
                b"redis".to_vec()
            ])
            .expect("CLIENT SETINFO should succeed"),
            b"+OK\r\n"
        );
        assert_eq!(
            handle_client(&[b"CLIENT".to_vec(), b"ID".to_vec()])
                .expect("CLIENT ID should succeed"),
            b":1\r\n"
        );
        assert_eq!(
            handle_client(&[b"CLIENT".to_vec(), b"LIST".to_vec()])
                .expect_err("unsupported CLIENT commands should fail"),
            b"-ERR CLIENT subcommand is not supported by Cloudish\r\n"
        );
    }

    #[test]
    fn elasticache_resp_parser_errors_and_helpers_are_explicit() {
        assert_eq!(bytes_to_string(b"hello").expect("UTF-8"), "hello");
        assert!(bytes_to_string(&[0xff]).is_err());
        assert_eq!(
            command_name(&[b"PING".to_vec()]).expect("command should decode"),
            "PING"
        );
        assert!(command_name(&[vec![0xff]]).is_none());
        assert_eq!(parse_decimal(b"12").expect("integer should parse"), 12);
        assert!(parse_decimal(b"abc").is_err());
        assert!(
            connect_backend(&proxy_spec(
                ElastiCacheAuthenticationType::Password,
                Endpoint::localhost(9)
            ))
            .is_err()
        );

        let panic_error =
            join_copy_thread(thread::spawn(|| -> io::Result<()> {
                panic!("copy worker should panic");
            }))
            .expect_err("panic joins should surface");
        assert_eq!(panic_error.kind(), io::ErrorKind::Other);

        let mut invalid_array = Cursor::new(b"+PING\r\n".to_vec());
        assert!(RespCommandReader::new(&mut invalid_array).read().is_err());
        let mut missing_header = Cursor::new(b"*1\r\n".to_vec());
        assert!(RespCommandReader::new(&mut missing_header).read().is_err());
        let mut invalid_bulk = Cursor::new(b"*1\r\n+PING\r\n".to_vec());
        assert!(RespCommandReader::new(&mut invalid_bulk).read().is_err());
        let mut bad_trailer = Cursor::new(b"*1\r\n$4\r\nPINGxx".to_vec());
        assert!(RespCommandReader::new(&mut bad_trailer).read().is_err());
        let mut short_line = Cursor::new(b"*1\r".to_vec());
        assert!(RespCommandReader::new(&mut short_line).read_line().is_err());
    }

    #[test]
    fn elasticache_backend_connection_routes_commands_and_explicit_errors() {
        let state = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        let (mut client, server) = socket_pair();
        let join = thread::spawn({
            let state = Arc::clone(&state);
            move || handle_backend_connection(server, state)
        });

        assert_eq!(
            send_command(&mut client, &[b"PING".to_vec()]),
            b"+PONG\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"ECHO".to_vec(), b"hi".to_vec()]),
            b"$2\r\nhi\r\n"
        );
        assert_eq!(
            send_command(
                &mut client,
                &[b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"GET".to_vec(), b"key".to_vec()]),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            send_command(
                &mut client,
                &[b"DEL".to_vec(), b"key".to_vec(), b"missing".to_vec()]
            ),
            b":1\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"SELECT".to_vec(), b"0".to_vec()]),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(
                &mut client,
                &[
                    b"CLIENT".to_vec(),
                    b"SETINFO".to_vec(),
                    b"LIB-NAME".to_vec(),
                    b"redis".to_vec()
                ]
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"CLIENT".to_vec(), b"ID".to_vec()]),
            b":1\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"COMMAND".to_vec()]),
            b"*0\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"ACL".to_vec(), b"LIST".to_vec()]),
            b"-ERR ACL command support is not implemented by Cloudish\r\n"
        );
        assert_eq!(
            send_command(
                &mut client,
                &[b"CLUSTER".to_vec(), b"INFO".to_vec()]
            ),
            b"-ERR cluster mode is not supported by Cloudish\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"WHATEVER".to_vec()]),
            b"-ERR unknown command `whatever`\r\n"
        );
        assert_eq!(send_command(&mut client, &[b"QUIT".to_vec()]), b"+OK\r\n");
        drop(client);
        join.join().expect("backend worker should finish");

        let (mut malformed_client, malformed_server) = socket_pair();
        let malformed_join = thread::spawn(move || {
            handle_backend_connection(
                malformed_server,
                Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            );
        });
        malformed_client
            .write_all(&serialize_command(&[vec![0xff]]))
            .expect("malformed command should write");
        malformed_client.flush().expect("malformed command should flush");
        assert_eq!(
            read_once(&mut malformed_client),
            b"-ERR malformed RESP command\r\n"
        );
        drop(malformed_client);
        malformed_join.join().expect("malformed backend worker should finish");

        let (mut error_client, error_server) = socket_pair();
        let error_join = thread::spawn(move || {
            handle_backend_connection(
                error_server,
                Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            );
        });
        error_client
            .write_all(&serialize_command(&[b"ECHO".to_vec()]))
            .expect("error command should write");
        error_client.flush().expect("error command should flush");
        assert_eq!(
            read_once(&mut error_client),
            b"-ERR wrong number of arguments for 'echo' command\r\n"
        );
        drop(error_client);
        error_join.join().expect("error backend worker should finish");
    }

    #[test]
    fn elasticache_proxy_helpers_cover_auth_hello_and_passthrough_paths() {
        let password_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            Endpoint::localhost(9),
        );
        assert!(
            validate_auth_command(
                &password_spec,
                &[b"AUTH".to_vec(), b"group-secret".to_vec()]
            )
            .expect("group password auth should validate")
        );
        assert!(
            validate_auth_command(
                &password_spec,
                &[
                    b"AUTH".to_vec(),
                    b"alice".to_vec(),
                    b"user-secret".to_vec()
                ]
            )
            .expect("username/password auth should validate")
        );
        assert!(
            validate_auth_command(&password_spec, &[b"AUTH".to_vec()])
                .is_err()
        );
        assert!(
            validate_credentials(
                &proxy_spec(
                    ElastiCacheAuthenticationType::NoPassword,
                    Endpoint::localhost(9)
                ),
                None,
                "ignored"
            )
            .expect("no-password auth should always pass")
        );
        assert!(
            validate_credentials(
                &proxy_spec(
                    ElastiCacheAuthenticationType::Iam,
                    Endpoint::localhost(9)
                ),
                None,
                "iam-token"
            )
            .expect("IAM tokens should validate")
        );
        assert!(
            !validate_credentials(
                &proxy_spec(
                    ElastiCacheAuthenticationType::Iam,
                    Endpoint::localhost(9)
                ),
                None,
                "bad"
            )
            .expect("IAM validation should run")
        );

        let (missing_args_ok, missing_args_response) = hello_exchange(
            ElastiCacheAuthenticationType::Password,
            vec![b"HELLO".to_vec()],
        );
        assert!(!missing_args_ok);
        assert_eq!(missing_args_response, WRONG_HELLO_ARGS_RESPONSE);

        let (wrong_version_ok, wrong_version_response) = hello_exchange(
            ElastiCacheAuthenticationType::Password,
            vec![b"HELLO".to_vec(), b"2".to_vec()],
        );
        assert!(!wrong_version_ok);
        assert_eq!(
            wrong_version_response,
            b"-ERR only HELLO 3 is supported by Cloudish.\r\n"
        );

        let (missing_auth_parts_ok, missing_auth_parts_response) =
            hello_exchange(
                ElastiCacheAuthenticationType::Password,
                vec![b"HELLO".to_vec(), b"3".to_vec(), b"AUTH".to_vec()],
            );
        assert!(!missing_auth_parts_ok);
        assert_eq!(missing_auth_parts_response, WRONG_HELLO_ARGS_RESPONSE);

        let (missing_setname_ok, missing_setname_response) = hello_exchange(
            ElastiCacheAuthenticationType::Password,
            vec![b"HELLO".to_vec(), b"3".to_vec(), b"SETNAME".to_vec()],
        );
        assert!(!missing_setname_ok);
        assert_eq!(missing_setname_response, WRONG_HELLO_ARGS_RESPONSE);

        let (missing_password_ok, missing_password_response) = hello_exchange(
            ElastiCacheAuthenticationType::Password,
            vec![b"HELLO".to_vec(), b"3".to_vec()],
        );
        assert!(!missing_password_ok);
        assert_eq!(missing_password_response, NOAUTH_RESPONSE);

        let (invalid_password_ok, invalid_password_response) = hello_exchange(
            ElastiCacheAuthenticationType::Password,
            vec![
                b"HELLO".to_vec(),
                b"3".to_vec(),
                b"AUTH".to_vec(),
                b"alice".to_vec(),
                b"wrong".to_vec(),
            ],
        );
        assert!(!invalid_password_ok);
        assert_eq!(invalid_password_response, INVALID_AUTH_RESPONSE);

        let (hello_ok, hello_response) = hello_exchange(
            ElastiCacheAuthenticationType::NoPassword,
            vec![
                b"HELLO".to_vec(),
                b"3".to_vec(),
                b"SETNAME".to_vec(),
                b"demo".to_vec(),
                b"LIB-NAME".to_vec(),
            ],
        );
        assert!(hello_ok);
        assert_eq!(hello_response, RESP3_HELLO_RESPONSE);
    }

    #[test]
    fn elasticache_proxy_connection_handles_auth_hello_and_noauth_paths() {
        let (empty_client, empty_server) = socket_pair();
        let empty_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            Endpoint::localhost(9),
        );
        let empty_join = thread::spawn(move || {
            handle_proxy_connection(empty_server, &empty_spec)
        });
        empty_client
            .shutdown(std::net::Shutdown::Both)
            .expect("client should shut down");
        empty_join
            .join()
            .expect("empty proxy worker should finish")
            .expect("empty proxy path should succeed");

        let (mut malformed_client, malformed_server) = socket_pair();
        let malformed_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            Endpoint::localhost(9),
        );
        let malformed_join = thread::spawn(move || {
            handle_proxy_connection(malformed_server, &malformed_spec)
        });
        malformed_client
            .write_all(&serialize_command(&[vec![0xff]]))
            .expect("malformed proxy command should write");
        malformed_client
            .flush()
            .expect("malformed proxy command should flush");
        assert_eq!(
            read_once(&mut malformed_client),
            b"-ERR malformed RESP command\r\n"
        );
        drop(malformed_client);
        malformed_join
            .join()
            .expect("malformed proxy worker should finish")
            .expect("malformed proxy path should succeed");

        let (auth_backend_endpoint, auth_backend_join) =
            spawn_backend_once(b"+PONG\r\n");
        let (mut auth_client, auth_server) = socket_pair();
        let auth_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            auth_backend_endpoint,
        );
        let auth_join = thread::spawn(move || {
            handle_proxy_connection(auth_server, &auth_spec)
        });
        assert_eq!(
            send_command(
                &mut auth_client,
                &[b"AUTH".to_vec(), b"group-secret".to_vec()]
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(&mut auth_client, &[b"PING".to_vec()]),
            b"+PONG\r\n"
        );
        drop(auth_client);
        assert_eq!(
            auth_backend_join.join().expect("auth backend should finish"),
            vec![b"PING".to_vec()]
        );
        auth_join
            .join()
            .expect("auth proxy worker should finish")
            .expect("auth proxy path should succeed");

        let (mut bad_auth_client, bad_auth_server) = socket_pair();
        let bad_auth_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            Endpoint::localhost(9),
        );
        let bad_auth_join = thread::spawn(move || {
            handle_proxy_connection(bad_auth_server, &bad_auth_spec)
        });
        assert_eq!(
            send_command(
                &mut bad_auth_client,
                &[b"AUTH".to_vec(), b"wrong".to_vec()]
            ),
            INVALID_AUTH_RESPONSE
        );
        drop(bad_auth_client);
        bad_auth_join
            .join()
            .expect("bad auth proxy worker should finish")
            .expect("bad auth proxy path should succeed");

        let (hello_backend_endpoint, hello_backend_join) =
            spawn_backend_once(b"+PONG\r\n");
        let (mut hello_client, hello_server) = socket_pair();
        let hello_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            hello_backend_endpoint,
        );
        let hello_join = thread::spawn(move || {
            handle_proxy_connection(hello_server, &hello_spec)
        });
        assert_eq!(
            send_command(
                &mut hello_client,
                &[
                    b"HELLO".to_vec(),
                    b"3".to_vec(),
                    b"AUTH".to_vec(),
                    b"alice".to_vec(),
                    b"user-secret".to_vec(),
                ]
            ),
            RESP3_HELLO_RESPONSE
        );
        assert_eq!(
            send_command(&mut hello_client, &[b"PING".to_vec()]),
            b"+PONG\r\n"
        );
        drop(hello_client);
        assert_eq!(
            hello_backend_join.join().expect("HELLO backend should finish"),
            vec![b"PING".to_vec()]
        );
        hello_join
            .join()
            .expect("HELLO proxy worker should finish")
            .expect("HELLO proxy path should succeed");

        let (nopass_backend_endpoint, nopass_backend_join) =
            spawn_backend_once(b"+PONG\r\n");
        let (mut nopass_client, nopass_server) = socket_pair();
        let nopass_spec = proxy_spec(
            ElastiCacheAuthenticationType::NoPassword,
            nopass_backend_endpoint,
        );
        let nopass_join = thread::spawn(move || {
            handle_proxy_connection(nopass_server, &nopass_spec)
        });
        assert_eq!(
            send_command(&mut nopass_client, &[b"PING".to_vec()]),
            b"+PONG\r\n"
        );
        drop(nopass_client);
        assert_eq!(
            nopass_backend_join
                .join()
                .expect("no-password backend should finish"),
            vec![b"PING".to_vec()]
        );
        nopass_join
            .join()
            .expect("no-password proxy worker should finish")
            .expect("no-password proxy path should succeed");

        let (mut noauth_client, noauth_server) = socket_pair();
        let noauth_spec = proxy_spec(
            ElastiCacheAuthenticationType::Password,
            Endpoint::localhost(9),
        );
        let noauth_join = thread::spawn(move || {
            handle_proxy_connection(noauth_server, &noauth_spec)
        });
        assert_eq!(
            send_command(&mut noauth_client, &[b"PING".to_vec()]),
            NOAUTH_RESPONSE
        );
        drop(noauth_client);
        noauth_join
            .join()
            .expect("noauth proxy worker should finish")
            .expect("noauth proxy path should succeed");
    }

    #[test]
    fn elasticache_thread_node_runtime_starts_serves_and_stops_cleanly() {
        let runtime = ThreadElastiCacheNodeRuntime::new();
        let handle = runtime
            .start(&ElastiCacheNodeSpec::new(
                ElastiCacheReplicationGroupId::new("cache-live")
                    .expect("test replication group id should parse"),
                ElastiCacheEngine::Redis,
                Endpoint::localhost(0),
            ))
            .expect("thread node runtime should start");
        let listen = handle.listen_endpoint();
        let mut client =
            TcpStream::connect(format!("{}:{}", listen.host(), listen.port()))
                .expect("node runtime should accept connections");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("node runtime timeout should set");

        assert_eq!(
            send_command(
                &mut client,
                &[b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"GET".to_vec(), b"key".to_vec()]),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(send_command(&mut client, &[b"QUIT".to_vec()]), b"+OK\r\n");
        drop(client);

        handle.stop().expect("node runtime should stop");
    }

    #[test]
    fn elasticache_thread_proxy_runtime_authenticates_and_relays_commands() {
        let backend = ThreadElastiCacheNodeRuntime::new()
            .start(&ElastiCacheNodeSpec::new(
                ElastiCacheReplicationGroupId::new("cache-live")
                    .expect("test replication group id should parse"),
                ElastiCacheEngine::Redis,
                Endpoint::localhost(0),
            ))
            .expect("backend runtime should start");
        let proxy = ThreadElastiCacheProxyRuntime::new()
            .start(&proxy_spec(
                ElastiCacheAuthenticationType::Password,
                backend.listen_endpoint(),
            ))
            .expect("proxy runtime should start");
        let listen = proxy.listen_endpoint();
        let mut client =
            TcpStream::connect(format!("{}:{}", listen.host(), listen.port()))
                .expect("proxy runtime should accept connections");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("proxy runtime timeout should set");

        assert_eq!(
            send_command(
                &mut client,
                &[b"AUTH".to_vec(), b"group-secret".to_vec()]
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(
                &mut client,
                &[b"SET".to_vec(), b"key".to_vec(), b"value".to_vec()]
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            send_command(&mut client, &[b"GET".to_vec(), b"key".to_vec()]),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(send_command(&mut client, &[b"QUIT".to_vec()]), b"+OK\r\n");
        drop(client);

        proxy.stop().expect("proxy runtime should stop");
        backend.stop().expect("backend runtime should stop");
    }
}
