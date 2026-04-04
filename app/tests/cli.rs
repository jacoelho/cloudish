#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use app::{EDGE_HOST_ENV, EDGE_PORT_ENV, EDGE_READY_FILE_ENV};
use aws::{DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, STATE_DIRECTORY_ENV};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use test_support::{send_http_request, temporary_directory};

struct RunningApp {
    address: SocketAddr,
    child: std::process::Child,
    root: PathBuf,
    state_directory: PathBuf,
}

impl RunningApp {
    fn start(label: &str, edge_host: Option<&str>) -> Self {
        let root = temporary_directory(label);
        let state_directory = root.join("state");
        let ready_file = root.join("ready").join("edge-address");
        let mut command = Command::new(env!("CARGO_BIN_EXE_app"));
        command
            .env(DEFAULT_ACCOUNT_ENV, "000000000000")
            .env(DEFAULT_REGION_ENV, "eu-west-2")
            .env(STATE_DIRECTORY_ENV, state_directory.display().to_string())
            .env(EDGE_PORT_ENV, "0")
            .env(EDGE_READY_FILE_ENV, ready_file.display().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(edge_host) = edge_host {
            command.env(EDGE_HOST_ENV, edge_host);
        }
        let mut child = require_ok(command.spawn(), "app binary should start");
        let address = wait_for_ready_address(&mut child, &ready_file);

        Self { address, child, root, state_directory }
    }

    fn stop(&mut self) -> std::process::ExitStatus {
        signal_interrupt(self.child.id());
        require_ok(self.child.wait(), "app binary should exit cleanly")
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn app_binary_reports_missing_required_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_app"))
        .env_remove(DEFAULT_ACCOUNT_ENV)
        .env_remove(DEFAULT_REGION_ENV)
        .env_remove(STATE_DIRECTORY_ENV)
        .output();

    let output = require_ok(output, "app binary should run");
    let stderr = require_ok(
        String::from_utf8(output.stderr),
        "stderr should be valid UTF-8",
    );

    assert!(!output.status.success());
    assert!(stderr.contains(
        "startup configuration error: missing required config values: \
CLOUDISH_DEFAULT_ACCOUNT, CLOUDISH_DEFAULT_REGION, CLOUDISH_STATE_DIR"
    ));
}

#[test]
fn app_binary_serves_the_health_endpoint() {
    let mut app = RunningApp::start("app-binary", None);
    let response = wait_for_health_response(app.address);
    let status_response = wait_for_status_response(app.address);
    let state_directory = app.state_directory.clone();
    let status = app.stop();

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"status\":\"ok\""));
    assert!(!response.contains("\"defaultRegion\""));
    assert!(status_response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(status_response.contains("\"defaultAccount\":\"000000000000\""));
    assert!(status_response.contains("\"defaultRegion\":\"eu-west-2\""));
    assert!(status_response.contains("\"stateDirectory\":\""));
    assert!(status.success());
    assert!(state_directory.exists());
    app.cleanup();
}

#[test]
fn app_binary_serves_the_health_endpoint_on_a_custom_edge_address() {
    let mut app = RunningApp::start("app-binary-custom-edge", Some("0.0.0.0"));
    let response = wait_for_health_response(app.address);
    let status_response = wait_for_status_response(app.address);
    let state_directory = app.state_directory.clone();
    let status = app.stop();

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"status\":\"ok\""));
    assert!(!response.contains("\"defaultRegion\""));
    assert!(status_response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(status_response.contains("\"defaultAccount\":\"000000000000\""));
    assert!(status_response.contains("\"defaultRegion\":\"eu-west-2\""));
    assert!(status_response.contains("\"stateDirectory\":\""));
    assert!(status.success());
    assert!(state_directory.exists());
    app.cleanup();
}

#[test]
fn app_binary_serves_other_clients_while_one_connection_is_stalled() {
    let mut app = RunningApp::start("app-binary-stalled-client", None);
    let ready_response = wait_for_health_response(app.address);
    assert!(ready_response.starts_with("HTTP/1.1 200 OK\r\n"));

    let mut stalled_client = require_ok(
        TcpStream::connect(app.address),
        "stalled client should connect",
    );
    stalled_client
        .write_all(
            b"PUT /sdk-stall/object.txt HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nabc",
        )
        .unwrap_or_else(|error| fail_test(format!(
            "stalled request should be written: {error}"
        )));

    let concurrent_response = wait_for_health_response(app.address);

    stalled_client.shutdown(Shutdown::Both).unwrap_or_else(|error| {
        fail_test(format!("stalled client should close: {error}"))
    });
    let status = app.stop();

    assert!(concurrent_response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(status.success());
    app.cleanup();
}

fn wait_for_health_response(address: SocketAddr) -> String {
    wait_for_internal_response(
        address,
        "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
}

fn wait_for_status_response(address: SocketAddr) -> String {
    wait_for_internal_response(
        address,
        "GET /__cloudish/status HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
}

fn wait_for_internal_response(address: SocketAddr, request: &str) -> String {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .unwrap_or_else(Instant::now);
    let mut last_error = None;

    while Instant::now() < deadline {
        match send_http_request(address, request) {
            Ok(response) => return response,
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    fail_test(format!(
        "app binary did not expose the internal endpoint in time: {last_error:?}"
    ));
}

fn wait_for_ready_address(
    child: &mut std::process::Child,
    path: &Path,
) -> SocketAddr {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(15))
        .unwrap_or_else(Instant::now);
    let mut last_error = None;

    while Instant::now() < deadline {
        if let Some(status) = require_ok(
            child.try_wait(),
            "app binary status should be available",
        ) {
            fail_test(format!(
                "app binary exited before reporting a ready address: {status}"
            ));
        }
        match std::fs::read_to_string(path) {
            Ok(value) => return connectable_address(value.trim()),
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(25));
            }
        }
    }

    fail_test(format!(
        "app binary did not report a ready address in time: {last_error:?}"
    ));
}

fn connectable_address(value: &str) -> SocketAddr {
    let address =
        require_ok(value.parse::<SocketAddr>(), "ready address should parse");
    match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), address.port())
        }
        _ => address,
    }
}

fn signal_interrupt(process_id: u32) {
    let status = Command::new("kill")
        .arg("-INT")
        .arg(process_id.to_string())
        .status()
        .unwrap_or_else(|error| {
            fail_test(format!("app binary should receive SIGINT: {error}"))
        });
    assert!(status.success());
}

fn require_ok<T, E>(result: Result<T, E>, context: &str) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| fail_test(format!("{context}: {error}")))
}

fn fail_test(message: String) -> ! {
    assert!(false, "{message}");
    std::process::abort();
}
