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
use app::{EDGE_HOST_ENV, EDGE_PORT_ENV};
use aws::{DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, STATE_DIRECTORY_ENV};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use test_support::{send_http_request, temporary_directory};

#[test]
fn app_binary_reports_missing_required_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_app"))
        .env_remove(DEFAULT_ACCOUNT_ENV)
        .env_remove(DEFAULT_REGION_ENV)
        .env_remove(STATE_DIRECTORY_ENV)
        .output();

    let output = output.expect("app binary should run");
    let stderr = String::from_utf8(output.stderr)
        .expect("stderr should be valid UTF-8");

    assert!(!output.status.success());
    assert!(stderr.contains(
        "startup configuration error: missing required config values: \
CLOUDISH_DEFAULT_ACCOUNT, CLOUDISH_DEFAULT_REGION, CLOUDISH_STATE_DIR"
    ));
}

#[test]
fn app_binary_serves_the_health_endpoint() {
    let state_directory = temporary_directory("app-binary").join("state");
    let state_directory = state_directory.to_string_lossy().into_owned();
    let mut child = Command::new(env!("CARGO_BIN_EXE_app"))
        .env(DEFAULT_ACCOUNT_ENV, "000000000000")
        .env(DEFAULT_REGION_ENV, "eu-west-2")
        .env(STATE_DIRECTORY_ENV, &state_directory)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("app binary should start");

    let response = wait_for_health_response(
        "127.0.0.1:4566".parse().expect("health address should parse"),
    );

    let child_signal =
        Command::new("kill").arg("-INT").arg(child.id().to_string()).status();
    child_signal.expect("app binary should receive SIGINT");
    let status = child.wait().expect("app binary should exit cleanly");

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"status\":\"ok\""));
    assert!(response.contains("\"defaultRegion\":\"eu-west-2\""));
    assert!(status.success());
    assert!(std::path::Path::new(&state_directory).exists());

    let root = std::path::Path::new(&state_directory)
        .parent()
        .expect("state directory should have a test root");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn app_binary_serves_the_health_endpoint_on_a_custom_edge_address() {
    let state_directory =
        temporary_directory("app-binary-custom-edge").join("state");
    let state_directory = state_directory.to_string_lossy().into_owned();
    let mut child = Command::new(env!("CARGO_BIN_EXE_app"))
        .env(DEFAULT_ACCOUNT_ENV, "000000000000")
        .env(DEFAULT_REGION_ENV, "eu-west-2")
        .env(STATE_DIRECTORY_ENV, &state_directory)
        .env(EDGE_HOST_ENV, "0.0.0.0")
        .env(EDGE_PORT_ENV, "4570")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("app binary should start");

    let response = wait_for_health_response(
        "127.0.0.1:4570".parse().expect("health address should parse"),
    );

    let child_signal =
        Command::new("kill").arg("-INT").arg(child.id().to_string()).status();
    child_signal.expect("app binary should receive SIGINT");
    let status = child.wait().expect("app binary should exit cleanly");

    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.contains("\"status\":\"ok\""));
    assert!(response.contains("\"defaultRegion\":\"eu-west-2\""));
    assert!(status.success());
    assert!(std::path::Path::new(&state_directory).exists());

    let root = std::path::Path::new(&state_directory)
        .parent()
        .expect("state directory should have a test root");
    let _ = std::fs::remove_dir_all(root);
}

fn wait_for_health_response(address: SocketAddr) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_error = None;

    while Instant::now() < deadline {
        match send_http_request(
            address,
            "GET /__cloudish/health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        ) {
            Ok(response) => return response,
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }

    panic!(
        "app binary did not expose the health endpoint in time: {last_error:?}"
    );
}
