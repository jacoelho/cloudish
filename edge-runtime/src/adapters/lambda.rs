use aws::{
    InfrastructureError, LambdaExecutionPackage, LambdaExecutor,
    LambdaInvocationRequest, LambdaInvocationResult,
};
use serde_json::json;
use std::fs::{self, File};
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use zip::ZipArchive;

#[derive(Debug, Clone)]
pub struct ProcessLambdaExecutor {
    root: Arc<PathBuf>,
    next_dir: Arc<AtomicUsize>,
}

impl Default for ProcessLambdaExecutor {
    fn default() -> Self {
        Self::new(std::env::temp_dir().join("cloudish-lambda"))
    }
}

impl ProcessLambdaExecutor {
    const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(25);

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            next_dir: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn invoke_inner(
        &self,
        request: &LambdaInvocationRequest,
    ) -> Result<LambdaInvocationResult, InfrastructureError> {
        let archive = match request.package() {
            LambdaExecutionPackage::ZipArchive(archive) => archive,
            LambdaExecutionPackage::ImageUri(_) => {
                return Err(io_error(
                    "invoke",
                    request.function_name(),
                    "image package invocations are not supported",
                ));
            }
        };

        validate_zip_archive(request.runtime(), request.handler(), archive)
            .map_err(|source| {
                InfrastructureError::container(
                    "validate",
                    request.function_name(),
                    source,
                )
            })?;

        let workdir = self.allocate_workdir(request.function_name());
        fs::create_dir_all(&workdir).map_err(|source| {
            InfrastructureError::container(
                "extract",
                request.function_name(),
                source,
            )
        })?;
        let result =
            (|| -> Result<LambdaInvocationResult, InfrastructureError> {
                extract_archive(archive, &workdir).map_err(|source| {
                    InfrastructureError::container(
                        "extract",
                        request.function_name(),
                        source,
                    )
                })?;
                run_request(request, &workdir)
            })();
        let _ = fs::remove_dir_all(&workdir);

        result
    }

    fn allocate_workdir(&self, function_name: &str) -> PathBuf {
        let id = self.next_dir.fetch_add(1, Ordering::Relaxed);
        self.root.join(format!("{function_name}-{id}"))
    }
}

impl LambdaExecutor for ProcessLambdaExecutor {
    fn invoke(
        &self,
        request: &LambdaInvocationRequest,
    ) -> Result<LambdaInvocationResult, InfrastructureError> {
        self.invoke_inner(request)
    }

    fn invoke_with_cancellation(
        &self,
        request: &LambdaInvocationRequest,
        is_cancelled: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<LambdaInvocationResult, InfrastructureError> {
        let archive = match request.package() {
            LambdaExecutionPackage::ZipArchive(archive) => archive,
            LambdaExecutionPackage::ImageUri(_) => {
                return Err(io_error(
                    "invoke",
                    request.function_name(),
                    "image package invocations are not supported",
                ));
            }
        };

        validate_zip_archive(request.runtime(), request.handler(), archive)
            .map_err(|source| {
                InfrastructureError::container(
                    "validate",
                    request.function_name(),
                    source,
                )
            })?;

        let workdir = self.allocate_workdir(request.function_name());
        fs::create_dir_all(&workdir).map_err(|source| {
            InfrastructureError::container(
                "extract",
                request.function_name(),
                source,
            )
        })?;
        let result =
            (|| -> Result<LambdaInvocationResult, InfrastructureError> {
                extract_archive(archive, &workdir).map_err(|source| {
                    InfrastructureError::container(
                        "extract",
                        request.function_name(),
                        source,
                    )
                })?;
                run_request_with_cancellation(request, &workdir, is_cancelled)
            })();
        let _ = fs::remove_dir_all(&workdir);

        result
    }

    fn invoke_async(
        &self,
        request: LambdaInvocationRequest,
    ) -> Result<(), InfrastructureError> {
        let executor = self.clone();
        let function_name = request.function_name().to_owned();
        thread::Builder::new()
            .name(format!("lambda-{function_name}"))
            .spawn(move || {
                let _ = executor.invoke_inner(&request);
            })
            .map_err(|source| {
                InfrastructureError::container("spawn", function_name, source)
            })?;

        Ok(())
    }

    fn validate_zip(
        &self,
        runtime: &str,
        handler: &str,
        archive: &[u8],
    ) -> Result<(), InfrastructureError> {
        validate_zip_archive(runtime, handler, archive).map_err(|source| {
            InfrastructureError::container("validate", "lambda-zip", source)
        })
    }
}

fn extract_archive(archive: &[u8], destination: &Path) -> io::Result<()> {
    let mut zip = ZipArchive::new(Cursor::new(archive))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    for index in 0..zip.len() {
        let mut entry = zip.by_index(index).map_err(|error| {
            io::Error::new(io::ErrorKind::InvalidData, error)
        })?;
        let Some(relative) = entry.enclosed_name().map(Path::to_path_buf)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zip entry escapes the extraction directory",
            ));
        };
        let output = destination.join(relative);

        if entry.is_dir() {
            fs::create_dir_all(&output)?;
            continue;
        }

        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(&output)?;
        io::copy(&mut entry, &mut file)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = entry
                .unix_mode()
                .or_else(|| {
                    output.file_name().and_then(|name| {
                        (name == "bootstrap").then_some(0o755)
                    })
                })
                .unwrap_or(0o644);
            fs::set_permissions(&output, fs::Permissions::from_mode(mode))?;
        }
    }

    Ok(())
}

fn error_payload(message: String) -> Vec<u8> {
    json!({
        "errorMessage": message,
        "errorType": "RuntimeError",
    })
    .to_string()
    .into_bytes()
}

fn find_python_command() -> io::Result<&'static str> {
    for candidate in ["python3", "python"] {
        let status = Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if status.is_ok_and(|status| status.success()) {
            return Ok(candidate);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "no python interpreter is available for lambda execution",
    ))
}

fn handler_module_path(handler: &str) -> io::Result<PathBuf> {
    let Some((module, function_name)) = handler.rsplit_once('.') else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "python handlers must use module.function syntax",
        ));
    };
    if module.trim().is_empty() || function_name.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "python handlers must use module.function syntax",
        ));
    }

    Ok(PathBuf::from(module.replace('.', "/")).with_extension("py"))
}

fn io_error(
    operation: &'static str,
    container: &str,
    message: &str,
) -> InfrastructureError {
    InfrastructureError::container(
        operation,
        container,
        io::Error::new(io::ErrorKind::InvalidInput, message.to_owned()),
    )
}

fn is_python_runtime(runtime: &str) -> bool {
    runtime.starts_with("python3.")
}

fn is_provided_runtime(runtime: &str) -> bool {
    matches!(runtime, "provided" | "provided.al2" | "provided.al2023")
}

fn python_function_name(handler: &str) -> io::Result<&str> {
    handler
        .rsplit_once('.')
        .map(|(_, function_name)| function_name)
        .filter(|name| !name.trim().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "python handlers must use module.function syntax",
            )
        })
}

fn python_helper() -> &'static str {
    r#"
import importlib
import json
import os
import sys

module_name = sys.argv[1]
function_name = sys.argv[2]
sys.path.insert(0, os.getcwd())
payload = sys.stdin.buffer.read()
event = {} if not payload else json.loads(payload.decode("utf-8"))
handler = getattr(importlib.import_module(module_name), function_name)
result = handler(event, None)

if isinstance(result, (bytes, bytearray)):
    sys.stdout.buffer.write(result)
elif isinstance(result, str):
    sys.stdout.write(result)
else:
    sys.stdout.write(json.dumps(result))
"#
}

fn run_request(
    request: &LambdaInvocationRequest,
    workdir: &Path,
) -> Result<LambdaInvocationResult, InfrastructureError> {
    run_request_with_cancellation(request, workdir, &|| false)
}

fn run_request_with_cancellation(
    request: &LambdaInvocationRequest,
    workdir: &Path,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<LambdaInvocationResult, InfrastructureError> {
    let mut command = build_command(request, workdir).map_err(|source| {
        InfrastructureError::container(
            "start",
            request.function_name(),
            source,
        )
    })?;
    command.current_dir(workdir);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.env("AWS_LAMBDA_FUNCTION_NAME", request.function_name());
    command.env("AWS_LAMBDA_FUNCTION_VERSION", request.version());
    command.env("LAMBDA_TASK_ROOT", workdir);
    command.env("_HANDLER", request.handler());
    command.envs(request.environment());

    let mut child = command.spawn().map_err(|source| {
        InfrastructureError::container(
            "start",
            request.function_name(),
            source,
        )
    })?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(request.payload()).map_err(|source| {
            InfrastructureError::container(
                "write",
                request.function_name(),
                source,
            )
        })?;
    }
    let output =
        wait_for_child_output(child, request.function_name(), is_cancelled)?;

    if output.status.success() {
        Ok(LambdaInvocationResult::new(output.stdout, Option::<String>::None))
    } else {
        let message =
            String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let message = if message.is_empty() {
            format!("lambda process exited with status {}", output.status)
        } else {
            message
        };

        Ok(LambdaInvocationResult::new(
            error_payload(message),
            Some("Unhandled"),
        ))
    }
}

fn wait_for_child_output(
    mut child: Child,
    function_name: &str,
    is_cancelled: &(dyn Fn() -> bool + Send + Sync),
) -> Result<std::process::Output, InfrastructureError> {
    loop {
        if is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(InfrastructureError::container(
                "wait",
                function_name,
                io::Error::new(
                    io::ErrorKind::Interrupted,
                    "lambda invocation cancelled",
                ),
            ));
        }

        match child.try_wait().map_err(|source| {
            InfrastructureError::container("wait", function_name, source)
        })? {
            Some(_) => {
                return child.wait_with_output().map_err(|source| {
                    InfrastructureError::container(
                        "wait",
                        function_name,
                        source,
                    )
                });
            }
            None => thread::sleep(
                ProcessLambdaExecutor::CANCELLATION_POLL_INTERVAL,
            ),
        }
    }
}

fn build_command(
    request: &LambdaInvocationRequest,
    workdir: &Path,
) -> io::Result<Command> {
    if is_provided_runtime(request.runtime()) {
        let program = workdir.join("bootstrap");
        return Ok(Command::new(program));
    }

    if is_python_runtime(request.runtime()) {
        let python = find_python_command()?;
        let module_path = handler_module_path(request.handler())?;
        if !workdir.join(&module_path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "lambda handler module `{}` is missing from the archive",
                    module_path.display()
                ),
            ));
        }
        let module =
            module_path.with_extension("").to_string_lossy().replace('/', ".");
        let function_name = python_function_name(request.handler())?;
        let mut command = Command::new(python);
        command.arg("-c").arg(python_helper()).arg(module).arg(function_name);

        return Ok(command);
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "runtime `{}` is not supported by the process lambda executor",
            request.runtime()
        ),
    ))
}

fn validate_zip_archive(
    runtime: &str,
    handler: &str,
    archive: &[u8],
) -> io::Result<()> {
    let mut zip = ZipArchive::new(Cursor::new(archive))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut entries = Vec::new();

    for index in 0..zip.len() {
        let entry = zip.by_index(index).map_err(|error| {
            io::Error::new(io::ErrorKind::InvalidData, error)
        })?;
        let Some(path) = entry.enclosed_name().map(Path::to_path_buf) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zip entry escapes the extraction directory",
            ));
        };
        entries.push(path);
    }

    if is_provided_runtime(runtime) {
        if entries.iter().any(|path| path == Path::new("bootstrap")) {
            return Ok(());
        }

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provided runtimes require a bootstrap file at the archive root",
        ));
    }

    if is_python_runtime(runtime) {
        let module_path = handler_module_path(handler)?;
        if entries.iter().any(|path| path == &module_path) {
            return Ok(());
        }

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "python handler module `{}` is missing from the archive",
                module_path.display()
            ),
        ));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsupported runtime `{runtime}`"),
    ))
}

#[cfg(test)]
mod tests {
    use super::{ProcessLambdaExecutor, validate_zip_archive};
    use aws::{
        LambdaExecutionPackage, LambdaExecutor, LambdaInvocationRequest,
    };
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use test_support::temporary_directory;
    use zip::write::FileOptions;

    fn zip_with_entries(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut cursor = std::io::Cursor::new(Vec::new());
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

        cursor.into_inner()
    }

    #[test]
    fn validate_zip_accepts_supported_archive_layouts() {
        let provided = zip_with_entries(&[("bootstrap", "#!/bin/sh\ncat\n")]);
        validate_zip_archive("provided.al2", "ignored.handler", &provided)
            .expect("provided runtime layout should validate");

        let python = zip_with_entries(&[(
            "handler.py",
            "def handler(event, _):\n    return event\n",
        )]);
        validate_zip_archive("python3.12", "handler.handler", &python)
            .expect("python runtime layout should validate");
    }

    #[test]
    fn validate_zip_rejects_missing_runtime_entrypoints() {
        let provided = zip_with_entries(&[("not-bootstrap", "echo nope")]);
        assert!(
            validate_zip_archive("provided.al2", "ignored.handler", &provided)
                .is_err()
        );

        let python = zip_with_entries(&[(
            "other.py",
            "def handler(event, _):\n    return event\n",
        )]);
        assert!(
            validate_zip_archive("python3.12", "handler.handler", &python)
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn process_lambda_executor_runs_provided_runtime_archives() {
        let executor = ProcessLambdaExecutor::new(temporary_directory(
            "hosting-lambda-executor",
        ));
        let archive = zip_with_entries(&[("bootstrap", "#!/bin/sh\ncat\n")]);
        let request = LambdaInvocationRequest::new(
            "echo",
            "$LATEST",
            "provided.al2",
            "ignored.handler",
            LambdaExecutionPackage::ZipArchive(archive),
            br#"{"ok":true}"#.to_vec(),
        );

        let result = executor.invoke(&request).expect("invoke should succeed");

        assert_eq!(result.function_error(), None);
        assert_eq!(result.payload(), br#"{"ok":true}"#);
    }

    #[cfg(unix)]
    #[test]
    fn process_lambda_executor_cancels_blocked_invocations_promptly() {
        let executor = ProcessLambdaExecutor::new(temporary_directory(
            "hosting-lambda-executor-cancel",
        ));
        let archive =
            zip_with_entries(&[("bootstrap", "#!/bin/sh\nsleep 20\ncat\n")]);
        let request = LambdaInvocationRequest::new(
            "slow",
            "$LATEST",
            "provided.al2",
            "ignored.handler",
            LambdaExecutionPackage::ZipArchive(archive),
            br#"{"ok":true}"#.to_vec(),
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let trigger = Arc::clone(&cancelled);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            trigger.store(true, Ordering::SeqCst);
        });

        let started_at = Instant::now();
        let error = executor
            .invoke_with_cancellation(&request, &|| {
                cancelled.load(Ordering::SeqCst)
            })
            .expect_err("cancelled invoke should fail");

        assert!(
            started_at.elapsed() < Duration::from_secs(1),
            "cancellation should stop the child promptly",
        );
        assert!(
            error.to_string().contains("lambda invocation cancelled"),
            "unexpected cancellation error: {error}",
        );
    }
}
