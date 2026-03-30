use std::io::Write;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match app::run_from_env().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "{error}");
            let _ = stderr.flush();
            ExitCode::FAILURE
        }
    }
}
