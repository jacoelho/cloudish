#[cfg(feature = "lambda")]
use crate::ManagedBackgroundTasks;
#[cfg(feature = "lambda")]
use aws::{BackgroundScheduler, InfrastructureError};
#[cfg(feature = "lambda")]
use lambda::LambdaService;
#[cfg(feature = "lambda")]
use std::sync::Arc;
#[cfg(feature = "lambda")]
use std::time::Duration;

#[cfg(feature = "lambda")]
pub(crate) fn start_lambda_background_tasks(
    lambda: LambdaService,
    scheduler: Arc<dyn BackgroundScheduler>,
) -> Result<ManagedBackgroundTasks, InfrastructureError> {
    let async_service = lambda.clone();
    let async_handle = scheduler.schedule_repeating(
        "lambda-async-invocations".to_owned(),
        Duration::from_millis(25),
        Arc::new(move || {
            let _ = async_service.run_async_invocation_cycle();
        }),
    )?;
    let mapping_service = lambda;
    let mapping_handle = scheduler.schedule_repeating(
        "lambda-sqs-mappings".to_owned(),
        Duration::from_millis(25),
        Arc::new(move || {
            let _ = mapping_service.run_sqs_event_source_mapping_cycle();
        }),
    )?;

    Ok(ManagedBackgroundTasks::new(vec![async_handle, mapping_handle]))
}
