#![forbid(unsafe_code)]
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

mod adapters;
mod builders;
mod composition;
mod runtime_services;

#[cfg(feature = "lambda")]
pub use adapters::ProcessLambdaExecutor;
#[cfg(feature = "rds")]
pub use adapters::ThreadRdsBackendRuntime;
pub use adapters::{
    DirectoryBlobStore, FixedClock, ManagedBackgroundTasks,
    ManualBackgroundScheduler, MemoryBlobStore, MemoryPayloadStore,
    OsRandomSource, ProcessContainerRuntime, RecordingLogSink,
    SequenceRandomSource, SystemClock, TcpHttpForwarder,
    ThreadBackgroundScheduler, ThreadTcpProxyRuntime,
};
#[cfg(feature = "elasticache")]
pub use adapters::{
    ThreadElastiCacheNodeRuntime, ThreadElastiCacheProxyRuntime,
};
#[cfg(feature = "step-functions")]
pub use adapters::{
    ThreadStepFunctionsExecutionSpawner, ThreadStepFunctionsSleeper,
};
pub use builders::{
    LocalRuntimeBuilder, RuntimeAssembly, RuntimeBuildError,
    TestRuntimeBuilder,
};
#[cfg(feature = "apigateway")]
pub use composition::ApiGatewayIntegrationExecutor;
#[cfg(feature = "step-functions")]
pub use composition::LambdaStepFunctionsTaskAdapter;
#[cfg(feature = "s3")]
pub use composition::S3NotificationDispatcher;
#[cfg(feature = "sns")]
pub use composition::{SnsServiceDependencies, build_sns_service};
pub use runtime_services::*;
pub use runtime_services::{
    EnabledServices, RuntimeServices, RuntimeServicesBuilder,
    supported_services,
};
