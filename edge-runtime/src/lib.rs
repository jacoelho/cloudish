#![cfg_attr(
    test,
    allow(
        clippy::unreachable,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

mod adapters;
mod builders;
mod composition;
mod runtime_services;

pub use adapters::ProcessLambdaExecutor;
pub use adapters::ThreadRdsBackendRuntime;
pub use adapters::{
    DirectoryBlobStore, FixedClock, ManagedBackgroundTasks,
    ManualBackgroundScheduler, MemoryBlobStore, MemoryPayloadStore,
    OsRandomSource, ProcessContainerRuntime, RecordingLogSink,
    SequenceRandomSource, SystemClock, TcpHttpForwarder,
    ThreadBackgroundScheduler, ThreadTcpProxyRuntime,
};
pub use adapters::{
    ThreadElastiCacheNodeRuntime, ThreadElastiCacheProxyRuntime,
};
pub use adapters::{
    ThreadStepFunctionsExecutionSpawner, ThreadStepFunctionsSleeper,
};
pub use builders::{
    LocalRuntimeBuilder, RuntimeAssembly, RuntimeBuildError,
    TestRuntimeBuilder,
};
pub use composition::ApiGatewayIntegrationExecutor;
pub use composition::LambdaStepFunctionsTaskAdapter;
pub use composition::S3NotificationDispatcher;
pub use composition::{
    SnsServiceDependencies, build_sns_service, cloudish_sns_signing_cert_pem,
};
pub use runtime_services::*;
pub use runtime_services::{RuntimeServices, RuntimeServicesBuilder};
