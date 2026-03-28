#![forbid(unsafe_code)]

#[cfg(feature = "elasticache")]
mod elasticache;
mod fakes;
mod filesystem;
#[cfg(feature = "lambda")]
mod lambda;
mod network;
mod process;
#[cfg(feature = "rds")]
mod rds;
mod runtime;

#[cfg(feature = "elasticache")]
pub use elasticache::{
    ThreadElastiCacheNodeRuntime, ThreadElastiCacheProxyRuntime,
};
pub use fakes::{
    FixedClock, ManualBackgroundScheduler, MemoryBlobStore,
    MemoryPayloadStore, RecordingLogSink, SequenceRandomSource,
};
pub use filesystem::DirectoryBlobStore;
#[cfg(feature = "lambda")]
pub use lambda::ProcessLambdaExecutor;
pub use network::{TcpHttpForwarder, ThreadTcpProxyRuntime};
pub use process::ProcessContainerRuntime;
#[cfg(feature = "rds")]
pub use rds::ThreadRdsBackendRuntime;
pub use runtime::{
    ManagedBackgroundTasks, OsRandomSource, SystemClock,
    ThreadBackgroundScheduler,
};
#[cfg(feature = "step-functions")]
pub use runtime::{
    ThreadStepFunctionsExecutionSpawner, ThreadStepFunctionsSleeper,
};
