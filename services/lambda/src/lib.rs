#![cfg_attr(
    test,
    allow(clippy::arithmetic_side_effects, clippy::indexing_slicing)
)]

pub mod aliases;
mod arns;
pub mod async_delivery;
pub mod code;
pub mod errors;
pub mod event_source_mappings;
pub mod functions;
pub mod invocation;
pub mod permissions;
#[doc(hidden)]
pub mod request_runtime;
pub mod scope;
mod state;
pub mod urls;
pub mod versions;

pub use aliases::{
    CreateAliasInput, LambdaAliasConfiguration, ListAliasesOutput,
    UpdateAliasInput,
};
pub use async_delivery::{
    DestinationConfigInput, DestinationConfigOutput, DestinationTargetInput,
    DestinationTargetOutput, FunctionEventInvokeConfigOutput,
    ListFunctionEventInvokeConfigsOutput, PutFunctionEventInvokeConfigInput,
    UpdateFunctionEventInvokeConfigInput,
};
pub use code::{LambdaCodeInput, UpdateFunctionCodeInput};
pub use errors::{LambdaError, LambdaInitError};
pub use event_source_mappings::{
    CreateEventSourceMappingInput, EventSourceMappingOutput,
    ListEventSourceMappingsOutput, UpdateEventSourceMappingInput,
};
pub use functions::{
    CreateFunctionInput, LambdaDeadLetterConfig, LambdaEnvironment,
    LambdaFunctionCodeLocation, LambdaFunctionConfiguration,
    LambdaGetFunctionOutput, LambdaService, LambdaServiceDependencies,
    ListFunctionsOutput,
};
pub use invocation::{
    ApiGatewayInvokeInput, InvokeInput, InvokeOutput, LambdaInvocationType,
};
pub use permissions::{AddPermissionInput, AddPermissionOutput};
pub use scope::LambdaScope;
pub use state::*;
pub use urls::{
    CreateFunctionUrlConfigInput, FunctionUrlInvocationInput,
    FunctionUrlInvocationOutput, LambdaFunctionUrlAuthType,
    LambdaFunctionUrlConfig, LambdaFunctionUrlInvokeMode,
    ListFunctionUrlConfigsOutput, ResolvedFunctionUrlTarget,
    UpdateFunctionUrlConfigInput,
};
pub use versions::{ListVersionsByFunctionOutput, PublishVersionInput};
