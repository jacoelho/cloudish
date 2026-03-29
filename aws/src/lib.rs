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

mod account_id;
mod arn;
mod caller_credentials;
mod credential_scope;
mod edge_addressing;
mod error;
mod execute_api_source_arn;
mod iam_identity;
mod kms_key_reference;
mod lambda_function_target;
mod pagination_token;
mod region_id;
mod request_context;
mod runtime;
mod runtime_defaults;
mod service_name;
mod session_credentials;

pub use account_id::{AccountId, AccountIdError};
pub use arn::{
    Arn, ArnError, ArnResource, Partition, PartitionError, S3ArnResource,
    S3ArnResourceError, SqsArnResource, SqsArnResourceError,
};
pub use caller_credentials::{
    AwsPrincipalType, CallerCredentialKind, StableAwsPrincipal,
    TemporaryCredentialKind,
};
pub use credential_scope::CredentialScope;
pub use edge_addressing::{
    AWS_PATH_PREFIX, AdvertisedEdge, AdvertisedEdgeTemplate,
    ReservedExecuteApiPath, ReservedLambdaFunctionUrlPath,
    SharedAdvertisedEdge, parse_reserved_execute_api_path,
    parse_reserved_lambda_function_url_path,
};
pub use error::{AwsError, AwsErrorBuildError, AwsErrorFamily};
pub use execute_api_source_arn::{
    ExecuteApiSourceArn, ExecuteApiSourceArnError,
};
pub use iam_identity::{
    IamAccessKeyLookup, IamAccessKeyRecord, IamAccessKeyStatus,
    IamInstanceProfileLookup, IamInstanceProfileRecord, IamResourceTag,
    IamRoleRecord,
};
pub use kms_key_reference::{
    KmsAliasName, KmsAliasNameError, KmsKeyId, KmsKeyIdError, KmsKeyReference,
    KmsKeyReferenceError,
};
pub use lambda_function_target::{
    LambdaFunctionTarget, LambdaFunctionTargetError,
};
pub use pagination_token::{PaginationToken, PaginationTokenError};
pub use region_id::{RegionId, RegionIdError};
pub use request_context::{
    CallerIdentity, CallerIdentityError, ProtocolFamily, RequestContext,
    RequestContextError,
};
pub use runtime::{
    BackgroundScheduler, BlobKey, BlobMetadata, BlobStore, Clock,
    ContainerCommand, ContainerRuntime, Endpoint, HttpForwardRequest,
    HttpForwardResponse, HttpForwarder, InfrastructureError,
    LambdaExecutionPackage, LambdaExecutor, LambdaInvocationRequest,
    LambdaInvocationResult, LogRecord, LogSink, PayloadId, PayloadStore,
    RandomSource, RunningContainer, RunningTcpProxy, ScheduledTaskHandle,
    StoredPayload, TcpProxyRuntime, TcpProxySpec,
};
pub use runtime_defaults::{
    DEFAULT_ACCOUNT_ENV, DEFAULT_REGION_ENV, RuntimeDefaults,
    RuntimeDefaultsError, STATE_DIRECTORY_ENV,
};
pub use service_name::{ServiceName, ServiceNameError};
pub use session_credentials::{
    SessionCredentialLookup, SessionCredentialRecord,
};
