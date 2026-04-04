use crate::adapters::ThreadWorkQueueShutdownOutcome;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use storage::StorageError;

pub struct ShutdownWarning {
    service: &'static str,
    error: Box<dyn std::error::Error + Send + Sync>,
}

impl ShutdownWarning {
    fn new(
        service: &'static str,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self { service, error: Box::new(error) }
    }

    pub fn service(&self) -> &str {
        self.service
    }
}

impl fmt::Display for ShutdownWarning {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.service, self.error)
    }
}

impl fmt::Debug for ShutdownWarning {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShutdownWarning")
            .field("service", &self.service)
            .field("error", &self.error.to_string())
            .finish()
    }
}

type StorageShutdownHook =
    Arc<dyn Fn() -> Result<(), StorageError> + Send + Sync>;
type LambdaBackgroundShutdownHook =
    Arc<dyn Fn() -> Result<(), InfrastructureError> + Send + Sync>;

pub use apigateway::{
    ApiGatewayError, ApiGatewayScope, ApiGatewayService, ApiKey, ApiMethod,
    ApiResource, Authorizer, CreateApiKeyInput, CreateAuthorizerInput,
    CreateDeploymentInput, CreateHttpApiAuthorizerInput,
    CreateHttpApiDeploymentInput, CreateHttpApiInput,
    CreateHttpApiIntegrationInput, CreateHttpApiRouteInput,
    CreateHttpApiStageInput, CreateRequestValidatorInput, CreateResourceInput,
    CreateRestApiInput, CreateStageInput, CreateUsagePlanInput,
    CreateUsagePlanKeyInput, Deployment, EndpointConfiguration,
    ExecuteApiError, ExecuteApiIntegrationExecutor, ExecuteApiIntegrationPlan,
    ExecuteApiInvocation, ExecuteApiLambdaProxyPlan,
    ExecuteApiPreparedResponse, ExecuteApiRequest, GetTagsOutput, HttpApi,
    HttpApiAuthorizer, HttpApiCollection, HttpApiDeployment,
    HttpApiIntegration, HttpApiRoute, HttpApiStage, Integration,
    ItemCollection, MethodResponse, PatchOperation, PutIntegrationInput,
    PutMethodInput, PutMethodResponseInput, QuotaSettings, RequestValidator,
    RestApi, Stage, TagResourceInput, ThrottleSettings,
    UpdateHttpApiAuthorizerInput, UpdateHttpApiInput,
    UpdateHttpApiIntegrationInput, UpdateHttpApiRouteInput,
    UpdateHttpApiStageInput, UsagePlan, UsagePlanApiStage, UsagePlanKey,
    UsagePlanKeyCollection, map_lambda_proxy_response,
    map_lambda_proxy_response_v2,
};
pub use aws::{
    BackgroundScheduler, BlobKey, BlobMetadata, BlobStore, Clock,
    ContainerCommand, ContainerRuntime, Endpoint, HttpForwardRequest,
    HttpForwardResponse, HttpForwarder, InfrastructureError, KmsAliasName,
    KmsKeyId, KmsKeyReference, KmsKeyReferenceError, LambdaExecutionPackage,
    LambdaExecutor, LambdaInvocationRequest, LambdaInvocationResult,
    LogRecord, LogSink, PayloadId, PayloadStore, RandomSource,
    RunningContainer, RunningTcpProxy, ScheduledTaskHandle, StoredPayload,
    TcpProxyRuntime, TcpProxySpec,
};
pub use cloudformation::{
    CloudFormationChangeSetDescription, CloudFormationChangeSetSummary,
    CloudFormationChangeSummary, CloudFormationCreateChangeSetInput,
    CloudFormationCreateChangeSetOutput, CloudFormationDependencies,
    CloudFormationError, CloudFormationPlanInput, CloudFormationPlannedOutput,
    CloudFormationPlannedResource, CloudFormationScope, CloudFormationService,
    CloudFormationStackDescription, CloudFormationStackEvent,
    CloudFormationStackOperationInput, CloudFormationStackOperationOutput,
    CloudFormationStackOutput, CloudFormationStackPlan,
    CloudFormationStackResourceDescription,
    CloudFormationStackResourceSummary, CloudFormationTemplateParameter,
    ValidateTemplateInput, ValidateTemplateOutput,
};
pub use cloudwatch::{
    AlarmStateValue, CloudWatchLogsError, CloudWatchMetricsError,
    CloudWatchScope, CloudWatchService, CreateLogGroupInput,
    CreateLogStreamInput, DeleteAlarmsInput, DeleteRetentionPolicyInput,
    DescribeAlarmsInput, DescribeAlarmsOutput, DescribeLogGroupsInput,
    DescribeLogGroupsOutput, DescribeLogStreamsInput,
    DescribeLogStreamsOutput, Dimension, DimensionFilter,
    FilterLogEventsInput, FilterLogEventsOutput, FilteredLogEvent,
    GetLogEventsInput, GetLogEventsOutput, GetMetricDataInput,
    GetMetricDataOutput, GetMetricStatisticsInput, GetMetricStatisticsOutput,
    InputLogEvent, ListMetricsInput, ListMetricsOutput, LogGroupDescription,
    LogStreamDescription, Metric, MetricAlarm, MetricDataQuery,
    MetricDataResult, MetricDatapoint, MetricDatum, MetricDescriptor,
    MetricNamespace, MetricNamespaceError, MetricStat, OutputLogEvent,
    PutLogEventsInput, PutLogEventsOutput, PutMetricAlarmInput,
    PutRetentionPolicyInput, ScanBy, SearchedLogStream, SetAlarmStateInput,
    StatisticSet,
};
pub use cognito::{
    AdminCreateUserInput as CognitoAdminCreateUserInput,
    AdminCreateUserOutput as CognitoAdminCreateUserOutput,
    AdminDeleteUserInput as CognitoAdminDeleteUserInput,
    AdminDeleteUserOutput as CognitoAdminDeleteUserOutput,
    AdminGetUserInput as CognitoAdminGetUserInput,
    AdminGetUserOutput as CognitoAdminGetUserOutput,
    AdminInitiateAuthInput as CognitoAdminInitiateAuthInput,
    AdminInitiateAuthOutput as CognitoAdminInitiateAuthOutput,
    AdminSetUserPasswordInput as CognitoAdminSetUserPasswordInput,
    AdminSetUserPasswordOutput as CognitoAdminSetUserPasswordOutput,
    AdminUpdateUserAttributesInput as CognitoAdminUpdateUserAttributesInput,
    AdminUpdateUserAttributesOutput as CognitoAdminUpdateUserAttributesOutput,
    AttributeType as CognitoAttributeType,
    ChangePasswordInput as CognitoChangePasswordInput,
    ChangePasswordOutput as CognitoChangePasswordOutput,
    CognitoAuthenticationResult, CognitoChallengeName,
    CognitoCodeDeliveryDetails, CognitoError, CognitoExplicitAuthFlow,
    CognitoJwk, CognitoJwksDocument, CognitoOpenIdConfiguration, CognitoScope,
    CognitoService, CognitoUser, CognitoUserPool, CognitoUserPoolClient,
    CognitoUserPoolClientDescription, CognitoUserPoolStatus,
    CognitoUserPoolSummary, CognitoUserStatus,
    ConfirmSignUpInput as CognitoConfirmSignUpInput,
    ConfirmSignUpOutput as CognitoConfirmSignUpOutput,
    CreateUserPoolClientInput as CreateCognitoUserPoolClientInput,
    CreateUserPoolClientOutput as CreateCognitoUserPoolClientOutput,
    CreateUserPoolInput as CreateCognitoUserPoolInput,
    CreateUserPoolOutput as CreateCognitoUserPoolOutput,
    DeleteUserPoolClientInput as DeleteCognitoUserPoolClientInput,
    DeleteUserPoolClientOutput as DeleteCognitoUserPoolClientOutput,
    DeleteUserPoolInput as DeleteCognitoUserPoolInput,
    DeleteUserPoolOutput as DeleteCognitoUserPoolOutput,
    DescribeUserPoolClientInput as DescribeCognitoUserPoolClientInput,
    DescribeUserPoolClientOutput as DescribeCognitoUserPoolClientOutput,
    DescribeUserPoolInput as DescribeCognitoUserPoolInput,
    DescribeUserPoolOutput as DescribeCognitoUserPoolOutput,
    GetUserInput as CognitoGetUserInput,
    GetUserOutput as CognitoGetUserOutput,
    InitiateAuthInput as CognitoInitiateAuthInput,
    InitiateAuthOutput as CognitoInitiateAuthOutput,
    ListUserPoolClientsInput as ListCognitoUserPoolClientsInput,
    ListUserPoolClientsOutput as ListCognitoUserPoolClientsOutput,
    ListUserPoolsInput as ListCognitoUserPoolsInput,
    ListUserPoolsOutput as ListCognitoUserPoolsOutput,
    ListUsersInput as CognitoListUsersInput,
    ListUsersOutput as CognitoListUsersOutput,
    RespondToAuthChallengeInput as CognitoRespondToAuthChallengeInput,
    RespondToAuthChallengeOutput as CognitoRespondToAuthChallengeOutput,
    SignUpInput as CognitoSignUpInput, SignUpOutput as CognitoSignUpOutput,
    UpdateUserAttributesInput as CognitoUpdateUserAttributesInput,
    UpdateUserAttributesOutput as CognitoUpdateUserAttributesOutput,
    UpdateUserPoolClientInput as UpdateCognitoUserPoolClientInput,
    UpdateUserPoolClientOutput as UpdateCognitoUserPoolClientOutput,
    UpdateUserPoolInput as UpdateCognitoUserPoolInput,
    UpdateUserPoolOutput as UpdateCognitoUserPoolOutput,
};
pub use dynamodb::{
    AttributeDefinition as DynamoDbAttributeDefinition,
    AttributeUpdate as DynamoDbAttributeUpdate,
    AttributeUpdateAction as DynamoDbAttributeUpdateAction,
    AttributeValue as DynamoDbAttributeValue,
    BatchGetItemInput as DynamoDbBatchGetItemInput,
    BatchGetItemOutput as DynamoDbBatchGetItemOutput,
    BatchWriteItemInput as DynamoDbBatchWriteItemInput,
    BatchWriteItemOutput as DynamoDbBatchWriteItemOutput,
    BatchWriteRequest as DynamoDbBatchWriteRequest,
    BillingMode as DynamoDbBillingMode,
    CancellationReason as DynamoDbCancellationReason,
    CreateTableInput as CreateDynamoDbTableInput,
    DeleteItemInput as DynamoDbDeleteItemInput,
    DeleteItemOutput as DynamoDbDeleteItemOutput,
    DescribeStreamInput as DynamoDbDescribeStreamInput, DynamoDbError,
    DynamoDbInitError, DynamoDbScope, DynamoDbService,
    GetItemInput as DynamoDbGetItemInput,
    GetItemOutput as DynamoDbGetItemOutput,
    GetRecordsInput as DynamoDbGetRecordsInput,
    GetRecordsOutput as DynamoDbGetRecordsOutput,
    GetShardIteratorInput as DynamoDbGetShardIteratorInput,
    GetShardIteratorOutput as DynamoDbGetShardIteratorOutput,
    GlobalSecondaryIndexDefinition as DynamoDbGlobalSecondaryIndexDefinition,
    Item as DynamoDbItem, KeySchemaElement as DynamoDbKeySchemaElement,
    KeyType as DynamoDbKeyType,
    KeysAndAttributes as DynamoDbKeysAndAttributes,
    ListStreamsInput as DynamoDbListStreamsInput,
    ListStreamsOutput as DynamoDbListStreamsOutput,
    ListTagsOfResourceInput as DynamoDbListTagsOfResourceInput,
    ListTagsOfResourceOutput as DynamoDbListTagsOfResourceOutput,
    LocalSecondaryIndexDefinition as DynamoDbLocalSecondaryIndexDefinition,
    Projection as DynamoDbProjection,
    ProjectionType as DynamoDbProjectionType,
    ProvisionedThroughput as DynamoDbProvisionedThroughput,
    PutItemInput as DynamoDbPutItemInput,
    PutItemOutput as DynamoDbPutItemOutput, QueryInput as DynamoDbQueryInput,
    QueryOutput as DynamoDbQueryOutput, ResourceTag as DynamoDbResourceTag,
    ReturnValues as DynamoDbReturnValues,
    ScalarAttributeType as DynamoDbScalarAttributeType,
    ScanInput as DynamoDbScanInput, ScanOutput as DynamoDbScanOutput,
    SequenceNumberRange as DynamoDbSequenceNumberRange,
    ShardIteratorType as DynamoDbShardIteratorType,
    StreamDescription as DynamoDbStreamDescription,
    StreamRecord as DynamoDbStreamRecord,
    StreamRecordData as DynamoDbStreamRecordData,
    StreamShard as DynamoDbStreamShard,
    StreamSpecification as DynamoDbStreamSpecification,
    StreamStatus as DynamoDbStreamStatus,
    StreamSummary as DynamoDbStreamSummary,
    StreamViewType as DynamoDbStreamViewType,
    TableDescription as DynamoDbTableDescription,
    TableName as DynamoDbTableName, TableStatus as DynamoDbTableStatus,
    TagResourceInput as DynamoDbTagResourceInput,
    TimeToLiveDescription as DynamoDbTimeToLiveDescription,
    TimeToLiveSpecification as DynamoDbTimeToLiveSpecification,
    TimeToLiveStatus as DynamoDbTimeToLiveStatus,
    TransactConditionCheck as DynamoDbTransactConditionCheck,
    TransactDeleteItem as DynamoDbTransactDeleteItem,
    TransactGetItem as DynamoDbTransactGetItem,
    TransactGetItemOutput as DynamoDbTransactGetItemOutput,
    TransactGetItemsInput as DynamoDbTransactGetItemsInput,
    TransactGetItemsOutput as DynamoDbTransactGetItemsOutput,
    TransactPutItem as DynamoDbTransactPutItem,
    TransactUpdateItem as DynamoDbTransactUpdateItem,
    TransactWriteItem as DynamoDbTransactWriteItem,
    TransactWriteItemsInput as DynamoDbTransactWriteItemsInput,
    UntagResourceInput as DynamoDbUntagResourceInput,
    UpdateItemInput as DynamoDbUpdateItemInput,
    UpdateItemOutput as DynamoDbUpdateItemOutput,
    UpdateTableInput as UpdateDynamoDbTableInput,
    UpdateTimeToLiveInput as DynamoDbUpdateTimeToLiveInput,
};
pub use elasticache::{
    AuthenticationModeInput as ElastiCacheAuthenticationModeInput,
    CreateReplicationGroupInput as CreateElastiCacheReplicationGroupInput,
    CreateUserInput as CreateElastiCacheUserInput,
    ElastiCacheAuthenticationType, ElastiCacheConnectionAuthenticator,
    ElastiCacheEndpoint, ElastiCacheEngine, ElastiCacheError,
    ElastiCacheIamTokenValidator, ElastiCacheNodeGroup,
    ElastiCacheNodeGroupMember, ElastiCacheNodeRuntime, ElastiCacheNodeSpec,
    ElastiCacheProxyRuntime, ElastiCacheProxySpec, ElastiCacheScope,
    ElastiCacheService, ElastiCacheServiceDependencies, ElastiCacheUser,
    ModifyUserInput as ModifyElastiCacheUserInput,
    RejectAllElastiCacheIamTokenValidator, ReplicationGroup,
    ReplicationGroupStatus, RunningElastiCacheNode, RunningElastiCacheProxy,
    UserAuthentication,
};
pub use eventbridge::{
    CreateEventBusInput, DescribeRuleOutput, EventBridgeDeliveryDispatcher,
    EventBridgeError, EventBridgeInputTransformer, EventBridgePlannedDelivery,
    EventBridgeRuleState, EventBridgeScope, EventBridgeService,
    EventBridgeServiceDependencies, EventBridgeTarget, EventBusDescription,
    ListEventBusesInput, ListEventBusesOutput, ListRulesInput,
    ListRulesOutput, ListTargetsByRuleInput, ListTargetsByRuleOutput,
    NoopEventBridgeDeliveryDispatcher, PutEventsInput, PutEventsOutput,
    PutEventsRequestEntry, PutEventsResultEntry, PutRuleInput, PutRuleOutput,
    PutTargetsFailureEntry, PutTargetsInput, PutTargetsOutput,
    RemoveTargetsFailureEntry, RemoveTargetsInput, RemoveTargetsOutput,
    RuleSummary,
};
pub use iam::{
    AttachedPolicy, CreateGroupInput, CreateInstanceProfileInput,
    CreatePolicyInput, CreatePolicyVersionInput, CreateRoleInput,
    CreateUserInput, GroupDetails, IamAccessKey, IamAccessKeyMetadata,
    IamError, IamGroup, IamInstanceProfile, IamPolicy, IamPolicyVersion,
    IamRole, IamScope, IamService, IamTag, IamUser, InlinePolicy,
    InlinePolicyInput,
};
pub use kinesis::{
    AddTagsToStreamInput as KinesisAddTagsToStreamInput, CreateStreamInput,
    DeleteStreamInput as DeleteKinesisStreamInput,
    DeregisterStreamConsumerInput, DescribeStreamConsumerInput,
    DescribeStreamInput as DescribeKinesisStreamInput,
    DescribeStreamSummaryInput, GetRecordsInput as KinesisGetRecordsInput,
    GetRecordsOutput as KinesisGetRecordsOutput,
    GetShardIteratorInput as KinesisGetShardIteratorInput,
    GetShardIteratorOutput as KinesisGetShardIteratorOutput, KinesisConsumer,
    KinesisEncryptionType, KinesisError, KinesisHashKeyRange, KinesisRecord,
    KinesisScope, KinesisSequenceNumberRange, KinesisService, KinesisShard,
    KinesisShardIteratorType, KinesisStreamDescription,
    KinesisStreamDescriptionSummary, KinesisStreamIdentifier, KinesisTag,
    ListShardsInput as KinesisListShardsInput,
    ListShardsOutput as KinesisListShardsOutput, ListStreamConsumersInput,
    ListStreamConsumersOutput, ListStreamsInput, ListStreamsOutput,
    ListTagsForStreamInput, ListTagsForStreamOutput, MergeShardsInput,
    PutRecordInput, PutRecordOutput, PutRecordsEntry, PutRecordsInput,
    PutRecordsOutput, PutRecordsResultEntry, RegisterStreamConsumerInput,
    RemoveTagsFromStreamInput, SplitShardInput, StartStreamEncryptionInput,
    StopStreamEncryptionInput,
};
pub use kms::{
    CancelKmsKeyDeletionInput, CancelKmsKeyDeletionOutput,
    CreateKmsAliasInput, CreateKmsKeyInput, CreateKmsKeyOutput,
    DeleteKmsAliasInput, DescribeKmsKeyOutput, KmsDecryptInput,
    KmsDecryptOutput, KmsEncryptInput, KmsEncryptOutput, KmsError,
    KmsGenerateDataKeyInput, KmsGenerateDataKeyOutput, KmsKeyListEntry,
    KmsKeyMetadata, KmsListResourceTagsInput, KmsListResourceTagsOutput,
    KmsReEncryptInput, KmsReEncryptOutput, KmsScope, KmsService, KmsSignInput,
    KmsSignOutput, KmsTag, KmsTagResourceInput, KmsUntagResourceInput,
    KmsVerifyInput, KmsVerifyOutput, ListKmsAliasesInput,
    ListKmsAliasesOutput, ListKmsKeysOutput, ScheduleKmsKeyDeletionInput,
    ScheduleKmsKeyDeletionOutput,
};
pub use lambda::request_runtime::LambdaRequestRuntime;
pub use lambda::{
    AddPermissionInput as AddLambdaPermissionInput,
    AddPermissionOutput as AddLambdaPermissionOutput,
    CreateAliasInput as CreateLambdaAliasInput, CreateEventSourceMappingInput,
    CreateFunctionInput, CreateFunctionUrlConfigInput,
    DestinationConfigInput as LambdaDestinationConfigInput,
    DestinationTargetInput as LambdaDestinationTargetInput,
    EventSourceMappingOutput, FunctionEventInvokeConfigOutput,
    FunctionUrlInvocationInput, FunctionUrlInvocationOutput,
    InvokeInput as LambdaInvokeInput, InvokeOutput as LambdaInvokeOutput,
    LambdaAliasConfiguration, LambdaCodeInput, LambdaEnvironment, LambdaError,
    LambdaFunctionCodeLocation, LambdaFunctionConfiguration,
    LambdaFunctionUrlAuthType, LambdaFunctionUrlConfig,
    LambdaFunctionUrlInvokeMode, LambdaGetFunctionOutput, LambdaInitError,
    LambdaInvocationType, LambdaPackageType, LambdaScope, LambdaService,
    LambdaServiceDependencies, ListAliasesOutput,
    ListEventSourceMappingsOutput, ListFunctionEventInvokeConfigsOutput,
    ListFunctionUrlConfigsOutput,
    ListFunctionsOutput as ListLambdaFunctionsOutput,
    ListVersionsByFunctionOutput, PublishVersionInput,
    PutFunctionEventInvokeConfigInput, UpdateAliasInput,
    UpdateEventSourceMappingInput, UpdateFunctionCodeInput,
    UpdateFunctionEventInvokeConfigInput, UpdateFunctionUrlConfigInput,
};
pub use rds::{
    CreateDbClusterInput, CreateDbInstanceInput, CreateDbParameterGroupInput,
    DbCluster, DbClusterMember, DbInstance, DbLifecycleStatus, DbParameter,
    DbParameterGroup, ModifyDbClusterInput, ModifyDbInstanceInput,
    ModifyDbParameterGroupInput, RdsAuthEndpointSource, RdsBackendRuntime,
    RdsBackendSpec, RdsEndpoint, RdsEngine, RdsError, RdsIamTokenValidator,
    RdsScope, RdsService, RdsServiceDependencies,
    RejectAllRdsIamTokenValidator, RunningRdsBackend,
};
pub use s3::{
    BucketNotificationConfiguration, BucketObjectLockConfiguration,
    BucketTaggingOutput, BucketVersioningOutput, BucketVersioningStatus,
    CannedAcl, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CompletedMultipartPart, CopyObjectInput, CopyObjectOutput,
    CreateBucketInput, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, CsvFileHeaderInfo, CsvInputSerialization,
    CsvOutputSerialization, DefaultObjectLockRetention,
    DefaultRetentionPeriod, DeleteObjectInput, DeleteObjectOutput,
    GetBucketLocationOutput, GetObjectOutput, HeadBucketInput,
    HeadBucketOutput, HeadObjectOutput, IfRangeCondition, LegalHoldStatus,
    ListBucketsOutput, ListMultipartUploadsOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListedBucket, ListedDeleteMarker,
    ListedObject, ListedObjectVersion, ListedVersionEntry,
    MultipartUploadSummary, NotificationFilter, ObjectLegalHoldOutput,
    ObjectLockMode, ObjectLockWriteOptions, ObjectRange, ObjectReadConditions,
    ObjectReadMetadata, ObjectReadPreconditionOutcome, ObjectReadRequest,
    ObjectReadResponseOverrides, ObjectRetention, ObjectTaggingOutput,
    PutObjectInput, PutObjectLegalHoldInput, PutObjectOutput,
    PutObjectRetentionInput, QueueNotificationConfiguration, S3Error,
    S3EventNotification, S3InitError, S3NotificationTransport, S3Scope,
    S3Service, SelectObjectContentInput, SelectObjectContentOutput,
    SelectObjectStats, StoredBucketAclInput, TaggingInput,
    TopicNotificationConfiguration, UploadPartInput, UploadPartOutput,
};
pub use secrets_manager::{
    CreateSecretInput as SecretsManagerCreateSecretInput,
    CreateSecretOutput as SecretsManagerCreateSecretOutput,
    DeleteSecretInput as SecretsManagerDeleteSecretInput,
    DeleteSecretOutput as SecretsManagerDeleteSecretOutput,
    DescribeSecretInput as SecretsManagerDescribeSecretInput,
    DescribeSecretOutput as SecretsManagerDescribeSecretOutput,
    GetSecretValueInput as SecretsManagerGetSecretValueInput,
    GetSecretValueOutput as SecretsManagerGetSecretValueOutput,
    ListSecretVersionIdsInput as SecretsManagerListSecretVersionIdsInput,
    ListSecretVersionIdsOutput as SecretsManagerListSecretVersionIdsOutput,
    ListSecretsFilter as SecretsManagerListSecretsFilter,
    ListSecretsFilterKey as SecretsManagerListSecretsFilterKey,
    ListSecretsInput as SecretsManagerListSecretsInput,
    ListSecretsOutput as SecretsManagerListSecretsOutput,
    ListSecretsSortBy as SecretsManagerListSecretsSortBy,
    ListSecretsSortOrder as SecretsManagerListSecretsSortOrder,
    PutSecretValueInput as SecretsManagerPutSecretValueInput,
    PutSecretValueOutput as SecretsManagerPutSecretValueOutput,
    RotateSecretInput as SecretsManagerRotateSecretInput,
    RotateSecretOutput as SecretsManagerRotateSecretOutput,
    SecretName as SecretsManagerSecretName,
    SecretNameError as SecretsManagerSecretNameError,
    SecretReference as SecretsManagerSecretReference,
    SecretReferenceError as SecretsManagerSecretReferenceError,
    SecretVersionListEntry as SecretsManagerSecretVersionListEntry,
    SecretsManagerError, SecretsManagerExternalMetadataItem,
    SecretsManagerRotationRules, SecretsManagerScope,
    SecretsManagerSecretListEntry, SecretsManagerService, SecretsManagerTag,
    TagResourceInput as SecretsManagerTagResourceInput,
    TagResourceOutput as SecretsManagerTagResourceOutput,
    UntagResourceInput as SecretsManagerUntagResourceInput,
    UntagResourceOutput as SecretsManagerUntagResourceOutput,
    UpdateSecretInput as SecretsManagerUpdateSecretInput,
    UpdateSecretOutput as SecretsManagerUpdateSecretOutput,
};
pub use sns::{
    ConfirmationDelivery, CreateTopicInput, DeliveryEndpoint,
    ListedSubscription, MessageAttributeValue, NotificationPayload,
    ParsedHttpEndpoint, PlannedDelivery, PublishBatchEntryInput,
    PublishBatchFailure, PublishBatchOutput, PublishBatchSuccess,
    PublishInput, PublishOutput, SequentialSnsIdentifierSource,
    SnsDeliveryTransport, SnsError, SnsIdentifierSource, SnsScope, SnsService,
    SubscribeInput, SubscribeOutput, SubscriptionProtocol, SubscriptionState,
};
pub use sqs::request_runtime::SqsRequestRuntime;
pub use sqs::{
    AddPermissionInput as SqsAddPermissionInput, BatchFailure,
    CancelMessageMoveTaskInput, CancelMessageMoveTaskOutput,
    ChangeMessageVisibilityBatchEntryInput,
    ChangeMessageVisibilityBatchOutput, ChangeMessageVisibilityBatchSuccess,
    CreateQueueInput, DeleteMessageBatchEntryInput, DeleteMessageBatchOutput,
    DeleteMessageBatchSuccess, ListDeadLetterSourceQueuesInput,
    ListMessageMoveTasksInput, ListMessageMoveTasksOutput,
    ListMessageMoveTasksResultEntry, ListQueuesInput,
    MessageAttributeValue as SqsMessageAttributeValue,
    PaginatedDeadLetterSourceQueues, PaginatedQueues, ReceiveMessageInput,
    ReceivedMessage, RemovePermissionInput as SqsRemovePermissionInput,
    SendMessageBatchEntryInput, SendMessageBatchOutput,
    SendMessageBatchSuccess, SendMessageInput, SendMessageOutput,
    SequentialSqsIdentifierSource, SqsError, SqsIdentifierSource,
    SqsQueueIdentity, SqsScope, SqsService, StartMessageMoveTaskInput,
    StartMessageMoveTaskOutput,
};
pub use ssm::{
    ParameterName, ParameterPath, ParameterReference,
    SsmAddTagsToResourceInput, SsmAddTagsToResourceOutput,
    SsmDeleteParameterInput, SsmDeleteParameterOutput,
    SsmDeleteParametersInput, SsmDeleteParametersOutput, SsmDescribeFilter,
    SsmDescribeParametersInput, SsmDescribeParametersOutput, SsmError,
    SsmGetParameterHistoryInput, SsmGetParameterHistoryOutput,
    SsmGetParameterInput, SsmGetParametersByPathInput,
    SsmGetParametersByPathOutput, SsmGetParametersInput,
    SsmGetParametersOutput, SsmLabelParameterVersionInput,
    SsmLabelParameterVersionOutput, SsmListTagsForResourceInput,
    SsmListTagsForResourceOutput, SsmParameter, SsmParameterHistoryEntry,
    SsmParameterMetadata, SsmParameterType, SsmPutParameterInput,
    SsmPutParameterOutput, SsmRemoveTagsFromResourceInput,
    SsmRemoveTagsFromResourceOutput, SsmResourceType, SsmScope, SsmService,
    SsmTag,
};
pub use step_functions::{
    CreateStateMachineInput, CreateStateMachineOutput, DescribeExecutionInput,
    DescribeExecutionOutput, DescribeStateMachineOutput, ExecutionListItem,
    ExecutionStatus, GetExecutionHistoryInput, GetExecutionHistoryOutput,
    HistoryEvent, ListExecutionsInput, ListExecutionsOutput,
    ListStateMachinesInput, ListStateMachinesOutput, StartExecutionInput,
    StartExecutionOutput, StateMachineListItem, StateMachineStatus,
    StateMachineType, StepFunctionsError, StepFunctionsExecutionSpawner,
    StepFunctionsScope, StepFunctionsService,
    StepFunctionsServiceDependencies, StepFunctionsSleeper,
    StepFunctionsTaskAdapter, StopExecutionInput, StopExecutionOutput,
    TaskInvocationFailure, TaskInvocationRequest, TaskInvocationResult,
};
pub use sts::{
    AssumeRoleInput, AssumeRoleOutput, AssumeRoleWithSamlInput,
    AssumeRoleWithSamlOutput, AssumeRoleWithWebIdentityInput,
    AssumeRoleWithWebIdentityOutput, AssumedRoleUser, CallerIdentityOutput,
    FederatedUser, GetFederationTokenInput, GetFederationTokenOutput,
    GetSessionTokenInput, SessionCredentials, StsCaller, StsError, StsService,
};

const EVENTBRIDGE_DELIVERY_DRAIN_TIMEOUT: Duration =
    Duration::from_millis(250);
const EVENTBRIDGE_DELIVERY_ABORT_TIMEOUT: Duration =
    Duration::from_millis(250);

#[derive(Clone)]
pub struct EventBridgeDeliveryShutdown {
    begin: Arc<dyn Fn() + Send + Sync>,
    request_hard_stop: Arc<dyn Fn() + Send + Sync>,
    finish: Arc<
        dyn Fn(
                Duration,
            )
                -> Result<ThreadWorkQueueShutdownOutcome, InfrastructureError>
            + Send
            + Sync,
    >,
}

impl EventBridgeDeliveryShutdown {
    pub(crate) fn new(
        begin: Arc<dyn Fn() + Send + Sync>,
        request_hard_stop: Arc<dyn Fn() + Send + Sync>,
        finish: Arc<
            dyn Fn(
                    Duration,
                ) -> Result<
                    ThreadWorkQueueShutdownOutcome,
                    InfrastructureError,
                > + Send
                + Sync,
        >,
    ) -> Self {
        Self { begin, request_hard_stop, finish }
    }

    fn begin(&self) {
        (self.begin)();
    }

    fn request_hard_stop(&self) {
        (self.request_hard_stop)();
    }

    fn finish(
        &self,
        timeout: Duration,
    ) -> Result<ThreadWorkQueueShutdownOutcome, InfrastructureError> {
        (self.finish)(timeout)
    }
}

#[derive(Clone)]
pub struct RuntimeServices {
    apigateway: ApiGatewayService,
    execute_api_executor: Arc<dyn ExecuteApiIntegrationExecutor>,
    cloudformation: CloudFormationService,
    cloudwatch: CloudWatchService,
    cognito: CognitoService,
    dynamodb: DynamoDbService,
    elasticache: ElastiCacheService,
    eventbridge: EventBridgeService,
    eventbridge_delivery_shutdown: Option<EventBridgeDeliveryShutdown>,
    iam: IamService,
    kinesis: KinesisService,
    kms: KmsService,
    lambda: LambdaService,
    lambda_background_shutdown: Option<LambdaBackgroundShutdownHook>,
    rds: RdsService,
    s3: S3Service,
    secrets_manager: SecretsManagerService,
    sns: SnsService,
    sqs: SqsService,
    ssm: SsmService,
    storage_shutdown: Option<StorageShutdownHook>,
    sts: StsService,
    step_functions: StepFunctionsService,
}

pub struct RuntimeServicesBuilder {
    pub apigateway: ApiGatewayService,
    pub execute_api_executor: Arc<dyn ExecuteApiIntegrationExecutor>,
    pub cloudformation: CloudFormationService,
    pub cloudwatch: CloudWatchService,
    pub cognito: CognitoService,
    pub dynamodb: DynamoDbService,
    pub elasticache: ElastiCacheService,
    pub eventbridge: EventBridgeService,
    pub eventbridge_delivery_shutdown: Option<EventBridgeDeliveryShutdown>,
    pub iam: IamService,
    pub kinesis: KinesisService,
    pub kms: KmsService,
    pub lambda: LambdaService,
    pub lambda_background_shutdown: Option<LambdaBackgroundShutdownHook>,
    pub rds: RdsService,
    pub s3: S3Service,
    pub secrets_manager: SecretsManagerService,
    pub sns: SnsService,
    pub sqs: SqsService,
    pub ssm: SsmService,
    pub storage_shutdown: Option<StorageShutdownHook>,
    pub sts: StsService,
    pub step_functions: StepFunctionsService,
}

impl RuntimeServicesBuilder {
    pub fn build(self) -> RuntimeServices {
        RuntimeServices::new(self)
    }
}

impl RuntimeServices {
    pub fn new(dependencies: RuntimeServicesBuilder) -> Self {
        Self {
            apigateway: dependencies.apigateway,
            execute_api_executor: dependencies.execute_api_executor,
            cloudformation: dependencies.cloudformation,
            cloudwatch: dependencies.cloudwatch,
            cognito: dependencies.cognito,
            dynamodb: dependencies.dynamodb,
            elasticache: dependencies.elasticache,
            eventbridge: dependencies.eventbridge,
            eventbridge_delivery_shutdown: dependencies
                .eventbridge_delivery_shutdown,
            iam: dependencies.iam,
            kinesis: dependencies.kinesis,
            kms: dependencies.kms,
            lambda: dependencies.lambda,
            lambda_background_shutdown: dependencies
                .lambda_background_shutdown,
            rds: dependencies.rds,
            s3: dependencies.s3,
            secrets_manager: dependencies.secrets_manager,
            sns: dependencies.sns,
            sqs: dependencies.sqs,
            ssm: dependencies.ssm,
            storage_shutdown: dependencies.storage_shutdown,
            sts: dependencies.sts,
            step_functions: dependencies.step_functions,
        }
    }

    pub fn begin_shutdown(&self) {
        if let Some(eventbridge_delivery_shutdown) =
            &self.eventbridge_delivery_shutdown
        {
            eventbridge_delivery_shutdown.begin();
        }
    }

    pub fn shutdown(&self) -> Vec<ShutdownWarning> {
        let mut warnings = Vec::new();
        self.begin_shutdown();
        if let Err(error) = self.eventbridge.shutdown() {
            warnings.push(ShutdownWarning::new("eventbridge", error));
        }
        if let Some(eventbridge_delivery_shutdown) =
            &self.eventbridge_delivery_shutdown
        {
            let drained = matches!(
                eventbridge_delivery_shutdown
                    .finish(EVENTBRIDGE_DELIVERY_DRAIN_TIMEOUT),
                Ok(ThreadWorkQueueShutdownOutcome::Drained)
            );
            if !drained {
                eventbridge_delivery_shutdown.request_hard_stop();
                if let Err(error) = eventbridge_delivery_shutdown
                    .finish(EVENTBRIDGE_DELIVERY_ABORT_TIMEOUT)
                {
                    warnings.push(ShutdownWarning::new(
                        "eventbridge-delivery",
                        error,
                    ));
                }
            }
        }
        if let Some(lambda_background_shutdown) =
            &self.lambda_background_shutdown
            && let Err(error) = lambda_background_shutdown()
        {
            warnings.push(ShutdownWarning::new("lambda-background", error));
        }
        if let Err(error) = self.rds.shutdown() {
            warnings.push(ShutdownWarning::new("rds", error));
        }
        if let Err(error) = self.elasticache.shutdown() {
            warnings.push(ShutdownWarning::new("elasticache", error));
        }
        if let Some(storage_shutdown) = &self.storage_shutdown
            && let Err(error) = storage_shutdown()
        {
            warnings.push(ShutdownWarning::new("storage", error));
        }
        warnings
    }
    pub fn apigateway(&self) -> &ApiGatewayService {
        &self.apigateway
    }
    pub fn execute_api_executor(
        &self,
    ) -> &(dyn ExecuteApiIntegrationExecutor + Send + Sync) {
        self.execute_api_executor.as_ref()
    }
    pub fn cloudformation(&self) -> &CloudFormationService {
        &self.cloudformation
    }
    pub fn cloudwatch(&self) -> &CloudWatchService {
        &self.cloudwatch
    }
    pub fn cognito(&self) -> &CognitoService {
        &self.cognito
    }
    pub fn dynamodb(&self) -> &DynamoDbService {
        &self.dynamodb
    }
    pub fn elasticache(&self) -> &ElastiCacheService {
        &self.elasticache
    }
    pub fn eventbridge(&self) -> &EventBridgeService {
        &self.eventbridge
    }
    #[cfg(test)]
    pub(crate) fn has_eventbridge_delivery_shutdown(&self) -> bool {
        self.eventbridge_delivery_shutdown.is_some()
    }

    pub fn iam(&self) -> &IamService {
        &self.iam
    }
    pub fn kinesis(&self) -> &KinesisService {
        &self.kinesis
    }
    pub fn kms(&self) -> &KmsService {
        &self.kms
    }
    pub fn lambda(&self) -> &LambdaService {
        &self.lambda
    }
    #[cfg(test)]
    pub(crate) fn has_lambda_background_shutdown(&self) -> bool {
        self.lambda_background_shutdown.is_some()
    }
    pub fn lambda_requests(&self) -> LambdaRequestRuntime {
        LambdaRequestRuntime::new(self.lambda.clone())
    }
    pub fn rds(&self) -> &RdsService {
        &self.rds
    }
    pub fn s3(&self) -> &S3Service {
        &self.s3
    }
    pub fn secrets_manager(&self) -> &SecretsManagerService {
        &self.secrets_manager
    }
    pub fn sns(&self) -> &SnsService {
        &self.sns
    }
    pub fn sqs(&self) -> &SqsService {
        &self.sqs
    }
    pub fn sqs_requests(&self) -> SqsRequestRuntime {
        SqsRequestRuntime::new(self.sqs.clone())
    }
    pub fn ssm(&self) -> &SsmService {
        &self.ssm
    }
    pub fn step_functions(&self) -> &StepFunctionsService {
        &self.step_functions
    }

    pub fn sts(&self) -> &StsService {
        &self.sts
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EventBridgeDeliveryShutdown, RuntimeServicesBuilder,
        ThreadWorkQueueShutdownOutcome,
    };
    use crate::{
        ApiGatewayIntegrationExecutor, CreateQueueInput, EventBridgeScope,
        IamScope, PutEventsInput, PutEventsRequestEntry, S3Scope, SqsScope,
        TestRuntimeBuilder,
    };
    use aws::InfrastructureError;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn runtime_services_builder_exposes_typed_service_accessors() {
        let runtime = TestRuntimeBuilder::new("runtime-services-builder")
            .build()
            .expect("test runtime should build")
            .into_parts();
        let rebuilt = RuntimeServicesBuilder {
            apigateway: runtime.apigateway().clone(),
            execute_api_executor: Arc::new(
                ApiGatewayIntegrationExecutor::new(
                    Some(runtime.lambda_requests()),
                    None,
                ),
            ),
            cloudformation: runtime.cloudformation().clone(),
            cloudwatch: runtime.cloudwatch().clone(),
            cognito: runtime.cognito().clone(),
            dynamodb: runtime.dynamodb().clone(),
            elasticache: runtime.elasticache().clone(),
            eventbridge: runtime.eventbridge().clone(),
            eventbridge_delivery_shutdown: None,
            iam: runtime.iam().clone(),
            kinesis: runtime.kinesis().clone(),
            kms: runtime.kms().clone(),
            lambda: runtime.lambda().clone(),
            lambda_background_shutdown: None,
            rds: runtime.rds().clone(),
            s3: runtime.s3().clone(),
            secrets_manager: runtime.secrets_manager().clone(),
            sns: runtime.sns().clone(),
            sqs: runtime.sqs().clone(),
            ssm: runtime.ssm().clone(),
            storage_shutdown: None,
            sts: runtime.sts().clone(),
            step_functions: runtime.step_functions().clone(),
        }
        .build();
        let iam_scope = IamScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let s3_scope = S3Scope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let sqs_scope = SqsScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let eventbridge_scope = EventBridgeScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );

        assert!(rebuilt.iam().list_users(&iam_scope, None).is_empty());
        assert!(rebuilt.s3().list_buckets(&s3_scope).buckets.is_empty());
        assert!(rebuilt.sqs().list_queues(&sqs_scope, None).is_empty());

        rebuilt
            .sqs()
            .create_queue(
                &sqs_scope,
                CreateQueueInput {
                    attributes: BTreeMap::new(),
                    queue_name: "builder-queue".to_owned(),
                },
            )
            .expect("queue should be creatable through rebuilt runtime");
        assert_eq!(rebuilt.sqs().list_queues(&sqs_scope, None).len(), 1);

        rebuilt
            .eventbridge()
            .put_events(
                &eventbridge_scope,
                PutEventsInput {
                    entries: vec![PutEventsRequestEntry {
                        detail: "{\"kind\":\"ping\"}".to_owned(),
                        detail_type: "Created".to_owned(),
                        event_bus_name: None,
                        resources: Vec::new(),
                        source: "orders".to_owned(),
                        time: None,
                        trace_header: None,
                    }],
                },
            )
            .expect(
                "eventbridge should accept events through rebuilt runtime",
            );
    }

    #[test]
    fn runtime_services_shutdown_runs_storage_hook_once() {
        let runtime =
            TestRuntimeBuilder::new("runtime-services-storage-shutdown")
                .build()
                .expect("test runtime should build")
                .into_parts();
        let shutdown_calls = Arc::new(AtomicUsize::new(0));
        let shutdown_calls_for_hook = Arc::clone(&shutdown_calls);
        let rebuilt = RuntimeServicesBuilder {
            apigateway: runtime.apigateway().clone(),
            execute_api_executor: Arc::new(
                ApiGatewayIntegrationExecutor::new(
                    Some(runtime.lambda_requests()),
                    None,
                ),
            ),
            cloudformation: runtime.cloudformation().clone(),
            cloudwatch: runtime.cloudwatch().clone(),
            cognito: runtime.cognito().clone(),
            dynamodb: runtime.dynamodb().clone(),
            elasticache: runtime.elasticache().clone(),
            eventbridge: runtime.eventbridge().clone(),
            eventbridge_delivery_shutdown: None,
            iam: runtime.iam().clone(),
            kinesis: runtime.kinesis().clone(),
            kms: runtime.kms().clone(),
            lambda: runtime.lambda().clone(),
            lambda_background_shutdown: None,
            rds: runtime.rds().clone(),
            s3: runtime.s3().clone(),
            secrets_manager: runtime.secrets_manager().clone(),
            sns: runtime.sns().clone(),
            sqs: runtime.sqs().clone(),
            ssm: runtime.ssm().clone(),
            storage_shutdown: Some(Arc::new(move || {
                shutdown_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                Ok::<(), storage::StorageError>(())
            })),
            sts: runtime.sts().clone(),
            step_functions: runtime.step_functions().clone(),
        }
        .build();

        rebuilt.shutdown();

        assert_eq!(shutdown_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_services_shutdown_runs_eventbridge_delivery_hook_once() {
        let runtime =
            TestRuntimeBuilder::new("runtime-services-eventbridge-shutdown")
                .build()
                .expect("test runtime should build")
                .into_parts();
        let begin_calls = Arc::new(AtomicUsize::new(0));
        let begin_calls_for_hook = Arc::clone(&begin_calls);
        let hard_stop_calls = Arc::new(AtomicUsize::new(0));
        let hard_stop_calls_for_hook = Arc::clone(&hard_stop_calls);
        let finish_calls = Arc::new(AtomicUsize::new(0));
        let finish_calls_for_hook = Arc::clone(&finish_calls);
        let rebuilt =
            RuntimeServicesBuilder {
                apigateway: runtime.apigateway().clone(),
                execute_api_executor: Arc::new(
                    ApiGatewayIntegrationExecutor::new(
                        Some(runtime.lambda_requests()),
                        None,
                    ),
                ),
                cloudformation: runtime.cloudformation().clone(),
                cloudwatch: runtime.cloudwatch().clone(),
                cognito: runtime.cognito().clone(),
                dynamodb: runtime.dynamodb().clone(),
                elasticache: runtime.elasticache().clone(),
                eventbridge: runtime.eventbridge().clone(),
                eventbridge_delivery_shutdown: Some(
                    EventBridgeDeliveryShutdown::new(
                        Arc::new(move || {
                            begin_calls_for_hook
                                .fetch_add(1, Ordering::SeqCst);
                        }),
                        Arc::new(move || {
                            hard_stop_calls_for_hook
                                .fetch_add(1, Ordering::SeqCst);
                        }),
                        Arc::new(move |timeout| {
                            assert_eq!(timeout, Duration::from_millis(250));
                            finish_calls_for_hook
                                .fetch_add(1, Ordering::SeqCst);
                            Ok::<
                                ThreadWorkQueueShutdownOutcome,
                                InfrastructureError,
                            >(
                                ThreadWorkQueueShutdownOutcome::Drained
                            )
                        }),
                    ),
                ),
                iam: runtime.iam().clone(),
                kinesis: runtime.kinesis().clone(),
                kms: runtime.kms().clone(),
                lambda: runtime.lambda().clone(),
                lambda_background_shutdown: None,
                rds: runtime.rds().clone(),
                s3: runtime.s3().clone(),
                secrets_manager: runtime.secrets_manager().clone(),
                sns: runtime.sns().clone(),
                sqs: runtime.sqs().clone(),
                ssm: runtime.ssm().clone(),
                storage_shutdown: None,
                sts: runtime.sts().clone(),
                step_functions: runtime.step_functions().clone(),
            }
            .build();

        rebuilt.shutdown();

        assert_eq!(begin_calls.load(Ordering::SeqCst), 1);
        assert_eq!(hard_stop_calls.load(Ordering::SeqCst), 0);
        assert_eq!(finish_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_services_shutdown_requests_hard_stop_after_timeout() {
        let runtime =
            TestRuntimeBuilder::new("runtime-services-eventbridge-hard-stop")
                .build()
                .expect("test runtime should build")
                .into_parts();
        let begin_calls = Arc::new(AtomicUsize::new(0));
        let begin_calls_for_hook = Arc::clone(&begin_calls);
        let hard_stop_calls = Arc::new(AtomicUsize::new(0));
        let hard_stop_calls_for_hook = Arc::clone(&hard_stop_calls);
        let finish_calls = Arc::new(AtomicUsize::new(0));
        let finish_calls_for_hook = Arc::clone(&finish_calls);
        let rebuilt =
            RuntimeServicesBuilder {
                apigateway: runtime.apigateway().clone(),
                execute_api_executor: Arc::new(
                    ApiGatewayIntegrationExecutor::new(
                        Some(runtime.lambda_requests()),
                        None,
                    ),
                ),
                cloudformation: runtime.cloudformation().clone(),
                cloudwatch: runtime.cloudwatch().clone(),
                cognito: runtime.cognito().clone(),
                dynamodb: runtime.dynamodb().clone(),
                elasticache: runtime.elasticache().clone(),
                eventbridge: runtime.eventbridge().clone(),
                eventbridge_delivery_shutdown: Some(
                    EventBridgeDeliveryShutdown::new(
                        Arc::new(move || {
                            begin_calls_for_hook
                                .fetch_add(1, Ordering::SeqCst);
                        }),
                        Arc::new(move || {
                            hard_stop_calls_for_hook
                                .fetch_add(1, Ordering::SeqCst);
                        }),
                        Arc::new(move |_timeout| {
                            let call = finish_calls_for_hook
                                .fetch_add(1, Ordering::SeqCst);
                            Ok::<
                                ThreadWorkQueueShutdownOutcome,
                                InfrastructureError,
                            >(if call == 0 {
                                ThreadWorkQueueShutdownOutcome::TimedOut
                            } else {
                                ThreadWorkQueueShutdownOutcome::Drained
                            })
                        }),
                    ),
                ),
                iam: runtime.iam().clone(),
                kinesis: runtime.kinesis().clone(),
                kms: runtime.kms().clone(),
                lambda: runtime.lambda().clone(),
                lambda_background_shutdown: None,
                rds: runtime.rds().clone(),
                s3: runtime.s3().clone(),
                secrets_manager: runtime.secrets_manager().clone(),
                sns: runtime.sns().clone(),
                sqs: runtime.sqs().clone(),
                ssm: runtime.ssm().clone(),
                storage_shutdown: None,
                sts: runtime.sts().clone(),
                step_functions: runtime.step_functions().clone(),
            }
            .build();

        rebuilt.shutdown();

        assert_eq!(begin_calls.load(Ordering::SeqCst), 1);
        assert_eq!(hard_stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(finish_calls.load(Ordering::SeqCst), 2);
    }
}
