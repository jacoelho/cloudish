use aws::{RuntimeDefaults, ServiceName};
use std::collections::BTreeSet;
use std::sync::Arc;
use storage::StorageError;

type StorageShutdownHook =
    Arc<dyn Fn() -> Result<(), StorageError> + Send + Sync>;

#[cfg(feature = "apigateway")]
pub use apigateway::{
    ApiGatewayError, ApiGatewayScope, ApiGatewayService, ApiKey, ApiMethod,
    ApiResource, Authorizer, BasePathMapping, CreateApiKeyInput,
    CreateAuthorizerInput, CreateBasePathMappingInput, CreateDeploymentInput,
    CreateDomainNameInput, CreateHttpApiAuthorizerInput,
    CreateHttpApiDeploymentInput, CreateHttpApiInput,
    CreateHttpApiIntegrationInput, CreateHttpApiRouteInput,
    CreateHttpApiStageInput, CreateRequestValidatorInput, CreateResourceInput,
    CreateRestApiInput, CreateStageInput, CreateUsagePlanInput,
    CreateUsagePlanKeyInput, Deployment, DomainName, EndpointConfiguration,
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
#[cfg(feature = "cloudformation")]
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
#[cfg(feature = "cloudwatch")]
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
#[cfg(feature = "cognito")]
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
#[cfg(feature = "dynamodb")]
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
#[cfg(feature = "elasticache")]
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
#[cfg(feature = "eventbridge")]
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
#[cfg(feature = "kinesis")]
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
#[cfg(feature = "kms")]
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
#[cfg(feature = "lambda")]
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
#[cfg(feature = "rds")]
pub use rds::{
    CreateDbClusterInput, CreateDbInstanceInput, CreateDbParameterGroupInput,
    DbCluster, DbClusterMember, DbInstance, DbLifecycleStatus, DbParameter,
    DbParameterGroup, ModifyDbClusterInput, ModifyDbInstanceInput,
    ModifyDbParameterGroupInput, RdsAuthEndpointSource, RdsBackendRuntime,
    RdsBackendSpec, RdsEndpoint, RdsEngine, RdsError, RdsIamTokenValidator,
    RdsScope, RdsService, RdsServiceDependencies,
    RejectAllRdsIamTokenValidator, RunningRdsBackend,
};
#[cfg(feature = "s3")]
pub use s3::{
    BucketNotificationConfiguration, BucketObjectLockConfiguration,
    BucketTaggingOutput, BucketVersioningOutput, BucketVersioningStatus,
    CannedAcl, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CompletedMultipartPart, CopyObjectInput, CopyObjectOutput,
    CreateBucketInput, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, CsvFileHeaderInfo, CsvInputSerialization,
    CsvOutputSerialization, DefaultObjectLockRetention,
    DefaultRetentionPeriod, DeleteObjectInput, DeleteObjectOutput,
    GetBucketLocationOutput, GetObjectOutput, HeadObjectOutput,
    LegalHoldStatus, ListBucketsOutput, ListBucketsPage,
    ListMultipartUploadsOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListedBucket, ListedDeleteMarker,
    ListedObject, ListedObjectVersion, ListedVersionEntry,
    MultipartUploadSummary, NotificationFilter, ObjectLegalHoldOutput,
    ObjectLockMode, ObjectLockWriteOptions, ObjectRetention,
    ObjectTaggingOutput, PutObjectInput, PutObjectLegalHoldInput,
    PutObjectOutput, PutObjectRetentionInput, QueueNotificationConfiguration,
    S3Error, S3EventNotification, S3InitError, S3NotificationTransport,
    S3Scope, S3Service, SelectObjectContentInput, SelectObjectContentOutput,
    SelectObjectStats, StoredBucketAclInput, TaggingInput,
    TopicNotificationConfiguration, UploadPartInput, UploadPartOutput,
};
#[cfg(feature = "secrets-manager")]
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
#[cfg(feature = "sns")]
pub use sns::{
    ConfirmationDelivery, CreateTopicInput, DeliveryEndpoint,
    ListedSubscription, MessageAttributeValue, NotificationPayload,
    ParsedHttpEndpoint, PlannedDelivery, PublishBatchEntryInput,
    PublishBatchFailure, PublishBatchOutput, PublishBatchSuccess,
    PublishInput, PublishOutput, SequentialSnsIdentifierSource,
    SnsDeliveryTransport, SnsError, SnsIdentifierSource, SnsScope, SnsService,
    SubscribeInput, SubscribeOutput, SubscriptionProtocol, SubscriptionState,
};
#[cfg(feature = "sqs")]
pub use sqs::{
    BatchFailure, ChangeMessageVisibilityBatchEntryInput,
    ChangeMessageVisibilityBatchOutput, ChangeMessageVisibilityBatchSuccess,
    CreateQueueInput, DeleteMessageBatchEntryInput, DeleteMessageBatchOutput,
    DeleteMessageBatchSuccess, ReceiveMessageInput, ReceivedMessage,
    SendMessageBatchEntryInput, SendMessageBatchOutput,
    SendMessageBatchSuccess, SendMessageInput, SendMessageOutput,
    SequentialSqsIdentifierSource, SqsError, SqsIdentifierSource,
    SqsQueueIdentity, SqsScope, SqsService, StartMessageMoveTaskInput,
    StartMessageMoveTaskOutput,
};
#[cfg(feature = "ssm")]
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
#[cfg(feature = "step-functions")]
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

const SUPPORTED_SERVICES: &[ServiceName] = &[
    #[cfg(feature = "apigateway")]
    ServiceName::ApiGateway,
    #[cfg(feature = "cloudformation")]
    ServiceName::CloudFormation,
    #[cfg(feature = "cloudwatch")]
    ServiceName::CloudWatch,
    #[cfg(feature = "cognito")]
    ServiceName::CognitoIdentityProvider,
    #[cfg(feature = "dynamodb")]
    ServiceName::DynamoDb,
    #[cfg(feature = "elasticache")]
    ServiceName::ElastiCache,
    #[cfg(feature = "eventbridge")]
    ServiceName::EventBridge,
    ServiceName::Iam,
    #[cfg(feature = "kinesis")]
    ServiceName::Kinesis,
    #[cfg(feature = "kms")]
    ServiceName::Kms,
    #[cfg(feature = "lambda")]
    ServiceName::Lambda,
    #[cfg(feature = "cloudwatch")]
    ServiceName::Logs,
    #[cfg(feature = "rds")]
    ServiceName::Rds,
    #[cfg(feature = "s3")]
    ServiceName::S3,
    #[cfg(feature = "secrets-manager")]
    ServiceName::SecretsManager,
    #[cfg(feature = "sns")]
    ServiceName::Sns,
    #[cfg(feature = "sqs")]
    ServiceName::Sqs,
    #[cfg(feature = "ssm")]
    ServiceName::Ssm,
    #[cfg(feature = "step-functions")]
    ServiceName::StepFunctions,
    ServiceName::Sts,
];

#[derive(Debug, Clone)]
pub struct EnabledServices {
    enabled_services: BTreeSet<ServiceName>,
}

impl EnabledServices {
    pub fn all() -> Self {
        Self::from_enabled_services(supported_services().iter().copied())
    }

    pub fn from_enabled_services<I>(enabled_services: I) -> Self
    where
        I: IntoIterator<Item = ServiceName>,
    {
        Self { enabled_services: enabled_services.into_iter().collect() }
    }

    pub fn bootstrap(_defaults: &RuntimeDefaults) -> Self {
        Self::all()
    }

    pub fn with_enabled_services<I>(
        _defaults: &RuntimeDefaults,
        enabled_services: I,
    ) -> Self
    where
        I: IntoIterator<Item = ServiceName>,
    {
        Self::from_enabled_services(enabled_services)
    }

    pub fn enabled_service_count(&self) -> usize {
        self.enabled_services.len()
    }

    pub fn enabled_services(&self) -> Vec<ServiceName> {
        self.enabled_services.iter().copied().collect()
    }

    pub fn is_enabled(&self, service: ServiceName) -> bool {
        self.enabled_services.contains(&service)
    }
}

pub fn supported_services() -> &'static [ServiceName] {
    SUPPORTED_SERVICES
}

#[derive(Clone)]
pub struct RuntimeServices {
    #[cfg(feature = "apigateway")]
    apigateway: ApiGatewayService,
    #[cfg(feature = "apigateway")]
    execute_api_executor: Arc<dyn ExecuteApiIntegrationExecutor>,
    #[cfg(feature = "cloudformation")]
    cloudformation: CloudFormationService,
    #[cfg(feature = "cloudwatch")]
    cloudwatch: CloudWatchService,
    #[cfg(feature = "cognito")]
    cognito: CognitoService,
    #[cfg(feature = "dynamodb")]
    dynamodb: DynamoDbService,
    #[cfg(feature = "elasticache")]
    elasticache: ElastiCacheService,
    #[cfg(feature = "eventbridge")]
    eventbridge: EventBridgeService,
    iam: IamService,
    #[cfg(feature = "kinesis")]
    kinesis: KinesisService,
    #[cfg(feature = "kms")]
    kms: KmsService,
    #[cfg(feature = "lambda")]
    lambda: LambdaService,
    #[cfg(feature = "rds")]
    rds: RdsService,
    #[cfg(feature = "s3")]
    s3: S3Service,
    #[cfg(feature = "secrets-manager")]
    secrets_manager: SecretsManagerService,
    #[cfg(feature = "sns")]
    sns: SnsService,
    #[cfg(feature = "sqs")]
    sqs: SqsService,
    #[cfg(feature = "ssm")]
    ssm: SsmService,
    storage_shutdown: Option<StorageShutdownHook>,
    sts: StsService,
    #[cfg(feature = "step-functions")]
    step_functions: StepFunctionsService,
}

pub struct RuntimeServicesBuilder {
    #[cfg(feature = "apigateway")]
    pub apigateway: ApiGatewayService,
    #[cfg(feature = "apigateway")]
    pub execute_api_executor: Arc<dyn ExecuteApiIntegrationExecutor>,
    #[cfg(feature = "cloudformation")]
    pub cloudformation: CloudFormationService,
    #[cfg(feature = "cloudwatch")]
    pub cloudwatch: CloudWatchService,
    #[cfg(feature = "cognito")]
    pub cognito: CognitoService,
    #[cfg(feature = "dynamodb")]
    pub dynamodb: DynamoDbService,
    #[cfg(feature = "elasticache")]
    pub elasticache: ElastiCacheService,
    #[cfg(feature = "eventbridge")]
    pub eventbridge: EventBridgeService,
    pub iam: IamService,
    #[cfg(feature = "kinesis")]
    pub kinesis: KinesisService,
    #[cfg(feature = "kms")]
    pub kms: KmsService,
    #[cfg(feature = "lambda")]
    pub lambda: LambdaService,
    #[cfg(feature = "rds")]
    pub rds: RdsService,
    #[cfg(feature = "s3")]
    pub s3: S3Service,
    #[cfg(feature = "secrets-manager")]
    pub secrets_manager: SecretsManagerService,
    #[cfg(feature = "sns")]
    pub sns: SnsService,
    #[cfg(feature = "sqs")]
    pub sqs: SqsService,
    #[cfg(feature = "ssm")]
    pub ssm: SsmService,
    pub storage_shutdown: Option<StorageShutdownHook>,
    pub sts: StsService,
    #[cfg(feature = "step-functions")]
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
            #[cfg(feature = "apigateway")]
            apigateway: dependencies.apigateway,
            #[cfg(feature = "apigateway")]
            execute_api_executor: dependencies.execute_api_executor,
            #[cfg(feature = "cloudformation")]
            cloudformation: dependencies.cloudformation,
            #[cfg(feature = "cloudwatch")]
            cloudwatch: dependencies.cloudwatch,
            #[cfg(feature = "cognito")]
            cognito: dependencies.cognito,
            #[cfg(feature = "dynamodb")]
            dynamodb: dependencies.dynamodb,
            #[cfg(feature = "elasticache")]
            elasticache: dependencies.elasticache,
            #[cfg(feature = "eventbridge")]
            eventbridge: dependencies.eventbridge,
            iam: dependencies.iam,
            #[cfg(feature = "kinesis")]
            kinesis: dependencies.kinesis,
            #[cfg(feature = "kms")]
            kms: dependencies.kms,
            #[cfg(feature = "lambda")]
            lambda: dependencies.lambda,
            #[cfg(feature = "rds")]
            rds: dependencies.rds,
            #[cfg(feature = "s3")]
            s3: dependencies.s3,
            #[cfg(feature = "secrets-manager")]
            secrets_manager: dependencies.secrets_manager,
            #[cfg(feature = "sns")]
            sns: dependencies.sns,
            #[cfg(feature = "sqs")]
            sqs: dependencies.sqs,
            #[cfg(feature = "ssm")]
            ssm: dependencies.ssm,
            storage_shutdown: dependencies.storage_shutdown,
            sts: dependencies.sts,
            #[cfg(feature = "step-functions")]
            step_functions: dependencies.step_functions,
        }
    }

    pub fn shutdown(&self) {
        #[cfg(feature = "eventbridge")]
        let _ = self.eventbridge.shutdown();
        #[cfg(feature = "rds")]
        let _ = self.rds.shutdown();
        #[cfg(feature = "elasticache")]
        let _ = self.elasticache.shutdown();
        if let Some(storage_shutdown) = &self.storage_shutdown {
            let _ = storage_shutdown();
        }
    }

    #[cfg(feature = "apigateway")]
    pub fn apigateway(&self) -> &ApiGatewayService {
        &self.apigateway
    }

    #[cfg(feature = "apigateway")]
    pub fn execute_api_executor(
        &self,
    ) -> &(dyn ExecuteApiIntegrationExecutor + Send + Sync) {
        self.execute_api_executor.as_ref()
    }

    #[cfg(feature = "cloudformation")]
    pub fn cloudformation(&self) -> &CloudFormationService {
        &self.cloudformation
    }

    #[cfg(feature = "cloudwatch")]
    pub fn cloudwatch(&self) -> &CloudWatchService {
        &self.cloudwatch
    }

    #[cfg(feature = "cognito")]
    pub fn cognito(&self) -> &CognitoService {
        &self.cognito
    }

    #[cfg(feature = "dynamodb")]
    pub fn dynamodb(&self) -> &DynamoDbService {
        &self.dynamodb
    }

    #[cfg(feature = "elasticache")]
    pub fn elasticache(&self) -> &ElastiCacheService {
        &self.elasticache
    }

    #[cfg(feature = "eventbridge")]
    pub fn eventbridge(&self) -> &EventBridgeService {
        &self.eventbridge
    }

    pub fn iam(&self) -> &IamService {
        &self.iam
    }

    #[cfg(feature = "kinesis")]
    pub fn kinesis(&self) -> &KinesisService {
        &self.kinesis
    }

    #[cfg(feature = "kms")]
    pub fn kms(&self) -> &KmsService {
        &self.kms
    }

    #[cfg(feature = "lambda")]
    pub fn lambda(&self) -> &LambdaService {
        &self.lambda
    }

    #[cfg(feature = "rds")]
    pub fn rds(&self) -> &RdsService {
        &self.rds
    }

    #[cfg(feature = "s3")]
    pub fn s3(&self) -> &S3Service {
        &self.s3
    }

    #[cfg(feature = "secrets-manager")]
    pub fn secrets_manager(&self) -> &SecretsManagerService {
        &self.secrets_manager
    }

    #[cfg(feature = "sns")]
    pub fn sns(&self) -> &SnsService {
        &self.sns
    }

    #[cfg(feature = "sqs")]
    pub fn sqs(&self) -> &SqsService {
        &self.sqs
    }

    #[cfg(feature = "ssm")]
    pub fn ssm(&self) -> &SsmService {
        &self.ssm
    }

    #[cfg(feature = "step-functions")]
    pub fn step_functions(&self) -> &StepFunctionsService {
        &self.step_functions
    }

    pub fn sts(&self) -> &StsService {
        &self.sts
    }
}

#[cfg(all(test, feature = "all-services"))]
mod tests {
    use super::{EnabledServices, RuntimeServicesBuilder, supported_services};
    use crate::{
        ApiGatewayIntegrationExecutor, CreateQueueInput, EventBridgeScope,
        IamScope, PutEventsInput, PutEventsRequestEntry, S3Scope, SqsScope,
        TestRuntimeBuilder,
    };
    use aws::ServiceName;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn enabled_services_all_matches_supported_services() {
        let enabled = EnabledServices::all();

        assert_eq!(
            enabled.enabled_service_count(),
            supported_services().len()
        );
        for service in supported_services() {
            assert!(enabled.is_enabled(*service));
        }
    }

    #[test]
    fn enabled_services_filters_requested_membership() {
        let enabled = EnabledServices::from_enabled_services([
            ServiceName::Lambda,
            ServiceName::S3,
        ]);

        assert_eq!(enabled.enabled_service_count(), 2);
        assert!(enabled.is_enabled(ServiceName::Lambda));
        assert!(enabled.is_enabled(ServiceName::S3));
        assert!(!enabled.is_enabled(ServiceName::Sqs));
    }

    #[test]
    fn runtime_services_builder_exposes_typed_service_accessors() {
        let (_, runtime, _) =
            TestRuntimeBuilder::new("runtime-services-builder")
                .build()
                .expect("test runtime should build")
                .into_parts();
        let rebuilt = RuntimeServicesBuilder {
            apigateway: runtime.apigateway().clone(),
            execute_api_executor: Arc::new(
                ApiGatewayIntegrationExecutor::new(
                    runtime.lambda().clone(),
                    None,
                ),
            ),
            cloudformation: runtime.cloudformation().clone(),
            cloudwatch: runtime.cloudwatch().clone(),
            cognito: runtime.cognito().clone(),
            dynamodb: runtime.dynamodb().clone(),
            elasticache: runtime.elasticache().clone(),
            eventbridge: runtime.eventbridge().clone(),
            iam: runtime.iam().clone(),
            kinesis: runtime.kinesis().clone(),
            kms: runtime.kms().clone(),
            lambda: runtime.lambda().clone(),
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
        let (_, runtime, _) =
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
                    runtime.lambda().clone(),
                    None,
                ),
            ),
            cloudformation: runtime.cloudformation().clone(),
            cloudwatch: runtime.cloudwatch().clone(),
            cognito: runtime.cognito().clone(),
            dynamodb: runtime.dynamodb().clone(),
            elasticache: runtime.elasticache().clone(),
            eventbridge: runtime.eventbridge().clone(),
            iam: runtime.iam().clone(),
            kinesis: runtime.kinesis().clone(),
            kms: runtime.kms().clone(),
            lambda: runtime.lambda().clone(),
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
}
