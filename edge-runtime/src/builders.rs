use crate::CloudWatchService;
use crate::CognitoService;
use crate::CreateRoleInput;
use crate::EventBridgeService;
use crate::IamScope;
use crate::IamService;
use crate::KinesisService;
use crate::KmsService;
use crate::SecretsManagerService;
use crate::SequentialSnsIdentifierSource;
use crate::SqsService;
use crate::SsmService;
use crate::StsService;
use crate::adapters::DirectoryBlobStore;
use crate::adapters::FixedClock;
use crate::adapters::OsRandomSource;
use crate::adapters::ProcessLambdaExecutor;
use crate::adapters::SystemClock;
use crate::adapters::TcpHttpForwarder;
use crate::adapters::ThreadBackgroundScheduler;
use crate::adapters::ThreadRdsBackendRuntime;
use crate::adapters::ThreadTcpProxyRuntime;
use crate::adapters::{MemoryBlobStore, SequenceRandomSource};
use crate::adapters::{
    ThreadElastiCacheNodeRuntime, ThreadElastiCacheProxyRuntime,
};
use crate::adapters::{
    ThreadStepFunctionsExecutionSpawner, ThreadStepFunctionsSleeper,
};
use crate::composition::ApiGatewayIntegrationExecutor;
use crate::composition::LambdaStepFunctionsTaskAdapter;
use crate::composition::S3NotificationDispatcher;
use crate::composition::build_eventbridge_dispatcher;
use crate::composition::start_lambda_background_tasks;
use crate::composition::{
    EventBridgeDispatcherAssembly, EventBridgeDispatcherDependencies,
};
use crate::composition::{SnsServiceDependencies, build_sns_service};
use crate::{ApiGatewayService, ExecuteApiIntegrationExecutor};
use crate::{CloudFormationDependencies, CloudFormationService};
use crate::{DynamoDbInitError, DynamoDbService};
use crate::{
    ElastiCacheError, ElastiCacheIamTokenValidator, ElastiCacheService,
    ElastiCacheServiceDependencies,
};
use crate::{
    EventBridgeDeliveryDispatcher, EventBridgeError,
    EventBridgeServiceDependencies, NoopEventBridgeDeliveryDispatcher,
};
use crate::{LambdaInitError, LambdaService, LambdaServiceDependencies};
use crate::{
    RdsError, RdsIamTokenValidator, RdsService, RdsServiceDependencies,
};
use crate::{RuntimeServices, RuntimeServicesBuilder};
use crate::{S3InitError, S3Service};
use crate::{StepFunctionsService, StepFunctionsServiceDependencies};
use auth::Authenticator;
use auth::{RequestAuth, RequestHeader};
use aws::Endpoint;
use aws::InfrastructureError;
use aws::{AccountId, RegionId};
use aws::{RuntimeDefaults, ServiceName, SharedAdvertisedEdge};
use elasticache::ElastiCacheReplicationGroupId;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use storage::{StorageConfig, StorageError, StorageFactory, StorageMode};
use thiserror::Error;

// Gate by capability ownership or the real transitive feature contract.
// Keep feature-local imports and flags close to the code that consumes them.
pub struct RuntimeAssembly {
    runtime_services: RuntimeServices,
}

impl RuntimeAssembly {
    pub fn new(runtime_services: RuntimeServices) -> Self {
        Self { runtime_services }
    }

    pub fn runtime_services(&self) -> &RuntimeServices {
        &self.runtime_services
    }

    pub fn into_parts(self) -> RuntimeServices {
        self.runtime_services
    }
}

#[derive(Debug, Error)]
pub enum RuntimeBuildError {
    #[error("invalid runtime configuration: {0}")]
    Configuration(String),
    #[error("failed to load DynamoDB state: {0}")]
    DynamoDbState(#[source] DynamoDbInitError),
    #[error("failed to restore EventBridge schedules: {0}")]
    EventBridgeRestore(#[source] EventBridgeError),
    #[error("failed to start EventBridge delivery runtime: {0}")]
    EventBridgeRuntime(#[source] InfrastructureError),
    #[error("failed to restore ElastiCache runtimes: {0}")]
    ElastiCacheRestore(#[source] ElastiCacheError),
    #[error("failed to start Lambda background runtime: {0}")]
    LambdaRuntime(#[source] InfrastructureError),
    #[error("failed to load Lambda state: {0}")]
    LambdaState(#[source] LambdaInitError),
    #[error("failed to load S3 state: {0}")]
    S3State(#[source] S3InitError),
    #[error("failed to restore RDS runtimes: {0}")]
    RdsRestore(#[source] RdsError),
    #[error("failed to load persistent runtime state: {0}")]
    StorageLoad(#[source] StorageError),
}

fn eventbridge_delivery_dispatcher(
    assembly: Option<&EventBridgeDispatcherAssembly>,
) -> Arc<dyn EventBridgeDeliveryDispatcher> {
    assembly.map(|assembly| assembly.dispatcher.clone()).unwrap_or_else(|| {
        Arc::new(NoopEventBridgeDeliveryDispatcher)
            as Arc<dyn EventBridgeDeliveryDispatcher>
    })
}

pub struct LocalRuntimeBuilder {
    advertised_edge: SharedAdvertisedEdge,
    authenticator: Authenticator,
    defaults: RuntimeDefaults,
}

impl LocalRuntimeBuilder {
    pub fn new(
        defaults: RuntimeDefaults,
        authenticator: Authenticator,
    ) -> Self {
        Self {
            advertised_edge: SharedAdvertisedEdge::default(),
            authenticator,
            defaults,
        }
    }

    pub fn with_advertised_edge(
        mut self,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        self.advertised_edge = advertised_edge;
        self
    }

    /// Builds the concrete runtime service graph for local execution.
    ///
    /// # Errors
    ///
    /// Returns a build error when a stateful service cannot initialize from
    /// disk or when a required runtime adapter fails to start.
    pub fn build(self) -> Result<RuntimeAssembly, RuntimeBuildError> {
        let advertised_edge = self.advertised_edge;
        let defaults = self.defaults;
        let authenticator = self.authenticator;
        let iam = IamService::new();
        let sts = StsService::new(iam.clone());
        let factory = Arc::new(StorageFactory::new(StorageConfig::new(
            defaults.state_directory().join("metadata"),
            StorageMode::Wal,
        )));
        let s3 = S3Service::new(
            &factory,
            Arc::new(DirectoryBlobStore::new(
                defaults.state_directory().join("s3").join("blobs"),
            )),
            Arc::new(SystemClock),
        )
        .map_err(RuntimeBuildError::S3State)?;
        let sqs = SqsService::new();
        let apigateway = ApiGatewayService::with_advertised_edge(
            &factory,
            Arc::new(SystemClock),
            advertised_edge.clone(),
        );
        let dynamodb = DynamoDbService::new(&factory)
            .map_err(RuntimeBuildError::DynamoDbState)?;
        let cloudwatch =
            CloudWatchService::new(&factory, Arc::new(SystemClock));
        let cognito = CognitoService::with_advertised_edge(
            &factory,
            Arc::new(SystemClock),
            advertised_edge.clone(),
        );
        let kinesis = KinesisService::new(&factory, Arc::new(SystemClock));
        let kms = KmsService::new(&factory, Arc::new(SystemClock));
        let secrets_manager =
            SecretsManagerService::new(&factory, Arc::new(SystemClock));
        let ssm = SsmService::new(&factory, Arc::new(SystemClock));
        let lambda = LambdaService::new(
            &factory,
            LambdaServiceDependencies {
                blob_store: Arc::new(DirectoryBlobStore::new(
                    defaults.state_directory().join("lambda").join("blobs"),
                )),
                executor: Arc::new(ProcessLambdaExecutor::new(
                    defaults.state_directory().join("lambda").join("work"),
                )),
                iam: iam.clone(),
                s3: s3.clone(),
                sqs: sqs.clone(),
                clock: Arc::new(SystemClock),
                random: Arc::new(OsRandomSource),
            },
        )
        .map_err(RuntimeBuildError::LambdaState)?;
        let http_forwarder: Option<
            Arc<dyn crate::HttpForwarder + Send + Sync>,
        > = Some(Arc::new(TcpHttpForwarder::new()));
        let sns = build_sns_service(
            advertised_edge.clone(),
            Arc::new(SystemTime::now),
            Arc::new(SequentialSnsIdentifierSource::default()),
            SnsServiceDependencies {
                advertised_edge: advertised_edge.clone(),
                http_forwarder: http_forwarder.clone(),
                http_signer: None,
                lambda: Some(lambda.clone()),
                sqs: Some(sqs.clone()),
            },
        );
        let eventbridge_dispatcher = Some(
            build_eventbridge_dispatcher(EventBridgeDispatcherDependencies {
                lambda: Some(lambda.clone()),
                sns: Some(sns.clone()),
                sqs: Some(sqs.clone()),
            })
            .map_err(RuntimeBuildError::EventBridgeRuntime)?,
        );
        let eventbridge = EventBridgeService::new(
            &factory,
            EventBridgeServiceDependencies {
                clock: Arc::new(SystemClock),
                dispatcher: eventbridge_delivery_dispatcher(
                    eventbridge_dispatcher.as_ref(),
                ),
                scheduler: Arc::new(ThreadBackgroundScheduler::new()),
            },
        );
        s3.set_notification_transport(Arc::new(S3NotificationDispatcher {
            sns: Some(sns.clone()),
            sqs: Some(sqs.clone()),
        }));
        let step_functions = StepFunctionsService::new(
            &factory,
            StepFunctionsServiceDependencies {
                clock: Arc::new(SystemClock),
                execution_spawner: Arc::new(
                    ThreadStepFunctionsExecutionSpawner,
                ),
                sleeper: Arc::new(ThreadStepFunctionsSleeper),
                task_adapter: Arc::new(LambdaStepFunctionsTaskAdapter::new(
                    Some(lambda.clone()),
                )),
            },
        );
        let rds = RdsService::new(
            &factory,
            RdsServiceDependencies {
                backend_runtime: Arc::new(ThreadRdsBackendRuntime::new()),
                clock: Arc::new(SystemClock),
                iam_token_validator: Arc::new(PostgresIamTokenValidator::new(
                    authenticator.clone(),
                    iam.clone(),
                    sts.clone(),
                )),
                proxy_runtime: Arc::new(ThreadTcpProxyRuntime::new()),
            },
        );
        let elasticache = ElastiCacheService::new(
            &factory,
            ElastiCacheServiceDependencies {
                clock: Arc::new(SystemClock),
                iam_token_validator: Arc::new(ElastiCacheTokenValidator::new(
                    authenticator,
                    iam.clone(),
                    sts.clone(),
                )),
                node_runtime: Arc::new(ThreadElastiCacheNodeRuntime::new()),
                proxy_runtime: Arc::new(ThreadElastiCacheProxyRuntime::new()),
            },
        );
        let cloudformation = CloudFormationService::with_dependencies(
            Arc::new(SystemTime::now),
            CloudFormationDependencies {
                advertised_edge,
                dynamodb: Some(Arc::new(dynamodb.clone())),
                iam: Some(Arc::new(iam.clone())),
                kms: Some(Arc::new(kms.clone())),
                lambda: Some(Arc::new(lambda.clone())),
                s3: Some(Arc::new(s3.clone())),
                secrets_manager: Some(Arc::new(secrets_manager.clone())),
                sns: Some(Arc::new(sns.clone())),
                sqs: Some(Arc::new(sqs.clone())),
                ssm: Some(Arc::new(ssm.clone())),
            },
        );
        factory.load_all().map_err(RuntimeBuildError::StorageLoad)?;
        let storage_shutdown_factory = Arc::clone(&factory);
        let storage_shutdown =
            Some(Arc::new(move || storage_shutdown_factory.shutdown_all())
                as Arc<dyn Fn() -> Result<(), StorageError> + Send + Sync>);
        let background_tasks = Arc::new(
            start_lambda_background_tasks(
                lambda.clone(),
                Arc::new(ThreadBackgroundScheduler::new()),
            )
            .map_err(RuntimeBuildError::LambdaRuntime)?,
        );
        let lambda_background_shutdown =
            Some(Arc::new(move || background_tasks.cancel()) as _);
        eventbridge
            .restore_schedules()
            .map_err(RuntimeBuildError::EventBridgeRestore)?;
        rds.restore_runtimes().map_err(RuntimeBuildError::RdsRestore)?;
        elasticache
            .restore_runtimes()
            .map_err(RuntimeBuildError::ElastiCacheRestore)?;
        let execute_api_executor: Arc<
            dyn ExecuteApiIntegrationExecutor + Send + Sync,
        > = Arc::new(ApiGatewayIntegrationExecutor::new(
            Some(lambda::request_runtime::LambdaRequestRuntime::new(
                lambda.clone(),
            )),
            http_forwarder.clone(),
        ));

        Ok(RuntimeAssembly::new(
            RuntimeServicesBuilder {
                apigateway,
                execute_api_executor,
                cloudformation,
                cloudwatch,
                cognito,
                dynamodb,
                elasticache,
                eventbridge,
                eventbridge_delivery_shutdown: eventbridge_dispatcher
                    .as_ref()
                    .map(|dispatcher| dispatcher.shutdown.clone()),
                iam,
                kinesis,
                kms,
                lambda,
                lambda_background_shutdown,
                rds,
                s3,
                secrets_manager,
                sns,
                sqs,
                ssm,
                storage_shutdown,
                sts,
                step_functions,
            }
            .build(),
        ))
    }
}

pub struct TestRuntimeBuilder {
    advertised_edge: SharedAdvertisedEdge,
    label: String,
    http_forwarder: Option<Arc<dyn crate::HttpForwarder + Send + Sync>>,
    lambda_executor: Arc<dyn crate::LambdaExecutor + Send + Sync>,
}

impl TestRuntimeBuilder {
    pub fn new(label: impl Into<String>) -> Self {
        let label = label.into();
        Self {
            advertised_edge: SharedAdvertisedEdge::default(),
            label: label.clone(),
            http_forwarder: Some(Arc::new(TcpHttpForwarder::new())),
            lambda_executor: Arc::new(ProcessLambdaExecutor::new(
                std::env::temp_dir().join(&label),
            )),
        }
    }

    pub fn with_advertised_edge(
        mut self,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        self.advertised_edge = advertised_edge;
        self
    }
    pub fn with_http_forwarder(
        mut self,
        http_forwarder: Option<Arc<dyn crate::HttpForwarder + Send + Sync>>,
    ) -> Self {
        self.http_forwarder = http_forwarder;
        self
    }
    pub fn with_lambda_executor(
        mut self,
        lambda_executor: Arc<dyn crate::LambdaExecutor + Send + Sync>,
    ) -> Self {
        self.lambda_executor = lambda_executor;
        self
    }

    /// Builds an in-memory runtime assembly for tests and router fixtures.
    ///
    /// # Errors
    ///
    /// Returns a build error when a service cannot initialize its in-memory
    /// state or when the required test fixture role cannot be seeded.
    pub fn build(self) -> Result<RuntimeAssembly, RuntimeBuildError> {
        let advertised_edge = self.advertised_edge;
        let label = self.label;
        let http_forwarder = self.http_forwarder;
        let iam = IamService::with_time_source(Arc::new(|| UNIX_EPOCH));
        seed_lambda_role(&iam)?;
        let sts = StsService::new(iam.clone());
        let s3 = S3Service::new(
            &memory_factory(&label, "s3"),
            Arc::new(MemoryBlobStore::new()),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )
        .map_err(RuntimeBuildError::S3State)?;
        let sqs = SqsService::new();
        let dynamodb =
            DynamoDbService::new(&memory_factory(&label, "dynamodb"))
                .map_err(RuntimeBuildError::DynamoDbState)?;
        let cloudwatch = CloudWatchService::new(
            &memory_factory(&label, "cloudwatch"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        let cognito_factory = memory_factory(&label, "cognito");
        let cognito = CognitoService::with_advertised_edge(
            &cognito_factory,
            Arc::new(FixedClock::new(UNIX_EPOCH)),
            advertised_edge.clone(),
        );
        let kinesis = KinesisService::new(
            &memory_factory(&label, "kinesis"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        let kms = KmsService::new(
            &memory_factory(&label, "kms"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        let secrets_manager = SecretsManagerService::new(
            &memory_factory(&label, "secrets-manager"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        let ssm = SsmService::new(
            &memory_factory(&label, "ssm"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        let apigateway = ApiGatewayService::with_advertised_edge(
            &memory_factory(&label, "apigateway"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
            advertised_edge.clone(),
        );
        let lambda = LambdaService::new(
            &memory_factory(&label, "lambda"),
            LambdaServiceDependencies {
                blob_store: Arc::new(MemoryBlobStore::new()),
                executor: self.lambda_executor,
                iam: iam.clone(),
                s3: s3.clone(),
                sqs: sqs.clone(),
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                random: Arc::new(SequenceRandomSource::new(vec![
                    0x10, 0x20, 0x30, 0x40,
                ])),
            },
        )
        .map_err(RuntimeBuildError::LambdaState)?;
        let sns = build_sns_service(
            advertised_edge.clone(),
            Arc::new(|| UNIX_EPOCH),
            Arc::new(SequentialSnsIdentifierSource::default()),
            SnsServiceDependencies {
                advertised_edge: advertised_edge.clone(),
                http_forwarder: http_forwarder.clone(),
                http_signer: None,
                lambda: Some(lambda.clone()),
                sqs: Some(sqs.clone()),
            },
        );
        let eventbridge_dispatcher = Some(
            build_eventbridge_dispatcher(EventBridgeDispatcherDependencies {
                lambda: Some(lambda.clone()),
                sns: Some(sns.clone()),
                sqs: Some(sqs.clone()),
            })
            .map_err(RuntimeBuildError::EventBridgeRuntime)?,
        );
        let eventbridge = EventBridgeService::new(
            &memory_factory(&label, "eventbridge"),
            EventBridgeServiceDependencies {
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                dispatcher: eventbridge_delivery_dispatcher(
                    eventbridge_dispatcher.as_ref(),
                ),
                scheduler: Arc::new(crate::ManualBackgroundScheduler::new()),
            },
        );
        s3.set_notification_transport(Arc::new(S3NotificationDispatcher {
            sns: Some(sns.clone()),
            sqs: Some(sqs.clone()),
        }));
        let step_functions = StepFunctionsService::new(
            &memory_factory(&label, "step-functions"),
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                execution_spawner: Arc::new(
                    ThreadStepFunctionsExecutionSpawner,
                ),
                sleeper: Arc::new(ThreadStepFunctionsSleeper),
                task_adapter: Arc::new(LambdaStepFunctionsTaskAdapter::new(
                    Some(lambda.clone()),
                )),
            },
        );
        let elasticache = ElastiCacheService::new(
            &memory_factory(&label, "elasticache"),
            ElastiCacheServiceDependencies {
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                iam_token_validator: Arc::new(
                    crate::RejectAllElastiCacheIamTokenValidator,
                ),
                node_runtime: Arc::new(ThreadElastiCacheNodeRuntime::new()),
                proxy_runtime: Arc::new(ThreadElastiCacheProxyRuntime::new()),
            },
        );
        let rds = RdsService::new(
            &memory_factory(&label, "rds"),
            RdsServiceDependencies {
                backend_runtime: Arc::new(ThreadRdsBackendRuntime::new()),
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                iam_token_validator: Arc::new(
                    crate::RejectAllRdsIamTokenValidator,
                ),
                proxy_runtime: Arc::new(ThreadTcpProxyRuntime::new()),
            },
        );
        let cloudformation = CloudFormationService::with_dependencies(
            Arc::new(|| UNIX_EPOCH),
            CloudFormationDependencies {
                advertised_edge,
                dynamodb: Some(Arc::new(dynamodb.clone())),
                iam: Some(Arc::new(iam.clone())),
                kms: Some(Arc::new(kms.clone())),
                lambda: Some(Arc::new(lambda.clone())),
                s3: Some(Arc::new(s3.clone())),
                secrets_manager: Some(Arc::new(secrets_manager.clone())),
                sns: Some(Arc::new(sns.clone())),
                sqs: Some(Arc::new(sqs.clone())),
                ssm: Some(Arc::new(ssm.clone())),
            },
        );
        let execute_api_executor: Arc<
            dyn ExecuteApiIntegrationExecutor + Send + Sync,
        > = Arc::new(ApiGatewayIntegrationExecutor::new(
            Some(lambda::request_runtime::LambdaRequestRuntime::new(
                lambda.clone(),
            )),
            http_forwarder.clone(),
        ));

        Ok(RuntimeAssembly::new(
            RuntimeServicesBuilder {
                apigateway,
                execute_api_executor,
                cloudformation,
                cloudwatch,
                cognito,
                dynamodb,
                elasticache,
                eventbridge,
                eventbridge_delivery_shutdown: eventbridge_dispatcher
                    .as_ref()
                    .map(|dispatcher| dispatcher.shutdown.clone()),
                iam,
                kinesis,
                kms,
                lambda,
                lambda_background_shutdown: None,
                rds,
                s3,
                secrets_manager,
                sns,
                sqs,
                ssm,
                storage_shutdown: None,
                sts,
                step_functions,
            }
            .build(),
        ))
    }
}
fn memory_factory(label: &str, service: &str) -> StorageFactory {
    StorageFactory::new(StorageConfig::new(
        std::env::temp_dir().join(format!("{label}-{service}")),
        StorageMode::Memory,
    ))
}

fn seed_lambda_role(iam: &IamService) -> Result<(), RuntimeBuildError> {
    iam.create_role(
        &IamScope::new(
            default_account_id()?,
            default_region_id()?,
        ),
        CreateRoleInput {
            assume_role_policy_document: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#.to_owned(),
            description: String::new(),
            max_session_duration: 3_600,
            path: "/".to_owned(),
            role_name: "lambda-role".to_owned(),
            tags: Vec::new(),
        },
    )
    .map_err(|error| {
        RuntimeBuildError::Configuration(format!(
            "failed to seed Lambda test role: {error}"
        ))
    })?;

    Ok(())
}

fn default_account_id() -> Result<AccountId, RuntimeBuildError> {
    "000000000000".parse().map_err(|error| {
        RuntimeBuildError::Configuration(format!(
            "invalid default account id: {error}"
        ))
    })
}

fn default_region_id() -> Result<RegionId, RuntimeBuildError> {
    "eu-west-2".parse().map_err(|error| {
        RuntimeBuildError::Configuration(format!(
            "invalid default region id: {error}"
        ))
    })
}

#[derive(Clone)]
struct PostgresIamTokenValidator {
    authenticator: Authenticator,
    iam: IamService,
    sts: StsService,
}

impl PostgresIamTokenValidator {
    fn new(
        authenticator: Authenticator,
        iam: IamService,
        sts: StsService,
    ) -> Self {
        Self { authenticator, iam, sts }
    }
}

impl std::fmt::Debug for PostgresIamTokenValidator {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("PostgresIamTokenValidator")
    }
}

impl RdsIamTokenValidator for PostgresIamTokenValidator {
    fn validate(
        &self,
        endpoint: &Endpoint,
        username: &str,
        token: &str,
    ) -> Result<(), String> {
        let parsed = parse_rds_iam_token(token)?;
        if !parsed.host.eq_ignore_ascii_case(endpoint.host())
            || parsed.port != endpoint.port()
        {
            return Err(format!(
                "IAM auth token host {}:{} does not match expected endpoint {}:{}.",
                parsed.host,
                parsed.port,
                endpoint.host(),
                endpoint.port(),
            ));
        }
        if parsed.action != "connect" {
            return Err("IAM auth token must use Action=connect.".to_owned());
        }
        if parsed.db_user != username {
            return Err(format!(
                "IAM auth token DBUser {} does not match username {username}.",
                parsed.db_user,
            ));
        }

        let host_header = format!("{}:{}", parsed.host, parsed.port);
        let request = RequestAuth::new(
            "GET",
            &parsed.path_and_query,
            vec![RequestHeader::new("Host", &host_header)],
            b"",
        );
        let verified = self
            .authenticator
            .verify(&request, &self.iam, &self.sts)
            .map_err(|error| error.message().to_owned())?
            .ok_or_else(|| {
                "IAM auth token is missing SigV4 signing metadata.".to_owned()
            })?;
        if verified.scope().service() != ServiceName::RdsDb {
            return Err(format!(
                "IAM auth token must be signed for service {}, got {}.",
                ServiceName::RdsDb.as_str(),
                verified.scope().service().as_str(),
            ));
        }

        Ok(())
    }
}

#[derive(Clone)]
struct ElastiCacheTokenValidator {
    authenticator: Authenticator,
    iam: IamService,
    sts: StsService,
}

impl ElastiCacheTokenValidator {
    fn new(
        authenticator: Authenticator,
        iam: IamService,
        sts: StsService,
    ) -> Self {
        Self { authenticator, iam, sts }
    }
}

impl std::fmt::Debug for ElastiCacheTokenValidator {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("ElastiCacheTokenValidator")
    }
}

impl ElastiCacheIamTokenValidator for ElastiCacheTokenValidator {
    fn validate(
        &self,
        replication_group_id: &ElastiCacheReplicationGroupId,
        token: &str,
    ) -> Result<(), String> {
        let parsed = parse_elasticache_iam_token(token)?;
        if !parsed.host.eq_ignore_ascii_case(replication_group_id.as_str()) {
            return Err(format!(
                "IAM auth token host {} does not match replication group {}.",
                parsed.host, replication_group_id,
            ));
        }
        if parsed.action != "connect" {
            return Err("IAM auth token must use Action=connect.".to_owned());
        }
        if parsed.user.is_empty() {
            return Err("IAM auth token is missing User.".to_owned());
        }

        let request = RequestAuth::new(
            "GET",
            &parsed.path_and_query,
            vec![RequestHeader::new("Host", &parsed.authority)],
            b"",
        );
        let verified = self
            .authenticator
            .verify(&request, &self.iam, &self.sts)
            .map_err(|error| error.message().to_owned())?
            .ok_or_else(|| {
                "IAM auth token is missing SigV4 signing metadata.".to_owned()
            })?;
        if verified.scope().service() != ServiceName::ElastiCache {
            return Err(format!(
                "IAM auth token must be signed for service {}, got {}.",
                ServiceName::ElastiCache.as_str(),
                verified.scope().service().as_str(),
            ));
        }

        Ok(())
    }
}

struct ParsedRdsIamToken {
    action: String,
    db_user: String,
    host: String,
    path_and_query: String,
    port: u16,
}

struct ParsedElastiCacheIamToken {
    action: String,
    authority: String,
    host: String,
    path_and_query: String,
    user: String,
}

fn parse_rds_iam_token(token: &str) -> Result<ParsedRdsIamToken, String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Err("IAM auth token must not be blank.".to_owned());
    }
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);
    let (authority, path) =
        without_scheme.split_once('/').ok_or_else(|| {
            "IAM auth token must include an endpoint and query string."
                .to_owned()
        })?;
    let (host, port) = authority.rsplit_once(':').ok_or_else(|| {
        "IAM auth token must include an explicit host:port authority."
            .to_owned()
    })?;
    let port = port.parse::<u16>().map_err(|_| {
        "IAM auth token port must be a valid TCP port.".to_owned()
    })?;
    let path_and_query = format!("/{path}");
    let query =
        path_and_query.split_once('?').map(|(_, query)| query).ok_or_else(
            || "IAM auth token must include a query string.".to_owned(),
        )?;

    let action = query_parameter(query, "Action")
        .ok_or_else(|| "IAM auth token is missing Action.".to_owned())?;
    let db_user = query_parameter(query, "DBUser")
        .ok_or_else(|| "IAM auth token is missing DBUser.".to_owned())?;

    Ok(ParsedRdsIamToken {
        action,
        db_user,
        host: host.to_owned(),
        path_and_query,
        port,
    })
}

fn parse_elasticache_iam_token(
    token: &str,
) -> Result<ParsedElastiCacheIamToken, String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Err("IAM auth token must not be blank.".to_owned());
    }
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);
    let (authority, path) =
        without_scheme.split_once('/').ok_or_else(|| {
            "IAM auth token must include an endpoint and query string."
                .to_owned()
        })?;
    let path_and_query = format!("/{path}");
    let query =
        path_and_query.split_once('?').map(|(_, query)| query).ok_or_else(
            || "IAM auth token must include a query string.".to_owned(),
        )?;
    let action = query_parameter(query, "Action")
        .ok_or_else(|| "IAM auth token is missing Action.".to_owned())?;
    let user = query_parameter(query, "User")
        .ok_or_else(|| "IAM auth token is missing User.".to_owned())?;
    let host = authority
        .split_once(':')
        .map(|(host, _)| host)
        .unwrap_or(authority)
        .to_ascii_lowercase();

    Ok(ParsedElastiCacheIamToken {
        action,
        authority: authority.to_owned(),
        host,
        path_and_query,
        user,
    })
}

fn query_parameter(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (raw_name, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(raw_name).ok()?.as_str() == name)
            .then(|| percent_decode(raw_value).ok())
            .flatten()
    })
}

fn percent_decode(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match *bytes.get(index).ok_or_else(|| {
            "IAM auth token contains malformed percent-encoding.".to_owned()
        })? {
            b'+' => {
                decoded.push(b' ');
                index = index.saturating_add(1);
            }
            b'%' if index.saturating_add(2) < bytes.len() => {
                let high = *bytes.get(index.saturating_add(1)).ok_or_else(|| {
                    "IAM auth token contains malformed percent-encoding."
                        .to_owned()
                })?;
                let low = *bytes.get(index.saturating_add(2)).ok_or_else(|| {
                    "IAM auth token contains malformed percent-encoding."
                        .to_owned()
                })?;
                decoded.push((hex_value(high)? << 4) | hex_value(low)?);
                index = index.saturating_add(3);
            }
            b'%' => {
                return Err(
                    "IAM auth token contains malformed percent-encoding."
                        .to_owned(),
                );
            }
            byte => {
                decoded.push(byte);
                index = index.saturating_add(1);
            }
        }
    }

    String::from_utf8(decoded)
        .map_err(|_| "IAM auth token must decode as UTF-8.".to_owned())
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte.saturating_sub(b'0')),
        b'a'..=b'f' => Ok(byte.saturating_sub(b'a').saturating_add(10)),
        b'A'..=b'F' => Ok(byte.saturating_sub(b'A').saturating_add(10)),
        _ => {
            Err("IAM auth token contains malformed percent-encoding."
                .to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalRuntimeBuilder, TestRuntimeBuilder};
    use crate::{CreateBucketInput, EventBridgeScope, S3Scope};
    use auth::Authenticator;
    use aws::RuntimeDefaults;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn test_runtime_builder_assembles_always_on_runtime() {
        let runtime_services = TestRuntimeBuilder::new("test-runtime-builder")
            .build()
            .expect("test runtime should build")
            .into_parts();
        let s3_scope = S3Scope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let eventbridge_scope = EventBridgeScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );

        assert!(runtime_services.has_eventbridge_delivery_shutdown());
        assert!(
            runtime_services.s3().list_buckets(&s3_scope).buckets.is_empty()
        );
        assert_eq!(
            runtime_services
                .eventbridge()
                .list_event_buses(
                    &eventbridge_scope,
                    crate::ListEventBusesInput {
                        limit: Some(10),
                        name_prefix: None,
                        next_token: None,
                    },
                )
                .expect("event buses should list")
                .event_buses
                .len(),
            1
        );
    }

    #[test]
    fn test_runtime_builder_does_not_start_lambda_background_shutdown() {
        let runtime_services =
            TestRuntimeBuilder::new("test-runtime-builder-no-lambda")
                .build()
                .expect("test runtime should build")
                .into_parts();

        assert!(!runtime_services.has_lambda_background_shutdown());
    }

    #[test]
    fn local_runtime_builder_assembles_production_runtime() {
        let label = unique_label("local-runtime-builder");
        let state_dir = std::env::temp_dir().join(&label);
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir)
            .expect("state directory should be creatable");
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_dir.to_string_lossy().into_owned()),
        )
        .expect("runtime defaults should build");
        let authenticator = Authenticator::new(defaults.clone());
        let s3_scope = S3Scope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let eventbridge_scope = EventBridgeScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );

        let runtime_services =
            LocalRuntimeBuilder::new(defaults, authenticator)
                .build()
                .expect("local runtime should build")
                .into_parts();

        assert!(runtime_services.has_eventbridge_delivery_shutdown());
        assert!(runtime_services.has_lambda_background_shutdown());
        assert!(
            runtime_services.s3().list_buckets(&s3_scope).buckets.is_empty()
        );
        assert_eq!(
            runtime_services
                .eventbridge()
                .list_event_buses(
                    &eventbridge_scope,
                    crate::ListEventBusesInput {
                        limit: Some(10),
                        name_prefix: None,
                        next_token: None,
                    },
                )
                .expect("event buses should list")
                .event_buses
                .len(),
            1
        );

        runtime_services.shutdown();
        if state_dir.exists() {
            fs::remove_dir_all(&state_dir)
                .expect("state directory should be removable");
        }
    }

    #[test]
    fn local_runtime_builder_shutdown_flushes_runtime_owned_storage() {
        let label = unique_label("local-runtime-builder-shutdown");
        let state_dir = std::env::temp_dir().join(&label);
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir)
            .expect("state directory should be creatable");
        let defaults = RuntimeDefaults::try_new(
            Some("000000000000".to_owned()),
            Some("eu-west-2".to_owned()),
            Some(state_dir.to_string_lossy().into_owned()),
        )
        .expect("runtime defaults should build");
        let s3_scope = S3Scope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let bucket_name = "runtime-shutdown-bucket";
        let runtime_services = LocalRuntimeBuilder::new(
            defaults.clone(),
            Authenticator::new(defaults.clone()),
        )
        .build()
        .expect("local runtime should build")
        .into_parts();

        runtime_services
            .s3()
            .create_bucket(
                &s3_scope,
                CreateBucketInput {
                    name: bucket_name.to_owned(),
                    object_lock_enabled: false,
                    region: s3_scope.region().clone(),
                },
            )
            .expect("bucket should be created");
        runtime_services.shutdown();

        let reloaded_runtime = LocalRuntimeBuilder::new(
            defaults.clone(),
            Authenticator::new(defaults),
        )
        .build()
        .expect("local runtime should rebuild")
        .into_parts();

        let buckets = reloaded_runtime.s3().list_buckets(&s3_scope).buckets;
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, bucket_name);
        assert_eq!(buckets[0].owner_account_id, "000000000000");
        assert_eq!(buckets[0].region, "eu-west-2");

        reloaded_runtime.shutdown();
        if state_dir.exists() {
            fs::remove_dir_all(&state_dir)
                .expect("state directory should be removable");
        }
    }

    fn unique_label(prefix: &str) -> String {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }
}
