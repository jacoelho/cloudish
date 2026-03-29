#[cfg(feature = "cloudwatch")]
use crate::CloudWatchService;
#[cfg(feature = "cognito")]
use crate::CognitoService;
#[cfg(feature = "eventbridge")]
use crate::EventBridgeService;
#[cfg(feature = "kinesis")]
use crate::KinesisService;
#[cfg(feature = "kms")]
use crate::KmsService;
#[cfg(feature = "secrets-manager")]
use crate::SecretsManagerService;
#[cfg(feature = "sns")]
use crate::SequentialSnsIdentifierSource;
#[cfg(feature = "sqs")]
use crate::SqsService;
#[cfg(feature = "ssm")]
use crate::SsmService;
#[cfg(feature = "lambda")]
use crate::adapters::ProcessLambdaExecutor;
#[cfg(feature = "rds")]
use crate::adapters::ThreadRdsBackendRuntime;
use crate::adapters::{
    DirectoryBlobStore, FixedClock, ManagedBackgroundTasks, MemoryBlobStore,
    OsRandomSource, SequenceRandomSource, SystemClock, TcpHttpForwarder,
    ThreadBackgroundScheduler, ThreadTcpProxyRuntime,
};
#[cfg(feature = "elasticache")]
use crate::adapters::{
    ThreadElastiCacheNodeRuntime, ThreadElastiCacheProxyRuntime,
};
#[cfg(feature = "step-functions")]
use crate::adapters::{
    ThreadStepFunctionsExecutionSpawner, ThreadStepFunctionsSleeper,
};
#[cfg(feature = "apigateway")]
use crate::composition::ApiGatewayIntegrationExecutor;
#[cfg(feature = "eventbridge")]
use crate::composition::EventBridgeDispatcherDependencies;
#[cfg(feature = "step-functions")]
use crate::composition::LambdaStepFunctionsTaskAdapter;
#[cfg(feature = "s3")]
use crate::composition::S3NotificationDispatcher;
#[cfg(feature = "eventbridge")]
use crate::composition::build_eventbridge_dispatcher;
#[cfg(feature = "lambda")]
use crate::composition::start_lambda_background_tasks;
#[cfg(feature = "sns")]
use crate::composition::{SnsServiceDependencies, build_sns_service};
#[cfg(feature = "apigateway")]
use crate::{ApiGatewayService, ExecuteApiIntegrationExecutor};
#[cfg(feature = "cloudformation")]
use crate::{CloudFormationDependencies, CloudFormationService};
use crate::{CreateRoleInput, StsService};
#[cfg(feature = "dynamodb")]
use crate::{DynamoDbInitError, DynamoDbService};
#[cfg(feature = "elasticache")]
use crate::{
    ElastiCacheError, ElastiCacheIamTokenValidator, ElastiCacheService,
    ElastiCacheServiceDependencies,
};
use crate::{EnabledServices, RuntimeServices, RuntimeServicesBuilder};
#[cfg(feature = "eventbridge")]
use crate::{EventBridgeError, EventBridgeServiceDependencies};
use crate::{IamScope, IamService};
#[cfg(feature = "lambda")]
use crate::{LambdaInitError, LambdaService, LambdaServiceDependencies};
#[cfg(feature = "rds")]
use crate::{
    RdsError, RdsIamTokenValidator, RdsService, RdsServiceDependencies,
};
#[cfg(feature = "s3")]
use crate::{S3InitError, S3Service};
#[cfg(feature = "step-functions")]
use crate::{StepFunctionsService, StepFunctionsServiceDependencies};
use auth::{Authenticator, RequestAuth, RequestHeader};
use aws::InfrastructureError;
use aws::{
    AccountId, Endpoint, RegionId, RuntimeDefaults, ServiceName,
    SharedAdvertisedEdge,
};
#[cfg(feature = "elasticache")]
use elasticache::ElastiCacheReplicationGroupId;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use storage::{StorageConfig, StorageError, StorageFactory, StorageMode};
use thiserror::Error;

pub struct RuntimeAssembly {
    background_tasks: ManagedBackgroundTasks,
    enabled_services: EnabledServices,
    runtime_services: RuntimeServices,
}

impl RuntimeAssembly {
    pub fn new(
        enabled_services: EnabledServices,
        runtime_services: RuntimeServices,
        background_tasks: ManagedBackgroundTasks,
    ) -> Self {
        Self { background_tasks, enabled_services, runtime_services }
    }

    pub fn enabled_services(&self) -> &EnabledServices {
        &self.enabled_services
    }

    pub fn runtime_services(&self) -> &RuntimeServices {
        &self.runtime_services
    }

    pub fn into_parts(
        self,
    ) -> (EnabledServices, RuntimeServices, ManagedBackgroundTasks) {
        (self.enabled_services, self.runtime_services, self.background_tasks)
    }
}

#[derive(Debug, Error)]
pub enum RuntimeBuildError {
    #[error("invalid runtime configuration: {0}")]
    Configuration(String),
    #[cfg(feature = "dynamodb")]
    #[error("failed to load DynamoDB state: {0}")]
    DynamoDbState(#[source] DynamoDbInitError),
    #[cfg(feature = "eventbridge")]
    #[error("failed to restore EventBridge schedules: {0}")]
    EventBridgeRestore(#[source] EventBridgeError),
    #[cfg(feature = "elasticache")]
    #[error("failed to restore ElastiCache runtimes: {0}")]
    ElastiCacheRestore(#[source] ElastiCacheError),
    #[cfg(feature = "lambda")]
    #[error("failed to start Lambda background runtime: {0}")]
    LambdaRuntime(#[source] InfrastructureError),
    #[cfg(feature = "lambda")]
    #[error("failed to load Lambda state: {0}")]
    LambdaState(#[source] LambdaInitError),
    #[cfg(feature = "s3")]
    #[error("failed to load S3 state: {0}")]
    S3State(#[source] S3InitError),
    #[cfg(feature = "rds")]
    #[error("failed to restore RDS runtimes: {0}")]
    RdsRestore(#[source] RdsError),
    #[error("failed to load persistent runtime state: {0}")]
    StorageLoad(#[source] StorageError),
}

pub struct LocalRuntimeBuilder {
    advertised_edge: SharedAdvertisedEdge,
    authenticator: Authenticator,
    defaults: RuntimeDefaults,
    enabled_services: EnabledServices,
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
            enabled_services: EnabledServices::all(),
        }
    }

    pub fn with_advertised_edge(
        mut self,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        self.advertised_edge = advertised_edge;
        self
    }

    pub fn with_enabled_services<I>(mut self, enabled_services: I) -> Self
    where
        I: IntoIterator<Item = ServiceName>,
    {
        self.enabled_services =
            EnabledServices::from_enabled_services(enabled_services);
        self
    }

    /// Builds the concrete runtime service graph and owned background tasks
    /// for local execution.
    ///
    /// # Errors
    ///
    /// Returns a build error when a stateful service cannot initialize from
    /// disk or when a required runtime adapter fails to start.
    pub fn build(self) -> Result<RuntimeAssembly, RuntimeBuildError> {
        let advertised_edge = self.advertised_edge;
        let defaults = self.defaults;
        let enabled_services = self.enabled_services;
        let authenticator = self.authenticator;
        let iam = IamService::new();
        let sts = StsService::new(iam.clone());
        let factory = StorageFactory::new(StorageConfig::new(
            defaults.state_directory().join("metadata"),
            StorageMode::Wal,
        ));

        #[cfg(feature = "s3")]
        let s3 = S3Service::new(
            &factory,
            Arc::new(DirectoryBlobStore::new(
                defaults.state_directory().join("s3").join("blobs"),
            )),
            Arc::new(SystemClock),
        )
        .map_err(RuntimeBuildError::S3State)?;
        #[cfg(feature = "sqs")]
        let sqs = SqsService::new();
        #[cfg(feature = "apigateway")]
        let apigateway = ApiGatewayService::with_advertised_edge(
            &factory,
            Arc::new(SystemClock),
            advertised_edge.clone(),
        );
        #[cfg(feature = "dynamodb")]
        let dynamodb = DynamoDbService::new(&factory)
            .map_err(RuntimeBuildError::DynamoDbState)?;
        #[cfg(feature = "cloudwatch")]
        let cloudwatch =
            CloudWatchService::new(&factory, Arc::new(SystemClock));
        #[cfg(feature = "cognito")]
        let cognito = CognitoService::with_advertised_edge(
            &factory,
            Arc::new(SystemClock),
            advertised_edge.clone(),
        );
        #[cfg(feature = "kinesis")]
        let kinesis = KinesisService::new(&factory, Arc::new(SystemClock));
        #[cfg(feature = "kms")]
        let kms = KmsService::new(&factory, Arc::new(SystemClock));
        #[cfg(feature = "secrets-manager")]
        let secrets_manager =
            SecretsManagerService::new(&factory, Arc::new(SystemClock));
        #[cfg(feature = "ssm")]
        let ssm = SsmService::new(&factory, Arc::new(SystemClock));
        #[cfg(feature = "lambda")]
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
        #[cfg(any(feature = "apigateway", feature = "sns"))]
        let http_forwarder: Option<
            Arc<dyn crate::HttpForwarder + Send + Sync>,
        > = Some(Arc::new(TcpHttpForwarder::new()));
        #[cfg(not(any(feature = "apigateway", feature = "sns")))]
        let http_forwarder: Option<
            Arc<dyn crate::HttpForwarder + Send + Sync>,
        > = None;
        #[cfg(feature = "sns")]
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
        #[cfg(feature = "eventbridge")]
        let eventbridge = EventBridgeService::new(
            &factory,
            EventBridgeServiceDependencies {
                clock: Arc::new(SystemClock),
                dispatcher: build_eventbridge_dispatcher(
                    EventBridgeDispatcherDependencies {
                        lambda: Some(lambda.clone()),
                        sns: Some(sns.clone()),
                        sqs: Some(sqs.clone()),
                    },
                ),
                scheduler: Arc::new(ThreadBackgroundScheduler::new()),
            },
        );
        #[cfg(feature = "s3")]
        s3.set_notification_transport(Arc::new(S3NotificationDispatcher {
            sns: Some(sns.clone()),
            sqs: Some(sqs.clone()),
        }));
        #[cfg(feature = "step-functions")]
        let step_functions = StepFunctionsService::new(
            &factory,
            StepFunctionsServiceDependencies {
                clock: Arc::new(SystemClock),
                execution_spawner: Arc::new(
                    ThreadStepFunctionsExecutionSpawner,
                ),
                sleeper: Arc::new(ThreadStepFunctionsSleeper),
                task_adapter: Arc::new(LambdaStepFunctionsTaskAdapter::new(
                    lambda.clone(),
                )),
            },
        );
        #[cfg(feature = "rds")]
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
        #[cfg(feature = "elasticache")]
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
        #[cfg(feature = "cloudformation")]
        let cloudformation = CloudFormationService::with_dependencies(
            Arc::new(SystemTime::now),
            CloudFormationDependencies {
                advertised_edge: advertised_edge.clone(),
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
        #[cfg(feature = "lambda")]
        let background_tasks = start_lambda_background_tasks(
            lambda.clone(),
            Arc::new(ThreadBackgroundScheduler::new()),
        )
        .map_err(RuntimeBuildError::LambdaRuntime)?;
        #[cfg(not(feature = "lambda"))]
        let background_tasks = ManagedBackgroundTasks::empty();
        #[cfg(feature = "eventbridge")]
        eventbridge
            .restore_schedules()
            .map_err(RuntimeBuildError::EventBridgeRestore)?;
        #[cfg(feature = "rds")]
        rds.restore_runtimes().map_err(RuntimeBuildError::RdsRestore)?;
        #[cfg(feature = "elasticache")]
        elasticache
            .restore_runtimes()
            .map_err(RuntimeBuildError::ElastiCacheRestore)?;
        #[cfg(feature = "apigateway")]
        let execute_api_executor: Arc<
            dyn ExecuteApiIntegrationExecutor + Send + Sync,
        > = Arc::new(ApiGatewayIntegrationExecutor::new(
            lambda.clone(),
            http_forwarder.clone(),
        ));

        Ok(RuntimeAssembly::new(
            enabled_services,
            RuntimeServicesBuilder {
                #[cfg(feature = "apigateway")]
                apigateway,
                #[cfg(feature = "apigateway")]
                execute_api_executor,
                #[cfg(feature = "cloudformation")]
                cloudformation,
                #[cfg(feature = "cloudwatch")]
                cloudwatch,
                #[cfg(feature = "cognito")]
                cognito,
                #[cfg(feature = "dynamodb")]
                dynamodb,
                #[cfg(feature = "elasticache")]
                elasticache,
                #[cfg(feature = "eventbridge")]
                eventbridge,
                iam,
                #[cfg(feature = "kinesis")]
                kinesis,
                #[cfg(feature = "kms")]
                kms,
                #[cfg(feature = "lambda")]
                lambda,
                #[cfg(feature = "rds")]
                rds,
                #[cfg(feature = "s3")]
                s3,
                #[cfg(feature = "secrets-manager")]
                secrets_manager,
                #[cfg(feature = "sns")]
                sns,
                #[cfg(feature = "sqs")]
                sqs,
                #[cfg(feature = "ssm")]
                ssm,
                sts,
                #[cfg(feature = "step-functions")]
                step_functions,
            }
            .build(),
            background_tasks,
        ))
    }
}

pub struct TestRuntimeBuilder {
    advertised_edge: SharedAdvertisedEdge,
    enabled_services: EnabledServices,
    label: String,
    #[cfg(any(feature = "apigateway", feature = "sns"))]
    http_forwarder: Option<Arc<dyn crate::HttpForwarder + Send + Sync>>,
    #[cfg(feature = "lambda")]
    lambda_executor: Arc<dyn crate::LambdaExecutor + Send + Sync>,
}

impl TestRuntimeBuilder {
    pub fn new(label: impl Into<String>) -> Self {
        let label = label.into();
        Self {
            advertised_edge: SharedAdvertisedEdge::default(),
            enabled_services: EnabledServices::all(),
            label: label.clone(),
            #[cfg(any(feature = "apigateway", feature = "sns"))]
            http_forwarder: Some(Arc::new(TcpHttpForwarder::new())),
            #[cfg(feature = "lambda")]
            lambda_executor: Arc::new(ProcessLambdaExecutor::new(
                std::env::temp_dir().join(&label),
            )),
        }
    }

    pub fn with_enabled_services<I>(mut self, enabled_services: I) -> Self
    where
        I: IntoIterator<Item = ServiceName>,
    {
        self.enabled_services =
            EnabledServices::from_enabled_services(enabled_services);
        self
    }

    pub fn with_advertised_edge(
        mut self,
        advertised_edge: SharedAdvertisedEdge,
    ) -> Self {
        self.advertised_edge = advertised_edge;
        self
    }

    #[cfg(any(feature = "apigateway", feature = "sns"))]
    pub fn with_http_forwarder(
        mut self,
        http_forwarder: Option<Arc<dyn crate::HttpForwarder + Send + Sync>>,
    ) -> Self {
        self.http_forwarder = http_forwarder;
        self
    }

    #[cfg(feature = "lambda")]
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
        let enabled_services = self.enabled_services;
        #[cfg(any(feature = "apigateway", feature = "sns"))]
        let http_forwarder = self.http_forwarder;
        let iam = IamService::new();
        #[cfg(feature = "lambda")]
        seed_lambda_role(&iam)?;
        let sts = StsService::new(iam.clone());
        #[cfg(feature = "s3")]
        let s3 = S3Service::new(
            &memory_factory(&label, "s3"),
            Arc::new(MemoryBlobStore::new()),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )
        .map_err(RuntimeBuildError::S3State)?;
        #[cfg(feature = "sqs")]
        let sqs = SqsService::new();
        #[cfg(feature = "dynamodb")]
        let dynamodb =
            DynamoDbService::new(&memory_factory(&label, "dynamodb"))
                .map_err(RuntimeBuildError::DynamoDbState)?;
        #[cfg(feature = "cloudwatch")]
        let cloudwatch = CloudWatchService::new(
            &memory_factory(&label, "cloudwatch"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        #[cfg(feature = "cognito")]
        let cognito_factory = memory_factory(&label, "cognito");
        #[cfg(feature = "cognito")]
        let cognito = CognitoService::with_advertised_edge(
            &cognito_factory,
            Arc::new(FixedClock::new(UNIX_EPOCH)),
            advertised_edge.clone(),
        );
        #[cfg(feature = "kinesis")]
        let kinesis = KinesisService::new(
            &memory_factory(&label, "kinesis"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        #[cfg(feature = "kms")]
        let kms = KmsService::new(
            &memory_factory(&label, "kms"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        #[cfg(feature = "secrets-manager")]
        let secrets_manager = SecretsManagerService::new(
            &memory_factory(&label, "secrets-manager"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        #[cfg(feature = "ssm")]
        let ssm = SsmService::new(
            &memory_factory(&label, "ssm"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        );
        #[cfg(feature = "apigateway")]
        let apigateway = ApiGatewayService::with_advertised_edge(
            &memory_factory(&label, "apigateway"),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
            advertised_edge.clone(),
        );
        #[cfg(feature = "lambda")]
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
        #[cfg(not(any(feature = "apigateway", feature = "sns")))]
        let http_forwarder: Option<
            Arc<dyn crate::HttpForwarder + Send + Sync>,
        > = None;
        #[cfg(feature = "sns")]
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
        #[cfg(feature = "eventbridge")]
        let eventbridge = EventBridgeService::new(
            &memory_factory(&label, "eventbridge"),
            EventBridgeServiceDependencies {
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                dispatcher: build_eventbridge_dispatcher(
                    EventBridgeDispatcherDependencies {
                        lambda: Some(lambda.clone()),
                        sns: Some(sns.clone()),
                        sqs: Some(sqs.clone()),
                    },
                ),
                scheduler: Arc::new(crate::ManualBackgroundScheduler::new()),
            },
        );
        #[cfg(feature = "s3")]
        s3.set_notification_transport(Arc::new(S3NotificationDispatcher {
            sns: Some(sns.clone()),
            sqs: Some(sqs.clone()),
        }));
        #[cfg(feature = "step-functions")]
        let step_functions = StepFunctionsService::new(
            &memory_factory(&label, "step-functions"),
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                execution_spawner: Arc::new(
                    ThreadStepFunctionsExecutionSpawner,
                ),
                sleeper: Arc::new(ThreadStepFunctionsSleeper),
                task_adapter: Arc::new(LambdaStepFunctionsTaskAdapter::new(
                    lambda.clone(),
                )),
            },
        );
        #[cfg(feature = "elasticache")]
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
        #[cfg(feature = "rds")]
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
        #[cfg(feature = "cloudformation")]
        let cloudformation = CloudFormationService::with_dependencies(
            Arc::new(|| UNIX_EPOCH),
            CloudFormationDependencies {
                advertised_edge: advertised_edge.clone(),
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
        #[cfg(feature = "apigateway")]
        let execute_api_executor: Arc<
            dyn ExecuteApiIntegrationExecutor + Send + Sync,
        > = Arc::new(ApiGatewayIntegrationExecutor::new(
            lambda.clone(),
            http_forwarder.clone(),
        ));

        Ok(RuntimeAssembly::new(
            enabled_services,
            RuntimeServicesBuilder {
                #[cfg(feature = "apigateway")]
                apigateway,
                #[cfg(feature = "apigateway")]
                execute_api_executor,
                #[cfg(feature = "cloudformation")]
                cloudformation,
                #[cfg(feature = "cloudwatch")]
                cloudwatch,
                #[cfg(feature = "cognito")]
                cognito,
                #[cfg(feature = "dynamodb")]
                dynamodb,
                #[cfg(feature = "elasticache")]
                elasticache,
                #[cfg(feature = "eventbridge")]
                eventbridge,
                iam,
                #[cfg(feature = "kinesis")]
                kinesis,
                #[cfg(feature = "kms")]
                kms,
                #[cfg(feature = "lambda")]
                lambda,
                #[cfg(feature = "rds")]
                rds,
                #[cfg(feature = "s3")]
                s3,
                #[cfg(feature = "secrets-manager")]
                secrets_manager,
                #[cfg(feature = "sns")]
                sns,
                #[cfg(feature = "sqs")]
                sqs,
                #[cfg(feature = "ssm")]
                ssm,
                sts,
                #[cfg(feature = "step-functions")]
                step_functions,
            }
            .build(),
            ManagedBackgroundTasks::empty(),
        ))
    }
}

fn memory_factory(label: &str, service: &str) -> StorageFactory {
    StorageFactory::new(StorageConfig::new(
        std::env::temp_dir().join(format!("{label}-{service}")),
        StorageMode::Memory,
    ))
}

#[cfg(feature = "lambda")]
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

#[cfg(feature = "rds")]
#[derive(Clone)]
struct PostgresIamTokenValidator {
    authenticator: Authenticator,
    iam: IamService,
    sts: StsService,
}

#[cfg(feature = "rds")]
impl PostgresIamTokenValidator {
    fn new(
        authenticator: Authenticator,
        iam: IamService,
        sts: StsService,
    ) -> Self {
        Self { authenticator, iam, sts }
    }
}

#[cfg(feature = "rds")]
impl std::fmt::Debug for PostgresIamTokenValidator {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("PostgresIamTokenValidator")
    }
}

#[cfg(feature = "rds")]
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

#[cfg(feature = "elasticache")]
#[derive(Clone)]
struct ElastiCacheTokenValidator {
    authenticator: Authenticator,
    iam: IamService,
    sts: StsService,
}

#[cfg(feature = "elasticache")]
impl ElastiCacheTokenValidator {
    fn new(
        authenticator: Authenticator,
        iam: IamService,
        sts: StsService,
    ) -> Self {
        Self { authenticator, iam, sts }
    }
}

#[cfg(feature = "elasticache")]
impl std::fmt::Debug for ElastiCacheTokenValidator {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("ElastiCacheTokenValidator")
    }
}

#[cfg(feature = "elasticache")]
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

#[cfg(feature = "rds")]
struct ParsedRdsIamToken {
    action: String,
    db_user: String,
    host: String,
    path_and_query: String,
    port: u16,
}

#[cfg(feature = "elasticache")]
struct ParsedElastiCacheIamToken {
    action: String,
    authority: String,
    host: String,
    path_and_query: String,
    user: String,
}

#[cfg(feature = "rds")]
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

#[cfg(feature = "elasticache")]
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

#[cfg(any(feature = "rds", feature = "elasticache"))]
fn query_parameter(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (raw_name, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(raw_name).ok()?.as_str() == name)
            .then(|| percent_decode(raw_value).ok())
            .flatten()
    })
}

#[cfg(any(feature = "rds", feature = "elasticache"))]
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
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = *bytes.get(index + 1).ok_or_else(|| {
                    "IAM auth token contains malformed percent-encoding."
                        .to_owned()
                })?;
                let low = *bytes.get(index + 2).ok_or_else(|| {
                    "IAM auth token contains malformed percent-encoding."
                        .to_owned()
                })?;
                decoded.push((hex_value(high)? << 4) | hex_value(low)?);
                index += 3;
            }
            b'%' => {
                return Err(
                    "IAM auth token contains malformed percent-encoding."
                        .to_owned(),
                );
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded)
        .map_err(|_| "IAM auth token must decode as UTF-8.".to_owned())
}

#[cfg(any(feature = "rds", feature = "elasticache"))]
fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => {
            Err("IAM auth token contains malformed percent-encoding."
                .to_owned())
        }
    }
}

#[cfg(all(test, feature = "all-services"))]
mod tests {
    use super::{LocalRuntimeBuilder, TestRuntimeBuilder};
    use crate::{EventBridgeScope, S3Scope};
    use auth::Authenticator;
    use aws::{RuntimeDefaults, ServiceName};
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn test_runtime_builder_assembles_requested_membership() {
        let assembly = TestRuntimeBuilder::new("test-runtime-builder")
            .with_enabled_services([ServiceName::Lambda, ServiceName::S3])
            .build()
            .expect("test runtime should build");
        let (enabled_services, runtime_services, _) = assembly.into_parts();
        let s3_scope = S3Scope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let eventbridge_scope = EventBridgeScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );

        assert_eq!(enabled_services.enabled_service_count(), 2);
        assert!(enabled_services.is_enabled(ServiceName::Lambda));
        assert!(enabled_services.is_enabled(ServiceName::S3));
        assert!(!enabled_services.is_enabled(ServiceName::Sqs));
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

        let (enabled_services, runtime_services, background_tasks) =
            LocalRuntimeBuilder::new(defaults, authenticator)
                .with_enabled_services([ServiceName::Lambda, ServiceName::S3])
                .build()
                .expect("local runtime should build")
                .into_parts();

        assert_eq!(enabled_services.enabled_service_count(), 2);
        assert!(enabled_services.is_enabled(ServiceName::Lambda));
        assert!(enabled_services.is_enabled(ServiceName::S3));
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

        drop(background_tasks);
        fs::remove_dir_all(&state_dir)
            .expect("state directory should be removable");
    }

    fn unique_label(prefix: &str) -> String {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{id}")
    }
}
