mod apigateway;
mod eventbridge;
mod lambda_background;
mod s3_notifications;
mod sns;
mod step_functions;

pub use self::apigateway::ApiGatewayIntegrationExecutor;
pub(crate) use self::eventbridge::{
    EventBridgeDispatcherAssembly, EventBridgeDispatcherDependencies,
    build_eventbridge_dispatcher,
};
pub(crate) use self::lambda_background::start_lambda_background_tasks;
pub use self::s3_notifications::S3NotificationDispatcher;
#[cfg(test)]
pub(crate) use self::sns::{CloudishSnsHttpSigner, cloudish_sns_signing_key};
pub use self::sns::{
    SnsServiceDependencies, build_sns_service, cloudish_sns_signing_cert_pem,
};
pub use self::step_functions::LambdaStepFunctionsTaskAdapter;

#[cfg(test)]
mod tests {
    use super::{
        ApiGatewayIntegrationExecutor, CloudishSnsHttpSigner,
        LambdaStepFunctionsTaskAdapter, S3NotificationDispatcher,
        SnsServiceDependencies, build_sns_service, cloudish_sns_signing_key,
        start_lambda_background_tasks,
    };
    use crate::{
        ApiGatewayScope, ExecuteApiError, ExecuteApiIntegrationExecutor,
        ExecuteApiIntegrationPlan, ExecuteApiInvocation,
        ExecuteApiLambdaProxyPlan, FixedClock, ManualBackgroundScheduler,
        MemoryBlobStore, SequenceRandomSource,
    };
    use aws::{
        AccountId, Arn, BackgroundScheduler, ExecuteApiSourceArn,
        HttpForwardResponse, HttpForwarder, InfrastructureError,
        LambdaExecutor, LambdaFunctionTarget, LambdaInvocationRequest,
        LambdaInvocationResult, RegionId, SharedAdvertisedEdge,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use iam::{CreateRoleInput, IamScope, IamService};
    use lambda::{
        CreateFunctionInput, LambdaCodeInput, LambdaPackageType, LambdaScope,
        LambdaService, LambdaServiceDependencies,
    };
    use rsa::pkcs1v15::Pkcs1v15Sign;
    use s3::{
        S3EventNotification, S3NotificationTransport, S3Scope, S3Service,
    };
    use serde_json::json;
    use sha1::{Digest, Sha1};
    use sns::{CreateTopicInput, SnsHttpSigner, SnsScope, SubscribeInput};
    use sqs::{CreateQueueInput, ReceiveMessageInput, SqsScope, SqsService};
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};
    use step_functions::{
        CreateStateMachineInput, DescribeExecutionInput, ExecutionStatus,
        StartExecutionInput, StepFunctionsExecutionSpawner,
        StepFunctionsScope, StepFunctionsService,
        StepFunctionsServiceDependencies, StepFunctionsSleeper,
        StepFunctionsSpawnHandle,
    };
    use storage::{StorageConfig, StorageFactory, StorageMode};

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedForwardRequest {
        body: String,
        headers: Vec<(String, String)>,
    }

    #[derive(Debug, Default)]
    struct RecordingForwarder {
        requests: Mutex<Vec<RecordedForwardRequest>>,
    }

    impl RecordingForwarder {
        fn bodies(&self) -> Vec<String> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .map(|request| request.body.clone())
                .collect()
        }

        fn requests(&self) -> Vec<RecordedForwardRequest> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl HttpForwarder for RecordingForwarder {
        fn forward(
            &self,
            request: &aws::HttpForwardRequest,
        ) -> Result<HttpForwardResponse, InfrastructureError> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(RecordedForwardRequest {
                    body: String::from_utf8_lossy(request.body()).into_owned(),
                    headers: request.headers().to_vec(),
                });
            Ok(HttpForwardResponse::new(200, "OK", Vec::new(), Vec::new()))
        }
    }

    #[derive(Debug, Clone)]
    struct TestSnsHttpSigner {
        signature: String,
        signing_cert_url: String,
    }

    impl SnsHttpSigner for TestSnsHttpSigner {
        fn sign(&self, _string_to_sign: &str) -> String {
            self.signature.clone()
        }

        fn signing_cert_url(&self) -> String {
            self.signing_cert_url.clone()
        }
    }

    #[test]
    fn cloudish_sns_http_signer_produces_verifiable_base64_signatures() {
        let signer =
            CloudishSnsHttpSigner::new(SharedAdvertisedEdge::default());
        let string_to_sign =
            "Message\nhello\nTimestamp\n1970-01-01T00:00:01Z\n";
        let signature = STANDARD
            .decode(signer.sign(string_to_sign))
            .expect("signature should be base64");
        let digest = Sha1::digest(string_to_sign.as_bytes());
        let public_key = cloudish_sns_signing_key()
            .as_ref()
            .expect("embedded SNS signing key should parse")
            .to_public_key();

        public_key
            .verify(Pkcs1v15Sign::new::<Sha1>(), &digest, &signature)
            .expect("signature should verify with the embedded key");
        assert_eq!(
            signer.signing_cert_url(),
            "http://localhost:4566/__aws/sns/signing-cert.pem"
        );
    }

    #[derive(Debug, Clone)]
    struct FakeLambdaExecutor {
        response: Arc<Mutex<LambdaInvocationResult>>,
    }

    impl FakeLambdaExecutor {
        fn success(payload: serde_json::Value) -> TestResult<Self> {
            let payload = serde_json::to_vec(&payload)?;

            Ok(Self {
                response: Arc::new(Mutex::new(LambdaInvocationResult::new(
                    payload,
                    Option::<String>::None,
                ))),
            })
        }

        fn function_error(payload: &str, error: &str) -> Self {
            Self {
                response: Arc::new(Mutex::new(LambdaInvocationResult::new(
                    payload.as_bytes().to_vec(),
                    Some(error.to_owned()),
                ))),
            }
        }
    }

    impl LambdaExecutor for FakeLambdaExecutor {
        fn invoke(
            &self,
            _request: &LambdaInvocationRequest,
        ) -> Result<LambdaInvocationResult, InfrastructureError> {
            Ok(self
                .response
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        }

        fn invoke_async(
            &self,
            _request: LambdaInvocationRequest,
        ) -> Result<(), InfrastructureError> {
            Ok(())
        }

        fn validate_zip(
            &self,
            _runtime: &str,
            _handler: &str,
            _archive: &[u8],
        ) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct InlineSpawner;

    impl StepFunctionsExecutionSpawner for InlineSpawner {
        fn spawn_paused(
            &self,
            _task_name: &str,
            task: Box<dyn FnOnce() + Send>,
        ) -> Result<Box<dyn StepFunctionsSpawnHandle>, InfrastructureError>
        {
            Ok(Box::new(InlineSpawnHandle { task: Some(task) }))
        }
    }

    #[derive(Default)]
    struct InlineSpawnHandle {
        task: Option<Box<dyn FnOnce() + Send>>,
    }

    impl StepFunctionsSpawnHandle for InlineSpawnHandle {
        fn start(mut self: Box<Self>) {
            if let Some(task) = self.task.take() {
                task();
            }
        }
    }

    #[derive(Debug, Default)]
    struct NoopSleeper;

    impl StepFunctionsSleeper for NoopSleeper {
        fn sleep(
            &self,
            _duration: Duration,
        ) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    fn account_id() -> TestResult<AccountId> {
        Ok("000000000000".parse::<AccountId>()?)
    }

    fn region_id() -> TestResult<RegionId> {
        Ok("eu-west-2".parse::<RegionId>()?)
    }

    fn iam() -> TestResult<IamService> {
        let iam = IamService::new();
        iam.create_role(
            &IamScope::new(account_id()?, region_id()?),
            CreateRoleInput {
                assume_role_policy_document: r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#.to_owned(),
                description: String::new(),
                max_session_duration: 3_600,
                path: "/".to_owned(),
                role_name: "lambda-role".to_owned(),
                tags: Vec::new(),
            },
        )?;
        Ok(iam)
    }

    fn lambda_scope() -> TestResult<LambdaScope> {
        Ok(LambdaScope::new(account_id()?, region_id()?))
    }

    fn sns_scope() -> TestResult<SnsScope> {
        Ok(SnsScope::new(account_id()?, region_id()?))
    }

    fn sqs_scope() -> TestResult<SqsScope> {
        Ok(SqsScope::new(account_id()?, region_id()?))
    }

    fn s3_scope() -> TestResult<S3Scope> {
        Ok(S3Scope::new(account_id()?, region_id()?))
    }

    fn step_functions_scope() -> TestResult<StepFunctionsScope> {
        Ok(StepFunctionsScope::new(account_id()?, region_id()?))
    }

    fn queue_arn(queue: &sqs::SqsQueueIdentity) -> TestResult<Arn> {
        Ok(format!(
            "arn:aws:sqs:{}:{}:{}",
            queue.region().as_str(),
            queue.account_id().as_str(),
            queue.queue_name(),
        )
        .parse()?)
    }

    fn step_functions_service(
        label: &str,
        task_adapter: Arc<dyn step_functions::StepFunctionsTaskAdapter>,
    ) -> StepFunctionsService {
        StepFunctionsService::new(
            &StorageFactory::new(StorageConfig::new(
                std::env::temp_dir()
                    .join(format!("app-step-functions-{label}")),
                StorageMode::Memory,
            )),
            StepFunctionsServiceDependencies {
                clock: Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(1),
                )),
                execution_spawner: Arc::new(InlineSpawner),
                sleeper: Arc::new(NoopSleeper),
                task_adapter,
            },
        )
    }

    fn lambda_service(
        label: &str,
        executor: Arc<dyn LambdaExecutor + Send + Sync>,
        sqs: SqsService,
    ) -> TestResult<LambdaService> {
        let factory = StorageFactory::new(StorageConfig::new(
            std::env::temp_dir().join(format!("app-composition-{label}")),
            StorageMode::Memory,
        ));
        let s3 = S3Service::new(
            &factory,
            Arc::new(MemoryBlobStore::new()),
            Arc::new(FixedClock::new(UNIX_EPOCH)),
        )?;
        let iam = iam()?;
        let lambda = LambdaService::new(
            &factory,
            LambdaServiceDependencies {
                blob_store: Arc::new(MemoryBlobStore::new()),
                executor,
                iam,
                s3,
                sqs,
                clock: Arc::new(FixedClock::new(UNIX_EPOCH)),
                random: Arc::new(SequenceRandomSource::new(vec![
                    0x10, 0x20, 0x30, 0x40,
                ])),
            },
        )?;
        lambda.create_function(
            &lambda_scope()?,
            CreateFunctionInput {
                code: LambdaCodeInput::InlineZip { archive: vec![1, 2, 3] },
                dead_letter_target_arn: None,
                description: None,
                environment: BTreeMap::new(),
                function_name: "demo".to_owned(),
                handler: Some("bootstrap.handler".to_owned()),
                memory_size: None,
                package_type: LambdaPackageType::Zip,
                publish: false,
                role: "arn:aws:iam::000000000000:role/lambda-role".to_owned(),
                runtime: Some("provided.al2".to_owned()),
                timeout: None,
            },
        )?;
        Ok(lambda)
    }

    #[test]
    fn lambda_background_tasks_can_be_cancelled() {
        let scheduler = Arc::new(ManualBackgroundScheduler::new());
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "lambda-background",
            Arc::new(
                FakeLambdaExecutor::success(json!({ "ok": true }))
                    .expect("payload should serialize"),
            ),
            sqs,
        )
        .expect("lambda service should build");

        let tasks = start_lambda_background_tasks(
            lambda,
            Arc::clone(&scheduler) as Arc<dyn BackgroundScheduler>,
        )
        .expect("background tasks should start");

        assert_eq!(scheduler.active_task_count(), 2);
        scheduler.run_pending();

        tasks.cancel().expect("tasks should cancel");
        assert_eq!(scheduler.active_task_count(), 0);
    }

    #[test]
    fn s3_notification_dispatcher_validates_and_delivers_sqs_targets() {
        let sqs = SqsService::new();
        let queue = sqs
            .create_queue(
                &sqs_scope().expect("SQS scope should build"),
                CreateQueueInput {
                    queue_name: "events".to_owned(),
                    attributes: BTreeMap::new(),
                },
            )
            .expect("queue should create");
        let queue_arn = queue_arn(&queue).expect("queue ARN should build");
        let dispatcher =
            S3NotificationDispatcher { sns: None, sqs: Some(sqs.clone()) };
        let notification = S3EventNotification {
            bucket_arn: "arn:aws:s3:::demo-bucket"
                .parse()
                .expect("bucket ARN should parse"),
            bucket_name: "demo-bucket".to_owned(),
            bucket_owner_account_id: "000000000000"
                .parse()
                .expect("account should parse"),
            configuration_id: Some("config".to_owned()),
            destination_arn: queue_arn.clone(),
            etag: Some("etag".to_owned()),
            event_name: "ObjectCreated:Put".to_owned(),
            event_time_epoch_seconds: 1,
            key: "path/file.txt".to_owned(),
            region: "eu-west-2".parse().expect("region should parse"),
            requester_account_id: "000000000000"
                .parse()
                .expect("account should parse"),
            sequencer: "1".to_owned(),
            size: 3,
            version_id: Some("v1".to_owned()),
        };

        dispatcher
            .validate_destination(
                &s3_scope().expect("S3 scope should build"),
                "demo-bucket",
                &"000000000000".parse().expect("account should parse"),
                &"eu-west-2".parse().expect("region should parse"),
                &queue_arn,
            )
            .expect("queue destination should validate");
        dispatcher.publish(&notification);

        let received = sqs
            .receive_message(
                &queue,
                ReceiveMessageInput {
                    max_number_of_messages: None,
                    visibility_timeout: None,
                    wait_time_seconds: None,
                },
            )
            .expect("receive should succeed");

        assert_eq!(received.len(), 1);
        assert!(received[0].body.contains("\"eventSource\":\"aws:s3\""));
    }

    #[test]
    fn sns_delivery_dispatcher_forwards_http_confirmation_and_notification() {
        let forwarder = Arc::new(RecordingForwarder::default());
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "sns-http",
            Arc::new(
                FakeLambdaExecutor::success(json!({ "ok": true }))
                    .expect("payload should serialize"),
            ),
            sqs.clone(),
        )
        .expect("lambda service should build");
        let sns = build_sns_service(
            SharedAdvertisedEdge::default(),
            Arc::new(|| UNIX_EPOCH + Duration::from_secs(1)),
            Arc::new(sns::SequentialSnsIdentifierSource::default()),
            SnsServiceDependencies {
                advertised_edge: SharedAdvertisedEdge::default(),
                http_forwarder: Some(Arc::clone(&forwarder)
                    as Arc<dyn HttpForwarder + Send + Sync>),
                http_signer: Some(Arc::new(TestSnsHttpSigner {
                    signature: "signed-by-test".to_owned(),
                    signing_cert_url: "https://example.com/cert.pem"
                        .to_owned(),
                })),
                lambda: Some(lambda),
                sqs: Some(sqs),
            },
        );
        let topic_arn = sns
            .create_topic(
                &sns_scope().expect("SNS scope should build"),
                CreateTopicInput {
                    attributes: BTreeMap::new(),
                    name: "topic".to_owned(),
                },
            )
            .expect("topic should create");

        sns.subscribe(SubscribeInput {
            attributes: BTreeMap::new(),
            endpoint: "http://example.com/hooks".to_owned(),
            protocol: "http".to_owned(),
            return_subscription_arn: false,
            topic_arn: topic_arn.clone(),
        })
        .expect("http subscription should create");
        let confirmation = forwarder.bodies();
        let confirmation_body =
            serde_json::from_str::<serde_json::Value>(&confirmation[0])
                .expect("confirmation body should be json");
        let token = confirmation_body["Token"]
            .as_str()
            .expect("confirmation token should be present")
            .to_owned();
        sns.confirm_subscription(&topic_arn, &token)
            .expect("subscription should confirm");
        sns.publish(sns::PublishInput {
            message: "hello".to_owned(),
            message_attributes: BTreeMap::new(),
            message_deduplication_id: None,
            message_group_id: None,
            subject: None,
            target_arn: None,
            topic_arn: Some(topic_arn),
        })
        .expect("publish should succeed");

        let requests = forwarder.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0].body.contains("\"Type\":\"SubscriptionConfirmation\"")
        );
        assert!(requests[1].body.contains("\"Type\":\"Notification\""));
        assert!(requests[0].headers.contains(&(
            "x-amz-sns-message-type".to_owned(),
            "SubscriptionConfirmation".to_owned(),
        )));
        assert!(requests[1].headers.contains(&(
            "x-amz-sns-message-type".to_owned(),
            "Notification".to_owned(),
        )));
        assert!(requests[1].headers.contains(&(
            "Content-Type".to_owned(),
            "text/plain; charset=UTF-8".to_owned(),
        )));
        assert!(requests[0].body.contains("\"Signature\":\"signed-by-test\""));
        assert!(requests[1].body.contains("\"Signature\":\"signed-by-test\""));
        assert!(
            requests[1].body.contains(
                "\"SigningCertURL\":\"https://example.com/cert.pem\""
            )
        );
        assert!(!requests[0].body.contains("\"Signature\":\"CLOUDISH\""));
    }

    #[test]
    fn sns_delivery_dispatcher_forwards_http_headers_without_optional_dependencies()
     {
        let forwarder = Arc::new(RecordingForwarder::default());
        let signer = Arc::new(TestSnsHttpSigner {
            signature: "signature-a".to_owned(),
            signing_cert_url: "https://example.com/cert-a.pem".to_owned(),
        });
        let sns = build_sns_service(
            SharedAdvertisedEdge::default(),
            Arc::new(|| UNIX_EPOCH + Duration::from_secs(1)),
            Arc::new(sns::SequentialSnsIdentifierSource::default()),
            SnsServiceDependencies {
                advertised_edge: SharedAdvertisedEdge::default(),
                http_forwarder: Some(Arc::clone(&forwarder)
                    as Arc<dyn HttpForwarder + Send + Sync>),
                http_signer: Some(signer),
                lambda: None,
                sqs: None,
            },
        );
        let topic_arn = sns
            .create_topic(
                &sns_scope().expect("SNS scope should build"),
                CreateTopicInput {
                    attributes: BTreeMap::new(),
                    name: "headers".to_owned(),
                },
            )
            .expect("topic should create");

        sns.subscribe(SubscribeInput {
            attributes: BTreeMap::new(),
            endpoint: "http://example.com/hooks".to_owned(),
            protocol: "http".to_owned(),
            return_subscription_arn: false,
            topic_arn: topic_arn.clone(),
        })
        .expect("http subscription should create");
        let confirmation_body = forwarder.bodies()[0].clone();
        let confirmation: serde_json::Value =
            serde_json::from_str(&confirmation_body)
                .expect("confirmation body should be JSON");
        let token = confirmation["Token"]
            .as_str()
            .expect("confirmation token should be present")
            .to_owned();
        sns.confirm_subscription(&topic_arn, &token)
            .expect("subscription should confirm");
        sns.publish(sns::PublishInput {
            message: "hello".to_owned(),
            message_attributes: BTreeMap::new(),
            message_deduplication_id: None,
            message_group_id: None,
            subject: None,
            target_arn: None,
            topic_arn: Some(topic_arn),
        })
        .expect("publish should succeed");

        let requests = forwarder.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].headers.contains(&(
            "x-amz-sns-topic-arn".to_owned(),
            "arn:aws:sns:eu-west-2:000000000000:headers".to_owned(),
        )));
        assert!(
            requests[1]
                .headers
                .iter()
                .any(|(name, _)| name == "x-amz-sns-subscription-arn")
        );
        assert!(requests[0].body.contains("\"Signature\":\"signature-a\""));
        assert!(requests[1].body.contains("\"Signature\":\"signature-a\""));
    }

    #[test]
    fn api_gateway_integration_executor_rejects_missing_lambda_dependency()
    {
        let executor = ApiGatewayIntegrationExecutor::new(None, None);
        let invocation = ExecuteApiInvocation::new(
            "api-id".to_owned(),
            ExecuteApiIntegrationPlan::LambdaProxy(Box::new(
                ExecuteApiLambdaProxyPlan::new(
                    LambdaFunctionTarget::parse(
                        "arn:aws:lambda:eu-west-2:000000000000:function:demo",
                    )
                    .expect("lambda target should parse"),
                    br#"{"path":"/pets"}"#.to_vec(),
                    ExecuteApiSourceArn::from_resource(
                        &region_id().expect("region should parse"),
                        &account_id().expect("account should parse"),
                        "api-id/dev/GET/pets",
                    )
                    .expect("source ARN should build"),
                    false,
                ),
            )),
            "/dev/pets".to_owned(),
            "resource-id".to_owned(),
            "/pets".to_owned(),
            "dev".to_owned(),
        );

        let error = executor
            .execute(
                &ApiGatewayScope::new(
                    account_id().expect("account should parse"),
                    region_id().expect("region should parse"),
                ),
                &invocation,
            )
            .expect_err("missing Lambda dependency should fail");

        assert_eq!(
            error,
            ExecuteApiError::IntegrationFailure {
                message: "Internal server error".to_owned(),
                status_code: 500,
            }
        );
    }

    #[test]
    fn lambda_step_functions_task_adapter_invokes_lambda_and_returns_json() {
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "step-functions-success",
            Arc::new(
                FakeLambdaExecutor::success(json!({ "ok": true }))
                    .expect("payload should serialize"),
            ),
            sqs,
        )
        .expect("lambda service should build");
        let function_arn =
            "arn:aws:lambda:eu-west-2:000000000000:function:demo".to_owned();
        let service = step_functions_service(
            "success",
            Arc::new(LambdaStepFunctionsTaskAdapter::new(Some(lambda))),
        );
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": function_arn,
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                CreateStateMachineInput {
                    definition,
                    name: "success".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                StartExecutionInput {
                    input: Some(json!({ "value": 1 }).to_string()),
                    name: Some("run-1".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Succeeded);
        assert_eq!(described.output, Some(json!({ "ok": true }).to_string()));
    }

    #[test]
    fn lambda_step_functions_task_adapter_surfaces_function_errors() {
        let sqs = SqsService::new();
        let lambda = lambda_service(
            "step-functions-error",
            Arc::new(FakeLambdaExecutor::function_error(
                "{\"errorMessage\":\"boom\"}",
                "Handled",
            )),
            sqs,
        )
        .expect("lambda service should build");
        let function_arn =
            "arn:aws:lambda:eu-west-2:000000000000:function:demo".to_owned();
        let service = step_functions_service(
            "error",
            Arc::new(LambdaStepFunctionsTaskAdapter::new(Some(lambda))),
        );
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": function_arn,
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                CreateStateMachineInput {
                    definition,
                    name: "failure".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                StartExecutionInput {
                    input: None,
                    name: Some("run-2".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Failed);
        assert_eq!(described.error.as_deref(), Some("Handled"));
        assert_eq!(
            described.cause.as_deref(),
            Some("{\"errorMessage\":\"boom\"}")
        );
    }

    #[test]
    fn lambda_step_functions_task_adapter_rejects_missing_lambda_dependency() {
        let function_arn =
            "arn:aws:lambda:eu-west-2:000000000000:function:demo".to_owned();
        let service = step_functions_service(
            "missing",
            Arc::new(LambdaStepFunctionsTaskAdapter::new(None)),
        );
        let definition = json!({
            "StartAt": "Task",
            "States": {
                "Task": {
                    "Type": "Task",
                    "Resource": function_arn,
                    "End": true
                }
            }
        })
        .to_string();

        let created = service
            .create_state_machine(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                CreateStateMachineInput {
                    definition,
                    name: "missing".to_owned(),
                    role_arn: "arn:aws:iam::000000000000:role/demo".to_owned(),
                    state_machine_type: None,
                },
            )
            .expect("state machine should create");
        let started = service
            .start_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                StartExecutionInput {
                    input: None,
                    name: Some("run-missing".to_owned()),
                    state_machine_arn: created.state_machine_arn,
                    trace_header: None,
                },
            )
            .expect("execution should start");
        let described = service
            .describe_execution(
                &step_functions_scope()
                    .expect("Step Functions scope should build"),
                DescribeExecutionInput {
                    execution_arn: started.execution_arn,
                },
            )
            .expect("execution should describe");

        assert_eq!(described.status, ExecutionStatus::Failed);
        assert_eq!(
            described.error.as_deref(),
            Some("ResourceNotFoundException")
        );
        assert_eq!(
            described.cause.as_deref(),
            Some("Lambda service is disabled.")
        );
    }
}
