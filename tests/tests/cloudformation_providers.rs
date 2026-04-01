#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_cloudformation::Client as CloudFormationClient;
use aws_sdk_cloudformation::error::ProvideErrorMetadata;
use aws_sdk_cloudformation::types::{Capability, ChangeSetType};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_kms::Client as KmsClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_secretsmanager::Client as SecretsManagerClient;
use aws_sdk_sns::Client as SnsClient;
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_ssm::Client as SsmClient;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("cloudformation_providers");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

const COMPREHENSIVE_TEMPLATE: &str = r#"Resources:
  AppBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: sdk-provider-bucket
  AppBucketPolicy:
    Type: AWS::S3::BucketPolicy
    Properties:
      Bucket: !Ref AppBucket
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal: "*"
            Action: s3:GetObject
            Resource: !Sub "arn:aws:s3:::${AppBucket}/*"
  AppQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: sdk-provider-queue
  AppQueuePolicy:
    Type: AWS::SQS::QueuePolicy
    Properties:
      Queues:
        - !Ref AppQueue
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal: "*"
            Action: sqs:SendMessage
            Resource: !GetAtt AppQueue.Arn
  AppTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: sdk-provider-topic
  AppRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: sdk-provider-role
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
  AppUser:
    Type: AWS::IAM::User
    Properties:
      UserName: sdk-provider-user
  AppAccessKey:
    Type: AWS::IAM::AccessKey
    Properties:
      UserName: !Ref AppUser
  AppPolicy:
    Type: AWS::IAM::Policy
    Properties:
      PolicyName: sdk-provider-inline
      Roles:
        - !Ref AppRole
      Users:
        - !Ref AppUser
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Action: sqs:ReceiveMessage
            Resource: !GetAtt AppQueue.Arn
  AppManagedPolicy:
    Type: AWS::IAM::ManagedPolicy
    Properties:
      ManagedPolicyName: sdk-provider-managed
      Roles:
        - !Ref AppRole
      Users:
        - !Ref AppUser
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Action: s3:ListBucket
            Resource: !GetAtt AppBucket.Arn
  AppProfile:
    Type: AWS::IAM::InstanceProfile
    Properties:
      InstanceProfileName: sdk-provider-profile
      Roles:
        - !Ref AppRole
  AppTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: sdk-provider-table
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: pk
          AttributeType: S
      KeySchema:
        - AttributeName: pk
          KeyType: HASH
  AppParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: /sdk/provider/value
      Type: String
      Value: ready
  AppKey:
    Type: AWS::KMS::Key
    Properties:
      Description: sdk-provider-key
  AppAlias:
    Type: AWS::KMS::Alias
    Properties:
      AliasName: alias/sdk-provider-key
      TargetKeyId: !Ref AppKey
  AppSecret:
    Type: AWS::SecretsManager::Secret
    Properties:
      Name: sdk/provider/secret
      SecretString: super-secret
  AppFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: sdk-provider-function
      Role: !GetAtt AppRole.Arn
      Runtime: python3.11
      Handler: index.handler
      Code:
        ZipFile: |
          def handler(event, context):
              return {"statusCode": 200, "body": "ok"}
Outputs:
  QueueArn:
    Value: !GetAtt AppQueue.Arn
  TopicArn:
    Value: !Ref AppTopic
  SecretArn:
    Value: !Ref AppSecret
"#;

const CHANGE_SET_BASE_TEMPLATE: &str = r#"Resources:
  AppParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: /sdk/change-set/value
      Type: String
      Value: before
"#;

const CHANGE_SET_UPDATED_TEMPLATE: &str = r#"Resources:
  AppParameter:
    Type: AWS::SSM::Parameter
    Properties:
      Name: /sdk/change-set/value
      Type: String
      Value: after
"#;

const UNSUPPORTED_TEMPLATE: &str = r#"Resources:
  Nested:
    Type: AWS::CloudFormation::Stack
    Properties: {}
"#;

async fn s3_client(target: &SdkSmokeTarget) -> S3Client {
    let shared = target.load().await;
    let config = S3ConfigBuilder::from(&shared).force_path_style(true).build();
    S3Client::from_conf(config)
}

#[tokio::test]
async fn cloudformation_providers_stack_lifecycle_provisions_supported_catalog()
 {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let s3_target =
        SdkSmokeTarget::new(runtime.localhost_endpoint_url(), "eu-west-2");
    let shared = target.load().await;
    let cloudformation = CloudFormationClient::new(&shared);
    let dynamodb = DynamoDbClient::new(&shared);
    let iam = IamClient::new(&shared);
    let kms = KmsClient::new(&shared);
    let lambda = LambdaClient::new(&shared);
    let s3 = s3_client(&s3_target).await;
    let secrets_manager = SecretsManagerClient::new(&shared);
    let sns = SnsClient::new(&shared);
    let sqs = SqsClient::new(&shared);
    let ssm = SsmClient::new(&shared);

    let created = cloudformation
        .create_stack()
        .stack_name("sdk-provider-stack")
        .template_body(COMPREHENSIVE_TEMPLATE)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
        .expect("stack should be created");
    assert!(created.stack_id().is_some());

    let described = cloudformation
        .describe_stacks()
        .stack_name("sdk-provider-stack")
        .send()
        .await
        .expect("stack should describe");
    let stack = &described.stacks()[0];
    assert_eq!(
        stack.stack_status().map(|value| value.as_str()),
        Some("CREATE_COMPLETE")
    );
    assert_eq!(stack.outputs().len(), 3);

    let buckets = s3.list_buckets().send().await.expect("buckets should list");
    assert!(
        buckets
            .buckets()
            .iter()
            .any(|bucket| { bucket.name() == Some("sdk-provider-bucket") })
    );
    let bucket_policy = s3
        .get_bucket_policy()
        .bucket("sdk-provider-bucket")
        .send()
        .await
        .expect("bucket policy should exist");
    assert!(bucket_policy.policy().is_some_and(|policy| {
        policy.contains("s3:GetObject")
            && policy.contains("sdk-provider-bucket")
    }));

    let queue_url = sqs
        .get_queue_url()
        .queue_name("sdk-provider-queue")
        .send()
        .await
        .expect("queue URL should resolve")
        .queue_url()
        .expect("queue URL should be present")
        .to_owned();
    let queue_attributes = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::Policy)
        .send()
        .await
        .expect("queue attributes should fetch");
    assert_eq!(
        queue_attributes
            .attributes()
            .and_then(|attributes| {
                attributes
                    .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
            })
            .map(String::as_str),
        Some("arn:aws:sqs:eu-west-2:000000000000:sdk-provider-queue")
    );
    assert!(
        queue_attributes
            .attributes()
            .and_then(|attributes| {
                attributes.get(&aws_sdk_sqs::types::QueueAttributeName::Policy)
            })
            .is_some()
    );

    let topic_arn = stack
        .outputs()
        .iter()
        .find(|output| output.output_key() == Some("TopicArn"))
        .and_then(|output| output.output_value())
        .expect("topic output should exist");
    let topic_attributes = sns
        .get_topic_attributes()
        .topic_arn(topic_arn)
        .send()
        .await
        .expect("topic should exist");
    assert_eq!(
        topic_attributes
            .attributes()
            .and_then(|attributes| attributes.get("TopicArn"))
            .map(String::as_str),
        Some(topic_arn)
    );

    let role = iam
        .get_role()
        .role_name("sdk-provider-role")
        .send()
        .await
        .expect("role should exist");
    assert_eq!(
        role.role().map(|value| value.role_name()),
        Some("sdk-provider-role")
    );
    let user = iam
        .get_user()
        .user_name("sdk-provider-user")
        .send()
        .await
        .expect("user should exist");
    assert_eq!(
        user.user().map(|value| value.user_name()),
        Some("sdk-provider-user")
    );
    let access_keys = iam
        .list_access_keys()
        .user_name("sdk-provider-user")
        .send()
        .await
        .expect("access keys should list");
    assert_eq!(access_keys.access_key_metadata().len(), 1);
    let profile = iam
        .get_instance_profile()
        .instance_profile_name("sdk-provider-profile")
        .send()
        .await
        .expect("instance profile should exist");
    assert_eq!(
        profile.instance_profile().map(|value| value.instance_profile_name()),
        Some("sdk-provider-profile")
    );

    let table = dynamodb
        .describe_table()
        .table_name("sdk-provider-table")
        .send()
        .await
        .expect("table should exist");
    assert_eq!(
        table.table().and_then(|value| value.table_name()).map(str::to_owned),
        Some("sdk-provider-table".to_owned())
    );

    let parameter = ssm
        .get_parameter()
        .name("/sdk/provider/value")
        .send()
        .await
        .expect("parameter should exist");
    assert_eq!(
        parameter
            .parameter()
            .and_then(|value| value.value())
            .map(str::to_owned),
        Some("ready".to_owned())
    );

    let key = kms
        .describe_key()
        .key_id("alias/sdk-provider-key")
        .send()
        .await
        .expect("KMS alias should resolve");
    assert!(key.key_metadata().and_then(|metadata| metadata.arn()).is_some());

    let secret = secrets_manager
        .describe_secret()
        .secret_id("sdk/provider/secret")
        .send()
        .await
        .expect("secret should exist");
    assert_eq!(secret.name(), Some("sdk/provider/secret"));
    let secret_value = secrets_manager
        .get_secret_value()
        .secret_id("sdk/provider/secret")
        .send()
        .await
        .expect("secret value should resolve");
    assert_eq!(secret_value.secret_string(), Some("super-secret"));

    let function = lambda
        .get_function()
        .function_name("sdk-provider-function")
        .send()
        .await
        .expect("function should exist");
    assert_eq!(
        function.configuration().and_then(|value| value.function_name()),
        Some("sdk-provider-function")
    );

    cloudformation
        .delete_stack()
        .stack_name("sdk-provider-stack")
        .send()
        .await
        .expect("stack should delete");

    let deleted_queue = sqs
        .get_queue_url()
        .queue_name("sdk-provider-queue")
        .send()
        .await
        .expect_err("queue should be deleted");
    assert!(deleted_queue.code().is_some());
}

#[tokio::test]
async fn cloudformation_providers_change_set_execution_updates_resources() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let shared = target.load().await;
    let cloudformation = CloudFormationClient::new(&shared);
    let ssm = SsmClient::new(&shared);

    cloudformation
        .create_stack()
        .stack_name("sdk-provider-change-set")
        .template_body(CHANGE_SET_BASE_TEMPLATE)
        .send()
        .await
        .expect("base stack should create");

    let created = cloudformation
        .create_change_set()
        .change_set_name("rename")
        .change_set_type(ChangeSetType::Update)
        .stack_name("sdk-provider-change-set")
        .template_body(CHANGE_SET_UPDATED_TEMPLATE)
        .send()
        .await
        .expect("change set should create");
    assert!(created.id().is_some());

    let described = cloudformation
        .describe_change_set()
        .change_set_name("rename")
        .stack_name("sdk-provider-change-set")
        .send()
        .await
        .expect("change set should describe");
    assert_eq!(described.changes().len(), 1);

    cloudformation
        .execute_change_set()
        .change_set_name("rename")
        .stack_name("sdk-provider-change-set")
        .send()
        .await
        .expect("change set should execute");

    let parameter = ssm
        .get_parameter()
        .name("/sdk/change-set/value")
        .send()
        .await
        .expect("updated parameter should exist");
    assert_eq!(
        parameter
            .parameter()
            .and_then(|value| value.value())
            .map(str::to_owned),
        Some("after".to_owned())
    );
}

#[tokio::test]
async fn cloudformation_providers_reject_unsupported_resource_types() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let shared = target.load().await;
    let cloudformation = CloudFormationClient::new(&shared);

    let error = cloudformation
        .create_stack()
        .stack_name("sdk-provider-unsupported")
        .template_body(UNSUPPORTED_TEMPLATE)
        .send()
        .await
        .expect_err("unsupported resource types should fail");

    assert_eq!(error.code(), Some("ValidationError"));
    assert!(error.message().is_some_and(|message| {
        message.contains("unsupported type AWS::CloudFormation::Stack")
    }));
}
