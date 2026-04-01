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
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("cloudformation_parser");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

const SUCCESS_TEMPLATE: &str = r#"Description: parser story
Parameters:
  Prefix:
    Type: String
    Default: demo
Conditions:
  UseRegion: !Equals [!Ref AWS::Region, eu-west-2]
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Join
        - "-"
        - - !Ref Prefix
          - !If
            - UseRegion
            - !Sub "${AWS::StackName}-${AWS::Region}"
            - fallback
Outputs:
  Summary:
    Value: !Sub "${Prefix}-${AWS::AccountId}"
"#;

const SUPPORTED_GET_ATT_TEMPLATE: &str = r#"Resources:
  Queue:
    Type: AWS::SQS::Queue
Outputs:
  QueueArn:
    Value: !GetAtt Queue.Arn
"#;

const UNSUPPORTED_GET_ATT_TEMPLATE: &str = r#"Resources:
  Queue:
    Type: AWS::SQS::Queue
Outputs:
  QueueBogus:
    Value: !GetAtt Queue.Bogus
"#;

#[tokio::test]
async fn cloudformation_parser_validate_template_reports_description_and_parameters()
 {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CloudFormationClient::new(&config);

    let output = client
        .validate_template()
        .template_body(SUCCESS_TEMPLATE)
        .send()
        .await
        .expect("ValidateTemplate should succeed");

    assert_eq!(output.description(), Some("parser story"));
    assert!(output.capabilities().is_empty());
    assert_eq!(output.capabilities_reason(), None);
    assert_eq!(output.parameters().len(), 1);
    let parameter = &output.parameters()[0];
    assert_eq!(parameter.parameter_key(), Some("Prefix"));
    assert_eq!(parameter.default_value(), Some("demo"));
    assert_eq!(parameter.no_echo(), Some(false));
}

#[tokio::test]
async fn cloudformation_parser_validate_template_accepts_supported_get_att() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CloudFormationClient::new(&config);

    let output = client
        .validate_template()
        .template_body(SUPPORTED_GET_ATT_TEMPLATE)
        .send()
        .await
        .expect("supported Fn::GetAtt should validate");

    assert!(output.parameters().is_empty());
}

#[tokio::test]
async fn cloudformation_parser_validate_template_rejects_unsupported_get_att()
{
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CloudFormationClient::new(&config);

    let error = client
        .validate_template()
        .template_body(UNSUPPORTED_GET_ATT_TEMPLATE)
        .send()
        .await
        .expect_err("unsupported Fn::GetAtt attribute should fail");

    assert_eq!(error.code(), Some("ValidationError"));
    assert!(error.message().is_some_and(|message| {
        message.contains("Fn::GetAtt attribute Bogus")
    }));
}
