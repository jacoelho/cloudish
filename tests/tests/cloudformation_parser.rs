#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_cloudformation::Client as CloudFormationClient;
use aws_sdk_cloudformation::error::ProvideErrorMetadata;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

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
    let runtime = RuntimeServer::spawn("sdk-cloudformation-parser").await;
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

    runtime.shutdown().await;
}

#[tokio::test]
async fn cloudformation_parser_validate_template_accepts_supported_get_att() {
    let runtime =
        RuntimeServer::spawn("sdk-cloudformation-parser-getatt").await;
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

    runtime.shutdown().await;
}

#[tokio::test]
async fn cloudformation_parser_validate_template_rejects_unsupported_get_att()
{
    let runtime =
        RuntimeServer::spawn("sdk-cloudformation-parser-getatt-bogus").await;
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

    runtime.shutdown().await;
}
