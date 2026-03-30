use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub enum ServiceName {
    #[serde(rename = "apigateway")]
    ApiGateway,
    #[serde(rename = "cloudformation")]
    CloudFormation,
    #[serde(rename = "cloudwatch")]
    CloudWatch,
    #[serde(rename = "cognito-idp")]
    CognitoIdentityProvider,
    #[serde(rename = "dynamodb")]
    DynamoDb,
    #[serde(rename = "elasticache")]
    ElastiCache,
    #[serde(rename = "events")]
    EventBridge,
    #[serde(rename = "iam")]
    Iam,
    #[serde(rename = "kinesis")]
    Kinesis,
    #[serde(rename = "kms")]
    Kms,
    #[serde(rename = "lambda")]
    Lambda,
    #[serde(rename = "logs")]
    Logs,
    #[serde(rename = "rds")]
    Rds,
    #[serde(rename = "rds-db")]
    RdsDb,
    #[serde(rename = "s3")]
    S3,
    #[serde(rename = "secretsmanager")]
    SecretsManager,
    #[serde(rename = "sns")]
    Sns,
    #[serde(rename = "sqs")]
    Sqs,
    #[serde(rename = "ssm")]
    Ssm,
    #[serde(rename = "states")]
    StepFunctions,
    #[serde(rename = "sts")]
    Sts,
}

impl ServiceName {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApiGateway => "apigateway",
            Self::CloudFormation => "cloudformation",
            Self::CloudWatch => "cloudwatch",
            Self::CognitoIdentityProvider => "cognito-idp",
            Self::DynamoDb => "dynamodb",
            Self::ElastiCache => "elasticache",
            Self::EventBridge => "events",
            Self::Iam => "iam",
            Self::Kinesis => "kinesis",
            Self::Kms => "kms",
            Self::Lambda => "lambda",
            Self::Logs => "logs",
            Self::Rds => "rds",
            Self::RdsDb => "rds-db",
            Self::S3 => "s3",
            Self::SecretsManager => "secretsmanager",
            Self::Sns => "sns",
            Self::Sqs => "sqs",
            Self::Ssm => "ssm",
            Self::StepFunctions => "states",
            Self::Sts => "sts",
        }
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ServiceName {
    type Err = ServiceNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "apigateway" => Ok(Self::ApiGateway),
            "execute-api" => Ok(Self::ApiGateway),
            "cloudformation" => Ok(Self::CloudFormation),
            "cloudwatch" | "monitoring" => Ok(Self::CloudWatch),
            "cognito-idp" => Ok(Self::CognitoIdentityProvider),
            "dynamodb" => Ok(Self::DynamoDb),
            "elasticache" => Ok(Self::ElastiCache),
            "events" => Ok(Self::EventBridge),
            "iam" => Ok(Self::Iam),
            "kinesis" => Ok(Self::Kinesis),
            "kms" => Ok(Self::Kms),
            "lambda" => Ok(Self::Lambda),
            "logs" => Ok(Self::Logs),
            "rds" => Ok(Self::Rds),
            "rds-db" => Ok(Self::RdsDb),
            "s3" => Ok(Self::S3),
            "secretsmanager" => Ok(Self::SecretsManager),
            "sns" => Ok(Self::Sns),
            "sqs" => Ok(Self::Sqs),
            "ssm" => Ok(Self::Ssm),
            "states" => Ok(Self::StepFunctions),
            "sts" => Ok(Self::Sts),
            _ => {
                Err(ServiceNameError::Unsupported { value: value.to_owned() })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServiceNameError {
    #[error("unsupported AWS service identifier {value:?}")]
    Unsupported { value: String },
}

#[cfg(test)]
mod tests {
    use super::{ServiceName, ServiceNameError};

    #[test]
    fn parse_supported_service_names() {
        let cases = [
            (ServiceName::ApiGateway, "apigateway"),
            (ServiceName::CloudFormation, "cloudformation"),
            (ServiceName::CloudWatch, "cloudwatch"),
            (ServiceName::CognitoIdentityProvider, "cognito-idp"),
            (ServiceName::DynamoDb, "dynamodb"),
            (ServiceName::ElastiCache, "elasticache"),
            (ServiceName::EventBridge, "events"),
            (ServiceName::Iam, "iam"),
            (ServiceName::Kinesis, "kinesis"),
            (ServiceName::Kms, "kms"),
            (ServiceName::Lambda, "lambda"),
            (ServiceName::Logs, "logs"),
            (ServiceName::Rds, "rds"),
            (ServiceName::RdsDb, "rds-db"),
            (ServiceName::S3, "s3"),
            (ServiceName::SecretsManager, "secretsmanager"),
            (ServiceName::Sns, "sns"),
            (ServiceName::Sqs, "sqs"),
            (ServiceName::Ssm, "ssm"),
            (ServiceName::StepFunctions, "states"),
            (ServiceName::Sts, "sts"),
        ];

        for (service, value) in cases {
            assert_eq!(
                value
                    .parse::<ServiceName>()
                    .expect("supported service should parse"),
                service
            );
            assert_eq!(service.as_str(), value);
            assert_eq!(service.to_string(), value);
        }
    }

    #[test]
    fn parse_cloudwatch_monitoring_alias() {
        assert_eq!(
            "monitoring"
                .parse::<ServiceName>()
                .expect("CloudWatch monitoring alias should parse"),
            ServiceName::CloudWatch
        );
        assert_eq!(ServiceName::CloudWatch.as_str(), "cloudwatch");
    }

    #[test]
    fn parse_execute_api_alias_as_api_gateway() {
        assert_eq!(
            "execute-api"
                .parse::<ServiceName>()
                .expect("execute-api alias should parse"),
            ServiceName::ApiGateway
        );
        assert_eq!(ServiceName::ApiGateway.as_str(), "apigateway");
    }

    #[test]
    fn reject_unsupported_service_names() {
        let error = "ec2"
            .parse::<ServiceName>()
            .expect_err("unsupported service should fail");

        assert_eq!(
            error,
            ServiceNameError::Unsupported { value: "ec2".to_owned() }
        );
        assert_eq!(
            error.to_string(),
            "unsupported AWS service identifier \"ec2\""
        );
    }
}
