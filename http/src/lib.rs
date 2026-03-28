#![forbid(unsafe_code)]
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

extern crate self as services;

#[cfg(feature = "apigateway")]
mod apigateway;
mod aws_error_shape;
#[cfg(feature = "cloudformation")]
mod cloudformation;
#[cfg(feature = "cloudwatch")]
mod cloudwatch;
#[cfg(feature = "cognito")]
mod cognito;
#[cfg(feature = "dynamodb")]
mod dynamodb;
#[cfg(feature = "elasticache")]
mod elasticache;
#[cfg(feature = "eventbridge")]
mod eventbridge;
mod iam_query;
#[cfg(feature = "kinesis")]
mod kinesis;
#[cfg(feature = "kms")]
mod kms;
#[cfg(feature = "lambda")]
mod lambda;
mod query;
#[cfg(feature = "rds")]
mod rds;
mod request;
mod runtime;
#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
mod s3_xml;
#[cfg(feature = "secrets-manager")]
mod secrets_manager;
#[path = "services.rs"]
mod service_api;
#[cfg(feature = "sns")]
mod sns;
#[cfg(feature = "sqs")]
mod sqs;
#[cfg(feature = "ssm")]
mod ssm;
#[cfg(feature = "step-functions")]
mod step_functions;
mod sts_query;
#[cfg(test)]
mod test_runtime;
mod xml;

pub use runtime::{EdgeResponse, EdgeRouter};
pub use service_api::*;
