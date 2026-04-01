#![cfg_attr(
    test,
    allow(
        clippy::unreachable,
        clippy::assertions_on_constants,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc
    )
)]

extern crate self as services;

mod apigateway;
mod aws_error_shape;
mod cloudformation;
mod cloudwatch;
mod cognito;
mod dynamodb;
mod elasticache;
mod eventbridge;
mod iam_query;
mod kinesis;
mod kms;
mod lambda;
mod query;
mod rds;
mod request;
mod routing;
mod runtime;
mod s3;
mod s3_xml;
mod secrets_manager;
#[path = "services.rs"]
mod service_api;
mod sns;
mod sqs;
mod ssm;
mod step_functions;
mod sts_query;
#[cfg(test)]
mod test_runtime;
mod xml;

pub use request::EdgeRequest;
pub use runtime::{EdgeRequestExecutor, EdgeResponse, EdgeRouter};
pub use service_api::*;
