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
mod s3_bucket_encoding;
mod s3_bucket_request_parsing;
mod s3_bucket_routes;
mod s3_copy_parsing;
mod s3_error_encoding;
mod s3_object_encoding;
mod s3_object_lock_parsing;
mod s3_object_reads;
mod s3_object_request_parsing;
mod s3_object_responses;
mod s3_object_routes;
mod s3_query_parsing;
mod s3_request_normalization;
mod s3_request_parsing;
mod s3_response_encoding;
mod s3_select_encoding;
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
