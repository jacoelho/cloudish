#[cfg(feature = "apigateway")]
use crate::apigateway;
#[cfg(feature = "cloudformation")]
use crate::cloudformation;
#[cfg(feature = "cloudwatch")]
use crate::cloudwatch;
#[cfg(feature = "cognito")]
use crate::cognito;
#[cfg(feature = "dynamodb")]
use crate::dynamodb;
#[cfg(feature = "elasticache")]
use crate::elasticache;
#[cfg(feature = "eventbridge")]
use crate::eventbridge;
use crate::iam_query;
#[cfg(feature = "kinesis")]
use crate::kinesis;
#[cfg(feature = "kms")]
use crate::kms;
#[cfg(feature = "lambda")]
use crate::lambda;
#[cfg(feature = "rds")]
use crate::rds;
use crate::runtime::{EdgeResponse, EdgeRouter};
#[cfg(feature = "secrets-manager")]
use crate::secrets_manager;
#[cfg(feature = "sns")]
use crate::sns;
#[cfg(feature = "sqs")]
use crate::sqs;
#[cfg(feature = "ssm")]
use crate::ssm;
#[cfg(feature = "step-functions")]
use crate::step_functions;
use crate::sts_query;
use auth::VerifiedRequest;
use aws::{AwsError, ProtocolFamily, RequestContext, ServiceName};
use edge_protocol::HttpRequest;
use std::sync::atomic::AtomicBool;

struct QueryDispatch {
    service: ServiceName,
    dispatch: fn(
        &EdgeRouter,
        &HttpRequest<'_>,
        &RequestContext,
        Option<&VerifiedRequest>,
        &AtomicBool,
    ) -> EdgeResponse,
}

struct JsonDispatch {
    service: ServiceName,
    dispatch: fn(
        &EdgeRouter,
        &HttpRequest<'_>,
        &RequestContext,
        &AtomicBool,
    ) -> EdgeResponse,
}

struct SmithyDispatch {
    service: ServiceName,
    dispatch:
        fn(&EdgeRouter, &HttpRequest<'_>, &RequestContext) -> EdgeResponse,
}

struct RestJsonDispatch {
    service: ServiceName,
    dispatch: fn(
        &EdgeRouter,
        &HttpRequest<'_>,
        &RequestContext,
        &AtomicBool,
    ) -> EdgeResponse,
}

const QUERY_DISPATCHES: &[QueryDispatch] = &[
    QueryDispatch { service: ServiceName::Iam, dispatch: dispatch_iam_query },
    QueryDispatch { service: ServiceName::Sts, dispatch: dispatch_sts_query },
    #[cfg(feature = "sns")]
    QueryDispatch { service: ServiceName::Sns, dispatch: dispatch_sns_query },
    #[cfg(feature = "sqs")]
    QueryDispatch { service: ServiceName::Sqs, dispatch: dispatch_sqs_query },
    #[cfg(feature = "cloudformation")]
    QueryDispatch {
        service: ServiceName::CloudFormation,
        dispatch: dispatch_cloudformation_query,
    },
    #[cfg(feature = "cloudwatch")]
    QueryDispatch {
        service: ServiceName::CloudWatch,
        dispatch: dispatch_cloudwatch_query,
    },
    #[cfg(feature = "rds")]
    QueryDispatch { service: ServiceName::Rds, dispatch: dispatch_rds_query },
    #[cfg(feature = "elasticache")]
    QueryDispatch {
        service: ServiceName::ElastiCache,
        dispatch: dispatch_elasticache_query,
    },
];

const JSON_DISPATCHES: &[JsonDispatch] = &[
    #[cfg(feature = "dynamodb")]
    JsonDispatch {
        service: ServiceName::DynamoDb,
        dispatch: dispatch_dynamodb_json,
    },
    #[cfg(feature = "sqs")]
    JsonDispatch { service: ServiceName::Sqs, dispatch: dispatch_sqs_json },
    #[cfg(feature = "sns")]
    JsonDispatch { service: ServiceName::Sns, dispatch: dispatch_sns_json },
    #[cfg(feature = "step-functions")]
    JsonDispatch {
        service: ServiceName::StepFunctions,
        dispatch: dispatch_step_functions_json,
    },
    #[cfg(feature = "cloudwatch")]
    JsonDispatch {
        service: ServiceName::CloudWatch,
        dispatch: dispatch_cloudwatch_metrics_json,
    },
    #[cfg(feature = "ssm")]
    JsonDispatch { service: ServiceName::Ssm, dispatch: dispatch_ssm_json },
    #[cfg(feature = "eventbridge")]
    JsonDispatch {
        service: ServiceName::EventBridge,
        dispatch: dispatch_eventbridge_json,
    },
    #[cfg(feature = "cloudwatch")]
    JsonDispatch {
        service: ServiceName::Logs,
        dispatch: dispatch_cloudwatch_logs_json,
    },
    #[cfg(feature = "secrets-manager")]
    JsonDispatch {
        service: ServiceName::SecretsManager,
        dispatch: dispatch_secrets_manager_json,
    },
    #[cfg(feature = "kinesis")]
    JsonDispatch {
        service: ServiceName::Kinesis,
        dispatch: dispatch_kinesis_json,
    },
    #[cfg(feature = "kms")]
    JsonDispatch { service: ServiceName::Kms, dispatch: dispatch_kms_json },
    #[cfg(feature = "cognito")]
    JsonDispatch {
        service: ServiceName::CognitoIdentityProvider,
        dispatch: dispatch_cognito_json,
    },
];

const SMITHY_DISPATCHES: &[SmithyDispatch] = &[
    #[cfg(feature = "cloudwatch")]
    SmithyDispatch {
        service: ServiceName::CloudWatch,
        dispatch: dispatch_cloudwatch_cbor,
    },
];

const REST_JSON_DISPATCHES: &[RestJsonDispatch] = &[
    #[cfg(feature = "lambda")]
    RestJsonDispatch {
        service: ServiceName::Lambda,
        dispatch: dispatch_lambda_rest_json,
    },
    #[cfg(feature = "apigateway")]
    RestJsonDispatch {
        service: ServiceName::ApiGateway,
        dispatch: dispatch_apigateway_rest_json,
    },
];

pub(crate) fn dispatch_query(
    router: &EdgeRouter,
    service: ServiceName,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    verified_request: Option<&VerifiedRequest>,
    request_cancellation: &AtomicBool,
) -> Option<EdgeResponse> {
    QUERY_DISPATCHES.iter().find_map(|dispatch| {
        (dispatch.service == service).then(|| {
            (dispatch.dispatch)(
                router,
                request,
                context,
                verified_request,
                request_cancellation,
            )
        })
    })
}

pub(crate) fn dispatch_json(
    router: &EdgeRouter,
    service: ServiceName,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    request_cancellation: &AtomicBool,
) -> Option<EdgeResponse> {
    JSON_DISPATCHES.iter().find_map(|dispatch| {
        (dispatch.service == service).then(|| {
            (dispatch.dispatch)(router, request, context, request_cancellation)
        })
    })
}

pub(crate) fn dispatch_smithy(
    router: &EdgeRouter,
    service: ServiceName,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Option<EdgeResponse> {
    SMITHY_DISPATCHES.iter().find_map(|dispatch| {
        (dispatch.service == service)
            .then(|| (dispatch.dispatch)(router, request, context))
    })
}

pub(crate) fn dispatch_rest_json(
    router: &EdgeRouter,
    service: ServiceName,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    request_cancellation: &AtomicBool,
) -> Option<EdgeResponse> {
    REST_JSON_DISPATCHES.iter().find_map(|dispatch| {
        (dispatch.service == service).then(|| {
            (dispatch.dispatch)(router, request, context, request_cancellation)
        })
    })
}

fn query_ok(result: Result<String, AwsError>) -> EdgeResponse {
    match result {
        Ok(body) => EdgeResponse::bytes(200, "text/xml", body.into_bytes()),
        Err(error) => EdgeResponse::aws(ProtocolFamily::Query, &error),
    }
}

#[cfg(any(
    feature = "cloudwatch",
    feature = "cognito",
    feature = "dynamodb",
    feature = "eventbridge",
    feature = "kinesis",
    feature = "kms",
    feature = "secrets-manager",
    feature = "sns",
    feature = "sqs",
    feature = "ssm",
    feature = "step-functions"
))]
fn json_ok(
    protocol: ProtocolFamily,
    content_type: &str,
    result: Result<Vec<u8>, AwsError>,
) -> EdgeResponse {
    match result {
        Ok(body) => EdgeResponse::bytes(200, content_type, body),
        Err(error) => EdgeResponse::aws(protocol, &error),
    }
}

fn dispatch_iam_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(iam_query::handle(
        router.runtime_services().iam(),
        request.body(),
        context,
    ))
}

fn dispatch_sts_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(sts_query::handle(
        router.runtime_services().sts(),
        request.body(),
        context,
        verified_request,
    ))
}

#[cfg(feature = "sns")]
fn dispatch_sns_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(sns::handle_query(
        router.runtime_services().sns(),
        request,
        context,
    ))
}

#[cfg(feature = "sqs")]
fn dispatch_sqs_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    request_cancellation: &AtomicBool,
) -> EdgeResponse {
    let advertised_edge = router.advertised_edge().current();
    let sqs_requests = router.runtime_services().sqs_requests();
    query_ok(sqs::handle_query(
        router.runtime_services().sqs(),
        &sqs_requests,
        &advertised_edge,
        request,
        context,
        request_cancellation,
    ))
}

#[cfg(feature = "cloudformation")]
fn dispatch_cloudformation_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(cloudformation::handle_query(
        router.runtime_services().cloudformation(),
        request.body(),
        context,
    ))
}

#[cfg(feature = "cloudwatch")]
fn dispatch_cloudwatch_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(cloudwatch::handle_metrics_query(
        router.runtime_services().cloudwatch(),
        request,
        context,
    ))
}

#[cfg(feature = "rds")]
fn dispatch_rds_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(rds::handle_query(
        router.runtime_services().rds(),
        request,
        context,
    ))
}

#[cfg(feature = "elasticache")]
fn dispatch_elasticache_query(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _verified_request: Option<&VerifiedRequest>,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    query_ok(elasticache::handle_query(
        router.runtime_services().elasticache(),
        request,
        context,
    ))
}

#[cfg(feature = "sqs")]
fn dispatch_sqs_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    request_cancellation: &AtomicBool,
) -> EdgeResponse {
    let advertised_edge = router.advertised_edge().current();
    let sqs_requests = router.runtime_services().sqs_requests();
    json_ok(
        ProtocolFamily::AwsJson10,
        "application/x-amz-json-1.0",
        sqs::handle_json(
            router.runtime_services().sqs(),
            &sqs_requests,
            &advertised_edge,
            request,
            context,
            request_cancellation,
        ),
    )
}

#[cfg(feature = "sns")]
fn dispatch_sns_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson10,
        "application/x-amz-json-1.0",
        sns::handle_json(router.runtime_services().sns(), request, context),
    )
}

#[cfg(feature = "dynamodb")]
fn dispatch_dynamodb_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    match dynamodb::handle_json(
        router.runtime_services().dynamodb(),
        request,
        context,
    ) {
        Ok(body) => {
            EdgeResponse::bytes(200, "application/x-amz-json-1.0", body)
        }
        Err(error) => {
            if let Some(body) = error.body() {
                EdgeResponse::bytes(
                    error.error().status_code(),
                    "application/x-amz-json-1.0",
                    body.to_vec(),
                )
                .set_header("x-amzn-errortype", error.code())
            } else {
                EdgeResponse::aws(ProtocolFamily::AwsJson10, error.error())
            }
        }
    }
}

#[cfg(feature = "ssm")]
fn dispatch_ssm_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        ssm::handle_json(router.runtime_services().ssm(), request, context),
    )
}

#[cfg(feature = "cognito")]
fn dispatch_cognito_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        cognito::handle_json(
            router.runtime_services().cognito(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "kinesis")]
fn dispatch_kinesis_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        kinesis::handle_json(
            router.runtime_services().kinesis(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "secrets-manager")]
fn dispatch_secrets_manager_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        secrets_manager::handle_json(
            router.runtime_services().secrets_manager(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "kms")]
fn dispatch_kms_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        kms::handle_json(router.runtime_services().kms(), request, context),
    )
}

#[cfg(feature = "cloudwatch")]
fn dispatch_cloudwatch_metrics_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson10,
        "application/x-amz-json-1.0",
        cloudwatch::handle_metrics_json(
            router.runtime_services().cloudwatch(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "cloudwatch")]
fn dispatch_cloudwatch_logs_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        cloudwatch::handle_logs_json(
            router.runtime_services().cloudwatch(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "step-functions")]
fn dispatch_step_functions_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson10,
        "application/x-amz-json-1.0",
        step_functions::handle_json(
            router.runtime_services().step_functions(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "eventbridge")]
fn dispatch_eventbridge_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    json_ok(
        ProtocolFamily::AwsJson11,
        "application/x-amz-json-1.1",
        eventbridge::handle_json(
            router.runtime_services().eventbridge(),
            request,
            context,
        ),
    )
}

#[cfg(feature = "cloudwatch")]
fn dispatch_cloudwatch_cbor(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> EdgeResponse {
    match cloudwatch::handle_metrics_cbor(
        router.runtime_services().cloudwatch(),
        request,
        context,
    ) {
        Ok(body) => EdgeResponse::bytes(200, "application/cbor", body)
            .with_header("Smithy-Protocol", "rpc-v2-cbor"),
        Err(error) => {
            EdgeResponse::aws(ProtocolFamily::SmithyRpcV2Cbor, &error)
        }
    }
}

#[cfg(feature = "lambda")]
fn dispatch_lambda_rest_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    request_cancellation: &AtomicBool,
) -> EdgeResponse {
    let advertised_edge = router.advertised_edge().current();
    let lambda_requests = router.runtime_services().lambda_requests();
    match lambda::handle_rest_json(
        router.runtime_services().lambda(),
        &lambda_requests,
        &advertised_edge,
        request,
        context,
        request_cancellation,
    ) {
        Ok(response) => response,
        Err(error) => EdgeResponse::aws(ProtocolFamily::RestJson, &error),
    }
}

#[cfg(feature = "apigateway")]
fn dispatch_apigateway_rest_json(
    router: &EdgeRouter,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    _request_cancellation: &AtomicBool,
) -> EdgeResponse {
    match apigateway::handle_rest_json(
        router.runtime_services().apigateway(),
        request,
        context,
    ) {
        Ok(response) => response,
        Err(error) => EdgeResponse::aws(ProtocolFamily::RestJson, &error),
    }
}
