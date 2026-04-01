use crate::apigateway;
use crate::cloudformation;
use crate::cloudwatch;
use crate::elasticache;
use crate::iam_query;
use crate::lambda;
use crate::rds;
use crate::request::HttpRequest;
use crate::s3;
use crate::sns;
use crate::sqs;
use crate::sts_query;
use aws::{AdvertisedEdge, ProtocolFamily, ServiceName};

const JSON_TARGETS: &[JsonTarget] = &[
    JsonTarget {
        service: ServiceName::DynamoDb,
        protocol: ProtocolFamily::AwsJson10,
        prefixes: &["DynamoDB_20120810.", "DynamoDBStreams_20120810."],
    },
    JsonTarget {
        service: ServiceName::Sqs,
        protocol: ProtocolFamily::AwsJson10,
        prefixes: &["AmazonSQS."],
    },
    JsonTarget {
        service: ServiceName::Sns,
        protocol: ProtocolFamily::AwsJson10,
        prefixes: &["SNS_20100331."],
    },
    JsonTarget {
        service: ServiceName::StepFunctions,
        protocol: ProtocolFamily::AwsJson10,
        prefixes: &["AWSStepFunctions."],
    },
    JsonTarget {
        service: ServiceName::CloudWatch,
        protocol: ProtocolFamily::AwsJson10,
        prefixes: &["GraniteServiceVersion20100801."],
    },
    JsonTarget {
        service: ServiceName::Ssm,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["AmazonSSM."],
    },
    JsonTarget {
        service: ServiceName::EventBridge,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["AWSEvents."],
    },
    JsonTarget {
        service: ServiceName::Logs,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["Logs_20140328."],
    },
    JsonTarget {
        service: ServiceName::SecretsManager,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["secretsmanager."],
    },
    JsonTarget {
        service: ServiceName::Kinesis,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["Kinesis_20131202."],
    },
    JsonTarget {
        service: ServiceName::Kms,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["TrentService."],
    },
    JsonTarget {
        service: ServiceName::CognitoIdentityProvider,
        protocol: ProtocolFamily::AwsJson11,
        prefixes: &["AWSCognitoIdentityProviderService."],
    },
];

const SMITHY_SERVICES: &[SmithyService] = &[SmithyService {
    service: ServiceName::CloudWatch,
    ids: &["GraniteServiceVersion20100801", "CloudWatch", "monitoring"],
}];

struct JsonTarget {
    service: ServiceName,
    protocol: ProtocolFamily,
    prefixes: &'static [&'static str],
}

struct SmithyService {
    service: ServiceName,
    ids: &'static [&'static str],
}

pub(crate) fn detect_generic_protocol(
    request: &HttpRequest<'_>,
    advertised_edge: &AdvertisedEdge,
) -> Option<ProtocolFamily> {
    let content_type = request.header("content-type").unwrap_or_default();
    let smithy_protocol =
        request.header("smithy-protocol").unwrap_or_default();
    let target = request.header("x-amz-target");
    let path = request.path_without_query();
    let s3_candidate = s3::is_rest_xml_request(request, advertised_edge);
    let s3_non_root_request = s3_candidate && path != "/";

    if smithy_path(path).is_some()
        || smithy_protocol.eq_ignore_ascii_case("rpc-v2-cbor")
    {
        return Some(ProtocolFamily::SmithyRpcV2Cbor);
    }

    if content_type.eq_ignore_ascii_case("application/cbor") {
        if s3_non_root_request {
            return None;
        }
        return Some(ProtocolFamily::SmithyRpcV2Cbor);
    }

    if enabled_rest_json_service(request).is_some() {
        return Some(ProtocolFamily::RestJson);
    }

    if !s3_non_root_request
        && compiled_out_rest_json_fallback(request).is_some()
    {
        return Some(ProtocolFamily::RestJson);
    }

    if content_type.contains("application/x-amz-json-1.0") {
        if s3_non_root_request {
            return None;
        }
        return Some(ProtocolFamily::AwsJson10);
    }

    if content_type.contains("application/x-amz-json-1.1") {
        if s3_non_root_request {
            return None;
        }
        return Some(ProtocolFamily::AwsJson11);
    }

    if let Some(target) = target {
        if json_target(ProtocolFamily::AwsJson10, target).is_some() {
            return Some(ProtocolFamily::AwsJson10);
        }
        if json_target(ProtocolFamily::AwsJson11, target).is_some() {
            return Some(ProtocolFamily::AwsJson11);
        }
    }

    if crate::query::is_query_request(request) {
        return Some(ProtocolFamily::Query);
    }

    if s3_non_root_request {
        return None;
    }

    if content_type.contains("application/x-www-form-urlencoded") {
        return Some(ProtocolFamily::Query);
    }

    None
}

pub(crate) fn supports_protocol(
    service: ServiceName,
    protocol: ProtocolFamily,
) -> bool {
    match protocol {
        ProtocolFamily::Query => matches!(
            service,
            ServiceName::CloudFormation
                | ServiceName::CloudWatch
                | ServiceName::ElastiCache
                | ServiceName::Iam
                | ServiceName::Rds
                | ServiceName::Sns
                | ServiceName::Sqs
                | ServiceName::Sts
        ),
        ProtocolFamily::AwsJson10 => matches!(
            service,
            ServiceName::CloudWatch
                | ServiceName::DynamoDb
                | ServiceName::Sns
                | ServiceName::Sqs
                | ServiceName::StepFunctions
        ),
        ProtocolFamily::AwsJson11 => matches!(
            service,
            ServiceName::CognitoIdentityProvider
                | ServiceName::EventBridge
                | ServiceName::Kinesis
                | ServiceName::Kms
                | ServiceName::Logs
                | ServiceName::SecretsManager
                | ServiceName::Ssm
        ),
        ProtocolFamily::RestJson => {
            matches!(service, ServiceName::ApiGateway | ServiceName::Lambda)
        }
        ProtocolFamily::RestXml => service == ServiceName::S3,
        ProtocolFamily::SmithyRpcV2Cbor => service == ServiceName::CloudWatch,
    }
}

pub(crate) fn resolve_unsigned_query_service(
    action: &str,
    version: Option<&str>,
) -> Option<ServiceName> {
    let query_services = [
        ServiceName::Iam,
        ServiceName::Sts,
        ServiceName::Sns,
        ServiceName::Sqs,
        ServiceName::CloudFormation,
        ServiceName::CloudWatch,
        ServiceName::Rds,
        ServiceName::ElastiCache,
    ];
    let mut exact_matches = query_services
        .into_iter()
        .filter(|service| query_action_matches(*service, action, version));
    if let Some(service) = exact_matches.next() {
        return exact_matches.next().is_none().then_some(service);
    }

    let mut action_matches = query_services
        .into_iter()
        .filter(|service| query_service_matches_action(*service, action));
    let service = action_matches.next()?;
    action_matches.next().is_none().then_some(service)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopedQueryValidation {
    Valid,
    ActionOutsideService,
    InvalidVersion,
}

pub(crate) fn validate_query_for_service(
    service: ServiceName,
    action: &str,
    version: &str,
) -> ScopedQueryValidation {
    if !query_service_matches_action(service, action) {
        return ScopedQueryValidation::ActionOutsideService;
    }
    if !query_action_matches_version(service, action, version) {
        return ScopedQueryValidation::InvalidVersion;
    }

    ScopedQueryValidation::Valid
}

pub(crate) fn query_action_matches_version(
    service: ServiceName,
    action: &str,
    version: &str,
) -> bool {
    query_action_matches(service, action, Some(version))
}

fn query_action_matches(
    service: ServiceName,
    action: &str,
    version: Option<&str>,
) -> bool {
    if !query_service_matches_action(service, action) {
        return false;
    }

    match service {
        ServiceName::Iam => {
            version.is_none_or(|value| value == iam_query::IAM_QUERY_VERSION)
        }
        ServiceName::Sts => {
            version.is_none_or(|value| value == sts_query::STS_QUERY_VERSION)
        }
        ServiceName::Sns => {
            version.is_none_or(|value| value == sns::SNS_QUERY_VERSION)
        }
        ServiceName::Sqs => {
            version.is_none_or(|value| value == sqs::SQS_QUERY_VERSION)
        }
        ServiceName::CloudFormation => version.is_none_or(|value| {
            value == cloudformation::CLOUDFORMATION_QUERY_VERSION
        }),
        ServiceName::CloudWatch => version
            .is_none_or(|value| value == cloudwatch::CLOUDWATCH_QUERY_VERSION),
        ServiceName::Rds => {
            version.is_none_or(|value| value == rds::RDS_QUERY_VERSION)
        }
        ServiceName::ElastiCache => version.is_none_or(|value| {
            elasticache::action_matches_version(action, Some(value))
        }),

        _ => false,
    }
}

fn query_service_matches_action(service: ServiceName, action: &str) -> bool {
    match service {
        ServiceName::Iam => iam_query::is_iam_action(action),
        ServiceName::Sts => sts_query::is_sts_action(action),
        ServiceName::Sns => sns::is_sns_action(action),
        ServiceName::Sqs => sqs::is_sqs_action(action),
        ServiceName::CloudFormation => {
            cloudformation::is_cloudformation_action(action)
        }
        ServiceName::CloudWatch => cloudwatch::is_metrics_query_action(action),
        ServiceName::Rds => rds::is_rds_action(action),
        ServiceName::ElastiCache => elasticache::is_elasticache_action(action),
        _ => false,
    }
}

pub(crate) fn json_target(
    protocol: ProtocolFamily,
    target: &str,
) -> Option<(ServiceName, &str)> {
    JSON_TARGETS.iter().find_map(|dispatch| {
        (dispatch.protocol == protocol).then_some(dispatch).and_then(
            |dispatch| {
                dispatch.prefixes.iter().find_map(|prefix| {
                    target
                        .strip_prefix(prefix)
                        .map(|operation| (dispatch.service, operation))
                })
            },
        )
    })
}

pub(crate) fn service_from_smithy_id(service_id: &str) -> Option<ServiceName> {
    SMITHY_SERVICES
        .iter()
        .find(|service| service.ids.contains(&service_id))
        .map(|service| service.service)
}

pub(crate) fn rest_json_service(
    request: &HttpRequest<'_>,
) -> Option<ServiceName> {
    enabled_rest_json_service(request)
        .or_else(|| compiled_out_rest_json_fallback(request))
}

fn enabled_rest_json_service(
    request: &HttpRequest<'_>,
) -> Option<ServiceName> {
    if lambda::is_rest_json_request(request) {
        return Some(ServiceName::Lambda);
    }
    if apigateway::is_rest_json_request(request) {
        return Some(ServiceName::ApiGateway);
    }

    None
}

fn compiled_out_rest_json_fallback(
    request: &HttpRequest<'_>,
) -> Option<ServiceName> {
    let path = request.path_without_query();

    if is_lambda_rest_json_path(path) {
        return Some(ServiceName::Lambda);
    }
    if is_apigateway_rest_json_path(path) {
        return Some(ServiceName::ApiGateway);
    }

    None
}

fn is_lambda_rest_json_path(path: &str) -> bool {
    [
        "/2015-03-31/functions",
        "/2015-03-31/event-source-mappings",
        "/2019-09-25/functions",
        "/2021-10-31/functions",
    ]
    .into_iter()
    .any(|prefix| path.starts_with(prefix))
}

fn is_apigateway_rest_json_path(path: &str) -> bool {
    path == "/v2/apis"
        || path.starts_with("/v2/apis/")
        || path == "/restapis"
        || path.starts_with("/restapis/")
        || path == "/apikeys"
        || path.starts_with("/apikeys/")
        || path == "/usageplans"
        || path.starts_with("/usageplans/")
        || path == "/domainnames"
        || path.starts_with("/domainnames/")
        || path.starts_with("/tags/")
}

fn smithy_path(path: &str) -> Option<(&str, &str)> {
    let path = path.trim_matches('/');
    let mut segments = path.split('/');
    let service_segment = segments.next()?;
    let service_id = segments.next()?;
    let operation_segment = segments.next()?;
    let operation = segments.next()?;

    if service_segment != "service"
        || operation_segment != "operation"
        || segments.next().is_some()
    {
        return None;
    }

    Some((service_id, operation))
}

#[cfg(test)]
mod tests {
    use super::{
        ScopedQueryValidation, detect_generic_protocol, json_target,
        query_action_matches_version, resolve_unsigned_query_service,
        rest_json_service, service_from_smithy_id, supports_protocol,
        validate_query_for_service,
    };
    use crate::iam_query;
    use crate::request::HttpRequest;
    use aws::{AdvertisedEdge, ProtocolFamily, ServiceName};

    #[test]
    fn query_catalog_keeps_compiled_out_services_resolvable() {
        assert_eq!(
            resolve_unsigned_query_service("CreateQueue", Some("2012-11-05")),
            Some(ServiceName::Sqs)
        );
        assert_eq!(
            resolve_unsigned_query_service(
                "PutMetricData",
                Some("2010-08-01")
            ),
            Some(ServiceName::CloudWatch)
        );
        assert!(query_action_matches_version(
            ServiceName::Sqs,
            "CreateQueue",
            "2012-11-05"
        ));
        assert!(!query_action_matches_version(
            ServiceName::Sqs,
            "CreateQueue",
            "2011-06-15"
        ));
        assert_eq!(
            resolve_unsigned_query_service("CreateUser", Some("2015-02-02")),
            Some(ServiceName::ElastiCache)
        );
        assert_eq!(
            resolve_unsigned_query_service("DeleteUser", Some("2015-02-02")),
            Some(ServiceName::ElastiCache)
        );
        assert_eq!(
            resolve_unsigned_query_service(
                "CreateUser",
                Some(iam_query::IAM_QUERY_VERSION)
            ),
            Some(ServiceName::Iam)
        );
        assert_eq!(
            resolve_unsigned_query_service(
                "DeleteUser",
                Some(iam_query::IAM_QUERY_VERSION)
            ),
            Some(ServiceName::Iam)
        );
    }

    #[test]
    fn query_catalog_requires_matching_elasticache_version() {
        assert!(!query_action_matches_version(
            ServiceName::ElastiCache,
            "CreateUser",
            "2014-10-31"
        ));
    }

    #[test]
    fn scoped_query_validation_distinguishes_action_mismatch_from_version_mismatch()
     {
        assert_eq!(
            validate_query_for_service(
                ServiceName::Iam,
                "CreateUser",
                iam_query::IAM_QUERY_VERSION
            ),
            ScopedQueryValidation::Valid
        );
        assert_eq!(
            validate_query_for_service(
                ServiceName::Iam,
                "CreateUser",
                "2015-02-02"
            ),
            ScopedQueryValidation::InvalidVersion
        );
        assert_eq!(
            validate_query_for_service(
                ServiceName::Sts,
                "CreateUser",
                iam_query::IAM_QUERY_VERSION
            ),
            ScopedQueryValidation::ActionOutsideService
        );
    }

    #[test]
    fn json_catalog_keeps_known_targets_for_disabled_services() {
        assert_eq!(
            json_target(
                ProtocolFamily::AwsJson10,
                "DynamoDB_20120810.ListTables"
            ),
            Some((ServiceName::DynamoDb, "ListTables"))
        );
        assert_eq!(
            json_target(ProtocolFamily::AwsJson11, "AmazonSSM.GetParameter"),
            Some((ServiceName::Ssm, "GetParameter"))
        );
    }

    #[test]
    fn smithy_catalog_keeps_known_service_ids_for_disabled_services() {
        assert_eq!(
            service_from_smithy_id("monitoring"),
            Some(ServiceName::CloudWatch)
        );
        assert_eq!(
            service_from_smithy_id("CloudWatch"),
            Some(ServiceName::CloudWatch)
        );
    }

    #[test]
    fn rest_json_catalog_is_pure_and_detectable() {
        let lambda_request = HttpRequest::parse(
            b"GET /2015-03-31/functions/demo HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should parse");
        let apigateway_request = HttpRequest::parse(
            b"GET /restapis HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should parse");

        assert_eq!(
            rest_json_service(&lambda_request),
            Some(ServiceName::Lambda)
        );
        assert_eq!(
            rest_json_service(&apigateway_request),
            Some(ServiceName::ApiGateway)
        );
        assert_eq!(
            detect_generic_protocol(
                &lambda_request,
                &AdvertisedEdge::default()
            ),
            Some(ProtocolFamily::RestJson)
        );
    }

    #[test]
    fn protocol_detection_prefers_catalogued_identities() {
        let request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nX-Amz-Target: AmazonSSM.GetParameter\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");

        assert_eq!(
            detect_generic_protocol(&request, &AdvertisedEdge::default()),
            Some(ProtocolFamily::AwsJson11)
        );
        assert!(supports_protocol(
            ServiceName::CloudWatch,
            ProtocolFamily::Query
        ));
        assert!(supports_protocol(
            ServiceName::CloudWatch,
            ProtocolFamily::SmithyRpcV2Cbor
        ));
    }
}
