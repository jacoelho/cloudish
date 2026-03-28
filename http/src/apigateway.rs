pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use crate::runtime::EdgeResponse;
use aws::{AwsError, AwsErrorFamily, RequestContext};
use serde::Deserialize;
use serde::Serialize;
use services::{
    ApiGatewayError, ApiGatewayScope, CreateResourceInput, CreateRestApiInput,
    PatchOperation, TagResourceInput,
};

#[path = "apigateway_v2.rs"]
mod v2;

pub(crate) fn is_rest_json_request(request: &HttpRequest<'_>) -> bool {
    let path = request.path_without_query();
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

pub(crate) fn handle_rest_json(
    apigateway: &services::ApiGatewayService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<EdgeResponse, AwsError> {
    let scope = ApiGatewayScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );
    if request.path_without_query() == "/v2/apis"
        || request.path_without_query().starts_with("/v2/apis/")
    {
        return v2::handle_http_api(apigateway, request, &scope);
    }
    let query = QueryParameters::parse(request.query_string())
        .map_err(|error| error.to_aws_error())?;

    if let Some(resource_arn) = tag_resource_arn(request.path_without_query())
        .map_err(|error| error.to_aws_error())?
    {
        return handle_tag_routes(
            apigateway,
            request,
            &scope,
            &resource_arn,
            &query,
        );
    }

    let owned_segments = decode_segments(request.path_without_query())
        .map_err(|error| error.to_aws_error())?;
    let segments =
        owned_segments.iter().map(String::as_str).collect::<Vec<_>>();

    match (request.method(), segments.as_slice()) {
        ("POST", ["restapis"]) => {
            let input: CreateRestApiInput = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_rest_api(&scope, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["restapis"]) => json_response(
            200,
            &apigateway
                .get_rest_apis(
                    &scope,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["restapis", api_id]) => json_response(
            200,
            &apigateway
                .get_rest_api(&scope, api_id)
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["restapis", api_id]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_rest_api(&scope, api_id, &operations)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["restapis", api_id]) => {
            apigateway
                .delete_rest_api(&scope, api_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("GET", ["restapis", api_id, "resources"]) => json_response(
            200,
            &apigateway
                .get_resources(
                    &scope,
                    api_id,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("POST", ["restapis", api_id, "resources", parent_id]) => {
            let input: CreateResourceInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_resource(&scope, api_id, parent_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["restapis", api_id, "resources", resource_id]) => {
            json_response(
                200,
                &apigateway
                    .get_resource(&scope, api_id, resource_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("PATCH", ["restapis", api_id, "resources", resource_id]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_resource(&scope, api_id, resource_id, &operations)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["restapis", api_id, "resources", resource_id]) => {
            apigateway
                .delete_resource(&scope, api_id, resource_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        (
            "PUT",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
            ],
        ) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .put_method(
                        &scope,
                        api_id,
                        resource_id,
                        http_method,
                        input,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "GET",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
            ],
        ) => json_response(
            200,
            &apigateway
                .get_method(&scope, api_id, resource_id, http_method)
                .map_err(|error| error.to_aws_error())?,
        ),
        (
            "PATCH",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
            ],
        ) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_method(
                        &scope,
                        api_id,
                        resource_id,
                        http_method,
                        &operations,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "DELETE",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
            ],
        ) => {
            apigateway
                .delete_method(&scope, api_id, resource_id, http_method)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        (
            "PUT",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
                "responses",
                status_code,
            ],
        ) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .put_method_response(
                        &scope,
                        api_id,
                        resource_id,
                        http_method,
                        status_code,
                        input,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "GET",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
                "responses",
                status_code,
            ],
        ) => json_response(
            200,
            &apigateway
                .get_method_response(
                    &scope,
                    api_id,
                    resource_id,
                    http_method,
                    status_code,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        (
            "DELETE",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
                "responses",
                status_code,
            ],
        ) => {
            apigateway
                .delete_method_response(
                    &scope,
                    api_id,
                    resource_id,
                    http_method,
                    status_code,
                )
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        (
            "PUT",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
                "integration",
            ],
        ) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .put_integration(
                        &scope,
                        api_id,
                        resource_id,
                        http_method,
                        input,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "GET",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
                "integration",
            ],
        ) => json_response(
            200,
            &apigateway
                .get_integration(&scope, api_id, resource_id, http_method)
                .map_err(|error| error.to_aws_error())?,
        ),
        (
            "DELETE",
            [
                "restapis",
                api_id,
                "resources",
                resource_id,
                "methods",
                http_method,
                "integration",
            ],
        ) => {
            apigateway
                .delete_integration(&scope, api_id, resource_id, http_method)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["restapis", api_id, "deployments"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_deployment(&scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["restapis", api_id, "deployments"]) => json_response(
            200,
            &apigateway
                .get_deployments(
                    &scope,
                    api_id,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["restapis", api_id, "deployments", deployment_id]) => {
            json_response(
                200,
                &apigateway
                    .get_deployment(&scope, api_id, deployment_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["restapis", api_id, "deployments", deployment_id]) => {
            apigateway
                .delete_deployment(&scope, api_id, deployment_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["restapis", api_id, "stages"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_stage(&scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["restapis", api_id, "stages"]) => json_response(
            200,
            &apigateway
                .get_stages(
                    &scope,
                    api_id,
                    query.first("deploymentId"),
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["restapis", api_id, "stages", stage_name]) => json_response(
            200,
            &apigateway
                .get_stage(&scope, api_id, stage_name)
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["restapis", api_id, "stages", stage_name]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_stage(&scope, api_id, stage_name, &operations)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["restapis", api_id, "stages", stage_name]) => {
            apigateway
                .delete_stage(&scope, api_id, stage_name)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["restapis", api_id, "authorizers"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_authorizer(&scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["restapis", api_id, "authorizers"]) => json_response(
            200,
            &apigateway
                .get_authorizers(
                    &scope,
                    api_id,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["restapis", api_id, "authorizers", authorizer_id]) => {
            json_response(
                200,
                &apigateway
                    .get_authorizer(&scope, api_id, authorizer_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("PATCH", ["restapis", api_id, "authorizers", authorizer_id]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_authorizer(
                        &scope,
                        api_id,
                        authorizer_id,
                        &operations,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["restapis", api_id, "authorizers", authorizer_id]) => {
            apigateway
                .delete_authorizer(&scope, api_id, authorizer_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["restapis", api_id, "requestvalidators"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_request_validator(&scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["restapis", api_id, "requestvalidators"]) => json_response(
            200,
            &apigateway
                .get_request_validators(
                    &scope,
                    api_id,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        (
            "GET",
            ["restapis", api_id, "requestvalidators", request_validator_id],
        ) => json_response(
            200,
            &apigateway
                .get_request_validator(&scope, api_id, request_validator_id)
                .map_err(|error| error.to_aws_error())?,
        ),
        (
            "PATCH",
            ["restapis", api_id, "requestvalidators", request_validator_id],
        ) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_request_validator(
                        &scope,
                        api_id,
                        request_validator_id,
                        &operations,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "DELETE",
            ["restapis", api_id, "requestvalidators", request_validator_id],
        ) => {
            apigateway
                .delete_request_validator(&scope, api_id, request_validator_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["apikeys"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_api_key(&scope, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["apikeys"]) => json_response(
            200,
            &apigateway
                .get_api_keys(
                    &scope,
                    query.first_bool("includeValues")?.unwrap_or(false),
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["apikeys", api_key_id]) => json_response(
            200,
            &apigateway
                .get_api_key(
                    &scope,
                    api_key_id,
                    query.first_bool("includeValue")?.unwrap_or(false),
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["apikeys", api_key_id]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_api_key(&scope, api_key_id, &operations)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["apikeys", api_key_id]) => {
            apigateway
                .delete_api_key(&scope, api_key_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["usageplans"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_usage_plan(&scope, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["usageplans"]) => json_response(
            200,
            &apigateway
                .get_usage_plans(
                    &scope,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["usageplans", usage_plan_id]) => json_response(
            200,
            &apigateway
                .get_usage_plan(&scope, usage_plan_id)
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["usageplans", usage_plan_id]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_usage_plan(&scope, usage_plan_id, &operations)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["usageplans", usage_plan_id]) => {
            apigateway
                .delete_usage_plan(&scope, usage_plan_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["usageplans", usage_plan_id, "keys"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_usage_plan_key(&scope, usage_plan_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["usageplans", usage_plan_id, "keys"]) => json_response(
            200,
            &apigateway
                .get_usage_plan_keys(
                    &scope,
                    usage_plan_id,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["usageplans", usage_plan_id, "keys", key_id]) => {
            json_response(
                200,
                &apigateway
                    .get_usage_plan_key(&scope, usage_plan_id, key_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["usageplans", usage_plan_id, "keys", key_id]) => {
            apigateway
                .delete_usage_plan_key(&scope, usage_plan_id, key_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["domainnames"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_domain_name(&scope, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["domainnames"]) => json_response(
            200,
            &apigateway
                .get_domain_names(
                    &scope,
                    query.first("position"),
                    query.first_u32("limit")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["domainnames", domain_name]) => json_response(
            200,
            &apigateway
                .get_domain_name(&scope, domain_name)
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["domainnames", domain_name]) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_domain_name(&scope, domain_name, &operations)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["domainnames", domain_name]) => {
            apigateway
                .delete_domain_name(&scope, domain_name)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        ("POST", ["domainnames", domain_name, "basepathmappings"]) => {
            let input = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_base_path_mapping(&scope, domain_name, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["domainnames", domain_name, "basepathmappings"]) => {
            json_response(
                200,
                &apigateway
                    .get_base_path_mappings(
                        &scope,
                        domain_name,
                        query.first("position"),
                        query.first_u32("limit")?,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "GET",
            ["domainnames", domain_name, "basepathmappings", base_path],
        ) => json_response(
            200,
            &apigateway
                .get_base_path_mapping(&scope, domain_name, base_path)
                .map_err(|error| error.to_aws_error())?,
        ),
        (
            "PATCH",
            ["domainnames", domain_name, "basepathmappings", base_path],
        ) => {
            let operations = parse_patch_operations(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_base_path_mapping(
                        &scope,
                        domain_name,
                        base_path,
                        &operations,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        (
            "DELETE",
            ["domainnames", domain_name, "basepathmappings", base_path],
        ) => {
            apigateway
                .delete_base_path_mapping(&scope, domain_name, base_path)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(202))
        }
        _ => Err(not_found_error(request.path_without_query())),
    }
}

fn handle_tag_routes(
    apigateway: &services::ApiGatewayService,
    request: &HttpRequest<'_>,
    scope: &ApiGatewayScope,
    resource_arn: &str,
    query: &QueryParameters,
) -> Result<EdgeResponse, AwsError> {
    match request.method() {
        "GET" => json_response(
            200,
            &apigateway
                .get_tags(scope, resource_arn)
                .map_err(|error| error.to_aws_error())?,
        ),
        "PUT" => {
            let input: TagResourceInput = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            apigateway
                .tag_resource(scope, resource_arn, input.tags)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        "DELETE" => {
            apigateway
                .untag_resource(scope, resource_arn, &query.all("tagKeys"))
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        _ => Err(not_found_error(request.path_without_query())),
    }
}

fn decode_segments(path: &str) -> Result<Vec<String>, ApiGatewayError> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            urlencoding::decode(segment)
                .map(|decoded| decoded.into_owned())
                .map_err(|_| ApiGatewayError::Validation {
                    message: format!("invalid path segment {segment:?}"),
                })
        })
        .collect()
}

fn tag_resource_arn(path: &str) -> Result<Option<String>, ApiGatewayError> {
    let Some(raw_arn) = path.strip_prefix("/tags/") else {
        return Ok(None);
    };
    urlencoding::decode(raw_arn)
        .map(|decoded| Some(decoded.into_owned()))
        .map_err(|_| ApiGatewayError::Validation {
            message: format!("invalid tag resource ARN path {raw_arn:?}"),
        })
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, ApiGatewayError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(body).map_err(|error| ApiGatewayError::Validation {
        message: format!("invalid JSON request body: {error}"),
    })
}

fn parse_patch_operations(
    body: &[u8],
) -> Result<Vec<PatchOperation>, ApiGatewayError> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PatchOperationsBody {
        #[serde(default)]
        patch_operations: Vec<PatchOperation>,
    }

    let body: PatchOperationsBody = parse_json_body(body)?;
    Ok(body.patch_operations)
}

fn json_response<T: Serialize>(
    status_code: u16,
    value: &T,
) -> Result<EdgeResponse, AwsError> {
    let body = serde_json::to_vec(value).map_err(|error| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            error.to_string(),
            500,
            false,
        )
    })?;

    Ok(EdgeResponse::bytes(status_code, "application/json", body))
}

fn empty_response(status_code: u16) -> EdgeResponse {
    EdgeResponse::bytes(status_code, "application/json", Vec::new())
}

fn not_found_error(path: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::NotFound,
        "NotFoundException",
        format!("REST path {path} was not found."),
        404,
        true,
    )
}

struct QueryParameters {
    values: std::collections::BTreeMap<String, Vec<String>>,
}

impl QueryParameters {
    fn parse(query: Option<&str>) -> Result<Self, ApiGatewayError> {
        let mut values = std::collections::BTreeMap::new();
        for pair in query
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
        {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let name = urlencoding::decode(name)
                .map_err(|_| ApiGatewayError::Validation {
                    message: format!("invalid query parameter name {name:?}"),
                })?
                .into_owned();
            let value = urlencoding::decode(value)
                .map_err(|_| ApiGatewayError::Validation {
                    message: format!(
                        "invalid query parameter value {value:?}"
                    ),
                })?
                .into_owned();
            values.entry(name).or_insert_with(Vec::new).push(value);
        }

        Ok(Self { values })
    }

    fn first(&self, key: &str) -> Option<&str> {
        self.values
            .get(key)
            .and_then(|values| values.first())
            .map(String::as_str)
    }

    fn all(&self, key: &str) -> Vec<String> {
        self.values.get(key).cloned().unwrap_or_default()
    }

    fn first_bool(&self, key: &str) -> Result<Option<bool>, AwsError> {
        self.first(key)
            .map(|value| {
                value.parse::<bool>().map_err(|_| {
                    ApiGatewayError::Validation {
                        message: format!(
                            "query parameter {key:?} must be boolean"
                        ),
                    }
                    .to_aws_error()
                })
            })
            .transpose()
    }

    fn first_u32(&self, key: &str) -> Result<Option<u32>, AwsError> {
        self.first(key)
            .map(|value| {
                value.parse::<u32>().map_err(|_| {
                    ApiGatewayError::Validation {
                        message: format!(
                            "query parameter {key:?} must be an integer"
                        ),
                    }
                    .to_aws_error()
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::is_rest_json_request;
    use crate::request::HttpRequest;
    use crate::runtime::EdgeRouter;
    use crate::test_runtime;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn router() -> EdgeRouter {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        test_runtime::router(&format!("http-apigw-tests-{id}"))
    }

    fn split_response(response: &[u8]) -> (&str, Vec<(&str, &str)>, &str) {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should contain a header terminator");
        let headers = std::str::from_utf8(&response[..header_end])
            .expect("response headers should be valid UTF-8");
        let body = std::str::from_utf8(&response[header_end + 4..])
            .expect("response body should be valid UTF-8");
        let mut lines = headers.split("\r\n");
        let status =
            lines.next().expect("response should contain a status line");
        let mut parsed_headers = Vec::new();
        for line in lines {
            let (name, value) =
                line.split_once(':').expect("header should contain ':'");
            parsed_headers.push((name, value.trim()));
        }

        (status, parsed_headers, body)
    }

    fn header_value<'a>(
        headers: &'a [(&'a str, &'a str)],
        name: &str,
    ) -> Option<&'a str> {
        headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| *value)
    }

    fn json_request(method: &str, path: &str, body: &str) -> Vec<u8> {
        format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn apigw_v1_control_rest_api_routes_round_trip() {
        let router = router();
        let create_api = router.handle_bytes(&json_request(
            "POST",
            "/restapis",
            r#"{"name":"demo"}"#,
        ));
        let response = create_api.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let api: serde_json::Value =
            serde_json::from_str(body).expect("api response should parse");
        let api_id = api["id"].as_str().expect("api id should be present");
        let root_id = api["rootResourceId"]
            .as_str()
            .expect("root resource id should be present");

        let create_resource = router.handle_bytes(&json_request(
            "POST",
            &format!("/restapis/{api_id}/resources/{root_id}"),
            r#"{"pathPart":"pets"}"#,
        ));
        let response = create_resource.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let resource: serde_json::Value = serde_json::from_str(body)
            .expect("resource response should parse");
        let resource_id =
            resource["id"].as_str().expect("resource id should be present");

        let put_method = router.handle_bytes(&json_request(
            "PUT",
            &format!("/restapis/{api_id}/resources/{resource_id}/methods/GET"),
            r#"{"authorizationType":"NONE"}"#,
        ));
        let response = put_method.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");

        let put_integration = router.handle_bytes(&json_request(
            "PUT",
            &format!(
                "/restapis/{api_id}/resources/{resource_id}/methods/GET/integration"
            ),
            r#"{"type":"HTTP_PROXY","httpMethod":"GET","uri":"https://example.com/pets"}"#,
        ));
        let response = put_integration.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");

        let deployment = router.handle_bytes(&json_request(
            "POST",
            &format!("/restapis/{api_id}/deployments"),
            r#"{"description":"first"}"#,
        ));
        let response = deployment.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let deployment: serde_json::Value =
            serde_json::from_str(body).expect("deployment should parse");
        let deployment_id = deployment["id"]
            .as_str()
            .expect("deployment id should be present");

        let create_stage = router.handle_bytes(&json_request(
            "POST",
            &format!("/restapis/{api_id}/stages"),
            &format!(
                r#"{{"deploymentId":"{deployment_id}","stageName":"dev"}}"#
            ),
        ));
        let response = create_stage.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");

        let get_stage = router.handle_bytes(&json_request(
            "GET",
            &format!("/restapis/{api_id}/stages/dev"),
            "",
        ));
        let response = get_stage.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let stage: serde_json::Value =
            serde_json::from_str(body).expect("stage should parse");
        assert_eq!(stage["stageName"], "dev");

        let list_apis =
            router.handle_bytes(&json_request("GET", "/restapis", ""));
        let response = list_apis.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let listed: serde_json::Value =
            serde_json::from_str(body).expect("list response should parse");
        assert_eq!(listed["item"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn apigw_v1_control_invalid_parent_and_private_integration_surface_explicit_errors()
     {
        let router = router();
        let create_api = router.handle_bytes(&json_request(
            "POST",
            "/restapis",
            r#"{"name":"demo"}"#,
        ));
        let response = create_api.to_http_bytes();
        let (_, _, body) = split_response(&response);
        let api: serde_json::Value =
            serde_json::from_str(body).expect("api response should parse");
        let api_id = api["id"].as_str().expect("api id should exist");
        let root_id = api["rootResourceId"]
            .as_str()
            .expect("root resource id should exist");

        let invalid_parent = router.handle_bytes(&json_request(
            "POST",
            &format!("/restapis/{api_id}/resources/fake-parent"),
            r#"{"pathPart":"pets"}"#,
        ));
        let response = invalid_parent.to_http_bytes();
        let (status, headers, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 404 Not Found");
        assert_eq!(
            header_value(&headers, "x-amzn-errortype"),
            Some("NotFoundException")
        );
        assert!(body.contains("fake-parent"));

        let put_method = router.handle_bytes(&json_request(
            "PUT",
            &format!("/restapis/{api_id}/resources/{root_id}/methods/GET"),
            r#"{"authorizationType":"NONE"}"#,
        ));
        let response = put_method.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");

        let private_integration = router.handle_bytes(&json_request(
            "PUT",
            &format!(
                "/restapis/{api_id}/resources/{root_id}/methods/GET/integration"
            ),
            r#"{"type":"HTTP_PROXY","httpMethod":"GET","uri":"https://example.com","connectionType":"VPC_LINK","connectionId":"vpclink-123"}"#,
        ));
        let response = private_integration.to_http_bytes();
        let (status, headers, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "x-amzn-errortype"),
            Some("BadRequestException")
        );
        assert!(body.contains("private integrations"));

        let duplicate_method = router.handle_bytes(&json_request(
            "PUT",
            &format!("/restapis/{api_id}/resources/{root_id}/methods/GET"),
            r#"{"authorizationType":"NONE"}"#,
        ));
        let response = duplicate_method.to_http_bytes();
        let (status, headers, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 409 Conflict");
        assert_eq!(
            header_value(&headers, "x-amzn-errortype"),
            Some("ConflictException")
        );
        assert!(body.contains("Method GET for resource"));
    }

    #[test]
    fn apigw_v1_control_request_detection_matches_paths() {
        let request = HttpRequest::parse(
            b"POST /restapis HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");
        assert!(is_rest_json_request(&request));
    }

    fn apigw_v2_create_api(router: &EdgeRouter, name: &str) -> String {
        let create_api = router.handle_bytes(&json_request(
            "POST",
            "/v2/apis",
            &format!(r#"{{"name":"{name}","protocolType":"HTTP"}}"#),
        ));
        let response = create_api.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let api: serde_json::Value =
            serde_json::from_str(body).expect("api should parse");
        api["apiId"].as_str().expect("api id should be present").to_owned()
    }

    #[test]
    fn apigw_v2_control_http_api_round_trip() {
        let router = router();
        let list_apis =
            router.handle_bytes(&json_request("GET", "/v2/apis", ""));
        let response = list_apis.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let empty_apis: serde_json::Value =
            serde_json::from_str(body).expect("empty api list should parse");
        assert_eq!(empty_apis["items"].as_array().map(Vec::len), Some(0));

        let api_id = apigw_v2_create_api(&router, "demo");
        let get_api = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}"),
            "",
        ));
        let response = get_api.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let fetched_api: serde_json::Value =
            serde_json::from_str(body).expect("api should parse");
        assert_eq!(fetched_api["name"], "demo");

        let patch_api = router.handle_bytes(&json_request(
            "PATCH",
            &format!("/v2/apis/{api_id}"),
            r#"{"name":"demo-v2","routeSelectionExpression":"${request.method} ${request.path}"}"#,
        ));
        let response = patch_api.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let patched_api: serde_json::Value =
            serde_json::from_str(body).expect("patched api should parse");
        assert_eq!(patched_api["name"], "demo-v2");

        let delete_api = router.handle_bytes(&json_request(
            "DELETE",
            &format!("/v2/apis/{api_id}"),
            "",
        ));
        let response = delete_api.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 204 No Content");
    }

    #[test]
    fn apigw_v2_control_integration_and_authorizer_round_trip() {
        let router = router();
        let api_id = apigw_v2_create_api(&router, "demo");

        let create_integration = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/integrations"),
            r#"{"integrationType":"HTTP_PROXY","integrationUri":"http://127.0.0.1/backend"}"#,
        ));
        let response = create_integration.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let integration: serde_json::Value =
            serde_json::from_str(body).expect("integration should parse");
        let integration_id = integration["integrationId"]
            .as_str()
            .expect("integration id should be present")
            .to_owned();

        let get_integration = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/integrations/{integration_id}"),
            "",
        ));
        let response = get_integration.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let fetched_integration: serde_json::Value =
            serde_json::from_str(body).expect("integration should parse");
        assert_eq!(
            fetched_integration["integrationUri"],
            "http://127.0.0.1/backend"
        );

        let patch_integration = router.handle_bytes(&json_request(
            "PATCH",
            &format!("/v2/apis/{api_id}/integrations/{integration_id}"),
            r#"{"integrationType":"HTTP_PROXY","integrationUri":"http://127.0.0.1/backend-v2"}"#,
        ));
        let response = patch_integration.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let patched_integration: serde_json::Value =
            serde_json::from_str(body).expect("integration should parse");
        assert_eq!(
            patched_integration["integrationUri"],
            "http://127.0.0.1/backend-v2"
        );

        let list_integrations = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/integrations?maxResults=1"),
            "",
        ));
        let response = list_integrations.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let integrations: serde_json::Value =
            serde_json::from_str(body).expect("integrations should parse");
        assert_eq!(integrations["items"].as_array().map(Vec::len), Some(1));

        let create_authorizer = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/authorizers"),
            r#"{"name":"auth","authorizerType":"REQUEST","authorizerUri":"arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:auth/invocations","authorizerPayloadFormatVersion":"2.0","identitySource":["$request.header.Authorization"]}"#,
        ));
        let response = create_authorizer.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let authorizer: serde_json::Value =
            serde_json::from_str(body).expect("authorizer should parse");
        let authorizer_id = authorizer["authorizerId"]
            .as_str()
            .expect("authorizer id should be present")
            .to_owned();

        let get_authorizer = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/authorizers/{authorizer_id}"),
            "",
        ));
        let response = get_authorizer.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let fetched_authorizer: serde_json::Value =
            serde_json::from_str(body).expect("authorizer should parse");
        assert_eq!(fetched_authorizer["name"], "auth");

        let patch_authorizer = router.handle_bytes(&json_request(
            "PATCH",
            &format!("/v2/apis/{api_id}/authorizers/{authorizer_id}"),
            r#"{"name":"auth-v2","authorizerType":"REQUEST","authorizerUri":"arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:auth-v2/invocations","authorizerPayloadFormatVersion":"2.0","identitySource":["$request.header.X-Auth"]}"#,
        ));
        let response = patch_authorizer.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let patched_authorizer: serde_json::Value =
            serde_json::from_str(body).expect("authorizer should parse");
        assert_eq!(patched_authorizer["name"], "auth-v2");

        let list_authorizers = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/authorizers?maxResults=1"),
            "",
        ));
        let response = list_authorizers.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let authorizers: serde_json::Value =
            serde_json::from_str(body).expect("authorizers should parse");
        assert_eq!(authorizers["items"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn apigw_v2_control_route_deployment_and_stage_round_trip() {
        let router = router();
        let api_id = apigw_v2_create_api(&router, "demo");

        let create_integration = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/integrations"),
            r#"{"integrationType":"HTTP_PROXY","integrationUri":"http://127.0.0.1/backend"}"#,
        ));
        let response = create_integration.to_http_bytes();
        let (_, _, body) = split_response(&response);
        let integration: serde_json::Value =
            serde_json::from_str(body).expect("integration should parse");
        let integration_id = integration["integrationId"]
            .as_str()
            .expect("integration id should be present")
            .to_owned();

        let create_authorizer = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/authorizers"),
            r#"{"name":"auth","authorizerType":"REQUEST","authorizerUri":"arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:eu-west-2:000000000000:function:auth/invocations","authorizerPayloadFormatVersion":"2.0","identitySource":["$request.header.Authorization"]}"#,
        ));
        let response = create_authorizer.to_http_bytes();
        let (_, _, body) = split_response(&response);
        let authorizer: serde_json::Value =
            serde_json::from_str(body).expect("authorizer should parse");
        let authorizer_id = authorizer["authorizerId"]
            .as_str()
            .expect("authorizer id should be present")
            .to_owned();

        let create_route = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/routes"),
            &format!(
                r#"{{"routeKey":"GET /pets","target":"integrations/{integration_id}"}}"#
            ),
        ));
        let response = create_route.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let route: serde_json::Value =
            serde_json::from_str(body).expect("route should parse");
        let route_id = route["routeId"]
            .as_str()
            .expect("route id should be present")
            .to_owned();

        let patch_route = router.handle_bytes(&json_request(
            "PATCH",
            &format!("/v2/apis/{api_id}/routes/{route_id}"),
            &format!(
                r#"{{"authorizationType":"CUSTOM","authorizerId":"{authorizer_id}","routeKey":"ANY /pets"}}"#
            ),
        ));
        let response = patch_route.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let patched_route: serde_json::Value =
            serde_json::from_str(body).expect("patched route should parse");
        assert_eq!(patched_route["routeKey"], "ANY /pets");

        let create_deployment = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/deployments"),
            r#"{"description":"first"}"#,
        ));
        let response = create_deployment.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");
        let deployment: serde_json::Value =
            serde_json::from_str(body).expect("deployment should parse");
        let deployment_id = deployment["deploymentId"]
            .as_str()
            .expect("deployment id should be present")
            .to_owned();

        let create_stage = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/stages"),
            &format!(
                r#"{{"stageName":"dev","deploymentId":"{deployment_id}"}}"#
            ),
        ));
        let response = create_stage.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 201 Created");

        let get_stage = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/stages/dev"),
            "",
        ));
        let response = get_stage.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let fetched_stage: serde_json::Value =
            serde_json::from_str(body).expect("stage should parse");
        assert_eq!(fetched_stage["stageName"], "dev");

        let patch_stage = router.handle_bytes(&json_request(
            "PATCH",
            &format!("/v2/apis/{api_id}/stages/dev"),
            &format!(
                r#"{{"deploymentId":"{deployment_id}","description":"updated","autoDeploy":true}}"#
            ),
        ));
        let response = patch_stage.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let patched_stage: serde_json::Value =
            serde_json::from_str(body).expect("stage should parse");
        assert_eq!(patched_stage["autoDeploy"], true);

        let list_stages = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/stages"),
            "",
        ));
        let response = list_stages.to_http_bytes();
        let (status, _, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 200 OK");
        let stages: serde_json::Value =
            serde_json::from_str(body).expect("stages should parse");
        assert_eq!(stages["items"].as_array().map(Vec::len), Some(1));

        let delete_route = router.handle_bytes(&json_request(
            "DELETE",
            &format!("/v2/apis/{api_id}/routes/{route_id}"),
            "",
        ));
        let response = delete_route.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 204 No Content");

        let missing_route = router.handle_bytes(&json_request(
            "GET",
            &format!("/v2/apis/{api_id}/routes/{route_id}"),
            "",
        ));
        let response = missing_route.to_http_bytes();
        let (status, _, _) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 404 Not Found");
    }

    #[test]
    fn apigw_v2_control_invalid_route_key_and_stage_variable_integration_surface_errors()
     {
        let router = router();
        let create_api = router.handle_bytes(&json_request(
            "POST",
            "/v2/apis",
            r#"{"name":"demo","protocolType":"HTTP"}"#,
        ));
        let response = create_api.to_http_bytes();
        let (_, _, body) = split_response(&response);
        let api: serde_json::Value =
            serde_json::from_str(body).expect("api should parse");
        let api_id = api["apiId"].as_str().expect("api id should be present");

        let invalid_route = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/routes"),
            r#"{"routeKey":"GET pets"}"#,
        ));
        let response = invalid_route.to_http_bytes();
        let (status, headers, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "x-amzn-errortype"),
            Some("BadRequestException")
        );
        assert!(body.contains("invalid routeKey"));

        let stage_variable_integration = router.handle_bytes(&json_request(
            "POST",
            &format!("/v2/apis/{api_id}/integrations"),
            r#"{"integrationType":"HTTP_PROXY","integrationUri":"https://${stageVariables.backend}/orders"}"#,
        ));
        let response = stage_variable_integration.to_http_bytes();
        let (status, headers, body) = split_response(&response);
        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert_eq!(
            header_value(&headers, "x-amzn-errortype"),
            Some("BadRequestException")
        );
        assert!(body.contains("stage-variable"));
    }
}
