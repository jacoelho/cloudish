use super::{
    QueryParameters, decode_segments, empty_response, json_response,
    not_found_error, parse_json_body,
};
pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use crate::runtime::EdgeResponse;
use aws::AwsError;
use services::{
    ApiGatewayScope, CreateHttpApiAuthorizerInput,
    CreateHttpApiDeploymentInput, CreateHttpApiInput,
    CreateHttpApiIntegrationInput, CreateHttpApiRouteInput,
    CreateHttpApiStageInput, UpdateHttpApiAuthorizerInput, UpdateHttpApiInput,
    UpdateHttpApiIntegrationInput, UpdateHttpApiRouteInput,
    UpdateHttpApiStageInput,
};

pub(super) fn handle_http_api(
    apigateway: &services::ApiGatewayService,
    request: &HttpRequest<'_>,
    scope: &ApiGatewayScope,
) -> Result<EdgeResponse, AwsError> {
    let query = QueryParameters::parse(request.query_string())
        .map_err(|error| error.to_aws_error())?;
    let owned_segments = decode_segments(request.path_without_query())
        .map_err(|error| error.to_aws_error())?;
    let segments =
        owned_segments.iter().map(String::as_str).collect::<Vec<_>>();

    match (request.method(), segments.as_slice()) {
        ("POST", ["v2", "apis"]) => {
            let input: CreateHttpApiInput = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_http_api(scope, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["v2", "apis"]) => json_response(
            200,
            &apigateway
                .get_http_apis(
                    scope,
                    query.first("nextToken"),
                    query.first_u32("maxResults")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["v2", "apis", api_id]) => json_response(
            200,
            &apigateway
                .get_http_api(scope, api_id)
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["v2", "apis", api_id]) => {
            let input: UpdateHttpApiInput = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_http_api(scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["v2", "apis", api_id]) => {
            apigateway
                .delete_http_api(scope, api_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        ("POST", ["v2", "apis", api_id, "routes"]) => {
            let input: CreateHttpApiRouteInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_http_api_route(scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["v2", "apis", api_id, "routes"]) => json_response(
            200,
            &apigateway
                .get_http_api_routes(
                    scope,
                    api_id,
                    query.first("nextToken"),
                    query.first_u32("maxResults")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["v2", "apis", api_id, "routes", route_id]) => json_response(
            200,
            &apigateway
                .get_http_api_route(scope, api_id, route_id)
                .map_err(|error| error.to_aws_error())?,
        ),
        ("PATCH", ["v2", "apis", api_id, "routes", route_id]) => {
            let input: UpdateHttpApiRouteInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_http_api_route(scope, api_id, route_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["v2", "apis", api_id, "routes", route_id]) => {
            apigateway
                .delete_http_api_route(scope, api_id, route_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        ("POST", ["v2", "apis", api_id, "integrations"]) => {
            let input: CreateHttpApiIntegrationInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_http_api_integration(scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["v2", "apis", api_id, "integrations"]) => json_response(
            200,
            &apigateway
                .get_http_api_integrations(
                    scope,
                    api_id,
                    query.first("nextToken"),
                    query.first_u32("maxResults")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["v2", "apis", api_id, "integrations", integration_id]) => {
            json_response(
                200,
                &apigateway
                    .get_http_api_integration(scope, api_id, integration_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("PATCH", ["v2", "apis", api_id, "integrations", integration_id]) => {
            let input: UpdateHttpApiIntegrationInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_http_api_integration(
                        scope,
                        api_id,
                        integration_id,
                        input,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["v2", "apis", api_id, "integrations", integration_id]) => {
            apigateway
                .delete_http_api_integration(scope, api_id, integration_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        ("POST", ["v2", "apis", api_id, "authorizers"]) => {
            let input: CreateHttpApiAuthorizerInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_http_api_authorizer(scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["v2", "apis", api_id, "authorizers"]) => json_response(
            200,
            &apigateway
                .get_http_api_authorizers(
                    scope,
                    api_id,
                    query.first("nextToken"),
                    query.first_u32("maxResults")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["v2", "apis", api_id, "authorizers", authorizer_id]) => {
            json_response(
                200,
                &apigateway
                    .get_http_api_authorizer(scope, api_id, authorizer_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("PATCH", ["v2", "apis", api_id, "authorizers", authorizer_id]) => {
            let input: UpdateHttpApiAuthorizerInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_http_api_authorizer(
                        scope,
                        api_id,
                        authorizer_id,
                        input,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["v2", "apis", api_id, "authorizers", authorizer_id]) => {
            apigateway
                .delete_http_api_authorizer(scope, api_id, authorizer_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        ("POST", ["v2", "apis", api_id, "stages"]) => {
            let input: CreateHttpApiStageInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_http_api_stage(scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["v2", "apis", api_id, "stages"]) => json_response(
            200,
            &apigateway
                .get_http_api_stages(
                    scope,
                    api_id,
                    query.first("nextToken"),
                    query.first_u32("maxResults")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["v2", "apis", api_id, "stages", stage_name]) => {
            json_response(
                200,
                &apigateway
                    .get_http_api_stage(scope, api_id, stage_name)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("PATCH", ["v2", "apis", api_id, "stages", stage_name]) => {
            let input: UpdateHttpApiStageInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                200,
                &apigateway
                    .update_http_api_stage(scope, api_id, stage_name, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["v2", "apis", api_id, "stages", stage_name]) => {
            apigateway
                .delete_http_api_stage(scope, api_id, stage_name)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        ("POST", ["v2", "apis", api_id, "deployments"]) => {
            let input: CreateHttpApiDeploymentInput =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            json_response(
                201,
                &apigateway
                    .create_http_api_deployment(scope, api_id, input)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("GET", ["v2", "apis", api_id, "deployments"]) => json_response(
            200,
            &apigateway
                .get_http_api_deployments(
                    scope,
                    api_id,
                    query.first("nextToken"),
                    query.first_u32("maxResults")?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        ("GET", ["v2", "apis", api_id, "deployments", deployment_id]) => {
            json_response(
                200,
                &apigateway
                    .get_http_api_deployment(scope, api_id, deployment_id)
                    .map_err(|error| error.to_aws_error())?,
            )
        }
        ("DELETE", ["v2", "apis", api_id, "deployments", deployment_id]) => {
            apigateway
                .delete_http_api_deployment(scope, api_id, deployment_id)
                .map_err(|error| error.to_aws_error())?;
            Ok(empty_response(204))
        }
        _ => Err(not_found_error(request.path_without_query())),
    }
}
