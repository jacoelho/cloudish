pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use crate::runtime::EdgeResponse;
use aws::{AwsError, RequestContext};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;
use serde::Serialize;
use services::{
    AddLambdaPermissionInput, CreateEventSourceMappingInput,
    CreateFunctionInput, CreateFunctionUrlConfigInput, CreateLambdaAliasInput,
    LambdaCodeInput, LambdaDestinationConfigInput,
    LambdaDestinationTargetInput, LambdaError, LambdaFunctionUrlAuthType,
    LambdaFunctionUrlConfig, LambdaFunctionUrlInvokeMode,
    LambdaInvocationType, LambdaPackageType, LambdaScope, LambdaService,
    PublishVersionInput, PutFunctionEventInvokeConfigInput, UpdateAliasInput,
    UpdateEventSourceMappingInput, UpdateFunctionCodeInput,
    UpdateFunctionEventInvokeConfigInput, UpdateFunctionUrlConfigInput,
};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) fn handle_rest_json(
    lambda: &LambdaService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<EdgeResponse, AwsError> {
    let route = parse_route(request).map_err(|error| error.to_aws_error())?;
    let scope = LambdaScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match route {
        LambdaRoute::CreateFunction => {
            let body: CreateFunctionRequest = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            let package_type =
                parse_package_type(body.package_type.as_deref())
                    .map_err(|error| error.to_aws_error())?;
            let code = parse_code_input(body.code, package_type)
                .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .create_function(
                    &scope,
                    CreateFunctionInput {
                        code,
                        dead_letter_target_arn: body
                            .dead_letter_config
                            .and_then(|config| config.target_arn),
                        description: body.description,
                        environment: body
                            .environment
                            .unwrap_or_default()
                            .variables,
                        function_name: body.function_name.unwrap_or_default(),
                        handler: body.handler,
                        memory_size: body.memory_size,
                        package_type,
                        publish: body.publish.unwrap_or(false),
                        role: body.role.unwrap_or_default(),
                        runtime: body.runtime,
                        timeout: body.timeout,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(201, &output)
        }
        LambdaRoute::CreateFunctionUrlConfig { function_name, qualifier } => {
            let body: CreateFunctionUrlConfigRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .create_function_url_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                    CreateFunctionUrlConfigInput {
                        auth_type: parse_function_url_auth_type(
                            body.auth_type.as_deref(),
                        )
                        .map_err(|error| error.to_aws_error())?,
                        invoke_mode: parse_function_url_invoke_mode(
                            body.invoke_mode.as_deref(),
                        )
                        .map_err(|error| error.to_aws_error())?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            let body = function_url_config_response(request, &output)
                .map_err(|error| error.to_aws_error())?;

            json_response(201, &body)
        }
        LambdaRoute::GetFunctionUrlConfig { function_name, qualifier } => {
            let output = lambda
                .get_function_url_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;
            let body = function_url_config_response(request, &output)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &body)
        }
        LambdaRoute::ListFunctionUrlConfigs {
            function_name,
            marker,
            max_items,
        } => {
            let output = lambda
                .list_function_url_configs(
                    &scope,
                    &function_name,
                    marker.as_deref(),
                    max_items,
                )
                .map_err(|error| error.to_aws_error())?;
            let body = function_url_config_list_response(request, &output)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &body)
        }
        LambdaRoute::UpdateFunctionUrlConfig { function_name, qualifier } => {
            let body: UpdateFunctionUrlConfigRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .update_function_url_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                    UpdateFunctionUrlConfigInput {
                        auth_type: parse_optional_function_url_auth_type(
                            body.auth_type.as_deref(),
                        )
                        .map_err(|error| error.to_aws_error())?,
                        invoke_mode: parse_optional_function_url_invoke_mode(
                            body.invoke_mode.as_deref(),
                        )
                        .map_err(|error| error.to_aws_error())?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            let body = function_url_config_response(request, &output)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &body)
        }
        LambdaRoute::DeleteFunctionUrlConfig { function_name, qualifier } => {
            lambda
                .delete_function_url_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(EdgeResponse::bytes(204, "application/json", Vec::new()))
        }
        LambdaRoute::ListFunctions => {
            json_response(200, &lambda.list_functions(&scope))
        }
        LambdaRoute::GetFunction { function_name, qualifier } => {
            let output = lambda
                .get_function(&scope, &function_name, qualifier.as_deref())
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::UpdateFunctionCode { function_name } => {
            let body: UpdateFunctionCodeRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let code = parse_update_code_input(body)
                .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .update_function_code(
                    &scope,
                    &function_name,
                    UpdateFunctionCodeInput {
                        code: code.0,
                        publish: code.1,
                        revision_id: code.2,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::DeleteFunction { function_name } => {
            lambda
                .delete_function(&scope, &function_name)
                .map_err(|error| error.to_aws_error())?;

            Ok(EdgeResponse::bytes(204, "application/json", Vec::new()))
        }
        LambdaRoute::PublishVersion { function_name } => {
            let body: PublishVersionRequest = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .publish_version(
                    &scope,
                    &function_name,
                    PublishVersionInput {
                        description: body.description,
                        revision_id: body.revision_id,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(201, &output)
        }
        LambdaRoute::ListVersionsByFunction { function_name } => {
            let output = lambda
                .list_versions_by_function(&scope, &function_name)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::CreateAlias { function_name } => {
            let body: CreateAliasRequest = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .create_alias(
                    &scope,
                    &function_name,
                    CreateLambdaAliasInput {
                        description: body.description,
                        function_version: body
                            .function_version
                            .unwrap_or_default(),
                        name: body.name.unwrap_or_default(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(201, &output)
        }
        LambdaRoute::GetAlias { function_name, alias_name } => {
            let output = lambda
                .get_alias(&scope, &function_name, &alias_name)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::ListAliases { function_name } => {
            let output = lambda
                .list_aliases(&scope, &function_name)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::UpdateAlias { function_name, alias_name } => {
            let body: UpdateAliasRequest = parse_json_body(request.body())
                .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .update_alias(
                    &scope,
                    &function_name,
                    &alias_name,
                    UpdateAliasInput {
                        description: body.description,
                        function_version: body.function_version,
                        revision_id: body.revision_id,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::DeleteAlias { function_name, alias_name } => {
            lambda
                .delete_alias(&scope, &function_name, &alias_name)
                .map_err(|error| error.to_aws_error())?;

            Ok(EdgeResponse::bytes(204, "application/json", Vec::new()))
        }
        LambdaRoute::AddPermission { function_name, qualifier } => {
            let body: AddPermissionRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .add_permission(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                    AddLambdaPermissionInput {
                        action: body.action.unwrap_or_default(),
                        function_url_auth_type:
                            parse_optional_function_url_auth_type(
                                body.function_url_auth_type.as_deref(),
                            )
                            .map_err(|error| error.to_aws_error())?,
                        principal: body.principal.unwrap_or_default(),
                        revision_id: body.revision_id,
                        source_arn: body.source_arn,
                        statement_id: body.statement_id.unwrap_or_default(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(
                201,
                &AddPermissionResponse {
                    statement: output.statement().to_owned(),
                },
            )
        }
        LambdaRoute::PutFunctionEventInvokeConfig {
            function_name,
            qualifier,
        } => {
            let body: FunctionEventInvokeConfigRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .put_function_event_invoke_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                    PutFunctionEventInvokeConfigInput {
                        destination_config: parse_destination_config(
                            body.destination_config,
                        ),
                        maximum_event_age_in_seconds: body
                            .maximum_event_age_in_seconds,
                        maximum_retry_attempts: body.maximum_retry_attempts,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::UpdateFunctionEventInvokeConfig {
            function_name,
            qualifier,
        } => {
            let body: FunctionEventInvokeConfigRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .update_function_event_invoke_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                    UpdateFunctionEventInvokeConfigInput {
                        destination_config: body
                            .destination_config
                            .map(parse_destination_config_required),
                        maximum_event_age_in_seconds: body
                            .maximum_event_age_in_seconds,
                        maximum_retry_attempts: body.maximum_retry_attempts,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::GetFunctionEventInvokeConfig {
            function_name,
            qualifier,
        } => {
            let output = lambda
                .get_function_event_invoke_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::ListFunctionEventInvokeConfigs {
            function_name,
            marker,
            max_items,
        } => {
            let output = lambda
                .list_function_event_invoke_configs(
                    &scope,
                    &function_name,
                    marker.as_deref(),
                    max_items,
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::DeleteFunctionEventInvokeConfig {
            function_name,
            qualifier,
        } => {
            lambda
                .delete_function_event_invoke_config(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(EdgeResponse::bytes(204, "application/json", Vec::new()))
        }
        LambdaRoute::CreateEventSourceMapping => {
            let body: CreateEventSourceMappingRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .create_event_source_mapping(
                    &scope,
                    CreateEventSourceMappingInput {
                        batch_size: body.batch_size,
                        enabled: body.enabled,
                        event_source_arn: body
                            .event_source_arn
                            .unwrap_or_default(),
                        function_name: body.function_name.unwrap_or_default(),
                        maximum_batching_window_in_seconds: body
                            .maximum_batching_window_in_seconds,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(201, &output)
        }
        LambdaRoute::GetEventSourceMapping { uuid } => {
            let output = lambda
                .get_event_source_mapping(&scope, &uuid)
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::ListEventSourceMappings {
            event_source_arn,
            function_name,
            marker,
            max_items,
        } => {
            let output = lambda
                .list_event_source_mappings(
                    &scope,
                    function_name.as_deref(),
                    event_source_arn.as_deref(),
                    marker.as_deref(),
                    max_items,
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::UpdateEventSourceMapping { uuid } => {
            let body: UpdateEventSourceMappingRequest =
                parse_json_body(request.body())
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .update_event_source_mapping(
                    &scope,
                    &uuid,
                    UpdateEventSourceMappingInput {
                        batch_size: body.batch_size,
                        enabled: body.enabled,
                        maximum_batching_window_in_seconds: body
                            .maximum_batching_window_in_seconds,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json_response(200, &output)
        }
        LambdaRoute::DeleteEventSourceMapping { uuid } => {
            let output = lambda
                .delete_event_source_mapping(&scope, &uuid)
                .map_err(|error| error.to_aws_error())?;

            json_response(202, &output)
        }
        LambdaRoute::Invoke { function_name, qualifier } => {
            let invocation_type =
                parse_invocation_type(request.header("x-amz-invocation-type"))
                    .map_err(|error| error.to_aws_error())?;
            let output = lambda
                .invoke(
                    &scope,
                    &function_name,
                    qualifier.as_deref(),
                    services::LambdaInvokeInput {
                        invocation_type,
                        payload: request.body().to_vec(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            let mut response = EdgeResponse::bytes(
                output.status_code(),
                "application/json",
                output.payload().to_vec(),
            )
            .set_header("X-Amz-Executed-Version", output.executed_version());
            if let Some(function_error) = output.function_error() {
                response = response
                    .set_header("X-Amz-Function-Error", function_error);
            }

            Ok(response)
        }
    }
}

pub(crate) fn is_rest_json_request(request: &HttpRequest<'_>) -> bool {
    [
        "/2015-03-31/functions",
        "/2015-03-31/event-source-mappings",
        "/2019-09-25/functions",
        "/2021-10-31/functions",
    ]
    .into_iter()
    .any(|prefix| request.path_without_query().starts_with(prefix))
}

fn json_response<T: Serialize>(
    status_code: u16,
    value: &T,
) -> Result<EdgeResponse, AwsError> {
    let body = serde_json::to_vec(value).map_err(|error| {
        AwsError::trusted_custom(
            aws::AwsErrorFamily::Internal,
            "ServiceException",
            error.to_string(),
            500,
            false,
        )
    })?;

    Ok(EdgeResponse::bytes(status_code, "application/json", body))
}

fn function_url_config_response(
    request: &HttpRequest<'_>,
    config: &LambdaFunctionUrlConfig,
) -> Result<FunctionUrlConfigResponse, LambdaError> {
    let last_modified_time =
        format_function_url_time(config.last_modified_epoch_seconds())?;

    Ok(FunctionUrlConfigResponse {
        auth_type: function_url_auth_type_name(config.auth_type()).to_owned(),
        creation_time: last_modified_time.clone(),
        function_arn: config.function_arn().to_owned(),
        function_url: function_url_endpoint(request, config),
        invoke_mode: function_url_invoke_mode_name(config.invoke_mode())
            .to_owned(),
        last_modified_time,
    })
}

fn function_url_config_list_response(
    request: &HttpRequest<'_>,
    output: &services::ListFunctionUrlConfigsOutput,
) -> Result<ListFunctionUrlConfigsResponse, LambdaError> {
    Ok(ListFunctionUrlConfigsResponse {
        function_url_configs: output
            .function_url_configs()
            .iter()
            .map(|config| function_url_config_response(request, config))
            .collect::<Result<Vec<_>, _>>()?,
        next_marker: output.next_marker().map(str::to_owned),
    })
}

fn function_url_endpoint(
    request: &HttpRequest<'_>,
    config: &LambdaFunctionUrlConfig,
) -> String {
    let suffix =
        request.header("host").and_then(host_port_suffix).unwrap_or_default();
    let region =
        config.function_arn().split(':').nth(3).unwrap_or("us-east-1");

    format!(
        "http://{}.lambda-url.{region}.localhost{suffix}/",
        config.url_id()
    )
}

fn host_port_suffix(host: &str) -> Option<String> {
    let host = host.trim();
    let (_, port) = host.rsplit_once(':')?;
    port.parse::<u16>().ok()?;

    Some(format!(":{port}"))
}

fn format_function_url_time(
    epoch_seconds: i64,
) -> Result<String, LambdaError> {
    OffsetDateTime::from_unix_timestamp(epoch_seconds)
        .map_err(|error| LambdaError::Internal { message: error.to_string() })?
        .format(&Rfc3339)
        .map_err(|error| LambdaError::Internal { message: error.to_string() })
}

fn function_url_auth_type_name(
    auth_type: LambdaFunctionUrlAuthType,
) -> &'static str {
    match auth_type {
        LambdaFunctionUrlAuthType::AwsIam => "AWS_IAM",
        LambdaFunctionUrlAuthType::None => "NONE",
    }
}

fn function_url_invoke_mode_name(
    invoke_mode: LambdaFunctionUrlInvokeMode,
) -> &'static str {
    match invoke_mode {
        LambdaFunctionUrlInvokeMode::Buffered => "BUFFERED",
        LambdaFunctionUrlInvokeMode::ResponseStream => "RESPONSE_STREAM",
    }
}

fn parse_code_input(
    code: FunctionCodeRequest,
    package_type: LambdaPackageType,
) -> Result<LambdaCodeInput, LambdaError> {
    match package_type {
        LambdaPackageType::Image => code
            .image_uri
            .map(|image_uri| LambdaCodeInput::ImageUri { image_uri })
            .ok_or_else(|| LambdaError::InvalidParameterValue {
                message: "ImageUri is required for Image package type."
                    .to_owned(),
            }),
        LambdaPackageType::Zip => {
            if let Some(zip_file) = code.zip_file {
                return Ok(LambdaCodeInput::InlineZip {
                    archive: decode_zip_blob(&zip_file)?,
                });
            }
            match (code.s3_bucket, code.s3_key) {
                (Some(bucket), Some(key)) => Ok(LambdaCodeInput::S3Object {
                    bucket,
                    key,
                    object_version: code.s3_object_version,
                }),
                _ => Err(LambdaError::InvalidParameterValue {
                    message: "Zip package type requires ZipFile or S3 source."
                        .to_owned(),
                }),
            }
        }
    }
}

fn parse_invocation_type(
    header: Option<&str>,
) -> Result<LambdaInvocationType, LambdaError> {
    match header.unwrap_or("RequestResponse") {
        "RequestResponse" => Ok(LambdaInvocationType::RequestResponse),
        "Event" => Ok(LambdaInvocationType::Event),
        "DryRun" => Ok(LambdaInvocationType::DryRun),
        value => Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {value} at 'invocationType' failed to satisfy constraint: Member must satisfy enum value set: [Event, RequestResponse, DryRun]"
            ),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, LambdaError>
where
    T: for<'de> Deserialize<'de> + Default,
{
    if body.is_empty() {
        return Ok(T::default());
    }

    serde_json::from_slice(body).map_err(|error| {
        LambdaError::InvalidRequestContent { message: error.to_string() }
    })
}

fn parse_package_type(
    package_type: Option<&str>,
) -> Result<LambdaPackageType, LambdaError> {
    match package_type.unwrap_or("Zip") {
        "Zip" => Ok(LambdaPackageType::Zip),
        "Image" => Ok(LambdaPackageType::Image),
        value => Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {value} at 'packageType' failed to satisfy constraint: Member must satisfy enum value set: [Zip, Image]"
            ),
        }),
    }
}

fn parse_function_url_auth_type(
    auth_type: Option<&str>,
) -> Result<LambdaFunctionUrlAuthType, LambdaError> {
    match auth_type.unwrap_or_default() {
        "AWS_IAM" => Ok(LambdaFunctionUrlAuthType::AwsIam),
        "NONE" => Ok(LambdaFunctionUrlAuthType::None),
        "" => Err(LambdaError::InvalidParameterValue {
            message: "AuthType is required.".to_owned(),
        }),
        value => Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {value} at 'authType' failed to satisfy constraint: Member must satisfy enum value set: [NONE, AWS_IAM]"
            ),
        }),
    }
}

fn parse_optional_function_url_auth_type(
    auth_type: Option<&str>,
) -> Result<Option<LambdaFunctionUrlAuthType>, LambdaError> {
    auth_type
        .map(|auth_type| parse_function_url_auth_type(Some(auth_type)))
        .transpose()
}

fn parse_function_url_invoke_mode(
    invoke_mode: Option<&str>,
) -> Result<LambdaFunctionUrlInvokeMode, LambdaError> {
    match invoke_mode.unwrap_or("BUFFERED") {
        "BUFFERED" => Ok(LambdaFunctionUrlInvokeMode::Buffered),
        "RESPONSE_STREAM" => Ok(LambdaFunctionUrlInvokeMode::ResponseStream),
        value => Err(LambdaError::InvalidParameterValue {
            message: format!(
                "Value {value} at 'invokeMode' failed to satisfy constraint: Member must satisfy enum value set: [BUFFERED, RESPONSE_STREAM]"
            ),
        }),
    }
}

fn parse_optional_function_url_invoke_mode(
    invoke_mode: Option<&str>,
) -> Result<Option<LambdaFunctionUrlInvokeMode>, LambdaError> {
    invoke_mode
        .map(|invoke_mode| parse_function_url_invoke_mode(Some(invoke_mode)))
        .transpose()
}

fn parse_destination_config(
    input: Option<DestinationConfigRequest>,
) -> Option<LambdaDestinationConfigInput> {
    input.map(parse_destination_config_required)
}

fn parse_destination_config_required(
    input: DestinationConfigRequest,
) -> LambdaDestinationConfigInput {
    LambdaDestinationConfigInput {
        on_failure: input.on_failure.map(|target| {
            LambdaDestinationTargetInput {
                destination: target.destination.unwrap_or_default(),
            }
        }),
        on_success: input.on_success.map(|target| {
            LambdaDestinationTargetInput {
                destination: target.destination.unwrap_or_default(),
            }
        }),
    }
}

fn parse_query_string(request: &HttpRequest<'_>) -> BTreeMap<String, String> {
    request
        .query_string()
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            Some((percent_decode(name).ok()?, percent_decode(value).ok()?))
        })
        .collect()
}

fn parse_max_items(
    value: Option<&String>,
) -> Result<Option<usize>, LambdaError> {
    value
        .map(|value| {
            value.parse::<usize>().map_err(|_| LambdaError::Validation {
                message: "MaxItems must be a positive integer.".to_owned(),
            })
        })
        .transpose()
}

fn parse_route(request: &HttpRequest<'_>) -> Result<LambdaRoute, LambdaError> {
    let path = request
        .path_without_query()
        .trim_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let query = parse_query_string(request);

    match (request.method(), path.as_slice()) {
        ("POST", ["2015-03-31", "functions"]) => {
            Ok(LambdaRoute::CreateFunction)
        }
        ("GET", ["2015-03-31", "functions"]) => Ok(LambdaRoute::ListFunctions),
        ("GET", ["2015-03-31", "functions", function_name]) => {
            Ok(LambdaRoute::GetFunction {
                function_name: percent_decode(function_name)?,
                qualifier: query.get("Qualifier").cloned(),
            })
        }
        ("DELETE", ["2015-03-31", "functions", function_name]) => {
            Ok(LambdaRoute::DeleteFunction {
                function_name: percent_decode(function_name)?,
            })
        }
        ("PUT", ["2015-03-31", "functions", function_name, "code"]) => {
            Ok(LambdaRoute::UpdateFunctionCode {
                function_name: percent_decode(function_name)?,
            })
        }
        ("POST", ["2015-03-31", "functions", function_name, "versions"]) => {
            Ok(LambdaRoute::PublishVersion {
                function_name: percent_decode(function_name)?,
            })
        }
        ("GET", ["2015-03-31", "functions", function_name, "versions"]) => {
            Ok(LambdaRoute::ListVersionsByFunction {
                function_name: percent_decode(function_name)?,
            })
        }
        ("POST", ["2015-03-31", "functions", function_name, "aliases"]) => {
            Ok(LambdaRoute::CreateAlias {
                function_name: percent_decode(function_name)?,
            })
        }
        ("GET", ["2015-03-31", "functions", function_name, "aliases"]) => {
            Ok(LambdaRoute::ListAliases {
                function_name: percent_decode(function_name)?,
            })
        }
        (
            "GET",
            ["2015-03-31", "functions", function_name, "aliases", alias_name],
        ) => Ok(LambdaRoute::GetAlias {
            alias_name: percent_decode(alias_name)?,
            function_name: percent_decode(function_name)?,
        }),
        (
            "PUT",
            ["2015-03-31", "functions", function_name, "aliases", alias_name],
        ) => Ok(LambdaRoute::UpdateAlias {
            alias_name: percent_decode(alias_name)?,
            function_name: percent_decode(function_name)?,
        }),
        (
            "DELETE",
            ["2015-03-31", "functions", function_name, "aliases", alias_name],
        ) => Ok(LambdaRoute::DeleteAlias {
            alias_name: percent_decode(alias_name)?,
            function_name: percent_decode(function_name)?,
        }),
        (
            "POST",
            ["2015-03-31", "functions", function_name, "invocations"],
        ) => Ok(LambdaRoute::Invoke {
            function_name: percent_decode(function_name)?,
            qualifier: query.get("Qualifier").cloned(),
        }),
        ("POST", ["2015-03-31", "functions", function_name, "policy"]) => {
            Ok(LambdaRoute::AddPermission {
                function_name: percent_decode(function_name)?,
                qualifier: query.get("Qualifier").cloned(),
            })
        }
        ("POST", ["2015-03-31", "event-source-mappings"]) => {
            Ok(LambdaRoute::CreateEventSourceMapping)
        }
        ("GET", ["2015-03-31", "event-source-mappings"]) => {
            Ok(LambdaRoute::ListEventSourceMappings {
                event_source_arn: query.get("EventSourceArn").cloned(),
                function_name: query.get("FunctionName").cloned(),
                marker: query.get("Marker").cloned(),
                max_items: parse_max_items(query.get("MaxItems"))?,
            })
        }
        ("GET", ["2015-03-31", "event-source-mappings", uuid]) => {
            Ok(LambdaRoute::GetEventSourceMapping {
                uuid: percent_decode(uuid)?,
            })
        }
        ("PUT", ["2015-03-31", "event-source-mappings", uuid]) => {
            Ok(LambdaRoute::UpdateEventSourceMapping {
                uuid: percent_decode(uuid)?,
            })
        }
        ("DELETE", ["2015-03-31", "event-source-mappings", uuid]) => {
            Ok(LambdaRoute::DeleteEventSourceMapping {
                uuid: percent_decode(uuid)?,
            })
        }
        (
            "PUT",
            ["2019-09-25", "functions", function_name, "event-invoke-config"],
        ) => Ok(LambdaRoute::PutFunctionEventInvokeConfig {
            function_name: percent_decode(function_name)?,
            qualifier: query.get("Qualifier").cloned(),
        }),
        (
            "POST",
            ["2019-09-25", "functions", function_name, "event-invoke-config"],
        ) => Ok(LambdaRoute::UpdateFunctionEventInvokeConfig {
            function_name: percent_decode(function_name)?,
            qualifier: query.get("Qualifier").cloned(),
        }),
        (
            "GET",
            ["2019-09-25", "functions", function_name, "event-invoke-config"],
        ) => Ok(LambdaRoute::GetFunctionEventInvokeConfig {
            function_name: percent_decode(function_name)?,
            qualifier: query.get("Qualifier").cloned(),
        }),
        (
            "GET",
            [
                "2019-09-25",
                "functions",
                function_name,
                "event-invoke-config",
                "list",
            ],
        ) => Ok(LambdaRoute::ListFunctionEventInvokeConfigs {
            function_name: percent_decode(function_name)?,
            marker: query.get("Marker").cloned(),
            max_items: parse_max_items(query.get("MaxItems"))?,
        }),
        (
            "DELETE",
            ["2019-09-25", "functions", function_name, "event-invoke-config"],
        ) => Ok(LambdaRoute::DeleteFunctionEventInvokeConfig {
            function_name: percent_decode(function_name)?,
            qualifier: query.get("Qualifier").cloned(),
        }),
        ("POST", ["2021-10-31", "functions", function_name, "url"]) => {
            Ok(LambdaRoute::CreateFunctionUrlConfig {
                function_name: percent_decode(function_name)?,
                qualifier: query.get("Qualifier").cloned(),
            })
        }
        ("GET", ["2021-10-31", "functions", function_name, "url"]) => {
            Ok(LambdaRoute::GetFunctionUrlConfig {
                function_name: percent_decode(function_name)?,
                qualifier: query.get("Qualifier").cloned(),
            })
        }
        ("PUT", ["2021-10-31", "functions", function_name, "url"]) => {
            Ok(LambdaRoute::UpdateFunctionUrlConfig {
                function_name: percent_decode(function_name)?,
                qualifier: query.get("Qualifier").cloned(),
            })
        }
        ("DELETE", ["2021-10-31", "functions", function_name, "url"]) => {
            Ok(LambdaRoute::DeleteFunctionUrlConfig {
                function_name: percent_decode(function_name)?,
                qualifier: query.get("Qualifier").cloned(),
            })
        }
        ("GET", ["2021-10-31", "functions", function_name, "urls"]) => {
            Ok(LambdaRoute::ListFunctionUrlConfigs {
                function_name: percent_decode(function_name)?,
                marker: query.get("Marker").cloned(),
                max_items: parse_max_items(query.get("MaxItems"))?,
            })
        }
        _ => Err(LambdaError::ResourceNotFound {
            message: "Lambda route not found.".to_owned(),
        }),
    }
}

fn parse_update_code_input(
    body: UpdateFunctionCodeRequest,
) -> Result<(LambdaCodeInput, bool, Option<String>), LambdaError> {
    let code =
        match (body.image_uri, body.zip_file, body.s3_bucket, body.s3_key) {
            (Some(image_uri), None, None, None) => {
                LambdaCodeInput::ImageUri { image_uri }
            }
            (None, Some(zip_file), None, None) => LambdaCodeInput::InlineZip {
                archive: decode_zip_blob(&zip_file)?,
            },
            (None, None, Some(bucket), Some(key)) => {
                LambdaCodeInput::S3Object {
                    bucket,
                    key,
                    object_version: body.s3_object_version,
                }
            }
            _ => {
                return Err(LambdaError::InvalidParameterValue {
                message:
                    "UpdateFunctionCode requires exactly one package source."
                        .to_owned(),
            });
            }
        };

    Ok((code, body.publish.unwrap_or(false), body.revision_id))
}

fn percent_decode(value: &str) -> Result<String, LambdaError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'%' => {
                let Some(&high_byte) = bytes.get(index + 1) else {
                    return Err(LambdaError::Validation {
                        message:
                            "Invalid percent-encoding in Lambda request path."
                                .to_owned(),
                    });
                };
                let Some(&low_byte) = bytes.get(index + 2) else {
                    return Err(LambdaError::Validation {
                        message:
                            "Invalid percent-encoding in Lambda request path."
                                .to_owned(),
                    });
                };
                let high = decode_hex(high_byte)?;
                let low = decode_hex(low_byte)?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            other => {
                decoded.push(other);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|error| LambdaError::Validation {
        message: error.to_string(),
    })
}

fn decode_hex(byte: u8) -> Result<u8, LambdaError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(LambdaError::Validation {
            message: "Invalid percent-encoding in Lambda request path."
                .to_owned(),
        }),
    }
}

fn decode_zip_blob(value: &str) -> Result<Vec<u8>, LambdaError> {
    BASE64_STANDARD
        .decode(value)
        .map_err(|_| LambdaError::InvalidParameterValue {
            message:
                "Could not unzip uploaded file. Please check your file, then try to upload again."
                    .to_owned(),
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LambdaRoute {
    AddPermission {
        function_name: String,
        qualifier: Option<String>,
    },
    CreateAlias {
        function_name: String,
    },
    CreateEventSourceMapping,
    CreateFunction,
    CreateFunctionUrlConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    DeleteAlias {
        alias_name: String,
        function_name: String,
    },
    DeleteEventSourceMapping {
        uuid: String,
    },
    DeleteFunction {
        function_name: String,
    },
    DeleteFunctionEventInvokeConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    DeleteFunctionUrlConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    GetEventSourceMapping {
        uuid: String,
    },
    GetAlias {
        alias_name: String,
        function_name: String,
    },
    GetFunction {
        function_name: String,
        qualifier: Option<String>,
    },
    GetFunctionEventInvokeConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    GetFunctionUrlConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    Invoke {
        function_name: String,
        qualifier: Option<String>,
    },
    ListAliases {
        function_name: String,
    },
    ListEventSourceMappings {
        event_source_arn: Option<String>,
        function_name: Option<String>,
        marker: Option<String>,
        max_items: Option<usize>,
    },
    ListFunctionEventInvokeConfigs {
        function_name: String,
        marker: Option<String>,
        max_items: Option<usize>,
    },
    ListFunctions,
    ListFunctionUrlConfigs {
        function_name: String,
        marker: Option<String>,
        max_items: Option<usize>,
    },
    ListVersionsByFunction {
        function_name: String,
    },
    PublishVersion {
        function_name: String,
    },
    PutFunctionEventInvokeConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    UpdateAlias {
        alias_name: String,
        function_name: String,
    },
    UpdateEventSourceMapping {
        uuid: String,
    },
    UpdateFunctionCode {
        function_name: String,
    },
    UpdateFunctionEventInvokeConfig {
        function_name: String,
        qualifier: Option<String>,
    },
    UpdateFunctionUrlConfig {
        function_name: String,
        qualifier: Option<String>,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateAliasRequest {
    description: Option<String>,
    function_version: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateFunctionRequest {
    code: FunctionCodeRequest,
    dead_letter_config: Option<DeadLetterConfigRequest>,
    description: Option<String>,
    environment: Option<LambdaEnvironmentRequest>,
    function_name: Option<String>,
    handler: Option<String>,
    memory_size: Option<u32>,
    package_type: Option<String>,
    publish: Option<bool>,
    role: Option<String>,
    runtime: Option<String>,
    timeout: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct FunctionCodeRequest {
    image_uri: Option<String>,
    s3_bucket: Option<String>,
    s3_key: Option<String>,
    s3_object_version: Option<String>,
    zip_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeadLetterConfigRequest {
    target_arn: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LambdaEnvironmentRequest {
    #[serde(default)]
    variables: BTreeMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PublishVersionRequest {
    description: Option<String>,
    revision_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateAliasRequest {
    description: Option<String>,
    function_version: Option<String>,
    revision_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateFunctionCodeRequest {
    image_uri: Option<String>,
    publish: Option<bool>,
    revision_id: Option<String>,
    s3_bucket: Option<String>,
    s3_key: Option<String>,
    s3_object_version: Option<String>,
    zip_file: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateFunctionUrlConfigRequest {
    auth_type: Option<String>,
    invoke_mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateFunctionUrlConfigRequest {
    auth_type: Option<String>,
    invoke_mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AddPermissionRequest {
    action: Option<String>,
    function_url_auth_type: Option<String>,
    principal: Option<String>,
    revision_id: Option<String>,
    source_arn: Option<String>,
    statement_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct FunctionEventInvokeConfigRequest {
    destination_config: Option<DestinationConfigRequest>,
    maximum_event_age_in_seconds: Option<u32>,
    maximum_retry_attempts: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DestinationConfigRequest {
    on_failure: Option<DestinationTargetRequest>,
    on_success: Option<DestinationTargetRequest>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DestinationTargetRequest {
    destination: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateEventSourceMappingRequest {
    batch_size: Option<u32>,
    enabled: Option<bool>,
    event_source_arn: Option<String>,
    function_name: Option<String>,
    maximum_batching_window_in_seconds: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UpdateEventSourceMappingRequest {
    batch_size: Option<u32>,
    enabled: Option<bool>,
    maximum_batching_window_in_seconds: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct FunctionUrlConfigResponse {
    auth_type: String,
    creation_time: String,
    function_arn: String,
    function_url: String,
    invoke_mode: String,
    last_modified_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct ListFunctionUrlConfigsResponse {
    function_url_configs: Vec<FunctionUrlConfigResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_marker: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct AddPermissionResponse {
    statement: String,
}

#[cfg(test)]
mod tests {
    use super::is_rest_json_request;
    use crate::runtime::EdgeRouter;
    use crate::test_runtime;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const PROVIDED_BOOTSTRAP_ZIP_BASE64: &str = "UEsDBBQAAAAAAAAAIQCEK9lNDgAAAA4AAAAJAAAAYm9vdHN0cmFwIyEvYmluL3NoCmNhdApQSwECFAMUAAAAAAAAACEAhCvZTQ4AAAAOAAAACQAAAAAAAAAAAAAA7QEAAAAAYm9vdHN0cmFwUEsFBgAAAAABAAEANwAAADUAAAAAAA==";

    fn router() -> EdgeRouter {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        test_runtime::router_with_http_forwarder(
            &format!("http-lambda-tests-{id}"),
            None,
        )
    }

    fn create_request(function_name: &str) -> Vec<u8> {
        let body = json!({
            "Code": {
                "ZipFile": PROVIDED_BOOTSTRAP_ZIP_BASE64,
            },
            "FunctionName": function_name,
            "Handler": "bootstrap.handler",
            "Role": "arn:aws:iam::000000000000:role/service-role/lambda-role",
            "Runtime": "provided.al2",
        })
        .to_string();

        format!(
            "POST /2015-03-31/functions HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn invoke_request(
        function_name: &str,
        qualifier: Option<&str>,
        invocation_type: &str,
        body: &str,
    ) -> Vec<u8> {
        let qualifier = qualifier
            .map(|qualifier| format!("?Qualifier={qualifier}"))
            .unwrap_or_default();

        format!(
            "POST /2015-03-31/functions/{function_name}/invocations{qualifier} HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/json\r\nX-Amz-Invocation-Type: {invocation_type}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn json_request(method: &str, path: &str, body: &str) -> Vec<u8> {
        format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn form_request(body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn get_request(path: &str) -> Vec<u8> {
        format!("GET {path} HTTP/1.1\r\nHost: localhost:4566\r\n\r\n")
            .into_bytes()
    }

    fn split_response(
        response: &[u8],
    ) -> (String, Vec<(String, String)>, String) {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should contain a header terminator");
        let headers = std::str::from_utf8(&response[..header_end])
            .expect("response headers should be UTF-8");
        let body = std::str::from_utf8(&response[header_end + 4..])
            .expect("response body should be UTF-8");
        let mut lines = headers.split("\r\n");
        let status =
            lines.next().expect("status line should exist").to_owned();
        let parsed_headers = lines
            .map(|line| {
                line.split_once(':').expect("header should contain ':'")
            })
            .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
            .collect();

        (status, parsed_headers, body.to_owned())
    }

    fn header_value<'a>(
        headers: &'a [(String, String)],
        name: &str,
    ) -> Option<&'a str> {
        headers.iter().find_map(|(header, value)| {
            header.eq_ignore_ascii_case(name).then_some(value.as_str())
        })
    }

    #[test]
    fn lambda_core_rest_json_create_and_request_response_invoke_round_trip() {
        let router = router();

        let create = router.handle_bytes(&create_request("demo"));
        let invoke = router.handle_bytes(&invoke_request(
            "demo",
            None,
            "RequestResponse",
            r#"{"hello":"world"}"#,
        ));

        let (create_status, _, create_body) =
            split_response(&create.to_http_bytes());
        let (invoke_status, invoke_headers, invoke_body) =
            split_response(&invoke.to_http_bytes());
        let create_json: serde_json::Value =
            serde_json::from_str(&create_body)
                .expect("create response should be JSON");

        assert_eq!(create_status, "HTTP/1.1 201 Created");
        assert_eq!(create_json["FunctionName"], "demo");
        assert_eq!(create_json["Version"], "$LATEST");
        assert_eq!(invoke_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&invoke_headers, "X-Amz-Executed-Version"),
            Some("$LATEST")
        );
        assert_eq!(invoke_body, r#"{"hello":"world"}"#);
    }

    #[test]
    fn lambda_core_rest_json_version_and_alias_routes_round_trip() {
        let router = router();

        router.handle_bytes(&create_request("demo"));
        let publish = router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions/demo/versions",
            "{}",
        ));
        let create_alias = router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions/demo/aliases",
            r#"{"FunctionVersion":"1","Name":"live"}"#,
        ));
        let get_alias = router.handle_bytes(&get_request(
            "/2015-03-31/functions/demo/aliases/live",
        ));
        let list_aliases = router
            .handle_bytes(&get_request("/2015-03-31/functions/demo/aliases"));
        let list_versions = router
            .handle_bytes(&get_request("/2015-03-31/functions/demo/versions"));

        let (publish_status, _, publish_body) =
            split_response(&publish.to_http_bytes());
        let (alias_status, _, alias_body) =
            split_response(&create_alias.to_http_bytes());
        let (get_alias_status, _, get_alias_body) =
            split_response(&get_alias.to_http_bytes());
        let (_, _, list_aliases_body) =
            split_response(&list_aliases.to_http_bytes());
        let (_, _, list_versions_body) =
            split_response(&list_versions.to_http_bytes());

        assert_eq!(publish_status, "HTTP/1.1 201 Created");
        assert_eq!(alias_status, "HTTP/1.1 201 Created");
        assert_eq!(get_alias_status, "HTTP/1.1 200 OK");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&publish_body)
                .expect("publish response should be JSON")["Version"],
            "1"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&alias_body)
                .expect("alias response should be JSON")["FunctionVersion"],
            "1"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&get_alias_body)
                .expect("get alias response should be JSON")["Name"],
            "live"
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&list_aliases_body)
                .expect("list aliases response should be JSON")["Aliases"]
                .as_array()
                .is_some_and(|aliases| aliases.len() == 1)
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&list_versions_body)
                .expect("list versions response should be JSON")["Versions"]
                .as_array()
                .is_some_and(|versions| versions.len() == 2)
        );
    }

    #[test]
    fn lambda_core_rest_json_dry_run_and_missing_qualifier_fail_cleanly() {
        let router = router();

        router.handle_bytes(&create_request("demo"));
        let dry_run = router.handle_bytes(&invoke_request(
            "demo",
            None,
            "DryRun",
            r#"{"noop":true}"#,
        ));
        let missing_qualifier = router.handle_bytes(&invoke_request(
            "demo",
            Some("missing"),
            "RequestResponse",
            r#"{"hello":"world"}"#,
        ));

        let (dry_run_status, dry_run_headers, dry_run_body) =
            split_response(&dry_run.to_http_bytes());
        let (missing_status, missing_headers, missing_body) =
            split_response(&missing_qualifier.to_http_bytes());

        assert_eq!(dry_run_status, "HTTP/1.1 204 No Content");
        assert_eq!(
            header_value(&dry_run_headers, "X-Amz-Executed-Version"),
            Some("$LATEST")
        );
        assert!(dry_run_body.is_empty());

        assert_eq!(missing_status, "HTTP/1.1 404 Not Found");
        assert_eq!(
            header_value(&missing_headers, "x-amzn-errortype"),
            Some("ResourceNotFoundException")
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&missing_body)
                .expect("missing-qualifier response should be JSON"),
            json!({
                "Type": "User",
                "message": "Function not found: arn:aws:lambda:eu-west-2:000000000000:function:demo:missing",
            })
        );
    }

    #[test]
    fn lambda_url_rest_json_function_url_control_plane_round_trip() {
        let router = router();

        router.handle_bytes(&create_request("demo"));
        router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions/demo/versions",
            "{}",
        ));
        router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions/demo/aliases",
            r#"{"FunctionVersion":"1","Name":"live"}"#,
        ));
        let create = router.handle_bytes(&json_request(
            "POST",
            "/2021-10-31/functions/demo/url?Qualifier=live",
            r#"{"AuthType":"NONE"}"#,
        ));
        let create_bytes = create.to_http_bytes();
        let (create_status, _, create_body) = split_response(&create_bytes);
        let create_json: serde_json::Value =
            serde_json::from_str(&create_body).unwrap();
        let function_url = create_json["FunctionUrl"]
            .as_str()
            .expect("function URL should be present")
            .to_owned();
        assert_eq!(create_status, "HTTP/1.1 201 Created");
        assert!(
            function_url.contains(".lambda-url.eu-west-2.localhost:4566/")
        );
        assert_eq!(create_json["AuthType"], "NONE");
        assert_eq!(create_json["InvokeMode"], "BUFFERED");

        let get = router.handle_bytes(&get_request(
            "/2021-10-31/functions/demo/url?Qualifier=live",
        ));
        let list = router
            .handle_bytes(&get_request("/2021-10-31/functions/demo/urls"));
        let update = router.handle_bytes(&json_request(
            "PUT",
            "/2021-10-31/functions/demo/url?Qualifier=live",
            r#"{"AuthType":"AWS_IAM","InvokeMode":"BUFFERED"}"#,
        ));
        let add_permission = router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/functions/demo/policy?Qualifier=live",
            r#"{"Action":"lambda:InvokeFunctionUrl","FunctionUrlAuthType":"AWS_IAM","Principal":"000000000000","StatementId":"allow-account"}"#,
        ));
        let delete = router.handle_bytes(&json_request(
            "DELETE",
            "/2021-10-31/functions/demo/url?Qualifier=live",
            "",
        ));

        let (get_status, _, get_body) = split_response(&get.to_http_bytes());
        let (_, _, list_body) = split_response(&list.to_http_bytes());
        let (update_status, _, update_body) =
            split_response(&update.to_http_bytes());
        let (permission_status, _, permission_body) =
            split_response(&add_permission.to_http_bytes());
        let (delete_status, _, delete_body) =
            split_response(&delete.to_http_bytes());

        assert_eq!(get_status, "HTTP/1.1 200 OK");
        assert_eq!(update_status, "HTTP/1.1 200 OK");
        assert_eq!(permission_status, "HTTP/1.1 201 Created");
        assert_eq!(delete_status, "HTTP/1.1 204 No Content");
        assert!(delete_body.is_empty());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&get_body).unwrap()["FunctionUrl"],
            function_url
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&list_body)
                .unwrap()["FunctionUrlConfigs"]
                .as_array()
                .is_some_and(|configs| configs.len() == 1)
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&update_body).unwrap()["AuthType"],
            "AWS_IAM"
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&permission_body)
                .unwrap()["Statement"]
                .as_str()
                .is_some_and(|statement| statement.contains("AWS_IAM"))
        );
    }

    #[test]
    fn lambda_url_rest_json_async_config_and_sqs_mapping_round_trip() {
        let router = router();
        let queue_name = "lambda-http-events";
        let queue_arn =
            format!("arn:aws:sqs:eu-west-2:000000000000:{queue_name}");

        router.handle_bytes(&create_request("demo"));
        router.handle_bytes(&form_request(&format!(
            "Action=CreateQueue&QueueName={queue_name}"
        )));
        let put_event_invoke = router.handle_bytes(&json_request(
            "PUT",
            "/2019-09-25/functions/demo/event-invoke-config",
            &json!({
                "DestinationConfig": {
                    "OnFailure": { "Destination": queue_arn }
                },
                "MaximumRetryAttempts": 0
            })
            .to_string(),
        ));
        let list_event_invoke = router.handle_bytes(&get_request(
            "/2019-09-25/functions/demo/event-invoke-config/list",
        ));
        let create_mapping = router.handle_bytes(&json_request(
            "POST",
            "/2015-03-31/event-source-mappings",
            &json!({
                "BatchSize": 5,
                "Enabled": true,
                "EventSourceArn": queue_arn,
                "FunctionName": "demo",
                "MaximumBatchingWindowInSeconds": 0
            })
            .to_string(),
        ));
        let create_mapping_json: serde_json::Value = serde_json::from_str(
            &split_response(&create_mapping.to_http_bytes()).2,
        )
        .unwrap();
        let mapping_uuid = create_mapping_json["UUID"]
            .as_str()
            .expect("mapping UUID should be present");
        let update_mapping = router.handle_bytes(&json_request(
            "PUT",
            &format!("/2015-03-31/event-source-mappings/{mapping_uuid}"),
            r#"{"BatchSize":20,"Enabled":false,"MaximumBatchingWindowInSeconds":1}"#,
        ));
        let delete_mapping = router.handle_bytes(&json_request(
            "DELETE",
            &format!("/2015-03-31/event-source-mappings/{mapping_uuid}"),
            "",
        ));

        let (put_status, _, put_body) =
            split_response(&put_event_invoke.to_http_bytes());
        let (_, _, list_body) =
            split_response(&list_event_invoke.to_http_bytes());
        let (create_status, _, create_body) =
            split_response(&create_mapping.to_http_bytes());
        let (update_status, _, update_body) =
            split_response(&update_mapping.to_http_bytes());
        let (delete_status, _, delete_body) =
            split_response(&delete_mapping.to_http_bytes());

        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert_eq!(create_status, "HTTP/1.1 201 Created");
        assert_eq!(update_status, "HTTP/1.1 200 OK");
        assert_eq!(delete_status, "HTTP/1.1 202 Accepted");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&put_body).unwrap()["DestinationConfig"]
                ["OnFailure"]["Destination"],
            queue_arn
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&list_body)
                .unwrap()["FunctionEventInvokeConfigs"]
                .as_array()
                .is_some_and(|configs| configs.len() == 1)
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&create_body).unwrap()["State"],
            "Creating"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&update_body).unwrap()["State"],
            "Disabled"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&delete_body).unwrap()["State"],
            "Deleting"
        );
    }

    #[test]
    fn lambda_core_rest_json_detection_matches_lambda_paths() {
        let request = crate::request::HttpRequest::parse(
            b"POST /2015-03-31/functions/demo/invocations HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should parse");

        assert!(is_rest_json_request(&request));
        let request = crate::request::HttpRequest::parse(
            b"POST /2021-10-31/functions/demo/url HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .expect("request should parse");
        assert!(is_rest_json_request(&request));
    }
}
