pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, RequestContext};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use services::{
    SsmAddTagsToResourceInput, SsmDeleteParameterInput,
    SsmDeleteParametersInput, SsmDescribeFilter, SsmDescribeParametersInput,
    SsmError, SsmGetParameterHistoryInput, SsmGetParameterInput,
    SsmGetParametersByPathInput, SsmGetParametersInput,
    SsmLabelParameterVersionInput, SsmListTagsForResourceInput, SsmParameter,
    SsmParameterHistoryEntry, SsmParameterMetadata, SsmPutParameterInput,
    SsmRemoveTagsFromResourceInput, SsmResourceType, SsmScope, SsmService,
    SsmTag,
};

pub(crate) fn handle_json(
    ssm: &SsmService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let operation = operation_from_target(request.header("x-amz-target"))
        .map_err(|error| error.to_aws_error())?;
    let scope =
        SsmScope::new(context.account_id().clone(), context.region().clone());

    match operation {
        SsmOperation::PutParameter => json_response(&put_parameter_response(
            &ssm.put_parameter(
                &scope,
                parse_put_parameter_input(request.body())?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        SsmOperation::GetParameter => json_response(&get_parameter_response(
            &ssm.get_parameter(
                &scope,
                parse_get_parameter_input(request.body())?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        SsmOperation::GetParameters => {
            json_response(&get_parameters_response(
                &ssm.get_parameters(
                    &scope,
                    parse_get_parameters_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::GetParametersByPath => {
            json_response(&get_parameters_by_path_response(
                &ssm.get_parameters_by_path(
                    &scope,
                    parse_get_parameters_by_path_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::DeleteParameter => {
            ssm.delete_parameter(
                &scope,
                parse_delete_parameter_input(request.body())?,
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        SsmOperation::DeleteParameters => {
            json_response(&delete_parameters_response(
                &ssm.delete_parameters(
                    &scope,
                    parse_delete_parameters_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::GetParameterHistory => {
            json_response(&get_parameter_history_response(
                &ssm.get_parameter_history(
                    &scope,
                    parse_get_parameter_history_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::DescribeParameters => {
            json_response(&describe_parameters_response(
                &ssm.describe_parameters(
                    &scope,
                    parse_describe_parameters_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::LabelParameterVersion => {
            json_response(&label_parameter_version_response(
                &ssm.label_parameter_version(
                    &scope,
                    parse_label_parameter_version_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::AddTagsToResource => {
            ssm.add_tags_to_resource(
                &scope,
                parse_add_tags_to_resource_input(request.body())?,
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
        SsmOperation::ListTagsForResource => {
            json_response(&list_tags_for_resource_response(
                &ssm.list_tags_for_resource(
                    &scope,
                    parse_list_tags_for_resource_input(request.body())?,
                )
                .map_err(|error| error.to_aws_error())?,
            ))
        }
        SsmOperation::RemoveTagsFromResource => {
            ssm.remove_tags_from_resource(
                &scope,
                parse_remove_tags_from_resource_input(request.body())?,
            )
            .map_err(|error| error.to_aws_error())?;
            json_response(&json!({}))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PutParameterRequest {
    description: Option<String>,
    name: String,
    overwrite: Option<bool>,
    #[serde(rename = "Type")]
    parameter_type: Option<String>,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParameterRequest {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParametersRequest {
    names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteParameterRequest {
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteParametersRequest {
    names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParameterHistoryRequest {
    max_results: Option<u32>,
    name: String,
    next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LabelParameterVersionRequest {
    labels: Vec<String>,
    name: String,
    parameter_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagRequest {
    key: String,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AddTagsToResourceRequest {
    resource_id: String,
    resource_type: String,
    tags: Vec<TagRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListTagsForResourceRequest {
    resource_id: String,
    resource_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RemoveTagsFromResourceRequest {
    resource_id: String,
    resource_type: String,
    tag_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ParameterFilterRequest {
    key: String,
    option: Option<String>,
    values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeFilterRequest {
    key: String,
    values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetParametersByPathRequest {
    max_results: Option<u32>,
    next_token: Option<String>,
    parameter_filters: Option<Vec<ParameterFilterRequest>>,
    path: String,
    recursive: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DescribeParametersRequest {
    filters: Option<Vec<DescribeFilterRequest>>,
    max_results: Option<u32>,
    next_token: Option<String>,
    parameter_filters: Option<Vec<ParameterFilterRequest>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SsmOperation {
    PutParameter,
    GetParameter,
    GetParameters,
    GetParametersByPath,
    DeleteParameter,
    DeleteParameters,
    GetParameterHistory,
    DescribeParameters,
    LabelParameterVersion,
    AddTagsToResource,
    ListTagsForResource,
    RemoveTagsFromResource,
}

fn action_from_target(target: Option<&str>) -> Result<&str, SsmError> {
    let target = target.ok_or_else(|| SsmError::UnsupportedOperation {
        message: "missing X-Amz-Target".to_owned(),
    })?;
    let Some((prefix, operation)) = target.split_once('.') else {
        return Err(SsmError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    };
    if prefix != "AmazonSSM" {
        return Err(SsmError::UnsupportedOperation {
            message: format!("Operation {target} is not supported."),
        });
    }

    Ok(operation)
}

fn operation_from_target(
    target: Option<&str>,
) -> Result<SsmOperation, SsmError> {
    let operation = action_from_target(target)?;

    match operation {
        "PutParameter" => Ok(SsmOperation::PutParameter),
        "GetParameter" => Ok(SsmOperation::GetParameter),
        "GetParameters" => Ok(SsmOperation::GetParameters),
        "GetParametersByPath" => Ok(SsmOperation::GetParametersByPath),
        "DeleteParameter" => Ok(SsmOperation::DeleteParameter),
        "DeleteParameters" => Ok(SsmOperation::DeleteParameters),
        "GetParameterHistory" => Ok(SsmOperation::GetParameterHistory),
        "DescribeParameters" => Ok(SsmOperation::DescribeParameters),
        "LabelParameterVersion" => Ok(SsmOperation::LabelParameterVersion),
        "AddTagsToResource" => Ok(SsmOperation::AddTagsToResource),
        "ListTagsForResource" => Ok(SsmOperation::ListTagsForResource),
        "RemoveTagsFromResource" => Ok(SsmOperation::RemoveTagsFromResource),
        _ => Err(SsmError::UnsupportedOperation {
            message: format!("Operation {operation} is not supported."),
        }),
    }
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, AwsError>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(body).map_err(|error| {
        SsmError::ValidationException {
            message: format!("The request body is not valid JSON: {error}"),
        }
        .to_aws_error()
    })
}

fn json_response<T>(value: &T) -> Result<Vec<u8>, AwsError>
where
    T: Serialize,
{
    serde_json::to_vec(value).map_err(|error| {
        SsmError::InternalFailure {
            message: format!("Failed to serialize SSM response: {error}"),
        }
        .to_aws_error()
    })
}

fn parse_put_parameter_input(
    body: &[u8],
) -> Result<SsmPutParameterInput, AwsError> {
    let request = parse_json_body::<PutParameterRequest>(body)?;
    Ok(SsmPutParameterInput {
        description: request.description,
        name: services::ParameterName::parse(request.name)
            .map_err(|error| error.to_aws_error())?,
        overwrite: request.overwrite,
        parameter_type: request
            .parameter_type
            .as_deref()
            .map(services::SsmParameterType::parse)
            .transpose()
            .map_err(|error| error.to_aws_error())?,
        value: request.value,
    })
}

fn parse_get_parameter_input(
    body: &[u8],
) -> Result<SsmGetParameterInput, AwsError> {
    let request = parse_json_body::<GetParameterRequest>(body)?;
    Ok(SsmGetParameterInput {
        reference: services::ParameterReference::parse(&request.name)
            .map_err(|error| error.to_aws_error())?,
    })
}

fn parse_get_parameters_input(
    body: &[u8],
) -> Result<SsmGetParametersInput, AwsError> {
    let request = parse_json_body::<GetParametersRequest>(body)?;
    Ok(SsmGetParametersInput { names: request.names })
}

fn parse_get_parameters_by_path_input(
    body: &[u8],
) -> Result<SsmGetParametersByPathInput, AwsError> {
    let request = parse_json_body::<GetParametersByPathRequest>(body)?;
    let label_filter = parse_path_label_filter(
        request.parameter_filters.unwrap_or_default(),
    )?;

    Ok(SsmGetParametersByPathInput {
        label_filter,
        max_results: request.max_results,
        next_token: request.next_token,
        path: services::ParameterPath::parse(request.path)
            .map_err(|error| error.to_aws_error())?,
        recursive: request.recursive.unwrap_or(false),
    })
}

fn parse_delete_parameter_input(
    body: &[u8],
) -> Result<SsmDeleteParameterInput, AwsError> {
    let request = parse_json_body::<DeleteParameterRequest>(body)?;
    Ok(SsmDeleteParameterInput {
        name: services::ParameterName::parse(request.name)
            .map_err(|error| error.to_aws_error())?,
    })
}

fn parse_delete_parameters_input(
    body: &[u8],
) -> Result<SsmDeleteParametersInput, AwsError> {
    let request = parse_json_body::<DeleteParametersRequest>(body)?;
    Ok(SsmDeleteParametersInput { names: request.names })
}

fn parse_get_parameter_history_input(
    body: &[u8],
) -> Result<SsmGetParameterHistoryInput, AwsError> {
    let request = parse_json_body::<GetParameterHistoryRequest>(body)?;
    Ok(SsmGetParameterHistoryInput {
        max_results: request.max_results,
        name: services::ParameterName::parse(request.name)
            .map_err(|error| error.to_aws_error())?,
        next_token: request.next_token,
    })
}

fn parse_describe_parameters_input(
    body: &[u8],
) -> Result<SsmDescribeParametersInput, AwsError> {
    let request = parse_json_body::<DescribeParametersRequest>(body)?;
    let mut filters = Vec::new();

    if let Some(parameter_filters) = request.parameter_filters {
        for filter in parameter_filters {
            let key = &filter.key;
            let option = filter.option.as_deref();
            let values = &filter.values;
            let parsed_filter = parse_name_equals_filter(key, option, values)?;
            filters.push(parsed_filter);
        }
    }

    if let Some(legacy_filters) = request.filters {
        for filter in legacy_filters {
            let parsed_filter = parse_name_equals_filter(
                &filter.key,
                Some("Equals"),
                &filter.values,
            )?;
            filters.push(parsed_filter);
        }
    }

    Ok(SsmDescribeParametersInput {
        filters,
        max_results: request.max_results,
        next_token: request.next_token,
    })
}

fn parse_label_parameter_version_input(
    body: &[u8],
) -> Result<SsmLabelParameterVersionInput, AwsError> {
    let request = parse_json_body::<LabelParameterVersionRequest>(body)?;
    Ok(SsmLabelParameterVersionInput {
        labels: request.labels,
        name: services::ParameterName::parse(request.name)
            .map_err(|error| error.to_aws_error())?,
        parameter_version: request.parameter_version,
    })
}

fn parse_add_tags_to_resource_input(
    body: &[u8],
) -> Result<SsmAddTagsToResourceInput, AwsError> {
    let request = parse_json_body::<AddTagsToResourceRequest>(body)?;
    Ok(SsmAddTagsToResourceInput {
        resource_id: services::ParameterName::parse(request.resource_id)
            .map_err(|error| error.to_aws_error())?,
        resource_type: SsmResourceType::parse(&request.resource_type)
            .map_err(|error| error.to_aws_error())?,
        tags: request
            .tags
            .into_iter()
            .map(|tag| SsmTag { key: tag.key, value: tag.value })
            .collect(),
    })
}

fn parse_list_tags_for_resource_input(
    body: &[u8],
) -> Result<SsmListTagsForResourceInput, AwsError> {
    let request = parse_json_body::<ListTagsForResourceRequest>(body)?;
    Ok(SsmListTagsForResourceInput {
        resource_id: services::ParameterName::parse(request.resource_id)
            .map_err(|error| error.to_aws_error())?,
        resource_type: SsmResourceType::parse(&request.resource_type)
            .map_err(|error| error.to_aws_error())?,
    })
}

fn parse_remove_tags_from_resource_input(
    body: &[u8],
) -> Result<SsmRemoveTagsFromResourceInput, AwsError> {
    let request = parse_json_body::<RemoveTagsFromResourceRequest>(body)?;
    Ok(SsmRemoveTagsFromResourceInput {
        resource_id: services::ParameterName::parse(request.resource_id)
            .map_err(|error| error.to_aws_error())?,
        resource_type: SsmResourceType::parse(&request.resource_type)
            .map_err(|error| error.to_aws_error())?,
        tag_keys: request.tag_keys,
    })
}

fn parse_path_label_filter(
    filters: Vec<ParameterFilterRequest>,
) -> Result<Option<Vec<String>>, AwsError> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut labels = Vec::new();
    for filter in filters {
        if filter.key != "Label" {
            return Err(SsmError::ValidationException {
                message:
                    "GetParametersByPath only supports Label Equals filters."
                        .to_owned(),
            }
            .to_aws_error());
        }
        if filter.option.as_deref().unwrap_or("Equals") != "Equals" {
            return Err(SsmError::ValidationException {
                message:
                    "GetParametersByPath only supports Label Equals filters."
                        .to_owned(),
            }
            .to_aws_error());
        }
        for value in filter.values {
            if !labels.contains(&value) {
                labels.push(value);
            }
        }
    }

    Ok(Some(labels))
}

fn parse_name_equals_filter(
    key: &str,
    option: Option<&str>,
    values: &[String],
) -> Result<SsmDescribeFilter, AwsError> {
    if key != "Name" || option.unwrap_or("Equals") != "Equals" {
        return Err(SsmError::ValidationException {
            message: "DescribeParameters only supports Name Equals filters."
                .to_owned(),
        }
        .to_aws_error());
    }

    Ok(SsmDescribeFilter::NameEquals(
        values
            .iter()
            .map(services::ParameterName::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_aws_error())?,
    ))
}

fn put_parameter_response(output: &services::SsmPutParameterOutput) -> Value {
    json!({ "Version": output.version })
}

fn get_parameter_response(parameter: &SsmParameter) -> Value {
    json!({ "Parameter": parameter_json(parameter) })
}

fn get_parameters_response(
    output: &services::SsmGetParametersOutput,
) -> Value {
    json!({
        "InvalidParameters": output.invalid_parameters,
        "Parameters": output
            .parameters
            .iter()
            .map(parameter_json)
            .collect::<Vec<_>>(),
    })
}

fn get_parameters_by_path_response(
    output: &services::SsmGetParametersByPathOutput,
) -> Value {
    json!({
        "Parameters": output
            .parameters
            .iter()
            .map(parameter_json)
            .collect::<Vec<_>>(),
    })
}

fn delete_parameters_response(
    output: &services::SsmDeleteParametersOutput,
) -> Value {
    json!({
        "DeletedParameters": output.deleted_parameters,
        "InvalidParameters": output.invalid_parameters,
    })
}

fn get_parameter_history_response(
    output: &services::SsmGetParameterHistoryOutput,
) -> Value {
    json!({
        "Parameters": output
            .parameters
            .iter()
            .map(parameter_history_entry_json)
            .collect::<Vec<_>>(),
    })
}

fn describe_parameters_response(
    output: &services::SsmDescribeParametersOutput,
) -> Value {
    json!({
        "Parameters": output
            .parameters
            .iter()
            .map(parameter_metadata_json)
            .collect::<Vec<_>>(),
    })
}

fn label_parameter_version_response(
    output: &services::SsmLabelParameterVersionOutput,
) -> Value {
    json!({
        "InvalidLabels": output.invalid_labels,
        "ParameterVersion": output.parameter_version,
    })
}

fn list_tags_for_resource_response(
    output: &services::SsmListTagsForResourceOutput,
) -> Value {
    json!({
        "TagList": output
            .tag_list
            .iter()
            .map(tag_json)
            .collect::<Vec<_>>(),
    })
}

fn parameter_json(parameter: &SsmParameter) -> Value {
    let mut value = json!({
        "ARN": parameter.arn,
        "DataType": parameter.data_type,
        "LastModifiedDate": epoch_millis_as_seconds(parameter.last_modified_date),
        "Name": parameter.name.as_str(),
        "Type": parameter.parameter_type.as_str(),
        "Value": parameter.value,
        "Version": parameter.version,
    });
    if let Some(selector) = parameter.selector.as_deref() {
        insert_object_field(&mut value, "Selector", json!(selector));
    }
    value
}

fn parameter_history_entry_json(entry: &SsmParameterHistoryEntry) -> Value {
    let mut value = json!({
        "DataType": entry.data_type,
        "LastModifiedDate": epoch_millis_as_seconds(entry.last_modified_date),
        "Name": entry.name.as_str(),
        "Type": entry.parameter_type.as_str(),
        "Value": entry.value,
        "Version": entry.version,
    });
    if let Some(description) = entry.description.as_deref() {
        insert_object_field(&mut value, "Description", json!(description));
    }
    if !entry.labels.is_empty() {
        insert_object_field(&mut value, "Labels", json!(entry.labels));
    }
    value
}

fn parameter_metadata_json(metadata: &SsmParameterMetadata) -> Value {
    let mut value = json!({
        "ARN": metadata.arn,
        "DataType": metadata.data_type,
        "LastModifiedDate": epoch_millis_as_seconds(metadata.last_modified_date),
        "Name": metadata.name.as_str(),
        "Type": metadata.parameter_type.as_str(),
        "Version": metadata.version,
    });
    if let Some(description) = metadata.description.as_deref() {
        insert_object_field(&mut value, "Description", json!(description));
    }
    value
}

fn insert_object_field(value: &mut Value, field: &str, inserted: Value) {
    if let Some(object) = value.as_object_mut() {
        object.insert(field.to_owned(), inserted);
    }
}

fn tag_json(tag: &SsmTag) -> Value {
    json!({
        "Key": tag.key,
        "Value": tag.value,
    })
}

fn epoch_millis_as_seconds(epoch_millis: u64) -> f64 {
    epoch_millis as f64 / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_runtime::{self, FixedClock};
    use aws::{ProtocolFamily, ServiceName};
    use serde::Serializer;
    use serde::ser::Error as _;
    use services::SsmService;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    fn service() -> SsmService {
        let factory = StorageFactory::new(StorageConfig::new(
            "/tmp/http-ssm-service",
            StorageMode::Memory,
        ));

        SsmService::with_max_history(
            &factory,
            Arc::new(FixedClock::new(UNIX_EPOCH)),
            5,
        )
    }

    fn context(operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Ssm,
            ProtocolFamily::AwsJson11,
            operation,
            None,
            false,
        )
        .expect("request context should build")
    }

    fn request_bytes(target: &str, body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn call(operation: &str, body: &str) -> Result<Vec<u8>, AwsError> {
        let target = format!("AmazonSSM.{operation}");
        let request_bytes = request_bytes(&target, body);
        let request =
            HttpRequest::parse(&request_bytes).expect("request should parse");

        handle_json(&service(), &request, &context(operation))
    }

    fn router() -> crate::runtime::EdgeRouter {
        test_runtime::router_with_http_forwarder("http-ssm-router", None)
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

    #[test]
    fn ssm_json_direct_handler_round_trips_core_operations() {
        let service = service();
        let put_request_bytes = request_bytes(
            "AmazonSSM.PutParameter",
            r#"{"Name":"/app/db/host","Value":"localhost","Type":"String"}"#,
        );
        let put_request = HttpRequest::parse(&put_request_bytes)
            .expect("request should parse");
        let get_request_bytes = request_bytes(
            "AmazonSSM.GetParameter",
            r#"{"Name":"/app/db/host"}"#,
        );
        let get_request = HttpRequest::parse(&get_request_bytes)
            .expect("request should parse");
        let get_parameters_request_bytes = request_bytes(
            "AmazonSSM.GetParameters",
            r#"{"Names":["/app/db/host","/missing"]}"#,
        );
        let get_parameters_request =
            HttpRequest::parse(&get_parameters_request_bytes)
                .expect("request should parse");
        let describe_request_bytes = request_bytes(
            "AmazonSSM.DescribeParameters",
            r#"{"ParameterFilters":[{"Key":"Name","Option":"Equals","Values":["/app/db/host"]}]}"#,
        );
        let describe_request = HttpRequest::parse(&describe_request_bytes)
            .expect("request should parse");
        let tag_request_bytes = request_bytes(
            "AmazonSSM.AddTagsToResource",
            r#"{"ResourceType":"Parameter","ResourceId":"/app/db/host","Tags":[{"Key":"env","Value":"dev"}]}"#,
        );
        let tag_request = HttpRequest::parse(&tag_request_bytes)
            .expect("request should parse");
        let list_tags_request_bytes = request_bytes(
            "AmazonSSM.ListTagsForResource",
            r#"{"ResourceType":"Parameter","ResourceId":"/app/db/host"}"#,
        );
        let list_tags_request = HttpRequest::parse(&list_tags_request_bytes)
            .expect("request should parse");

        let put =
            handle_json(&service, &put_request, &context("PutParameter"))
                .expect("put should succeed");
        let get =
            handle_json(&service, &get_request, &context("GetParameter"))
                .expect("get should succeed");
        let get_parameters = handle_json(
            &service,
            &get_parameters_request,
            &context("GetParameters"),
        )
        .expect("get parameters should succeed");
        let describe = handle_json(
            &service,
            &describe_request,
            &context("DescribeParameters"),
        )
        .expect("describe should succeed");
        let tag =
            handle_json(&service, &tag_request, &context("AddTagsToResource"))
                .expect("tag should succeed");
        let list_tags = handle_json(
            &service,
            &list_tags_request,
            &context("ListTagsForResource"),
        )
        .expect("tag listing should succeed");

        assert_eq!(
            serde_json::from_slice::<Value>(&put)
                .expect("put response should decode")["Version"],
            1
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&get)
                .expect("get response should decode")["Parameter"]["Value"],
            "localhost"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&get_parameters)
                .expect("get parameters response should decode")["InvalidParameters"],
            serde_json::json!(["/missing"])
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&describe)
                .expect("describe response should decode")["Parameters"][0]["Name"],
            "/app/db/host"
        );
        assert_eq!(tag, b"{}");
        assert_eq!(
            serde_json::from_slice::<Value>(&list_tags)
                .expect("tag list response should decode")["TagList"][0]["Key"],
            "env"
        );
    }

    #[test]
    fn ssm_json_direct_handlers_report_explicit_errors_and_gap_paths() {
        let ssm = service();
        let missing_target_request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");
        let missing_target = handle_json(
            &ssm,
            &missing_target_request,
            &context("GetParameter"),
        )
        .expect_err("missing target should fail");
        let invalid_json_request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.1\r\nX-Amz-Target: AmazonSSM.GetParameter\r\nContent-Length: 1\r\n\r\n{",
        )
        .expect("request should parse");
        let invalid_json =
            handle_json(&ssm, &invalid_json_request, &context("GetParameter"))
                .expect_err("invalid JSON should fail");
        let unsupported_filter_request_bytes = request_bytes(
            "AmazonSSM.GetParametersByPath",
            r#"{"Path":"/","ParameterFilters":[{"Key":"Type","Values":["String"]}]}"#,
        );
        let unsupported_filter_request =
            HttpRequest::parse(&unsupported_filter_request_bytes)
                .expect("request should parse");
        let unsupported_filter = handle_json(
            &ssm,
            &unsupported_filter_request,
            &context("GetParametersByPath"),
        )
        .expect_err("unsupported path filter should fail");
        let unsupported_operation =
            call("Nope", "{}").expect_err("unsupported operation should fail");
        let pagination_error =
            call("DescribeParameters", r#"{"NextToken":"next"}"#)
                .expect_err("pagination gap should fail");
        let malformed_target = action_from_target(Some("AmazonSSM"))
            .expect_err("targets without an operation should fail");
        let wrong_prefix = action_from_target(Some("Other.GetParameter"))
            .expect_err("non-SSM targets should fail");
        let bad_label_option = call(
            "GetParametersByPath",
            r#"{"Path":"/","ParameterFilters":[{"Key":"Label","Option":"BeginsWith","Values":["stable"]}]}"#,
        )
        .expect_err("unsupported label filter options should fail");
        let bad_describe_filter = call(
            "DescribeParameters",
            r#"{"Filters":[{"Key":"Type","Values":["String"]}]}"#,
        )
        .expect_err("unsupported describe filters should fail");

        assert_eq!(missing_target.code(), "UnsupportedOperation");
        assert_eq!(invalid_json.code(), "ValidationException");
        assert_eq!(unsupported_filter.code(), "ValidationException");
        assert_eq!(unsupported_operation.code(), "UnsupportedOperation");
        assert_eq!(pagination_error.code(), "ValidationException");
        assert_eq!(
            malformed_target.to_aws_error().code(),
            "UnsupportedOperation"
        );
        assert_eq!(wrong_prefix.to_aws_error().code(), "UnsupportedOperation");
        assert_eq!(bad_label_option.code(), "ValidationException");
        assert_eq!(bad_describe_filter.code(), "ValidationException");
    }

    #[test]
    fn ssm_json_helper_parsers_cover_legacy_filters_and_serializer_failures() {
        #[derive(Debug)]
        struct FailingSerialize;

        impl Serialize for FailingSerialize {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                Err(S::Error::custom("boom"))
            }
        }

        let by_path = parse_get_parameters_by_path_input(
            br#"{"Path":"/app","ParameterFilters":[{"Key":"Label","Option":"Equals","Values":["stable","stable"]}]}"#,
        )
        .expect("label filters should parse");
        let describe = parse_describe_parameters_input(
            br#"{"Filters":[{"Key":"Name","Values":["/app/db/host"]}]}"#,
        )
        .expect("legacy filters should parse");
        let serialize_error = json_response(&FailingSerialize)
            .expect_err("serializer failures should surface");

        assert_eq!(by_path.label_filter, Some(vec!["stable".to_owned()]));
        assert_eq!(
            describe.filters,
            vec![SsmDescribeFilter::NameEquals(vec![
                services::ParameterName::parse("/app/db/host")
                    .expect("filter name should parse"),
            ])]
        );
        assert_eq!(serialize_error.code(), "InternalServerError");
    }

    #[test]
    fn ssm_json_generic_helpers_cover_all_request_and_response_shapes() {
        let put = parse_put_parameter_input(
            br#"{"Name":"/app/db/host","Value":"localhost","Type":"String"}"#,
        )
        .expect("put input should parse");
        let get = parse_get_parameter_input(br#"{"Name":"/app/db/host"}"#)
            .expect("get input should parse");
        let get_many = parse_get_parameters_input(
            br#"{"Names":["/app/db/host","/missing"]}"#,
        )
        .expect("batch get input should parse");
        let delete =
            parse_delete_parameter_input(br#"{"Name":"/app/db/host"}"#)
                .expect("delete input should parse");
        let delete_many = parse_delete_parameters_input(
            br#"{"Names":["/app/db/host","/missing"]}"#,
        )
        .expect("batch delete input should parse");
        let history =
            parse_get_parameter_history_input(br#"{"Name":"/app/db/host"}"#)
                .expect("history input should parse");
        let label = parse_label_parameter_version_input(
            br#"{"Name":"/app/db/host","ParameterVersion":1,"Labels":["stable"]}"#,
        )
        .expect("label input should parse");
        let add_tags = parse_add_tags_to_resource_input(
            br#"{"ResourceType":"Parameter","ResourceId":"/app/db/host","Tags":[{"Key":"env","Value":"dev"}]}"#,
        )
        .expect("tag input should parse");
        let list_tags = parse_list_tags_for_resource_input(
            br#"{"ResourceType":"Parameter","ResourceId":"/app/db/host"}"#,
        )
        .expect("list tags input should parse");
        let remove_tags = parse_remove_tags_from_resource_input(
            br#"{"ResourceType":"Parameter","ResourceId":"/app/db/host","TagKeys":["env"]}"#,
        )
        .expect("remove tags input should parse");
        let by_path: GetParametersByPathRequest = parse_json_body(
            br#"{"Path":"/app","Recursive":true,"ParameterFilters":[{"Key":"Label","Option":"Equals","Values":["stable"]}]}"#,
        )
        .expect("path request should parse");
        let describe: DescribeParametersRequest = parse_json_body(
            br#"{"ParameterFilters":[{"Key":"Name","Option":"Equals","Values":["/app/db/host"]}]}"#,
        )
        .expect("describe request should parse");
        let parameter = SsmParameter {
            arn: "arn:aws:ssm:eu-west-2:000000000000:parameter/app/db/host"
                .to_owned(),
            data_type: "text".to_owned(),
            last_modified_date: 1_000,
            name: services::ParameterName::parse("/app/db/host")
                .expect("name should parse"),
            selector: Some(":stable".to_owned()),
            parameter_type: services::SsmParameterType::String,
            value: "localhost".to_owned(),
            version: 1,
        };
        let history_entry = SsmParameterHistoryEntry {
            data_type: "text".to_owned(),
            description: Some("demo".to_owned()),
            labels: vec!["stable".to_owned()],
            last_modified_date: 1_000,
            name: services::ParameterName::parse("/app/db/host")
                .expect("name should parse"),
            parameter_type: services::SsmParameterType::String,
            value: "localhost".to_owned(),
            version: 1,
        };
        let metadata = SsmParameterMetadata {
            arn: parameter.arn.clone(),
            data_type: "text".to_owned(),
            description: Some("demo".to_owned()),
            last_modified_date: 1_000,
            name: services::ParameterName::parse("/app/db/host")
                .expect("name should parse"),
            parameter_type: services::SsmParameterType::String,
            version: 1,
        };

        assert_eq!(put.name, "/app/db/host");
        assert_eq!(
            get.reference.name(),
            &services::ParameterName::parse("/app/db/host")
                .expect("name should parse")
        );
        assert_eq!(
            get_many.names,
            vec!["/app/db/host".to_owned(), "/missing".to_owned()]
        );
        assert_eq!(delete.name, "/app/db/host");
        assert_eq!(
            delete_many.names,
            vec!["/app/db/host".to_owned(), "/missing".to_owned()]
        );
        assert_eq!(history.name, "/app/db/host");
        assert_eq!(label.parameter_version, Some(1));
        assert_eq!(add_tags.tags.len(), 1);
        assert_eq!(list_tags.resource_type, SsmResourceType::Parameter);
        assert_eq!(remove_tags.tag_keys, vec!["env"]);
        assert_eq!(by_path.path, "/app");
        assert_eq!(describe.parameter_filters.expect("filters").len(), 1);

        assert!(
            json_response(&get_parameter_response(&parameter))
                .expect("get parameter response should serialize")
                .starts_with(b"{")
        );
        assert!(
            json_response(&put_parameter_response(
                &services::SsmPutParameterOutput { version: 1 },
            ))
            .expect("put response should serialize")
            .starts_with(b"{")
        );
        assert!(
            json_response(&get_parameters_response(
                &services::SsmGetParametersOutput {
                    invalid_parameters: vec!["/missing".to_owned()],
                    parameters: vec![parameter.clone()],
                },
            ))
            .expect("get parameters response should serialize")
            .starts_with(b"{")
        );
        assert_eq!(
            json_response(&json!({}))
                .expect("delete response should serialize"),
            b"{}"
        );
        assert!(
            json_response(&delete_parameters_response(
                &services::SsmDeleteParametersOutput {
                    deleted_parameters: vec!["/app/db/host".to_owned()],
                    invalid_parameters: vec!["/missing".to_owned()],
                },
            ))
            .expect("delete parameters response should serialize")
            .starts_with(b"{")
        );
        assert!(
            json_response(&get_parameter_history_response(
                &services::SsmGetParameterHistoryOutput {
                    parameters: vec![history_entry],
                },
            ))
            .expect("history response should serialize")
            .starts_with(b"{")
        );
        assert!(
            json_response(&describe_parameters_response(
                &services::SsmDescribeParametersOutput {
                    parameters: vec![metadata],
                },
            ))
            .expect("describe response should serialize")
            .starts_with(b"{")
        );
        assert!(
            json_response(&get_parameters_by_path_response(
                &services::SsmGetParametersByPathOutput {
                    parameters: vec![parameter],
                },
            ))
            .expect("path response should serialize")
            .starts_with(b"{")
        );
        assert!(
            json_response(&label_parameter_version_response(
                &services::SsmLabelParameterVersionOutput {
                    invalid_labels: Vec::new(),
                    parameter_version: 1,
                },
            ))
            .expect("label response should serialize")
            .starts_with(b"{")
        );
        assert_eq!(
            json_response(&json!({})).expect("tag response should serialize"),
            b"{}"
        );
        assert!(
            json_response(&list_tags_for_resource_response(
                &services::SsmListTagsForResourceOutput {
                    tag_list: vec![SsmTag {
                        key: "env".to_owned(),
                        value: "dev".to_owned(),
                    }],
                },
            ))
            .expect("list tags response should serialize")
            .starts_with(b"{")
        );
        assert_eq!(
            json_response(&json!({}))
                .expect("remove tags response should serialize"),
            b"{}"
        );
    }

    #[test]
    fn ssm_json_router_round_trips_history_labels_and_tags() {
        let router = router();

        let put = router.handle_bytes(&request_bytes(
            "AmazonSSM.PutParameter",
            r#"{"Name":"/app/db/host","Value":"v1","Type":"String"}"#,
        ));
        let overwrite = router.handle_bytes(&request_bytes(
            "AmazonSSM.PutParameter",
            r#"{"Name":"/app/db/host","Value":"v2","Type":"String","Overwrite":true}"#,
        ));
        let label = router.handle_bytes(&request_bytes(
            "AmazonSSM.LabelParameterVersion",
            r#"{"Name":"/app/db/host","ParameterVersion":1,"Labels":["stable"]}"#,
        ));
        let path = router.handle_bytes(&request_bytes(
            "AmazonSSM.GetParametersByPath",
            r#"{"Path":"/app","Recursive":true,"ParameterFilters":[{"Key":"Label","Option":"Equals","Values":["stable"]}]}"#,
        ));
        let history = router.handle_bytes(&request_bytes(
            "AmazonSSM.GetParameterHistory",
            r#"{"Name":"/app/db/host"}"#,
        ));
        let tag = router.handle_bytes(&request_bytes(
            "AmazonSSM.AddTagsToResource",
            r#"{"ResourceType":"Parameter","ResourceId":"/app/db/host","Tags":[{"Key":"env","Value":"dev"}]}"#,
        ));
        let list_tags = router.handle_bytes(&request_bytes(
            "AmazonSSM.ListTagsForResource",
            r#"{"ResourceType":"Parameter","ResourceId":"/app/db/host"}"#,
        ));
        let delete = router.handle_bytes(&request_bytes(
            "AmazonSSM.DeleteParameters",
            r#"{"Names":["/app/db/host","/missing"]}"#,
        ));

        let (put_status, put_headers, put_body) =
            split_response(&put.to_http_bytes());
        let (_, _, overwrite_body) =
            split_response(&overwrite.to_http_bytes());
        let (_, _, label_body) = split_response(&label.to_http_bytes());
        let (_, _, path_body) = split_response(&path.to_http_bytes());
        let (_, _, history_body) = split_response(&history.to_http_bytes());
        let (_, _, tag_body) = split_response(&tag.to_http_bytes());
        let (_, _, list_tags_body) =
            split_response(&list_tags.to_http_bytes());
        let (_, _, delete_body) = split_response(&delete.to_http_bytes());

        assert_eq!(put_status, "HTTP/1.1 200 OK");
        assert!(put_headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("content-type")
                && value == "application/x-amz-json-1.1"
        }));
        assert!(put_body.contains("\"Version\":1"));
        assert!(overwrite_body.contains("\"Version\":2"));
        assert!(label_body.contains("\"InvalidLabels\":[]"));
        assert!(path_body.contains("\"Value\":\"v1\""));
        assert!(history_body.contains("\"Labels\":[\"stable\"]"));
        assert_eq!(tag_body, "{}");
        assert!(list_tags_body.contains("\"Key\":\"env\""));
        assert!(
            delete_body.contains("\"DeletedParameters\":[\"/app/db/host\"]")
        );
        assert!(delete_body.contains("\"InvalidParameters\":[\"/missing\"]"));
    }
}
