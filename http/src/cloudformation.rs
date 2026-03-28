pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{
    QueryParameters, missing_action_error, missing_parameter_error,
};
use crate::xml::XmlBuilder;
use aws::{AwsError, AwsErrorFamily, RequestContext};
use services::{
    CloudFormationChangeSetDescription, CloudFormationChangeSetSummary,
    CloudFormationChangeSummary, CloudFormationCreateChangeSetInput,
    CloudFormationCreateChangeSetOutput, CloudFormationScope,
    CloudFormationService, CloudFormationStackDescription,
    CloudFormationStackEvent, CloudFormationStackOperationInput,
    CloudFormationStackOperationOutput, CloudFormationStackOutput,
    CloudFormationStackResourceDescription,
    CloudFormationStackResourceSummary, CloudFormationTemplateParameter,
    ValidateTemplateOutput,
};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const CLOUDFORMATION_XMLNS: &str =
    "http://cloudformation.amazonaws.com/doc/2010-05-15/";
const REQUEST_ID: &str = "0000000000000000";

pub(crate) fn is_cloudformation_action(action: &str) -> bool {
    matches!(
        action,
        "CreateChangeSet"
            | "CreateStack"
            | "DeleteStack"
            | "DescribeChangeSet"
            | "DescribeStackEvents"
            | "DescribeStackResource"
            | "DescribeStackResources"
            | "DescribeStacks"
            | "ExecuteChangeSet"
            | "GetTemplate"
            | "ListChangeSets"
            | "ListStackResources"
            | "ListStacks"
            | "UpdateStack"
            | "ValidateTemplate"
    )
}

pub(crate) fn handle_query(
    cloudformation: &CloudFormationService,
    body: &[u8],
    context: &RequestContext,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(body)?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope = CloudFormationScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match action {
        "ValidateTemplate" => {
            let output = cloudformation
                .validate_template_source(
                    &scope,
                    params.optional("TemplateBody").map(str::to_owned),
                    params.optional("TemplateURL").map(str::to_owned),
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(validate_template_response(&output))
        }
        "CreateStack" => Ok(stack_operation_response(
            "CreateStack",
            &cloudformation
                .create_stack(&scope, parse_stack_operation_input(&params)?)
                .map_err(|error| error.to_aws_error())?,
        )),
        "UpdateStack" => Ok(stack_operation_response(
            "UpdateStack",
            &cloudformation
                .update_stack(&scope, parse_stack_operation_input(&params)?)
                .map_err(|error| error.to_aws_error())?,
        )),
        "DeleteStack" => {
            cloudformation
                .delete_stack(&scope, params.required("StackName")?)
                .map_err(|error| error.to_aws_error())?;
            Ok(response_without_result("DeleteStack"))
        }
        "DescribeStacks" => Ok(describe_stacks_response(
            &cloudformation
                .describe_stacks(&scope, params.optional("StackName"))
                .map_err(|error| error.to_aws_error())?,
        )),
        "ListStacks" => {
            Ok(list_stacks_response(&cloudformation.list_stacks(&scope)))
        }
        "CreateChangeSet" => Ok(create_change_set_response(
            &cloudformation
                .create_change_set(
                    &scope,
                    parse_create_change_set_input(&params)?,
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeChangeSet" => Ok(describe_change_set_response(
            &cloudformation
                .describe_change_set(
                    &scope,
                    params.required("StackName")?,
                    params.required("ChangeSetName")?,
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "ExecuteChangeSet" => {
            cloudformation
                .execute_change_set(
                    &scope,
                    params.required("StackName")?,
                    params.required("ChangeSetName")?,
                )
                .map_err(|error| error.to_aws_error())?;
            Ok(response_without_result("ExecuteChangeSet"))
        }
        "ListChangeSets" => Ok(list_change_sets_response(
            &cloudformation
                .list_change_sets(&scope, params.required("StackName")?)
                .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeStackEvents" => Ok(describe_stack_events_response(
            &cloudformation
                .describe_stack_events(&scope, params.required("StackName")?)
                .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeStackResources" => Ok(describe_stack_resources_response(
            &cloudformation
                .describe_stack_resources(
                    &scope,
                    params.required("StackName")?,
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeStackResource" => Ok(describe_stack_resource_response(
            &cloudformation
                .describe_stack_resource(
                    &scope,
                    params.required("StackName")?,
                    params.required("LogicalResourceId")?,
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "ListStackResources" => Ok(list_stack_resources_response(
            &cloudformation
                .list_stack_resources(&scope, params.required("StackName")?)
                .map_err(|error| error.to_aws_error())?,
        )),
        "GetTemplate" => Ok(get_template_response(
            &cloudformation
                .get_template(&scope, params.required("StackName")?)
                .map_err(|error| error.to_aws_error())?,
        )),
        other => Err(invalid_action_error(other)),
    }
}

fn parse_stack_operation_input(
    params: &QueryParameters,
) -> Result<CloudFormationStackOperationInput, AwsError> {
    Ok(CloudFormationStackOperationInput {
        capabilities: indexed_member_values(params, "Capabilities.member.")?,
        parameters: parse_stack_parameters(params)?,
        stack_name: params.required("StackName")?.to_owned(),
        template_body: params.optional("TemplateBody").map(str::to_owned),
        template_url: params.optional("TemplateURL").map(str::to_owned),
    })
}

fn parse_create_change_set_input(
    params: &QueryParameters,
) -> Result<CloudFormationCreateChangeSetInput, AwsError> {
    Ok(CloudFormationCreateChangeSetInput {
        capabilities: indexed_member_values(params, "Capabilities.member.")?,
        change_set_name: params.required("ChangeSetName")?.to_owned(),
        change_set_type: params.optional("ChangeSetType").map(str::to_owned),
        parameters: parse_stack_parameters(params)?,
        stack_name: params.required("StackName")?.to_owned(),
        template_body: params.optional("TemplateBody").map(str::to_owned),
        template_url: params.optional("TemplateURL").map(str::to_owned),
    })
}

#[derive(Debug, Default)]
struct QueryStackParameter {
    key: Option<String>,
    use_previous_value: bool,
    value: Option<String>,
}

fn parse_stack_parameters(
    params: &QueryParameters,
) -> Result<BTreeMap<String, String>, AwsError> {
    let mut members = BTreeMap::<usize, QueryStackParameter>::new();

    for (name, value) in params.iter() {
        let Some(rest) = name.strip_prefix("Parameters.member.") else {
            continue;
        };
        let Some((index, field)) = rest.split_once('.') else {
            continue;
        };
        let entry = members.entry(parse_member_index(index)?).or_default();
        match field {
            "ParameterKey" => entry.key = Some(value.to_owned()),
            "ParameterValue" => entry.value = Some(value.to_owned()),
            "UsePreviousValue" => entry.use_previous_value = is_truthy(value),
            _ => {}
        }
    }

    let mut resolved = BTreeMap::new();
    for parameter in members.into_values() {
        let key = parameter.key.ok_or_else(|| {
            missing_parameter_error("Parameters.member.N.ParameterKey")
        })?;
        if parameter.use_previous_value && parameter.value.is_none() {
            continue;
        }
        resolved.insert(key, parameter.value.unwrap_or_default());
    }

    Ok(resolved)
}

fn indexed_member_values(
    params: &QueryParameters,
    prefix: &str,
) -> Result<Vec<String>, AwsError> {
    let mut members = BTreeMap::new();

    for (name, value) in params.iter() {
        let Some(index) = name.strip_prefix(prefix) else {
            continue;
        };
        members.insert(parse_member_index(index)?, value.to_owned());
    }

    Ok(members.into_values().collect())
}

fn parse_member_index(index: &str) -> Result<usize, AwsError> {
    index.parse::<usize>().map_err(|_| {
        AwsError::trusted_custom(
            AwsErrorFamily::Validation,
            "ValidationError",
            format!("Query member index {index} is invalid."),
            400,
            true,
        )
    })
}

fn is_truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "True" | "TRUE")
}

fn validate_template_response(output: &ValidateTemplateOutput) -> String {
    response_with_result(
        "ValidateTemplate",
        &XmlBuilder::new()
            .raw(&optional_elem("Description", output.description.as_deref()))
            .raw(&parameters_xml(&output.parameters))
            .raw(&string_members_xml("Capabilities", &output.capabilities))
            .raw(&optional_elem(
                "CapabilitiesReason",
                output.capabilities_reason.as_deref(),
            ))
            .raw(&string_members_xml(
                "DeclaredTransforms",
                &output.declared_transforms,
            ))
            .build(),
    )
}

fn stack_operation_response(
    action: &str,
    output: &CloudFormationStackOperationOutput,
) -> String {
    response_with_result(
        action,
        &XmlBuilder::new().elem("StackId", &output.stack_id).build(),
    )
}

fn create_change_set_response(
    output: &CloudFormationCreateChangeSetOutput,
) -> String {
    response_with_result(
        "CreateChangeSet",
        &XmlBuilder::new()
            .elem("Id", &output.id)
            .elem("StackId", &output.stack_id)
            .build(),
    )
}

fn describe_stacks_response(
    stacks: &[CloudFormationStackDescription],
) -> String {
    let mut xml = XmlBuilder::new().start("Stacks", None);
    for stack in stacks {
        xml = xml.raw(&stack_xml(stack));
    }

    response_with_result("DescribeStacks", &xml.end("Stacks").build())
}

fn list_stacks_response(stacks: &[CloudFormationStackDescription]) -> String {
    let mut xml = XmlBuilder::new().start("StackSummaries", None);
    for stack in stacks {
        xml = xml.raw(&stack_summary_xml(stack));
    }

    response_with_result("ListStacks", &xml.end("StackSummaries").build())
}

fn describe_change_set_response(
    change_set: &CloudFormationChangeSetDescription,
) -> String {
    response_with_result(
        "DescribeChangeSet",
        &XmlBuilder::new()
            .elem("ChangeSetId", &change_set.change_set_id)
            .elem("ChangeSetName", &change_set.change_set_name)
            .elem("ChangeSetType", &change_set.change_set_type)
            .elem(
                "CreationTime",
                &format_timestamp(change_set.creation_time_epoch_seconds),
            )
            .raw(&changes_xml(&change_set.changes))
            .elem("ExecutionStatus", &change_set.execution_status)
            .elem("StackId", &change_set.stack_id)
            .elem("StackName", &change_set.stack_name)
            .elem("Status", &change_set.status)
            .raw(&optional_elem(
                "StatusReason",
                change_set.status_reason.as_deref(),
            ))
            .build(),
    )
}

fn list_change_sets_response(
    change_sets: &[CloudFormationChangeSetSummary],
) -> String {
    let mut xml = XmlBuilder::new().start("Summaries", None);
    for change_set in change_sets {
        xml = xml.raw(&change_set_summary_xml(change_set));
    }

    response_with_result("ListChangeSets", &xml.end("Summaries").build())
}

fn describe_stack_events_response(
    events: &[CloudFormationStackEvent],
) -> String {
    let mut xml = XmlBuilder::new().start("StackEvents", None);
    for event in events {
        xml = xml.raw(&stack_event_xml(event));
    }

    response_with_result(
        "DescribeStackEvents",
        &xml.end("StackEvents").build(),
    )
}

fn describe_stack_resources_response(
    resources: &[CloudFormationStackResourceDescription],
) -> String {
    let mut xml = XmlBuilder::new().start("StackResources", None);
    for resource in resources {
        xml = xml.raw(&stack_resource_xml(resource));
    }

    response_with_result(
        "DescribeStackResources",
        &xml.end("StackResources").build(),
    )
}

fn describe_stack_resource_response(
    resource: &CloudFormationStackResourceDescription,
) -> String {
    response_with_result(
        "DescribeStackResource",
        &XmlBuilder::new()
            .start("StackResourceDetail", None)
            .raw(&stack_resource_fields_xml(resource))
            .end("StackResourceDetail")
            .build(),
    )
}

fn list_stack_resources_response(
    resources: &[CloudFormationStackResourceSummary],
) -> String {
    let mut xml = XmlBuilder::new().start("StackResourceSummaries", None);
    for resource in resources {
        xml = xml.raw(&stack_resource_summary_xml(resource));
    }

    response_with_result(
        "ListStackResources",
        &xml.end("StackResourceSummaries").build(),
    )
}

fn get_template_response(template_body: &str) -> String {
    response_with_result(
        "GetTemplate",
        &XmlBuilder::new().elem("TemplateBody", template_body).build(),
    )
}

fn stack_xml(stack: &CloudFormationStackDescription) -> String {
    XmlBuilder::new()
        .start("member", None)
        .raw(&stack_fields_xml(stack))
        .end("member")
        .build()
}

fn stack_fields_xml(stack: &CloudFormationStackDescription) -> String {
    let mut xml = XmlBuilder::new()
        .raw(&string_members_xml("Capabilities", &stack.capabilities))
        .elem(
            "CreationTime",
            &format_timestamp(stack.creation_time_epoch_seconds),
        )
        .raw(&optional_elem("Description", stack.description.as_deref()))
        .raw(&optional_timestamp_elem(
            "LastUpdatedTime",
            stack.last_updated_time_epoch_seconds,
        ))
        .raw(&stack_outputs_xml(&stack.outputs))
        .elem("StackId", &stack.stack_id)
        .elem("StackName", &stack.stack_name)
        .elem("StackStatus", &stack.stack_status)
        .raw(&optional_elem(
            "StackStatusReason",
            stack.stack_status_reason.as_deref(),
        ));

    if stack.stack_status == "DELETE_COMPLETE" {
        xml = xml.elem(
            "DeletionTime",
            &format_timestamp(
                stack
                    .last_updated_time_epoch_seconds
                    .unwrap_or(stack.creation_time_epoch_seconds),
            ),
        );
    }

    xml.build()
}

fn stack_summary_xml(stack: &CloudFormationStackDescription) -> String {
    let deletion_time = if stack.stack_status == "DELETE_COMPLETE" {
        optional_timestamp_elem(
            "DeletionTime",
            stack.last_updated_time_epoch_seconds,
        )
    } else {
        String::new()
    };

    XmlBuilder::new()
        .start("member", None)
        .elem(
            "CreationTime",
            &format_timestamp(stack.creation_time_epoch_seconds),
        )
        .raw(&optional_elem(
            "TemplateDescription",
            stack.description.as_deref(),
        ))
        .raw(&deletion_time)
        .elem("StackId", &stack.stack_id)
        .elem("StackName", &stack.stack_name)
        .elem("StackStatus", &stack.stack_status)
        .end("member")
        .build()
}

fn stack_outputs_xml(outputs: &[CloudFormationStackOutput]) -> String {
    if outputs.is_empty() {
        return String::new();
    }

    let mut xml = XmlBuilder::new().start("Outputs", None);
    for output in outputs {
        xml = xml.raw(&output_xml(output));
    }

    xml.end("Outputs").build()
}

fn output_xml(output: &CloudFormationStackOutput) -> String {
    XmlBuilder::new()
        .start("member", None)
        .raw(&optional_elem("Description", output.description.as_deref()))
        .elem("OutputKey", &output.output_key)
        .elem("OutputValue", &output.output_value)
        .end("member")
        .build()
}

fn changes_xml(changes: &[CloudFormationChangeSummary]) -> String {
    if changes.is_empty() {
        return String::new();
    }

    let mut xml = XmlBuilder::new().start("Changes", None);
    for change in changes {
        xml = xml.raw(&change_xml(change));
    }

    xml.end("Changes").build()
}

fn change_xml(change: &CloudFormationChangeSummary) -> String {
    XmlBuilder::new()
        .start("member", None)
        .elem("Type", "Resource")
        .start("ResourceChange", None)
        .elem("Action", &change.action)
        .elem("LogicalResourceId", &change.logical_resource_id)
        .raw(&optional_elem("Replacement", change.replacement.as_deref()))
        .elem("ResourceType", &change.resource_type)
        .end("ResourceChange")
        .end("member")
        .build()
}

fn change_set_summary_xml(
    change_set: &CloudFormationChangeSetSummary,
) -> String {
    XmlBuilder::new()
        .start("member", None)
        .elem("ChangeSetId", &change_set.change_set_id)
        .elem("ChangeSetName", &change_set.change_set_name)
        .elem(
            "CreationTime",
            &format_timestamp(change_set.creation_time_epoch_seconds),
        )
        .elem("ExecutionStatus", &change_set.execution_status)
        .elem("Status", &change_set.status)
        .end("member")
        .build()
}

fn stack_event_xml(event: &CloudFormationStackEvent) -> String {
    let mut xml = XmlBuilder::new()
        .start("member", None)
        .elem("EventId", &event.event_id)
        .elem("LogicalResourceId", &event.logical_resource_id)
        .raw(&optional_elem(
            "PhysicalResourceId",
            event.physical_resource_id.as_deref(),
        ))
        .elem("ResourceStatus", &event.resource_status)
        .raw(&optional_elem(
            "ResourceStatusReason",
            event.resource_status_reason.as_deref(),
        ))
        .elem("ResourceType", &event.resource_type)
        .elem("StackId", &event.stack_id)
        .elem("StackName", &event.stack_name)
        .elem("Timestamp", &format_timestamp(event.timestamp_epoch_seconds));

    xml = xml.end("member");
    xml.build()
}

fn stack_resource_xml(
    resource: &CloudFormationStackResourceDescription,
) -> String {
    XmlBuilder::new()
        .start("member", None)
        .raw(&stack_resource_fields_xml(resource))
        .end("member")
        .build()
}

fn stack_resource_fields_xml(
    resource: &CloudFormationStackResourceDescription,
) -> String {
    XmlBuilder::new()
        .elem(
            "LastUpdatedTimestamp",
            &format_timestamp(resource.last_updated_timestamp_epoch_seconds),
        )
        .elem("LogicalResourceId", &resource.logical_resource_id)
        .elem("PhysicalResourceId", &resource.physical_resource_id)
        .elem("ResourceStatus", &resource.resource_status)
        .raw(&optional_elem(
            "ResourceStatusReason",
            resource.resource_status_reason.as_deref(),
        ))
        .elem("ResourceType", &resource.resource_type)
        .elem("StackId", &resource.stack_id)
        .elem("StackName", &resource.stack_name)
        .build()
}

fn stack_resource_summary_xml(
    resource: &CloudFormationStackResourceSummary,
) -> String {
    XmlBuilder::new()
        .start("member", None)
        .elem(
            "LastUpdatedTimestamp",
            &format_timestamp(resource.last_updated_timestamp_epoch_seconds),
        )
        .elem("LogicalResourceId", &resource.logical_resource_id)
        .elem("PhysicalResourceId", &resource.physical_resource_id)
        .elem("ResourceStatus", &resource.resource_status)
        .raw(&optional_elem(
            "ResourceStatusReason",
            resource.resource_status_reason.as_deref(),
        ))
        .elem("ResourceType", &resource.resource_type)
        .end("member")
        .build()
}

fn response_with_result(action: &str, result_xml: &str) -> String {
    let response_name = format!("{action}Response");
    let result_name = format!("{action}Result");

    XmlBuilder::new()
        .start(&response_name, Some(CLOUDFORMATION_XMLNS))
        .start(&result_name, None)
        .raw(result_xml)
        .end(&result_name)
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_without_result(action: &str) -> String {
    let response_name = format!("{action}Response");
    XmlBuilder::new()
        .start(&response_name, Some(CLOUDFORMATION_XMLNS))
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn parameters_xml(parameters: &[CloudFormationTemplateParameter]) -> String {
    if parameters.is_empty() {
        return String::new();
    }

    let mut xml = XmlBuilder::new().start("Parameters", None);
    for parameter in parameters {
        let mut member = XmlBuilder::new()
            .start("member", None)
            .elem("ParameterKey", &parameter.parameter_key)
            .elem("NoEcho", if parameter.no_echo { "true" } else { "false" });
        if let Some(description) = parameter.description.as_deref() {
            member = member.elem("Description", description);
        }
        if let Some(default_value) = parameter.default_value.as_deref() {
            member = member.elem("DefaultValue", default_value);
        }
        xml = xml.raw(&member.end("member").build());
    }

    xml.end("Parameters").build()
}

fn string_members_xml(name: &str, values: &[String]) -> String {
    if values.is_empty() {
        return String::new();
    }

    let mut xml = XmlBuilder::new().start(name, None);
    for value in values {
        xml = xml.raw(&XmlBuilder::new().elem("member", value).build());
    }

    xml.end(name).build()
}

fn optional_elem(name: &str, value: Option<&str>) -> String {
    value
        .map(|value| XmlBuilder::new().elem(name, value).build())
        .unwrap_or_default()
}

fn optional_timestamp_elem(name: &str, value: Option<u64>) -> String {
    value
        .map(|value| {
            XmlBuilder::new().elem(name, &format_timestamp(value)).build()
        })
        .unwrap_or_default()
}

fn response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn format_timestamp(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

fn invalid_action_error(action: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        format!("Unknown action {action}."),
        400,
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::{handle_query, is_cloudformation_action};
    use aws::{ProtocolFamily, RequestContext, ServiceName};
    use services::{
        CloudFormationDependencies, CloudFormationScope,
        CloudFormationService, SqsQueueIdentity, SqsService,
    };
    use std::sync::Arc;

    fn context(action: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::CloudFormation,
            ProtocolFamily::Query,
            action,
            None,
            false,
        )
        .expect("context should build")
    }

    fn provider_service() -> (CloudFormationService, SqsService) {
        let sqs = SqsService::new();
        (
            CloudFormationService::with_dependencies(
                Arc::new(std::time::SystemTime::now),
                CloudFormationDependencies {
                    sqs: Some(Arc::new(sqs.clone())),
                    ..CloudFormationDependencies::default()
                },
            ),
            sqs,
        )
    }

    #[test]
    fn cloudformation_query_validate_template_serializes_the_expected_xml() {
        let body = r#"Action=ValidateTemplate&TemplateBody=Description%3A+demo%0AParameters%3A%0A++Env%3A%0A++++Type%3A+String%0A++++Default%3A+dev%0AResources%3A%0A++Queue%3A%0A++++Type%3A+AWS%3A%3ASQS%3A%3AQueue%0A++++Properties%3A%0A++++++QueueName%3A+demo"#;
        let xml = handle_query(
            &CloudFormationService::new(),
            body.as_bytes(),
            &context("ValidateTemplate"),
        )
        .expect("ValidateTemplate should succeed");

        assert!(xml.contains("<ValidateTemplateResponse"));
        assert!(xml.contains("<Description>demo</Description>"));
        assert!(xml.contains("<ParameterKey>Env</ParameterKey>"));
        assert!(xml.contains("<DefaultValue>dev</DefaultValue>"));
    }

    #[test]
    fn cloudformation_query_validate_template_reports_parser_errors_as_validation_errors()
     {
        let body = r#"Action=ValidateTemplate&TemplateBody=Transform%3A+AWS%3A%3AServerless-2016-10-31%0AResources%3A+%7B%7D"#;
        let error = handle_query(
            &CloudFormationService::new(),
            body.as_bytes(),
            &context("ValidateTemplate"),
        )
        .expect_err("unsupported transform should fail");

        assert_eq!(error.code(), "ValidationError");
        assert!(error.message().contains("unsupported transform"));
    }

    #[test]
    fn cloudformation_providers_query_create_stack_executes_supported_providers()
     {
        let body = concat!(
            "Action=CreateStack",
            "&StackName=demo-stack",
            "&TemplateBody=Resources%3A%0A++Queue%3A%0A++++Type%3A+AWS%3A%3ASQS%3A%3AQueue%0A++++Properties%3A%0A++++++QueueName%3A+demo-queue%0AOutputs%3A%0A++QueueUrl%3A%0A++++Value%3A+%21Ref+Queue"
        );
        let (service, sqs) = provider_service();
        let xml =
            handle_query(&service, body.as_bytes(), &context("CreateStack"))
                .expect("CreateStack should succeed");

        assert!(xml.contains("<CreateStackResponse"));
        assert!(xml.contains("arn:aws:cloudformation:"));

        let describe = handle_query(
            &service,
            b"Action=DescribeStacks&StackName=demo-stack",
            &context("DescribeStacks"),
        )
        .expect("DescribeStacks should succeed");
        assert!(
            describe.contains("<StackStatus>CREATE_COMPLETE</StackStatus>")
        );
        assert!(describe.contains("<OutputKey>QueueUrl</OutputKey>"));

        let queue = service
            .list_stack_resources(
                &CloudFormationScope::new(
                    "000000000000".parse().unwrap(),
                    "eu-west-2".parse().unwrap(),
                ),
                "demo-stack",
            )
            .expect("stack resources should list");
        let queue_identity = SqsQueueIdentity::new(
            "000000000000".parse().unwrap(),
            "eu-west-2".parse().unwrap(),
            queue[0].physical_resource_id.rsplit('/').next().unwrap(),
        )
        .expect("queue identity should parse");
        let attributes = sqs
            .get_queue_attributes(&queue_identity, &[])
            .expect("queue should exist downstream");
        assert_eq!(
            attributes.get("QueueArn"),
            Some(&"arn:aws:sqs:eu-west-2:000000000000:demo-queue".to_owned())
        );
    }

    #[test]
    fn cloudformation_providers_query_create_and_execute_change_set() {
        let (service, sqs) = provider_service();
        let create_stack = concat!(
            "Action=CreateStack",
            "&StackName=change-stack",
            "&TemplateBody=Resources%3A%0A++Queue%3A%0A++++Type%3A+AWS%3A%3ASQS%3A%3AQueue%0A++++Properties%3A%0A++++++QueueName%3A+change-queue"
        );
        handle_query(
            &service,
            create_stack.as_bytes(),
            &context("CreateStack"),
        )
        .expect("initial stack should succeed");

        let change_set_body = concat!(
            "Action=CreateChangeSet",
            "&ChangeSetName=rename",
            "&ChangeSetType=UPDATE",
            "&StackName=change-stack",
            "&TemplateBody=Resources%3A%0A++Queue%3A%0A++++Type%3A+AWS%3A%3ASQS%3A%3AQueue%0A++++Properties%3A%0A++++++QueueName%3A+change-queue-updated"
        );
        let create_change_set = handle_query(
            &service,
            change_set_body.as_bytes(),
            &context("CreateChangeSet"),
        )
        .expect("CreateChangeSet should succeed");
        assert!(create_change_set.contains("<CreateChangeSetResponse"));

        let describe = handle_query(
            &service,
            b"Action=DescribeChangeSet&StackName=change-stack&ChangeSetName=rename",
            &context("DescribeChangeSet"),
        )
        .expect("DescribeChangeSet should succeed");
        assert!(describe.contains("<Action>Modify</Action>"));

        let execute = handle_query(
            &service,
            b"Action=ExecuteChangeSet&StackName=change-stack&ChangeSetName=rename",
            &context("ExecuteChangeSet"),
        )
        .expect("ExecuteChangeSet should succeed");
        assert!(execute.contains("<ExecuteChangeSetResponse"));

        let resources = service
            .list_stack_resources(
                &CloudFormationScope::new(
                    "000000000000".parse().unwrap(),
                    "eu-west-2".parse().unwrap(),
                ),
                "change-stack",
            )
            .expect("stack resources should list");
        let queue_name = resources[0]
            .physical_resource_id
            .rsplit('/')
            .next()
            .expect("queue URL should contain a queue name");
        let queue_attributes = sqs
            .get_queue_attributes(
                &SqsQueueIdentity::new(
                    "000000000000".parse().unwrap(),
                    "eu-west-2".parse().unwrap(),
                    queue_name,
                )
                .expect("queue identity should build"),
                &[],
            )
            .expect("updated queue should exist");
        assert_eq!(
            queue_attributes.get("QueueArn"),
            Some(
                &"arn:aws:sqs:eu-west-2:000000000000:change-queue-updated"
                    .to_owned()
            )
        );
    }

    #[test]
    fn cloudformation_query_invalid_actions_still_fail_validation() {
        let error = handle_query(
            &CloudFormationService::new(),
            b"Action=NotCloudFormation",
            &context("NotCloudFormation"),
        )
        .expect_err("unknown actions should fail");

        assert_eq!(error.code(), "InvalidAction");
    }

    #[test]
    fn cloudformation_query_action_detection_covers_validate_template() {
        assert!(is_cloudformation_action("ValidateTemplate"));
        assert!(is_cloudformation_action("CreateChangeSet"));
        assert!(!is_cloudformation_action("GetCallerIdentity"));
    }
}
