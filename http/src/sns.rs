pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{QueryParameters, missing_action_error};
use crate::request::HttpRequest;
use crate::xml::XmlBuilder;
use aws::{Arn, AwsError, AwsErrorFamily, RequestContext, ServiceName};
use serde_json::{Map, Value, json};
use services::{
    CreateTopicInput, ListedSubscription, MessageAttributeValue,
    PublishBatchEntryInput, PublishBatchOutput, PublishInput, SnsError,
    SnsScope, SnsService, SubscribeInput,
};
use std::collections::{BTreeMap, BTreeSet};

const REQUEST_ID: &str = "0000000000000000";
const SNS_XMLNS: &str = "https://sns.amazonaws.com/doc/2010-03-31/";
pub(crate) const SNS_QUERY_VERSION: &str = "2010-03-31";

pub(crate) fn is_sns_action(action: &str) -> bool {
    matches!(
        action,
        "ConfirmSubscription"
            | "CreateTopic"
            | "DeleteTopic"
            | "GetSubscriptionAttributes"
            | "GetTopicAttributes"
            | "ListSubscriptions"
            | "ListSubscriptionsByTopic"
            | "ListTagsForResource"
            | "ListTopics"
            | "Publish"
            | "PublishBatch"
            | "SetSubscriptionAttributes"
            | "SetTopicAttributes"
            | "Subscribe"
            | "TagResource"
            | "Unsubscribe"
            | "UntagResource"
    )
}

pub(crate) fn handle_query(
    sns: &SnsService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(request.body())?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope =
        SnsScope::new(context.account_id().clone(), context.region().clone());

    match action {
        "CreateTopic" => {
            let topic_arn = sns
                .create_topic(
                    &scope,
                    CreateTopicInput {
                        attributes: query_map_field(
                            &params,
                            &["Attributes.entry."],
                            "key",
                            "value",
                        ),
                        name: params.required("Name")?.to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem("TopicArn", &topic_arn.to_string())
                    .build(),
            ))
        }
        "DeleteTopic" => {
            sns.delete_topic(&required_sns_arn_query(&params, "TopicArn")?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "ListTopics" => {
            let topics = sns.list_topics(&scope);
            let mut xml = XmlBuilder::new().start("Topics", None);
            for topic_arn in topics {
                xml = xml
                    .start("member", None)
                    .elem("TopicArn", &topic_arn.to_string())
                    .end("member");
            }

            Ok(response_with_result(action, &xml.end("Topics").build()))
        }
        "GetTopicAttributes" => {
            let attributes = sns
                .get_topic_attributes(&required_sns_arn_query(
                    &params, "TopicArn",
                )?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_attributes_result(&attributes),
            ))
        }
        "SetTopicAttributes" => {
            sns.set_topic_attributes(
                &required_sns_arn_query(&params, "TopicArn")?,
                params.required("AttributeName")?,
                params.optional("AttributeValue"),
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "Subscribe" => {
            let subscribed = sns
                .subscribe(SubscribeInput {
                    attributes: query_map_field(
                        &params,
                        &["Attributes.entry."],
                        "key",
                        "value",
                    ),
                    endpoint: params.required("Endpoint")?.to_owned(),
                    protocol: params.required("Protocol")?.to_owned(),
                    return_subscription_arn: params
                        .optional("ReturnSubscriptionArn")
                        .is_some_and(parse_query_bool),
                    topic_arn: required_sns_arn_query(&params, "TopicArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem(
                        "SubscriptionArn",
                        &subscribed.response_subscription_arn,
                    )
                    .build(),
            ))
        }
        "Unsubscribe" => {
            sns.unsubscribe(&required_sns_arn_query(
                &params,
                "SubscriptionArn",
            )?)
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "ConfirmSubscription" => {
            let subscription_arn = sns
                .confirm_subscription(
                    &required_sns_arn_query(&params, "TopicArn")?,
                    params.required("Token")?,
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem("SubscriptionArn", &subscription_arn.to_string())
                    .build(),
            ))
        }
        "ListSubscriptions" => Ok(response_with_result(
            action,
            &query_subscriptions_result(&sns.list_subscriptions(&scope)),
        )),
        "ListSubscriptionsByTopic" => {
            let subscriptions = sns
                .list_subscriptions_by_topic(&required_sns_arn_query(
                    &params, "TopicArn",
                )?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_subscriptions_result(&subscriptions),
            ))
        }
        "GetSubscriptionAttributes" => {
            let attributes = sns
                .get_subscription_attributes(&required_sns_arn_query(
                    &params,
                    "SubscriptionArn",
                )?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_attributes_result(&attributes),
            ))
        }
        "SetSubscriptionAttributes" => {
            sns.set_subscription_attributes(
                &required_sns_arn_query(&params, "SubscriptionArn")?,
                params.required("AttributeName")?,
                params.required("AttributeValue")?,
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "Publish" => {
            let published = sns
                .publish(PublishInput {
                    message: params.required("Message")?.to_owned(),
                    message_attributes: query_message_attributes(
                        &params,
                        "MessageAttributes.entry.",
                    )?,
                    message_deduplication_id: params
                        .optional("MessageDeduplicationId")
                        .map(str::to_owned),
                    message_group_id: params
                        .optional("MessageGroupId")
                        .map(str::to_owned),
                    subject: params.optional("Subject").map(str::to_owned),
                    target_arn: optional_sns_arn_query(&params, "TargetArn")?,
                    topic_arn: optional_sns_arn_query(&params, "TopicArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem("MessageId", &published.message_id)
                    .build(),
            ))
        }
        "PublishBatch" => {
            let published = sns
                .publish_batch(
                    &required_sns_arn_query(&params, "TopicArn")?,
                    query_publish_batch_entries(&params)?,
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_publish_batch_result(&published),
            ))
        }
        "TagResource" => {
            let tags = query_map_field(
                &params,
                &["Tags.member.", "Tags.Tag."],
                "Key",
                "Value",
            );
            if tags.is_empty() {
                return Err(sns_missing_parameter("Tags"));
            }

            sns.tag_resource(
                &required_sns_arn_query(&params, "ResourceArn")?,
                tags,
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "UntagResource" => {
            sns.untag_resource(
                &required_sns_arn_query(&params, "ResourceArn")?,
                &query_string_list(&params, &["TagKeys.member."]),
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "ListTagsForResource" => {
            let tags = sns
                .list_tags_for_resource(&required_sns_arn_query(
                    &params,
                    "ResourceArn",
                )?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(action, &query_tags_result(&tags)))
        }
        _ => Err(unsupported_operation_error(action)),
    }
}

pub(crate) fn handle_json(
    sns: &SnsService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let target = request
        .header("x-amz-target")
        .ok_or_else(|| unsupported_operation_error("missing X-Amz-Target"))?;
    let action = target
        .strip_prefix("SNS_20100331.")
        .ok_or_else(|| unsupported_operation_error(target))?;
    let body = parse_json_body(request.body())?;
    let scope =
        SnsScope::new(context.account_id().clone(), context.region().clone());

    let response = match action {
        "CreateTopic" => {
            let topic_arn = sns
                .create_topic(
                    &scope,
                    CreateTopicInput {
                        attributes: json_string_map_field(
                            &body,
                            "Attributes",
                        )?,
                        name: required_string_json(&body, "Name")?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json!({ "TopicArn": topic_arn })
        }
        "DeleteTopic" => {
            sns.delete_topic(&required_sns_arn_json(&body, "TopicArn")?)
                .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "ListTopics" => {
            let topics = sns
                .list_topics(&scope)
                .into_iter()
                .map(|topic_arn| json!({ "TopicArn": topic_arn }))
                .collect::<Vec<_>>();

            json!({ "Topics": topics })
        }
        "GetTopicAttributes" => {
            let attributes = sns
                .get_topic_attributes(&required_sns_arn_json(
                    &body, "TopicArn",
                )?)
                .map_err(|error| error.to_aws_error())?;

            json!({ "Attributes": attributes })
        }
        "SetTopicAttributes" => {
            sns.set_topic_attributes(
                &required_sns_arn_json(&body, "TopicArn")?,
                &required_string_json(&body, "AttributeName")?,
                optional_string_json(&body, "AttributeValue"),
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "Subscribe" => {
            let subscribed = sns
                .subscribe(SubscribeInput {
                    attributes: json_string_map_field(&body, "Attributes")?,
                    endpoint: required_string_json(&body, "Endpoint")?,
                    protocol: required_string_json(&body, "Protocol")?,
                    return_subscription_arn: optional_bool_json(
                        &body,
                        "ReturnSubscriptionArn",
                    )?
                    .unwrap_or(false),
                    topic_arn: required_sns_arn_json(&body, "TopicArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            json!({ "SubscriptionArn": subscribed.response_subscription_arn })
        }
        "Unsubscribe" => {
            sns.unsubscribe(&required_sns_arn_json(&body, "SubscriptionArn")?)
                .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "ListSubscriptions" => {
            json!({ "Subscriptions": json_subscriptions(&sns.list_subscriptions(&scope)) })
        }
        "ListSubscriptionsByTopic" => {
            let subscriptions = sns
                .list_subscriptions_by_topic(&required_sns_arn_json(
                    &body, "TopicArn",
                )?)
                .map_err(|error| error.to_aws_error())?;

            json!({ "Subscriptions": json_subscriptions(&subscriptions) })
        }
        "Publish" => {
            let published = sns
                .publish(PublishInput {
                    message: required_string_json(&body, "Message")?,
                    message_attributes: json_message_attributes_field(
                        &body,
                        "MessageAttributes",
                    )?,
                    message_deduplication_id: optional_string_json(
                        &body,
                        "MessageDeduplicationId",
                    )
                    .map(str::to_owned),
                    message_group_id: optional_string_json(
                        &body,
                        "MessageGroupId",
                    )
                    .map(str::to_owned),
                    subject: optional_string_json(&body, "Subject")
                        .map(str::to_owned),
                    target_arn: optional_sns_arn_json(&body, "TargetArn")?,
                    topic_arn: optional_sns_arn_json(&body, "TopicArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            json!({ "MessageId": published.message_id })
        }
        "TagResource" => {
            let tags = json_tag_list_field(&body, "Tags")?;
            if tags.is_empty() {
                return Err(sns_missing_parameter("Tags"));
            }
            sns.tag_resource(
                &required_sns_arn_json(&body, "ResourceArn")?,
                tags,
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "UntagResource" => {
            sns.untag_resource(
                &required_sns_arn_json(&body, "ResourceArn")?,
                &json_string_list_field(&body, "TagKeys")?,
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "ListTagsForResource" => {
            let tags = sns
                .list_tags_for_resource(&required_sns_arn_json(
                    &body,
                    "ResourceArn",
                )?)
                .map_err(|error| error.to_aws_error())?;
            let tags = tags
                .into_iter()
                .map(|(key, value)| json!({ "Key": key, "Value": value }))
                .collect::<Vec<_>>();

            json!({ "Tags": tags })
        }
        "ConfirmSubscription"
        | "GetSubscriptionAttributes"
        | "PublishBatch"
        | "SetSubscriptionAttributes" => {
            return Err(unsupported_operation_error(action));
        }
        _ => return Err(unsupported_operation_error(action)),
    };

    serde_json::to_vec(&response).map_err(|error| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            format!("Failed to serialize SNS response: {error}"),
            500,
            false,
        )
    })
}

fn parse_json_body(body: &[u8]) -> Result<Value, AwsError> {
    if body.is_empty() {
        return Ok(Value::Object(Map::new()));
    }

    serde_json::from_slice(body).map_err(|_| invalid_json_parameter("body"))
}

fn required_string_json(
    body: &Value,
    field: &str,
) -> Result<String, AwsError> {
    optional_string_json(body, field)
        .map(str::to_owned)
        .ok_or_else(|| sns_missing_parameter(field))
}

fn optional_string_json<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(Value::as_str)
}

fn required_sns_arn_query(
    params: &QueryParameters,
    field: &str,
) -> Result<Arn, AwsError> {
    parse_sns_arn(params.required(field)?, field)
}

fn optional_sns_arn_query(
    params: &QueryParameters,
    field: &str,
) -> Result<Option<Arn>, AwsError> {
    params.optional(field).map(|value| parse_sns_arn(value, field)).transpose()
}

fn required_sns_arn_json(body: &Value, field: &str) -> Result<Arn, AwsError> {
    parse_sns_arn(&required_string_json(body, field)?, field)
}

fn optional_sns_arn_json(
    body: &Value,
    field: &str,
) -> Result<Option<Arn>, AwsError> {
    optional_string_json(body, field)
        .map(|value| parse_sns_arn(value, field))
        .transpose()
}

fn parse_sns_arn(value: &str, field: &str) -> Result<Arn, AwsError> {
    let arn = value.parse::<Arn>().map_err(|_| {
        SnsError::InvalidParameter {
            message: format!("Invalid parameter: {field}"),
        }
        .to_aws_error()
    })?;
    if arn.service() != ServiceName::Sns {
        return Err(SnsError::InvalidParameter {
            message: format!("Invalid parameter: {field}"),
        }
        .to_aws_error());
    }

    Ok(arn)
}

fn optional_bool_json(
    body: &Value,
    field: &str,
) -> Result<Option<bool>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    if let Some(boolean) = value.as_bool() {
        return Ok(Some(boolean));
    }
    if let Some(text) = value.as_str() {
        return match text {
            "true" | "True" => Ok(Some(true)),
            "false" | "False" => Ok(Some(false)),
            _ => Err(invalid_json_parameter(field)),
        };
    }

    Err(invalid_json_parameter(field))
}

fn json_string_map_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(values) = value else {
        return Err(invalid_json_parameter(field));
    };
    let mut collected = BTreeMap::new();
    for (key, value) in values {
        let Some(string_value) = value_to_string(value) else {
            return Err(invalid_json_parameter(field));
        };
        collected.insert(key.clone(), string_value);
    }

    Ok(collected)
}

fn json_message_attributes_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, MessageAttributeValue>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(values) = value else {
        return Err(invalid_json_parameter(field));
    };
    let mut collected = BTreeMap::new();
    for (name, value) in values {
        let Value::Object(attribute) = value else {
            return Err(invalid_json_parameter(field));
        };
        let Some(data_type) =
            attribute.get("DataType").and_then(Value::as_str)
        else {
            return Err(invalid_json_parameter(field));
        };
        let Some(string_value) =
            attribute.get("StringValue").and_then(Value::as_str)
        else {
            return Err(invalid_json_parameter(field));
        };
        collected.insert(
            name.clone(),
            MessageAttributeValue {
                data_type: data_type.to_owned(),
                value: string_value.to_owned(),
            },
        );
    }

    Ok(collected)
}

fn json_tag_list_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let Value::Array(tags) = value else {
        return Err(invalid_json_parameter(field));
    };
    let mut collected = BTreeMap::new();
    for tag in tags {
        let Value::Object(tag) = tag else {
            return Err(invalid_json_parameter(field));
        };
        let Some(key) = tag.get("Key").and_then(Value::as_str) else {
            return Err(invalid_json_parameter(field));
        };
        let Some(value) = tag.get("Value").and_then(Value::as_str) else {
            return Err(invalid_json_parameter(field));
        };
        collected.insert(key.to_owned(), value.to_owned());
    }

    Ok(collected)
}

fn json_string_list_field(
    body: &Value,
    field: &str,
) -> Result<Vec<String>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(Vec::new());
    };
    let Value::Array(items) = value else {
        return Err(invalid_json_parameter(field));
    };
    let mut collected = Vec::with_capacity(items.len());
    for item in items {
        let Some(item) = item.as_str() else {
            return Err(invalid_json_parameter(field));
        };
        collected.push(item.to_owned());
    }

    Ok(collected)
}

type QueryMessageAttributeParts =
    (Option<String>, Option<String>, Option<String>);

fn query_message_attributes(
    params: &QueryParameters,
    prefix: &str,
) -> Result<BTreeMap<String, MessageAttributeValue>, AwsError> {
    let mut indexed: BTreeMap<String, QueryMessageAttributeParts> =
        BTreeMap::new();
    for (name, value) in params.iter() {
        let Some(remainder) = name.strip_prefix(prefix) else {
            continue;
        };
        let Some((index, field)) = remainder.split_once('.') else {
            continue;
        };
        let entry = indexed.entry(index.to_owned()).or_default();
        match field {
            "Name" => entry.0 = Some(value.to_owned()),
            "Value.DataType" => entry.1 = Some(value.to_owned()),
            "Value.StringValue" => entry.2 = Some(value.to_owned()),
            _ => {}
        }
    }

    indexed
        .into_values()
        .map(|(name, data_type, value)| {
            let name = name.ok_or_else(|| invalid_query_parameter(prefix))?;
            let data_type =
                data_type.ok_or_else(|| invalid_query_parameter(prefix))?;
            let value =
                value.ok_or_else(|| invalid_query_parameter(prefix))?;
            Ok((name, MessageAttributeValue { data_type, value }))
        })
        .collect()
}

fn query_publish_batch_entries(
    params: &QueryParameters,
) -> Result<Vec<PublishBatchEntryInput>, AwsError> {
    let prefix = "PublishBatchRequestEntries.member.";
    let mut indexes = BTreeSet::new();

    for (name, _) in params.iter() {
        let Some(remainder) = name.strip_prefix(prefix) else {
            continue;
        };
        let Some((index, _)) = remainder.split_once('.') else {
            continue;
        };
        indexes.insert(index.to_owned());
    }

    indexes
        .into_iter()
        .map(|index| {
            let field = |name: &str| format!("{prefix}{index}.{name}");
            Ok(PublishBatchEntryInput {
                id: params.required(&field("Id"))?.to_owned(),
                message: params
                    .optional(&field("Message"))
                    .unwrap_or_default()
                    .to_owned(),
                message_attributes: query_message_attributes(
                    params,
                    &field("MessageAttributes.entry."),
                )?,
                message_deduplication_id: params
                    .optional(&field("MessageDeduplicationId"))
                    .map(str::to_owned),
                message_group_id: params
                    .optional(&field("MessageGroupId"))
                    .map(str::to_owned),
                subject: params.optional(&field("Subject")).map(str::to_owned),
            })
        })
        .collect()
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn query_map_field(
    params: &QueryParameters,
    prefixes: &[&str],
    key_label: &str,
    value_label: &str,
) -> BTreeMap<String, String> {
    let mut collected: BTreeMap<String, (Option<String>, Option<String>)> =
        BTreeMap::new();

    for (name, value) in params.iter() {
        let Some((index, field)) = prefixes.iter().find_map(|prefix| {
            let remainder = name.strip_prefix(prefix)?;
            let (index, field) = remainder.split_once('.')?;
            Some((index, field))
        }) else {
            continue;
        };

        let entry = collected.entry(index.to_owned()).or_default();
        if field == key_label {
            entry.0 = Some(value.to_owned());
        }
        if field == value_label {
            entry.1 = Some(value.to_owned());
        }
    }

    collected
        .into_values()
        .filter_map(|(key, value)| Some((key?, value?)))
        .collect()
}

fn query_string_list(
    params: &QueryParameters,
    prefixes: &[&str],
) -> Vec<String> {
    let mut collected = BTreeMap::new();

    for (name, value) in params.iter() {
        let Some(index) =
            prefixes.iter().find_map(|prefix| name.strip_prefix(prefix))
        else {
            continue;
        };
        collected.insert(index.to_owned(), value.to_owned());
    }

    collected.into_values().collect()
}

fn query_attributes_result(attributes: &BTreeMap<String, String>) -> String {
    let mut xml = XmlBuilder::new().start("Attributes", None);
    for (key, value) in attributes {
        xml = xml
            .start("entry", None)
            .elem("key", key)
            .elem("value", value)
            .end("entry");
    }

    xml.end("Attributes").build()
}

fn query_publish_batch_result(output: &PublishBatchOutput) -> String {
    let mut xml = XmlBuilder::new().start("Successful", None);
    for success in &output.successful {
        xml = xml
            .start("member", None)
            .elem("Id", &success.id)
            .elem("MessageId", &success.message_id)
            .end("member");
    }
    xml = xml.end("Successful").start("Failed", None);
    for failure in &output.failed {
        xml = xml
            .start("member", None)
            .elem("Id", &failure.id)
            .elem("Code", &failure.code)
            .elem("Message", &failure.message)
            .elem("SenderFault", &failure.sender_fault.to_string())
            .end("member");
    }

    xml.end("Failed").build()
}

fn query_subscriptions_result(subscriptions: &[ListedSubscription]) -> String {
    let mut xml = XmlBuilder::new().start("Subscriptions", None);
    for subscription in subscriptions {
        let topic_arn = subscription.topic_arn.to_string();
        let subscription_arn = subscription
            .state
            .listed_subscription_arn(&subscription.subscription_arn);
        xml = xml
            .start("member", None)
            .elem("TopicArn", &topic_arn)
            .elem("Protocol", &subscription.protocol)
            .elem("SubscriptionArn", &subscription_arn)
            .elem("Owner", subscription.owner.as_str())
            .elem("Endpoint", &subscription.endpoint)
            .end("member");
    }

    xml.end("Subscriptions").build()
}

fn query_tags_result(tags: &BTreeMap<String, String>) -> String {
    let mut xml = XmlBuilder::new().start("Tags", None);
    for (key, value) in tags {
        xml = xml
            .start("member", None)
            .elem("Key", key)
            .elem("Value", value)
            .end("member");
    }

    xml.end("Tags").build()
}

fn json_subscriptions(subscriptions: &[ListedSubscription]) -> Vec<Value> {
    subscriptions
        .iter()
        .map(|subscription| {
            json!({
                "Endpoint": subscription.endpoint,
                "Owner": subscription.owner.as_str(),
                "Protocol": subscription.protocol,
                "SubscriptionArn": subscription.state.listed_subscription_arn(&subscription.subscription_arn),
                "TopicArn": subscription.topic_arn,
            })
        })
        .collect()
}

fn response_with_result(action: &str, result: &str) -> String {
    let response_name = format!("{action}Response");
    let result_name = format!("{action}Result");

    XmlBuilder::new()
        .start(&response_name, Some(SNS_XMLNS))
        .start(&result_name, None)
        .raw(result)
        .end(&result_name)
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_without_result(action: &str) -> String {
    let response_name = format!("{action}Response");

    XmlBuilder::new()
        .start(&response_name, Some(SNS_XMLNS))
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn unsupported_operation_error(action: &str) -> AwsError {
    SnsError::UnsupportedOperation {
        message: format!("Operation {action} is not supported."),
    }
    .to_aws_error()
}

fn sns_missing_parameter(name: &str) -> AwsError {
    SnsError::MissingParameter {
        message: format!("The request must contain the parameter {name}."),
    }
    .to_aws_error()
}

fn invalid_json_parameter(field: &str) -> AwsError {
    SnsError::InvalidParameter {
        message: format!("Value for parameter {field} is invalid."),
    }
    .to_aws_error()
}

fn invalid_query_parameter(field: &str) -> AwsError {
    SnsError::InvalidParameter {
        message: format!("Value for parameter {field} is invalid."),
    }
    .to_aws_error()
}

fn parse_query_bool(value: &str) -> bool {
    matches!(value, "true" | "True" | "1")
}

#[cfg(test)]
mod tests {
    use super::{
        handle_json, handle_query, is_sns_action, query_map_field,
        query_string_list, response_without_result,
    };
    use crate::runtime::EdgeRouter;
    use crate::test_runtime;
    use aws::{ProtocolFamily, ServiceName};
    use services::SnsService;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct RecordingForwarder {
        bodies: Mutex<Vec<String>>,
    }

    impl services::HttpForwarder for RecordingForwarder {
        fn forward(
            &self,
            request: &services::HttpForwardRequest,
        ) -> Result<services::HttpForwardResponse, services::InfrastructureError>
        {
            self.bodies
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(
                    std::str::from_utf8(request.body())
                        .expect("body should be utf-8")
                        .to_owned(),
                );

            Ok(services::HttpForwardResponse::new(
                200,
                "OK",
                Vec::new(),
                Vec::new(),
            ))
        }
    }

    fn router(
        forwarder: Option<Arc<dyn services::HttpForwarder + Send + Sync>>,
    ) -> EdgeRouter {
        test_runtime::router_with_http_forwarder("http-sns-tests", forwarder)
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

    fn query_request(path: &str, body: &str) -> Vec<u8> {
        let body = query_body(body);
        format!(
            "POST {path} HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn query_body(body: &str) -> String {
        if body.contains("Version=") {
            body.to_owned()
        } else if body.is_empty() {
            "Version=2010-03-31".to_owned()
        } else {
            format!("{body}&Version=2010-03-31")
        }
    }

    fn json_request(target: &str, body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn sns_core_query_topic_lifecycle_and_tags_round_trip() {
        let router = router(None);

        let create = router.handle_bytes(&query_request(
            "/",
            "Action=CreateTopic&Name=orders",
        ));
        let set_attributes = router.handle_bytes(&query_request(
            "/",
            "Action=SetTopicAttributes&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&AttributeName=DisplayName&AttributeValue=Orders",
        ));
        let get_attributes = router.handle_bytes(&query_request(
            "/",
            "Action=GetTopicAttributes&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders",
        ));
        let tag = router.handle_bytes(&query_request(
            "/",
            "Action=TagResource&ResourceArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&Tags.member.1.Key=env&Tags.member.1.Value=dev",
        ));
        let list_tags = router.handle_bytes(&query_request(
            "/",
            "Action=ListTagsForResource&ResourceArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders",
        ));
        let publish = router.handle_bytes(&query_request(
            "/",
            "Action=Publish&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&Message=payload",
        ));

        let (create_status, _, create_body) =
            split_response(&create.to_http_bytes());
        let (set_status, _, set_body) =
            split_response(&set_attributes.to_http_bytes());
        let (_, _, get_body) = split_response(&get_attributes.to_http_bytes());
        let (tag_status, _, tag_body) = split_response(&tag.to_http_bytes());
        let (_, _, list_tags_body) =
            split_response(&list_tags.to_http_bytes());
        let (_, _, publish_body) = split_response(&publish.to_http_bytes());

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert!(create_body.contains(
            "<TopicArn>arn:aws:sns:eu-west-2:000000000000:orders</TopicArn>"
        ));
        assert_eq!(set_status, "HTTP/1.1 200 OK");
        assert!(set_body.contains("<SetTopicAttributesResponse"));
        assert!(
            get_body.contains("<key>DisplayName</key><value>Orders</value>")
        );
        assert_eq!(tag_status, "HTTP/1.1 200 OK");
        assert!(tag_body.contains("<TagResourceResponse"));
        assert!(list_tags_body.contains("<Key>env</Key><Value>dev</Value>"));
        assert!(publish_body.contains(
            "<MessageId>00000000-0000-0000-0000-000000000001</MessageId>"
        ));
    }

    #[test]
    fn sns_core_query_confirmation_flow_clears_pending_list_state() {
        let forwarder = Arc::new(RecordingForwarder::default());
        let router = router(Some(forwarder.clone()));

        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateTopic&Name=orders",
        ));
        let subscribe = router.handle_bytes(&query_request(
            "/",
            "Action=Subscribe&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&Protocol=http&Endpoint=http%3A%2F%2F127.0.0.1%3A9010%2Fsubscription&ReturnSubscriptionArn=true",
        ));
        let subscribe_body =
            split_response(&subscribe.to_http_bytes()).2.clone();
        let token = forwarder
            .bodies
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())[0]
            .split("\"Token\":\"")
            .nth(1)
            .and_then(|value| value.split('"').next())
            .expect("token should be present")
            .to_owned();
        let pending = router.handle_bytes(&query_request(
            "/",
            "Action=ListSubscriptionsByTopic&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders",
        ));
        let confirm = router.handle_bytes(&query_request(
            "/",
            &format!(
                "Action=ConfirmSubscription&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&Token={token}"
            ),
        ));
        let listed = router.handle_bytes(&query_request(
            "/",
            "Action=ListSubscriptionsByTopic&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders",
        ));

        let (_, _, pending_body) = split_response(&pending.to_http_bytes());
        let (_, _, confirm_body) = split_response(&confirm.to_http_bytes());
        let (_, _, listed_body) = split_response(&listed.to_http_bytes());

        assert!(
            subscribe_body.contains("00000000-0000-0000-0000-000000000001")
        );
        assert!(pending_body.contains("PendingConfirmation"));
        assert!(confirm_body.contains("00000000-0000-0000-0000-000000000001"));
        assert!(!listed_body.contains("PendingConfirmation"));
    }

    #[test]
    fn sns_core_query_invalid_confirmation_token_returns_explicit_error() {
        let router = router(None);
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateTopic&Name=orders",
        ));
        let confirm = router.handle_bytes(&query_request(
            "/",
            "Action=ConfirmSubscription&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&Token=bad-token",
        ));
        let (status, _, body) = split_response(&confirm.to_http_bytes());

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert!(body.contains("<Code>InvalidParameter</Code>"));
        assert!(body.contains("Token"));
    }

    #[test]
    fn sns_delivery_query_subscription_attributes_round_trip() {
        let router = router(None);
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateTopic&Name=orders",
        ));
        let subscribe = router.handle_bytes(&query_request(
            "/",
            "Action=Subscribe&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&Protocol=sqs&Endpoint=arn%3Aaws%3Asqs%3Aeu-west-2%3A000000000000%3Aorders-queue&ReturnSubscriptionArn=true&Attributes.entry.1.key=RawMessageDelivery&Attributes.entry.1.value=true&Attributes.entry.2.key=FilterPolicy&Attributes.entry.2.value=%7B%22store%22%3A%5B%22eu-west%22%5D%7D",
        ));
        let (_, _, subscribe_body) =
            split_response(&subscribe.to_http_bytes());
        let subscription_arn = "arn:aws:sns:eu-west-2:000000000000:orders:00000000-0000-0000-0000-000000000001";
        assert!(subscribe_body.contains(subscription_arn));

        let get_initial = router.handle_bytes(&query_request(
            "/",
            &format!(
                "Action=GetSubscriptionAttributes&SubscriptionArn={}",
                "arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders%3A00000000-0000-0000-0000-000000000001"
            ),
        ));
        let set_scope = router.handle_bytes(&query_request(
            "/",
            "Action=SetSubscriptionAttributes&SubscriptionArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders%3A00000000-0000-0000-0000-000000000001&AttributeName=FilterPolicyScope&AttributeValue=MessageBody",
        ));
        let set_policy = router.handle_bytes(&query_request(
            "/",
            "Action=SetSubscriptionAttributes&SubscriptionArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders%3A00000000-0000-0000-0000-000000000001&AttributeName=FilterPolicy&AttributeValue=%7B%22detail%22%3A%7B%22kind%22%3A%5B%7B%22prefix%22%3A%22order-%22%7D%5D%7D%7D",
        ));
        let get_updated = router.handle_bytes(&query_request(
            "/",
            "Action=GetSubscriptionAttributes&SubscriptionArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders%3A00000000-0000-0000-0000-000000000001",
        ));

        let (_, _, initial_body) =
            split_response(&get_initial.to_http_bytes());
        let (scope_status, _, scope_body) =
            split_response(&set_scope.to_http_bytes());
        let (policy_status, _, policy_body) =
            split_response(&set_policy.to_http_bytes());
        let (_, _, updated_body) =
            split_response(&get_updated.to_http_bytes());

        assert!(
            initial_body
                .contains("<key>RawMessageDelivery</key><value>true</value>")
        );
        assert!(initial_body.contains(
            "<key>FilterPolicyScope</key><value>MessageAttributes</value>"
        ));
        assert_eq!(scope_status, "HTTP/1.1 200 OK");
        assert!(scope_body.contains("<SetSubscriptionAttributesResponse"));
        assert_eq!(policy_status, "HTTP/1.1 200 OK");
        assert!(policy_body.contains("<SetSubscriptionAttributesResponse"));
        assert!(updated_body.contains(
            "<key>FilterPolicyScope</key><value>MessageBody</value>"
        ));
        assert!(updated_body.contains(
            "<key>FilterPolicy</key><value>{&quot;detail&quot;:{&quot;kind&quot;:[{&quot;prefix&quot;:&quot;order-&quot;}]}}</value>"
        ));
    }

    #[test]
    fn sns_delivery_query_publish_batch_reports_partial_failure() {
        let router = router(None);
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateTopic&Name=orders",
        ));
        let publish_batch = router.handle_bytes(&query_request(
            "/",
            "Action=PublishBatch&TopicArn=arn%3Aaws%3Asns%3Aeu-west-2%3A000000000000%3Aorders&PublishBatchRequestEntries.member.1.Id=ok&PublishBatchRequestEntries.member.1.Message=payload&PublishBatchRequestEntries.member.2.Id=bad",
        ));
        let (_, _, body) = split_response(&publish_batch.to_http_bytes());

        assert!(body.contains("<Id>ok</Id>"));
        assert!(body.contains(
            "<MessageId>00000000-0000-0000-0000-000000000001</MessageId>"
        ));
        assert!(body.contains("<Id>bad</Id>"));
        assert!(body.contains("<Code>InvalidParameter</Code>"));
        assert!(
            body.contains("<Message>Invalid parameter: Message</Message>")
        );
    }

    #[test]
    fn sns_core_json_topic_lifecycle_and_unsupported_confirm_surface() {
        let router = router(None);

        let create = router.handle_bytes(&json_request(
            "SNS_20100331.CreateTopic",
            r#"{"Name":"orders"}"#,
        ));
        let publish = router.handle_bytes(&json_request(
            "SNS_20100331.Publish",
            r#"{"TopicArn":"arn:aws:sns:eu-west-2:000000000000:orders","Message":"payload"}"#,
        ));
        let tag = router.handle_bytes(&json_request(
            "SNS_20100331.TagResource",
            r#"{"ResourceArn":"arn:aws:sns:eu-west-2:000000000000:orders","Tags":[{"Key":"env","Value":"dev"}]}"#,
        ));
        let list_tags = router.handle_bytes(&json_request(
            "SNS_20100331.ListTagsForResource",
            r#"{"ResourceArn":"arn:aws:sns:eu-west-2:000000000000:orders"}"#,
        ));
        let unsupported = router.handle_bytes(&json_request(
            "SNS_20100331.ConfirmSubscription",
            r#"{"TopicArn":"arn:aws:sns:eu-west-2:000000000000:orders","Token":"bad"}"#,
        ));

        let (_, _, create_body) = split_response(&create.to_http_bytes());
        let (_, _, publish_body) = split_response(&publish.to_http_bytes());
        let (_, _, tag_body) = split_response(&tag.to_http_bytes());
        let (_, _, list_tags_body) =
            split_response(&list_tags.to_http_bytes());
        let (unsupported_status, _, unsupported_body) =
            split_response(&unsupported.to_http_bytes());

        assert!(create_body.contains(
            "\"TopicArn\":\"arn:aws:sns:eu-west-2:000000000000:orders\""
        ));
        assert!(publish_body.contains(
            "\"MessageId\":\"00000000-0000-0000-0000-000000000001\""
        ));
        assert_eq!(tag_body, "{}");
        assert!(list_tags_body.contains("\"Key\":\"env\""));
        assert_eq!(unsupported_status, "HTTP/1.1 400 Bad Request");
        assert!(
            unsupported_body.contains("\"__type\":\"UnsupportedOperation\"")
        );
    }

    #[test]
    fn sns_core_internal_helpers_parse_query_structures() {
        let params = crate::query::QueryParameters::parse(
            b"Tags.member.1.Key=env&Tags.member.1.Value=dev&TagKeys.member.1=env&Attributes.entry.1.key=DisplayName&Attributes.entry.1.value=Orders",
        )
        .expect("params should parse");

        assert!(is_sns_action("CreateTopic"));
        assert_eq!(
            query_map_field(&params, &["Tags.member."], "Key", "Value"),
            BTreeMap::from([("env".to_owned(), "dev".to_owned())])
        );
        assert_eq!(
            query_string_list(&params, &["TagKeys.member."]),
            vec!["env".to_owned()]
        );
        assert!(
            response_without_result("DeleteTopic")
                .contains("ResponseMetadata")
        );
        assert_eq!(ServiceName::Sns.as_str(), "sns");
        assert!(matches!(
            ProtocolFamily::AwsJson10,
            ProtocolFamily::AwsJson10
        ));
    }

    #[test]
    fn sns_core_direct_handlers_report_explicit_parameter_errors() {
        let sns = SnsService::new();
        let context = aws::RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::Sns,
            ProtocolFamily::Query,
            "CreateTopic",
            None,
            false,
        )
        .expect("context should build");
        let missing_action_request = crate::request::HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 8\r\n\r\nName=test",
        )
        .expect("request should parse");
        let missing_action =
            handle_query(&sns, &missing_action_request, &context)
                .expect_err("missing action should fail");
        let invalid_json_request = crate::request::HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: SNS_20100331.CreateTopic\r\nContent-Length: 1\r\n\r\n{",
        )
        .expect("request should parse");
        let invalid_json = handle_json(&sns, &invalid_json_request, &context)
            .expect_err("invalid json should fail");

        assert_eq!(missing_action, crate::query::missing_action_error());
        assert_eq!(invalid_json.code(), "InvalidParameter");
        assert_eq!(
            crate::query::malformed_query_error().code(),
            "MalformedQueryString"
        );
    }
}
