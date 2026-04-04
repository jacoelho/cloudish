pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{
    QueryParameters, malformed_query_error, missing_action_error,
};

use crate::request::HttpRequest;
use crate::xml::XmlBuilder;
use aws::{
    AccountId, AdvertisedEdge, Arn, AwsError, AwsErrorFamily, RequestContext,
    ServiceName,
};
use edge_runtime::SqsRequestRuntime;
use serde_json::{Map, Value, json};
use services::{
    CancelMessageMoveTaskInput, ChangeMessageVisibilityBatchEntryInput,
    CreateQueueInput, DeleteMessageBatchEntryInput, ListMessageMoveTasksInput,
    ListQueuesInput, ReceiveMessageInput, SendMessageBatchEntryInput,
    SendMessageInput, SqsAddPermissionInput, SqsError, SqsQueueIdentity,
    SqsRemovePermissionInput, SqsScope, SqsService, StartMessageMoveTaskInput,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

mod json_protocol;
mod message_attributes;
mod query_protocol;

const REQUEST_ID: &str = "0000000000000000";
const SQS_XMLNS: &str = "http://queue.amazonaws.com/doc/2012-11-05/";
pub(crate) const SQS_QUERY_VERSION: &str = "2012-11-05";

pub(crate) fn is_sqs_action(action: &str) -> bool {
    matches!(
        action,
        "AddPermission"
            | "CancelMessageMoveTask"
            | "ChangeMessageVisibility"
            | "ChangeMessageVisibilityBatch"
            | "CreateQueue"
            | "DeleteMessage"
            | "DeleteMessageBatch"
            | "DeleteQueue"
            | "GetQueueAttributes"
            | "GetQueueUrl"
            | "ListDeadLetterSourceQueues"
            | "ListMessageMoveTasks"
            | "ListQueueTags"
            | "ListQueues"
            | "PurgeQueue"
            | "ReceiveMessage"
            | "RemovePermission"
            | "SendMessage"
            | "SendMessageBatch"
            | "SetQueueAttributes"
            | "StartMessageMoveTask"
            | "TagQueue"
            | "UntagQueue"
    )
}

pub(crate) fn handle_query(
    sqs: &SqsService,
    sqs_requests: &SqsRequestRuntime,
    advertised_edge: &AdvertisedEdge,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    shutdown_signal: &AtomicBool,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(request.body())?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope =
        SqsScope::new(context.account_id().clone(), context.region().clone());

    match action {
        "CreateQueue" => {
            let queue = sqs
                .create_queue(
                    &scope,
                    CreateQueueInput {
                        attributes: query_map(&params, "Attribute"),
                        queue_name: params.required("QueueName")?.to_owned(),
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem("QueueUrl", &queue_url(advertised_edge, &queue))
                    .build(),
            ))
        }
        "DeleteQueue" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.delete_queue(&queue).map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "GetQueueUrl" => {
            let get_queue_scope = scope_with_queue_owner(
                &scope,
                params.optional("QueueOwnerAWSAccountId"),
            )?;
            let queue = sqs
                .get_queue_url(&get_queue_scope, params.required("QueueName")?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem("QueueUrl", &queue_url(advertised_edge, &queue))
                    .build(),
            ))
        }
        "GetQueueAttributes" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let attributes = sqs
                .get_queue_attributes(
                    &queue,
                    &query_list(&params, "AttributeName"),
                )
                .map_err(|error| error.to_aws_error())?;

            let mut xml = XmlBuilder::new();
            for (name, value) in attributes {
                xml = xml
                    .start("Attribute", None)
                    .elem("Name", &name)
                    .elem("Value", &value)
                    .end("Attribute");
            }

            Ok(response_with_result(action, &xml.build()))
        }
        "SetQueueAttributes" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.set_queue_attributes(&queue, query_map(&params, "Attribute"))
                .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "AddPermission" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.add_permission(
                &queue,
                SqsAddPermissionInput {
                    actions: query_list(&params, "ActionName"),
                    aws_account_ids: query_list(&params, "AWSAccountId"),
                    label: params.required("Label")?.to_owned(),
                },
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "RemovePermission" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.remove_permission(
                &queue,
                SqsRemovePermissionInput {
                    label: params.required("Label")?.to_owned(),
                },
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "ListQueues" => {
            let queues = sqs
                .list_queues_page(
                    &scope,
                    &ListQueuesInput {
                        max_results: optional_u32_query(
                            &params,
                            "MaxResults",
                        )?,
                        next_token: params
                            .optional("NextToken")
                            .map(str::to_owned),
                        queue_name_prefix: params
                            .optional("QueueNamePrefix")
                            .map(str::to_owned),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            let mut xml = XmlBuilder::new();
            for queue in queues.queue_urls {
                xml =
                    xml.elem("QueueUrl", &queue_url(advertised_edge, &queue));
            }
            if let Some(next_token) = queues.next_token.as_deref() {
                xml = xml.elem("NextToken", next_token);
            }

            Ok(response_with_result(action, &xml.build()))
        }
        "SendMessage" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let sent = sqs
                .send_message(
                    &queue,
                    SendMessageInput {
                        body: params.required("MessageBody")?.to_owned(),
                        delay_seconds: optional_u32_query(
                            &params,
                            "DelaySeconds",
                        )?,
                        message_attributes:
                            message_attributes::query_message_attributes(
                                &params,
                                "MessageAttribute.",
                            )?,
                        message_deduplication_id: params
                            .optional("MessageDeduplicationId")
                            .map(str::to_owned),
                        message_group_id: params
                            .optional("MessageGroupId")
                            .map(str::to_owned),
                        message_system_attributes:
                            message_attributes::query_message_attributes(
                                &params,
                                "MessageSystemAttribute.",
                            )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(action, &{
                let mut xml = XmlBuilder::new()
                    .elem("MessageId", &sent.message_id)
                    .elem("MD5OfMessageBody", &sent.md5_of_message_body);
                if let Some(md5) = sent.md5_of_message_attributes.as_deref() {
                    xml = xml.elem("MD5OfMessageAttributes", md5);
                }
                if let Some(md5) =
                    sent.md5_of_message_system_attributes.as_deref()
                {
                    xml = xml.elem("MD5OfMessageSystemAttributes", md5);
                }
                if let Some(sequence_number) = sent.sequence_number.as_deref()
                {
                    xml = xml.elem("SequenceNumber", sequence_number);
                }
                xml.build()
            }))
        }
        "SendMessageBatch" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let batch = sqs
                .send_message_batch(
                    &queue,
                    query_send_message_batch_entries(&params)?,
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_protocol::query_send_message_batch_result_xml(&batch),
            ))
        }
        "ReceiveMessage" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let messages = sqs_requests
                .receive_message(
                    &queue,
                    ReceiveMessageInput {
                        attribute_names: query_list(&params, "AttributeName"),
                        max_number_of_messages: optional_u32_query(
                            &params,
                            "MaxNumberOfMessages",
                        )?,
                        message_attribute_names: query_list(
                            &params,
                            "MessageAttributeName",
                        ),
                        message_system_attribute_names: query_list(
                            &params,
                            "MessageSystemAttributeName",
                        ),
                        receive_request_attempt_id: params
                            .optional("ReceiveRequestAttemptId")
                            .map(str::to_owned),
                        visibility_timeout: optional_u32_query(
                            &params,
                            "VisibilityTimeout",
                        )?,
                        wait_time_seconds: optional_u32_query(
                            &params,
                            "WaitTimeSeconds",
                        )?,
                    },
                    &|| shutdown_signal.load(Ordering::SeqCst),
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_protocol::query_received_messages_xml(&messages),
            ))
        }
        "DeleteMessage" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.delete_message(&queue, params.required("ReceiptHandle")?)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "DeleteMessageBatch" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let batch = sqs
                .delete_message_batch(
                    &queue,
                    query_delete_message_batch_entries(&params)?,
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_protocol::query_delete_message_batch_result_xml(&batch),
            ))
        }
        "ChangeMessageVisibility" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let visibility_timeout =
                required_u32_query(&params, "VisibilityTimeout")?;
            sqs.change_message_visibility(
                &queue,
                params.required("ReceiptHandle")?,
                visibility_timeout,
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "ChangeMessageVisibilityBatch" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let batch = sqs
                .change_message_visibility_batch(
                    &queue,
                    query_change_visibility_batch_entries(&params)?,
                )
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_protocol::query_change_visibility_batch_result_xml(
                    &batch,
                ),
            ))
        }
        "ListDeadLetterSourceQueues" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let sources = sqs
                .list_dead_letter_source_queues_page(
                    &queue,
                    &services::ListDeadLetterSourceQueuesInput {
                        max_results: optional_u32_query(
                            &params,
                            "MaxResults",
                        )?,
                        next_token: params
                            .optional("NextToken")
                            .map(str::to_owned),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            let mut xml = XmlBuilder::new();
            for source in sources.queue_urls {
                xml =
                    xml.elem("QueueUrl", &queue_url(advertised_edge, &source));
            }
            if let Some(next_token) = sources.next_token.as_deref() {
                xml = xml.elem("NextToken", next_token);
            }

            Ok(response_with_result(action, &xml.build()))
        }
        "StartMessageMoveTask" => {
            let task = sqs
                .start_message_move_task(StartMessageMoveTaskInput {
                    destination_arn: optional_sqs_arn_query(
                        &params,
                        "DestinationArn",
                    )?,
                    max_number_of_messages_per_second: optional_u32_query(
                        &params,
                        "MaxNumberOfMessagesPerSecond",
                    )?,
                    source_arn: required_sqs_arn_query(&params, "SourceArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem("TaskHandle", &task.task_handle)
                    .build(),
            ))
        }
        "ListMessageMoveTasks" => {
            let tasks = sqs
                .list_message_move_tasks(ListMessageMoveTasksInput {
                    max_results: optional_i32_query(&params, "MaxResults")?,
                    source_arn: required_sqs_arn_query(&params, "SourceArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &query_protocol::query_list_message_move_tasks_xml(&tasks),
            ))
        }
        "CancelMessageMoveTask" => {
            let cancelled = sqs
                .cancel_message_move_task(CancelMessageMoveTaskInput {
                    task_handle: params.required("TaskHandle")?.to_owned(),
                })
                .map_err(|error| error.to_aws_error())?;

            Ok(response_with_result(
                action,
                &XmlBuilder::new()
                    .elem(
                        "ApproximateNumberOfMessagesMoved",
                        &cancelled
                            .approximate_number_of_messages_moved
                            .to_string(),
                    )
                    .build(),
            ))
        }
        "PurgeQueue" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.purge_queue(&queue).map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "TagQueue" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let tags = query_map(&params, "Tag");
            if tags.is_empty() {
                return Err(sqs_missing_parameter("Tags"));
            }
            sqs.tag_queue(&queue, tags)
                .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "UntagQueue" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            sqs.untag_queue(&queue, query_list(&params, "TagKey"))
                .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "ListQueueTags" => {
            let queue = queue_identity_from_request(
                params.optional("QueueUrl"),
                request,
                context,
            )?;
            let tags = sqs
                .list_queue_tags(&queue)
                .map_err(|error| error.to_aws_error())?;

            let mut xml = XmlBuilder::new();
            for (key, value) in tags {
                xml = xml
                    .start("Tag", None)
                    .elem("Key", &key)
                    .elem("Value", &value)
                    .end("Tag");
            }

            Ok(response_with_result(action, &xml.build()))
        }
        _ => Err(unsupported_operation_error(action)),
    }
}

pub(crate) fn handle_json(
    sqs: &SqsService,
    sqs_requests: &SqsRequestRuntime,
    advertised_edge: &AdvertisedEdge,
    request: &HttpRequest<'_>,
    context: &RequestContext,
    shutdown_signal: &AtomicBool,
) -> Result<Vec<u8>, AwsError> {
    let target = request
        .header("x-amz-target")
        .ok_or_else(|| unsupported_operation_error("missing X-Amz-Target"))?;
    let action = target
        .strip_prefix("AmazonSQS.")
        .ok_or_else(|| unsupported_operation_error(target))?;
    let body = parse_json_body(request.body())?;
    let scope =
        SqsScope::new(context.account_id().clone(), context.region().clone());

    let response = match action {
        "CreateQueue" => {
            let queue = sqs
                .create_queue(
                    &scope,
                    CreateQueueInput {
                        attributes: json_map_field(&body, "Attributes")?,
                        queue_name: required_string_json(&body, "QueueName")?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            json!({ "QueueUrl": queue_url(advertised_edge, &queue) })
        }
        "DeleteQueue" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.delete_queue(&queue).map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "GetQueueUrl" => {
            let get_queue_scope = scope_with_queue_owner(
                &scope,
                optional_string_json(&body, "QueueOwnerAWSAccountId"),
            )?;
            let queue = sqs
                .get_queue_url(
                    &get_queue_scope,
                    &required_string_json(&body, "QueueName")?,
                )
                .map_err(|error| error.to_aws_error())?;

            json!({ "QueueUrl": queue_url(advertised_edge, &queue) })
        }
        "GetQueueAttributes" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let attribute_names =
                json_string_list_field(&body, "AttributeNames")?;
            let attributes = sqs
                .get_queue_attributes(&queue, &attribute_names)
                .map_err(|error| error.to_aws_error())?;

            json!({ "Attributes": attributes })
        }
        "SetQueueAttributes" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.set_queue_attributes(
                &queue,
                json_map_field(&body, "Attributes")?,
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "AddPermission" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.add_permission(
                &queue,
                SqsAddPermissionInput {
                    actions: json_string_list_field(&body, "Actions")?,
                    aws_account_ids: json_string_list_field(
                        &body,
                        "AWSAccountIds",
                    )?,
                    label: required_string_json(&body, "Label")?,
                },
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "RemovePermission" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.remove_permission(
                &queue,
                SqsRemovePermissionInput {
                    label: required_string_json(&body, "Label")?,
                },
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "ListQueues" => {
            let queues = sqs
                .list_queues_page(
                    &scope,
                    &ListQueuesInput {
                        max_results: optional_u32_json(&body, "MaxResults")?,
                        next_token: optional_string_json(&body, "NextToken")
                            .map(str::to_owned),
                        queue_name_prefix: optional_string_json(
                            &body,
                            "QueueNamePrefix",
                        )
                        .map(str::to_owned),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            let urls = queues
                .queue_urls
                .into_iter()
                .map(|queue| Value::String(queue_url(advertised_edge, &queue)))
                .collect::<Vec<_>>();
            let mut response = json!({ "QueueUrls": urls });
            if let Some(next_token) = queues.next_token
                && let Some(object) = response.as_object_mut()
            {
                object
                    .insert("NextToken".to_owned(), Value::String(next_token));
            }

            response
        }
        "SendMessage" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let sent = sqs
                .send_message(
                    &queue,
                    SendMessageInput {
                        body: required_string_json(&body, "MessageBody")?,
                        delay_seconds: optional_u32_json(
                            &body,
                            "DelaySeconds",
                        )?,
                        message_attributes:
                            message_attributes::json_message_attributes_field(
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
                        message_system_attributes:
                            message_attributes::json_message_attributes_field(
                                &body,
                                "MessageSystemAttributes",
                            )?,
                    },
                )
                .map_err(|error| error.to_aws_error())?;

            let mut response = json!({
                "MD5OfMessageBody": sent.md5_of_message_body,
                "MessageId": sent.message_id,
            });
            if let Some(sequence_number) = sent.sequence_number
                && let Some(object) = response.as_object_mut()
            {
                object.insert(
                    "SequenceNumber".to_owned(),
                    Value::String(sequence_number),
                );
            }
            if let Some(md5_of_message_attributes) =
                sent.md5_of_message_attributes
                && let Some(object) = response.as_object_mut()
            {
                object.insert(
                    "MD5OfMessageAttributes".to_owned(),
                    Value::String(md5_of_message_attributes),
                );
            }
            if let Some(md5_of_message_system_attributes) =
                sent.md5_of_message_system_attributes
                && let Some(object) = response.as_object_mut()
            {
                object.insert(
                    "MD5OfMessageSystemAttributes".to_owned(),
                    Value::String(md5_of_message_system_attributes),
                );
            }
            response
        }
        "SendMessageBatch" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let batch = sqs
                .send_message_batch(
                    &queue,
                    json_send_message_batch_entries(&body)?,
                )
                .map_err(|error| error.to_aws_error())?;

            json_protocol::json_send_message_batch_result(&batch)
        }
        "ReceiveMessage" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let messages = sqs_requests
                .receive_message(
                    &queue,
                    ReceiveMessageInput {
                        attribute_names: json_string_list_field(
                            &body,
                            "AttributeNames",
                        )?,
                        max_number_of_messages: optional_u32_json(
                            &body,
                            "MaxNumberOfMessages",
                        )?,
                        message_attribute_names: json_string_list_field(
                            &body,
                            "MessageAttributeNames",
                        )?,
                        message_system_attribute_names:
                            json_string_list_field(
                                &body,
                                "MessageSystemAttributeNames",
                            )?,
                        receive_request_attempt_id: optional_string_json(
                            &body,
                            "ReceiveRequestAttemptId",
                        )
                        .map(str::to_owned),
                        visibility_timeout: optional_u32_json(
                            &body,
                            "VisibilityTimeout",
                        )?,
                        wait_time_seconds: optional_u32_json(
                            &body,
                            "WaitTimeSeconds",
                        )?,
                    },
                    &|| shutdown_signal.load(Ordering::SeqCst),
                )
                .map_err(|error| error.to_aws_error())?;

            json!({ "Messages": json_protocol::json_received_messages(&messages) })
        }
        "DeleteMessage" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.delete_message(
                &queue,
                &required_string_json(&body, "ReceiptHandle")?,
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "DeleteMessageBatch" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let batch = sqs
                .delete_message_batch(
                    &queue,
                    json_delete_message_batch_entries(&body)?,
                )
                .map_err(|error| error.to_aws_error())?;

            json_protocol::json_delete_message_batch_result(&batch)
        }
        "ChangeMessageVisibility" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.change_message_visibility(
                &queue,
                &required_string_json(&body, "ReceiptHandle")?,
                required_u32_json(&body, "VisibilityTimeout")?,
            )
            .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "ChangeMessageVisibilityBatch" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let batch = sqs
                .change_message_visibility_batch(
                    &queue,
                    json_change_visibility_batch_entries(&body)?,
                )
                .map_err(|error| error.to_aws_error())?;

            json_protocol::json_change_visibility_batch_result(&batch)
        }
        "ListDeadLetterSourceQueues" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let sources = sqs
                .list_dead_letter_source_queues_page(
                    &queue,
                    &services::ListDeadLetterSourceQueuesInput {
                        max_results: optional_u32_json(&body, "MaxResults")?,
                        next_token: optional_string_json(&body, "NextToken")
                            .map(str::to_owned),
                    },
                )
                .map_err(|error| error.to_aws_error())?;
            let queue_urls = sources
                .queue_urls
                .into_iter()
                .map(|queue| Value::String(queue_url(advertised_edge, &queue)))
                .collect::<Vec<_>>();
            let mut response = json!({ "queueUrls": queue_urls });
            if let Some(next_token) = sources.next_token
                && let Some(object) = response.as_object_mut()
            {
                object
                    .insert("NextToken".to_owned(), Value::String(next_token));
            }

            response
        }
        "StartMessageMoveTask" => {
            let task = sqs
                .start_message_move_task(StartMessageMoveTaskInput {
                    destination_arn: optional_sqs_arn_json(
                        &body,
                        "DestinationArn",
                    )?,
                    max_number_of_messages_per_second: optional_u32_json(
                        &body,
                        "MaxNumberOfMessagesPerSecond",
                    )?,
                    source_arn: required_sqs_arn_json(&body, "SourceArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            json!({ "TaskHandle": task.task_handle })
        }
        "ListMessageMoveTasks" => {
            let tasks = sqs
                .list_message_move_tasks(ListMessageMoveTasksInput {
                    max_results: optional_i32_json(&body, "MaxResults")?,
                    source_arn: required_sqs_arn_json(&body, "SourceArn")?,
                })
                .map_err(|error| error.to_aws_error())?;

            json!({ "Results": json_protocol::json_list_message_move_tasks(&tasks) })
        }
        "CancelMessageMoveTask" => {
            let cancelled = sqs
                .cancel_message_move_task(CancelMessageMoveTaskInput {
                    task_handle: required_string_json(&body, "TaskHandle")?,
                })
                .map_err(|error| error.to_aws_error())?;

            json!({
                "ApproximateNumberOfMessagesMoved":
                    cancelled.approximate_number_of_messages_moved,
            })
        }
        "PurgeQueue" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.purge_queue(&queue).map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "TagQueue" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let tags = json_map_field(&body, "Tags")?;
            if tags.is_empty() {
                return Err(sqs_missing_parameter("Tags"));
            }
            sqs.tag_queue(&queue, tags)
                .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "UntagQueue" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            sqs.untag_queue(&queue, json_string_list_field(&body, "TagKeys")?)
                .map_err(|error| error.to_aws_error())?;

            json!({})
        }
        "ListQueueTags" => {
            let queue = queue_identity_from_request(
                optional_string_json(&body, "QueueUrl"),
                request,
                context,
            )?;
            let tags = sqs
                .list_queue_tags(&queue)
                .map_err(|error| error.to_aws_error())?;

            json!({ "Tags": tags })
        }
        _ => return Err(unsupported_operation_error(action)),
    };

    serde_json::to_vec(&response).map_err(|error| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            format!("Failed to serialize SQS response: {error}"),
            500,
            false,
        )
    })
}

fn queue_identity_from_request(
    queue_url: Option<&str>,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<SqsQueueIdentity, AwsError> {
    let candidate = queue_url
        .filter(|queue_url| !queue_url.trim().is_empty())
        .or_else(|| queue_path(request));
    let Some(candidate) = candidate else {
        return Err(sqs_missing_parameter("QueueUrl"));
    };

    parse_queue_identity(candidate, context)
}

fn scope_with_queue_owner(
    default_scope: &SqsScope,
    queue_owner_account_id: Option<&str>,
) -> Result<SqsScope, AwsError> {
    let Some(queue_owner_account_id) = queue_owner_account_id else {
        return Ok(default_scope.clone());
    };
    let account_id = queue_owner_account_id.parse::<AccountId>().map_err(|_| {
        SqsError::InvalidAddress {
            message: format!(
                "Value {queue_owner_account_id} for parameter QueueOwnerAWSAccountId is invalid."
            ),
        }
        .to_aws_error()
    })?;

    Ok(SqsScope::new(account_id, default_scope.region().clone()))
}

fn parse_queue_identity(
    queue_url_or_path: &str,
    context: &RequestContext,
) -> Result<SqsQueueIdentity, AwsError> {
    let path = queue_url_or_path
        .split_once("://")
        .map(|(_, remainder)| remainder)
        .unwrap_or(queue_url_or_path);
    let path = path
        .split_once('/')
        .map(|(_, remainder)| remainder)
        .unwrap_or(path)
        .split('?')
        .next()
        .unwrap_or_default()
        .trim_matches('/');
    let segments = path.split('/').collect::<Vec<_>>();
    if segments.len() != 2 {
        return Err(SqsError::QueueDoesNotExist.to_aws_error());
    }

    let account_id = segments
        .first()
        .ok_or_else(|| SqsError::QueueDoesNotExist.to_aws_error())?
        .parse::<AccountId>()
        .map_err(|_| SqsError::QueueDoesNotExist.to_aws_error())?;
    SqsQueueIdentity::new(
        account_id,
        context.region().clone(),
        (*segments
            .get(1)
            .ok_or_else(|| SqsError::QueueDoesNotExist.to_aws_error())?)
        .to_owned(),
    )
    .map_err(|_| SqsError::QueueDoesNotExist.to_aws_error())
}

fn queue_path<'a>(request: &'a HttpRequest<'_>) -> Option<&'a str> {
    let path = request.path_without_query().trim();
    if path == "/" || path.is_empty() { None } else { Some(path) }
}

fn queue_url(
    advertised_edge: &AdvertisedEdge,
    queue: &SqsQueueIdentity,
) -> String {
    advertised_edge.sqs_queue_url(queue.account_id(), queue.queue_name())
}

fn query_list(params: &QueryParameters, prefix: &str) -> Vec<String> {
    let mut values = Vec::new();
    for index in 1.. {
        let name = format!("{prefix}.{index}");
        let Some(value) = params.optional(&name) else {
            break;
        };
        values.push(value.to_owned());
    }

    values
}

fn query_map(
    params: &QueryParameters,
    prefix: &str,
) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for index in 1.. {
        let key_name = format!("{prefix}.{index}.Name");
        let Some(key) = params.optional(&key_name) else {
            break;
        };
        let value_name = format!("{prefix}.{index}.Value");
        let Some(value) = params.optional(&value_name) else {
            continue;
        };
        values.insert(key.to_owned(), value.to_owned());
    }

    values
}

fn query_send_message_batch_entries(
    params: &QueryParameters,
) -> Result<Vec<SendMessageBatchEntryInput>, AwsError> {
    let mut entries = Vec::new();
    for index in 1.. {
        let prefix = format!("SendMessageBatchRequestEntry.{index}");
        let Some(id) = params.optional(&format!("{prefix}.Id")) else {
            break;
        };
        let body_name = format!("{prefix}.MessageBody");
        let delay_name = format!("{prefix}.DelaySeconds");
        let dedup_name = format!("{prefix}.MessageDeduplicationId");
        let group_name = format!("{prefix}.MessageGroupId");
        entries.push(SendMessageBatchEntryInput {
            body: params.required(&body_name)?.to_owned(),
            delay_seconds: optional_u32_query(params, &delay_name)?,
            id: id.to_owned(),
            message_attributes: message_attributes::query_message_attributes(
                params,
                &format!("{prefix}.MessageAttribute."),
            )?,
            message_deduplication_id: params
                .optional(&dedup_name)
                .map(str::to_owned),
            message_group_id: params.optional(&group_name).map(str::to_owned),
            message_system_attributes:
                message_attributes::query_message_attributes(
                    params,
                    &format!("{prefix}.MessageSystemAttribute."),
                )?,
        });
    }

    Ok(entries)
}

fn query_delete_message_batch_entries(
    params: &QueryParameters,
) -> Result<Vec<DeleteMessageBatchEntryInput>, AwsError> {
    let mut entries = Vec::new();
    for index in 1.. {
        let prefix = format!("DeleteMessageBatchRequestEntry.{index}");
        let Some(id) = params.optional(&format!("{prefix}.Id")) else {
            break;
        };
        let receipt_name = format!("{prefix}.ReceiptHandle");
        entries.push(DeleteMessageBatchEntryInput {
            id: id.to_owned(),
            receipt_handle: params.required(&receipt_name)?.to_owned(),
        });
    }

    Ok(entries)
}

fn query_change_visibility_batch_entries(
    params: &QueryParameters,
) -> Result<Vec<ChangeMessageVisibilityBatchEntryInput>, AwsError> {
    let mut entries = Vec::new();
    for index in 1.. {
        let prefix =
            format!("ChangeMessageVisibilityBatchRequestEntry.{index}");
        let Some(id) = params.optional(&format!("{prefix}.Id")) else {
            break;
        };
        let receipt_name = format!("{prefix}.ReceiptHandle");
        let visibility_name = format!("{prefix}.VisibilityTimeout");
        entries.push(ChangeMessageVisibilityBatchEntryInput {
            id: id.to_owned(),
            receipt_handle: params.required(&receipt_name)?.to_owned(),
            visibility_timeout: required_u32_query(params, &visibility_name)?,
        });
    }

    Ok(entries)
}

fn optional_u32_query(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<u32>, AwsError> {
    match params.optional(name) {
        Some(value) => value.parse::<u32>().map(Some).map_err(|_| {
            SqsError::InvalidParameterValue {
                message: format!(
                    "Value {value} for parameter {name} is invalid."
                ),
            }
            .to_aws_error()
        }),
        None => Ok(None),
    }
}

fn optional_i32_query(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<i32>, AwsError> {
    match params.optional(name) {
        Some(value) => value.parse::<i32>().map(Some).map_err(|_| {
            SqsError::InvalidParameterValue {
                message: format!(
                    "Value {value} for parameter {name} is invalid."
                ),
            }
            .to_aws_error()
        }),
        None => Ok(None),
    }
}

fn required_u32_query(
    params: &QueryParameters,
    name: &str,
) -> Result<u32, AwsError> {
    params.required(name)?.parse::<u32>().map_err(|_| malformed_query_error())
}

fn parse_json_body(body: &[u8]) -> Result<Value, AwsError> {
    if body.is_empty() {
        return Ok(Value::Object(Map::new()));
    }

    serde_json::from_slice(body).map_err(|_| {
        AwsError::trusted_custom(
            AwsErrorFamily::Validation,
            "InvalidParameterValue",
            "The request body is not valid JSON.",
            400,
            true,
        )
    })
}

fn required_string_json(
    body: &Value,
    field: &str,
) -> Result<String, AwsError> {
    optional_string_json(body, field)
        .map(str::to_owned)
        .ok_or_else(|| sqs_missing_parameter(field))
}

fn optional_string_json<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(Value::as_str)
}

fn required_sqs_arn_query(
    params: &QueryParameters,
    field: &str,
) -> Result<Arn, AwsError> {
    parse_sqs_arn(params.required(field)?, field)
}

fn optional_sqs_arn_query(
    params: &QueryParameters,
    field: &str,
) -> Result<Option<Arn>, AwsError> {
    params.optional(field).map(|value| parse_sqs_arn(value, field)).transpose()
}

fn required_sqs_arn_json(body: &Value, field: &str) -> Result<Arn, AwsError> {
    parse_sqs_arn(&required_string_json(body, field)?, field)
}

fn optional_sqs_arn_json(
    body: &Value,
    field: &str,
) -> Result<Option<Arn>, AwsError> {
    optional_string_json(body, field)
        .map(|value| parse_sqs_arn(value, field))
        .transpose()
}

fn parse_sqs_arn(value: &str, field: &str) -> Result<Arn, AwsError> {
    let arn = value.parse::<Arn>().map_err(|_| {
        SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter {field} is invalid."
            ),
        }
        .to_aws_error()
    })?;
    if arn.service() != ServiceName::Sqs {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter {field} is invalid."
            ),
        }
        .to_aws_error());
    }

    Ok(arn)
}

fn required_u32_json(body: &Value, field: &str) -> Result<u32, AwsError> {
    optional_u32_json(body, field)?.ok_or_else(|| sqs_missing_parameter(field))
}

fn optional_u32_json(
    body: &Value,
    field: &str,
) -> Result<Option<u32>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };

    match value {
        Value::Number(number) => number
            .as_u64()
            .and_then(|value| value.try_into().ok())
            .map(Some)
            .ok_or_else(|| invalid_json_parameter(field)),
        Value::String(value) => value
            .parse::<u32>()
            .map(Some)
            .map_err(|_| invalid_json_parameter(field)),
        _ => Err(invalid_json_parameter(field)),
    }
}

fn optional_i32_json(
    body: &Value,
    field: &str,
) -> Result<Option<i32>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };

    match value {
        Value::Number(number) => number
            .as_i64()
            .and_then(|value| value.try_into().ok())
            .map(Some)
            .ok_or_else(|| invalid_json_parameter(field)),
        Value::String(value) => value
            .parse::<i32>()
            .map(Some)
            .map_err(|_| invalid_json_parameter(field)),
        _ => Err(invalid_json_parameter(field)),
    }
}

fn json_string_list_field(
    body: &Value,
    field: &str,
) -> Result<Vec<String>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(invalid_json_parameter(field));
    };

    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid_json_parameter(field))
        })
        .collect()
}

fn json_map_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let Some(entries) = value.as_object() else {
        return Err(invalid_json_parameter(field));
    };
    let mut map = BTreeMap::new();
    for (key, value) in entries {
        let string_value = match value {
            Value::String(value) => value.clone(),
            Value::Number(value) => value.to_string(),
            Value::Bool(value) => value.to_string(),
            _ => return Err(invalid_json_parameter(field)),
        };
        map.insert(key.clone(), string_value);
    }

    Ok(map)
}

fn json_send_message_batch_entries(
    body: &Value,
) -> Result<Vec<SendMessageBatchEntryInput>, AwsError> {
    let Some(entries) = body.get("Entries") else {
        return Ok(Vec::new());
    };
    let Some(entries) = entries.as_array() else {
        return Err(invalid_json_parameter("Entries"));
    };

    entries
        .iter()
        .map(|entry| {
            Ok(SendMessageBatchEntryInput {
                body: required_string_json(entry, "MessageBody")?,
                delay_seconds: optional_u32_json(entry, "DelaySeconds")?,
                id: required_string_json(entry, "Id")?,
                message_attributes:
                    message_attributes::json_message_attributes_field(
                        entry,
                        "MessageAttributes",
                    )?,
                message_deduplication_id: optional_string_json(
                    entry,
                    "MessageDeduplicationId",
                )
                .map(str::to_owned),
                message_group_id: optional_string_json(
                    entry,
                    "MessageGroupId",
                )
                .map(str::to_owned),
                message_system_attributes:
                    message_attributes::json_message_attributes_field(
                        entry,
                        "MessageSystemAttributes",
                    )?,
            })
        })
        .collect()
}

fn json_delete_message_batch_entries(
    body: &Value,
) -> Result<Vec<DeleteMessageBatchEntryInput>, AwsError> {
    let Some(entries) = body.get("Entries") else {
        return Ok(Vec::new());
    };
    let Some(entries) = entries.as_array() else {
        return Err(invalid_json_parameter("Entries"));
    };

    entries
        .iter()
        .map(|entry| {
            Ok(DeleteMessageBatchEntryInput {
                id: required_string_json(entry, "Id")?,
                receipt_handle: required_string_json(entry, "ReceiptHandle")?,
            })
        })
        .collect()
}

fn json_change_visibility_batch_entries(
    body: &Value,
) -> Result<Vec<ChangeMessageVisibilityBatchEntryInput>, AwsError> {
    let Some(entries) = body.get("Entries") else {
        return Ok(Vec::new());
    };
    let Some(entries) = entries.as_array() else {
        return Err(invalid_json_parameter("Entries"));
    };

    entries
        .iter()
        .map(|entry| {
            Ok(ChangeMessageVisibilityBatchEntryInput {
                id: required_string_json(entry, "Id")?,
                receipt_handle: required_string_json(entry, "ReceiptHandle")?,
                visibility_timeout: required_u32_json(
                    entry,
                    "VisibilityTimeout",
                )?,
            })
        })
        .collect()
}

fn response_with_result(action: &str, result: &str) -> String {
    let response_name = format!("{action}Response");
    let result_name = format!("{action}Result");

    XmlBuilder::new()
        .start(&response_name, Some(SQS_XMLNS))
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
        .start(&response_name, Some(SQS_XMLNS))
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

fn sqs_missing_parameter(name: &str) -> AwsError {
    SqsError::MissingParameter {
        message: format!("The request must contain the parameter {name}."),
    }
    .to_aws_error()
}

fn invalid_json_parameter(name: &str) -> AwsError {
    SqsError::InvalidParameterValue {
        message: format!("Value for parameter {name} is invalid."),
    }
    .to_aws_error()
}

fn unsupported_operation_error(action: &str) -> AwsError {
    SqsError::UnsupportedOperation {
        message: format!("Operation {action} is not supported."),
    }
    .to_aws_error()
}

#[cfg(test)]
mod tests {
    use super::queue_url;
    use crate::request::EdgeRequest;
    use crate::runtime::EdgeRouter;
    use crate::test_runtime;
    use aws::{
        AccountId, AdvertisedEdge, ProtocolFamily, RegionId, RequestContext,
        ServiceName,
    };
    use edge_runtime::SqsRequestRuntime;
    use serde_json::{Map, Value, json};
    use services::{
        CreateQueueInput, SendMessageInput, SqsQueueIdentity, SqsScope,
        SqsService,
    };
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicBool;

    fn router() -> EdgeRouter {
        test_runtime::router_with_http_forwarder("http-sqs-tests", None)
    }

    fn split_response(response: &[u8]) -> (&str, Vec<(&str, &str)>, &str) {
        let header_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("response should contain a header terminator");
        let headers = std::str::from_utf8(&response[..header_end])
            .expect("response headers should be UTF-8");
        let body = std::str::from_utf8(&response[header_end + 4..])
            .expect("response body should be UTF-8");
        let mut lines = headers.split("\r\n");
        let status = lines.next().expect("status line should exist");
        let parsed_headers = lines
            .map(|line| {
                line.split_once(':').expect("header should contain ':'")
            })
            .map(|(name, value)| (name, value.trim()))
            .collect();

        (status, parsed_headers, body)
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
            "Version=2012-11-05".to_owned()
        } else {
            format!("{body}&Version=2012-11-05")
        }
    }

    fn json_request(target: &str, body: &str) -> Vec<u8> {
        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn parse_request(request: &[u8]) -> EdgeRequest {
        EdgeRequest::parse(request).expect("request should parse")
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

    fn context(protocol: ProtocolFamily, operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse::<AccountId>().expect("account should parse"),
            "eu-west-2".parse::<RegionId>().expect("region should parse"),
            ServiceName::Sqs,
            protocol,
            operation,
            None,
            false,
        )
        .expect("request context should build")
    }

    #[test]
    fn sqs_standard_query_queue_path_lifecycle_round_trips() {
        let router = router();

        let create = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let send = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SendMessage&MessageBody=payload",
        ));
        let receive = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=ReceiveMessage&VisibilityTimeout=0&WaitTimeSeconds=0",
        ));
        let delete = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=DeleteMessage&ReceiptHandle=AQEB00000000000000000001",
        ));
        let get_attributes = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=GetQueueAttributes&AttributeName.1=QueueArn",
        ));

        let create_bytes = create.to_http_bytes();
        let send_bytes = send.to_http_bytes();
        let receive_bytes = receive.to_http_bytes();
        let delete_bytes = delete.to_http_bytes();
        let attributes_bytes = get_attributes.to_http_bytes();

        let (create_status, create_headers, create_body) =
            split_response(&create_bytes);
        let (send_status, _, send_body) = split_response(&send_bytes);
        let (receive_status, _, receive_body) = split_response(&receive_bytes);
        let (delete_status, _, delete_body) = split_response(&delete_bytes);
        let (attributes_status, _, attributes_body) =
            split_response(&attributes_bytes);

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&create_headers, "content-type"),
            Some("text/xml")
        );
        assert!(create_body.contains(
            "<QueueUrl>http://localhost:4566/000000000000/orders</QueueUrl>"
        ));
        assert_eq!(send_status, "HTTP/1.1 200 OK");
        assert!(send_body.contains(
            "<MessageId>00000000-0000-0000-0000-000000000001</MessageId>"
        ));
        assert_eq!(receive_status, "HTTP/1.1 200 OK");
        assert!(receive_body.contains(
            "<ReceiptHandle>AQEB00000000000000000001</ReceiptHandle>"
        ));
        assert_eq!(delete_status, "HTTP/1.1 200 OK");
        assert!(delete_body.contains("<DeleteMessageResponse"));
        assert_eq!(attributes_status, "HTTP/1.1 200 OK");
        assert!(attributes_body.contains(
            "<Value>arn:aws:sqs:eu-west-2:000000000000:orders</Value>"
        ));
    }

    #[test]
    fn sqs_standard_query_invalid_receipt_handle_returns_sqs_error() {
        let router = router();

        let create = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let _ = create.to_http_bytes();
        let delete = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=DeleteMessage&ReceiptHandle=garbage",
        ));
        let delete_bytes = delete.to_http_bytes();
        let (status, _, body) = split_response(&delete_bytes);

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert!(body.contains("<Code>ReceiptHandleIsInvalid</Code>"));
        assert!(body.contains("garbage"));
    }

    #[test]
    fn sqs_standard_json_lifecycle_and_tag_flow_round_trips() {
        let router = router();

        let create = router.handle_bytes(&json_request(
            "AmazonSQS.CreateQueue",
            r#"{"QueueName":"orders","Attributes":{"VisibilityTimeout":"45"}}"#,
        ));
        let send = router.handle_bytes(&json_request(
            "AmazonSQS.SendMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","MessageBody":"payload"}"#,
        ));
        let tag = router.handle_bytes(&json_request(
            "AmazonSQS.TagQueue",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","Tags":{"env":"dev","team":"platform"}}"#,
        ));
        let list_tags = router.handle_bytes(&json_request(
            "AmazonSQS.ListQueueTags",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders"}"#,
        ));
        let receive = router.handle_bytes(&json_request(
            "AmazonSQS.ReceiveMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","VisibilityTimeout":0,"WaitTimeSeconds":0}"#,
        ));

        let create_bytes = create.to_http_bytes();
        let send_bytes = send.to_http_bytes();
        let tag_bytes = tag.to_http_bytes();
        let list_tags_bytes = list_tags.to_http_bytes();
        let receive_bytes = receive.to_http_bytes();

        let (create_status, create_headers, create_body) =
            split_response(&create_bytes);
        let (send_status, _, send_body) = split_response(&send_bytes);
        let (tag_status, _, tag_body) = split_response(&tag_bytes);
        let (_, _, list_tags_body) = split_response(&list_tags_bytes);
        let (receive_status, _, receive_body) = split_response(&receive_bytes);

        assert_eq!(create_status, "HTTP/1.1 200 OK");
        assert_eq!(
            header_value(&create_headers, "content-type"),
            Some("application/x-amz-json-1.0")
        );
        assert!(create_body.contains(
            "\"QueueUrl\":\"http://localhost:4566/000000000000/orders\""
        ));
        assert_eq!(send_status, "HTTP/1.1 200 OK");
        assert!(send_body.contains(
            "\"MessageId\":\"00000000-0000-0000-0000-000000000001\""
        ));
        assert_eq!(tag_status, "HTTP/1.1 200 OK");
        assert_eq!(tag_body, "{}");
        assert!(list_tags_body.contains("\"team\":\"platform\""));
        assert!(list_tags_body.contains("\"env\":\"dev\""));
        assert_eq!(receive_status, "HTTP/1.1 200 OK");
        assert!(
            receive_body
                .contains("\"ReceiptHandle\":\"AQEB00000000000000000001\"")
        );
    }

    #[test]
    fn sqs_standard_json_missing_queue_url_returns_explicit_error() {
        let router = router();
        let response = router.handle_bytes(&json_request(
            "AmazonSQS.DeleteMessage",
            r#"{"ReceiptHandle":"garbage"}"#,
        ));
        let bytes = response.to_http_bytes();
        let (status, _, body) = split_response(&bytes);

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert!(body.contains("\"__type\":\"MissingParameter\""));
        assert!(body.contains("QueueUrl"));
    }

    #[test]
    fn sqs_query_receive_message_uses_queue_default_wait_time_when_request_omits_it()
     {
        let sqs = SqsService::new();
        let advertised_edge = AdvertisedEdge::new("http", "localhost", 4566);
        let scope = SqsScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let queue = sqs
            .create_queue(
                &scope,
                CreateQueueInput {
                    attributes: BTreeMap::from([(
                        "ReceiveMessageWaitTimeSeconds".to_owned(),
                        "1".to_owned(),
                    )]),
                    queue_name: "orders".to_owned(),
                },
            )
            .expect("queue should create");
        let request = parse_request(&query_request(
            "/000000000000/orders",
            "Action=ReceiveMessage",
        ));
        let sender = sqs.clone();
        let delayed_send = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            sender
                .send_message(
                    &queue,
                    SendMessageInput {
                        body: "payload".to_owned(),
                        delay_seconds: None,
                        message_attributes: BTreeMap::new(),
                        message_deduplication_id: None,
                        message_group_id: None,
                        message_system_attributes: BTreeMap::new(),
                    },
                )
                .expect("message should send");
        });
        let shutdown_signal = AtomicBool::new(false);
        let sqs_requests = SqsRequestRuntime::new(sqs.clone());

        let response = super::handle_query(
            &sqs,
            &sqs_requests,
            &advertised_edge,
            &request,
            &context(ProtocolFamily::Query, "ReceiveMessage"),
            &shutdown_signal,
        )
        .expect("query receive should succeed");
        delayed_send.join().expect("delayed sender should finish cleanly");

        assert!(response.contains("<Body>payload</Body>"));
    }

    #[test]
    fn sqs_json_receive_message_uses_request_wait_time_over_queue_default() {
        let sqs = SqsService::new();
        let advertised_edge = AdvertisedEdge::new("http", "localhost", 4566);
        let scope = SqsScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        );
        let queue = sqs
            .create_queue(
                &scope,
                CreateQueueInput {
                    attributes: BTreeMap::new(),
                    queue_name: "orders".to_owned(),
                },
            )
            .expect("queue should create");
        let request = parse_request(&json_request(
            "AmazonSQS.ReceiveMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","WaitTimeSeconds":1}"#,
        ));
        let sender = sqs.clone();
        let delayed_send = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            sender
                .send_message(
                    &queue,
                    SendMessageInput {
                        body: "payload".to_owned(),
                        delay_seconds: None,
                        message_attributes: BTreeMap::new(),
                        message_deduplication_id: None,
                        message_group_id: None,
                        message_system_attributes: BTreeMap::new(),
                    },
                )
                .expect("message should send");
        });
        let shutdown_signal = AtomicBool::new(false);
        let sqs_requests = SqsRequestRuntime::new(sqs.clone());

        let response = super::handle_json(
            &sqs,
            &sqs_requests,
            &advertised_edge,
            &request,
            &context(ProtocolFamily::AwsJson10, "ReceiveMessage"),
            &shutdown_signal,
        )
        .expect("json receive should succeed");
        delayed_send.join().expect("delayed sender should finish cleanly");
        let response: Value = serde_json::from_slice(&response)
            .expect("response should be json");

        assert_eq!(response["Messages"][0]["Body"], "payload");
    }

    #[test]
    fn sqs_standard_queue_url_helper_uses_advertised_edge() {
        let advertised_edge = AdvertisedEdge::new("http", "localhost", 9999);
        let queue = SqsQueueIdentity::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            "orders",
        )
        .expect("queue identity should build");

        assert_eq!(
            queue_url(&advertised_edge, &queue),
            "http://localhost:9999/000000000000/orders"
        );
        assert!(matches!(
            ProtocolFamily::AwsJson10,
            ProtocolFamily::AwsJson10
        ));
        assert!(matches!(ServiceName::Sqs, ServiceName::Sqs));
    }

    #[test]
    fn sqs_standard_query_admin_actions_round_trip() {
        let router = router();

        let create = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let get_url = router.handle_bytes(&query_request(
            "/",
            "Action=GetQueueUrl&QueueName=orders",
        ));
        let list = router.handle_bytes(&query_request(
            "/",
            "Action=ListQueues&QueueNamePrefix=ord",
        ));
        let set_attributes = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SetQueueAttributes&Attribute.1.Name=DelaySeconds&Attribute.1.Value=2",
        ));
        let tag = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=TagQueue&Tag.1.Name=env&Tag.1.Value=dev",
        ));
        let list_tags = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=ListQueueTags",
        ));
        let untag = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=UntagQueue&TagKey.1=env",
        ));
        let send = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SendMessage&MessageBody=payload",
        ));
        let receive = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=ReceiveMessage&VisibilityTimeout=5&WaitTimeSeconds=0",
        ));
        let change_visibility = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=ChangeMessageVisibility&ReceiptHandle=AQEB00000000000000000001&VisibilityTimeout=0",
        ));
        let purge = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=PurgeQueue",
        ));
        let delete = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=DeleteQueue",
        ));

        let _create_bytes = create.to_http_bytes();
        let get_url_bytes = get_url.to_http_bytes();
        let list_bytes = list.to_http_bytes();
        let set_attributes_bytes = set_attributes.to_http_bytes();
        let tag_bytes = tag.to_http_bytes();
        let list_tags_bytes = list_tags.to_http_bytes();
        let untag_bytes = untag.to_http_bytes();
        let send_bytes = send.to_http_bytes();
        let receive_bytes = receive.to_http_bytes();
        let change_visibility_bytes = change_visibility.to_http_bytes();
        let purge_bytes = purge.to_http_bytes();
        let delete_bytes = delete.to_http_bytes();

        let (_, _, get_url_body) = split_response(&get_url_bytes);
        let (_, _, list_body) = split_response(&list_bytes);
        let (set_status, _, set_body) = split_response(&set_attributes_bytes);
        let (tag_status, _, tag_body) = split_response(&tag_bytes);
        let (_, _, list_tags_body) = split_response(&list_tags_bytes);
        let (untag_status, _, untag_body) = split_response(&untag_bytes);
        let (_, _, send_body) = split_response(&send_bytes);
        let (_, _, receive_body) = split_response(&receive_bytes);
        let (change_status, _, change_body) =
            split_response(&change_visibility_bytes);
        let (purge_status, _, purge_body) = split_response(&purge_bytes);
        let (delete_status, _, delete_body) = split_response(&delete_bytes);
        assert!(get_url_body.contains(
            "<QueueUrl>http://localhost:4566/000000000000/orders</QueueUrl>"
        ));
        assert!(list_body.contains(
            "<QueueUrl>http://localhost:4566/000000000000/orders</QueueUrl>"
        ));
        assert_eq!(set_status, "HTTP/1.1 200 OK");
        assert!(set_body.contains("<SetQueueAttributesResponse"));
        assert_eq!(tag_status, "HTTP/1.1 200 OK");
        assert!(tag_body.contains("<TagQueueResponse"));
        assert!(list_tags_body.contains("<Key>env</Key>"));
        assert_eq!(untag_status, "HTTP/1.1 200 OK");
        assert!(untag_body.contains("<UntagQueueResponse"));
        assert!(send_body.contains("<MD5OfMessageBody>"));
        assert!(receive_body.contains(
            "<ReceiptHandle>AQEB00000000000000000001</ReceiptHandle>"
        ));
        assert_eq!(change_status, "HTTP/1.1 200 OK");
        assert!(change_body.contains("<ChangeMessageVisibilityResponse"));
        assert_eq!(purge_status, "HTTP/1.1 200 OK");
        assert!(purge_body.contains("<PurgeQueueResponse"));
        assert_eq!(delete_status, "HTTP/1.1 200 OK");
        assert!(delete_body.contains("<DeleteQueueResponse"));
    }

    #[test]
    fn sqs_standard_json_admin_actions_round_trip() {
        let router = router();

        let create = router.handle_bytes(&json_request(
            "AmazonSQS.CreateQueue",
            r#"{"QueueName":"orders"}"#,
        ));
        let get_url = router.handle_bytes(&json_request(
            "AmazonSQS.GetQueueUrl",
            r#"{"QueueName":"orders"}"#,
        ));
        let list = router.handle_bytes(&json_request(
            "AmazonSQS.ListQueues",
            r#"{"QueueNamePrefix":"ord"}"#,
        ));
        let set_attributes = router.handle_bytes(&json_request(
            "AmazonSQS.SetQueueAttributes",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","Attributes":{"DelaySeconds":"2"}}"#,
        ));
        let get_attributes = router.handle_bytes(&json_request(
            "AmazonSQS.GetQueueAttributes",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","AttributeNames":["DelaySeconds"]}"#,
        ));
        let send = router.handle_bytes(&json_request(
            "AmazonSQS.SendMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","MessageBody":"payload"}"#,
        ));
        let receive = router.handle_bytes(&json_request(
            "AmazonSQS.ReceiveMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","VisibilityTimeout":5,"WaitTimeSeconds":0}"#,
        ));
        let delete_message = router.handle_bytes(&json_request(
            "AmazonSQS.DeleteMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","ReceiptHandle":"AQEB00000000000000000001"}"#,
        ));
        let tag = router.handle_bytes(&json_request(
            "AmazonSQS.TagQueue",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","Tags":{"env":"dev"}}"#,
        ));
        let untag = router.handle_bytes(&json_request(
            "AmazonSQS.UntagQueue",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders","TagKeys":["env"]}"#,
        ));
        let list_tags = router.handle_bytes(&json_request(
            "AmazonSQS.ListQueueTags",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders"}"#,
        ));
        let purge = router.handle_bytes(&json_request(
            "AmazonSQS.PurgeQueue",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders"}"#,
        ));
        let delete_queue = router.handle_bytes(&json_request(
            "AmazonSQS.DeleteQueue",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders"}"#,
        ));

        let _create_bytes = create.to_http_bytes();
        let get_url_bytes = get_url.to_http_bytes();
        let list_bytes = list.to_http_bytes();
        let set_attributes_bytes = set_attributes.to_http_bytes();
        let get_attributes_bytes = get_attributes.to_http_bytes();
        let send_bytes = send.to_http_bytes();
        let receive_bytes = receive.to_http_bytes();
        let delete_message_bytes = delete_message.to_http_bytes();
        let tag_bytes = tag.to_http_bytes();
        let untag_bytes = untag.to_http_bytes();
        let list_tags_bytes = list_tags.to_http_bytes();
        let purge_bytes = purge.to_http_bytes();
        let delete_queue_bytes = delete_queue.to_http_bytes();

        let (_, _, get_url_body) = split_response(&get_url_bytes);
        let (_, _, list_body) = split_response(&list_bytes);
        let (set_status, _, set_body) = split_response(&set_attributes_bytes);
        let (_, _, get_attributes_body) =
            split_response(&get_attributes_bytes);
        let (_, _, send_body) = split_response(&send_bytes);
        let (_, _, receive_body) = split_response(&receive_bytes);
        let (delete_message_status, _, delete_message_body) =
            split_response(&delete_message_bytes);
        let (tag_status, _, tag_body) = split_response(&tag_bytes);
        let (untag_status, _, untag_body) = split_response(&untag_bytes);
        let (_, _, list_tags_body) = split_response(&list_tags_bytes);
        let (purge_status, _, purge_body) = split_response(&purge_bytes);
        let (delete_status, _, delete_body) =
            split_response(&delete_queue_bytes);

        assert!(get_url_body.contains(
            "\"QueueUrl\":\"http://localhost:4566/000000000000/orders\""
        ));
        assert!(list_body.contains(
            "\"QueueUrls\":[\"http://localhost:4566/000000000000/orders\"]"
        ));
        assert_eq!(set_status, "HTTP/1.1 200 OK");
        assert_eq!(set_body, "{}");
        assert!(get_attributes_body.contains("\"DelaySeconds\":\"2\""));
        assert!(send_body.contains("\"MD5OfMessageBody\""));
        assert!(
            receive_body
                .contains("\"ReceiptHandle\":\"AQEB00000000000000000001\"")
        );
        assert_eq!(delete_message_status, "HTTP/1.1 200 OK");
        assert_eq!(delete_message_body, "{}");
        assert_eq!(tag_status, "HTTP/1.1 200 OK");
        assert_eq!(tag_body, "{}");
        assert_eq!(untag_status, "HTTP/1.1 200 OK");
        assert_eq!(untag_body, "{}");
        assert!(!list_tags_body.contains("\"env\":\"dev\""));
        assert_eq!(purge_status, "HTTP/1.1 200 OK");
        assert_eq!(purge_body, "{}");
        assert_eq!(delete_status, "HTTP/1.1 200 OK");
        assert_eq!(delete_body, "{}");
    }

    #[test]
    fn sqs_standard_internal_helpers_report_explicit_errors() {
        let advertised_edge = AdvertisedEdge::default();
        let query_context = context(ProtocolFamily::Query, "MissingAction");
        let json_context = context(ProtocolFamily::AwsJson10, "ListQueues");
        let shutdown_signal = AtomicBool::new(false);
        let sqs = SqsService::default();
        let sqs_requests = SqsRequestRuntime::new(sqs.clone());

        let missing_action_bytes = query_request("/", "QueueName=orders");
        let missing_action_request =
            crate::request::HttpRequest::parse(&missing_action_bytes)
                .expect("request should parse");
        let missing_action = super::handle_query(
            &sqs,
            &sqs_requests,
            &advertised_edge,
            &missing_action_request,
            &query_context,
            &shutdown_signal,
        )
        .expect_err("missing actions should fail");
        assert_eq!(missing_action.code(), "MissingAction");

        let unsupported_query_bytes = query_request("/", "Action=Nope");
        let unsupported_query_request =
            crate::request::HttpRequest::parse(&unsupported_query_bytes)
                .expect("request should parse");
        let unsupported_query = super::handle_query(
            &sqs,
            &sqs_requests,
            &advertised_edge,
            &unsupported_query_request,
            &query_context,
            &shutdown_signal,
        )
        .expect_err("unsupported query actions should fail");
        assert_eq!(
            unsupported_query.code(),
            "AWS.SimpleQueueService.UnsupportedOperation"
        );

        let missing_target_request = crate::request::HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");
        let missing_target = super::handle_json(
            &sqs,
            &sqs_requests,
            &advertised_edge,
            &missing_target_request,
            &json_context,
            &shutdown_signal,
        )
        .expect_err("missing JSON targets should fail");
        assert_eq!(
            missing_target.code(),
            "AWS.SimpleQueueService.UnsupportedOperation"
        );

        let invalid_json_request = crate::request::HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: AmazonSQS.ListQueues\r\nContent-Length: 1\r\n\r\n{",
        )
        .expect("request should parse");
        let invalid_json = super::handle_json(
            &sqs,
            &sqs_requests,
            &advertised_edge,
            &invalid_json_request,
            &json_context,
            &shutdown_signal,
        )
        .expect_err("invalid JSON bodies should fail");
        assert_eq!(invalid_json.code(), "InvalidParameterValue");

        let root_request = crate::request::HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost:4566\r\n\r\n",
        )
        .expect("request should parse");
        let missing_queue = super::queue_identity_from_request(
            None,
            &root_request,
            &json_context,
        )
        .expect_err("missing queue URLs should fail");
        assert_eq!(missing_queue.code(), "MissingParameter");

        let invalid_queue = super::parse_queue_identity(
            "http://localhost:4566/bad",
            &json_context,
        )
        .expect_err("invalid queue URLs should fail");
        assert_eq!(
            invalid_queue.code(),
            "AWS.SimpleQueueService.NonExistentQueue"
        );

        let query_params = crate::query::QueryParameters::parse(
            b"Tag.1.Name=env&VisibilityTimeout=bad",
        )
        .expect("query parameters should parse");
        assert_eq!(
            super::query_map(&query_params, "Tag"),
            BTreeMap::<String, String>::new()
        );
        assert_eq!(
            super::optional_u32_query(&query_params, "VisibilityTimeout")
                .expect_err("invalid numeric query params should fail")
                .code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            super::required_u32_query(&query_params, "VisibilityTimeout")
                .expect_err("required numeric query params should fail"),
            crate::query::malformed_query_error()
        );

        assert_eq!(
            super::parse_json_body(b"")
                .expect("empty JSON bodies should normalize"),
            Value::Object(Map::new())
        );
        assert_eq!(
            super::optional_u32_json(
                &json!({"WaitTimeSeconds": 5}),
                "WaitTimeSeconds"
            )
            .expect("numeric JSON parameters should parse"),
            Some(5)
        );
        assert_eq!(
            super::optional_u32_json(
                &json!({"WaitTimeSeconds": "5"}),
                "WaitTimeSeconds",
            )
            .expect("string JSON parameters should parse"),
            Some(5)
        );
        assert_eq!(
            super::optional_u32_json(
                &json!({"WaitTimeSeconds": true}),
                "WaitTimeSeconds",
            )
            .expect_err("non-numeric JSON parameters should fail")
            .code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            super::json_string_list_field(
                &json!({"TagKeys": "env"}),
                "TagKeys"
            )
            .expect_err("non-array string lists should fail")
            .code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            super::json_string_list_field(
                &json!({"TagKeys": ["env", 1]}),
                "TagKeys",
            )
            .expect_err("non-string array items should fail")
            .code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            super::json_map_field(&json!({"Tags": true}), "Tags")
                .expect_err("non-object maps should fail")
                .code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            super::json_map_field(&json!({"Tags": {"env": true}}), "Tags")
                .expect("boolean map values should stringify"),
            BTreeMap::from([("env".to_owned(), "true".to_owned())])
        );
        assert_eq!(
            super::json_map_field(&json!({"Tags": {"delay": 2}}), "Tags")
                .expect("numeric map values should stringify"),
            BTreeMap::from([("delay".to_owned(), "2".to_owned())])
        );
        assert_eq!(
            super::json_map_field(&json!({"Tags": {"env": []}}), "Tags")
                .expect_err("nested arrays in JSON maps should fail")
                .code(),
            "InvalidParameterValue"
        );

        assert!(
            super::response_with_result(
                "ListQueues",
                "<QueueUrl>x</QueueUrl>"
            )
            .contains("ResponseMetadata")
        );
        assert!(
            super::response_without_result("DeleteQueue")
                .contains("ResponseMetadata")
        );
        assert_eq!(
            super::sqs_missing_parameter("QueueUrl").code(),
            "MissingParameter"
        );
        assert_eq!(
            super::invalid_json_parameter("QueueUrl").code(),
            "InvalidParameterValue"
        );
        assert_eq!(
            super::unsupported_operation_error("Nope").code(),
            "AWS.SimpleQueueService.UnsupportedOperation"
        );
    }

    #[test]
    fn sqs_query_message_attribute_zero_index_returns_validation_error() {
        let router = router();
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let response = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SendMessage&MessageBody=payload&MessageAttribute.1.Name=broken&MessageAttribute.1.Value.DataType=String&MessageAttribute.1.Value.StringListValue.0=value",
        ));
        let bytes = response.to_http_bytes();
        let (status, _, body) = split_response(&bytes);

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert!(body.contains("<Code>InvalidParameterValue</Code>"));
    }

    #[test]
    fn sqs_query_message_attribute_sparse_index_returns_validation_error() {
        let router = router();
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let response = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SendMessage&MessageBody=payload&MessageAttribute.2.Name=broken&MessageAttribute.2.Value.DataType=String&MessageAttribute.2.Value.StringValue=value",
        ));
        let bytes = response.to_http_bytes();
        let (status, _, body) = split_response(&bytes);

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert!(body.contains("<Code>InvalidParameterValue</Code>"));
    }

    #[test]
    fn sqs_query_message_system_attribute_sparse_index_returns_validation_error()
     {
        let router = router();
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let response = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SendMessage&MessageBody=payload&MessageSystemAttribute.2.Name=AWSTraceHeader&MessageSystemAttribute.2.Value.DataType=String&MessageSystemAttribute.2.Value.StringValue=Root=1-67891233-abcdef012345678912345678",
        ));
        let bytes = response.to_http_bytes();
        let (status, _, body) = split_response(&bytes);

        assert_eq!(status, "HTTP/1.1 400 Bad Request");
        assert!(body.contains("<Code>InvalidParameterValue</Code>"));
    }

    #[test]
    fn sqs_query_binary_message_attributes_are_base64_decoded() {
        let router = router();
        let _ = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders",
        ));
        let send = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=SendMessage&MessageBody=payload&MessageAttribute.1.Name=blob&MessageAttribute.1.Value.DataType=Binary&MessageAttribute.1.Value.BinaryValue=AQI=",
        ));
        let receive = router.handle_bytes(&query_request(
            "/000000000000/orders",
            "Action=ReceiveMessage&MessageAttributeName.1=All&VisibilityTimeout=0&WaitTimeSeconds=0",
        ));
        let send_bytes = send.to_http_bytes();
        let receive_bytes = receive.to_http_bytes();
        let (send_status, _, send_body) = split_response(&send_bytes);
        let (receive_status, _, receive_body) = split_response(&receive_bytes);

        assert_eq!(send_status, "HTTP/1.1 200 OK");
        assert!(send_body.contains("<MD5OfMessageAttributes>"));
        assert_eq!(receive_status, "HTTP/1.1 200 OK");
        assert!(receive_body.contains(
            "<MessageAttribute><Name>blob</Name><Value><DataType>Binary</DataType><BinaryValue>AQI=</BinaryValue></Value></MessageAttribute>"
        ));
    }

    #[test]
    fn sqs_fifo_query_batch_and_redrive_actions_round_trip() {
        let router = router();
        let dlq_arn =
            "arn:aws:sqs:eu-west-2:000000000000:orders-dlq.fifo".to_owned();

        let create_dlq = router.handle_bytes(&query_request(
            "/",
            "Action=CreateQueue&QueueName=orders-dlq.fifo&Attribute.1.Name=FifoQueue&Attribute.1.Value=true&Attribute.2.Name=ContentBasedDeduplication&Attribute.2.Value=true",
        ));
        let create_source = router.handle_bytes(&query_request(
            "/",
            &format!(
                "Action=CreateQueue&QueueName=orders-source.fifo&Attribute.1.Name=FifoQueue&Attribute.1.Value=true&Attribute.2.Name=ContentBasedDeduplication&Attribute.2.Value=true&Attribute.3.Name=RedrivePolicy&Attribute.3.Value={{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":1}}"
            ),
        ));
        let batch_send = router.handle_bytes(&query_request(
            "/000000000000/orders-source.fifo",
            "Action=SendMessageBatch&SendMessageBatchRequestEntry.1.Id=first&SendMessageBatchRequestEntry.1.MessageBody=payload&SendMessageBatchRequestEntry.1.MessageGroupId=group-1&SendMessageBatchRequestEntry.2.Id=second&SendMessageBatchRequestEntry.2.MessageBody=ignored",
        ));
        let first_receive = router.handle_bytes(&query_request(
            "/000000000000/orders-source.fifo",
            "Action=ReceiveMessage&VisibilityTimeout=0&WaitTimeSeconds=0",
        ));
        let second_receive = router.handle_bytes(&query_request(
            "/000000000000/orders-source.fifo",
            "Action=ReceiveMessage&VisibilityTimeout=0&WaitTimeSeconds=0",
        ));
        let list_sources = router.handle_bytes(&query_request(
            "/000000000000/orders-dlq.fifo",
            "Action=ListDeadLetterSourceQueues",
        ));
        let start_move = router.handle_bytes(&query_request(
            "/",
            &format!("Action=StartMessageMoveTask&SourceArn={dlq_arn}"),
        ));
        let redriven_receive = router.handle_bytes(&query_request(
            "/000000000000/orders-source.fifo",
            "Action=ReceiveMessage&VisibilityTimeout=0&WaitTimeSeconds=0",
        ));

        let create_dlq_bytes = create_dlq.to_http_bytes();
        let create_source_bytes = create_source.to_http_bytes();
        let batch_send_bytes = batch_send.to_http_bytes();
        let first_receive_bytes = first_receive.to_http_bytes();
        let second_receive_bytes = second_receive.to_http_bytes();
        let list_sources_bytes = list_sources.to_http_bytes();
        let start_move_bytes = start_move.to_http_bytes();
        let redriven_receive_bytes = redriven_receive.to_http_bytes();

        let (_, _, create_dlq_body) = split_response(&create_dlq_bytes);
        let (_, _, create_source_body) = split_response(&create_source_bytes);
        let (batch_status, _, batch_body) = split_response(&batch_send_bytes);
        let (first_receive_status, _, first_receive_body) =
            split_response(&first_receive_bytes);
        let (_, _, second_receive_body) =
            split_response(&second_receive_bytes);
        let (list_status, _, list_body) = split_response(&list_sources_bytes);
        let (move_status, _, move_body) = split_response(&start_move_bytes);
        let (redriven_status, _, redriven_body) =
            split_response(&redriven_receive_bytes);

        assert!(create_dlq_body.contains("orders-dlq.fifo"));
        assert!(create_source_body.contains("orders-source.fifo"));
        assert_eq!(batch_status, "HTTP/1.1 200 OK");
        assert!(batch_body.contains("<SendMessageBatchResultEntry>"));
        assert!(batch_body.contains("<BatchResultErrorEntry>"));
        assert!(batch_body.contains("<Code>MissingParameter</Code>"));
        assert_eq!(first_receive_status, "HTTP/1.1 200 OK");
        assert!(first_receive_body.contains("<Body>payload</Body>"));
        assert!(!second_receive_body.contains("<Message>"));
        assert_eq!(list_status, "HTTP/1.1 200 OK");
        assert!(list_body.contains(
            "<QueueUrl>http://localhost:4566/000000000000/orders-source.fifo</QueueUrl>"
        ));
        assert_eq!(move_status, "HTTP/1.1 200 OK");
        assert!(move_body.contains("<TaskHandle>"));
        assert_eq!(redriven_status, "HTTP/1.1 200 OK");
        assert!(redriven_body.contains("<Body>payload</Body>"));
    }

    #[test]
    fn sqs_fifo_json_batch_and_visibility_actions_round_trip() {
        let router = router();

        let create = router.handle_bytes(&json_request(
            "AmazonSQS.CreateQueue",
            r#"{"QueueName":"orders.fifo","Attributes":{"FifoQueue":"true"}}"#,
        ));
        let batch_send = router.handle_bytes(&json_request(
            "AmazonSQS.SendMessageBatch",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","Entries":[{"Id":"first","MessageBody":"payload","MessageGroupId":"group-1","MessageDeduplicationId":"dedup-1"},{"Id":"second","MessageBody":"ignored","MessageDeduplicationId":"dedup-2"}]}"#,
        ));
        let receive = router.handle_bytes(&json_request(
            "AmazonSQS.ReceiveMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","VisibilityTimeout":0,"WaitTimeSeconds":0}"#,
        ));
        let delete_batch = router.handle_bytes(&json_request(
            "AmazonSQS.DeleteMessageBatch",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","Entries":[{"Id":"delete-ok","ReceiptHandle":"AQEB00000000000000000001"},{"Id":"delete-bad","ReceiptHandle":"garbage"}]}"#,
        ));
        let send_again = router.handle_bytes(&json_request(
            "AmazonSQS.SendMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","MessageBody":"next","MessageGroupId":"group-1","MessageDeduplicationId":"dedup-3"}"#,
        ));
        let receive_again = router.handle_bytes(&json_request(
            "AmazonSQS.ReceiveMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","VisibilityTimeout":0,"WaitTimeSeconds":0}"#,
        ));
        let change_visibility_batch = router.handle_bytes(&json_request(
            "AmazonSQS.ChangeMessageVisibilityBatch",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","Entries":[{"Id":"change-ok","ReceiptHandle":"AQEB00000000000000000002","VisibilityTimeout":30},{"Id":"change-bad","ReceiptHandle":"garbage","VisibilityTimeout":30}]}"#,
        ));
        let hidden = router.handle_bytes(&json_request(
            "AmazonSQS.ReceiveMessage",
            r#"{"QueueUrl":"http://localhost:4566/000000000000/orders.fifo","VisibilityTimeout":0,"WaitTimeSeconds":0}"#,
        ));

        let create_bytes = create.to_http_bytes();
        let batch_send_bytes = batch_send.to_http_bytes();
        let receive_bytes = receive.to_http_bytes();
        let delete_batch_bytes = delete_batch.to_http_bytes();
        let send_again_bytes = send_again.to_http_bytes();
        let receive_again_bytes = receive_again.to_http_bytes();
        let change_visibility_batch_bytes =
            change_visibility_batch.to_http_bytes();
        let hidden_bytes = hidden.to_http_bytes();

        let (_, _, create_body) = split_response(&create_bytes);
        let (batch_status, _, batch_body) = split_response(&batch_send_bytes);
        let (_, _, receive_body) = split_response(&receive_bytes);
        let (delete_status, _, delete_body) =
            split_response(&delete_batch_bytes);
        let (_, _, send_again_body) = split_response(&send_again_bytes);
        let (_, _, receive_again_body) = split_response(&receive_again_bytes);
        let (change_status, _, change_body) =
            split_response(&change_visibility_batch_bytes);
        let (_, _, hidden_body) = split_response(&hidden_bytes);

        assert!(create_body.contains(
            "\"QueueUrl\":\"http://localhost:4566/000000000000/orders.fifo\""
        ));
        assert_eq!(batch_status, "HTTP/1.1 200 OK");
        assert!(batch_body.contains("\"Successful\":["));
        assert!(batch_body.contains("\"Id\":\"first\""));
        assert!(batch_body.contains("\"Failed\":["));
        assert!(batch_body.contains("\"Code\":\"MissingParameter\""));
        assert!(
            receive_body
                .contains("\"ReceiptHandle\":\"AQEB00000000000000000001\"")
        );
        assert_eq!(delete_status, "HTTP/1.1 200 OK");
        assert!(delete_body.contains("\"Id\":\"delete-ok\""));
        assert!(delete_body.contains("\"Code\":\"ReceiptHandleIsInvalid\""));
        assert!(send_again_body.contains("\"SequenceNumber\":\"2\""));
        assert!(
            receive_again_body
                .contains("\"ReceiptHandle\":\"AQEB00000000000000000002\"")
        );
        assert_eq!(change_status, "HTTP/1.1 200 OK");
        assert!(change_body.contains("\"Id\":\"change-ok\""));
        assert!(change_body.contains("\"Code\":\"ReceiptHandleIsInvalid\""));
        assert_eq!(hidden_body, "{\"Messages\":[]}");
    }
}
