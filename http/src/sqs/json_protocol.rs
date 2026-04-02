use super::message_attributes::json_message_attributes_map;
use serde_json::{Value, json};
use services::{
    BatchFailure, ChangeMessageVisibilityBatchOutput,
    DeleteMessageBatchOutput, ListMessageMoveTasksOutput, ReceivedMessage,
    SendMessageBatchOutput,
};

pub(super) fn json_received_messages(
    messages: &[ReceivedMessage],
) -> Vec<Value> {
    messages
        .iter()
        .map(|message| {
            let mut value = json!({
                "Attributes": message.attributes,
                "Body": message.body,
                "MD5OfBody": message.md5_of_body,
                "MessageId": message.message_id,
                "ReceiptHandle": message.receipt_handle,
                "MessageAttributes": json_message_attributes_map(
                    &message.message_attributes,
                ),
            });
            if let Some(md5_of_message_attributes) =
                &message.md5_of_message_attributes
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "MD5OfMessageAttributes".to_owned(),
                    Value::String(md5_of_message_attributes.clone()),
                );
            }
            value
        })
        .collect()
}

pub(super) fn json_list_message_move_tasks(
    tasks: &ListMessageMoveTasksOutput,
) -> Vec<Value> {
    tasks
        .results
        .iter()
        .map(|task| {
            let mut value = json!({
                "ApproximateNumberOfMessagesMoved":
                    task.approximate_number_of_messages_moved,
                "SourceArn": task.source_arn,
                "StartedTimestamp": task.started_timestamp,
                "Status": task.status,
            });
            if let Some(object) = value.as_object_mut() {
                if let Some(task_handle) = task.task_handle.as_ref() {
                    object.insert(
                        "TaskHandle".to_owned(),
                        Value::String(task_handle.clone()),
                    );
                }
                if let Some(destination_arn) = task.destination_arn.as_ref() {
                    object.insert(
                        "DestinationArn".to_owned(),
                        Value::String(destination_arn.clone()),
                    );
                }
                if let Some(max_number_of_messages_per_second) =
                    task.max_number_of_messages_per_second
                {
                    object.insert(
                        "MaxNumberOfMessagesPerSecond".to_owned(),
                        json!(max_number_of_messages_per_second),
                    );
                }
                if let Some(approximate_number_of_messages_to_move) =
                    task.approximate_number_of_messages_to_move
                {
                    object.insert(
                        "ApproximateNumberOfMessagesToMove".to_owned(),
                        json!(approximate_number_of_messages_to_move),
                    );
                }
                if let Some(failure_reason) = task.failure_reason.as_ref() {
                    object.insert(
                        "FailureReason".to_owned(),
                        Value::String(failure_reason.clone()),
                    );
                }
            }

            value
        })
        .collect()
}

pub(super) fn json_send_message_batch_result(
    batch: &SendMessageBatchOutput,
) -> Value {
    let successful = batch
        .successful
        .iter()
        .map(|entry| {
            let mut value = json!({
                "Id": entry.id,
                "MD5OfMessageBody": entry.md5_of_message_body,
                "MessageId": entry.message_id,
            });
            if let Some(md5_of_message_attributes) =
                &entry.md5_of_message_attributes
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "MD5OfMessageAttributes".to_owned(),
                    Value::String(md5_of_message_attributes.clone()),
                );
            }
            if let Some(md5_of_message_system_attributes) =
                &entry.md5_of_message_system_attributes
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "MD5OfMessageSystemAttributes".to_owned(),
                    Value::String(md5_of_message_system_attributes.clone()),
                );
            }
            if let Some(sequence_number) = &entry.sequence_number
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "SequenceNumber".to_owned(),
                    Value::String(sequence_number.clone()),
                );
            }
            value
        })
        .collect::<Vec<_>>();

    json!({
        "Failed": json_batch_failures(&batch.failed),
        "Successful": successful,
    })
}

pub(super) fn json_delete_message_batch_result(
    batch: &DeleteMessageBatchOutput,
) -> Value {
    let successful = batch
        .successful
        .iter()
        .map(|entry| json!({ "Id": entry.id }))
        .collect::<Vec<_>>();

    json!({
        "Failed": json_batch_failures(&batch.failed),
        "Successful": successful,
    })
}

pub(super) fn json_change_visibility_batch_result(
    batch: &ChangeMessageVisibilityBatchOutput,
) -> Value {
    let successful = batch
        .successful
        .iter()
        .map(|entry| json!({ "Id": entry.id }))
        .collect::<Vec<_>>();

    json!({
        "Failed": json_batch_failures(&batch.failed),
        "Successful": successful,
    })
}

fn json_batch_failures(failed: &[BatchFailure]) -> Vec<Value> {
    failed
        .iter()
        .map(|failure| {
            json!({
                "Code": failure.code,
                "Id": failure.id,
                "Message": failure.message,
                "SenderFault": failure.sender_fault,
            })
        })
        .collect()
}
