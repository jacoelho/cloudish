use super::message_attributes::query_message_attribute_value_xml;
use crate::xml::XmlBuilder;
use services::{
    BatchFailure, ChangeMessageVisibilityBatchOutput,
    DeleteMessageBatchOutput, ListMessageMoveTasksOutput, ReceivedMessage,
    SendMessageBatchOutput,
};

pub(super) fn query_list_message_move_tasks_xml(
    tasks: &ListMessageMoveTasksOutput,
) -> String {
    let mut xml = XmlBuilder::new();
    for task in &tasks.results {
        xml = xml.start("Result", None);
        if let Some(task_handle) = task.task_handle.as_deref() {
            xml = xml.elem("TaskHandle", task_handle);
        }
        xml = xml
            .elem("Status", &task.status)
            .elem("SourceArn", &task.source_arn)
            .elem(
                "ApproximateNumberOfMessagesMoved",
                &task.approximate_number_of_messages_moved.to_string(),
            )
            .elem("StartedTimestamp", &task.started_timestamp.to_string());
        if let Some(destination_arn) = task.destination_arn.as_deref() {
            xml = xml.elem("DestinationArn", destination_arn);
        }
        if let Some(max_number_of_messages_per_second) =
            task.max_number_of_messages_per_second
        {
            xml = xml.elem(
                "MaxNumberOfMessagesPerSecond",
                &max_number_of_messages_per_second.to_string(),
            );
        }
        if let Some(approximate_number_of_messages_to_move) =
            task.approximate_number_of_messages_to_move
        {
            xml = xml.elem(
                "ApproximateNumberOfMessagesToMove",
                &approximate_number_of_messages_to_move.to_string(),
            );
        }
        if let Some(failure_reason) = task.failure_reason.as_deref() {
            xml = xml.elem("FailureReason", failure_reason);
        }
        xml = xml.end("Result");
    }

    xml.build()
}

pub(super) fn query_received_messages_xml(
    messages: &[ReceivedMessage],
) -> String {
    let mut xml = XmlBuilder::new();
    for message in messages {
        xml = xml
            .start("Message", None)
            .elem("MessageId", &message.message_id)
            .elem("ReceiptHandle", &message.receipt_handle)
            .elem("MD5OfBody", &message.md5_of_body)
            .elem("Body", &message.body);
        if let Some(md5_of_message_attributes) =
            message.md5_of_message_attributes.as_deref()
        {
            xml =
                xml.elem("MD5OfMessageAttributes", md5_of_message_attributes);
        }

        for (name, value) in &message.attributes {
            xml = xml
                .start("Attribute", None)
                .elem("Name", name)
                .elem("Value", value)
                .end("Attribute");
        }
        for (name, value) in &message.message_attributes {
            xml = xml
                .start("MessageAttribute", None)
                .elem("Name", name)
                .raw(&query_message_attribute_value_xml(value))
                .end("MessageAttribute");
        }

        xml = xml.end("Message");
    }

    xml.build()
}

pub(super) fn query_send_message_batch_result_xml(
    batch: &SendMessageBatchOutput,
) -> String {
    let mut xml = XmlBuilder::new();
    for entry in &batch.successful {
        xml = xml
            .start("SendMessageBatchResultEntry", None)
            .elem("Id", &entry.id)
            .elem("MessageId", &entry.message_id)
            .elem("MD5OfMessageBody", &entry.md5_of_message_body);
        if let Some(md5_of_message_attributes) =
            &entry.md5_of_message_attributes
        {
            xml =
                xml.elem("MD5OfMessageAttributes", md5_of_message_attributes);
        }
        if let Some(md5_of_message_system_attributes) =
            &entry.md5_of_message_system_attributes
        {
            xml = xml.elem(
                "MD5OfMessageSystemAttributes",
                md5_of_message_system_attributes,
            );
        }
        if let Some(sequence_number) = &entry.sequence_number {
            xml = xml.elem("SequenceNumber", sequence_number);
        }
        xml = xml.end("SendMessageBatchResultEntry");
    }

    xml.raw(&query_batch_failures_xml(&batch.failed)).build()
}

pub(super) fn query_delete_message_batch_result_xml(
    batch: &DeleteMessageBatchOutput,
) -> String {
    let mut xml = XmlBuilder::new();
    for entry in &batch.successful {
        xml = xml
            .start("DeleteMessageBatchResultEntry", None)
            .elem("Id", &entry.id)
            .end("DeleteMessageBatchResultEntry");
    }

    xml.raw(&query_batch_failures_xml(&batch.failed)).build()
}

pub(super) fn query_change_visibility_batch_result_xml(
    batch: &ChangeMessageVisibilityBatchOutput,
) -> String {
    let mut xml = XmlBuilder::new();
    for entry in &batch.successful {
        xml = xml
            .start("ChangeMessageVisibilityBatchResultEntry", None)
            .elem("Id", &entry.id)
            .end("ChangeMessageVisibilityBatchResultEntry");
    }

    xml.raw(&query_batch_failures_xml(&batch.failed)).build()
}

fn query_batch_failures_xml(failures: &[BatchFailure]) -> String {
    let mut xml = XmlBuilder::new();
    for failure in failures {
        xml = xml
            .start("BatchResultErrorEntry", None)
            .elem("Id", &failure.id)
            .elem("SenderFault", &failure.sender_fault.to_string())
            .elem("Code", &failure.code);
        if !failure.message.is_empty() {
            xml = xml.elem("Message", &failure.message);
        }
        xml = xml.end("BatchResultErrorEntry");
    }

    xml.build()
}
