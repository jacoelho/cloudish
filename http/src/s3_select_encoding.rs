use aws::{AwsError, AwsErrorFamily};
use aws_smithy_eventstream::error::Error;
use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};

use crate::runtime::EdgeResponse;

const REQUEST_ID: &str = "0000000000000000";

pub(crate) fn select_object_content_response(
    output: &services::SelectObjectContentOutput,
) -> Result<EdgeResponse, AwsError> {
    let body = encode_select_object_content_body(output)?;
    Ok(EdgeResponse::bytes(200, "application/vnd.amazon.eventstream", body)
        .set_header("x-amz-request-id", REQUEST_ID)
        .set_header("x-amz-id-2", REQUEST_ID))
}

fn encode_select_object_content_body(
    output: &services::SelectObjectContentOutput,
) -> Result<Vec<u8>, AwsError> {
    let mut body = Vec::new();
    for chunk in [output.records.as_bytes()] {
        write_message_to(
            &event_stream_message(
                "Records",
                Some("application/octet-stream"),
                chunk.to_vec(),
            ),
            &mut body,
        )
        .map_err(select_object_content_encode_error)?;
    }

    write_message_to(
        &event_stream_message(
            "Stats",
            Some("text/xml"),
            select_stats_xml(&output.stats).into_bytes(),
        ),
        &mut body,
    )
    .map_err(select_object_content_encode_error)?;
    write_message_to(
        &event_stream_message("End", None, Vec::new()),
        &mut body,
    )
    .map_err(select_object_content_encode_error)?;

    Ok(body)
}

fn select_object_content_encode_error(error: Error) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "SerializationError",
        format!("Failed to encode SelectObjectContent event stream: {error}"),
        500,
        false,
    )
}

fn event_stream_message(
    event_type: &str,
    content_type: Option<&str>,
    payload: Vec<u8>,
) -> Message {
    let mut headers = vec![
        Header::new(
            ":message-type",
            HeaderValue::String("event".to_owned().into()),
        ),
        Header::new(
            ":event-type",
            HeaderValue::String(event_type.to_owned().into()),
        ),
    ];
    if let Some(content_type) = content_type {
        headers.push(Header::new(
            ":content-type",
            HeaderValue::String(content_type.to_owned().into()),
        ));
    }

    Message::new_from_parts(headers, payload)
}

fn select_stats_xml(stats: &services::SelectObjectStats) -> String {
    format!(
        "<Stats><BytesScanned>{}</BytesScanned><BytesProcessed>{}</BytesProcessed><BytesReturned>{}</BytesReturned></Stats>",
        stats.bytes_scanned, stats.bytes_processed, stats.bytes_returned
    )
}
