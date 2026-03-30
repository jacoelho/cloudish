use aws::{Arn, AwsError, AwsErrorFamily, ServiceName};
use bytes::Bytes;
use quick_xml::de::from_reader;
use serde::Deserialize;
use services::{
    BucketNotificationConfiguration, BucketObjectLockConfiguration,
    BucketVersioningStatus, CompletedMultipartPart, CsvFileHeaderInfo,
    CsvInputSerialization, CsvOutputSerialization, DefaultObjectLockRetention,
    DefaultRetentionPeriod, LegalHoldStatus, NotificationFilter,
    ObjectLockMode, ObjectRetention, QueueNotificationConfiguration,
    SelectObjectContentInput, TopicNotificationConfiguration,
};
use std::collections::BTreeMap;
use std::io::Cursor;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) fn parse_bucket_versioning(
    body: Bytes,
) -> Result<BucketVersioningStatus, AwsError> {
    let document: VersioningConfigurationDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    match document.status.as_deref() {
        Some("Enabled") => Ok(BucketVersioningStatus::Enabled),
        Some("Suspended") => Ok(BucketVersioningStatus::Suspended),
        _ => Err(malformed_xml_error()),
    }
}

pub(crate) fn parse_tagging(
    body: Bytes,
) -> Result<BTreeMap<String, String>, AwsError> {
    let document: TaggingDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    let mut tags = BTreeMap::new();

    for tag in document.tag_set.tags {
        if tags.insert(tag.key.clone(), tag.value).is_some() {
            return Err(invalid_tag_error(
                "The TagSet may not contain duplicate tag keys.",
            ));
        }
    }

    Ok(tags)
}

pub(crate) fn parse_complete_multipart_upload(
    body: Bytes,
) -> Result<Vec<CompletedMultipartPart>, AwsError> {
    let document: CompleteMultipartUploadDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    if document.parts.is_empty() {
        return Err(malformed_xml_error());
    }

    Ok(document
        .parts
        .into_iter()
        .map(|part| CompletedMultipartPart {
            etag: part.etag,
            part_number: part.part_number,
        })
        .collect())
}

pub(crate) fn parse_bucket_notification_configuration(
    body: Bytes,
) -> Result<BucketNotificationConfiguration, AwsError> {
    let document: NotificationConfigurationDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    if !document.event_bridge_configurations.is_empty() {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "EventBridge bucket notifications are not supported.",
        ));
    }

    Ok(BucketNotificationConfiguration {
        queue_configurations: document
            .queue_configurations
            .into_iter()
            .map(parse_queue_notification_configuration)
            .collect::<Result<Vec<_>, _>>()?,
        topic_configurations: document
            .topic_configurations
            .into_iter()
            .map(parse_topic_notification_configuration)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub(crate) fn parse_object_lock_configuration(
    body: Bytes,
) -> Result<BucketObjectLockConfiguration, AwsError> {
    let document: ObjectLockConfigurationDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    let Some(enabled) = document.object_lock_enabled else {
        return Err(malformed_xml_error());
    };
    if enabled != "Enabled" {
        return Err(malformed_xml_error());
    }

    Ok(BucketObjectLockConfiguration {
        object_lock_enabled: true,
        default_retention: document
            .rule
            .map(parse_object_lock_rule)
            .transpose()?,
    })
}

pub(crate) fn parse_object_retention(
    body: Bytes,
) -> Result<ObjectRetention, AwsError> {
    let document: RetentionDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;

    Ok(ObjectRetention {
        mode: document.mode.map(parse_object_lock_mode).transpose()?,
        retain_until_epoch_seconds: document
            .retain_until_date
            .map(parse_rfc3339_epoch_seconds)
            .transpose()?,
    })
}

pub(crate) fn parse_object_legal_hold(
    body: Bytes,
) -> Result<LegalHoldStatus, AwsError> {
    let document: LegalHoldDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    let Some(status) = document.status else {
        return Err(malformed_xml_error());
    };
    parse_legal_hold_status(status)
}

pub(crate) fn parse_select_object_content_request(
    body: Bytes,
    bucket: String,
    key: String,
    version_id: Option<String>,
) -> Result<SelectObjectContentInput, AwsError> {
    let document: SelectObjectContentRequestDocument =
        from_reader(Cursor::new(body)).map_err(|_| malformed_xml_error())?;
    if document.scan_range.is_some() {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "Select scan ranges are not supported.",
        ));
    }
    let Some(expression) = document.expression else {
        return Err(malformed_xml_error());
    };
    if document.expression_type.as_deref() != Some("SQL") {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "Only SQL select expressions are supported.",
        ));
    }
    let input_serialization =
        document.input_serialization.ok_or_else(malformed_xml_error)?;
    let output_serialization =
        document.output_serialization.ok_or_else(malformed_xml_error)?;
    if input_serialization.compression_type.as_deref().unwrap_or("NONE")
        != "NONE"
    {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "Only uncompressed CSV select inputs are supported.",
        ));
    }
    if input_serialization.json.is_some()
        || input_serialization.parquet.is_some()
    {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "Only CSV select inputs are supported.",
        ));
    }
    if output_serialization.json.is_some() {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "Only CSV select outputs are supported.",
        ));
    }
    let csv_input = input_serialization.csv.ok_or_else(|| {
        invalid_argument_error(
            "InvalidArgument",
            "Only CSV select inputs are supported.",
        )
    })?;
    let csv_output = output_serialization.csv.ok_or_else(|| {
        invalid_argument_error(
            "InvalidArgument",
            "Only CSV select outputs are supported.",
        )
    })?;
    if csv_input.allow_quoted_record_delimiter.unwrap_or(false) {
        return Err(invalid_argument_error(
            "InvalidArgument",
            "Quoted record delimiters are not supported.",
        ));
    }

    Ok(SelectObjectContentInput {
        bucket,
        csv_input: CsvInputSerialization {
            comments: csv_input.comments.map(parse_single_char).transpose()?,
            field_delimiter: csv_input
                .field_delimiter
                .map(parse_single_char)
                .transpose()?
                .unwrap_or(','),
            file_header_info: csv_input
                .file_header_info
                .as_deref()
                .map(parse_csv_file_header_info)
                .transpose()?
                .unwrap_or(CsvFileHeaderInfo::None),
            quote_character: csv_input
                .quote_character
                .map(parse_single_char)
                .transpose()?
                .unwrap_or('"'),
            quote_escape_character: csv_input
                .quote_escape_character
                .map(parse_single_char)
                .transpose()?
                .unwrap_or('"'),
            record_delimiter: csv_input
                .record_delimiter
                .map(parse_single_char)
                .transpose()?
                .unwrap_or('\n'),
        },
        csv_output: CsvOutputSerialization {
            always_quote: matches!(
                csv_output.quote_fields.as_deref(),
                Some("ALWAYS")
            ),
            field_delimiter: csv_output
                .field_delimiter
                .map(parse_single_char)
                .transpose()?
                .unwrap_or(','),
            quote_character: csv_output
                .quote_character
                .map(parse_single_char)
                .transpose()?
                .unwrap_or('"'),
            quote_escape_character: csv_output
                .quote_escape_character
                .map(parse_single_char)
                .transpose()?
                .unwrap_or('"'),
            record_delimiter: csv_output
                .record_delimiter
                .map(parse_single_char)
                .transpose()?
                .unwrap_or('\n'),
        },
        expression,
        key,
        version_id,
    })
}

fn invalid_tag_error(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidTag",
        message,
        400,
        true,
    )
}

fn invalid_argument_error(
    code: &'static str,
    message: impl Into<String>,
) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        code,
        message,
        400,
        true,
    )
}

fn malformed_xml_error() -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema.",
        400,
        true,
    )
}

fn parse_queue_notification_configuration(
    document: QueueConfigurationDocument,
) -> Result<QueueNotificationConfiguration, AwsError> {
    let queue_arn = parse_service_arn(
        document.queue_arn.ok_or_else(malformed_xml_error)?,
        ServiceName::Sqs,
    )?;
    Ok(QueueNotificationConfiguration {
        events: document.events,
        filter: document.filter.map(parse_notification_filter).transpose()?,
        id: document.id,
        queue_arn,
    })
}

fn parse_topic_notification_configuration(
    document: TopicConfigurationDocument,
) -> Result<TopicNotificationConfiguration, AwsError> {
    let topic_arn = parse_service_arn(
        document.topic_arn.ok_or_else(malformed_xml_error)?,
        ServiceName::Sns,
    )?;
    Ok(TopicNotificationConfiguration {
        events: document.events,
        filter: document.filter.map(parse_notification_filter).transpose()?,
        id: document.id,
        topic_arn,
    })
}

fn parse_service_arn(
    value: String,
    service: ServiceName,
) -> Result<Arn, AwsError> {
    let arn = value.parse::<Arn>().map_err(|_| {
        invalid_argument_error(
            "InvalidArgument",
            format!("Unable to validate destination {value}."),
        )
    })?;
    if arn.service() != service {
        return Err(invalid_argument_error(
            "InvalidArgument",
            format!("Unable to validate destination {value}."),
        ));
    }
    Ok(arn)
}

fn parse_notification_filter(
    document: NotificationFilterDocument,
) -> Result<NotificationFilter, AwsError> {
    let Some(key_filter) = document.s3_key else {
        return Ok(NotificationFilter { prefix: None, suffix: None });
    };

    let mut prefix = None;
    let mut suffix = None;
    for rule in key_filter.rules {
        match rule.name.as_deref() {
            Some("prefix") if prefix.is_none() => prefix = rule.value,
            Some("suffix") if suffix.is_none() => suffix = rule.value,
            _ => return Err(malformed_xml_error()),
        }
    }

    Ok(NotificationFilter { prefix, suffix })
}

fn parse_object_lock_rule(
    document: ObjectLockRuleDocument,
) -> Result<DefaultObjectLockRetention, AwsError> {
    let retention =
        document.default_retention.ok_or_else(malformed_xml_error)?;
    let mode = parse_object_lock_mode(
        retention.mode.ok_or_else(malformed_xml_error)?,
    )?;
    let period = match (retention.days, retention.years) {
        (Some(days), None) if days > 0 => {
            DefaultRetentionPeriod::Days(days as u32)
        }
        (None, Some(years)) if years > 0 => {
            DefaultRetentionPeriod::Years(years as u32)
        }
        _ => return Err(malformed_xml_error()),
    };

    Ok(DefaultObjectLockRetention { mode, period })
}

fn parse_object_lock_mode(value: String) -> Result<ObjectLockMode, AwsError> {
    match value.as_str() {
        "COMPLIANCE" => Ok(ObjectLockMode::Compliance),
        "GOVERNANCE" => Ok(ObjectLockMode::Governance),
        _ => Err(invalid_argument_error(
            "InvalidArgument",
            "Unknown wormMode directive.",
        )),
    }
}

fn parse_legal_hold_status(
    value: String,
) -> Result<LegalHoldStatus, AwsError> {
    match value.as_str() {
        "ON" => Ok(LegalHoldStatus::On),
        "OFF" => Ok(LegalHoldStatus::Off),
        _ => Err(malformed_xml_error()),
    }
}

fn parse_rfc3339_epoch_seconds(value: String) -> Result<u64, AwsError> {
    let parsed = OffsetDateTime::parse(&value, &Rfc3339)
        .map_err(|_| malformed_xml_error())?;
    Ok(parsed.unix_timestamp().max(0) as u64)
}

fn parse_single_char(value: String) -> Result<char, AwsError> {
    let mut characters = value.chars();
    match (characters.next(), characters.next()) {
        (Some(character), None) => Ok(character),
        _ => Err(invalid_argument_error(
            "InvalidArgument",
            "CSV select delimiters must be a single character.",
        )),
    }
}

fn parse_csv_file_header_info(
    value: &str,
) -> Result<CsvFileHeaderInfo, AwsError> {
    match value {
        "IGNORE" => Ok(CsvFileHeaderInfo::Ignore),
        "NONE" => Ok(CsvFileHeaderInfo::None),
        "USE" => Ok(CsvFileHeaderInfo::Use),
        _ => Err(malformed_xml_error()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename = "VersioningConfiguration")]
struct VersioningConfigurationDocument {
    #[serde(rename = "Status")]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Tagging")]
struct TaggingDocument {
    #[serde(rename = "TagSet")]
    tag_set: TagSetDocument,
}

#[derive(Debug, Deserialize)]
struct TagSetDocument {
    #[serde(rename = "Tag", default)]
    tags: Vec<TagDocument>,
}

#[derive(Debug, Deserialize)]
struct TagDocument {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct CompleteMultipartUploadDocument {
    #[serde(rename = "Part", default)]
    parts: Vec<CompleteMultipartPartDocument>,
}

#[derive(Debug, Deserialize)]
struct CompleteMultipartPartDocument {
    #[serde(rename = "ETag")]
    etag: Option<String>,
    #[serde(rename = "PartNumber")]
    part_number: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "NotificationConfiguration")]
struct NotificationConfigurationDocument {
    #[serde(rename = "QueueConfiguration", default)]
    queue_configurations: Vec<QueueConfigurationDocument>,
    #[serde(rename = "TopicConfiguration", default)]
    topic_configurations: Vec<TopicConfigurationDocument>,
    #[serde(rename = "EventBridgeConfiguration", default)]
    event_bridge_configurations: Vec<EventBridgeConfigurationDocument>,
}

#[derive(Debug, Deserialize)]
struct QueueConfigurationDocument {
    #[serde(rename = "Event", default)]
    events: Vec<String>,
    #[serde(rename = "Filter")]
    filter: Option<NotificationFilterDocument>,
    #[serde(rename = "Id")]
    id: Option<String>,
    #[serde(rename = "Queue")]
    queue_arn: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopicConfigurationDocument {
    #[serde(rename = "Event", default)]
    events: Vec<String>,
    #[serde(rename = "Filter")]
    filter: Option<NotificationFilterDocument>,
    #[serde(rename = "Id")]
    id: Option<String>,
    #[serde(rename = "Topic")]
    topic_arn: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventBridgeConfigurationDocument {}

#[derive(Debug, Deserialize)]
struct NotificationFilterDocument {
    #[serde(rename = "S3Key")]
    s3_key: Option<S3KeyFilterDocument>,
}

#[derive(Debug, Deserialize)]
struct S3KeyFilterDocument {
    #[serde(rename = "FilterRule", default)]
    rules: Vec<FilterRuleDocument>,
}

#[derive(Debug, Deserialize)]
struct FilterRuleDocument {
    #[serde(rename = "Name")]
    name: Option<String>,
    #[serde(rename = "Value")]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "ObjectLockConfiguration")]
struct ObjectLockConfigurationDocument {
    #[serde(rename = "ObjectLockEnabled")]
    object_lock_enabled: Option<String>,
    #[serde(rename = "Rule")]
    rule: Option<ObjectLockRuleDocument>,
}

#[derive(Debug, Deserialize)]
struct ObjectLockRuleDocument {
    #[serde(rename = "DefaultRetention")]
    default_retention: Option<DefaultRetentionDocument>,
}

#[derive(Debug, Deserialize)]
struct DefaultRetentionDocument {
    #[serde(rename = "Days")]
    days: Option<i32>,
    #[serde(rename = "Mode")]
    mode: Option<String>,
    #[serde(rename = "Years")]
    years: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Retention")]
struct RetentionDocument {
    #[serde(rename = "Mode")]
    mode: Option<String>,
    #[serde(rename = "RetainUntilDate")]
    retain_until_date: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "LegalHold")]
struct LegalHoldDocument {
    #[serde(rename = "Status")]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "SelectObjectContentRequest")]
struct SelectObjectContentRequestDocument {
    #[serde(rename = "Expression")]
    expression: Option<String>,
    #[serde(rename = "ExpressionType")]
    expression_type: Option<String>,
    #[serde(rename = "InputSerialization")]
    input_serialization: Option<SelectInputSerializationDocument>,
    #[serde(rename = "OutputSerialization")]
    output_serialization: Option<SelectOutputSerializationDocument>,
    #[serde(rename = "ScanRange")]
    scan_range: Option<SelectScanRangeDocument>,
}

#[derive(Debug, Deserialize)]
struct SelectInputSerializationDocument {
    #[serde(rename = "CompressionType")]
    compression_type: Option<String>,
    #[serde(rename = "CSV")]
    csv: Option<SelectCsvInputDocument>,
    #[serde(rename = "JSON")]
    json: Option<SelectJsonDocument>,
    #[serde(rename = "Parquet")]
    parquet: Option<SelectParquetDocument>,
}

#[derive(Debug, Deserialize)]
struct SelectOutputSerializationDocument {
    #[serde(rename = "CSV")]
    csv: Option<SelectCsvOutputDocument>,
    #[serde(rename = "JSON")]
    json: Option<SelectJsonDocument>,
}

#[derive(Debug, Deserialize)]
struct SelectCsvInputDocument {
    #[serde(rename = "AllowQuotedRecordDelimiter")]
    allow_quoted_record_delimiter: Option<bool>,
    #[serde(rename = "Comments")]
    comments: Option<String>,
    #[serde(rename = "FieldDelimiter")]
    field_delimiter: Option<String>,
    #[serde(rename = "FileHeaderInfo")]
    file_header_info: Option<String>,
    #[serde(rename = "QuoteCharacter")]
    quote_character: Option<String>,
    #[serde(rename = "QuoteEscapeCharacter")]
    quote_escape_character: Option<String>,
    #[serde(rename = "RecordDelimiter")]
    record_delimiter: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SelectCsvOutputDocument {
    #[serde(rename = "FieldDelimiter")]
    field_delimiter: Option<String>,
    #[serde(rename = "QuoteCharacter")]
    quote_character: Option<String>,
    #[serde(rename = "QuoteEscapeCharacter")]
    quote_escape_character: Option<String>,
    #[serde(rename = "QuoteFields")]
    quote_fields: Option<String>,
    #[serde(rename = "RecordDelimiter")]
    record_delimiter: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SelectJsonDocument {}

#[derive(Debug, Deserialize)]
struct SelectParquetDocument {}

#[derive(Debug, Deserialize)]
struct SelectScanRangeDocument {}

#[cfg(test)]
mod tests {
    use super::{
        parse_bucket_versioning, parse_complete_multipart_upload,
        parse_tagging,
    };
    use bytes::Bytes;
    use services::BucketVersioningStatus;

    #[test]
    fn s3_advanced_xml_parses_versioning_payloads() {
        let status = parse_bucket_versioning(Bytes::from_static(
            br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#,
        ))
        .expect("versioning payload should parse");

        assert_eq!(status, BucketVersioningStatus::Enabled);
    }

    #[test]
    fn s3_advanced_xml_parses_tag_sets() {
        let tags = parse_tagging(Bytes::from_static(
            br#"<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet><Tag><Key>env</Key><Value>dev</Value></Tag></TagSet></Tagging>"#,
        ))
        .expect("tagging payload should parse");

        assert_eq!(tags.get("env"), Some(&"dev".to_owned()));
    }

    #[test]
    fn s3_advanced_xml_parses_completed_parts() {
        let parts = parse_complete_multipart_upload(Bytes::from_static(
            br#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><ETag>"etag-1"</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#,
        ))
        .expect("complete multipart payload should parse");

        assert_eq!(parts[0].part_number, 1);
        assert_eq!(parts[0].etag.as_deref(), Some("\"etag-1\""));
    }
}
