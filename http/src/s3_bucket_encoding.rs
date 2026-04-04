use crate::runtime::EdgeResponse;
use crate::s3_response_encoding::{bool_text, iso_timestamp, object_xml};
use crate::xml::XmlBuilder;
use services::{
    BucketNotificationConfiguration, BucketObjectLockConfiguration,
    BucketTaggingOutput, BucketVersioningOutput, GetBucketLocationOutput,
    ListBucketsOutput, ListMultipartUploadsOutput, ListObjectVersionsOutput,
    ListObjectsOutput, ListObjectsV2Output, NotificationFilter, S3Scope,
};

const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub(crate) fn bucket_tagging_response(
    output: &BucketTaggingOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("Tagging", Some(S3_XMLNS))
        .start("TagSet", None);
    for (key, value) in &output.tags {
        xml = xml
            .start("Tag", None)
            .elem("Key", key)
            .elem("Value", value)
            .end("Tag");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("TagSet").end("Tagging").build().into_bytes(),
    )
}

pub(crate) fn bucket_versioning_response(
    output: &BucketVersioningOutput,
) -> EdgeResponse {
    let mut xml =
        XmlBuilder::new().start("VersioningConfiguration", Some(S3_XMLNS));
    if let Some(status) = output.status {
        xml = xml.elem("Status", status.as_str());
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("VersioningConfiguration").build().into_bytes(),
    )
}

pub(crate) fn notification_configuration_response(
    output: &BucketNotificationConfiguration,
) -> EdgeResponse {
    let mut xml =
        XmlBuilder::new().start("NotificationConfiguration", Some(S3_XMLNS));

    for queue in &output.queue_configurations {
        xml = xml.start("QueueConfiguration", None);
        if let Some(id) = queue.id.as_deref() {
            xml = xml.elem("Id", id);
        }
        xml = xml.elem("Queue", &queue.queue_arn.to_string());
        for event in &queue.events {
            xml = xml.elem("Event", event);
        }
        xml = append_notification_filter(xml, queue.filter.as_ref());
        xml = xml.end("QueueConfiguration");
    }

    for topic in &output.topic_configurations {
        xml = xml.start("TopicConfiguration", None);
        if let Some(id) = topic.id.as_deref() {
            xml = xml.elem("Id", id);
        }
        xml = xml.elem("Topic", &topic.topic_arn.to_string());
        for event in &topic.events {
            xml = xml.elem("Event", event);
        }
        xml = append_notification_filter(xml, topic.filter.as_ref());
        xml = xml.end("TopicConfiguration");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("NotificationConfiguration").build().into_bytes(),
    )
}

fn append_notification_filter(
    mut xml: XmlBuilder,
    filter: Option<&NotificationFilter>,
) -> XmlBuilder {
    let Some(filter) = filter else {
        return xml;
    };
    if filter.prefix.is_none() && filter.suffix.is_none() {
        return xml;
    }

    xml = xml.start("Filter", None).start("S3Key", None);
    if let Some(prefix) = filter.prefix.as_deref() {
        xml = xml
            .start("FilterRule", None)
            .elem("Name", "prefix")
            .elem("Value", prefix)
            .end("FilterRule");
    }
    if let Some(suffix) = filter.suffix.as_deref() {
        xml = xml
            .start("FilterRule", None)
            .elem("Name", "suffix")
            .elem("Value", suffix)
            .end("FilterRule");
    }
    xml.end("S3Key").end("Filter")
}

pub(crate) fn object_lock_configuration_response(
    output: &BucketObjectLockConfiguration,
) -> EdgeResponse {
    let mut xml =
        XmlBuilder::new().start("ObjectLockConfiguration", Some(S3_XMLNS));
    if output.object_lock_enabled {
        xml = xml.elem("ObjectLockEnabled", "Enabled");
    }
    if let Some(retention) = output.default_retention.as_ref() {
        xml = xml.start("Rule", None).start("DefaultRetention", None);
        xml = xml.elem("Mode", retention.mode.as_str());
        xml = match retention.period {
            services::DefaultRetentionPeriod::Days(days) => {
                xml.elem("Days", &days.to_string())
            }
            services::DefaultRetentionPeriod::Years(years) => {
                xml.elem("Years", &years.to_string())
            }
        };
        xml = xml.end("DefaultRetention").end("Rule");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ObjectLockConfiguration").build().into_bytes(),
    )
}

pub(crate) fn list_multipart_uploads_response(
    output: &ListMultipartUploadsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListMultipartUploadsResult", Some(S3_XMLNS))
        .elem("Bucket", &output.bucket);
    for upload in &output.uploads {
        xml = xml
            .start("Upload", None)
            .elem("Key", &upload.key)
            .elem("UploadId", &upload.upload_id)
            .elem("Initiated", &iso_timestamp(upload.initiated_epoch_seconds))
            .end("Upload");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListMultipartUploadsResult").build().into_bytes(),
    )
}

pub(crate) fn list_object_versions_response(
    output: &ListObjectVersionsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListVersionsResult", Some(S3_XMLNS))
        .elem("Name", &output.bucket)
        .elem("Prefix", output.prefix.as_deref().unwrap_or_default())
        .elem("KeyMarker", "")
        .elem("VersionIdMarker", "")
        .elem("MaxKeys", &output.max_keys.to_string())
        .elem("IsTruncated", bool_text(output.is_truncated));

    for entry in &output.entries {
        match entry {
            services::ListedVersionEntry::DeleteMarker(marker) => {
                xml = xml
                    .start("DeleteMarker", None)
                    .elem("Key", &marker.key)
                    .elem("VersionId", &marker.version_id)
                    .elem("IsLatest", bool_text(marker.is_latest))
                    .elem(
                        "LastModified",
                        &iso_timestamp(marker.last_modified_epoch_seconds),
                    )
                    .end("DeleteMarker");
            }
            services::ListedVersionEntry::Version(version) => {
                xml = xml
                    .start("Version", None)
                    .elem("Key", &version.key)
                    .elem("VersionId", &version.version_id)
                    .elem("IsLatest", bool_text(version.is_latest))
                    .elem(
                        "LastModified",
                        &iso_timestamp(version.last_modified_epoch_seconds),
                    )
                    .elem("ETag", &version.etag)
                    .elem("Size", &version.size.to_string())
                    .elem("StorageClass", &version.storage_class)
                    .end("Version");
            }
        }
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListVersionsResult").build().into_bytes(),
    )
}

pub(crate) fn get_bucket_location_response(
    location: &GetBucketLocationOutput,
) -> EdgeResponse {
    let value = if location.region == "us-east-1" {
        String::new()
    } else {
        location.region.clone()
    };
    let body = XmlBuilder::new()
        .start("LocationConstraint", Some(S3_XMLNS))
        .raw(&crate::xml::escape(&value))
        .end("LocationConstraint")
        .build()
        .into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

pub(crate) fn list_buckets_response(
    scope: &S3Scope,
    output: &ListBucketsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListAllMyBucketsResult", Some(S3_XMLNS))
        .start("Owner", None)
        .elem("ID", scope.account_id().as_str())
        .elem("DisplayName", scope.account_id().as_str())
        .end("Owner")
        .start("Buckets", None);

    for bucket in &output.buckets {
        xml = xml
            .start("Bucket", None)
            .elem("Name", &bucket.name)
            .elem(
                "CreationDate",
                &iso_timestamp(bucket.created_at_epoch_seconds),
            )
            .end("Bucket");
    }

    let body =
        xml.end("Buckets").end("ListAllMyBucketsResult").build().into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

pub(crate) fn list_objects_response(
    output: &ListObjectsOutput,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListBucketResult", Some(S3_XMLNS))
        .elem("Name", &output.bucket)
        .elem("Prefix", output.prefix.as_deref().unwrap_or_default())
        .elem("Marker", output.marker.as_deref().unwrap_or_default())
        .elem("MaxKeys", &output.max_keys.to_string())
        .elem("IsTruncated", bool_text(output.is_truncated));

    if let Some(delimiter) = output.delimiter.as_deref() {
        xml = xml.elem("Delimiter", delimiter);
    }

    if let Some(next_marker) = output.next_marker.as_deref() {
        xml = xml.elem("NextMarker", next_marker);
    }

    for object in &output.contents {
        xml = object_xml(xml, object);
    }
    for prefix in &output.common_prefixes {
        xml = xml
            .start("CommonPrefixes", None)
            .elem("Prefix", prefix)
            .end("CommonPrefixes");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListBucketResult").build().into_bytes(),
    )
}

pub(crate) fn list_objects_v2_response(
    output: &ListObjectsV2Output,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new()
        .start("ListBucketResult", Some(S3_XMLNS))
        .elem("Name", &output.bucket)
        .elem("Prefix", output.prefix.as_deref().unwrap_or_default())
        .elem("KeyCount", &output.key_count.to_string())
        .elem("MaxKeys", &output.max_keys.to_string())
        .elem("IsTruncated", bool_text(output.is_truncated));

    if let Some(token) = output.continuation_token.as_deref() {
        xml = xml.elem("ContinuationToken", token);
    }
    if let Some(token) = output.next_continuation_token.as_deref() {
        xml = xml.elem("NextContinuationToken", token);
    }
    if let Some(start_after) = output.start_after.as_deref() {
        xml = xml.elem("StartAfter", start_after);
    }
    if let Some(delimiter) = output.delimiter.as_deref() {
        xml = xml.elem("Delimiter", delimiter);
    }

    for object in &output.contents {
        xml = object_xml(xml, object);
    }
    for prefix in &output.common_prefixes {
        xml = xml
            .start("CommonPrefixes", None)
            .elem("Prefix", prefix)
            .end("CommonPrefixes");
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("ListBucketResult").build().into_bytes(),
    )
}
