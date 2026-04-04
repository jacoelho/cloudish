use crate::runtime::EdgeResponse;
use crate::s3_response_encoding::{
    empty_xml_response, iso_timestamp, rfc3339_timestamp,
};
use crate::xml::XmlBuilder;
use services::{
    DeleteObjectOutput, ObjectLegalHoldOutput, ObjectRetention,
    ObjectTaggingOutput,
};

const S3_XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub(crate) fn copy_object_response(
    etag: &str,
    last_modified_epoch_seconds: u64,
    version_id: Option<&str>,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("CopyObjectResult", Some(S3_XMLNS))
        .elem("ETag", etag)
        .elem("LastModified", &iso_timestamp(last_modified_epoch_seconds))
        .end("CopyObjectResult")
        .build()
        .into_bytes();

    let mut response = EdgeResponse::bytes(200, "application/xml", body);
    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

pub(crate) fn create_multipart_upload_response(
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("InitiateMultipartUploadResult", Some(S3_XMLNS))
        .elem("Bucket", bucket)
        .elem("Key", key)
        .elem("UploadId", upload_id)
        .end("InitiateMultipartUploadResult")
        .build()
        .into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

pub(crate) fn complete_multipart_upload_response(
    bucket: &str,
    key: &str,
    etag: &str,
    version_id: Option<&str>,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("CompleteMultipartUploadResult", Some(S3_XMLNS))
        .elem("Bucket", bucket)
        .elem("Key", key)
        .elem("ETag", etag)
        .end("CompleteMultipartUploadResult")
        .build()
        .into_bytes();

    let mut response = EdgeResponse::bytes(200, "application/xml", body);
    if let Some(version_id) = version_id {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

pub(crate) fn object_tagging_response(
    output: &ObjectTaggingOutput,
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

    let mut response = EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("TagSet").end("Tagging").build().into_bytes(),
    );
    if let Some(version_id) = output.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

pub(crate) fn tagging_write_response(
    status_code: u16,
    output: &ObjectTaggingOutput,
) -> EdgeResponse {
    let mut response = empty_xml_response(status_code);
    if let Some(version_id) = output.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}

pub(crate) fn object_retention_response(
    output: &ObjectRetention,
) -> EdgeResponse {
    let mut xml = XmlBuilder::new().start("Retention", Some(S3_XMLNS));
    if let Some(mode) = output.mode {
        xml = xml.elem("Mode", mode.as_str());
    }
    if let Some(retain_until_epoch_seconds) = output.retain_until_epoch_seconds
    {
        xml = xml.elem(
            "RetainUntilDate",
            &rfc3339_timestamp(retain_until_epoch_seconds),
        );
    }

    EdgeResponse::bytes(
        200,
        "application/xml",
        xml.end("Retention").build().into_bytes(),
    )
}

pub(crate) fn object_legal_hold_response(
    output: &ObjectLegalHoldOutput,
) -> EdgeResponse {
    let body = XmlBuilder::new()
        .start("LegalHold", Some(S3_XMLNS))
        .elem("Status", output.status.as_str())
        .end("LegalHold")
        .build()
        .into_bytes();

    EdgeResponse::bytes(200, "application/xml", body)
}

pub(crate) fn delete_object_response(
    output: &DeleteObjectOutput,
) -> EdgeResponse {
    let mut response = EdgeResponse::bytes(204, "application/xml", Vec::new());
    if output.delete_marker {
        response = response.set_header("x-amz-delete-marker", "true");
    }
    if let Some(version_id) = output.version_id.as_deref() {
        response = response.set_header("x-amz-version-id", version_id);
    }
    response
}
