use crate::runtime::EdgeResponse;
use aws::{AwsError, AwsErrorFamily};
use services::{LegalHoldStatus, ObjectLockMode};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) fn invalid_request_error(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidRequest",
        message,
        400,
        true,
    )
}

pub(crate) fn empty_xml_response(status_code: u16) -> EdgeResponse {
    EdgeResponse::bytes(status_code, "application/xml", Vec::new())
}

pub(crate) fn iso_timestamp(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .and_then(|formatted| {
            formatted.strip_suffix('Z').map(|value| format!("{value}.000Z"))
        })
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_owned())
}

pub(crate) fn object_xml(
    mut xml: crate::xml::XmlBuilder,
    object: &services::ListedObject,
) -> crate::xml::XmlBuilder {
    xml = xml
        .start("Contents", None)
        .elem("Key", &object.key)
        .elem(
            "LastModified",
            &iso_timestamp(object.last_modified_epoch_seconds),
        )
        .elem("ETag", &object.etag)
        .elem("Size", &object.size.to_string())
        .elem("StorageClass", &object.storage_class)
        .end("Contents");
    xml
}

pub(crate) fn bool_text(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

pub(crate) fn apply_object_lock_headers(
    mut response: EdgeResponse,
    mode: Option<ObjectLockMode>,
    retain_until_epoch_seconds: Option<u64>,
    legal_hold_status: Option<LegalHoldStatus>,
) -> EdgeResponse {
    if let Some(mode) = mode {
        response =
            response.set_header("x-amz-object-lock-mode", mode.as_str());
    }
    if let Some(retain_until_epoch_seconds) = retain_until_epoch_seconds {
        response = response.set_header(
            "x-amz-object-lock-retain-until-date",
            rfc3339_timestamp(retain_until_epoch_seconds),
        );
    }
    if let Some(status) = legal_hold_status {
        response = response
            .set_header("x-amz-object-lock-legal-hold", status.as_str());
    }
    response
}

pub(crate) fn rfc3339_timestamp(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}
