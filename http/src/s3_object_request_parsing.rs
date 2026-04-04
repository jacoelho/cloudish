use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use crate::s3_request_parsing::malformed_xml_error;
use aws::AwsError;
use services::S3Error;
use std::collections::BTreeMap;

pub(crate) fn request_metadata(
    request: &HttpRequest<'_>,
) -> BTreeMap<String, String> {
    request
        .headers()
        .filter_map(|(name, value)| {
            name.strip_prefix("x-amz-meta-")
                .map(|name| (name.to_ascii_lowercase(), value.to_owned()))
        })
        .collect()
}

pub(crate) fn request_tags(
    request: &HttpRequest<'_>,
) -> Result<BTreeMap<String, String>, AwsError> {
    let mut tags = BTreeMap::new();
    let Some(header) = request.header("x-amz-tagging") else {
        return Ok(tags);
    };

    for pair in header.split('&') {
        let (key, value) = pair.split_once('=').ok_or_else(|| {
            S3Error::InvalidArgument {
                code: "InvalidTag",
                message: "The object tagging header is malformed.".to_owned(),
                status_code: 400,
            }
            .to_aws_error()
        })?;
        tags.insert(
            percent_decode_tag_component(key)?,
            percent_decode_tag_component(value)?,
        );
    }

    Ok(tags)
}

fn percent_decode_tag_component(value: &str) -> Result<String, AwsError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'+' => {
                decoded.push(b' ');
                index =
                    index.checked_add(1).ok_or_else(malformed_xml_error)?;
            }
            b'%' => {
                let Some(high_index) = index.checked_add(1) else {
                    return Err(malformed_xml_error());
                };
                let Some(&high_byte) = bytes.get(high_index) else {
                    return Err(malformed_xml_error());
                };
                let Some(low_index) = index.checked_add(2) else {
                    return Err(malformed_xml_error());
                };
                let Some(&low_byte) = bytes.get(low_index) else {
                    return Err(malformed_xml_error());
                };
                decoded.push((hex(high_byte)? << 4) | hex(low_byte)?);
                index =
                    index.checked_add(3).ok_or_else(malformed_xml_error)?;
            }
            other => {
                decoded.push(other);
                index =
                    index.checked_add(1).ok_or_else(malformed_xml_error)?;
            }
        }
    }

    String::from_utf8(decoded).map_err(|_| malformed_xml_error())
}

fn hex(value: u8) -> Result<u8, AwsError> {
    char::from(value)
        .to_digit(16)
        .and_then(|digit| u8::try_from(digit).ok())
        .ok_or_else(malformed_xml_error)
}
