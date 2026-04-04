use crate::errors::SqsError;
use std::collections::BTreeMap;

const MAX_MESSAGE_ATTRIBUTES: usize = 10;
const MAX_MESSAGE_ATTRIBUTE_NAME_LENGTH: usize = 256;
const MESSAGE_ATTRIBUTE_TRANSPORT_STRING: u8 = 1;
const MESSAGE_ATTRIBUTE_TRANSPORT_BINARY: u8 = 2;
const UNSUPPORTED_LIST_VALUE_MESSAGE: &str =
    "StringListValues and BinaryListValues are not supported.";
const INVALID_TRACE_HEADER_MESSAGE: &str =
    "Value AWSTraceHeader for parameter MessageSystemAttributes is invalid.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageAttributeLogicalType {
    Binary,
    Number,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedMessageAttributeDataType {
    custom_label: Option<String>,
    logical_type: MessageAttributeLogicalType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAttributeValue {
    pub binary_list_values: Vec<Vec<u8>>,
    pub binary_value: Option<Vec<u8>>,
    pub data_type: String,
    pub string_list_values: Vec<String>,
    pub string_value: Option<String>,
}

pub(crate) fn validate_message_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
    validate_string: impl Fn(&str) -> Result<(), SqsError>,
) -> Result<(), SqsError> {
    if attributes.len() > MAX_MESSAGE_ATTRIBUTES {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Number of message attributes [{count}] exceeds the allowed maximum [{MAX_MESSAGE_ATTRIBUTES}].",
                count = attributes.len()
            ),
        });
    }
    for (name, value) in attributes {
        validate_message_attribute_name(name)?;
        validate_message_attribute_value(value, &validate_string)?;
    }

    Ok(())
}

pub(crate) fn validate_message_system_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
    validate_string: impl Fn(&str) -> Result<(), SqsError>,
) -> Result<(), SqsError> {
    for (name, value) in attributes {
        if name != "AWSTraceHeader" {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {name} for parameter MessageSystemAttributes is invalid."
                ),
            });
        }
        let parsed =
            validate_message_attribute_value(value, &validate_string)?;
        if parsed.logical_type != MessageAttributeLogicalType::String
            || parsed.custom_label.is_some()
        {
            return Err(SqsError::InvalidParameterValue {
                message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
            });
        }
        let Some(string_value) = value.string_value.as_deref() else {
            return Err(SqsError::InvalidParameterValue {
                message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
            });
        };
        validate_trace_header(string_value)?;
    }

    Ok(())
}

pub(crate) fn message_attribute_size(
    name: &str,
    value: &MessageAttributeValue,
) -> usize {
    let mut size = name.len().saturating_add(value.data_type.len());
    if let Some(string_value) = value.string_value.as_deref() {
        size = size.saturating_add(string_value.len());
    }
    if let Some(binary_value) = value.binary_value.as_ref() {
        size = size.saturating_add(binary_value.len());
    }
    size = size.saturating_add(
        value.string_list_values.iter().map(String::len).sum::<usize>(),
    );
    size.saturating_add(
        value.binary_list_values.iter().map(Vec::len).sum::<usize>(),
    )
}

pub(crate) fn selected_message_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
    selectors: &[String],
) -> BTreeMap<String, MessageAttributeValue> {
    if selectors.is_empty() {
        return BTreeMap::new();
    }
    if matches!(selectors.first(), Some(selector) if selector == "All") {
        return attributes.clone();
    }

    attributes
        .iter()
        .filter(|(name, _)| {
            selectors.iter().any(|selector| {
                selector == *name
                    || selector.strip_suffix(".*").is_some_and(|prefix| {
                        name.strip_prefix(prefix)
                            .is_some_and(|suffix| suffix.starts_with('.'))
                    })
            })
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub(crate) fn md5_of_message_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
) -> Option<String> {
    md5_of_attribute_map(attributes)
}

pub(crate) fn md5_of_message_system_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
) -> Option<String> {
    md5_of_attribute_map(attributes)
}

fn md5_of_attribute_map(
    attributes: &BTreeMap<String, MessageAttributeValue>,
) -> Option<String> {
    if attributes.is_empty() {
        return None;
    }
    let mut bytes = Vec::new();
    for (name, value) in attributes {
        write_attribute_bytes(&mut bytes, name, value);
    }

    Some(format!("{:x}", md5::compute(bytes)))
}

pub(crate) fn validate_message_attribute_name(
    name: &str,
) -> Result<(), SqsError> {
    let lowercase_name = name.to_ascii_lowercase();
    if name.is_empty()
        || name.len() > MAX_MESSAGE_ATTRIBUTE_NAME_LENGTH
        || name.starts_with('.')
        || name.ends_with('.')
        || name.contains("..")
        || lowercase_name.starts_with("aws.")
        || lowercase_name.starts_with("amazon.")
        || !name.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '-' | '.')
        })
    {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {name} for parameter MessageAttributeName is invalid."
            ),
        });
    }

    Ok(())
}

fn validate_message_attribute_value(
    value: &MessageAttributeValue,
    validate_string: &impl Fn(&str) -> Result<(), SqsError>,
) -> Result<ParsedMessageAttributeDataType, SqsError> {
    let parsed =
        parse_message_attribute_data_type(&value.data_type, validate_string)?;
    let value_count = [
        value.string_value.is_some(),
        value.binary_value.is_some(),
        !value.string_list_values.is_empty(),
        !value.binary_list_values.is_empty(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if value_count != 1 {
        return Err(SqsError::InvalidParameterValue {
            message: "Exactly one message attribute value field must be set."
                .to_owned(),
        });
    }
    if !value.string_list_values.is_empty()
        || !value.binary_list_values.is_empty()
    {
        return Err(SqsError::InvalidParameterValue {
            message: UNSUPPORTED_LIST_VALUE_MESSAGE.to_owned(),
        });
    }

    match parsed.logical_type {
        MessageAttributeLogicalType::Binary => {
            if value.binary_value.is_none() {
                return Err(SqsError::InvalidParameterValue {
                    message: format!(
                        "Value {} for parameter DataType is invalid.",
                        value.data_type
                    ),
                });
            }
        }
        MessageAttributeLogicalType::Number => {
            let Some(string_value) = value.string_value.as_deref() else {
                return Err(SqsError::InvalidParameterValue {
                    message: format!(
                        "Value {} for parameter DataType is invalid.",
                        value.data_type
                    ),
                });
            };
            validate_string(string_value)?;
            validate_number_attribute_value(string_value)?;
        }
        MessageAttributeLogicalType::String => {
            let Some(string_value) = value.string_value.as_deref() else {
                return Err(SqsError::InvalidParameterValue {
                    message: format!(
                        "Value {} for parameter DataType is invalid.",
                        value.data_type
                    ),
                });
            };
            validate_string(string_value)?;
        }
    }

    Ok(parsed)
}

fn parse_message_attribute_data_type(
    data_type: &str,
    validate_string: &impl Fn(&str) -> Result<(), SqsError>,
) -> Result<ParsedMessageAttributeDataType, SqsError> {
    if data_type.trim().is_empty() {
        return Err(SqsError::InvalidParameterValue {
            message: "Message attribute data types must not be empty."
                .to_owned(),
        });
    }
    if data_type.len() > MAX_MESSAGE_ATTRIBUTE_NAME_LENGTH {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {data_type} for parameter DataType is invalid."
            ),
        });
    }
    validate_string(data_type)?;

    let (logical_type, custom_label) = match data_type.split_once('.') {
        Some((logical_type, custom_label)) if !custom_label.is_empty() => {
            validate_data_type_custom_label(data_type, custom_label)?;
            (logical_type, Some(custom_label.to_owned()))
        }
        Some(_) => {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {data_type} for parameter DataType is invalid."
                ),
            });
        }
        None => (data_type, None),
    };

    let logical_type = match logical_type {
        "Binary" => MessageAttributeLogicalType::Binary,
        "Number" => MessageAttributeLogicalType::Number,
        "String" => MessageAttributeLogicalType::String,
        _ => {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {data_type} for parameter DataType is invalid."
                ),
            });
        }
    };

    Ok(ParsedMessageAttributeDataType { custom_label, logical_type })
}

fn validate_data_type_custom_label(
    data_type: &str,
    label: &str,
) -> Result<(), SqsError> {
    if label.starts_with('.')
        || label.ends_with('.')
        || label.contains("..")
        || !label.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '-' | '.')
        })
    {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {data_type} for parameter DataType is invalid."
            ),
        });
    }

    Ok(())
}

fn validate_number_attribute_value(value: &str) -> Result<(), SqsError> {
    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent = index
                .checked_add(1)
                .and_then(|start| value.get(start..))
                .ok_or_else(|| SqsError::InvalidParameterValue {
                    message: format!(
                        "Value {value} for parameter StringValue is invalid."
                    ),
                })?;
            (&value[..index], Some(exponent))
        }
        None => (value, None),
    };
    let mantissa = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    if mantissa.is_empty() {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter StringValue is invalid."
            ),
        });
    }

    let (integer_part, fraction_part) = match mantissa.split_once('.') {
        Some((integer_part, fraction_part)) => (integer_part, fraction_part),
        None => (mantissa, ""),
    };
    if integer_part.is_empty() && fraction_part.is_empty() {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter StringValue is invalid."
            ),
        });
    }
    if !integer_part.chars().all(|character| character.is_ascii_digit())
        || !fraction_part.chars().all(|character| character.is_ascii_digit())
    {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter StringValue is invalid."
            ),
        });
    }

    let digits = integer_part
        .chars()
        .chain(fraction_part.chars())
        .filter(|character| character.is_ascii_digit())
        .count();
    if digits == 0 || digits > 38 {
        return Err(SqsError::InvalidParameterValue {
            message: format!(
                "Value {value} for parameter StringValue is invalid."
            ),
        });
    }

    if let Some(exponent) = exponent {
        let exponent = exponent.parse::<i32>().map_err(|_| {
            SqsError::InvalidParameterValue {
                message: format!(
                    "Value {value} for parameter StringValue is invalid."
                ),
            }
        })?;
        if !(-128..=126).contains(&exponent) {
            return Err(SqsError::InvalidParameterValue {
                message: format!(
                    "Value {value} for parameter StringValue is invalid."
                ),
            });
        }
    }

    Ok(())
}

fn validate_trace_header(value: &str) -> Result<(), SqsError> {
    let mut root_found = false;

    for segment in value.split(';') {
        let Some((key, segment_value)) = segment.split_once('=') else {
            return Err(SqsError::InvalidParameterValue {
                message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
            });
        };
        if key.is_empty() || segment_value.is_empty() {
            return Err(SqsError::InvalidParameterValue {
                message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
            });
        }
        match key {
            "Root" => {
                root_found = validate_trace_root(segment_value);
                if !root_found {
                    return Err(SqsError::InvalidParameterValue {
                        message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
                    });
                }
            }
            "Parent" => {
                if !is_lower_hex(segment_value, 16) {
                    return Err(SqsError::InvalidParameterValue {
                        message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
                    });
                }
            }
            "Sampled" => {
                if !matches!(segment_value, "0" | "1" | "?") {
                    return Err(SqsError::InvalidParameterValue {
                        message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
                    });
                }
            }
            _ => {}
        }
    }

    if root_found {
        Ok(())
    } else {
        Err(SqsError::InvalidParameterValue {
            message: INVALID_TRACE_HEADER_MESSAGE.to_owned(),
        })
    }
}

fn validate_trace_root(value: &str) -> bool {
    let mut segments = value.split('-');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next()
        ),
        (Some("1"), Some(timestamp), Some(identifier), None)
            if is_lower_hex(timestamp, 8) && is_lower_hex(identifier, 24)
    )
}

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn write_attribute_bytes(
    bytes: &mut Vec<u8>,
    name: &str,
    value: &MessageAttributeValue,
) {
    write_bytes(bytes, name.as_bytes());
    write_bytes(bytes, value.data_type.as_bytes());
    if let Some(string_value) = value.string_value.as_deref() {
        bytes.push(MESSAGE_ATTRIBUTE_TRANSPORT_STRING);
        write_bytes(bytes, string_value.as_bytes());
    } else if let Some(binary_value) = value.binary_value.as_ref() {
        bytes.push(MESSAGE_ATTRIBUTE_TRANSPORT_BINARY);
        write_bytes(bytes, binary_value);
    } else if !value.string_list_values.is_empty() {
        for string_value in &value.string_list_values {
            bytes.push(MESSAGE_ATTRIBUTE_TRANSPORT_STRING);
            write_bytes(bytes, string_value.as_bytes());
        }
    } else if !value.binary_list_values.is_empty() {
        for binary_value in &value.binary_list_values {
            bytes.push(MESSAGE_ATTRIBUTE_TRANSPORT_BINARY);
            write_bytes(bytes, binary_value);
        }
    }
}

fn write_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    target.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    target.extend_from_slice(bytes);
}
