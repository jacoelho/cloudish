use super::{QueryParameters, invalid_json_parameter};
use crate::xml::XmlBuilder;
use aws::AwsError;
use base64::Engine as _;
use serde_json::{Map, Value};
use services::SqsMessageAttributeValue;
use std::collections::BTreeMap;

#[derive(Default)]
struct QueryMessageAttributeParts {
    binary_list_values: BTreeMap<usize, Vec<u8>>,
    binary_value: Option<Vec<u8>>,
    data_type: Option<String>,
    name: Option<String>,
    string_list_values: BTreeMap<usize, String>,
    string_value: Option<String>,
}

pub(super) fn json_message_attributes_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, SqsMessageAttributeValue>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let Some(entries) = value.as_object() else {
        return Err(invalid_json_parameter(field));
    };
    let mut attributes = BTreeMap::new();
    for (name, value) in entries {
        let Some(entry) = value.as_object() else {
            return Err(invalid_json_parameter(field));
        };
        let Some(data_type) = entry.get("DataType").and_then(Value::as_str)
        else {
            return Err(invalid_json_parameter(field));
        };
        let string_list_values = entry
            .get("StringListValues")
            .map(json_string_list)
            .transpose()?
            .unwrap_or_default();
        let binary_list_values = entry
            .get("BinaryListValues")
            .map(json_binary_list)
            .transpose()?
            .unwrap_or_default();
        attributes.insert(
            name.clone(),
            SqsMessageAttributeValue {
                binary_list_values,
                binary_value: entry
                    .get("BinaryValue")
                    .map(json_binary_value)
                    .transpose()?,
                data_type: data_type.to_owned(),
                string_list_values,
                string_value: entry
                    .get("StringValue")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            },
        );
    }

    Ok(attributes)
}

pub(super) fn query_message_attributes(
    params: &QueryParameters,
    prefix: &str,
) -> Result<BTreeMap<String, SqsMessageAttributeValue>, AwsError> {
    let mut indexed: BTreeMap<usize, QueryMessageAttributeParts> =
        BTreeMap::new();
    for (name, value) in params.iter() {
        let Some(remainder) = name.strip_prefix(prefix) else {
            continue;
        };
        let Some((index, field)) = remainder.split_once('.') else {
            return Err(invalid_json_parameter(prefix));
        };
        let index = parse_query_attribute_list_index(index, prefix)?;
        let entry = indexed.entry(index).or_default();
        match field {
            "Name" => entry.name = Some(value.to_owned()),
            "Value.DataType" => entry.data_type = Some(value.to_owned()),
            "Value.StringValue" => entry.string_value = Some(value.to_owned()),
            "Value.BinaryValue" => {
                entry.binary_value = Some(
                    base64::engine::general_purpose::STANDARD
                        .decode(value)
                        .map_err(|_| invalid_json_parameter(prefix))?,
                );
            }
            _ => {
                if let Some(member) =
                    field.strip_prefix("Value.StringListValue.")
                {
                    let index =
                        parse_query_attribute_list_index(member, prefix)?;
                    if entry
                        .string_list_values
                        .insert(index, value.to_owned())
                        .is_some()
                    {
                        return Err(invalid_json_parameter(prefix));
                    }
                } else if let Some(member) =
                    field.strip_prefix("Value.BinaryListValue.")
                {
                    let index =
                        parse_query_attribute_list_index(member, prefix)?;
                    let decoded = base64::engine::general_purpose::STANDARD
                        .decode(value)
                        .map_err(|_| invalid_json_parameter(prefix))?;
                    if entry
                        .binary_list_values
                        .insert(index, decoded)
                        .is_some()
                    {
                        return Err(invalid_json_parameter(prefix));
                    }
                }
            }
        }
    }

    collect_contiguous_query_indexed_values(indexed, prefix)?
        .into_iter()
        .map(|entry| {
            let name =
                entry.name.ok_or_else(|| invalid_json_parameter(prefix))?;
            let data_type = entry
                .data_type
                .ok_or_else(|| invalid_json_parameter(prefix))?;
            Ok((
                name,
                SqsMessageAttributeValue {
                    binary_list_values: collect_query_attribute_list_values(
                        entry.binary_list_values,
                        prefix,
                    )?,
                    binary_value: entry.binary_value,
                    data_type,
                    string_list_values: collect_query_attribute_list_values(
                        entry.string_list_values,
                        prefix,
                    )?,
                    string_value: entry.string_value,
                },
            ))
        })
        .collect()
}

pub(super) fn json_message_attributes_map(
    attributes: &BTreeMap<String, SqsMessageAttributeValue>,
) -> Value {
    let mut object = Map::new();
    for (name, value) in attributes {
        object.insert(name.clone(), json_message_attribute_value(value));
    }

    Value::Object(object)
}

pub(super) fn query_message_attribute_value_xml(
    value: &SqsMessageAttributeValue,
) -> String {
    let mut xml = XmlBuilder::new().start("Value", None);
    xml = xml.elem("DataType", &value.data_type);
    if let Some(string_value) = value.string_value.as_deref() {
        xml = xml.elem("StringValue", string_value);
    }
    if let Some(binary_value) = value.binary_value.as_ref() {
        xml = xml.elem(
            "BinaryValue",
            &base64::engine::general_purpose::STANDARD.encode(binary_value),
        );
    }
    for string_value in &value.string_list_values {
        xml = xml.elem("StringListValue", string_value);
    }
    for binary_value in &value.binary_list_values {
        xml = xml.elem(
            "BinaryListValue",
            &base64::engine::general_purpose::STANDARD.encode(binary_value),
        );
    }

    xml.end("Value").build()
}

fn json_message_attribute_value(value: &SqsMessageAttributeValue) -> Value {
    let mut object = Map::new();
    object
        .insert("DataType".to_owned(), Value::String(value.data_type.clone()));
    if let Some(string_value) = value.string_value.as_ref() {
        object.insert(
            "StringValue".to_owned(),
            Value::String(string_value.clone()),
        );
    }
    if let Some(binary_value) = value.binary_value.as_ref() {
        object.insert(
            "BinaryValue".to_owned(),
            Value::String(
                base64::engine::general_purpose::STANDARD.encode(binary_value),
            ),
        );
    }
    if !value.string_list_values.is_empty() {
        object.insert(
            "StringListValues".to_owned(),
            Value::Array(
                value
                    .string_list_values
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if !value.binary_list_values.is_empty() {
        object.insert(
            "BinaryListValues".to_owned(),
            Value::Array(
                value
                    .binary_list_values
                    .iter()
                    .map(|entry| {
                        Value::String(
                            base64::engine::general_purpose::STANDARD
                                .encode(entry),
                        )
                    })
                    .collect(),
            ),
        );
    }

    Value::Object(object)
}

fn json_string_list(value: &Value) -> Result<Vec<String>, AwsError> {
    let Some(items) = value.as_array() else {
        return Err(invalid_json_parameter("MessageAttributes"));
    };

    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid_json_parameter("MessageAttributes"))
        })
        .collect()
}

fn json_binary_list(value: &Value) -> Result<Vec<Vec<u8>>, AwsError> {
    let Some(items) = value.as_array() else {
        return Err(invalid_json_parameter("MessageAttributes"));
    };

    items.iter().map(json_binary_value).collect()
}

fn json_binary_value(value: &Value) -> Result<Vec<u8>, AwsError> {
    let Some(value) = value.as_str() else {
        return Err(invalid_json_parameter("MessageAttributes"));
    };

    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| invalid_json_parameter("MessageAttributes"))
}

fn parse_query_attribute_list_index(
    value: &str,
    prefix: &str,
) -> Result<usize, AwsError> {
    let index =
        value.parse::<usize>().map_err(|_| invalid_json_parameter(prefix))?;
    if index == 0 {
        return Err(invalid_json_parameter(prefix));
    }

    Ok(index)
}

fn collect_contiguous_query_indexed_values<T>(
    mut values: BTreeMap<usize, T>,
    prefix: &str,
) -> Result<Vec<T>, AwsError> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let Some(max_index) = values.keys().next_back().copied() else {
        return Ok(Vec::new());
    };
    if values.len() != max_index {
        return Err(invalid_json_parameter(prefix));
    }

    let mut items = Vec::with_capacity(max_index);
    for index in 1..=max_index {
        let Some(value) = values.remove(&index) else {
            return Err(invalid_json_parameter(prefix));
        };
        items.push(value);
    }

    Ok(items)
}

fn collect_query_attribute_list_values<T: Clone>(
    values: BTreeMap<usize, T>,
    prefix: &str,
) -> Result<Vec<T>, AwsError> {
    collect_contiguous_query_indexed_values(values, prefix)
}
