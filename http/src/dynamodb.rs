pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::request::HttpRequest;
use aws::{AwsError, AwsErrorFamily, RequestContext};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::{Map, Value, json};
use services::{
    CreateDynamoDbTableInput, DynamoDbAttributeDefinition,
    DynamoDbAttributeUpdate, DynamoDbAttributeUpdateAction,
    DynamoDbAttributeValue, DynamoDbAttributeValue as AttributeValue,
    DynamoDbBatchGetItemInput, DynamoDbBatchWriteItemInput,
    DynamoDbBatchWriteRequest, DynamoDbBillingMode,
    DynamoDbCancellationReason, DynamoDbDeleteItemInput,
    DynamoDbDescribeStreamInput, DynamoDbGetItemInput,
    DynamoDbGetRecordsInput, DynamoDbGetShardIteratorInput,
    DynamoDbGlobalSecondaryIndexDefinition, DynamoDbItem,
    DynamoDbKeySchemaElement, DynamoDbKeyType, DynamoDbKeysAndAttributes,
    DynamoDbListStreamsInput, DynamoDbListTagsOfResourceInput,
    DynamoDbLocalSecondaryIndexDefinition, DynamoDbProjection,
    DynamoDbProjectionType, DynamoDbProvisionedThroughput,
    DynamoDbPutItemInput, DynamoDbQueryInput, DynamoDbResourceTag,
    DynamoDbReturnValues, DynamoDbScalarAttributeType, DynamoDbScanInput,
    DynamoDbScope, DynamoDbSequenceNumberRange, DynamoDbService,
    DynamoDbShardIteratorType, DynamoDbStreamDescription,
    DynamoDbStreamRecord, DynamoDbStreamShard, DynamoDbStreamSpecification,
    DynamoDbStreamStatus, DynamoDbStreamSummary, DynamoDbStreamViewType,
    DynamoDbTableDescription, DynamoDbTableName, DynamoDbTableStatus,
    DynamoDbTagResourceInput, DynamoDbTimeToLiveDescription,
    DynamoDbTimeToLiveSpecification, DynamoDbTimeToLiveStatus,
    DynamoDbTransactConditionCheck, DynamoDbTransactDeleteItem,
    DynamoDbTransactGetItem, DynamoDbTransactGetItemsInput,
    DynamoDbTransactPutItem, DynamoDbTransactUpdateItem,
    DynamoDbTransactWriteItem, DynamoDbTransactWriteItemsInput,
    DynamoDbUntagResourceInput, DynamoDbUpdateItemInput,
    DynamoDbUpdateTimeToLiveInput,
    UpdateDynamoDbTableInput as DynamoDbUpdateTableInput,
};
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) enum DynamoDbJsonError {
    Aws(AwsError),
    CustomAwsJson { body: Vec<u8>, error: AwsError },
}

impl DynamoDbJsonError {
    pub(crate) fn body(&self) -> Option<&[u8]> {
        match self {
            Self::Aws(_) => None,
            Self::CustomAwsJson { body, .. } => Some(body),
        }
    }

    pub(crate) fn code(&self) -> &str {
        self.error().code()
    }

    pub(crate) fn error(&self) -> &AwsError {
        match self {
            Self::Aws(error) | Self::CustomAwsJson { error, .. } => error,
        }
    }
}

impl From<AwsError> for DynamoDbJsonError {
    fn from(error: AwsError) -> Self {
        Self::Aws(error)
    }
}

pub(crate) fn handle_json(
    dynamodb: &DynamoDbService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, DynamoDbJsonError> {
    let target = request
        .header("x-amz-target")
        .ok_or_else(|| unsupported_operation_error("missing X-Amz-Target"))
        .map_err(DynamoDbJsonError::from)?;
    let (action, stream_target) =
        if let Some(action) = target.strip_prefix("DynamoDB_20120810.") {
            (action, false)
        } else if let Some(action) =
            target.strip_prefix("DynamoDBStreams_20120810.")
        {
            (action, true)
        } else {
            return Err(unsupported_operation_error(target).into());
        };
    let body = parse_json_body(request.body())?;
    let scope = DynamoDbScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    let response = match (stream_target, action) {
        (false, "CreateTable") => json!({
            "TableDescription": table_description_json(
                &dynamodb
                    .create_table(&scope, create_table_input(&body)?)
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (false, "DescribeTable") => json!({
            "Table": table_description_json(
                &dynamodb
                    .describe_table(
                        &scope,
                        &required_table_name_json(&body, "TableName")?,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (false, "ListTables") => json!({
            "TableNames": dynamodb
                .list_tables(&scope)
                .into_iter()
                .map(|table_name| table_name.to_string())
                .collect::<Vec<_>>()
        }),
        (false, "DeleteTable") => json!({
            "TableDescription": table_description_json(
                &dynamodb
                    .delete_table(
                        &scope,
                        &required_table_name_json(&body, "TableName")?,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (false, "PutItem") => {
            let attributes = dynamodb
                .put_item(&scope, put_item_input(&body)?)
                .map_err(|error| error.to_aws_error())?
                .attributes;
            put_attributes_response(attributes)
        }
        (false, "GetItem") => {
            let item = dynamodb
                .get_item(&scope, get_item_input(&body)?)
                .map_err(|error| error.to_aws_error())?
                .item;
            get_item_response(item)
        }
        (false, "DeleteItem") => {
            let attributes = dynamodb
                .delete_item(&scope, delete_item_input(&body)?)
                .map_err(|error| error.to_aws_error())?
                .attributes;
            put_attributes_response(attributes)
        }
        (false, "UpdateItem") => {
            let attributes = dynamodb
                .update_item(&scope, update_item_input(&body)?)
                .map_err(|error| error.to_aws_error())?
                .attributes;
            put_attributes_response(attributes)
        }
        (false, "Query") => query_response(
            dynamodb
                .query(&scope, query_input(&body)?)
                .map_err(|error| error.to_aws_error())?,
        ),
        (false, "Scan") => scan_response(
            dynamodb
                .scan(&scope, scan_input(&body)?)
                .map_err(|error| error.to_aws_error())?,
        ),
        (false, "BatchWriteItem") => {
            let output = dynamodb
                .batch_write_item(&scope, batch_write_item_input(&body)?)
                .map_err(|error| error.to_aws_error())?;
            json!({ "UnprocessedItems": batch_write_unprocessed_json(&output.unprocessed_items) })
        }
        (false, "BatchGetItem") => {
            let output = dynamodb
                .batch_get_item(&scope, batch_get_item_input(&body)?)
                .map_err(|error| error.to_aws_error())?;
            json!({
                "Responses": batch_get_responses_json(&output.responses),
                "UnprocessedKeys": batch_get_unprocessed_json(&output.unprocessed_keys),
            })
        }
        (false, "TransactWriteItems") => {
            if let Err(error) = dynamodb.transact_write_items(
                &scope,
                transact_write_items_input(&body)?,
            ) {
                if let Some(cancellation_reasons) =
                    error.cancellation_reasons()
                {
                    return Err(transaction_canceled_error(
                        error.to_aws_error(),
                        cancellation_reasons,
                    )?);
                }
                return Err(error.to_aws_error().into());
            }
            json!({})
        }
        (false, "TransactGetItems") => transact_get_response(
            dynamodb
                .transact_get_items(&scope, transact_get_items_input(&body)?)
                .map_err(|error| error.to_aws_error())?,
        ),
        (false, "UpdateTable") => json!({
            "TableDescription": table_description_json(
                &dynamodb
                    .update_table(&scope, update_table_input(&body)?)
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (false, "DescribeTimeToLive") => json!({
            "TimeToLiveDescription": time_to_live_description_json(
                &dynamodb
                    .describe_time_to_live(
                        &scope,
                        &required_string_json(&body, "TableName")?,
                    )
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (false, "UpdateTimeToLive") => json!({
            "TimeToLiveSpecification": time_to_live_specification_json(
                &dynamodb
                    .update_time_to_live(&scope, update_time_to_live_input(&body)?)
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (false, "TagResource") => {
            dynamodb
                .tag_resource(&scope, tag_resource_input(&body)?)
                .map_err(|error| error.to_aws_error())?;
            json!({})
        }
        (false, "UntagResource") => {
            dynamodb
                .untag_resource(&scope, untag_resource_input(&body)?)
                .map_err(|error| error.to_aws_error())?;
            json!({})
        }
        (false, "ListTagsOfResource") => list_tags_of_resource_response(
            dynamodb
                .list_tags_of_resource(
                    &scope,
                    list_tags_of_resource_input(&body)?,
                )
                .map_err(|error| error.to_aws_error())?,
        ),
        (true, "ListStreams") => list_streams_response(
            dynamodb
                .list_streams(&scope, list_streams_input(&body)?)
                .map_err(|error| error.to_aws_error())?,
        ),
        (true, "DescribeStream") => json!({
            "StreamDescription": stream_description_json(
                &dynamodb
                    .describe_stream(&scope, describe_stream_input(&body)?)
                    .map_err(|error| error.to_aws_error())?,
            )
        }),
        (true, "GetShardIterator") => json!({
            "ShardIterator": dynamodb
                .get_shard_iterator(&scope, get_shard_iterator_input(&body)?)
                .map_err(|error| error.to_aws_error())?
                .shard_iterator
        }),
        (true, "GetRecords") => get_records_response(
            dynamodb
                .get_records(&scope, get_records_input(&body)?)
                .map_err(|error| error.to_aws_error())?,
        ),
        _ => return Err(unsupported_operation_error(action).into()),
    };

    serialize_json_body(&response).map_err(DynamoDbJsonError::from)
}

fn create_table_input(
    body: &Value,
) -> Result<CreateDynamoDbTableInput, AwsError> {
    Ok(CreateDynamoDbTableInput {
        attribute_definitions: attribute_definitions_json(
            required_array_json(body, "AttributeDefinitions")?,
        )?,
        billing_mode: optional_billing_mode_json(body, "BillingMode")?,
        global_secondary_indexes: global_secondary_indexes_json(
            optional_array_json(body, "GlobalSecondaryIndexes")?,
        )?,
        key_schema: key_schema_json(required_array_json(body, "KeySchema")?)?,
        local_secondary_indexes: local_secondary_indexes_json(
            optional_array_json(body, "LocalSecondaryIndexes")?,
        )?,
        provisioned_throughput: optional_provisioned_throughput_json(
            body,
            "ProvisionedThroughput",
        )?,
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn put_item_input(body: &Value) -> Result<DynamoDbPutItemInput, AwsError> {
    Ok(DynamoDbPutItemInput {
        condition_expression: optional_string_json(
            body,
            "ConditionExpression",
        )
        .map(str::to_owned),
        expression_attribute_names: string_map_field(
            body,
            "ExpressionAttributeNames",
        )?,
        expression_attribute_values: attribute_value_map_field(
            body,
            "ExpressionAttributeValues",
        )?,
        item: item_field(body, "Item")?,
        return_values: DynamoDbReturnValues::parse(optional_string_json(
            body,
            "ReturnValues",
        ))
        .map_err(|error| error.to_aws_error())?,
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn get_item_input(body: &Value) -> Result<DynamoDbGetItemInput, AwsError> {
    Ok(DynamoDbGetItemInput {
        key: item_field(body, "Key")?,
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn delete_item_input(
    body: &Value,
) -> Result<DynamoDbDeleteItemInput, AwsError> {
    Ok(DynamoDbDeleteItemInput {
        condition_expression: optional_string_json(
            body,
            "ConditionExpression",
        )
        .map(str::to_owned),
        expression_attribute_names: string_map_field(
            body,
            "ExpressionAttributeNames",
        )?,
        expression_attribute_values: attribute_value_map_field(
            body,
            "ExpressionAttributeValues",
        )?,
        key: item_field(body, "Key")?,
        return_values: DynamoDbReturnValues::parse(optional_string_json(
            body,
            "ReturnValues",
        ))
        .map_err(|error| error.to_aws_error())?,
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn update_item_input(
    body: &Value,
) -> Result<DynamoDbUpdateItemInput, AwsError> {
    Ok(DynamoDbUpdateItemInput {
        attribute_updates: attribute_updates_field(body, "AttributeUpdates")?,
        condition_expression: optional_string_json(
            body,
            "ConditionExpression",
        )
        .map(str::to_owned),
        expression_attribute_names: string_map_field(
            body,
            "ExpressionAttributeNames",
        )?,
        expression_attribute_values: attribute_value_map_field(
            body,
            "ExpressionAttributeValues",
        )?,
        key: item_field(body, "Key")?,
        return_values: DynamoDbReturnValues::parse(optional_string_json(
            body,
            "ReturnValues",
        ))
        .map_err(|error| error.to_aws_error())?,
        table_name: required_table_name_json(body, "TableName")?,
        update_expression: optional_string_json(body, "UpdateExpression")
            .map(str::to_owned),
    })
}

fn query_input(body: &Value) -> Result<DynamoDbQueryInput, AwsError> {
    Ok(DynamoDbQueryInput {
        exclusive_start_key: optional_item_field(body, "ExclusiveStartKey")?,
        expression_attribute_names: string_map_field(
            body,
            "ExpressionAttributeNames",
        )?,
        expression_attribute_values: attribute_value_map_field(
            body,
            "ExpressionAttributeValues",
        )?,
        filter_expression: optional_string_json(body, "FilterExpression")
            .map(str::to_owned),
        index_name: optional_string_json(body, "IndexName").map(str::to_owned),
        key_condition_expression: required_string_json(
            body,
            "KeyConditionExpression",
        )?,
        limit: optional_usize_json(body, "Limit")?,
        scan_index_forward: optional_bool_json(body, "ScanIndexForward")
            .unwrap_or(true),
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn scan_input(body: &Value) -> Result<DynamoDbScanInput, AwsError> {
    Ok(DynamoDbScanInput {
        exclusive_start_key: optional_item_field(body, "ExclusiveStartKey")?,
        expression_attribute_names: string_map_field(
            body,
            "ExpressionAttributeNames",
        )?,
        expression_attribute_values: attribute_value_map_field(
            body,
            "ExpressionAttributeValues",
        )?,
        filter_expression: optional_string_json(body, "FilterExpression")
            .map(str::to_owned),
        index_name: optional_string_json(body, "IndexName").map(str::to_owned),
        limit: optional_usize_json(body, "Limit")?,
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn batch_write_item_input(
    body: &Value,
) -> Result<DynamoDbBatchWriteItemInput, AwsError> {
    let request_items = body
        .get("RequestItems")
        .and_then(Value::as_object)
        .ok_or_else(|| missing_field_error("RequestItems"))?;
    let mut items = BTreeMap::new();
    for (table_name, value) in request_items {
        let entries = value
            .as_array()
            .ok_or_else(|| invalid_parameter_error("RequestItems"))?;
        let mut requests = Vec::new();
        for entry in entries {
            let object = entry
                .as_object()
                .ok_or_else(|| invalid_parameter_error("RequestItems"))?;
            if let Some(put_request) = object.get("PutRequest") {
                requests.push(DynamoDbBatchWriteRequest::Put {
                    item: item_from_value(
                        put_request.get("Item").ok_or_else(|| {
                            missing_field_error("PutRequest.Item")
                        })?,
                    )?,
                });
                continue;
            }
            if let Some(delete_request) = object.get("DeleteRequest") {
                requests.push(DynamoDbBatchWriteRequest::Delete {
                    key: item_from_value(
                        delete_request.get("Key").ok_or_else(|| {
                            missing_field_error("DeleteRequest.Key")
                        })?,
                    )?,
                });
                continue;
            }
            return Err(invalid_parameter_error("RequestItems"));
        }
        items.insert(
            DynamoDbTableName::parse(table_name)
                .map_err(|error| error.to_aws_error())?,
            requests,
        );
    }

    Ok(DynamoDbBatchWriteItemInput { request_items: items })
}

fn batch_get_item_input(
    body: &Value,
) -> Result<DynamoDbBatchGetItemInput, AwsError> {
    let request_items = body
        .get("RequestItems")
        .and_then(Value::as_object)
        .ok_or_else(|| missing_field_error("RequestItems"))?;
    let mut items = BTreeMap::new();
    for (table_name, value) in request_items {
        let object = value
            .as_object()
            .ok_or_else(|| invalid_parameter_error("RequestItems"))?;
        let keys = object
            .get("Keys")
            .and_then(Value::as_array)
            .ok_or_else(|| missing_field_error("Keys"))?;
        items.insert(
            DynamoDbTableName::parse(table_name)
                .map_err(|error| error.to_aws_error())?,
            DynamoDbKeysAndAttributes {
                keys: keys
                    .iter()
                    .map(item_from_value)
                    .collect::<Result<Vec<_>, _>>()?,
            },
        );
    }

    Ok(DynamoDbBatchGetItemInput { request_items: items })
}

fn update_table_input(
    body: &Value,
) -> Result<DynamoDbUpdateTableInput, AwsError> {
    if body.get("GlobalSecondaryIndexUpdates").is_some() {
        return Err(validation_error(
            "UpdateTable GlobalSecondaryIndexUpdates are not supported yet.",
        ));
    }

    Ok(DynamoDbUpdateTableInput {
        billing_mode: optional_billing_mode_json(body, "BillingMode")?,
        provisioned_throughput: optional_provisioned_throughput_json(
            body,
            "ProvisionedThroughput",
        )?,
        stream_specification: body
            .get("StreamSpecification")
            .map(stream_specification_from_value)
            .transpose()?,
        table_name: required_table_name_json(body, "TableName")?,
    })
}

fn update_time_to_live_input(
    body: &Value,
) -> Result<DynamoDbUpdateTimeToLiveInput, AwsError> {
    let specification = body
        .get("TimeToLiveSpecification")
        .ok_or_else(|| missing_field_error("TimeToLiveSpecification"))?;
    let specification = specification
        .as_object()
        .ok_or_else(|| invalid_parameter_error("TimeToLiveSpecification"))?;

    Ok(DynamoDbUpdateTimeToLiveInput {
        table_name: required_string_json(body, "TableName")?,
        time_to_live_specification: DynamoDbTimeToLiveSpecification {
            attribute_name: required_string_from_object(
                specification,
                "AttributeName",
            )?,
            enabled: specification
                .get("Enabled")
                .and_then(Value::as_bool)
                .ok_or_else(|| missing_field_error("Enabled"))?,
        },
    })
}

fn transact_get_items_input(
    body: &Value,
) -> Result<DynamoDbTransactGetItemsInput, AwsError> {
    let transact_items = required_array_json(body, "TransactItems")?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| invalid_parameter_error("TransactItems"))?;
            let get = object
                .get("Get")
                .and_then(Value::as_object)
                .ok_or_else(|| invalid_parameter_error("TransactItems"))?;

            Ok(DynamoDbTransactGetItem {
                key: item_from_value(
                    get.get("Key")
                        .ok_or_else(|| missing_field_error("Get.Key"))?,
                )?,
                table_name: required_table_name_from_object(get, "TableName")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DynamoDbTransactGetItemsInput { transact_items })
}

fn transact_write_items_input(
    body: &Value,
) -> Result<DynamoDbTransactWriteItemsInput, AwsError> {
    let transact_items = required_array_json(body, "TransactItems")?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| invalid_parameter_error("TransactItems"))?;
            if let Some(put) = object.get("Put").and_then(Value::as_object) {
                return Ok(DynamoDbTransactWriteItem::Put(
                    DynamoDbTransactPutItem {
                        condition_expression: optional_string_from_object(
                            put,
                            "ConditionExpression",
                        )
                        .map(str::to_owned),
                        expression_attribute_names: string_map_from_object(
                            put,
                            "ExpressionAttributeNames",
                        )?,
                        expression_attribute_values:
                            attribute_value_map_from_object(
                                put,
                                "ExpressionAttributeValues",
                            )?,
                        item: item_from_value(put.get("Item").ok_or_else(
                            || missing_field_error("Put.Item"),
                        )?)?,
                        table_name: required_table_name_from_object(
                            put,
                            "TableName",
                        )?,
                    },
                ));
            }
            if let Some(delete) =
                object.get("Delete").and_then(Value::as_object)
            {
                return Ok(DynamoDbTransactWriteItem::Delete(
                    DynamoDbTransactDeleteItem {
                        condition_expression: optional_string_from_object(
                            delete,
                            "ConditionExpression",
                        )
                        .map(str::to_owned),
                        expression_attribute_names: string_map_from_object(
                            delete,
                            "ExpressionAttributeNames",
                        )?,
                        expression_attribute_values:
                            attribute_value_map_from_object(
                                delete,
                                "ExpressionAttributeValues",
                            )?,
                        key: item_from_value(delete.get("Key").ok_or_else(
                            || missing_field_error("Delete.Key"),
                        )?)?,
                        table_name: required_table_name_from_object(
                            delete,
                            "TableName",
                        )?,
                    },
                ));
            }
            if let Some(update) =
                object.get("Update").and_then(Value::as_object)
            {
                return Ok(DynamoDbTransactWriteItem::Update(
                    DynamoDbTransactUpdateItem {
                        condition_expression: optional_string_from_object(
                            update,
                            "ConditionExpression",
                        )
                        .map(str::to_owned),
                        expression_attribute_names: string_map_from_object(
                            update,
                            "ExpressionAttributeNames",
                        )?,
                        expression_attribute_values:
                            attribute_value_map_from_object(
                                update,
                                "ExpressionAttributeValues",
                            )?,
                        key: item_from_value(update.get("Key").ok_or_else(
                            || missing_field_error("Update.Key"),
                        )?)?,
                        table_name: required_table_name_from_object(
                            update,
                            "TableName",
                        )?,
                        update_expression: required_string_from_object(
                            update,
                            "UpdateExpression",
                        )?,
                    },
                ));
            }
            if let Some(check) =
                object.get("ConditionCheck").and_then(Value::as_object)
            {
                return Ok(DynamoDbTransactWriteItem::ConditionCheck(
                    DynamoDbTransactConditionCheck {
                        condition_expression: required_string_from_object(
                            check,
                            "ConditionExpression",
                        )?,
                        expression_attribute_names: string_map_from_object(
                            check,
                            "ExpressionAttributeNames",
                        )?,
                        expression_attribute_values:
                            attribute_value_map_from_object(
                                check,
                                "ExpressionAttributeValues",
                            )?,
                        key: item_from_value(check.get("Key").ok_or_else(
                            || missing_field_error("ConditionCheck.Key"),
                        )?)?,
                        table_name: required_table_name_from_object(
                            check,
                            "TableName",
                        )?,
                    },
                ));
            }

            Err(invalid_parameter_error("TransactItems"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DynamoDbTransactWriteItemsInput { transact_items })
}

fn tag_resource_input(
    body: &Value,
) -> Result<DynamoDbTagResourceInput, AwsError> {
    let tags = optional_array_json(body, "Tags")?
        .unwrap_or(&[])
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| invalid_parameter_error("Tags"))?;
            Ok(DynamoDbResourceTag {
                key: required_string_from_object(object, "Key")?,
                value: required_string_from_object(object, "Value")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DynamoDbTagResourceInput {
        resource_arn: required_string_json(body, "ResourceArn")?,
        tags,
    })
}

fn untag_resource_input(
    body: &Value,
) -> Result<DynamoDbUntagResourceInput, AwsError> {
    let tag_keys = required_array_json(body, "TagKeys")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid_parameter_error("TagKeys"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DynamoDbUntagResourceInput {
        resource_arn: required_string_json(body, "ResourceArn")?,
        tag_keys,
    })
}

fn list_tags_of_resource_input(
    body: &Value,
) -> Result<DynamoDbListTagsOfResourceInput, AwsError> {
    Ok(DynamoDbListTagsOfResourceInput {
        next_token: optional_string_json(body, "NextToken").map(str::to_owned),
        resource_arn: required_string_json(body, "ResourceArn")?,
    })
}

fn list_streams_input(
    body: &Value,
) -> Result<DynamoDbListStreamsInput, AwsError> {
    Ok(DynamoDbListStreamsInput {
        exclusive_start_stream_arn: optional_string_json(
            body,
            "ExclusiveStartStreamArn",
        )
        .map(str::to_owned),
        limit: optional_usize_json(body, "Limit")?,
        table_name: optional_string_json(body, "TableName")
            .map(DynamoDbTableName::parse)
            .transpose()
            .map_err(|error| error.to_aws_error())?,
    })
}

fn describe_stream_input(
    body: &Value,
) -> Result<DynamoDbDescribeStreamInput, AwsError> {
    Ok(DynamoDbDescribeStreamInput {
        exclusive_start_shard_id: optional_string_json(
            body,
            "ExclusiveStartShardId",
        )
        .map(str::to_owned),
        limit: optional_usize_json(body, "Limit")?,
        stream_arn: required_string_json(body, "StreamArn")?,
    })
}

fn get_shard_iterator_input(
    body: &Value,
) -> Result<DynamoDbGetShardIteratorInput, AwsError> {
    Ok(DynamoDbGetShardIteratorInput {
        sequence_number: optional_string_json(body, "SequenceNumber")
            .map(str::to_owned),
        shard_id: required_string_json(body, "ShardId")?,
        shard_iterator_type: DynamoDbShardIteratorType::parse(
            &required_string_json(body, "ShardIteratorType")?,
        )
        .map_err(|error| error.to_aws_error())?,
        stream_arn: required_string_json(body, "StreamArn")?,
    })
}

fn get_records_input(
    body: &Value,
) -> Result<DynamoDbGetRecordsInput, AwsError> {
    Ok(DynamoDbGetRecordsInput {
        limit: optional_usize_json(body, "Limit")?,
        shard_iterator: required_string_json(body, "ShardIterator")?,
    })
}

fn put_attributes_response(attributes: Option<DynamoDbItem>) -> Value {
    attributes
        .map(|attributes| json!({ "Attributes": item_json(&attributes) }))
        .unwrap_or_else(|| json!({}))
}

fn get_item_response(item: Option<DynamoDbItem>) -> Value {
    item.map(|item| json!({ "Item": item_json(&item) }))
        .unwrap_or_else(|| json!({}))
}

fn insert_object_field(value: &mut Value, field: &str, inserted: Value) {
    if let Some(object) = value.as_object_mut() {
        object.insert(field.to_owned(), inserted);
    }
}

fn query_response(output: services::DynamoDbQueryOutput) -> Value {
    let mut response = json!({
        "Count": output.count,
        "Items": output.items.iter().map(item_json).collect::<Vec<_>>(),
        "ScannedCount": output.scanned_count,
    });
    if let Some(last_evaluated_key) = output.last_evaluated_key {
        insert_object_field(
            &mut response,
            "LastEvaluatedKey",
            item_json(&last_evaluated_key),
        );
    }
    response
}

fn scan_response(output: services::DynamoDbScanOutput) -> Value {
    let mut response = json!({
        "Count": output.count,
        "Items": output.items.iter().map(item_json).collect::<Vec<_>>(),
        "ScannedCount": output.scanned_count,
    });
    if let Some(last_evaluated_key) = output.last_evaluated_key {
        insert_object_field(
            &mut response,
            "LastEvaluatedKey",
            item_json(&last_evaluated_key),
        );
    }
    response
}

fn batch_write_unprocessed_json(
    request_items: &BTreeMap<
        DynamoDbTableName,
        Vec<DynamoDbBatchWriteRequest>,
    >,
) -> Value {
    let object = request_items
        .iter()
        .map(|(table_name, requests)| {
            (
                table_name.to_string(),
                Value::Array(
                    requests
                        .iter()
                        .map(batch_write_request_json)
                        .collect::<Vec<_>>(),
                ),
            )
        })
        .collect::<Map<String, Value>>();
    Value::Object(object)
}

fn batch_write_request_json(request: &DynamoDbBatchWriteRequest) -> Value {
    match request {
        DynamoDbBatchWriteRequest::Delete { key } => {
            json!({ "DeleteRequest": { "Key": item_json(key) } })
        }
        DynamoDbBatchWriteRequest::Put { item } => {
            json!({ "PutRequest": { "Item": item_json(item) } })
        }
    }
}

fn batch_get_responses_json(
    responses: &BTreeMap<DynamoDbTableName, Vec<DynamoDbItem>>,
) -> Value {
    Value::Object(
        responses
            .iter()
            .map(|(table_name, items)| {
                (
                    table_name.to_string(),
                    Value::Array(
                        items.iter().map(item_json).collect::<Vec<_>>(),
                    ),
                )
            })
            .collect(),
    )
}

fn batch_get_unprocessed_json(
    request_items: &BTreeMap<DynamoDbTableName, DynamoDbKeysAndAttributes>,
) -> Value {
    Value::Object(
        request_items
            .iter()
            .map(|(table_name, keys)| {
                (
                    table_name.to_string(),
                    json!({
                        "Keys": keys.keys.iter().map(item_json).collect::<Vec<_>>(),
                    }),
                )
            })
            .collect(),
    )
}

fn transact_get_response(
    output: services::DynamoDbTransactGetItemsOutput,
) -> Value {
    json!({
        "Responses": output
            .responses
            .into_iter()
            .map(|response| {
                response
                    .item
                    .map(|item| json!({ "Item": item_json(&item) }))
                    .unwrap_or_else(|| json!({}))
            })
            .collect::<Vec<_>>(),
    })
}

fn time_to_live_description_json(
    description: &DynamoDbTimeToLiveDescription,
) -> Value {
    let mut value = json!({
        "TimeToLiveStatus": time_to_live_status_string(
            description.time_to_live_status,
        )
    });
    if let Some(attribute_name) = description.attribute_name.as_ref() {
        insert_object_field(
            &mut value,
            "AttributeName",
            Value::String(attribute_name.clone()),
        );
    }
    value
}

fn time_to_live_specification_json(
    specification: &DynamoDbTimeToLiveSpecification,
) -> Value {
    json!({
        "AttributeName": specification.attribute_name,
        "Enabled": specification.enabled,
    })
}

fn list_tags_of_resource_response(
    output: services::DynamoDbListTagsOfResourceOutput,
) -> Value {
    let mut value = json!({
        "Tags": output.tags.into_iter().map(resource_tag_json).collect::<Vec<_>>()
    });
    if let Some(next_token) = output.next_token {
        insert_object_field(
            &mut value,
            "NextToken",
            Value::String(next_token),
        );
    }
    value
}

fn resource_tag_json(tag: DynamoDbResourceTag) -> Value {
    json!({
        "Key": tag.key,
        "Value": tag.value,
    })
}

fn list_streams_response(
    output: services::DynamoDbListStreamsOutput,
) -> Value {
    let mut value = json!({
        "Streams": output.streams.iter().map(stream_summary_json).collect::<Vec<_>>()
    });
    if let Some(last_evaluated_stream_arn) = output.last_evaluated_stream_arn {
        insert_object_field(
            &mut value,
            "LastEvaluatedStreamArn",
            Value::String(last_evaluated_stream_arn),
        );
    }
    value
}

fn stream_summary_json(summary: &DynamoDbStreamSummary) -> Value {
    json!({
        "StreamArn": summary.stream_arn,
        "StreamLabel": summary.stream_label,
        "TableName": summary.table_name.to_string(),
    })
}

fn stream_description_json(description: &DynamoDbStreamDescription) -> Value {
    let mut value = json!({
        "CreationRequestDateTime": description.creation_request_date_time,
        "KeySchema": description.key_schema.iter().map(key_schema_element_json).collect::<Vec<_>>(),
        "Shards": description.shards.iter().map(stream_shard_json).collect::<Vec<_>>(),
        "StreamArn": description.stream_arn,
        "StreamLabel": description.stream_label,
        "StreamStatus": stream_status_string(description.stream_status),
        "StreamViewType": stream_view_type_string(description.stream_view_type),
        "TableName": description.table_name.to_string(),
    });
    if let Some(last_evaluated_shard_id) =
        description.last_evaluated_shard_id.as_ref()
    {
        insert_object_field(
            &mut value,
            "LastEvaluatedShardId",
            Value::String(last_evaluated_shard_id.clone()),
        );
    }
    value
}

fn stream_shard_json(shard: &DynamoDbStreamShard) -> Value {
    let mut value = json!({
        "SequenceNumberRange": sequence_number_range_json(
            &shard.sequence_number_range
        ),
        "ShardId": shard.shard_id,
    });
    if let Some(parent_shard_id) = shard.parent_shard_id.as_ref() {
        insert_object_field(
            &mut value,
            "ParentShardId",
            Value::String(parent_shard_id.clone()),
        );
    }
    value
}

fn sequence_number_range_json(range: &DynamoDbSequenceNumberRange) -> Value {
    let mut value = json!({
        "StartingSequenceNumber": range.starting_sequence_number
    });
    if let Some(ending_sequence_number) = range.ending_sequence_number.as_ref()
    {
        insert_object_field(
            &mut value,
            "EndingSequenceNumber",
            Value::String(ending_sequence_number.clone()),
        );
    }
    value
}

fn get_records_response(output: services::DynamoDbGetRecordsOutput) -> Value {
    let mut value = json!({
        "Records": output.records.iter().map(stream_record_json).collect::<Vec<_>>()
    });
    if let Some(next_shard_iterator) = output.next_shard_iterator {
        insert_object_field(
            &mut value,
            "NextShardIterator",
            Value::String(next_shard_iterator),
        );
    }
    value
}

fn stream_record_json(record: &DynamoDbStreamRecord) -> Value {
    json!({
        "awsRegion": record.aws_region,
        "dynamodb": stream_record_data_json(&record.dynamodb),
        "eventID": record.event_id,
        "eventName": record.event_name,
        "eventSource": record.event_source,
        "eventVersion": record.event_version,
    })
}

fn stream_record_data_json(
    data: &services::DynamoDbStreamRecordData,
) -> Value {
    let mut value = json!({
        "ApproximateCreationDateTime": data.approximate_creation_date_time,
        "Keys": item_json(&data.keys),
        "SequenceNumber": data.sequence_number,
        "SizeBytes": data.size_bytes,
        "StreamViewType": stream_view_type_string(data.stream_view_type),
    });
    if let Some(new_image) = data.new_image.as_ref() {
        insert_object_field(&mut value, "NewImage", item_json(new_image));
    }
    if let Some(old_image) = data.old_image.as_ref() {
        insert_object_field(&mut value, "OldImage", item_json(old_image));
    }
    value
}

fn table_description_json(table: &DynamoDbTableDescription) -> Value {
    let mut description = json!({
        "AttributeDefinitions": table
            .attribute_definitions
            .iter()
            .map(attribute_definition_json)
            .collect::<Vec<_>>(),
        "BillingModeSummary": {
            "BillingMode": billing_mode_string(table.billing_mode)
        },
        "CreationDateTime": table.created_at_epoch_seconds,
        "ItemCount": table.item_count,
        "KeySchema": table.key_schema.iter().map(key_schema_element_json).collect::<Vec<_>>(),
        "TableArn": table.table_arn,
        "TableName": table.table_name.to_string(),
        "TableSizeBytes": table.table_size_bytes,
        "TableStatus": table_status_string(table.table_status),
    });
    if let Some(provisioned_throughput) = table.provisioned_throughput.as_ref()
    {
        insert_object_field(
            &mut description,
            "ProvisionedThroughput",
            provisioned_throughput_json(provisioned_throughput),
        );
    }
    if let Some(latest_stream_arn) = table.latest_stream_arn.as_ref() {
        insert_object_field(
            &mut description,
            "LatestStreamArn",
            Value::String(latest_stream_arn.clone()),
        );
    }
    if let Some(latest_stream_label) = table.latest_stream_label.as_ref() {
        insert_object_field(
            &mut description,
            "LatestStreamLabel",
            Value::String(latest_stream_label.clone()),
        );
    }
    if let Some(stream_specification) = table.stream_specification.as_ref() {
        insert_object_field(
            &mut description,
            "StreamSpecification",
            stream_specification_json(stream_specification),
        );
    }
    if !table.global_secondary_indexes.is_empty() {
        insert_object_field(
            &mut description,
            "GlobalSecondaryIndexes",
            Value::Array(
                table
                    .global_secondary_indexes
                    .iter()
                    .map(|index| {
                        global_secondary_index_json(index, &table.table_arn)
                    })
                    .collect::<Vec<_>>(),
            ),
        );
    }
    if !table.local_secondary_indexes.is_empty() {
        insert_object_field(
            &mut description,
            "LocalSecondaryIndexes",
            Value::Array(
                table
                    .local_secondary_indexes
                    .iter()
                    .map(|index| {
                        local_secondary_index_json(index, &table.table_arn)
                    })
                    .collect::<Vec<_>>(),
            ),
        );
    }
    description
}

fn attribute_definition_json(
    definition: &DynamoDbAttributeDefinition,
) -> Value {
    json!({
        "AttributeName": definition.attribute_name,
        "AttributeType": scalar_attribute_type_string(definition.attribute_type),
    })
}

fn key_schema_element_json(element: &DynamoDbKeySchemaElement) -> Value {
    json!({
        "AttributeName": element.attribute_name,
        "KeyType": key_type_string(element.key_type),
    })
}

fn global_secondary_index_json(
    index: &DynamoDbGlobalSecondaryIndexDefinition,
    table_arn: &str,
) -> Value {
    let mut value = json!({
        "IndexArn": format!("{table_arn}/index/{}", index.index_name),
        "IndexName": index.index_name,
        "IndexStatus": "ACTIVE",
        "KeySchema": index.key_schema.iter().map(key_schema_element_json).collect::<Vec<_>>(),
        "Projection": projection_json(&index.projection),
    });
    if let Some(provisioned_throughput) = index.provisioned_throughput.as_ref()
    {
        insert_object_field(
            &mut value,
            "ProvisionedThroughput",
            provisioned_throughput_json(provisioned_throughput),
        );
    }
    value
}

fn local_secondary_index_json(
    index: &DynamoDbLocalSecondaryIndexDefinition,
    table_arn: &str,
) -> Value {
    json!({
        "IndexArn": format!("{table_arn}/index/{}", index.index_name),
        "IndexName": index.index_name,
        "KeySchema": index.key_schema.iter().map(key_schema_element_json).collect::<Vec<_>>(),
        "Projection": projection_json(&index.projection),
    })
}

fn projection_json(projection: &DynamoDbProjection) -> Value {
    let mut value = json!({
        "ProjectionType": projection_type_string(projection.projection_type),
    });
    if !projection.non_key_attributes.is_empty() {
        insert_object_field(
            &mut value,
            "NonKeyAttributes",
            Value::Array(
                projection
                    .non_key_attributes
                    .iter()
                    .map(|value| Value::String(value.clone()))
                    .collect(),
            ),
        );
    }
    value
}

fn stream_specification_json(
    specification: &DynamoDbStreamSpecification,
) -> Value {
    let mut value = json!({
        "StreamEnabled": specification.stream_enabled
    });
    if let Some(stream_view_type) = specification.stream_view_type {
        insert_object_field(
            &mut value,
            "StreamViewType",
            Value::String(
                stream_view_type_string(stream_view_type).to_owned(),
            ),
        );
    }
    value
}

fn provisioned_throughput_json(
    provisioned_throughput: &DynamoDbProvisionedThroughput,
) -> Value {
    json!({
        "ReadCapacityUnits": provisioned_throughput.read_capacity_units,
        "WriteCapacityUnits": provisioned_throughput.write_capacity_units,
    })
}

fn item_json(item: &DynamoDbItem) -> Value {
    Value::Object(
        item.iter()
            .map(|(name, value)| (name.clone(), attribute_value_json(value)))
            .collect(),
    )
}

fn attribute_value_json(value: &AttributeValue) -> Value {
    match value {
        AttributeValue::Binary(value) => {
            json!({ "B": BASE64_STANDARD.encode(value) })
        }
        AttributeValue::Bool(value) => json!({ "BOOL": value }),
        AttributeValue::BinarySet(values) => json!({
            "BS": values
                .iter()
                .map(|value| Value::String(BASE64_STANDARD.encode(value)))
                .collect::<Vec<_>>()
        }),
        AttributeValue::List(values) => json!({
            "L": values.iter().map(attribute_value_json).collect::<Vec<_>>()
        }),
        AttributeValue::Map(values) => json!({
            "M": values
                .iter()
                .map(|(name, value)| (name.clone(), attribute_value_json(value)))
                .collect::<Map<String, Value>>()
        }),
        AttributeValue::Number(value) => json!({ "N": value }),
        AttributeValue::NumberSet(values) => json!({
            "NS": values.iter().map(|value| Value::String(value.clone())).collect::<Vec<_>>()
        }),
        AttributeValue::Null(value) => json!({ "NULL": value }),
        AttributeValue::String(value) => json!({ "S": value }),
        AttributeValue::StringSet(values) => json!({
            "SS": values.iter().map(|value| Value::String(value.clone())).collect::<Vec<_>>()
        }),
    }
}

fn attribute_definitions_json(
    values: &[Value],
) -> Result<Vec<DynamoDbAttributeDefinition>, AwsError> {
    values
        .iter()
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                invalid_parameter_error("AttributeDefinitions")
            })?;
            Ok(DynamoDbAttributeDefinition {
                attribute_name: required_string_from_object(
                    object,
                    "AttributeName",
                )?,
                attribute_type: scalar_attribute_type_json(
                    required_string_from_object(object, "AttributeType")?
                        .as_str(),
                )?,
            })
        })
        .collect()
}

fn key_schema_json(
    values: &[Value],
) -> Result<Vec<DynamoDbKeySchemaElement>, AwsError> {
    values
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| invalid_parameter_error("KeySchema"))?;
            Ok(DynamoDbKeySchemaElement {
                attribute_name: required_string_from_object(
                    object,
                    "AttributeName",
                )?,
                key_type: key_type_json(
                    required_string_from_object(object, "KeyType")?.as_str(),
                )?,
            })
        })
        .collect()
}

fn global_secondary_indexes_json(
    values: Option<&[Value]>,
) -> Result<Vec<DynamoDbGlobalSecondaryIndexDefinition>, AwsError> {
    let Some(values) = values else {
        return Ok(Vec::new());
    };
    values
        .iter()
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                invalid_parameter_error("GlobalSecondaryIndexes")
            })?;
            Ok(DynamoDbGlobalSecondaryIndexDefinition {
                index_name: required_string_from_object(object, "IndexName")?,
                key_schema: key_schema_json(required_array_from_object(
                    object,
                    "KeySchema",
                )?)?,
                projection: projection_from_value(
                    object
                        .get("Projection")
                        .ok_or_else(|| missing_field_error("Projection"))?,
                )?,
                provisioned_throughput: object
                    .get("ProvisionedThroughput")
                    .map(provisioned_throughput_from_value)
                    .transpose()?,
            })
        })
        .collect()
}

fn local_secondary_indexes_json(
    values: Option<&[Value]>,
) -> Result<Vec<DynamoDbLocalSecondaryIndexDefinition>, AwsError> {
    let Some(values) = values else {
        return Ok(Vec::new());
    };
    values
        .iter()
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                invalid_parameter_error("LocalSecondaryIndexes")
            })?;
            Ok(DynamoDbLocalSecondaryIndexDefinition {
                index_name: required_string_from_object(object, "IndexName")?,
                key_schema: key_schema_json(required_array_from_object(
                    object,
                    "KeySchema",
                )?)?,
                projection: projection_from_value(
                    object
                        .get("Projection")
                        .ok_or_else(|| missing_field_error("Projection"))?,
                )?,
            })
        })
        .collect()
}

fn projection_from_value(
    value: &Value,
) -> Result<DynamoDbProjection, AwsError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_parameter_error("Projection"))?;
    Ok(DynamoDbProjection {
        non_key_attributes: optional_array_from_object(
            object,
            "NonKeyAttributes",
        )
        .unwrap_or(&[])
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid_parameter_error("NonKeyAttributes"))
        })
        .collect::<Result<Vec<_>, _>>()?,
        projection_type: projection_type_json(
            required_string_from_object(object, "ProjectionType")?.as_str(),
        )?,
    })
}

fn string_map_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let object =
        value.as_object().ok_or_else(|| invalid_parameter_error(field))?;
    object
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_owned()))
                .ok_or_else(|| invalid_parameter_error(field))
        })
        .collect()
}

fn string_map_from_object(
    object: &Map<String, Value>,
    field: &str,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(value) = object.get(field) else {
        return Ok(BTreeMap::new());
    };
    let value =
        value.as_object().ok_or_else(|| invalid_parameter_error(field))?;
    value
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_owned()))
                .ok_or_else(|| invalid_parameter_error(field))
        })
        .collect()
}

fn attribute_value_map_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, DynamoDbAttributeValue>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let object =
        value.as_object().ok_or_else(|| invalid_parameter_error(field))?;
    object
        .iter()
        .map(|(name, value)| {
            json_to_attribute_value(value).map(|value| (name.clone(), value))
        })
        .collect()
}

fn attribute_value_map_from_object(
    object: &Map<String, Value>,
    field: &str,
) -> Result<BTreeMap<String, DynamoDbAttributeValue>, AwsError> {
    let Some(value) = object.get(field) else {
        return Ok(BTreeMap::new());
    };
    let value =
        value.as_object().ok_or_else(|| invalid_parameter_error(field))?;
    value
        .iter()
        .map(|(name, value)| {
            json_to_attribute_value(value).map(|value| (name.clone(), value))
        })
        .collect()
}

fn attribute_updates_field(
    body: &Value,
    field: &str,
) -> Result<BTreeMap<String, DynamoDbAttributeUpdate>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(BTreeMap::new());
    };
    let object =
        value.as_object().ok_or_else(|| invalid_parameter_error(field))?;
    object
        .iter()
        .map(|(name, value)| {
            let object = value
                .as_object()
                .ok_or_else(|| invalid_parameter_error(field))?;
            let action = match object
                .get("Action")
                .and_then(Value::as_str)
                .unwrap_or("PUT")
            {
                "DELETE" => DynamoDbAttributeUpdateAction::Delete,
                "PUT" => DynamoDbAttributeUpdateAction::Put,
                action => {
                    return Err(validation_error(format!(
                        "Unsupported AttributeUpdates Action `{action}`."
                    )));
                }
            };
            let value = object
                .get("Value")
                .map(json_to_attribute_value)
                .transpose()?;
            Ok((name.clone(), DynamoDbAttributeUpdate { action, value }))
        })
        .collect()
}

fn item_field(body: &Value, field: &str) -> Result<DynamoDbItem, AwsError> {
    item_from_value(body.get(field).ok_or_else(|| missing_field_error(field))?)
}

fn optional_item_field(
    body: &Value,
    field: &str,
) -> Result<Option<DynamoDbItem>, AwsError> {
    body.get(field).map(item_from_value).transpose()
}

fn item_from_value(value: &Value) -> Result<DynamoDbItem, AwsError> {
    let object =
        value.as_object().ok_or_else(|| invalid_parameter_error("item"))?;
    object
        .iter()
        .map(|(name, value)| {
            json_to_attribute_value(value).map(|value| (name.clone(), value))
        })
        .collect()
}

fn json_to_attribute_value(
    value: &Value,
) -> Result<DynamoDbAttributeValue, AwsError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_parameter_error("AttributeValue"))?;
    if object.len() != 1 {
        return Err(invalid_parameter_error("AttributeValue"));
    }
    let Some((kind, value)) = object.iter().next() else {
        return Err(invalid_parameter_error("AttributeValue"));
    };
    match kind.as_str() {
        "B" => value
            .as_str()
            .ok_or_else(|| invalid_parameter_error("AttributeValue"))
            .and_then(|value| {
                BASE64_STANDARD
                    .decode(value)
                    .map(AttributeValue::Binary)
                    .map_err(|_| invalid_parameter_error("AttributeValue"))
            }),
        "BOOL" => value
            .as_bool()
            .map(AttributeValue::Bool)
            .ok_or_else(|| invalid_parameter_error("AttributeValue")),
        "BS" => value
            .as_array()
            .ok_or_else(|| invalid_parameter_error("AttributeValue"))
            .and_then(|values| {
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or_else(|| {
                                invalid_parameter_error("AttributeValue")
                            })
                            .and_then(|value| {
                                BASE64_STANDARD.decode(value).map_err(|_| {
                                    invalid_parameter_error("AttributeValue")
                                })
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(AttributeValue::BinarySet)
            }),
        "L" => value
            .as_array()
            .ok_or_else(|| invalid_parameter_error("AttributeValue"))
            .and_then(|values| {
                values
                    .iter()
                    .map(json_to_attribute_value)
                    .collect::<Result<Vec<_>, _>>()
                    .map(AttributeValue::List)
            }),
        "M" => value
            .as_object()
            .ok_or_else(|| invalid_parameter_error("AttributeValue"))
            .and_then(|values| {
                values
                    .iter()
                    .map(|(name, value)| {
                        json_to_attribute_value(value)
                            .map(|value| (name.clone(), value))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
                    .map(AttributeValue::Map)
            }),
        "N" => number_string_json(value).map(AttributeValue::Number),
        "NS" => value
            .as_array()
            .ok_or_else(|| invalid_parameter_error("AttributeValue"))
            .and_then(|values| {
                values
                    .iter()
                    .map(number_string_json)
                    .collect::<Result<Vec<_>, _>>()
                    .map(AttributeValue::NumberSet)
            }),
        "NULL" => value
            .as_bool()
            .map(AttributeValue::Null)
            .ok_or_else(|| invalid_parameter_error("AttributeValue")),
        "S" => value
            .as_str()
            .map(|value| AttributeValue::String(value.to_owned()))
            .ok_or_else(|| invalid_parameter_error("AttributeValue")),
        "SS" => value
            .as_array()
            .ok_or_else(|| invalid_parameter_error("AttributeValue"))
            .and_then(|values| {
                values
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            invalid_parameter_error("AttributeValue")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(AttributeValue::StringSet)
            }),
        _ => Err(invalid_parameter_error("AttributeValue")),
    }
}

fn number_string_json(value: &Value) -> Result<String, AwsError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(value) => Ok(value.clone()),
        _ => Err(invalid_parameter_error("AttributeValue")),
    }
}

fn parse_json_body(body: &[u8]) -> Result<Value, AwsError> {
    if body.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    serde_json::from_slice(body).map_err(|_| invalid_parameter_error("body"))
}

fn serialize_json_body(body: &Value) -> Result<Vec<u8>, AwsError> {
    serde_json::to_vec(body).map_err(|_| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            "Failed to serialize DynamoDB response body.",
            500,
            false,
        )
    })
}

fn transaction_canceled_error(
    error: AwsError,
    cancellation_reasons: &[DynamoDbCancellationReason],
) -> Result<DynamoDbJsonError, DynamoDbJsonError> {
    let body = json!({
        "__type": error.code(),
        "message": error.message(),
        "CancellationReasons": cancellation_reasons
            .iter()
            .map(|reason| {
                let mut value = json!({
                    "Code": reason.code.as_deref().unwrap_or("None"),
                });
                if let Some(message) = reason.message.as_ref() {
                    insert_object_field(
                        &mut value,
                        "Message",
                        Value::String(message.clone()),
                    );
                }
                value
            })
            .collect::<Vec<_>>(),
    });
    let body = serialize_json_body(&body).map_err(DynamoDbJsonError::from)?;

    Ok(DynamoDbJsonError::CustomAwsJson { body, error })
}

fn required_string_json(
    body: &Value,
    field: &str,
) -> Result<String, AwsError> {
    optional_string_json(body, field)
        .map(str::to_owned)
        .ok_or_else(|| missing_field_error(field))
}

fn required_table_name_json(
    body: &Value,
    field: &str,
) -> Result<DynamoDbTableName, AwsError> {
    DynamoDbTableName::parse(required_string_json(body, field)?)
        .map_err(|error| error.to_aws_error())
}

fn optional_string_json<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(Value::as_str)
}

fn optional_string_from_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Option<&'a str> {
    object.get(field).and_then(Value::as_str)
}

fn required_string_from_object(
    object: &Map<String, Value>,
    field: &str,
) -> Result<String, AwsError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| missing_field_error(field))
}

fn required_table_name_from_object(
    object: &Map<String, Value>,
    field: &str,
) -> Result<DynamoDbTableName, AwsError> {
    DynamoDbTableName::parse(required_string_from_object(object, field)?)
        .map_err(|error| error.to_aws_error())
}

fn required_array_json<'a>(
    body: &'a Value,
    field: &str,
) -> Result<&'a [Value], AwsError> {
    body.get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| missing_field_error(field))
}

fn optional_array_json<'a>(
    body: &'a Value,
    field: &str,
) -> Result<Option<&'a [Value]>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    value
        .as_array()
        .map(Vec::as_slice)
        .map(Some)
        .ok_or_else(|| invalid_parameter_error(field))
}

fn required_array_from_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a [Value], AwsError> {
    object
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| missing_field_error(field))
}

fn optional_array_from_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Option<&'a [Value]> {
    object.get(field).and_then(Value::as_array).map(Vec::as_slice)
}

fn optional_usize_json(
    body: &Value,
    field: &str,
) -> Result<Option<usize>, AwsError> {
    let Some(value) = body.get(field) else {
        return Ok(None);
    };
    match value {
        Value::Number(number) => number
            .as_u64()
            .and_then(|value| value.try_into().ok())
            .map(Some)
            .ok_or_else(|| invalid_parameter_error(field)),
        Value::String(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| invalid_parameter_error(field)),
        _ => Err(invalid_parameter_error(field)),
    }
}

fn optional_bool_json(body: &Value, field: &str) -> Option<bool> {
    body.get(field).and_then(Value::as_bool)
}

fn optional_billing_mode_json(
    body: &Value,
    field: &str,
) -> Result<Option<DynamoDbBillingMode>, AwsError> {
    optional_string_json(body, field).map(billing_mode_json).transpose()
}

fn optional_provisioned_throughput_json(
    body: &Value,
    field: &str,
) -> Result<Option<DynamoDbProvisionedThroughput>, AwsError> {
    body.get(field).map(provisioned_throughput_from_value).transpose()
}

fn provisioned_throughput_from_value(
    value: &Value,
) -> Result<DynamoDbProvisionedThroughput, AwsError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_parameter_error("ProvisionedThroughput"))?;
    Ok(DynamoDbProvisionedThroughput {
        read_capacity_units: required_u64_from_object(
            object,
            "ReadCapacityUnits",
        )?,
        write_capacity_units: required_u64_from_object(
            object,
            "WriteCapacityUnits",
        )?,
    })
}

fn stream_specification_from_value(
    value: &Value,
) -> Result<DynamoDbStreamSpecification, AwsError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_parameter_error("StreamSpecification"))?;
    let stream_enabled = object
        .get("StreamEnabled")
        .and_then(Value::as_bool)
        .ok_or_else(|| missing_field_error("StreamEnabled"))?;
    let stream_view_type = object
        .get("StreamViewType")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| invalid_parameter_error("StreamViewType"))
                .and_then(|value| {
                    DynamoDbStreamViewType::parse(value)
                        .map_err(|error| error.to_aws_error())
                })
        })
        .transpose()?;

    Ok(DynamoDbStreamSpecification { stream_enabled, stream_view_type })
}

fn required_u64_from_object(
    object: &Map<String, Value>,
    field: &str,
) -> Result<u64, AwsError> {
    match object.get(field) {
        Some(Value::Number(number)) => {
            number.as_u64().ok_or_else(|| invalid_parameter_error(field))
        }
        Some(Value::String(value)) => {
            value.parse::<u64>().map_err(|_| invalid_parameter_error(field))
        }
        Some(_) => Err(invalid_parameter_error(field)),
        None => Err(missing_field_error(field)),
    }
}

fn scalar_attribute_type_json(
    value: &str,
) -> Result<DynamoDbScalarAttributeType, AwsError> {
    match value {
        "B" => Ok(DynamoDbScalarAttributeType::Binary),
        "N" => Ok(DynamoDbScalarAttributeType::Number),
        "S" => Ok(DynamoDbScalarAttributeType::String),
        _ => Err(validation_error(format!(
            "Unsupported DynamoDB AttributeType `{value}`."
        ))),
    }
}

fn key_type_json(value: &str) -> Result<DynamoDbKeyType, AwsError> {
    match value {
        "HASH" => Ok(DynamoDbKeyType::Hash),
        "RANGE" => Ok(DynamoDbKeyType::Range),
        _ => Err(validation_error(format!(
            "Unsupported DynamoDB KeyType `{value}`."
        ))),
    }
}

fn billing_mode_json(value: &str) -> Result<DynamoDbBillingMode, AwsError> {
    match value {
        "PAY_PER_REQUEST" => Ok(DynamoDbBillingMode::PayPerRequest),
        "PROVISIONED" => Ok(DynamoDbBillingMode::Provisioned),
        _ => Err(validation_error(format!(
            "Unsupported BillingMode `{value}`."
        ))),
    }
}

fn projection_type_json(
    value: &str,
) -> Result<DynamoDbProjectionType, AwsError> {
    match value {
        "ALL" => Ok(DynamoDbProjectionType::All),
        "INCLUDE" => Ok(DynamoDbProjectionType::Include),
        "KEYS_ONLY" => Ok(DynamoDbProjectionType::KeysOnly),
        _ => Err(validation_error(format!(
            "Unsupported ProjectionType `{value}`."
        ))),
    }
}

fn scalar_attribute_type_string(
    value: DynamoDbScalarAttributeType,
) -> &'static str {
    match value {
        DynamoDbScalarAttributeType::Binary => "B",
        DynamoDbScalarAttributeType::Number => "N",
        DynamoDbScalarAttributeType::String => "S",
    }
}

fn key_type_string(value: DynamoDbKeyType) -> &'static str {
    match value {
        DynamoDbKeyType::Hash => "HASH",
        DynamoDbKeyType::Range => "RANGE",
    }
}

fn billing_mode_string(value: DynamoDbBillingMode) -> &'static str {
    match value {
        DynamoDbBillingMode::PayPerRequest => "PAY_PER_REQUEST",
        DynamoDbBillingMode::Provisioned => "PROVISIONED",
    }
}

fn projection_type_string(value: DynamoDbProjectionType) -> &'static str {
    match value {
        DynamoDbProjectionType::All => "ALL",
        DynamoDbProjectionType::Include => "INCLUDE",
        DynamoDbProjectionType::KeysOnly => "KEYS_ONLY",
    }
}

fn stream_view_type_string(value: DynamoDbStreamViewType) -> &'static str {
    match value {
        DynamoDbStreamViewType::KeysOnly => "KEYS_ONLY",
        DynamoDbStreamViewType::NewImage => "NEW_IMAGE",
        DynamoDbStreamViewType::OldImage => "OLD_IMAGE",
        DynamoDbStreamViewType::NewAndOldImages => "NEW_AND_OLD_IMAGES",
    }
}

fn table_status_string(value: DynamoDbTableStatus) -> &'static str {
    match value {
        DynamoDbTableStatus::Active => "ACTIVE",
        DynamoDbTableStatus::Deleting => "DELETING",
    }
}

fn stream_status_string(value: DynamoDbStreamStatus) -> &'static str {
    match value {
        DynamoDbStreamStatus::Disabled => "DISABLED",
        DynamoDbStreamStatus::Enabled => "ENABLED",
    }
}

fn time_to_live_status_string(
    value: DynamoDbTimeToLiveStatus,
) -> &'static str {
    match value {
        DynamoDbTimeToLiveStatus::Disabled => "DISABLED",
        DynamoDbTimeToLiveStatus::Disabling => "DISABLING",
        DynamoDbTimeToLiveStatus::Enabled => "ENABLED",
        DynamoDbTimeToLiveStatus::Enabling => "ENABLING",
    }
}

fn validation_error(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "ValidationException",
        message.into(),
        400,
        true,
    )
}

fn missing_field_error(field: &str) -> AwsError {
    validation_error(format!("Missing required field `{field}`."))
}

fn invalid_parameter_error(field: &str) -> AwsError {
    validation_error(format!("The `{field}` parameter is not valid JSON."))
}

fn unsupported_operation_error(action: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "UnknownOperationException",
        format!("Operation {action} is not supported."),
        400,
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws::{ProtocolFamily, RequestContext, ServiceName};
    use serde_json::json;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    fn service() -> DynamoDbService {
        DynamoDbService::new(&StorageFactory::new(StorageConfig::new(
            "/tmp/http-dynamodb-tests",
            StorageMode::Memory,
        )))
        .expect("dynamodb service should build")
    }

    fn context(operation: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::DynamoDb,
            ProtocolFamily::AwsJson10,
            operation,
            None,
            false,
        )
        .expect("request context should build")
    }

    fn request_bytes(target: &str, body: &Value) -> Vec<u8> {
        let body = serde_json::to_string(body)
            .expect("request body should serialize");

        format!(
            "POST / HTTP/1.1\r\nHost: localhost:4566\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: {target}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn call_target(
        dynamodb: &DynamoDbService,
        target_prefix: &str,
        operation: &str,
        body: Value,
    ) -> Result<Value, DynamoDbJsonError> {
        let target = format!("{target_prefix}.{operation}");
        let request_bytes = request_bytes(&target, &body);
        let request =
            HttpRequest::parse(&request_bytes).expect("request should parse");
        let response = handle_json(dynamodb, &request, &context(operation))?;

        serde_json::from_slice(&response).map_err(|error| {
            DynamoDbJsonError::from(
                AwsError::custom(
                    AwsErrorFamily::Internal,
                    "InternalFailure",
                    format!(
                        "Failed to decode DynamoDB test response: {error}"
                    ),
                    500,
                    false,
                )
                .expect("test response decode error should build"),
            )
        })
    }

    fn call(
        dynamodb: &DynamoDbService,
        operation: &str,
        body: Value,
    ) -> Result<Value, AwsError> {
        call_target(dynamodb, "DynamoDB_20120810", operation, body)
            .map_err(|error| error.error().clone())
    }

    fn call_streams(
        dynamodb: &DynamoDbService,
        operation: &str,
        body: Value,
    ) -> Result<Value, AwsError> {
        call_target(dynamodb, "DynamoDBStreams_20120810", operation, body)
            .map_err(|error| error.error().clone())
    }

    fn call_json_error(
        dynamodb: &DynamoDbService,
        operation: &str,
        body: Value,
    ) -> DynamoDbJsonError {
        call_target(dynamodb, "DynamoDB_20120810", operation, body)
            .expect_err("operation should fail")
    }

    fn create_orders_table(dynamodb: &DynamoDbService) -> Value {
        call(
            dynamodb,
            "CreateTable",
            json!({
                "AttributeDefinitions": [
                    {"AttributeName": "tenant", "AttributeType": "S"},
                    {"AttributeName": "order", "AttributeType": "S"},
                    {"AttributeName": "status", "AttributeType": "S"},
                    {"AttributeName": "created_at", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [{
                    "IndexName": "status-index",
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"}
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    }
                }],
                "KeySchema": [
                    {"AttributeName": "tenant", "KeyType": "HASH"},
                    {"AttributeName": "order", "KeyType": "RANGE"}
                ],
                "LocalSecondaryIndexes": [{
                    "IndexName": "created-at-index",
                    "KeySchema": [
                        {"AttributeName": "tenant", "KeyType": "HASH"},
                        {"AttributeName": "created_at", "KeyType": "RANGE"}
                    ],
                    "Projection": {
                        "ProjectionType": "ALL"
                    }
                }],
                "TableName": "orders"
            }),
        )
        .expect("create orders table should succeed")
    }

    fn seed_orders_table(dynamodb: &DynamoDbService) {
        let put = call(
            dynamodb,
            "PutItem",
            json!({
                "Item": {
                    "created_at": {"S": "2026-03-25T00:00:00Z"},
                    "order": {"S": "001"},
                    "status": {"S": "pending"},
                    "tenant": {"S": "tenant-a"},
                    "total": {"N": "10"}
                },
                "TableName": "orders"
            }),
        )
        .expect("put item should succeed");
        assert_eq!(put, json!({}));

        let batch_write = call(
            dynamodb,
            "BatchWriteItem",
            json!({
                "RequestItems": {
                    "orders": [
                        {
                            "PutRequest": {
                                "Item": {
                                    "created_at": {"S": "2026-03-25T00:01:00Z"},
                                    "order": {"S": "002"},
                                    "status": {"S": "shipped"},
                                    "tenant": {"S": "tenant-a"},
                                    "total": {"N": "20"}
                                }
                            }
                        },
                        {
                            "PutRequest": {
                                "Item": {
                                    "created_at": {"S": "2026-03-25T00:02:00Z"},
                                    "order": {"S": "001"},
                                    "status": {"S": "pending"},
                                    "tenant": {"S": "tenant-b"},
                                    "total": {"N": "30"}
                                }
                            }
                        }
                    ]
                }
            }),
        )
        .expect("batch write should succeed");
        assert_eq!(batch_write, json!({"UnprocessedItems": {}}));
    }

    #[test]
    fn dynamodb_core_json_table_lifecycle_round_trip() {
        let dynamodb = service();
        let created = create_orders_table(&dynamodb);
        assert_eq!(created["TableDescription"]["TableName"], "orders");
        assert_eq!(created["TableDescription"]["TableStatus"], "ACTIVE");
        assert_eq!(
            created["TableDescription"]["GlobalSecondaryIndexes"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            created["TableDescription"]["LocalSecondaryIndexes"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );

        let described =
            call(&dynamodb, "DescribeTable", json!({"TableName": "orders"}))
                .expect("describe table should succeed");
        assert_eq!(described["Table"]["TableName"], "orders");

        let listed = call(&dynamodb, "ListTables", json!({}))
            .expect("list tables should succeed");
        assert_eq!(listed["TableNames"], json!(["orders"]));

        let updated_table = call(
            &dynamodb,
            "UpdateTable",
            json!({
                "BillingMode": "PROVISIONED",
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 4,
                    "WriteCapacityUnits": 8
                },
                "TableName": "orders"
            }),
        )
        .expect("update table should succeed");
        assert_eq!(
            updated_table["TableDescription"]["BillingModeSummary"]["BillingMode"],
            "PROVISIONED"
        );
        assert_eq!(
            updated_table["TableDescription"]["ProvisionedThroughput"]["ReadCapacityUnits"],
            4
        );

        let deleted_table =
            call(&dynamodb, "DeleteTable", json!({"TableName": "orders"}))
                .expect("delete table should succeed");
        assert_eq!(
            deleted_table["TableDescription"]["TableStatus"],
            "DELETING"
        );

        let missing =
            call(&dynamodb, "DescribeTable", json!({"TableName": "orders"}))
                .expect_err("deleted table should not describe");
        assert_eq!(missing.code(), "ResourceNotFoundException");
    }

    #[test]
    fn dynamodb_core_json_item_access_paths_round_trip() {
        let dynamodb = service();
        create_orders_table(&dynamodb);
        seed_orders_table(&dynamodb);

        let fetched = call(
            &dynamodb,
            "GetItem",
            json!({
                "Key": {
                    "order": {"S": "001"},
                    "tenant": {"S": "tenant-a"}
                },
                "TableName": "orders"
            }),
        )
        .expect("get item should succeed");
        assert_eq!(fetched["Item"]["status"]["S"], "pending");

        let updated = call(
            &dynamodb,
            "UpdateItem",
            json!({
                "ExpressionAttributeValues": {
                    ":next": {"N": "11"},
                    ":note": {"S": "priority"}
                },
                "Key": {
                    "order": {"S": "001"},
                    "tenant": {"S": "tenant-a"}
                },
                "ReturnValues": "ALL_NEW",
                "TableName": "orders",
                "UpdateExpression": "SET total = :next, note = :note"
            }),
        )
        .expect("update item should succeed");
        assert_eq!(updated["Attributes"]["total"]["N"], "11");
        assert_eq!(updated["Attributes"]["note"]["S"], "priority");

        let queried = call(
            &dynamodb,
            "Query",
            json!({
                "ExpressionAttributeNames": {
                    "#order": "order"
                },
                "ExpressionAttributeValues": {
                    ":prefix": {"S": "00"},
                    ":tenant": {"S": "tenant-a"}
                },
                "KeyConditionExpression": "tenant = :tenant AND begins_with(#order, :prefix)",
                "TableName": "orders"
            }),
        )
        .expect("query should succeed");
        assert_eq!(queried["Count"], 2);
        assert_eq!(queried["ScannedCount"], 2);
        assert_eq!(queried["Items"].as_array().map(Vec::len), Some(2));

        let indexed = call(
            &dynamodb,
            "Query",
            json!({
                "ExpressionAttributeValues": {
                    ":status": {"S": "pending"}
                },
                "IndexName": "status-index",
                "KeyConditionExpression": "status = :status",
                "TableName": "orders"
            }),
        )
        .expect("indexed query should succeed");
        assert_eq!(indexed["Count"], 2);

        let scanned = call(
            &dynamodb,
            "Scan",
            json!({
                "ExpressionAttributeValues": {
                    ":high": {"N": "40"},
                    ":low": {"N": "15"}
                },
                "FilterExpression": "total BETWEEN :low AND :high",
                "Limit": 2,
                "TableName": "orders"
            }),
        )
        .expect("scan should succeed");
        assert_eq!(scanned["Count"], 1);
        assert_eq!(scanned["ScannedCount"], 2);
        assert!(scanned.get("LastEvaluatedKey").is_some());

        let batch_get = call(
            &dynamodb,
            "BatchGetItem",
            json!({
                "RequestItems": {
                    "orders": {
                        "Keys": [
                            {
                                "order": {"S": "001"},
                                "tenant": {"S": "tenant-a"}
                            },
                            {
                                "order": {"S": "002"},
                                "tenant": {"S": "tenant-a"}
                            },
                            {
                                "order": {"S": "missing"},
                                "tenant": {"S": "tenant-a"}
                            }
                        ]
                    }
                }
            }),
        )
        .expect("batch get should succeed");
        assert_eq!(
            batch_get["Responses"]["orders"].as_array().map(Vec::len),
            Some(2)
        );
        assert_eq!(batch_get["UnprocessedKeys"], json!({}));

        let deleted = call(
            &dynamodb,
            "DeleteItem",
            json!({
                "ConditionExpression": "attribute_exists(tenant)",
                "Key": {
                    "order": {"S": "001"},
                    "tenant": {"S": "tenant-b"}
                },
                "ReturnValues": "ALL_OLD",
                "TableName": "orders"
            }),
        )
        .expect("delete item should succeed");
        assert_eq!(deleted["Attributes"]["status"]["S"], "pending");
    }

    #[test]
    fn dynamodb_core_json_errors_are_explicit_and_non_mutating() {
        let dynamodb = service();
        call(
            &dynamodb,
            "CreateTable",
            json!({
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ],
                "TableName": "users"
            }),
        )
        .expect("users table should create");
        call(
            &dynamodb,
            "PutItem",
            json!({
                "Item": {
                    "id": {"S": "user-1"},
                    "version": {"N": "1"}
                },
                "TableName": "users"
            }),
        )
        .expect("seed item should write");

        let duplicate = call(
            &dynamodb,
            "CreateTable",
            json!({
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ],
                "TableName": "users"
            }),
        )
        .expect_err("duplicate table should fail");
        assert_eq!(duplicate.code(), "ResourceInUseException");
        assert!(duplicate.message().contains("users"));

        let invalid_schema = call(
            &dynamodb,
            "CreateTable",
            json!({
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "other", "KeyType": "HASH"}
                ],
                "TableName": "broken"
            }),
        )
        .expect_err("invalid schema should fail");
        assert_eq!(invalid_schema.code(), "ValidationException");

        let conditional_put = call(
            &dynamodb,
            "PutItem",
            json!({
                "ConditionExpression": "attribute_not_exists(id)",
                "Item": {
                    "id": {"S": "user-1"},
                    "version": {"N": "2"}
                },
                "TableName": "users"
            }),
        )
        .expect_err("conditional put should fail");
        assert_eq!(conditional_put.code(), "ConditionalCheckFailedException");

        let stored = call(
            &dynamodb,
            "GetItem",
            json!({
                "Key": {
                    "id": {"S": "user-1"}
                },
                "TableName": "users"
            }),
        )
        .expect("stored item should remain");
        assert_eq!(stored["Item"]["version"]["N"], "1");

        let unsupported_update = call(
            &dynamodb,
            "UpdateTable",
            json!({
                "GlobalSecondaryIndexUpdates": [],
                "TableName": "users"
            }),
        )
        .expect_err("unsupported update-table shapes should fail");
        assert_eq!(unsupported_update.code(), "ValidationException");
        assert!(
            unsupported_update
                .message()
                .contains("GlobalSecondaryIndexUpdates")
        );
    }

    #[test]
    fn dynamodb_core_json_helper_functions_cover_supported_shapes_and_errors()
    {
        let nested = json!({
            "M": {
                "blob": {"B": "AQID"},
                "enabled": {"BOOL": true},
                "items": {"L": [{"N": "1"}, {"S": "two"}]},
                "labels": {"SS": ["a", "b"]},
                "metrics": {"NS": ["7", "9"]},
                "nested": {"M": {"flag": {"NULL": true}}}
            }
        });
        let attribute = json_to_attribute_value(&nested)
            .expect("nested attribute value should parse");
        assert_eq!(attribute_value_json(&attribute), nested);
        assert_eq!(number_string_json(&json!(12)).as_deref(), Ok("12"));
        assert_eq!(number_string_json(&json!("12")).as_deref(), Ok("12"));
        assert_eq!(
            parse_json_body(b"").expect("empty body should parse"),
            json!({})
        );
        assert_eq!(
            optional_bool_json(
                &json!({"ScanIndexForward": false}),
                "ScanIndexForward"
            ),
            Some(false)
        );
        assert_eq!(
            optional_billing_mode_json(
                &json!({"BillingMode": "PAY_PER_REQUEST"}),
                "BillingMode",
            )
            .expect("billing mode should parse"),
            Some(DynamoDbBillingMode::PayPerRequest)
        );
        assert_eq!(
            optional_usize_json(&json!({"Limit": "3"}), "Limit")
                .expect("string limits should parse"),
            Some(3)
        );

        let update_error = attribute_updates_field(
            &json!({
                "AttributeUpdates": {
                    "name": {
                        "Action": "ADD"
                    }
                }
            }),
            "AttributeUpdates",
        )
        .expect_err("unsupported attribute update action should fail");
        assert_eq!(update_error.code(), "ValidationException");

        let batch_error = batch_write_item_input(&json!({
            "RequestItems": {
                "orders": [
                    {
                        "Unsupported": {}
                    }
                ]
            }
        }))
        .expect_err("invalid batch write shape should fail");
        assert_eq!(batch_error.code(), "ValidationException");

        let stream_specification = update_table_input(&json!({
            "StreamSpecification": {
                "StreamEnabled": true
            },
            "TableName": "orders"
        }))
        .expect("stream specification should parse");
        assert_eq!(
            stream_specification.stream_specification,
            Some(DynamoDbStreamSpecification {
                stream_enabled: true,
                stream_view_type: None,
            })
        );

        let serialized = serialize_json_body(&json!({"ok": true}))
            .expect("response bodies should serialize");
        let serialized: Value = serde_json::from_slice(&serialized)
            .expect("serialized response should decode");
        assert_eq!(serialized["ok"], true);

        let invalid_request = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: DynamoDB_20120810.CreateTable\r\nContent-Length: 1\r\n\r\n{",
        )
        .expect("request should parse");
        let invalid_json =
            handle_json(&service(), &invalid_request, &context("CreateTable"))
                .expect_err("invalid json should fail");
        assert_eq!(invalid_json.code(), "ValidationException");
        assert!(invalid_json.error().message().contains("body"));

        let missing_target = HttpRequest::parse(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: 2\r\n\r\n{}",
        )
        .expect("request should parse");
        let missing_target =
            handle_json(&service(), &missing_target, &context("ListTables"))
                .expect_err("missing target should fail");
        assert_eq!(missing_target.code(), "UnknownOperationException");
        assert!(missing_target.error().message().contains("X-Amz-Target"));
    }

    #[test]
    fn dynamodb_transactions_json_round_trip_ttl_tags_and_streams() {
        let dynamodb = service();
        let created = call(
            &dynamodb,
            "CreateTable",
            json!({
                "AttributeDefinitions": [
                    {"AttributeName": "tenant", "AttributeType": "S"},
                    {"AttributeName": "order", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [
                    {"AttributeName": "tenant", "KeyType": "HASH"},
                    {"AttributeName": "order", "KeyType": "RANGE"}
                ],
                "TableName": "orders"
            }),
        )
        .expect("create table should succeed");
        let table_arn = created["TableDescription"]["TableArn"]
            .as_str()
            .expect("table arn should be present")
            .to_owned();

        let updated = call(
            &dynamodb,
            "UpdateTable",
            json!({
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                },
                "TableName": "orders"
            }),
        )
        .expect("stream update should succeed");
        let table = &updated["TableDescription"];
        assert_eq!(table["StreamSpecification"]["StreamEnabled"], true);
        assert_eq!(
            table["StreamSpecification"]["StreamViewType"],
            "NEW_AND_OLD_IMAGES"
        );

        let ttl = call(
            &dynamodb,
            "UpdateTimeToLive",
            json!({
                "TableName": "orders",
                "TimeToLiveSpecification": {
                    "AttributeName": "expires_at",
                    "Enabled": true
                }
            }),
        )
        .expect("ttl update should succeed");
        assert_eq!(
            ttl["TimeToLiveSpecification"],
            json!({
                "AttributeName": "expires_at",
                "Enabled": true
            })
        );

        let ttl_description = call(
            &dynamodb,
            "DescribeTimeToLive",
            json!({
                "TableName": "orders"
            }),
        )
        .expect("ttl description should succeed");
        assert_eq!(
            ttl_description["TimeToLiveDescription"]["AttributeName"],
            "expires_at"
        );
        assert_eq!(
            ttl_description["TimeToLiveDescription"]["TimeToLiveStatus"],
            "ENABLING"
        );

        call(
            &dynamodb,
            "TagResource",
            json!({
                "ResourceArn": table_arn,
                "Tags": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "payments"}
                ]
            }),
        )
        .expect("tagging should succeed");
        let tags = call(
            &dynamodb,
            "ListTagsOfResource",
            json!({
                "ResourceArn": table["TableArn"]
            }),
        )
        .expect("list tags should succeed");
        assert_eq!(
            tags["Tags"],
            json!([
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "payments"}
            ])
        );
        call(
            &dynamodb,
            "UntagResource",
            json!({
                "ResourceArn": table["TableArn"],
                "TagKeys": ["env"]
            }),
        )
        .expect("untag should succeed");
        let tags = call(
            &dynamodb,
            "ListTagsOfResource",
            json!({
                "ResourceArn": table["TableArn"]
            }),
        )
        .expect("list tags should succeed");
        assert_eq!(
            tags["Tags"],
            json!([{ "Key": "team", "Value": "payments" }])
        );

        call(
            &dynamodb,
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {
                        "Put": {
                            "Item": {
                                "tenant": {"S": "tenant-a"},
                                "order": {"S": "001"},
                                "status": {"S": "pending"}
                            },
                            "TableName": "orders"
                        }
                    },
                    {
                        "Put": {
                            "Item": {
                                "tenant": {"S": "tenant-a"},
                                "order": {"S": "002"},
                                "status": {"S": "confirmed"}
                            },
                            "TableName": "orders"
                        }
                    }
                ]
            }),
        )
        .expect("transactional write should succeed");

        let fetched = call(
            &dynamodb,
            "TransactGetItems",
            json!({
                "TransactItems": [
                    {
                        "Get": {
                            "Key": {
                                "tenant": {"S": "tenant-a"},
                                "order": {"S": "001"}
                            },
                            "TableName": "orders"
                        }
                    },
                    {
                        "Get": {
                            "Key": {
                                "tenant": {"S": "tenant-a"},
                                "order": {"S": "002"}
                            },
                            "TableName": "orders"
                        }
                    }
                ]
            }),
        )
        .expect("transactional get should succeed");
        assert_eq!(fetched["Responses"].as_array().map(Vec::len), Some(2));
        assert_eq!(fetched["Responses"][0]["Item"]["status"]["S"], "pending");
        assert_eq!(
            fetched["Responses"][1]["Item"]["status"]["S"],
            "confirmed"
        );

        let streams = call_streams(
            &dynamodb,
            "ListStreams",
            json!({
                "TableName": "orders"
            }),
        )
        .expect("list streams should succeed");
        let stream_arn = streams["Streams"][0]["StreamArn"]
            .as_str()
            .expect("stream arn should be present")
            .to_owned();

        let description = call_streams(
            &dynamodb,
            "DescribeStream",
            json!({
                "StreamArn": stream_arn
            }),
        )
        .expect("describe stream should succeed");
        assert_eq!(
            description["StreamDescription"]["StreamStatus"],
            "ENABLED"
        );
        assert_eq!(
            description["StreamDescription"]["StreamViewType"],
            "NEW_AND_OLD_IMAGES"
        );
        let shard_id =
            description["StreamDescription"]["Shards"][0]["ShardId"]
                .as_str()
                .expect("stream shard should be present")
                .to_owned();

        let iterator = call_streams(
            &dynamodb,
            "GetShardIterator",
            json!({
                "ShardId": shard_id,
                "ShardIteratorType": "TRIM_HORIZON",
                "StreamArn": description["StreamDescription"]["StreamArn"]
            }),
        )
        .expect("get shard iterator should succeed");
        let records = call_streams(
            &dynamodb,
            "GetRecords",
            json!({
                "ShardIterator": iterator["ShardIterator"]
            }),
        )
        .expect("get records should succeed");
        assert_eq!(records["Records"].as_array().map(Vec::len), Some(2));
        assert_eq!(records["Records"][0]["eventName"], "INSERT");
        assert!(
            records["Records"][0]["dynamodb"]["SequenceNumber"]
                .as_str()
                .expect("sequence number should be a string")
                .ends_with('1')
        );
        assert_eq!(
            records["Records"][0]["dynamodb"]["Keys"],
            json!({
                "order": {"S": "001"},
                "tenant": {"S": "tenant-a"}
            })
        );
        assert_eq!(
            records["Records"][1]["dynamodb"]["NewImage"]["status"]["S"],
            "confirmed"
        );
        assert!(records.get("NextShardIterator").is_some());
    }

    #[test]
    fn dynamodb_transactions_json_errors_are_explicit() {
        let dynamodb = service();
        call(
            &dynamodb,
            "CreateTable",
            json!({
                "AttributeDefinitions": [
                    {"AttributeName": "id", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"}
                ],
                "TableName": "users"
            }),
        )
        .expect("users table should create");
        call(
            &dynamodb,
            "PutItem",
            json!({
                "Item": {
                    "id": {"S": "user-1"},
                    "version": {"N": "1"}
                },
                "TableName": "users"
            }),
        )
        .expect("seed item should write");

        let error = call_json_error(
            &dynamodb,
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {
                        "ConditionCheck": {
                            "ConditionExpression": "attribute_not_exists(id)",
                            "Key": {
                                "id": {"S": "user-1"}
                            },
                            "TableName": "users"
                        }
                    },
                    {
                        "Put": {
                            "Item": {
                                "id": {"S": "user-2"},
                                "version": {"N": "1"}
                            },
                            "TableName": "users"
                        }
                    }
                ]
            }),
        );
        assert_eq!(error.code(), "TransactionCanceledException");
        let body = serde_json::from_slice::<Value>(
            error.body().expect("transaction cancel should serialize a body"),
        )
        .expect("transaction cancel response should be json");
        assert_eq!(body["__type"], "TransactionCanceledException");
        assert_eq!(
            body["CancellationReasons"],
            json!([
                {
                    "Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed"
                },
                {
                    "Code": "None"
                }
            ])
        );

        let stored = call(
            &dynamodb,
            "GetItem",
            json!({
                "Key": {
                    "id": {"S": "user-2"}
                },
                "TableName": "users"
            }),
        )
        .expect("get item should succeed");
        assert_eq!(stored, json!({}));

        let invalid_iterator = call_streams(
            &dynamodb,
            "GetRecords",
            json!({
                "ShardIterator": "not-base64"
            }),
        )
        .expect_err("invalid shard iterator should fail");
        assert_eq!(invalid_iterator.code(), "ValidationException");
        assert!(invalid_iterator.message().contains("valid shard iterator"));
    }
}
