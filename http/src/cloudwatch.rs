pub(crate) use crate::aws_error_shape::{
    AwsErrorShape, QueryModeAwsErrorShape,
};
use crate::query::{QueryParameters, missing_parameter_error};
use crate::request::HttpRequest;
use crate::xml::XmlBuilder;
use aws::{AwsError, AwsErrorFamily, RequestContext};
use ciborium::value::{Integer as CborInteger, Value as CborValue};
use ciborium::{from_reader, into_writer};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use services::{
    AlarmStateValue, CloudWatchLogsError, CloudWatchMetricsError,
    CloudWatchScope, CloudWatchService, CreateLogGroupInput,
    CreateLogStreamInput, DeleteAlarmsInput, DeleteRetentionPolicyInput,
    DescribeAlarmsInput, DescribeAlarmsOutput, DescribeLogGroupsInput,
    DescribeLogGroupsOutput, DescribeLogStreamsInput,
    DescribeLogStreamsOutput, Dimension, DimensionFilter,
    FilterLogEventsInput, FilterLogEventsOutput, GetLogEventsInput,
    GetLogEventsOutput, GetMetricDataInput, GetMetricDataOutput,
    GetMetricStatisticsInput, GetMetricStatisticsOutput, InputLogEvent,
    ListMetricsInput, ListMetricsOutput, Metric, MetricAlarm, MetricDataQuery,
    MetricDatum, MetricNamespace, MetricStat, PutLogEventsInput,
    PutLogEventsOutput, PutMetricAlarmInput, PutRetentionPolicyInput, ScanBy,
    SetAlarmStateInput, StatisticSet,
};
use std::collections::{BTreeMap, BTreeSet};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const CLOUDWATCH_QUERY_XMLNS: &str =
    "http://monitoring.amazonaws.com/doc/2010-08-01/";
const REQUEST_ID: &str = "0000000000000000";
pub(crate) const CLOUDWATCH_QUERY_VERSION: &str = "2010-08-01";

enum MetricsResponse {
    DescribeAlarms(DescribeAlarmsOutput),
    Empty,
    GetMetricData(GetMetricDataOutput),
    GetMetricStatistics(GetMetricStatisticsOutput),
    ListMetrics(ListMetricsOutput),
}

enum LogsResponse {
    DescribeLogGroups(DescribeLogGroupsOutput),
    DescribeLogStreams(DescribeLogStreamsOutput),
    Empty,
    FilterLogEvents(FilterLogEventsOutput),
    GetLogEvents(GetLogEventsOutput),
    ListTags(BTreeMap<String, String>),
    PutLogEvents(PutLogEventsOutput),
}

pub(crate) fn is_metrics_query_action(action: &str) -> bool {
    matches!(
        action,
        "DeleteAlarms"
            | "DescribeAlarms"
            | "GetMetricData"
            | "GetMetricStatistics"
            | "ListMetrics"
            | "PutMetricAlarm"
            | "PutMetricData"
            | "SetAlarmState"
    )
}

pub(crate) fn handle_metrics_query(
    cloudwatch: &CloudWatchService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(request.body())?;
    let scope = cloudwatch_scope(context);
    let response = match context.operation() {
        "PutMetricData" => {
            cloudwatch
                .put_metric_data(
                    &scope,
                    parse_metric_namespace(
                        params.required("Namespace")?,
                        true,
                    )?,
                    parse_query_metric_data(&params, "MetricData.member")?,
                )
                .map_err(|error| error.to_aws_error(true))?;
            MetricsResponse::Empty
        }
        "ListMetrics" => MetricsResponse::ListMetrics(
            cloudwatch
                .list_metrics(
                    &scope,
                    ListMetricsInput {
                        dimensions: parse_query_dimension_filters(
                            &params,
                            "Dimensions.member",
                        )?,
                        metric_name: query_optional_string(
                            &params,
                            "MetricName",
                        ),
                        namespace: query_optional_string(&params, "Namespace")
                            .map(|value| parse_metric_namespace(&value, true))
                            .transpose()?,
                        next_token: query_optional_string(
                            &params,
                            "NextToken",
                        ),
                    },
                )
                .map_err(|error| error.to_aws_error(true))?,
        ),
        "GetMetricStatistics" => MetricsResponse::GetMetricStatistics(
            cloudwatch
                .get_metric_statistics(
                    &scope,
                    GetMetricStatisticsInput {
                        dimensions: parse_query_dimensions(
                            &params,
                            "Dimensions.member",
                        )?,
                        end_time_millis: query_required_timestamp_millis(
                            &params, "EndTime", true,
                        )?,
                        metric_name: params.required("MetricName")?.to_owned(),
                        namespace: parse_metric_namespace(
                            params.required("Namespace")?,
                            true,
                        )?,
                        period_seconds: query_required_i32(
                            &params, "Period", true,
                        )?,
                        start_time_millis: query_required_timestamp_millis(
                            &params,
                            "StartTime",
                            true,
                        )?,
                        statistics: query_string_list(
                            &params,
                            "Statistics.member",
                        ),
                        unit: query_optional_string(&params, "Unit"),
                    },
                )
                .map_err(|error| error.to_aws_error(true))?,
        ),
        "PutMetricAlarm" => {
            cloudwatch
                .put_metric_alarm(
                    &scope,
                    PutMetricAlarmInput {
                        actions_enabled: query_optional_bool(
                            &params,
                            "ActionsEnabled",
                            true,
                        )?,
                        alarm_actions: query_string_list(
                            &params,
                            "AlarmActions.member",
                        ),
                        alarm_description: query_optional_string(
                            &params,
                            "AlarmDescription",
                        ),
                        alarm_name: params.required("AlarmName")?.to_owned(),
                        comparison_operator: params
                            .required("ComparisonOperator")?
                            .to_owned(),
                        datapoints_to_alarm: query_optional_i32(
                            &params,
                            "DatapointsToAlarm",
                            true,
                        )?,
                        dimensions: parse_query_dimensions(
                            &params,
                            "Dimensions.member",
                        )?,
                        evaluation_periods: query_required_i32(
                            &params,
                            "EvaluationPeriods",
                            true,
                        )?,
                        insufficient_data_actions: query_string_list(
                            &params,
                            "InsufficientDataActions.member",
                        ),
                        metric_name: query_optional_string(
                            &params,
                            "MetricName",
                        ),
                        metrics: parse_query_metric_data_queries(
                            &params,
                            "Metrics.member",
                            true,
                        )?,
                        namespace: query_optional_string(&params, "Namespace")
                            .map(|value| parse_metric_namespace(&value, true))
                            .transpose()?,
                        ok_actions: query_string_list(
                            &params,
                            "OKActions.member",
                        ),
                        period_seconds: query_optional_i32(
                            &params, "Period", true,
                        )?,
                        statistic: query_optional_string(&params, "Statistic"),
                        threshold: query_optional_f64(
                            &params,
                            "Threshold",
                            true,
                        )?,
                        threshold_metric_id: query_optional_string(
                            &params,
                            "ThresholdMetricId",
                        ),
                        treat_missing_data: query_optional_string(
                            &params,
                            "TreatMissingData",
                        ),
                        unit: query_optional_string(&params, "Unit"),
                    },
                )
                .map_err(|error| error.to_aws_error(true))?;
            MetricsResponse::Empty
        }
        "DescribeAlarms" => MetricsResponse::DescribeAlarms(
            cloudwatch
                .describe_alarms(
                    &scope,
                    DescribeAlarmsInput {
                        alarm_name_prefix: query_optional_string(
                            &params,
                            "AlarmNamePrefix",
                        ),
                        alarm_names: query_string_list(
                            &params,
                            "AlarmNames.member",
                        ),
                        max_records: query_optional_i32(
                            &params,
                            "MaxRecords",
                            true,
                        )?,
                        next_token: query_optional_string(
                            &params,
                            "NextToken",
                        ),
                        state_value: query_optional_string(
                            &params,
                            "StateValue",
                        )
                        .map(|value| {
                            AlarmStateValue::parse(&value)
                                .map_err(|error| error.to_aws_error(true))
                        })
                        .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error(true))?,
        ),
        "DeleteAlarms" => {
            let alarm_names = query_string_list(&params, "AlarmNames.member");
            if alarm_names.is_empty() {
                return Err(missing_parameter_error("AlarmNames"));
            }
            cloudwatch
                .delete_alarms(&scope, DeleteAlarmsInput { alarm_names })
                .map_err(|error| error.to_aws_error(true))?;
            MetricsResponse::Empty
        }
        "SetAlarmState" => {
            cloudwatch
                .set_alarm_state(
                    &scope,
                    SetAlarmStateInput {
                        alarm_name: params.required("AlarmName")?.to_owned(),
                        state_reason: params
                            .required("StateReason")?
                            .to_owned(),
                        state_reason_data: query_optional_string(
                            &params,
                            "StateReasonData",
                        ),
                        state_value: AlarmStateValue::parse(
                            params.required("StateValue")?,
                        )
                        .map_err(|error| error.to_aws_error(true))?,
                    },
                )
                .map_err(|error| error.to_aws_error(true))?;
            MetricsResponse::Empty
        }
        "GetMetricData" => MetricsResponse::GetMetricData(
            cloudwatch
                .get_metric_data(
                    &scope,
                    GetMetricDataInput {
                        end_time_millis: query_required_timestamp_millis(
                            &params, "EndTime", true,
                        )?,
                        max_datapoints: query_optional_i32(
                            &params,
                            "MaxDatapoints",
                            true,
                        )?,
                        metric_data_queries: parse_query_metric_data_queries(
                            &params,
                            "MetricDataQueries.member",
                            true,
                        )?,
                        next_token: query_optional_string(
                            &params,
                            "NextToken",
                        ),
                        scan_by: query_optional_string(&params, "ScanBy")
                            .map(|value| {
                                ScanBy::parse(&value)
                                    .map_err(|error| error.to_aws_error(true))
                            })
                            .transpose()?,
                        start_time_millis: query_required_timestamp_millis(
                            &params,
                            "StartTime",
                            true,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error(true))?,
        ),
        other => return Err(unknown_operation_error(other)),
    };

    Ok(metrics_query_response(context.operation(), &response))
}

pub(crate) fn handle_metrics_json(
    cloudwatch: &CloudWatchService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let query_mode = request_query_mode(request);
    let body = parse_json_body(request.body(), query_mode)?;
    let response = execute_metrics_json(
        cloudwatch,
        context.operation(),
        &body,
        context,
        query_mode,
    )?;
    serialize_json_value(&metrics_json_response(&response, false))
}

pub(crate) fn handle_metrics_cbor(
    cloudwatch: &CloudWatchService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let query_mode = request_query_mode(request);
    let body = parse_cbor_body(request.body(), query_mode)?;
    let response = execute_metrics_json(
        cloudwatch,
        context.operation(),
        &body,
        context,
        query_mode,
    )?;
    serialize_cbor_value(&metrics_cbor_response(&response))
}

pub(crate) fn handle_logs_json(
    cloudwatch: &CloudWatchService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<Vec<u8>, AwsError> {
    let body = parse_logs_json_body(request.body())?;
    let scope = cloudwatch_scope(context);
    let response = match context.operation() {
        "CreateLogGroup" => {
            cloudwatch
                .create_log_group(
                    &scope,
                    CreateLogGroupInput {
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        tags: json_string_map(&body, "tags", false)?,
                    },
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "DeleteLogGroup" => {
            cloudwatch
                .delete_log_group(
                    &scope,
                    &json_required_string(&body, "logGroupName", false)?,
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "DescribeLogGroups" => LogsResponse::DescribeLogGroups(
            cloudwatch
                .describe_log_groups(
                    &scope,
                    DescribeLogGroupsInput {
                        log_group_name_prefix: json_optional_string(
                            &body,
                            "logGroupNamePrefix",
                        ),
                        next_token: json_optional_string(&body, "nextToken"),
                    },
                )
                .map_err(logs_aws_error)?,
        ),
        "CreateLogStream" => {
            cloudwatch
                .create_log_stream(
                    &scope,
                    CreateLogStreamInput {
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        log_stream_name: json_required_string(
                            &body,
                            "logStreamName",
                            false,
                        )?,
                    },
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "DeleteLogStream" => {
            cloudwatch
                .delete_log_stream(
                    &scope,
                    &json_required_string(&body, "logGroupName", false)?,
                    &json_required_string(&body, "logStreamName", false)?,
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "DescribeLogStreams" => LogsResponse::DescribeLogStreams(
            cloudwatch
                .describe_log_streams(
                    &scope,
                    DescribeLogStreamsInput {
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        log_stream_name_prefix: json_optional_string(
                            &body,
                            "logStreamNamePrefix",
                        ),
                        next_token: json_optional_string(&body, "nextToken"),
                    },
                )
                .map_err(logs_aws_error)?,
        ),
        "PutLogEvents" => LogsResponse::PutLogEvents(
            cloudwatch
                .put_log_events(
                    &scope,
                    PutLogEventsInput {
                        log_events: parse_json_log_events(
                            &body,
                            "logEvents",
                            false,
                        )?,
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        log_stream_name: json_required_string(
                            &body,
                            "logStreamName",
                            false,
                        )?,
                    },
                )
                .map_err(logs_aws_error)?,
        ),
        "GetLogEvents" => LogsResponse::GetLogEvents(
            cloudwatch
                .get_log_events(
                    &scope,
                    GetLogEventsInput {
                        end_time_millis: json_optional_epoch_millis(
                            &body, "endTime",
                        )?,
                        limit: json_optional_u32(&body, "limit", false)?,
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        log_stream_name: json_required_string(
                            &body,
                            "logStreamName",
                            false,
                        )?,
                        next_token: json_optional_string(&body, "nextToken"),
                        start_from_head: json_optional_bool(
                            &body,
                            "startFromHead",
                            false,
                        )?
                        .unwrap_or(false),
                        start_time_millis: json_optional_epoch_millis(
                            &body,
                            "startTime",
                        )?,
                    },
                )
                .map_err(logs_aws_error)?,
        ),
        "FilterLogEvents" => LogsResponse::FilterLogEvents(
            cloudwatch
                .filter_log_events(
                    &scope,
                    FilterLogEventsInput {
                        end_time_millis: json_optional_epoch_millis(
                            &body, "endTime",
                        )?,
                        filter_pattern: json_optional_string(
                            &body,
                            "filterPattern",
                        ),
                        limit: json_optional_u32(&body, "limit", false)?,
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        log_stream_name_prefix: json_optional_string(
                            &body,
                            "logStreamNamePrefix",
                        ),
                        log_stream_names: json_string_list(
                            &body,
                            "logStreamNames",
                            false,
                        )?,
                        next_token: json_optional_string(&body, "nextToken"),
                        start_time_millis: json_optional_epoch_millis(
                            &body,
                            "startTime",
                        )?,
                    },
                )
                .map_err(logs_aws_error)?,
        ),
        "PutRetentionPolicy" => {
            cloudwatch
                .put_retention_policy(
                    &scope,
                    PutRetentionPolicyInput {
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                        retention_in_days: json_required_i32(
                            &body,
                            "retentionInDays",
                            false,
                        )?,
                    },
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "DeleteRetentionPolicy" => {
            cloudwatch
                .delete_retention_policy(
                    &scope,
                    DeleteRetentionPolicyInput {
                        log_group_name: json_required_string(
                            &body,
                            "logGroupName",
                            false,
                        )?,
                    },
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "TagLogGroup" => {
            cloudwatch
                .tag_log_group(
                    &scope,
                    &json_required_string(&body, "logGroupName", false)?,
                    json_string_map(&body, "tags", false)?,
                )
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "UntagLogGroup" => {
            let log_group_name =
                json_required_string(&body, "logGroupName", false)?;
            let tag_keys = json_string_list(&body, "tags", false)?;
            cloudwatch
                .untag_log_group(&scope, &log_group_name, &tag_keys)
                .map_err(logs_aws_error)?;
            LogsResponse::Empty
        }
        "ListTagsLogGroup" => LogsResponse::ListTags(
            cloudwatch
                .list_tags_log_group(
                    &scope,
                    &json_required_string(&body, "logGroupName", false)?,
                )
                .map_err(logs_aws_error)?,
        ),
        other => return Err(unknown_operation_error(other)),
    };

    serialize_json_value(&logs_json_response(&response))
}

fn execute_metrics_json(
    cloudwatch: &CloudWatchService,
    operation: &str,
    body: &JsonValue,
    context: &RequestContext,
    query_mode: bool,
) -> Result<MetricsResponse, AwsError> {
    let scope = cloudwatch_scope(context);

    match operation {
        "PutMetricData" => {
            cloudwatch
                .put_metric_data(
                    &scope,
                    parse_metric_namespace(
                        &json_required_string(body, "Namespace", query_mode)?,
                        query_mode,
                    )?,
                    parse_json_metric_data(body, "MetricData", query_mode)?,
                )
                .map_err(|error| error.to_aws_error(query_mode))?;
            Ok(MetricsResponse::Empty)
        }
        "ListMetrics" => Ok(MetricsResponse::ListMetrics(
            cloudwatch
                .list_metrics(
                    &scope,
                    ListMetricsInput {
                        dimensions: parse_json_dimension_filters(
                            body,
                            "Dimensions",
                            query_mode,
                        )?,
                        metric_name: json_optional_string(body, "MetricName"),
                        namespace: json_optional_string(body, "Namespace")
                            .map(|value| {
                                parse_metric_namespace(&value, query_mode)
                            })
                            .transpose()?,
                        next_token: json_optional_string(body, "NextToken"),
                    },
                )
                .map_err(|error| error.to_aws_error(query_mode))?,
        )),
        "GetMetricStatistics" => Ok(MetricsResponse::GetMetricStatistics(
            cloudwatch
                .get_metric_statistics(
                    &scope,
                    GetMetricStatisticsInput {
                        dimensions: parse_json_dimensions(
                            body,
                            "Dimensions",
                            query_mode,
                        )?,
                        end_time_millis: json_required_timestamp_millis(
                            body, "EndTime", query_mode,
                        )?,
                        metric_name: json_required_string(
                            body,
                            "MetricName",
                            query_mode,
                        )?,
                        namespace: parse_metric_namespace(
                            &json_required_string(
                                body,
                                "Namespace",
                                query_mode,
                            )?,
                            query_mode,
                        )?,
                        period_seconds: json_required_i32(
                            body, "Period", query_mode,
                        )?,
                        start_time_millis: json_required_timestamp_millis(
                            body,
                            "StartTime",
                            query_mode,
                        )?,
                        statistics: json_string_list(
                            body,
                            "Statistics",
                            query_mode,
                        )?,
                        unit: json_optional_string(body, "Unit"),
                    },
                )
                .map_err(|error| error.to_aws_error(query_mode))?,
        )),
        "PutMetricAlarm" => {
            cloudwatch
                .put_metric_alarm(
                    &scope,
                    PutMetricAlarmInput {
                        actions_enabled: json_optional_bool(
                            body,
                            "ActionsEnabled",
                            query_mode,
                        )?,
                        alarm_actions: json_string_list(
                            body,
                            "AlarmActions",
                            query_mode,
                        )?,
                        alarm_description: json_optional_string(
                            body,
                            "AlarmDescription",
                        ),
                        alarm_name: json_required_string(
                            body,
                            "AlarmName",
                            query_mode,
                        )?,
                        comparison_operator: json_required_string(
                            body,
                            "ComparisonOperator",
                            query_mode,
                        )?,
                        datapoints_to_alarm: json_optional_i32(
                            body,
                            "DatapointsToAlarm",
                            query_mode,
                        )?,
                        dimensions: parse_json_dimensions(
                            body,
                            "Dimensions",
                            query_mode,
                        )?,
                        evaluation_periods: json_required_i32(
                            body,
                            "EvaluationPeriods",
                            query_mode,
                        )?,
                        insufficient_data_actions: json_string_list(
                            body,
                            "InsufficientDataActions",
                            query_mode,
                        )?,
                        metric_name: json_optional_string(body, "MetricName"),
                        metrics: parse_json_metric_data_queries(
                            body, "Metrics", query_mode,
                        )?,
                        namespace: json_optional_string(body, "Namespace")
                            .map(|value| {
                                parse_metric_namespace(&value, query_mode)
                            })
                            .transpose()?,
                        ok_actions: json_string_list(
                            body,
                            "OKActions",
                            query_mode,
                        )?,
                        period_seconds: json_optional_i32(
                            body, "Period", query_mode,
                        )?,
                        statistic: json_optional_string(body, "Statistic"),
                        threshold: json_optional_f64(
                            body,
                            "Threshold",
                            query_mode,
                        )?,
                        threshold_metric_id: json_optional_string(
                            body,
                            "ThresholdMetricId",
                        ),
                        treat_missing_data: json_optional_string(
                            body,
                            "TreatMissingData",
                        ),
                        unit: json_optional_string(body, "Unit"),
                    },
                )
                .map_err(|error| error.to_aws_error(query_mode))?;
            Ok(MetricsResponse::Empty)
        }
        "DescribeAlarms" => Ok(MetricsResponse::DescribeAlarms(
            cloudwatch
                .describe_alarms(
                    &scope,
                    DescribeAlarmsInput {
                        alarm_name_prefix: json_optional_string(
                            body,
                            "AlarmNamePrefix",
                        ),
                        alarm_names: json_string_list(
                            body,
                            "AlarmNames",
                            query_mode,
                        )?,
                        max_records: json_optional_i32(
                            body,
                            "MaxRecords",
                            query_mode,
                        )?,
                        next_token: json_optional_string(body, "NextToken"),
                        state_value: json_optional_string(body, "StateValue")
                            .map(|value| {
                                AlarmStateValue::parse(&value).map_err(
                                    |error| error.to_aws_error(query_mode),
                                )
                            })
                            .transpose()?,
                    },
                )
                .map_err(|error| error.to_aws_error(query_mode))?,
        )),
        "DeleteAlarms" => {
            let alarm_names =
                json_string_list(body, "AlarmNames", query_mode)?;
            if alarm_names.is_empty() {
                return Err(missing_parameter_error("AlarmNames"));
            }
            cloudwatch
                .delete_alarms(&scope, DeleteAlarmsInput { alarm_names })
                .map_err(|error| error.to_aws_error(query_mode))?;
            Ok(MetricsResponse::Empty)
        }
        "SetAlarmState" => {
            cloudwatch
                .set_alarm_state(
                    &scope,
                    SetAlarmStateInput {
                        alarm_name: json_required_string(
                            body,
                            "AlarmName",
                            query_mode,
                        )?,
                        state_reason: json_required_string(
                            body,
                            "StateReason",
                            query_mode,
                        )?,
                        state_reason_data: json_optional_string(
                            body,
                            "StateReasonData",
                        ),
                        state_value: AlarmStateValue::parse(
                            &json_required_string(
                                body,
                                "StateValue",
                                query_mode,
                            )?,
                        )
                        .map_err(|error| error.to_aws_error(query_mode))?,
                    },
                )
                .map_err(|error| error.to_aws_error(query_mode))?;
            Ok(MetricsResponse::Empty)
        }
        "GetMetricData" => Ok(MetricsResponse::GetMetricData(
            cloudwatch
                .get_metric_data(
                    &scope,
                    GetMetricDataInput {
                        end_time_millis: json_required_timestamp_millis(
                            body, "EndTime", query_mode,
                        )?,
                        max_datapoints: json_optional_i32(
                            body,
                            "MaxDatapoints",
                            query_mode,
                        )?,
                        metric_data_queries: parse_json_metric_data_queries(
                            body,
                            "MetricDataQueries",
                            query_mode,
                        )?,
                        next_token: json_optional_string(body, "NextToken"),
                        scan_by: json_optional_string(body, "ScanBy")
                            .map(|value| {
                                ScanBy::parse(&value).map_err(|error| {
                                    error.to_aws_error(query_mode)
                                })
                            })
                            .transpose()?,
                        start_time_millis: json_required_timestamp_millis(
                            body,
                            "StartTime",
                            query_mode,
                        )?,
                    },
                )
                .map_err(|error| error.to_aws_error(query_mode))?,
        )),
        other => Err(unknown_operation_error(other)),
    }
}

fn metrics_query_response(
    operation: &str,
    response: &MetricsResponse,
) -> String {
    match response {
        MetricsResponse::Empty => query_empty_response(operation),
        MetricsResponse::ListMetrics(output) => query_result_response(
            operation,
            &query_list_metrics_result_xml(output),
        ),
        MetricsResponse::GetMetricStatistics(output) => query_result_response(
            operation,
            &query_get_metric_statistics_result_xml(output),
        ),
        MetricsResponse::DescribeAlarms(output) => query_result_response(
            operation,
            &query_describe_alarms_result_xml(output),
        ),
        MetricsResponse::GetMetricData(output) => query_result_response(
            operation,
            &query_get_metric_data_result_xml(output),
        ),
    }
}

fn metrics_json_response(
    response: &MetricsResponse,
    cbor_timestamps: bool,
) -> JsonValue {
    match response {
        MetricsResponse::Empty => JsonValue::Object(JsonMap::new()),
        MetricsResponse::ListMetrics(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "Metrics".to_owned(),
                JsonValue::Array(
                    output
                        .metrics
                        .iter()
                        .map(json_metric_descriptor)
                        .collect(),
                ),
            );
            if let Some(next_token) = &output.next_token {
                body.insert(
                    "NextToken".to_owned(),
                    JsonValue::String(next_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
        MetricsResponse::GetMetricStatistics(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "Datapoints".to_owned(),
                JsonValue::Array(
                    output
                        .datapoints
                        .iter()
                        .map(|datapoint| {
                            json_metric_datapoint(datapoint, cbor_timestamps)
                        })
                        .collect(),
                ),
            );
            body.insert(
                "Label".to_owned(),
                JsonValue::String(output.label.clone()),
            );
            JsonValue::Object(body)
        }
        MetricsResponse::DescribeAlarms(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "CompositeAlarms".to_owned(),
                JsonValue::Array(Vec::new()),
            );
            body.insert(
                "MetricAlarms".to_owned(),
                JsonValue::Array(
                    output
                        .metric_alarms
                        .iter()
                        .map(|alarm| json_metric_alarm(alarm, cbor_timestamps))
                        .collect(),
                ),
            );
            if let Some(next_token) = &output.next_token {
                body.insert(
                    "NextToken".to_owned(),
                    JsonValue::String(next_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
        MetricsResponse::GetMetricData(output) => {
            let mut body = JsonMap::new();
            body.insert("Messages".to_owned(), JsonValue::Array(Vec::new()));
            body.insert(
                "MetricDataResults".to_owned(),
                JsonValue::Array(
                    output
                        .metric_data_results
                        .iter()
                        .map(|result| {
                            json_metric_data_result(result, cbor_timestamps)
                        })
                        .collect(),
                ),
            );
            if let Some(next_token) = &output.next_token {
                body.insert(
                    "NextToken".to_owned(),
                    JsonValue::String(next_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
    }
}

fn metrics_cbor_response(response: &MetricsResponse) -> CborValue {
    json_to_cbor_value(&metrics_json_response(response, true))
}

fn logs_json_response(response: &LogsResponse) -> JsonValue {
    match response {
        LogsResponse::Empty => JsonValue::Object(JsonMap::new()),
        LogsResponse::DescribeLogGroups(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "logGroups".to_owned(),
                JsonValue::Array(
                    output
                        .log_groups
                        .iter()
                        .map(json_log_group_description)
                        .collect(),
                ),
            );
            if let Some(next_token) = &output.next_token {
                body.insert(
                    "nextToken".to_owned(),
                    JsonValue::String(next_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
        LogsResponse::DescribeLogStreams(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "logStreams".to_owned(),
                JsonValue::Array(
                    output
                        .log_streams
                        .iter()
                        .map(json_log_stream_description)
                        .collect(),
                ),
            );
            if let Some(next_token) = &output.next_token {
                body.insert(
                    "nextToken".to_owned(),
                    JsonValue::String(next_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
        LogsResponse::PutLogEvents(output) => {
            let mut body = JsonMap::new();
            if let Some(next_sequence_token) = &output.next_sequence_token {
                body.insert(
                    "nextSequenceToken".to_owned(),
                    JsonValue::String(next_sequence_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
        LogsResponse::GetLogEvents(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "events".to_owned(),
                JsonValue::Array(
                    output.events.iter().map(json_output_log_event).collect(),
                ),
            );
            body.insert(
                "nextBackwardToken".to_owned(),
                JsonValue::String(output.next_backward_token.clone()),
            );
            body.insert(
                "nextForwardToken".to_owned(),
                JsonValue::String(output.next_forward_token.clone()),
            );
            JsonValue::Object(body)
        }
        LogsResponse::FilterLogEvents(output) => {
            let mut body = JsonMap::new();
            body.insert(
                "events".to_owned(),
                JsonValue::Array(
                    output
                        .events
                        .iter()
                        .map(json_filtered_log_event)
                        .collect(),
                ),
            );
            body.insert(
                "searchedLogStreams".to_owned(),
                JsonValue::Array(
                    output
                        .searched_log_streams
                        .iter()
                        .map(json_searched_log_stream)
                        .collect(),
                ),
            );
            if let Some(next_token) = &output.next_token {
                body.insert(
                    "nextToken".to_owned(),
                    JsonValue::String(next_token.clone()),
                );
            }
            JsonValue::Object(body)
        }
        LogsResponse::ListTags(tags) => {
            let mut body = JsonMap::new();
            body.insert(
                "tags".to_owned(),
                JsonValue::Object(
                    tags.iter()
                        .map(|(key, value)| {
                            (key.clone(), JsonValue::String(value.clone()))
                        })
                        .collect(),
                ),
            );
            JsonValue::Object(body)
        }
    }
}

fn query_result_response(operation: &str, body: &str) -> String {
    let response = format!("{operation}Response");
    let result = format!("{operation}Result");

    XmlBuilder::new()
        .start(&response, Some(CLOUDWATCH_QUERY_XMLNS))
        .start(&result, None)
        .raw(body)
        .end(&result)
        .raw(&query_response_metadata_xml())
        .end(&response)
        .build()
}

fn query_empty_response(operation: &str) -> String {
    let response = format!("{operation}Response");

    XmlBuilder::new()
        .start(&response, Some(CLOUDWATCH_QUERY_XMLNS))
        .raw(&query_response_metadata_xml())
        .end(&response)
        .build()
}

fn query_response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn query_list_metrics_result_xml(output: &ListMetricsOutput) -> String {
    let mut xml = XmlBuilder::new()
        .raw(&query_metric_descriptor_list_xml("Metrics", &output.metrics));
    if let Some(next_token) = &output.next_token {
        xml = xml.elem("NextToken", next_token);
    }
    xml.build()
}

fn query_get_metric_statistics_result_xml(
    output: &GetMetricStatisticsOutput,
) -> String {
    XmlBuilder::new()
        .raw(&query_metric_datapoint_list_xml(
            "Datapoints",
            &output.datapoints,
        ))
        .elem("Label", &output.label)
        .build()
}

fn query_describe_alarms_result_xml(output: &DescribeAlarmsOutput) -> String {
    let mut xml = XmlBuilder::new()
        .raw(&query_empty_member_list_xml("CompositeAlarms"))
        .raw(&query_metric_alarm_list_xml(
            "MetricAlarms",
            &output.metric_alarms,
        ));
    if let Some(next_token) = &output.next_token {
        xml = xml.elem("NextToken", next_token);
    }
    xml.build()
}

fn query_get_metric_data_result_xml(output: &GetMetricDataOutput) -> String {
    let mut xml = XmlBuilder::new()
        .raw(&query_empty_member_list_xml("Messages"))
        .raw(&query_metric_data_result_list_xml(
            "MetricDataResults",
            &output.metric_data_results,
        ));
    if let Some(next_token) = &output.next_token {
        xml = xml.elem("NextToken", next_token);
    }
    xml.build()
}

fn query_metric_descriptor_list_xml(
    name: &str,
    metrics: &[services::MetricDescriptor],
) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for metric in metrics {
        xml = xml.start("member", None);
        xml = xml.elem("Namespace", metric.namespace.as_str());
        xml = xml.elem("MetricName", &metric.metric_name);
        xml = xml
            .raw(&query_dimension_list_xml("Dimensions", &metric.dimensions));
        xml = xml.end("member");
    }
    xml.end(name).build()
}

fn query_metric_datapoint_list_xml(
    name: &str,
    datapoints: &[services::MetricDatapoint],
) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for datapoint in datapoints {
        xml = xml.start("member", None);
        if let Some(average) = datapoint.average {
            xml = xml.elem("Average", &format_f64(average));
        }
        if let Some(maximum) = datapoint.maximum {
            xml = xml.elem("Maximum", &format_f64(maximum));
        }
        if let Some(minimum) = datapoint.minimum {
            xml = xml.elem("Minimum", &format_f64(minimum));
        }
        if let Some(sample_count) = datapoint.sample_count {
            xml = xml.elem("SampleCount", &format_f64(sample_count));
        }
        if let Some(sum) = datapoint.sum {
            xml = xml.elem("Sum", &format_f64(sum));
        }
        xml = xml.elem(
            "Timestamp",
            &format_query_timestamp(datapoint.timestamp_millis),
        );
        if let Some(unit) = &datapoint.unit {
            xml = xml.elem("Unit", unit);
        }
        xml = xml.end("member");
    }
    xml.end(name).build()
}

fn query_metric_alarm_list_xml(name: &str, alarms: &[MetricAlarm]) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for alarm in alarms {
        xml = xml.start("member", None);
        xml = xml.elem(
            "ActionsEnabled",
            if alarm.actions_enabled { "true" } else { "false" },
        );
        xml = xml
            .raw(&query_string_list_xml("AlarmActions", &alarm.alarm_actions));
        xml = xml.elem("AlarmArn", &alarm.alarm_arn);
        xml = xml.elem(
            "AlarmConfigurationUpdatedTimestamp",
            &format_query_timestamp(
                alarm.alarm_configuration_updated_timestamp_millis,
            ),
        );
        if let Some(description) = &alarm.alarm_description {
            xml = xml.elem("AlarmDescription", description);
        }
        xml = xml.elem("AlarmName", &alarm.alarm_name);
        xml = xml.elem("ComparisonOperator", &alarm.comparison_operator);
        xml = xml
            .raw(&query_dimension_list_xml("Dimensions", &alarm.dimensions));
        xml = xml
            .elem("EvaluationPeriods", &alarm.evaluation_periods.to_string());
        xml = xml.raw(&query_string_list_xml(
            "InsufficientDataActions",
            &alarm.insufficient_data_actions,
        ));
        xml = xml.elem("MetricName", &alarm.metric_name);
        xml = xml.elem("Namespace", alarm.namespace.as_str());
        xml = xml.raw(&query_string_list_xml("OKActions", &alarm.ok_actions));
        xml = xml.elem("Period", &alarm.period_seconds.to_string());
        xml = xml.elem("StateReason", &alarm.state_reason);
        if let Some(state_reason_data) = &alarm.state_reason_data {
            xml = xml.elem("StateReasonData", state_reason_data);
        }
        xml = xml.elem(
            "StateUpdatedTimestamp",
            &format_query_timestamp(alarm.state_updated_timestamp_millis),
        );
        xml = xml.elem("StateValue", alarm.state_value.as_str());
        xml = xml.elem("Statistic", &alarm.statistic);
        xml = xml.elem("Threshold", &format_f64(alarm.threshold));
        if let Some(treat_missing_data) = &alarm.treat_missing_data {
            xml = xml.elem("TreatMissingData", treat_missing_data);
        }
        if let Some(unit) = &alarm.unit {
            xml = xml.elem("Unit", unit);
        }
        xml = xml.end("member");
    }
    xml.end(name).build()
}

fn query_metric_data_result_list_xml(
    name: &str,
    results: &[services::MetricDataResult],
) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for result in results {
        xml = xml.start("member", None);
        xml = xml.elem("Id", &result.id);
        xml = xml.elem("Label", &result.label);
        xml = xml.elem("StatusCode", &result.status_code);
        xml = xml.raw(&query_timestamp_list_xml(
            "Timestamps",
            &result.timestamps_millis,
        ));
        xml = xml.raw(&query_f64_list_xml("Values", &result.values));
        xml = xml.end("member");
    }
    xml.end(name).build()
}

fn query_dimension_list_xml(name: &str, dimensions: &[Dimension]) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for dimension in dimensions {
        xml = xml
            .start("member", None)
            .elem("Name", &dimension.name)
            .elem("Value", &dimension.value)
            .end("member");
    }
    xml.end(name).build()
}

fn query_string_list_xml(name: &str, values: &[String]) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for value in values {
        xml = xml.elem("member", value);
    }
    xml.end(name).build()
}

fn query_empty_member_list_xml(name: &str) -> String {
    XmlBuilder::new().start(name, None).end(name).build()
}

fn query_timestamp_list_xml(name: &str, values: &[u64]) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for value in values {
        xml = xml
            .start("member", None)
            .raw(&format_query_timestamp(*value))
            .end("member");
    }
    xml.end(name).build()
}

fn query_f64_list_xml(name: &str, values: &[f64]) -> String {
    let mut xml = XmlBuilder::new().start(name, None);
    for value in values {
        xml = xml.start("member", None).raw(&format_f64(*value)).end("member");
    }
    xml.end(name).build()
}

fn json_metric_descriptor(metric: &services::MetricDescriptor) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(
        "Dimensions".to_owned(),
        JsonValue::Array(
            metric.dimensions.iter().map(json_dimension).collect(),
        ),
    );
    object.insert(
        "MetricName".to_owned(),
        JsonValue::String(metric.metric_name.clone()),
    );
    object.insert(
        "Namespace".to_owned(),
        JsonValue::String(metric.namespace.to_string()),
    );
    JsonValue::Object(object)
}

fn json_metric_datapoint(
    datapoint: &services::MetricDatapoint,
    _cbor_timestamps: bool,
) -> JsonValue {
    let mut object = JsonMap::new();
    if let Some(average) = datapoint.average {
        object.insert("Average".to_owned(), json_number_value(average));
    }
    if let Some(maximum) = datapoint.maximum {
        object.insert("Maximum".to_owned(), json_number_value(maximum));
    }
    if let Some(minimum) = datapoint.minimum {
        object.insert("Minimum".to_owned(), json_number_value(minimum));
    }
    if let Some(sample_count) = datapoint.sample_count {
        object
            .insert("SampleCount".to_owned(), json_number_value(sample_count));
    }
    if let Some(sum) = datapoint.sum {
        object.insert("Sum".to_owned(), json_number_value(sum));
    }
    object.insert(
        "Timestamp".to_owned(),
        json_number_value(timestamp_seconds(datapoint.timestamp_millis)),
    );
    if let Some(unit) = &datapoint.unit {
        object.insert("Unit".to_owned(), JsonValue::String(unit.clone()));
    }
    JsonValue::Object(object)
}

fn json_metric_alarm(
    alarm: &MetricAlarm,
    _cbor_timestamps: bool,
) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(
        "ActionsEnabled".to_owned(),
        JsonValue::Bool(alarm.actions_enabled),
    );
    object.insert(
        "AlarmActions".to_owned(),
        JsonValue::Array(
            alarm
                .alarm_actions
                .iter()
                .map(|value| JsonValue::String(value.clone()))
                .collect(),
        ),
    );
    object.insert(
        "AlarmArn".to_owned(),
        JsonValue::String(alarm.alarm_arn.clone()),
    );
    object.insert(
        "AlarmConfigurationUpdatedTimestamp".to_owned(),
        json_number_value(timestamp_seconds(
            alarm.alarm_configuration_updated_timestamp_millis,
        )),
    );
    if let Some(description) = &alarm.alarm_description {
        object.insert(
            "AlarmDescription".to_owned(),
            JsonValue::String(description.clone()),
        );
    }
    object.insert(
        "AlarmName".to_owned(),
        JsonValue::String(alarm.alarm_name.clone()),
    );
    object.insert(
        "ComparisonOperator".to_owned(),
        JsonValue::String(alarm.comparison_operator.clone()),
    );
    object.insert(
        "Dimensions".to_owned(),
        JsonValue::Array(
            alarm.dimensions.iter().map(json_dimension).collect(),
        ),
    );
    object.insert(
        "EvaluationPeriods".to_owned(),
        JsonValue::Number(alarm.evaluation_periods.into()),
    );
    object.insert(
        "InsufficientDataActions".to_owned(),
        JsonValue::Array(
            alarm
                .insufficient_data_actions
                .iter()
                .map(|value| JsonValue::String(value.clone()))
                .collect(),
        ),
    );
    object.insert(
        "MetricName".to_owned(),
        JsonValue::String(alarm.metric_name.clone()),
    );
    object.insert(
        "Namespace".to_owned(),
        JsonValue::String(alarm.namespace.to_string()),
    );
    object.insert(
        "OKActions".to_owned(),
        JsonValue::Array(
            alarm
                .ok_actions
                .iter()
                .map(|value| JsonValue::String(value.clone()))
                .collect(),
        ),
    );
    object.insert(
        "Period".to_owned(),
        JsonValue::Number(alarm.period_seconds.into()),
    );
    object.insert(
        "StateReason".to_owned(),
        JsonValue::String(alarm.state_reason.clone()),
    );
    if let Some(state_reason_data) = &alarm.state_reason_data {
        object.insert(
            "StateReasonData".to_owned(),
            JsonValue::String(state_reason_data.clone()),
        );
    }
    object.insert(
        "StateUpdatedTimestamp".to_owned(),
        json_number_value(timestamp_seconds(
            alarm.state_updated_timestamp_millis,
        )),
    );
    object.insert(
        "StateValue".to_owned(),
        JsonValue::String(alarm.state_value.as_str().to_owned()),
    );
    object.insert(
        "Statistic".to_owned(),
        JsonValue::String(alarm.statistic.clone()),
    );
    object.insert("Threshold".to_owned(), json_number_value(alarm.threshold));
    if let Some(treat_missing_data) = &alarm.treat_missing_data {
        object.insert(
            "TreatMissingData".to_owned(),
            JsonValue::String(treat_missing_data.clone()),
        );
    }
    if let Some(unit) = &alarm.unit {
        object.insert("Unit".to_owned(), JsonValue::String(unit.clone()));
    }
    JsonValue::Object(object)
}

fn json_metric_data_result(
    result: &services::MetricDataResult,
    _cbor_timestamps: bool,
) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert("Id".to_owned(), JsonValue::String(result.id.clone()));
    object.insert("Label".to_owned(), JsonValue::String(result.label.clone()));
    object.insert(
        "StatusCode".to_owned(),
        JsonValue::String(result.status_code.clone()),
    );
    object.insert(
        "Timestamps".to_owned(),
        JsonValue::Array(
            result
                .timestamps_millis
                .iter()
                .map(|timestamp| {
                    json_number_value(timestamp_seconds(*timestamp))
                })
                .collect(),
        ),
    );
    object.insert(
        "Values".to_owned(),
        JsonValue::Array(
            result
                .values
                .iter()
                .map(|value| json_number_value(*value))
                .collect(),
        ),
    );
    JsonValue::Object(object)
}

fn json_dimension(dimension: &Dimension) -> JsonValue {
    let mut object = JsonMap::new();
    object
        .insert("Name".to_owned(), JsonValue::String(dimension.name.clone()));
    object.insert(
        "Value".to_owned(),
        JsonValue::String(dimension.value.clone()),
    );
    JsonValue::Object(object)
}

fn json_log_group_description(
    group: &services::LogGroupDescription,
) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert("arn".to_owned(), JsonValue::String(group.arn.clone()));
    object.insert(
        "creationTime".to_owned(),
        JsonValue::Number(group.creation_time_millis.into()),
    );
    object.insert(
        "logGroupArn".to_owned(),
        JsonValue::String(group.log_group_arn.clone()),
    );
    object.insert(
        "logGroupName".to_owned(),
        JsonValue::String(group.log_group_name.clone()),
    );
    object.insert(
        "metricFilterCount".to_owned(),
        JsonValue::Number(group.metric_filter_count.into()),
    );
    if let Some(retention_in_days) = group.retention_in_days {
        object.insert(
            "retentionInDays".to_owned(),
            JsonValue::Number(retention_in_days.into()),
        );
    }
    object.insert(
        "storedBytes".to_owned(),
        JsonValue::Number(group.stored_bytes.into()),
    );
    JsonValue::Object(object)
}

fn json_log_stream_description(
    stream: &services::LogStreamDescription,
) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert("arn".to_owned(), JsonValue::String(stream.arn.clone()));
    object.insert(
        "creationTime".to_owned(),
        JsonValue::Number(stream.creation_time_millis.into()),
    );
    if let Some(first_event_timestamp_millis) =
        stream.first_event_timestamp_millis
    {
        object.insert(
            "firstEventTimestamp".to_owned(),
            JsonValue::Number(first_event_timestamp_millis.into()),
        );
    }
    if let Some(last_event_timestamp_millis) =
        stream.last_event_timestamp_millis
    {
        object.insert(
            "lastEventTimestamp".to_owned(),
            JsonValue::Number(last_event_timestamp_millis.into()),
        );
    }
    if let Some(last_ingestion_time_millis) = stream.last_ingestion_time_millis
    {
        object.insert(
            "lastIngestionTime".to_owned(),
            JsonValue::Number(last_ingestion_time_millis.into()),
        );
    }
    object.insert(
        "logStreamName".to_owned(),
        JsonValue::String(stream.log_stream_name.clone()),
    );
    object.insert(
        "storedBytes".to_owned(),
        JsonValue::Number(stream.stored_bytes.into()),
    );
    if let Some(upload_sequence_token) = &stream.upload_sequence_token {
        object.insert(
            "uploadSequenceToken".to_owned(),
            JsonValue::String(upload_sequence_token.clone()),
        );
    }
    JsonValue::Object(object)
}

fn json_output_log_event(event: &services::OutputLogEvent) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(
        "ingestionTime".to_owned(),
        JsonValue::Number(event.ingestion_time_millis.into()),
    );
    object.insert(
        "message".to_owned(),
        JsonValue::String(event.message.clone()),
    );
    object.insert(
        "timestamp".to_owned(),
        JsonValue::Number(event.timestamp_millis.into()),
    );
    JsonValue::Object(object)
}

fn json_filtered_log_event(event: &services::FilteredLogEvent) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(
        "eventId".to_owned(),
        JsonValue::String(event.event_id.clone()),
    );
    object.insert(
        "ingestionTime".to_owned(),
        JsonValue::Number(event.ingestion_time_millis.into()),
    );
    object.insert(
        "logStreamName".to_owned(),
        JsonValue::String(event.log_stream_name.clone()),
    );
    object.insert(
        "message".to_owned(),
        JsonValue::String(event.message.clone()),
    );
    object.insert(
        "timestamp".to_owned(),
        JsonValue::Number(event.timestamp_millis.into()),
    );
    JsonValue::Object(object)
}

fn json_searched_log_stream(
    stream: &services::SearchedLogStream,
) -> JsonValue {
    let mut object = JsonMap::new();
    object.insert(
        "logStreamName".to_owned(),
        JsonValue::String(stream.log_stream_name.clone()),
    );
    object.insert(
        "searchedCompletely".to_owned(),
        JsonValue::Bool(stream.searched_completely),
    );
    JsonValue::Object(object)
}

fn parse_query_metric_data(
    params: &QueryParameters,
    prefix: &str,
) -> Result<Vec<MetricDatum>, AwsError> {
    query_member_indices(params, prefix)
        .into_iter()
        .map(|index| {
            let prefix = format!("{prefix}.{index}");
            Ok(MetricDatum {
                counts: query_f64_list(
                    params,
                    &format!("{prefix}.Counts.member"),
                )?,
                dimensions: parse_query_dimensions(
                    params,
                    &format!("{prefix}.Dimensions.member"),
                )?,
                metric_name: query_optional_string(
                    params,
                    &format!("{prefix}.MetricName"),
                )
                .unwrap_or_default(),
                statistic_values: parse_query_statistic_set(
                    params,
                    &format!("{prefix}.StatisticValues"),
                )?,
                storage_resolution: query_optional_i32(
                    params,
                    &format!("{prefix}.StorageResolution"),
                    true,
                )?,
                timestamp_millis: query_optional_timestamp_millis(
                    params,
                    &format!("{prefix}.Timestamp"),
                    true,
                )?,
                unit: query_optional_string(params, &format!("{prefix}.Unit")),
                value: query_optional_f64(
                    params,
                    &format!("{prefix}.Value"),
                    true,
                )?,
                values: query_f64_list(
                    params,
                    &format!("{prefix}.Values.member"),
                )?,
            })
        })
        .collect()
}

fn parse_query_dimensions(
    params: &QueryParameters,
    prefix: &str,
) -> Result<Vec<Dimension>, AwsError> {
    query_member_indices(params, prefix)
        .into_iter()
        .map(|index| {
            let prefix = format!("{prefix}.{index}");
            Ok(Dimension {
                name: query_optional_string(params, &format!("{prefix}.Name"))
                    .unwrap_or_default(),
                value: query_optional_string(
                    params,
                    &format!("{prefix}.Value"),
                )
                .unwrap_or_default(),
            })
        })
        .collect()
}

fn parse_query_dimension_filters(
    params: &QueryParameters,
    prefix: &str,
) -> Result<Vec<DimensionFilter>, AwsError> {
    query_member_indices(params, prefix)
        .into_iter()
        .map(|index| {
            let prefix = format!("{prefix}.{index}");
            Ok(DimensionFilter {
                name: query_optional_string(params, &format!("{prefix}.Name"))
                    .unwrap_or_default(),
                value: query_optional_string(
                    params,
                    &format!("{prefix}.Value"),
                ),
            })
        })
        .collect()
}

fn parse_query_statistic_set(
    params: &QueryParameters,
    prefix: &str,
) -> Result<Option<StatisticSet>, AwsError> {
    if !query_has_prefix(params, prefix) {
        return Ok(None);
    }

    Ok(Some(StatisticSet {
        maximum: query_required_f64(
            params,
            &format!("{prefix}.Maximum"),
            true,
        )?,
        minimum: query_required_f64(
            params,
            &format!("{prefix}.Minimum"),
            true,
        )?,
        sample_count: query_required_f64(
            params,
            &format!("{prefix}.SampleCount"),
            true,
        )?,
        sum: query_required_f64(params, &format!("{prefix}.Sum"), true)?,
    }))
}

fn parse_query_metric_data_queries(
    params: &QueryParameters,
    prefix: &str,
    query_mode: bool,
) -> Result<Vec<MetricDataQuery>, AwsError> {
    query_member_indices(params, prefix)
        .into_iter()
        .map(|index| {
            let prefix = format!("{prefix}.{index}");
            Ok(MetricDataQuery {
                expression: query_optional_string(
                    params,
                    &format!("{prefix}.Expression"),
                ),
                id: query_optional_string(params, &format!("{prefix}.Id"))
                    .unwrap_or_default(),
                label: query_optional_string(
                    params,
                    &format!("{prefix}.Label"),
                ),
                metric_stat: parse_query_metric_stat(
                    params,
                    &format!("{prefix}.MetricStat"),
                    query_mode,
                )?,
                period_seconds: query_optional_i32(
                    params,
                    &format!("{prefix}.Period"),
                    query_mode,
                )?,
                return_data: query_optional_bool(
                    params,
                    &format!("{prefix}.ReturnData"),
                    query_mode,
                )?,
            })
        })
        .collect()
}

fn parse_query_metric_stat(
    params: &QueryParameters,
    prefix: &str,
    query_mode: bool,
) -> Result<Option<MetricStat>, AwsError> {
    if !query_has_prefix(params, prefix) {
        return Ok(None);
    }

    Ok(Some(MetricStat {
        metric: parse_query_metric(
            params,
            &format!("{prefix}.Metric"),
            query_mode,
        )?
        .ok_or_else(|| {
            CloudWatchMetricsError::MissingParameter {
                message: "MetricStat.Metric is required.".to_owned(),
            }
            .to_aws_error(query_mode)
        })?,
        period_seconds: query_optional_i32(
            params,
            &format!("{prefix}.Period"),
            query_mode,
        )?
        .unwrap_or_default(),
        stat: query_optional_string(params, &format!("{prefix}.Stat"))
            .unwrap_or_default(),
        unit: query_optional_string(params, &format!("{prefix}.Unit")),
    }))
}

fn parse_query_metric(
    params: &QueryParameters,
    prefix: &str,
    query_mode: bool,
) -> Result<Option<Metric>, AwsError> {
    if !query_has_prefix(params, prefix) {
        return Ok(None);
    }

    Ok(Some(Metric {
        dimensions: parse_query_dimensions(
            params,
            &format!("{prefix}.Dimensions.member"),
        )?,
        metric_name: query_optional_string(
            params,
            &format!("{prefix}.MetricName"),
        )
        .unwrap_or_default(),
        namespace: parse_metric_namespace(
            &query_optional_string(params, &format!("{prefix}.Namespace"))
                .unwrap_or_default(),
            query_mode,
        )?,
    }))
}

fn parse_json_metric_data(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<MetricDatum>, AwsError> {
    json_array(body, field, query_mode)?
        .iter()
        .map(|value| {
            Ok(MetricDatum {
                counts: json_f64_list(value, "Counts", query_mode)?,
                dimensions: parse_json_dimensions(
                    value,
                    "Dimensions",
                    query_mode,
                )?,
                metric_name: json_required_string(
                    value,
                    "MetricName",
                    query_mode,
                )?,
                statistic_values: parse_json_statistic_set(
                    value,
                    "StatisticValues",
                    query_mode,
                )?,
                storage_resolution: json_optional_i32(
                    value,
                    "StorageResolution",
                    query_mode,
                )?,
                timestamp_millis: json_optional_timestamp_millis(
                    value,
                    "Timestamp",
                    query_mode,
                )?,
                unit: json_optional_string(value, "Unit"),
                value: json_optional_f64(value, "Value", query_mode)?,
                values: json_f64_list(value, "Values", query_mode)?,
            })
        })
        .collect()
}

fn parse_json_dimensions(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<Dimension>, AwsError> {
    json_array(body, field, query_mode)?
        .iter()
        .map(|value| {
            Ok(Dimension {
                name: json_required_string(value, "Name", query_mode)?,
                value: json_required_string(value, "Value", query_mode)?,
            })
        })
        .collect()
}

fn parse_json_dimension_filters(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<DimensionFilter>, AwsError> {
    json_array(body, field, query_mode)?
        .iter()
        .map(|value| {
            Ok(DimensionFilter {
                name: json_required_string(value, "Name", query_mode)?,
                value: json_optional_string(value, "Value"),
            })
        })
        .collect()
}

fn parse_json_statistic_set(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<StatisticSet>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    Ok(Some(StatisticSet {
        maximum: json_required_f64(value, "Maximum", query_mode)?,
        minimum: json_required_f64(value, "Minimum", query_mode)?,
        sample_count: json_required_f64(value, "SampleCount", query_mode)?,
        sum: json_required_f64(value, "Sum", query_mode)?,
    }))
}

fn parse_json_metric_data_queries(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<MetricDataQuery>, AwsError> {
    json_array(body, field, query_mode)?
        .iter()
        .map(|value| {
            Ok(MetricDataQuery {
                expression: json_optional_string(value, "Expression"),
                id: json_required_string(value, "Id", query_mode)?,
                label: json_optional_string(value, "Label"),
                metric_stat: parse_json_metric_stat(
                    value,
                    "MetricStat",
                    query_mode,
                )?,
                period_seconds: json_optional_i32(
                    value, "Period", query_mode,
                )?,
                return_data: json_optional_bool(
                    value,
                    "ReturnData",
                    query_mode,
                )?,
            })
        })
        .collect()
}

fn parse_json_metric_stat(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<MetricStat>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    Ok(Some(MetricStat {
        metric: parse_json_metric(value, "Metric", query_mode)?.ok_or_else(
            || {
                CloudWatchMetricsError::MissingParameter {
                    message: "MetricStat.Metric is required.".to_owned(),
                }
                .to_aws_error(query_mode)
            },
        )?,
        period_seconds: json_required_i32(value, "Period", query_mode)?,
        stat: json_required_string(value, "Stat", query_mode)?,
        unit: json_optional_string(value, "Unit"),
    }))
}

fn parse_json_metric(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<Metric>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    Ok(Some(Metric {
        dimensions: parse_json_dimensions(value, "Dimensions", query_mode)?,
        metric_name: json_required_string(value, "MetricName", query_mode)?,
        namespace: parse_metric_namespace(
            &json_required_string(value, "Namespace", query_mode)?,
            query_mode,
        )?,
    }))
}

fn parse_json_log_events(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<InputLogEvent>, AwsError> {
    json_array(body, field, query_mode)?
        .iter()
        .map(|value| {
            Ok(InputLogEvent {
                message: json_required_string(value, "message", query_mode)?,
                timestamp_millis: json_required_epoch_millis(
                    value,
                    "timestamp",
                )?,
            })
        })
        .collect()
}

fn query_member_indices(params: &QueryParameters, prefix: &str) -> Vec<usize> {
    let prefix = format!("{prefix}.");
    let mut indices = BTreeSet::new();

    for (name, _) in params.iter() {
        if let Some(rest) = name.strip_prefix(&prefix) {
            let Some(index) = rest.split('.').next() else {
                continue;
            };
            if let Ok(index) = index.parse::<usize>() {
                indices.insert(index);
            }
        }
    }

    indices.into_iter().collect()
}

fn query_has_prefix(params: &QueryParameters, prefix: &str) -> bool {
    let prefix = format!("{prefix}.");
    params.iter().any(|(name, _)| name.starts_with(&prefix))
}

fn query_optional_string(
    params: &QueryParameters,
    name: &str,
) -> Option<String> {
    params.optional(name).map(str::to_owned)
}

fn parse_metric_namespace(
    value: &str,
    query_mode: bool,
) -> Result<MetricNamespace, AwsError> {
    value.parse::<MetricNamespace>().map_err(|error| {
        CloudWatchMetricsError::InvalidParameterValue {
            message: error.to_string(),
        }
        .to_aws_error(query_mode)
    })
}

fn query_string_list(params: &QueryParameters, prefix: &str) -> Vec<String> {
    query_member_indices(params, prefix)
        .into_iter()
        .filter_map(|index| {
            query_optional_string(params, &format!("{prefix}.{index}"))
        })
        .collect()
}

fn query_f64_list(
    params: &QueryParameters,
    prefix: &str,
) -> Result<Vec<f64>, AwsError> {
    query_member_indices(params, prefix)
        .into_iter()
        .filter_map(|index| {
            params
                .optional(&format!("{prefix}.{index}"))
                .map(|value| query_parse_f64(value, prefix, true))
        })
        .collect()
}

fn query_optional_bool(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<Option<bool>, AwsError> {
    params
        .optional(name)
        .map(|value| query_parse_bool(value, name, query_mode))
        .transpose()
}

fn query_optional_i32(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<Option<i32>, AwsError> {
    params
        .optional(name)
        .map(|value| query_parse_i32(value, name, query_mode))
        .transpose()
}

fn query_required_i32(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<i32, AwsError> {
    query_parse_i32(params.required(name)?, name, query_mode)
}

fn query_optional_f64(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<Option<f64>, AwsError> {
    params
        .optional(name)
        .map(|value| query_parse_f64(value, name, query_mode))
        .transpose()
}

fn query_required_f64(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<f64, AwsError> {
    query_parse_f64(params.required(name)?, name, query_mode)
}

fn query_optional_timestamp_millis(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<Option<u64>, AwsError> {
    params
        .optional(name)
        .map(|value| parse_query_timestamp_millis(value, name, query_mode))
        .transpose()
}

fn query_required_timestamp_millis(
    params: &QueryParameters,
    name: &str,
    query_mode: bool,
) -> Result<u64, AwsError> {
    parse_query_timestamp_millis(params.required(name)?, name, query_mode)
}

fn query_parse_bool(
    value: &str,
    field: &str,
    query_mode: bool,
) -> Result<bool, AwsError> {
    value.parse::<bool>().map_err(|_| {
        metrics_validation_error(
            format!("Expected boolean value for {field}."),
            query_mode,
        )
    })
}

fn query_parse_i32(
    value: &str,
    field: &str,
    query_mode: bool,
) -> Result<i32, AwsError> {
    value.parse::<i32>().map_err(|_| {
        metrics_validation_error(
            format!("Expected integer value for {field}."),
            query_mode,
        )
    })
}

fn query_parse_f64(
    value: &str,
    field: &str,
    query_mode: bool,
) -> Result<f64, AwsError> {
    value.parse::<f64>().map_err(|_| {
        metrics_validation_error(
            format!("Expected numeric value for {field}."),
            query_mode,
        )
    })
}

fn parse_query_timestamp_millis(
    value: &str,
    field: &str,
    query_mode: bool,
) -> Result<u64, AwsError> {
    let parsed = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| {
        metrics_validation_error(
            format!("Expected RFC3339 timestamp for {field}."),
            query_mode,
        )
    })?;
    let nanos = parsed.unix_timestamp_nanos();
    if nanos < 0 {
        return Err(metrics_validation_error(
            format!("Timestamp {field} must not be negative."),
            query_mode,
        ));
    }

    Ok((nanos / 1_000_000) as u64)
}

fn parse_json_body(
    body: &[u8],
    query_mode: bool,
) -> Result<JsonValue, AwsError> {
    if body.is_empty() {
        return Ok(JsonValue::Object(JsonMap::new()));
    }

    serde_json::from_slice(body).map_err(|error| {
        metrics_validation_error(
            format!("The request body is not valid JSON: {error}"),
            query_mode,
        )
    })
}

fn parse_logs_json_body(body: &[u8]) -> Result<JsonValue, AwsError> {
    if body.is_empty() {
        return Ok(JsonValue::Object(JsonMap::new()));
    }

    serde_json::from_slice(body).map_err(|error| {
        logs_validation_error(format!(
            "The request body is not valid JSON: {error}"
        ))
    })
}

fn parse_cbor_body(
    body: &[u8],
    query_mode: bool,
) -> Result<JsonValue, AwsError> {
    let value = from_reader::<CborValue, _>(body).map_err(|error| {
        metrics_validation_error(
            format!("The request body is not valid CBOR: {error}"),
            query_mode,
        )
    })?;
    cbor_to_json_value(value)
        .map_err(|message| metrics_validation_error(message, query_mode))
}

fn json_field<'a>(body: &'a JsonValue, field: &str) -> Option<&'a JsonValue> {
    body.as_object()?.get(field)
}

fn json_optional_string(body: &JsonValue, field: &str) -> Option<String> {
    let value = json_field(body, field)?;
    if value.is_null() {
        return None;
    }
    value.as_str().map(str::to_owned)
}

fn json_required_string(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<String, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Err(missing_parameter_error(field));
    };
    let Some(value) = value.as_str() else {
        return Err(json_field_type_error(
            field,
            format!("Expected string value for {field}."),
            query_mode,
        ));
    };
    Ok(value.to_owned())
}

fn json_optional_bool(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<bool>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .ok_or_else(|| {
            json_field_type_error(
                field,
                format!("Expected boolean value for {field}."),
                query_mode,
            )
        })
        .map(Some)
}

fn json_optional_i32(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<i32>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    json_value_to_i32(value).map(Some).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected integer value for {field}."),
            query_mode,
        )
    })
}

fn json_required_i32(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<i32, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Err(missing_parameter_error(field));
    };
    json_value_to_i32(value).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected integer value for {field}."),
            query_mode,
        )
    })
}

fn json_optional_u32(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<u32>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    json_value_to_u32(value).map(Some).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected integer value for {field}."),
            query_mode,
        )
    })
}

fn json_optional_f64(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<f64>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_f64()
        .ok_or_else(|| {
            json_field_type_error(
                field,
                format!("Expected numeric value for {field}."),
                query_mode,
            )
        })
        .map(Some)
}

fn json_required_f64(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<f64, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Err(missing_parameter_error(field));
    };
    value.as_f64().ok_or_else(|| {
        json_field_type_error(
            field,
            format!("Expected numeric value for {field}."),
            query_mode,
        )
    })
}

fn json_optional_timestamp_millis(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Option<u64>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    json_value_to_timestamp_millis(value).map(Some).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected timestamp value for {field}."),
            query_mode,
        )
    })
}

fn json_optional_epoch_millis(
    body: &JsonValue,
    field: &str,
) -> Result<Option<u64>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    json_value_to_epoch_millis(value).map(Some).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected millisecond timestamp value for {field}."),
            false,
        )
    })
}

fn json_required_timestamp_millis(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<u64, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Err(missing_parameter_error(field));
    };
    json_value_to_timestamp_millis(value).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected timestamp value for {field}."),
            query_mode,
        )
    })
}

fn json_required_epoch_millis(
    body: &JsonValue,
    field: &str,
) -> Result<u64, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Err(missing_parameter_error(field));
    };
    json_value_to_epoch_millis(value).map_err(|_| {
        json_field_type_error(
            field,
            format!("Expected millisecond timestamp value for {field}."),
            false,
        )
    })
}

fn json_array<'a>(
    body: &'a JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<&'a [JsonValue], AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(&[]);
    };
    if value.is_null() {
        return Ok(&[]);
    }
    value.as_array().map(Vec::as_slice).ok_or_else(|| {
        json_field_type_error(
            field,
            format!("Expected array value for {field}."),
            query_mode,
        )
    })
}

fn json_string_list(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<String>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let Some(array) = value.as_array() else {
        return Err(json_field_type_error(
            field,
            format!("Expected array value for {field}."),
            query_mode,
        ));
    };

    array
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                json_field_type_error(
                    field,
                    format!("Expected string values for {field}."),
                    query_mode,
                )
            })
        })
        .collect()
}

fn json_f64_list(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<Vec<f64>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let Some(array) = value.as_array() else {
        return Err(json_field_type_error(
            field,
            format!("Expected array value for {field}."),
            query_mode,
        ));
    };
    array
        .iter()
        .map(|value| {
            value.as_f64().ok_or_else(|| {
                json_field_type_error(
                    field,
                    format!("Expected numeric values for {field}."),
                    query_mode,
                )
            })
        })
        .collect()
}

fn json_string_map(
    body: &JsonValue,
    field: &str,
    query_mode: bool,
) -> Result<BTreeMap<String, String>, AwsError> {
    let Some(value) = json_field(body, field) else {
        return Ok(BTreeMap::new());
    };
    if value.is_null() {
        return Ok(BTreeMap::new());
    }
    let Some(object) = value.as_object() else {
        return Err(json_field_type_error(
            field,
            format!("Expected object value for {field}."),
            query_mode,
        ));
    };

    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_owned()))
                .ok_or_else(|| {
                    json_field_type_error(
                        field,
                        format!("Expected string values for {field}."),
                        query_mode,
                    )
                })
        })
        .collect()
}

fn json_value_to_i32(value: &JsonValue) -> Result<i32, ()> {
    let Some(number) = value.as_i64() else {
        return Err(());
    };
    i32::try_from(number).map_err(|_| ())
}

fn json_value_to_u32(value: &JsonValue) -> Result<u32, ()> {
    let Some(number) = value.as_u64() else {
        return Err(());
    };
    u32::try_from(number).map_err(|_| ())
}

fn json_value_to_timestamp_millis(value: &JsonValue) -> Result<u64, ()> {
    match value {
        JsonValue::String(value) => {
            let parsed =
                OffsetDateTime::parse(value, &Rfc3339).map_err(|_| ())?;
            let nanos = parsed.unix_timestamp_nanos();
            if nanos < 0 {
                return Err(());
            }
            Ok((nanos / 1_000_000) as u64)
        }
        JsonValue::Number(number) => {
            let seconds = number.as_f64().ok_or(())?;
            if !seconds.is_finite() || seconds < 0.0 {
                return Err(());
            }
            Ok((seconds * 1_000.0).round() as u64)
        }
        _ => Err(()),
    }
}

fn json_value_to_epoch_millis(value: &JsonValue) -> Result<u64, ()> {
    let JsonValue::Number(number) = value else {
        return Err(());
    };
    if let Some(value) = number.as_u64() {
        return Ok(value);
    }
    number
        .as_i64()
        .ok_or(())
        .and_then(|value| u64::try_from(value).map_err(|_| ()))
}

fn serialize_json_value(value: &JsonValue) -> Result<Vec<u8>, AwsError> {
    serde_json::to_vec(value).map_err(|error| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            format!("Failed to serialize CloudWatch JSON response: {error}"),
            500,
            false,
        )
    })
}

fn serialize_cbor_value(value: &CborValue) -> Result<Vec<u8>, AwsError> {
    let mut body = Vec::new();
    into_writer(value, &mut body).map_err(|error| {
        AwsError::trusted_custom(
            AwsErrorFamily::Internal,
            "InternalFailure",
            format!("Failed to serialize CloudWatch CBOR response: {error}"),
            500,
            false,
        )
    })?;
    Ok(body)
}

fn json_number_value(value: f64) -> JsonValue {
    match JsonNumber::from_f64(value) {
        Some(number) => JsonValue::Number(number),
        None => JsonValue::String(value.to_string()),
    }
}

fn timestamp_seconds(timestamp_millis: u64) -> f64 {
    timestamp_millis as f64 / 1_000.0
}

fn format_query_timestamp(timestamp_millis: u64) -> String {
    let nanos = i128::from(timestamp_millis).saturating_mul(1_000_000);
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

fn format_f64(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn json_to_cbor_value(value: &JsonValue) -> CborValue {
    match value {
        JsonValue::Null => CborValue::Null,
        JsonValue::Bool(value) => CborValue::Bool(*value),
        JsonValue::Number(value) => value
            .as_u64()
            .map(CborInteger::from)
            .map(CborValue::Integer)
            .or_else(|| {
                value.as_i64().map(CborInteger::from).map(CborValue::Integer)
            })
            .unwrap_or_else(|| match value.as_f64() {
                Some(number) => CborValue::Float(number),
                None => CborValue::Text(value.to_string()),
            }),
        JsonValue::String(value) => CborValue::Text(value.clone()),
        JsonValue::Array(values) => {
            CborValue::Array(values.iter().map(json_to_cbor_value).collect())
        }
        JsonValue::Object(values) => {
            let object = values
                .iter()
                .map(|(key, value)| {
                    if key.ends_with("Timestamp")
                        || key == "Timestamp"
                        || key == "Timestamps"
                    {
                        (
                            CborValue::Text(key.clone()),
                            cbor_timestamp_field(value),
                        )
                    } else {
                        (
                            CborValue::Text(key.clone()),
                            json_to_cbor_value(value),
                        )
                    }
                })
                .collect();
            CborValue::Map(object)
        }
    }
}

fn cbor_timestamp_field(value: &JsonValue) -> CborValue {
    match value {
        JsonValue::Number(number) => match number.as_f64() {
            Some(timestamp) => {
                CborValue::Tag(1, Box::new(CborValue::Float(timestamp)))
            }
            None => CborValue::Text(number.to_string()),
        },
        JsonValue::Array(values) => {
            CborValue::Array(values.iter().map(cbor_timestamp_field).collect())
        }
        _ => json_to_cbor_value(value),
    }
}

fn cbor_to_json_value(value: CborValue) -> Result<JsonValue, String> {
    match value {
        CborValue::Null => Ok(JsonValue::Null),
        CborValue::Bool(value) => Ok(JsonValue::Bool(value)),
        CborValue::Integer(value) => {
            if let Ok(value) = i64::try_from(value) {
                Ok(JsonValue::Number(value.into()))
            } else if let Ok(value) = u64::try_from(value) {
                Ok(JsonValue::Number(value.into()))
            } else {
                Err("CBOR integer does not fit into supported JSON number range.".to_owned())
            }
        }
        CborValue::Float(value) => Ok(json_number_value(value)),
        CborValue::Text(value) => Ok(JsonValue::String(value)),
        CborValue::Bytes(_) => {
            Err("CBOR byte strings are not supported in CloudWatch requests."
                .to_owned())
        }
        CborValue::Tag(_, value) => cbor_to_json_value(*value),
        CborValue::Array(values) => Ok(JsonValue::Array(
            values
                .into_iter()
                .map(cbor_to_json_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        CborValue::Map(values) => {
            let mut object = JsonMap::new();
            for (key, value) in values {
                let CborValue::Text(key) = key else {
                    return Err(
                        "CBOR object keys must be strings for CloudWatch requests."
                            .to_owned(),
                    );
                };
                object.insert(key, cbor_to_json_value(value)?);
            }
            Ok(JsonValue::Object(object))
        }
        _ => Err("Unsupported CBOR value in CloudWatch request.".to_owned()),
    }
}

fn cloudwatch_scope(context: &RequestContext) -> CloudWatchScope {
    CloudWatchScope::new(
        context.account_id().clone(),
        context.region().clone(),
    )
}

fn request_query_mode(request: &HttpRequest<'_>) -> bool {
    request
        .header("x-amzn-query-mode")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn metrics_validation_error(
    message: impl Into<String>,
    query_mode: bool,
) -> AwsError {
    CloudWatchMetricsError::InvalidParameterValue { message: message.into() }
        .to_aws_error(query_mode)
}

fn logs_validation_error(message: impl Into<String>) -> AwsError {
    CloudWatchLogsError::InvalidParameterException { message: message.into() }
        .to_aws_error()
}

fn logs_aws_error(error: CloudWatchLogsError) -> AwsError {
    error.to_aws_error()
}

fn json_field_type_error(
    field: &str,
    message: impl Into<String>,
    query_mode: bool,
) -> AwsError {
    if field.chars().next().is_some_and(char::is_uppercase) {
        metrics_validation_error(message, query_mode)
    } else {
        logs_validation_error(message)
    }
}

fn unknown_operation_error(operation: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::UnsupportedOperation,
        "UnknownOperationException",
        format!("Operation {operation} is not supported."),
        400,
        true,
    )
}
