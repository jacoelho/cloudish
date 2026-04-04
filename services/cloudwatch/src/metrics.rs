use crate::{
    current_epoch_millis,
    errors::{CloudWatchMetricsError, MetricNamespaceError},
    scope::CloudWatchScope,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct MetricNamespace(String);

impl MetricNamespace {
    /// # Errors
    ///
    /// Returns [`MetricNamespaceError::Blank`] when the namespace is empty
    /// after trimming.
    pub fn new(
        value: impl Into<String>,
    ) -> Result<Self, MetricNamespaceError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty() {
            return Err(MetricNamespaceError::Blank);
        }

        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MetricNamespace {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for MetricNamespace {
    type Err = MetricNamespaceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_owned())
    }
}

impl TryFrom<String> for MetricNamespace {
    type Error = MetricNamespaceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<MetricNamespace> for String {
    fn from(value: MetricNamespace) -> Self {
        value.0
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Dimension {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionFilter {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatisticSet {
    pub maximum: f64,
    pub minimum: f64,
    pub sample_count: f64,
    pub sum: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricDatum {
    pub counts: Vec<f64>,
    pub dimensions: Vec<Dimension>,
    pub metric_name: String,
    pub statistic_values: Option<StatisticSet>,
    pub storage_resolution: Option<i32>,
    pub timestamp_millis: Option<u64>,
    pub unit: Option<String>,
    pub value: Option<f64>,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMetricsInput {
    pub dimensions: Vec<DimensionFilter>,
    pub metric_name: Option<String>,
    pub namespace: Option<MetricNamespace>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricDescriptor {
    pub dimensions: Vec<Dimension>,
    pub metric_name: String,
    pub namespace: MetricNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMetricsOutput {
    pub metrics: Vec<MetricDescriptor>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMetricStatisticsInput {
    pub dimensions: Vec<Dimension>,
    pub end_time_millis: u64,
    pub metric_name: String,
    pub namespace: MetricNamespace,
    pub period_seconds: i32,
    pub start_time_millis: u64,
    pub statistics: Vec<String>,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricDatapoint {
    pub average: Option<f64>,
    pub maximum: Option<f64>,
    pub minimum: Option<f64>,
    pub sample_count: Option<f64>,
    pub sum: Option<f64>,
    pub timestamp_millis: u64,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GetMetricStatisticsOutput {
    pub datapoints: Vec<MetricDatapoint>,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PutMetricAlarmInput {
    pub actions_enabled: Option<bool>,
    pub alarm_actions: Vec<String>,
    pub alarm_description: Option<String>,
    pub alarm_name: String,
    pub comparison_operator: String,
    pub datapoints_to_alarm: Option<i32>,
    pub dimensions: Vec<Dimension>,
    pub evaluation_periods: i32,
    pub insufficient_data_actions: Vec<String>,
    pub metric_name: Option<String>,
    pub metrics: Vec<MetricDataQuery>,
    pub namespace: Option<MetricNamespace>,
    pub ok_actions: Vec<String>,
    pub period_seconds: Option<i32>,
    pub statistic: Option<String>,
    pub threshold: Option<f64>,
    pub threshold_metric_id: Option<String>,
    pub treat_missing_data: Option<String>,
    pub unit: Option<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum AlarmStateValue {
    Ok,
    Alarm,
    InsufficientData,
}

impl AlarmStateValue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Alarm => "ALARM",
            Self::InsufficientData => "INSUFFICIENT_DATA",
        }
    }

    /// # Errors
    ///
    /// Returns [`CloudWatchMetricsError::InvalidFormat`] when the alarm state
    /// value is unsupported.
    pub fn parse(value: &str) -> Result<Self, CloudWatchMetricsError> {
        match value {
            "OK" => Ok(Self::Ok),
            "ALARM" => Ok(Self::Alarm),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            other => Err(CloudWatchMetricsError::InvalidFormat {
                message: format!(
                    "Value '{other}' at 'stateValue' failed to satisfy the \
                     supported alarm state set."
                ),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricAlarm {
    pub actions_enabled: bool,
    pub alarm_actions: Vec<String>,
    pub alarm_arn: String,
    pub alarm_configuration_updated_timestamp_millis: u64,
    pub alarm_description: Option<String>,
    pub alarm_name: String,
    pub comparison_operator: String,
    pub dimensions: Vec<Dimension>,
    pub evaluation_periods: i32,
    pub insufficient_data_actions: Vec<String>,
    pub metric_name: String,
    pub namespace: MetricNamespace,
    pub ok_actions: Vec<String>,
    pub period_seconds: i32,
    pub state_reason: String,
    pub state_reason_data: Option<String>,
    pub state_updated_timestamp_millis: u64,
    pub state_value: AlarmStateValue,
    pub statistic: String,
    pub threshold: f64,
    pub treat_missing_data: Option<String>,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAlarmsInput {
    pub alarm_name_prefix: Option<String>,
    pub alarm_names: Vec<String>,
    pub max_records: Option<i32>,
    pub next_token: Option<String>,
    pub state_value: Option<AlarmStateValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeAlarmsOutput {
    pub metric_alarms: Vec<MetricAlarm>,
    pub next_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAlarmsInput {
    pub alarm_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAlarmStateInput {
    pub alarm_name: String,
    pub state_reason: String,
    pub state_reason_data: Option<String>,
    pub state_value: AlarmStateValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metric {
    pub dimensions: Vec<Dimension>,
    pub metric_name: String,
    pub namespace: MetricNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricStat {
    pub metric: Metric,
    pub period_seconds: i32,
    pub stat: String,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricDataQuery {
    pub expression: Option<String>,
    pub id: String,
    pub label: Option<String>,
    pub metric_stat: Option<MetricStat>,
    pub period_seconds: Option<i32>,
    pub return_data: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanBy {
    TimestampAscending,
    TimestampDescending,
}

impl ScanBy {
    /// # Errors
    ///
    /// Returns [`CloudWatchMetricsError::InvalidParameterValue`] when the scan
    /// direction is unsupported.
    pub fn parse(value: &str) -> Result<Self, CloudWatchMetricsError> {
        match value {
            "TimestampAscending" => Ok(Self::TimestampAscending),
            "TimestampDescending" => Ok(Self::TimestampDescending),
            other => Err(CloudWatchMetricsError::InvalidParameterValue {
                message: format!("Unsupported ScanBy value {other}."),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMetricDataInput {
    pub end_time_millis: u64,
    pub max_datapoints: Option<i32>,
    pub metric_data_queries: Vec<MetricDataQuery>,
    pub next_token: Option<String>,
    pub scan_by: Option<ScanBy>,
    pub start_time_millis: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricDataResult {
    pub id: String,
    pub label: String,
    pub status_code: String,
    pub timestamps_millis: Vec<u64>,
    pub values: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GetMetricDataOutput {
    pub metric_data_results: Vec<MetricDataResult>,
    pub next_token: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct MetricSeriesStorageKey {
    pub(crate) account_id: aws::AccountId,
    pub(crate) dimensions: Vec<Dimension>,
    pub(crate) metric_name: String,
    pub(crate) namespace: MetricNamespace,
    pub(crate) region: aws::RegionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MetricSample {
    pub(crate) max: f64,
    pub(crate) min: f64,
    pub(crate) sample_count: f64,
    pub(crate) sum: f64,
    pub(crate) timestamp_millis: u64,
    pub(crate) unit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMetricSeries {
    pub(crate) samples: Vec<MetricSample>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct AlarmStorageKey {
    pub(crate) account_id: aws::AccountId,
    pub(crate) alarm_name: String,
    pub(crate) region: aws::RegionId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredAlarm {
    pub(crate) actions_enabled: bool,
    pub(crate) alarm_actions: Vec<String>,
    pub(crate) alarm_configuration_updated_timestamp_millis: u64,
    pub(crate) alarm_description: Option<String>,
    pub(crate) alarm_name: String,
    pub(crate) comparison_operator: String,
    pub(crate) datapoints_to_alarm: Option<i32>,
    pub(crate) dimensions: Vec<Dimension>,
    pub(crate) evaluation_periods: i32,
    pub(crate) insufficient_data_actions: Vec<String>,
    pub(crate) metric_name: String,
    pub(crate) namespace: MetricNamespace,
    pub(crate) ok_actions: Vec<String>,
    pub(crate) period_seconds: i32,
    pub(crate) state_reason: String,
    pub(crate) state_reason_data: Option<String>,
    pub(crate) state_updated_timestamp_millis: u64,
    pub(crate) state_value: AlarmStateValue,
    pub(crate) statistic: String,
    pub(crate) threshold: f64,
    pub(crate) treat_missing_data: Option<String>,
    pub(crate) unit: Option<String>,
}

pub(crate) fn metric_series_key(
    scope: &CloudWatchScope,
    namespace: &MetricNamespace,
    metric_name: &str,
    dimensions: Vec<Dimension>,
) -> MetricSeriesStorageKey {
    MetricSeriesStorageKey {
        account_id: scope.account_id().clone(),
        dimensions,
        metric_name: metric_name.to_owned(),
        namespace: namespace.clone(),
        region: scope.region().clone(),
    }
}

pub(crate) fn alarm_key(
    scope: &CloudWatchScope,
    alarm_name: &str,
) -> AlarmStorageKey {
    AlarmStorageKey {
        account_id: scope.account_id().clone(),
        alarm_name: alarm_name.to_owned(),
        region: scope.region().clone(),
    }
}

pub(crate) fn alarm_scope_filter(
    scope: &CloudWatchScope,
) -> impl Fn(&AlarmStorageKey) -> bool + Send + Sync + 'static {
    let account_id = scope.account_id().clone();
    let region = scope.region().clone();

    move |key| key.account_id == account_id && key.region == region
}

pub(crate) fn build_metric_alarm(
    scope: &CloudWatchScope,
    alarm: StoredAlarm,
) -> MetricAlarm {
    MetricAlarm {
        actions_enabled: alarm.actions_enabled,
        alarm_actions: alarm.alarm_actions,
        alarm_arn: format_alarm_arn(scope, &alarm.alarm_name),
        alarm_configuration_updated_timestamp_millis: alarm
            .alarm_configuration_updated_timestamp_millis,
        alarm_description: alarm.alarm_description,
        alarm_name: alarm.alarm_name,
        comparison_operator: alarm.comparison_operator,
        dimensions: alarm.dimensions,
        evaluation_periods: alarm.evaluation_periods,
        insufficient_data_actions: alarm.insufficient_data_actions,
        metric_name: alarm.metric_name,
        namespace: alarm.namespace,
        ok_actions: alarm.ok_actions,
        period_seconds: alarm.period_seconds,
        state_reason: alarm.state_reason,
        state_reason_data: alarm.state_reason_data,
        state_updated_timestamp_millis: alarm.state_updated_timestamp_millis,
        state_value: alarm.state_value,
        statistic: alarm.statistic,
        threshold: alarm.threshold,
        treat_missing_data: alarm.treat_missing_data,
        unit: alarm.unit,
    }
}

pub(crate) fn validate_metric_name(
    field: &str,
    value: &str,
) -> Result<String, CloudWatchMetricsError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CloudWatchMetricsError::InvalidParameterValue {
            message: format!("The {field} must not be blank."),
        });
    }

    Ok(value.to_owned())
}

pub(crate) fn validate_required_metric_field(
    field: &str,
    value: Option<String>,
) -> Result<String, CloudWatchMetricsError> {
    let value =
        value.ok_or_else(|| CloudWatchMetricsError::MissingParameter {
            message: format!("{field} is required."),
        })?;
    validate_metric_name(field, &value)
}

pub(crate) fn validate_positive_i32(
    field: &str,
    value: i32,
) -> Result<i32, CloudWatchMetricsError> {
    if value <= 0 {
        return Err(CloudWatchMetricsError::InvalidParameterValue {
            message: format!("{field} must be positive."),
        });
    }

    Ok(value)
}

pub(crate) fn validate_required_positive_i32(
    field: &str,
    value: Option<i32>,
) -> Result<i32, CloudWatchMetricsError> {
    let value =
        value.ok_or_else(|| CloudWatchMetricsError::MissingParameter {
            message: format!("{field} is required."),
        })?;
    validate_positive_i32(field, value)
}

pub(crate) fn validate_time_range(
    start_time_millis: u64,
    end_time_millis: u64,
    operation: &str,
) -> Result<(), CloudWatchMetricsError> {
    if start_time_millis >= end_time_millis {
        return Err(CloudWatchMetricsError::InvalidParameterValue {
            message: format!(
                "{operation} requires StartTime to be earlier than EndTime."
            ),
        });
    }

    Ok(())
}

pub(crate) fn reject_metric_token(
    operation: &str,
    next_token: Option<&str>,
) -> Result<(), CloudWatchMetricsError> {
    if next_token.is_some() {
        return Err(CloudWatchMetricsError::InvalidNextToken {
            message: format!(
                "{operation} pagination tokens are not supported."
            ),
        });
    }

    Ok(())
}

pub(crate) fn parse_metric_token(
    token: &str,
) -> Result<usize, CloudWatchMetricsError> {
    token.parse::<usize>().map_err(|_| {
        CloudWatchMetricsError::InvalidNextToken {
            message: "The provided NextToken is invalid.".to_owned(),
        }
    })
}

pub(crate) fn canonical_dimensions(
    dimensions: Vec<Dimension>,
) -> Result<Vec<Dimension>, CloudWatchMetricsError> {
    let mut seen = BTreeSet::new();
    let mut canonical = Vec::with_capacity(dimensions.len());

    for dimension in dimensions {
        let name = validate_metric_name("dimension name", &dimension.name)?;
        let value = validate_metric_name("dimension value", &dimension.value)?;
        if !seen.insert(name.clone()) {
            return Err(CloudWatchMetricsError::InvalidParameterValue {
                message: format!("Duplicate dimension {name} is not allowed."),
            });
        }
        canonical.push(Dimension { name, value });
    }

    canonical.sort();
    Ok(canonical)
}

pub(crate) fn canonical_dimension_filters(
    filters: Vec<DimensionFilter>,
) -> Result<Vec<DimensionFilter>, CloudWatchMetricsError> {
    let mut seen = BTreeSet::new();
    let mut canonical = Vec::with_capacity(filters.len());

    for filter in filters {
        let name =
            validate_metric_name("dimension filter name", &filter.name)?;
        if !seen.insert(name.clone()) {
            return Err(CloudWatchMetricsError::InvalidParameterValue {
                message: format!(
                    "Duplicate dimension filter {name} is not allowed."
                ),
            });
        }
        let value = filter
            .value
            .map(|value| {
                validate_metric_name("dimension filter value", &value)
            })
            .transpose()?;
        canonical.push(DimensionFilter { name, value });
    }

    canonical.sort_by(|left, right| {
        left.name.cmp(&right.name).then_with(|| left.value.cmp(&right.value))
    });
    Ok(canonical)
}

pub(crate) fn canonical_statistics(
    statistics: Vec<String>,
) -> Result<Vec<String>, CloudWatchMetricsError> {
    if statistics.is_empty() {
        return Err(CloudWatchMetricsError::MissingParameter {
            message: "Statistics must contain at least one statistic."
                .to_owned(),
        });
    }

    let mut canonical = Vec::with_capacity(statistics.len());
    for statistic in statistics {
        let statistic = statistic.trim().to_owned();
        if !matches!(
            statistic.as_str(),
            "Average" | "Maximum" | "Minimum" | "SampleCount" | "Sum"
        ) {
            return Err(CloudWatchMetricsError::InvalidParameterValue {
                message: format!("Unsupported statistic {statistic}."),
            });
        }
        canonical.push(statistic);
    }

    canonical.sort();
    canonical.dedup();
    Ok(canonical)
}

pub(crate) fn metric_matches_filters(
    dimensions: &[Dimension],
    filters: &[DimensionFilter],
) -> bool {
    filters.iter().all(|filter| {
        dimensions.iter().any(|dimension| {
            dimension.name == filter.name
                && filter
                    .value
                    .as_deref()
                    .is_none_or(|value| dimension.value == value)
        })
    })
}

pub(crate) fn sample_from_metric_datum(
    datum: &MetricDatum,
    clock: &dyn aws::Clock,
) -> Result<MetricSample, CloudWatchMetricsError> {
    if let Some(storage_resolution) = datum.storage_resolution
        && storage_resolution <= 0
    {
        return Err(CloudWatchMetricsError::InvalidParameterValue {
            message: "StorageResolution must be positive.".to_owned(),
        });
    }
    let timestamp_millis =
        datum.timestamp_millis.unwrap_or_else(|| current_epoch_millis(clock));
    let populated = [
        datum.value.is_some(),
        datum.statistic_values.is_some(),
        !datum.values.is_empty(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count();
    if populated != 1 {
        return Err(CloudWatchMetricsError::InvalidParameterCombination {
            message:
                "Metric data must include exactly one of Value, StatisticValues, \
                 or Values."
                    .to_owned(),
        });
    }

    if let Some(value) = datum.value {
        return Ok(MetricSample {
            max: value,
            min: value,
            sample_count: 1.0,
            sum: value,
            timestamp_millis,
            unit: datum.unit.clone(),
        });
    }

    if let Some(statistic_values) = &datum.statistic_values {
        if statistic_values.sample_count <= 0.0 {
            return Err(CloudWatchMetricsError::InvalidParameterValue {
                message: "StatisticValues.SampleCount must be positive."
                    .to_owned(),
            });
        }
        if statistic_values.minimum > statistic_values.maximum {
            return Err(CloudWatchMetricsError::InvalidParameterValue {
                message: "StatisticValues.Minimum must be <= Maximum."
                    .to_owned(),
            });
        }

        return Ok(MetricSample {
            max: statistic_values.maximum,
            min: statistic_values.minimum,
            sample_count: statistic_values.sample_count,
            sum: statistic_values.sum,
            timestamp_millis,
            unit: datum.unit.clone(),
        });
    }

    let counts = if datum.counts.is_empty() {
        vec![1.0; datum.values.len()]
    } else {
        if datum.counts.len() != datum.values.len() {
            return Err(CloudWatchMetricsError::InvalidParameterCombination {
                message:
                    "Values and Counts must contain the same number of entries."
                        .to_owned(),
            });
        }
        datum.counts.clone()
    };

    if datum.values.is_empty() {
        return Err(CloudWatchMetricsError::InvalidParameterValue {
            message: "Values must contain at least one entry.".to_owned(),
        });
    }

    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut sample_count = 0.0;
    let mut sum = 0.0;
    for (value, count) in datum.values.iter().zip(counts.iter()) {
        min = min.min(*value);
        max = max.max(*value);
        sample_count += *count;
        sum += *value * *count;
    }

    Ok(MetricSample {
        max,
        min,
        sample_count,
        sum,
        timestamp_millis,
        unit: datum.unit.clone(),
    })
}

pub(crate) fn aggregate_statistics(
    samples: &[MetricSample],
    start_time_millis: u64,
    end_time_millis: u64,
    period_seconds: i32,
    requested_unit: Option<&str>,
    statistics: &[String],
) -> Vec<MetricDatapoint> {
    let period_millis = (period_seconds as u64).saturating_mul(1_000);
    if period_millis == 0 {
        return Vec::new();
    }
    let mut buckets = BTreeMap::<u64, Bucket>::new();

    for sample in samples {
        if sample.timestamp_millis < start_time_millis
            || sample.timestamp_millis > end_time_millis
        {
            continue;
        }
        if requested_unit
            .is_some_and(|unit| sample.unit.as_deref() != Some(unit))
        {
            continue;
        }
        let bucket_start = sample
            .timestamp_millis
            .checked_div(period_millis)
            .unwrap_or(0)
            .saturating_mul(period_millis);
        let bucket = buckets.entry(bucket_start).or_insert_with(|| Bucket {
            max: sample.max,
            min: sample.min,
            sample_count: 0.0,
            sum: 0.0,
            unit: sample.unit.clone(),
        });
        bucket.max = bucket.max.max(sample.max);
        bucket.min = bucket.min.min(sample.min);
        bucket.sample_count += sample.sample_count;
        bucket.sum += sample.sum;
        if bucket.unit.is_none() {
            bucket.unit = sample.unit.clone();
        }
    }

    buckets
        .into_iter()
        .map(|(timestamp_millis, bucket)| MetricDatapoint {
            average: statistics.iter().any(|stat| stat == "Average").then(
                || {
                    if bucket.sample_count == 0.0 {
                        0.0
                    } else {
                        bucket.sum / bucket.sample_count
                    }
                },
            ),
            maximum: statistics
                .iter()
                .any(|stat| stat == "Maximum")
                .then_some(bucket.max),
            minimum: statistics
                .iter()
                .any(|stat| stat == "Minimum")
                .then_some(bucket.min),
            sample_count: statistics
                .iter()
                .any(|stat| stat == "SampleCount")
                .then_some(bucket.sample_count),
            sum: statistics
                .iter()
                .any(|stat| stat == "Sum")
                .then_some(bucket.sum),
            timestamp_millis,
            unit: bucket.unit,
        })
        .collect()
}

pub(crate) fn metric_data_value(
    datapoint: &MetricDatapoint,
    statistic: &str,
) -> f64 {
    match statistic {
        "Average" => datapoint.average.unwrap_or(0.0),
        "Maximum" => datapoint.maximum.unwrap_or(0.0),
        "Minimum" => datapoint.minimum.unwrap_or(0.0),
        "SampleCount" => datapoint.sample_count.unwrap_or(0.0),
        "Sum" => datapoint.sum.unwrap_or(0.0),
        _ => 0.0,
    }
}

pub(crate) fn order_datapoints(
    datapoints: &mut [MetricDatapoint],
    scan_by: ScanBy,
) {
    datapoints.sort_by(|left, right| {
        left.timestamp_millis.cmp(&right.timestamp_millis)
    });
    if matches!(scan_by, ScanBy::TimestampDescending) {
        datapoints.reverse();
    }
}

pub(crate) fn format_alarm_arn(
    scope: &CloudWatchScope,
    alarm_name: &str,
) -> String {
    format!(
        "arn:aws:cloudwatch:{}:{}:alarm:{alarm_name}",
        scope.region().as_str(),
        scope.account_id().as_str()
    )
}

#[derive(Debug, Clone, PartialEq)]
struct Bucket {
    max: f64,
    min: f64,
    sample_count: f64,
    sum: f64,
    unit: Option<String>,
}
