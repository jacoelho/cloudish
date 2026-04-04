mod errors;
mod logs;
mod metrics;
mod scope;

use self::logs::{
    LogGroupStorageKey, LogStreamStorageKey, StoredLogEvent, StoredLogGroup,
    StoredLogStream, build_log_group_description,
    build_log_stream_description, ensure_log_group_exists,
    ensure_log_stream_exists, log_group_key, log_stream_key, parse_logs_token,
    reject_logs_pagination, resolved_log_limit, timestamp_in_range,
    validate_log_events, validate_log_name,
};
use self::metrics::{
    AlarmStorageKey, MetricSeriesStorageKey, StoredAlarm, StoredMetricSeries,
    aggregate_statistics, alarm_key, alarm_scope_filter, build_metric_alarm,
    canonical_dimension_filters, canonical_dimensions, canonical_statistics,
    metric_data_value, metric_matches_filters, metric_series_key,
    order_datapoints, parse_metric_token, reject_metric_token,
    sample_from_metric_datum, validate_metric_name, validate_positive_i32,
    validate_required_metric_field, validate_required_positive_i32,
    validate_time_range,
};
use aws::Clock;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use storage::{StorageFactory, StorageHandle};

pub use self::errors::{
    CloudWatchLogsError, CloudWatchMetricsError, MetricNamespaceError,
};
pub use self::logs::{
    CreateLogGroupInput, CreateLogStreamInput, DeleteRetentionPolicyInput,
    DescribeLogGroupsInput, DescribeLogGroupsOutput, DescribeLogStreamsInput,
    DescribeLogStreamsOutput, FilterLogEventsInput, FilterLogEventsOutput,
    FilteredLogEvent, GetLogEventsInput, GetLogEventsOutput, InputLogEvent,
    LogGroupDescription, LogStreamDescription, OutputLogEvent,
    PutLogEventsInput, PutLogEventsOutput, PutRetentionPolicyInput,
    SearchedLogStream,
};
pub use self::metrics::{
    AlarmStateValue, DeleteAlarmsInput, DescribeAlarmsInput,
    DescribeAlarmsOutput, Dimension, DimensionFilter, GetMetricDataInput,
    GetMetricDataOutput, GetMetricStatisticsInput, GetMetricStatisticsOutput,
    ListMetricsInput, ListMetricsOutput, Metric, MetricAlarm, MetricDataQuery,
    MetricDataResult, MetricDatapoint, MetricDatum, MetricDescriptor,
    MetricNamespace, MetricStat, PutMetricAlarmInput, ScanBy,
    SetAlarmStateInput, StatisticSet,
};
pub use self::scope::CloudWatchScope;

const DEFAULT_MAX_LOG_EVENTS_PER_QUERY: usize = 10_000;
const DEFAULT_METRIC_PAGE_SIZE: usize = 500;
const MAX_METRIC_ALARMS: usize = 1_000;

#[derive(Clone)]
pub struct CloudWatchService {
    clock: Arc<dyn Clock>,
    log_group_store: StorageHandle<LogGroupStorageKey, StoredLogGroup>,
    log_stream_store: StorageHandle<LogStreamStorageKey, StoredLogStream>,
    max_log_events_per_query: usize,
    metric_alarm_store: StorageHandle<AlarmStorageKey, StoredAlarm>,
    metric_store: StorageHandle<MetricSeriesStorageKey, StoredMetricSeries>,
}

impl CloudWatchService {
    pub fn new(factory: &StorageFactory, clock: Arc<dyn Clock>) -> Self {
        Self::with_limits(factory, clock, DEFAULT_MAX_LOG_EVENTS_PER_QUERY)
    }

    pub fn with_limits(
        factory: &StorageFactory,
        clock: Arc<dyn Clock>,
        max_log_events_per_query: usize,
    ) -> Self {
        Self {
            clock,
            log_group_store: factory.create("cloudwatch", "log-groups"),
            log_stream_store: factory.create("cloudwatch", "log-streams"),
            max_log_events_per_query: max_log_events_per_query.max(1),
            metric_alarm_store: factory.create("cloudwatch", "metric-alarms"),
            metric_store: factory.create("cloudwatch", "metrics"),
        }
    }

    /// # Errors
    ///
    /// Returns validation, duplicate-name, or persistence errors when the log
    /// group name is invalid or the group cannot be stored.
    pub fn create_log_group(
        &self,
        scope: &CloudWatchScope,
        input: CreateLogGroupInput,
    ) -> Result<(), CloudWatchLogsError> {
        let name = validate_log_name("log group", &input.log_group_name)?;
        let key = log_group_key(scope, &name);
        if self.log_group_store.get(&key).is_some() {
            return Err(CloudWatchLogsError::ResourceAlreadyExistsException {
                message: format!(
                    "The specified log group {name} already exists."
                ),
            });
        }

        self.log_group_store.put(
            key,
            StoredLogGroup {
                creation_time_millis: current_epoch_millis(&*self.clock),
                retention_in_days: None,
                tags: input.tags,
            },
        )?;

        Ok(())
    }

    /// # Errors
    ///
    /// Returns not-found, validation, or persistence errors when the log group
    /// does not exist or cannot be removed.
    pub fn delete_log_group(
        &self,
        scope: &CloudWatchScope,
        log_group_name: &str,
    ) -> Result<(), CloudWatchLogsError> {
        let name = validate_log_name("log group", log_group_name)?;
        let key = log_group_key(scope, &name);
        ensure_log_group_exists(&self.log_group_store, &key, &name)?;

        self.log_group_store.delete(&key)?;
        for key in
            self.log_stream_store.keys().into_iter().filter(|stream_key| {
                stream_key.account_id == *scope.account_id()
                    && stream_key.region == *scope.region()
                    && stream_key.log_group_name == name
            })
        {
            self.log_stream_store.delete(&key)?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation or pagination errors when the request contains an
    /// invalid name prefix or paging token.
    pub fn describe_log_groups(
        &self,
        scope: &CloudWatchScope,
        input: DescribeLogGroupsInput,
    ) -> Result<DescribeLogGroupsOutput, CloudWatchLogsError> {
        reject_logs_pagination(
            "DescribeLogGroups",
            input.next_token.as_deref(),
        )?;
        let prefix = input.log_group_name_prefix.unwrap_or_default();
        let mut log_groups = self
            .log_group_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
                    && key.name.starts_with(&prefix)
            })
            .filter_map(|key| {
                self.log_group_store.get(&key).map(|stored| {
                    build_log_group_description(scope, &key.name, stored)
                })
            })
            .collect::<Vec<_>>();
        log_groups.sort_by(|left, right| {
            left.log_group_name.cmp(&right.log_group_name)
        });

        Ok(DescribeLogGroupsOutput { log_groups, next_token: None })
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, duplicate-name, or persistence
    /// errors when the log stream cannot be created.
    pub fn create_log_stream(
        &self,
        scope: &CloudWatchScope,
        input: CreateLogStreamInput,
    ) -> Result<(), CloudWatchLogsError> {
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        ensure_log_group_exists(
            &self.log_group_store,
            &log_group_key(scope, &group_name),
            &group_name,
        )?;
        let stream_name =
            validate_log_name("log stream", &input.log_stream_name)?;
        let key = log_stream_key(scope, &group_name, &stream_name);
        if self.log_stream_store.get(&key).is_some() {
            return Err(CloudWatchLogsError::ResourceAlreadyExistsException {
                message: format!(
                    "The specified log stream {stream_name} already exists."
                ),
            });
        }

        self.log_stream_store.put(
            key,
            StoredLogStream {
                creation_time_millis: current_epoch_millis(&*self.clock),
                events: Vec::new(),
            },
        )?;

        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the log
    /// stream does not exist or cannot be removed.
    pub fn delete_log_stream(
        &self,
        scope: &CloudWatchScope,
        log_group_name: &str,
        log_stream_name: &str,
    ) -> Result<(), CloudWatchLogsError> {
        let group_name = validate_log_name("log group", log_group_name)?;
        let stream_name = validate_log_name("log stream", log_stream_name)?;
        let key = log_stream_key(scope, &group_name, &stream_name);
        ensure_log_stream_exists(
            &self.log_stream_store,
            &key,
            &group_name,
            &stream_name,
        )?;
        self.log_stream_store.delete(&key)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, or pagination errors when the
    /// request cannot be satisfied.
    pub fn describe_log_streams(
        &self,
        scope: &CloudWatchScope,
        input: DescribeLogStreamsInput,
    ) -> Result<DescribeLogStreamsOutput, CloudWatchLogsError> {
        reject_logs_pagination(
            "DescribeLogStreams",
            input.next_token.as_deref(),
        )?;
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        ensure_log_group_exists(
            &self.log_group_store,
            &log_group_key(scope, &group_name),
            &group_name,
        )?;
        let prefix = input.log_stream_name_prefix.unwrap_or_default();
        let mut log_streams = self
            .log_stream_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
                    && key.log_group_name == group_name
                    && key.log_stream_name.starts_with(&prefix)
            })
            .filter_map(|key| {
                self.log_stream_store.get(&key).map(|stored| {
                    build_log_stream_description(
                        scope,
                        &group_name,
                        &key.log_stream_name,
                        stored,
                    )
                })
            })
            .collect::<Vec<_>>();
        log_streams.sort_by(|left, right| {
            left.log_stream_name.cmp(&right.log_stream_name)
        });

        Ok(DescribeLogStreamsOutput { log_streams, next_token: None })
    }

    /// # Errors
    ///
    /// Returns validation, missing-stream, or persistence errors when the
    /// event batch is invalid or cannot be stored.
    pub fn put_log_events(
        &self,
        scope: &CloudWatchScope,
        input: PutLogEventsInput,
    ) -> Result<PutLogEventsOutput, CloudWatchLogsError> {
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        let stream_name =
            validate_log_name("log stream", &input.log_stream_name)?;
        let key = log_stream_key(scope, &group_name, &stream_name);
        let mut stored = ensure_log_stream_exists(
            &self.log_stream_store,
            &key,
            &group_name,
            &stream_name,
        )?;
        validate_log_events(&input.log_events)?;

        for event in input.log_events {
            stored.events.push(StoredLogEvent {
                ingestion_time_millis: current_epoch_millis(&*self.clock),
                message: event.message,
                timestamp_millis: event.timestamp_millis,
            });
        }
        stored.events.sort_by(|left, right| {
            left.timestamp_millis
                .cmp(&right.timestamp_millis)
                .then_with(|| {
                    left.ingestion_time_millis
                        .cmp(&right.ingestion_time_millis)
                })
                .then_with(|| left.message.cmp(&right.message))
        });
        let token = stored.events.len().to_string();
        self.log_stream_store.put(key, stored)?;

        Ok(PutLogEventsOutput { next_sequence_token: Some(token) })
    }

    /// # Errors
    ///
    /// Returns validation, missing-stream, or pagination-token errors when the
    /// log event page cannot be resolved.
    pub fn get_log_events(
        &self,
        scope: &CloudWatchScope,
        input: GetLogEventsInput,
    ) -> Result<GetLogEventsOutput, CloudWatchLogsError> {
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        let stream_name =
            validate_log_name("log stream", &input.log_stream_name)?;
        let key = log_stream_key(scope, &group_name, &stream_name);
        let stored = ensure_log_stream_exists(
            &self.log_stream_store,
            &key,
            &group_name,
            &stream_name,
        )?;

        let filtered = stored
            .events
            .into_iter()
            .filter(|event| {
                timestamp_in_range(
                    event.timestamp_millis,
                    input.start_time_millis,
                    input.end_time_millis,
                )
            })
            .collect::<Vec<_>>();
        let limit =
            resolved_log_limit(input.limit, self.max_log_events_per_query);
        let offset = match input.next_token.as_deref() {
            Some(token) => parse_logs_token(token)?,
            None if input.start_from_head => 0,
            None => filtered.len().saturating_sub(limit),
        };
        let start = offset.min(filtered.len());
        let end = start.saturating_add(limit).min(filtered.len());
        let events = match filtered.get(start..end) {
            Some(page) => page.to_vec(),
            None => Vec::new(),
        }
        .iter()
        .map(|event| OutputLogEvent {
            ingestion_time_millis: event.ingestion_time_millis,
            message: event.message.clone(),
            timestamp_millis: event.timestamp_millis,
        })
        .collect::<Vec<_>>();

        Ok(GetLogEventsOutput {
            events,
            next_backward_token: start.to_string(),
            next_forward_token: end.to_string(),
        })
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, or pagination-token errors when the
    /// filtered event page cannot be resolved.
    pub fn filter_log_events(
        &self,
        scope: &CloudWatchScope,
        input: FilterLogEventsInput,
    ) -> Result<FilterLogEventsOutput, CloudWatchLogsError> {
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        ensure_log_group_exists(
            &self.log_group_store,
            &log_group_key(scope, &group_name),
            &group_name,
        )?;

        let requested_streams = input
            .log_stream_names
            .into_iter()
            .map(|name| validate_log_name("log stream", &name))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let prefix = input.log_stream_name_prefix.unwrap_or_default();
        let pattern = input.filter_pattern.unwrap_or_default();

        let mut matching = Vec::new();
        let mut searched = Vec::new();
        for key in self.log_stream_store.keys().into_iter().filter(|key| {
            key.account_id == *scope.account_id()
                && key.region == *scope.region()
                && key.log_group_name == group_name
        }) {
            if !requested_streams.is_empty()
                && !requested_streams.contains(&key.log_stream_name)
            {
                continue;
            }
            if !prefix.is_empty() && !key.log_stream_name.starts_with(&prefix)
            {
                continue;
            }
            let Some(stream) = self.log_stream_store.get(&key) else {
                continue;
            };

            searched.push(SearchedLogStream {
                log_stream_name: key.log_stream_name.clone(),
                searched_completely: true,
            });

            for (index, event) in stream.events.iter().enumerate() {
                if !timestamp_in_range(
                    event.timestamp_millis,
                    input.start_time_millis,
                    input.end_time_millis,
                ) {
                    continue;
                }
                if !pattern.is_empty() && !event.message.contains(&pattern) {
                    continue;
                }
                matching.push(FilteredLogEvent {
                    event_id: format!(
                        "{group_name}:{}:{index}",
                        key.log_stream_name
                    ),
                    ingestion_time_millis: event.ingestion_time_millis,
                    log_stream_name: key.log_stream_name.clone(),
                    message: event.message.clone(),
                    timestamp_millis: event.timestamp_millis,
                });
            }
        }

        matching.sort_by(|left, right| {
            left.timestamp_millis
                .cmp(&right.timestamp_millis)
                .then_with(|| {
                    left.ingestion_time_millis
                        .cmp(&right.ingestion_time_millis)
                })
                .then_with(|| left.log_stream_name.cmp(&right.log_stream_name))
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        searched.sort_by(|left, right| {
            left.log_stream_name.cmp(&right.log_stream_name)
        });

        let limit =
            resolved_log_limit(input.limit, self.max_log_events_per_query);
        let start = input
            .next_token
            .as_deref()
            .map(parse_logs_token)
            .transpose()?
            .unwrap_or(0)
            .min(matching.len());
        let end = start.saturating_add(limit).min(matching.len());

        Ok(FilterLogEventsOutput {
            events: match matching.get(start..end) {
                Some(page) => page.to_vec(),
                None => Vec::new(),
            },
            next_token: (end < matching.len()).then(|| end.to_string()),
            searched_log_streams: searched,
        })
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, or persistence errors when the
    /// retention policy cannot be updated.
    pub fn put_retention_policy(
        &self,
        scope: &CloudWatchScope,
        input: PutRetentionPolicyInput,
    ) -> Result<(), CloudWatchLogsError> {
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        if input.retention_in_days <= 0 {
            return Err(CloudWatchLogsError::InvalidParameterException {
                message: "RetentionInDays must be positive.".to_owned(),
            });
        }
        let key = log_group_key(scope, &group_name);
        let mut stored =
            ensure_log_group_exists(&self.log_group_store, &key, &group_name)?;
        stored.retention_in_days = Some(input.retention_in_days);
        self.log_group_store.put(key, stored)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, or persistence errors when the
    /// retention policy cannot be removed.
    pub fn delete_retention_policy(
        &self,
        scope: &CloudWatchScope,
        input: DeleteRetentionPolicyInput,
    ) -> Result<(), CloudWatchLogsError> {
        let group_name =
            validate_log_name("log group", &input.log_group_name)?;
        let key = log_group_key(scope, &group_name);
        let mut stored =
            ensure_log_group_exists(&self.log_group_store, &key, &group_name)?;
        stored.retention_in_days = None;
        self.log_group_store.put(key, stored)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, or persistence errors when the log
    /// group tags cannot be updated.
    pub fn tag_log_group(
        &self,
        scope: &CloudWatchScope,
        log_group_name: &str,
        tags: BTreeMap<String, String>,
    ) -> Result<(), CloudWatchLogsError> {
        let group_name = validate_log_name("log group", log_group_name)?;
        let key = log_group_key(scope, &group_name);
        let mut stored =
            ensure_log_group_exists(&self.log_group_store, &key, &group_name)?;
        stored.tags.extend(tags);
        self.log_group_store.put(key, stored)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, missing-group, or persistence errors when the log
    /// group tags cannot be updated.
    pub fn untag_log_group(
        &self,
        scope: &CloudWatchScope,
        log_group_name: &str,
        tag_keys: &[String],
    ) -> Result<(), CloudWatchLogsError> {
        let group_name = validate_log_name("log group", log_group_name)?;
        let key = log_group_key(scope, &group_name);
        let mut stored =
            ensure_log_group_exists(&self.log_group_store, &key, &group_name)?;
        for tag_key in tag_keys {
            stored.tags.remove(tag_key);
        }
        self.log_group_store.put(key, stored)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation or not-found errors when the log group does not
    /// exist.
    pub fn list_tags_log_group(
        &self,
        scope: &CloudWatchScope,
        log_group_name: &str,
    ) -> Result<BTreeMap<String, String>, CloudWatchLogsError> {
        let group_name = validate_log_name("log group", log_group_name)?;
        let key = log_group_key(scope, &group_name);
        let stored =
            ensure_log_group_exists(&self.log_group_store, &key, &group_name)?;
        Ok(stored.tags)
    }

    /// # Errors
    ///
    /// Returns validation or persistence errors when the metric datum set is
    /// invalid or cannot be stored.
    pub fn put_metric_data(
        &self,
        scope: &CloudWatchScope,
        namespace: MetricNamespace,
        metric_data: Vec<MetricDatum>,
    ) -> Result<(), CloudWatchMetricsError> {
        if metric_data.is_empty() {
            return Err(CloudWatchMetricsError::MissingParameter {
                message: "MetricData must contain at least one datum."
                    .to_owned(),
            });
        }

        for datum in metric_data {
            let metric_name =
                validate_metric_name("metric name", &datum.metric_name)?;
            let sample = sample_from_metric_datum(&datum, &*self.clock)?;
            let dimensions = canonical_dimensions(datum.dimensions)?;
            let key = metric_series_key(
                scope,
                &namespace,
                &metric_name,
                dimensions.clone(),
            );
            let mut stored = self
                .metric_store
                .get(&key)
                .unwrap_or(StoredMetricSeries { samples: Vec::new() });
            stored.samples.push(sample);
            self.metric_store.put(key, stored)?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation or pagination-token errors when the metric listing
    /// request is invalid.
    pub fn list_metrics(
        &self,
        scope: &CloudWatchScope,
        input: ListMetricsInput,
    ) -> Result<ListMetricsOutput, CloudWatchMetricsError> {
        reject_metric_token("ListMetrics", input.next_token.as_deref())?;
        let metric_name = input
            .metric_name
            .map(|value| validate_metric_name("metric name", &value))
            .transpose()?;
        let filters = canonical_dimension_filters(input.dimensions)?;
        let mut metrics = self
            .metric_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
            })
            .filter(|key| {
                input
                    .namespace
                    .as_ref()
                    .is_none_or(|value| &key.namespace == value)
            })
            .filter(|key| {
                metric_name
                    .as_deref()
                    .is_none_or(|value| key.metric_name == value)
            })
            .filter(|key| metric_matches_filters(&key.dimensions, &filters))
            .map(|key| MetricDescriptor {
                dimensions: key.dimensions,
                metric_name: key.metric_name,
                namespace: key.namespace,
            })
            .collect::<Vec<_>>();
        metrics.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.metric_name.cmp(&right.metric_name))
                .then_with(|| left.dimensions.cmp(&right.dimensions))
        });

        Ok(ListMetricsOutput { metrics, next_token: None })
    }

    /// # Errors
    ///
    /// Returns validation errors when the metric query is malformed.
    pub fn get_metric_statistics(
        &self,
        scope: &CloudWatchScope,
        input: GetMetricStatisticsInput,
    ) -> Result<GetMetricStatisticsOutput, CloudWatchMetricsError> {
        validate_time_range(
            input.start_time_millis,
            input.end_time_millis,
            "GetMetricStatistics",
        )?;
        let metric_name =
            validate_metric_name("metric name", &input.metric_name)?;
        let dimensions = canonical_dimensions(input.dimensions)?;
        let statistics = canonical_statistics(input.statistics)?;
        let period_seconds =
            validate_positive_i32("Period", input.period_seconds)?;
        let points = self.metric_store.get(&metric_series_key(
            scope,
            &input.namespace,
            &metric_name,
            dimensions,
        ));
        let datapoints = match points {
            Some(stored) => aggregate_statistics(
                &stored.samples,
                input.start_time_millis,
                input.end_time_millis,
                period_seconds,
                input.unit.as_deref(),
                &statistics,
            ),
            None => Vec::new(),
        };

        Ok(GetMetricStatisticsOutput { datapoints, label: metric_name })
    }

    /// # Errors
    ///
    /// Returns validation, limit, or persistence errors when the alarm
    /// definition is invalid or cannot be stored.
    pub fn put_metric_alarm(
        &self,
        scope: &CloudWatchScope,
        input: PutMetricAlarmInput,
    ) -> Result<(), CloudWatchMetricsError> {
        let alarm_name =
            validate_metric_name("alarm name", &input.alarm_name)?;
        let scoped_alarm_count = self
            .metric_alarm_store
            .keys()
            .into_iter()
            .filter(|key| {
                key.account_id == *scope.account_id()
                    && key.region == *scope.region()
            })
            .count();
        if scoped_alarm_count >= MAX_METRIC_ALARMS
            && self
                .metric_alarm_store
                .get(&alarm_key(scope, &alarm_name))
                .is_none()
        {
            return Err(CloudWatchMetricsError::LimitExceeded {
                message: "The maximum number of alarms has been reached."
                    .to_owned(),
            });
        }
        if !input.metrics.is_empty() || input.threshold_metric_id.is_some() {
            return Err(CloudWatchMetricsError::InvalidParameterValue {
                message: "Metric math alarm definitions are not supported."
                    .to_owned(),
            });
        }

        let namespace = input.namespace.ok_or_else(|| {
            CloudWatchMetricsError::MissingParameter {
                message: "Namespace is required.".to_owned(),
            }
        })?;
        let metric_name =
            validate_required_metric_field("MetricName", input.metric_name)?;
        let statistic =
            validate_required_metric_field("Statistic", input.statistic)?;
        let threshold = input.threshold.ok_or_else(|| {
            CloudWatchMetricsError::MissingParameter {
                message: "Threshold is required.".to_owned(),
            }
        })?;
        let period_seconds =
            validate_required_positive_i32("Period", input.period_seconds)?;
        let evaluation_periods = validate_positive_i32(
            "EvaluationPeriods",
            input.evaluation_periods,
        )?;
        let dimensions = canonical_dimensions(input.dimensions)?;
        let state_updated_timestamp_millis =
            current_epoch_millis(&*self.clock);
        let stored = StoredAlarm {
            actions_enabled: input.actions_enabled.unwrap_or(true),
            alarm_actions: input.alarm_actions,
            alarm_configuration_updated_timestamp_millis:
                state_updated_timestamp_millis,
            alarm_description: input.alarm_description,
            alarm_name: alarm_name.clone(),
            comparison_operator: input.comparison_operator,
            datapoints_to_alarm: input.datapoints_to_alarm,
            dimensions,
            evaluation_periods,
            insufficient_data_actions: input.insufficient_data_actions,
            metric_name,
            namespace,
            ok_actions: input.ok_actions,
            period_seconds,
            state_reason: "Unchecked: Initial alarm creation".to_owned(),
            state_reason_data: None,
            state_updated_timestamp_millis,
            state_value: AlarmStateValue::InsufficientData,
            statistic,
            threshold,
            treat_missing_data: input.treat_missing_data,
            unit: input.unit,
        };
        self.metric_alarm_store.put(alarm_key(scope, &alarm_name), stored)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation or pagination-token errors when the alarm page
    /// request is invalid.
    pub fn describe_alarms(
        &self,
        scope: &CloudWatchScope,
        input: DescribeAlarmsInput,
    ) -> Result<DescribeAlarmsOutput, CloudWatchMetricsError> {
        let prefix = input.alarm_name_prefix.unwrap_or_default();
        let names = input.alarm_names.into_iter().collect::<BTreeSet<_>>();
        let mut alarms = self
            .metric_alarm_store
            .scan(&alarm_scope_filter(scope))
            .into_iter()
            .filter(|alarm| {
                names.is_empty() || names.contains(&alarm.alarm_name)
            })
            .filter(|alarm| {
                prefix.is_empty() || alarm.alarm_name.starts_with(&prefix)
            })
            .filter(|alarm| {
                input
                    .state_value
                    .is_none_or(|state| alarm.state_value == state)
            })
            .map(|alarm| build_metric_alarm(scope, alarm))
            .collect::<Vec<_>>();
        alarms.sort_by(|left, right| left.alarm_name.cmp(&right.alarm_name));

        let start = input
            .next_token
            .as_deref()
            .map(parse_metric_token)
            .transpose()?
            .unwrap_or(0)
            .min(alarms.len());
        let end = input
            .max_records
            .map(|count| validate_positive_i32("MaxRecords", count))
            .transpose()?
                .map(|count| start.saturating_add(count as usize))
            .unwrap_or(alarms.len())
            .min(alarms.len());

        Ok(DescribeAlarmsOutput {
            metric_alarms: match alarms.get(start..end) {
                Some(page) => page.to_vec(),
                None => Vec::new(),
            },
            next_token: (end < alarms.len()).then(|| end.to_string()),
        })
    }

    /// # Errors
    ///
    /// Returns validation or persistence errors when an alarm name is invalid
    /// or an existing alarm cannot be removed.
    pub fn delete_alarms(
        &self,
        scope: &CloudWatchScope,
        input: DeleteAlarmsInput,
    ) -> Result<(), CloudWatchMetricsError> {
        for alarm_name in input.alarm_names {
            let name = validate_metric_name("alarm name", &alarm_name)?;
            let key = alarm_key(scope, &name);
            if self.metric_alarm_store.get(&key).is_some() {
                self.metric_alarm_store.delete(&key)?;
            }
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, not-found, or persistence errors when the alarm
    /// state cannot be updated.
    pub fn set_alarm_state(
        &self,
        scope: &CloudWatchScope,
        input: SetAlarmStateInput,
    ) -> Result<(), CloudWatchMetricsError> {
        let alarm_name =
            validate_metric_name("alarm name", &input.alarm_name)?;
        let key = alarm_key(scope, &alarm_name);
        let mut stored =
            self.metric_alarm_store.get(&key).ok_or_else(|| {
                CloudWatchMetricsError::ResourceNotFound {
                    message: format!("Alarm {alarm_name} does not exist."),
                }
            })?;
        if input.state_reason.trim().is_empty() {
            return Err(CloudWatchMetricsError::InvalidFormat {
                message: "StateReason must not be blank.".to_owned(),
            });
        }
        stored.state_reason = input.state_reason;
        stored.state_reason_data = input.state_reason_data;
        stored.state_updated_timestamp_millis =
            current_epoch_millis(&*self.clock);
        stored.state_value = input.state_value;
        self.metric_alarm_store.put(key, stored)?;
        Ok(())
    }

    /// # Errors
    ///
    /// Returns validation, pagination-token, or internal consistency errors
    /// when the metric data page cannot be produced.
    pub fn get_metric_data(
        &self,
        scope: &CloudWatchScope,
        input: GetMetricDataInput,
    ) -> Result<GetMetricDataOutput, CloudWatchMetricsError> {
        validate_time_range(
            input.start_time_millis,
            input.end_time_millis,
            "GetMetricData",
        )?;
        let page_size = input
            .max_datapoints
            .map(|value| validate_positive_i32("MaxDatapoints", value))
            .transpose()?
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_METRIC_PAGE_SIZE);
        let offset = input
            .next_token
            .as_deref()
            .map(parse_metric_token)
            .transpose()?
            .unwrap_or(0);
        let scan_by = input.scan_by.unwrap_or(ScanBy::TimestampDescending);
        let mut results = Vec::new();
        let mut max_len = 0usize;

        for query in input.metric_data_queries {
            if query.expression.is_some() {
                return Err(CloudWatchMetricsError::InvalidParameterValue {
                    message:
                        "Metric math expressions are not supported in GetMetricData."
                            .to_owned(),
                });
            }
            if !query.return_data.unwrap_or(true) {
                continue;
            }
            let metric_stat = query.metric_stat.ok_or_else(|| {
                CloudWatchMetricsError::InvalidParameterValue {
                    message:
                        "GetMetricData requires MetricStat queries for supported paths."
                            .to_owned(),
                }
            })?;
            let period_seconds =
                validate_positive_i32("Period", metric_stat.period_seconds)?;
            let metric_name = validate_metric_name(
                "metric name",
                &metric_stat.metric.metric_name,
            )?;
            let dimensions =
                canonical_dimensions(metric_stat.metric.dimensions)?;
            let statistics =
                canonical_statistics(vec![metric_stat.stat.clone()])?;
            let stored = self.metric_store.get(&metric_series_key(
                scope,
                &metric_stat.metric.namespace,
                &metric_name,
                dimensions,
            ));
            let mut datapoints = stored
                .map(|stored| {
                    aggregate_statistics(
                        &stored.samples,
                        input.start_time_millis,
                        input.end_time_millis,
                        period_seconds,
                        metric_stat.unit.as_deref(),
                        &statistics,
                    )
                })
                .unwrap_or_default();
            order_datapoints(&mut datapoints, scan_by);
            max_len = max_len.max(datapoints.len());

            let start = offset.min(datapoints.len());
            let end = start.saturating_add(page_size).min(datapoints.len());
            let label = query.label.unwrap_or(metric_name);
            let statistic = statistics.first().ok_or_else(|| {
                CloudWatchMetricsError::InternalServiceError {
                    message:
                        "CloudWatch metric statistic aggregation returned no \
                         statistics."
                            .to_owned(),
                }
            })?;
            let page = match datapoints.get(start..end) {
                Some(slice) => slice,
                None => &[],
            };
            results.push(MetricDataResult {
                id: query.id,
                label,
                status_code: "Complete".to_owned(),
                timestamps_millis: page
                    .iter()
                    .map(|datapoint| datapoint.timestamp_millis)
                    .collect(),
                values: page
                    .iter()
                    .map(|datapoint| metric_data_value(datapoint, statistic))
                    .collect(),
            });
        }

        if offset > max_len {
            return Err(CloudWatchMetricsError::InvalidNextToken {
                message: "The provided NextToken is invalid.".to_owned(),
            });
        }

        let next_offset = offset.saturating_add(page_size);
        let next_token =
            (next_offset < max_len).then(|| next_offset.to_string());

        Ok(GetMetricDataOutput { metric_data_results: results, next_token })
    }
}

pub(crate) fn current_epoch_millis(clock: &dyn Clock) -> u64 {
    clock
        .now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug)]
    struct FixedClock(SystemTime);

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    fn scope() -> CloudWatchScope {
        CloudWatchScope::new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn metric_namespace(value: &str) -> MetricNamespace {
        value.parse().expect("metric namespace should parse")
    }

    fn service(label: &str, clock: SystemTime) -> CloudWatchService {
        CloudWatchService::new(
            &StorageFactory::new(StorageConfig::new(
                format!("/tmp/{label}"),
                StorageMode::Memory,
            )),
            Arc::new(FixedClock(clock)),
        )
    }

    #[test]
    fn cloudwatch_logs_round_trip_retention_tags_and_duplicate_group_errors() {
        let service = service(
            "services-cloudwatch-logs",
            UNIX_EPOCH + Duration::from_secs(300),
        );
        let scope = scope();

        service
            .create_log_group(
                &scope,
                CreateLogGroupInput {
                    log_group_name: "app-logs".to_owned(),
                    tags: BTreeMap::from([(
                        "env".to_owned(),
                        "dev".to_owned(),
                    )]),
                },
            )
            .expect("log group should create");
        let duplicate = service
            .create_log_group(
                &scope,
                CreateLogGroupInput {
                    log_group_name: "app-logs".to_owned(),
                    tags: BTreeMap::new(),
                },
            )
            .expect_err("duplicate log group should fail");
        assert!(matches!(
            duplicate,
            CloudWatchLogsError::ResourceAlreadyExistsException { .. }
        ));

        service
            .put_retention_policy(
                &scope,
                PutRetentionPolicyInput {
                    log_group_name: "app-logs".to_owned(),
                    retention_in_days: 7,
                },
            )
            .expect("retention should update");
        service
            .tag_log_group(
                &scope,
                "app-logs",
                BTreeMap::from([("team".to_owned(), "api".to_owned())]),
            )
            .expect("tagging should succeed");

        let groups = service
            .describe_log_groups(
                &scope,
                DescribeLogGroupsInput {
                    log_group_name_prefix: Some("app".to_owned()),
                    next_token: None,
                },
            )
            .expect("describe log groups should succeed");
        assert_eq!(groups.log_groups.len(), 1);
        assert_eq!(groups.log_groups[0].retention_in_days, Some(7));
        assert_eq!(
            service
                .list_tags_log_group(&scope, "app-logs")
                .expect("tags should list"),
            BTreeMap::from([
                ("env".to_owned(), "dev".to_owned()),
                ("team".to_owned(), "api".to_owned()),
            ])
        );

        service
            .create_log_stream(
                &scope,
                CreateLogStreamInput {
                    log_group_name: "app-logs".to_owned(),
                    log_stream_name: "backend".to_owned(),
                },
            )
            .expect("log stream should create");
        service
            .put_log_events(
                &scope,
                PutLogEventsInput {
                    log_events: vec![
                        InputLogEvent {
                            message: "info startup".to_owned(),
                            timestamp_millis: 120_000,
                        },
                        InputLogEvent {
                            message: "error failed".to_owned(),
                            timestamp_millis: 121_000,
                        },
                    ],
                    log_group_name: "app-logs".to_owned(),
                    log_stream_name: "backend".to_owned(),
                },
            )
            .expect("log events should write");

        let fetched = service
            .get_log_events(
                &scope,
                GetLogEventsInput {
                    end_time_millis: None,
                    limit: None,
                    log_group_name: "app-logs".to_owned(),
                    log_stream_name: "backend".to_owned(),
                    next_token: None,
                    start_from_head: true,
                    start_time_millis: None,
                },
            )
            .expect("log events should load");
        assert_eq!(
            fetched
                .events
                .iter()
                .map(|event| event.message.as_str())
                .collect::<Vec<_>>(),
            vec!["info startup", "error failed"]
        );

        let filtered = service
            .filter_log_events(
                &scope,
                FilterLogEventsInput {
                    end_time_millis: None,
                    filter_pattern: Some("error".to_owned()),
                    limit: None,
                    log_group_name: "app-logs".to_owned(),
                    log_stream_name_prefix: None,
                    log_stream_names: vec!["backend".to_owned()],
                    next_token: None,
                    start_time_millis: None,
                },
            )
            .expect("filtered log events should load");
        assert_eq!(filtered.events.len(), 1);
        assert_eq!(filtered.events[0].message, "error failed");

        service
            .delete_log_group(&scope, "app-logs")
            .expect("log group should delete");
        let missing = service
            .describe_log_streams(
                &scope,
                DescribeLogStreamsInput {
                    log_group_name: "app-logs".to_owned(),
                    log_stream_name_prefix: None,
                    next_token: None,
                },
            )
            .expect_err("deleted log group should not resolve");
        assert!(matches!(
            missing,
            CloudWatchLogsError::ResourceNotFoundException { .. }
        ));
    }

    #[test]
    fn cloudwatch_metrics_statistics_list_and_get_metric_data_share_results() {
        let service = service(
            "services-cloudwatch-metrics",
            UNIX_EPOCH + Duration::from_secs(60),
        );
        let scope = scope();

        service
            .put_metric_data(
                &scope,
                metric_namespace("App/Metrics"),
                vec![MetricDatum {
                    counts: vec![2.0, 4.0],
                    dimensions: vec![
                        Dimension {
                            name: "Service".to_owned(),
                            value: "api".to_owned(),
                        },
                        Dimension {
                            name: "Stage".to_owned(),
                            value: "dev".to_owned(),
                        },
                    ],
                    metric_name: "RequestCount".to_owned(),
                    statistic_values: None,
                    storage_resolution: None,
                    timestamp_millis: Some(120_000),
                    unit: Some("Count".to_owned()),
                    value: None,
                    values: vec![1.0, 10.0],
                }],
            )
            .expect("metric data should store");

        let metrics = service
            .list_metrics(
                &scope,
                ListMetricsInput {
                    dimensions: vec![DimensionFilter {
                        name: "Service".to_owned(),
                        value: Some("api".to_owned()),
                    }],
                    metric_name: Some("RequestCount".to_owned()),
                    namespace: Some(metric_namespace("App/Metrics")),
                    next_token: None,
                },
            )
            .expect("metrics should list");
        assert_eq!(metrics.metrics.len(), 1);
        assert_eq!(metrics.metrics[0].dimensions[0].name, "Service");
        assert_eq!(metrics.metrics[0].dimensions[1].name, "Stage");

        let statistics = service
            .get_metric_statistics(
                &scope,
                GetMetricStatisticsInput {
                    dimensions: vec![
                        Dimension {
                            name: "Stage".to_owned(),
                            value: "dev".to_owned(),
                        },
                        Dimension {
                            name: "Service".to_owned(),
                            value: "api".to_owned(),
                        },
                    ],
                    end_time_millis: 180_000,
                    metric_name: "RequestCount".to_owned(),
                    namespace: metric_namespace("App/Metrics"),
                    period_seconds: 60,
                    start_time_millis: 0,
                    statistics: vec![
                        "SampleCount".to_owned(),
                        "Sum".to_owned(),
                        "Maximum".to_owned(),
                    ],
                    unit: Some("Count".to_owned()),
                },
            )
            .expect("statistics should compute");
        assert_eq!(statistics.datapoints.len(), 1);
        assert_eq!(statistics.datapoints[0].sample_count, Some(6.0));
        assert_eq!(statistics.datapoints[0].sum, Some(42.0));
        assert_eq!(statistics.datapoints[0].maximum, Some(10.0));

        let metric_data = service
            .get_metric_data(
                &scope,
                GetMetricDataInput {
                    end_time_millis: 180_000,
                    max_datapoints: None,
                    metric_data_queries: vec![MetricDataQuery {
                        expression: None,
                        id: "requests".to_owned(),
                        label: None,
                        metric_stat: Some(MetricStat {
                            metric: Metric {
                                dimensions: vec![
                                    Dimension {
                                        name: "Service".to_owned(),
                                        value: "api".to_owned(),
                                    },
                                    Dimension {
                                        name: "Stage".to_owned(),
                                        value: "dev".to_owned(),
                                    },
                                ],
                                metric_name: "RequestCount".to_owned(),
                                namespace: metric_namespace("App/Metrics"),
                            },
                            period_seconds: 60,
                            stat: "Sum".to_owned(),
                            unit: Some("Count".to_owned()),
                        }),
                        period_seconds: None,
                        return_data: None,
                    }],
                    next_token: None,
                    scan_by: Some(ScanBy::TimestampAscending),
                    start_time_millis: 0,
                },
            )
            .expect("metric data should compute");
        assert_eq!(metric_data.metric_data_results.len(), 1);
        assert_eq!(metric_data.metric_data_results[0].label, "RequestCount");
        assert_eq!(metric_data.metric_data_results[0].values, vec![42.0]);
        assert_eq!(
            metric_data.metric_data_results[0].timestamps_millis,
            vec![120_000]
        );
    }

    #[test]
    fn cloudwatch_metric_math_and_invalid_alarm_mutations_fail_explicitly() {
        let service = service(
            "services-cloudwatch-alarms",
            UNIX_EPOCH + Duration::from_secs(30),
        );
        let scope = scope();

        let unsupported_alarm = service
            .put_metric_alarm(
                &scope,
                PutMetricAlarmInput {
                    actions_enabled: None,
                    alarm_actions: Vec::new(),
                    alarm_description: None,
                    alarm_name: "api-alarm".to_owned(),
                    comparison_operator: "GreaterThanThreshold".to_owned(),
                    datapoints_to_alarm: None,
                    dimensions: Vec::new(),
                    evaluation_periods: 1,
                    insufficient_data_actions: Vec::new(),
                    metric_name: None,
                    metrics: vec![MetricDataQuery {
                        expression: Some("m1 + m2".to_owned()),
                        id: "expr".to_owned(),
                        label: None,
                        metric_stat: None,
                        period_seconds: None,
                        return_data: None,
                    }],
                    namespace: None,
                    ok_actions: Vec::new(),
                    period_seconds: None,
                    statistic: None,
                    threshold: None,
                    threshold_metric_id: None,
                    treat_missing_data: None,
                    unit: None,
                },
            )
            .expect_err("metric math alarms should fail");
        assert!(matches!(
            unsupported_alarm,
            CloudWatchMetricsError::InvalidParameterValue { .. }
        ));

        service
            .put_metric_alarm(
                &scope,
                PutMetricAlarmInput {
                    actions_enabled: Some(true),
                    alarm_actions: Vec::new(),
                    alarm_description: None,
                    alarm_name: "plain-alarm".to_owned(),
                    comparison_operator: "GreaterThanThreshold".to_owned(),
                    datapoints_to_alarm: None,
                    dimensions: Vec::new(),
                    evaluation_periods: 1,
                    insufficient_data_actions: Vec::new(),
                    metric_name: Some("RequestCount".to_owned()),
                    metrics: Vec::new(),
                    namespace: Some(metric_namespace("App/Metrics")),
                    ok_actions: Vec::new(),
                    period_seconds: Some(60),
                    statistic: Some("Sum".to_owned()),
                    threshold: Some(30.0),
                    threshold_metric_id: None,
                    treat_missing_data: None,
                    unit: Some("Count".to_owned()),
                },
            )
            .expect("plain alarm should store");

        let invalid_state = service
            .set_alarm_state(
                &scope,
                SetAlarmStateInput {
                    alarm_name: "plain-alarm".to_owned(),
                    state_reason: " ".to_owned(),
                    state_reason_data: None,
                    state_value: AlarmStateValue::Alarm,
                },
            )
            .expect_err("blank state reason should fail");
        assert!(matches!(
            invalid_state,
            CloudWatchMetricsError::InvalidFormat { .. }
        ));

        let unsupported_query = service
            .get_metric_data(
                &scope,
                GetMetricDataInput {
                    end_time_millis: 120_000,
                    max_datapoints: None,
                    metric_data_queries: vec![MetricDataQuery {
                        expression: Some("m1 + m2".to_owned()),
                        id: "expr".to_owned(),
                        label: None,
                        metric_stat: None,
                        period_seconds: None,
                        return_data: None,
                    }],
                    next_token: None,
                    scan_by: None,
                    start_time_millis: 0,
                },
            )
            .expect_err("metric math queries should fail");
        assert!(matches!(
            unsupported_query,
            CloudWatchMetricsError::InvalidParameterValue { .. }
        ));
    }
}
