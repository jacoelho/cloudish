use aws::{AccountId, Arn, ArnResource, Partition, RegionId, ServiceName};
use std::sync::Arc;

use crate::{
    bucket::BucketRecord, errors::S3Error, retention::malformed_xml_s3_error,
    scope::S3Scope, state::S3Service,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BucketNotificationConfiguration {
    #[serde(default)]
    pub queue_configurations: Vec<QueueNotificationConfiguration>,
    #[serde(default)]
    pub topic_configurations: Vec<TopicNotificationConfiguration>,
}

impl BucketNotificationConfiguration {
    pub(crate) fn is_empty(&self) -> bool {
        self.queue_configurations.is_empty()
            && self.topic_configurations.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationFilter {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNotificationConfiguration {
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub filter: Option<NotificationFilter>,
    #[serde(default)]
    pub id: Option<String>,
    pub queue_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicNotificationConfiguration {
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub filter: Option<NotificationFilter>,
    #[serde(default)]
    pub id: Option<String>,
    pub topic_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3EventNotification {
    pub bucket_arn: Arn,
    pub bucket_name: String,
    pub bucket_owner_account_id: AccountId,
    pub configuration_id: Option<String>,
    pub destination_arn: Arn,
    pub etag: Option<String>,
    pub event_name: String,
    pub event_time_epoch_seconds: u64,
    pub key: String,
    pub region: RegionId,
    pub requester_account_id: AccountId,
    pub sequencer: String,
    pub size: u64,
    pub version_id: Option<String>,
}

pub trait S3NotificationTransport: Send + Sync {
    fn publish(&self, notification: &S3EventNotification);

    /// # Errors
    ///
    /// Returns an [`S3Error`] when the destination ARN is not acceptable for
    /// the current bucket notification configuration.
    fn validate_destination(
        &self,
        _scope: &S3Scope,
        _bucket_name: &str,
        _bucket_owner_account_id: &AccountId,
        _bucket_region: &RegionId,
        _destination_arn: &Arn,
    ) -> Result<(), S3Error> {
        Ok(())
    }
}

#[derive(Debug)]
struct NoopS3NotificationTransport;

impl S3NotificationTransport for NoopS3NotificationTransport {
    fn publish(&self, _notification: &S3EventNotification) {}
}

pub(crate) fn default_notification_transport()
-> Arc<dyn S3NotificationTransport + Send + Sync> {
    Arc::new(NoopS3NotificationTransport)
}

struct ObjectNotificationDetails<'a> {
    event_name: &'a str,
    event_time_epoch_seconds: u64,
    etag: Option<&'a str>,
    key: &'a str,
    size: u64,
    version_id: Option<&'a str>,
}

impl S3Service {
    pub(crate) fn replace_notification_transport(
        &self,
        transport: Arc<dyn S3NotificationTransport + Send + Sync>,
    ) {
        *self
            .notification_transport_cell()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = transport;
    }

    pub(crate) fn notification_transport(
        &self,
    ) -> Arc<dyn S3NotificationTransport + Send + Sync> {
        self.notification_transport_cell()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn set_notification_transport(
        &self,
        transport: Arc<dyn S3NotificationTransport + Send + Sync>,
    ) {
        self.replace_notification_transport(transport);
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn put_bucket_notification_configuration(
        &self,
        scope: &S3Scope,
        bucket: &str,
        configuration: BucketNotificationConfiguration,
    ) -> Result<(), S3Error> {
        let _guard = self.lock_state();
        let mut bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        validate_notification_configuration(&configuration)?;
        self.validate_notification_destinations(
            scope,
            &bucket_record,
            &configuration,
        )?;
        bucket_record.set_notification_configuration(configuration);
        self.persist_bucket_record(bucket, bucket_record)
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn get_bucket_notification_configuration(
        &self,
        scope: &S3Scope,
        bucket: &str,
    ) -> Result<BucketNotificationConfiguration, S3Error> {
        let bucket_record = self.ensure_bucket_owned(scope, bucket)?;
        Ok(bucket_record.into_notification_configuration())
    }

    pub(crate) fn validate_notification_destinations(
        &self,
        scope: &S3Scope,
        bucket_record: &BucketRecord,
        configuration: &BucketNotificationConfiguration,
    ) -> Result<(), S3Error> {
        let transport = self.notification_transport();
        let (bucket_owner_account_id, bucket_region) =
            bucket_scope(bucket_record)?;

        for queue in &configuration.queue_configurations {
            transport.validate_destination(
                scope,
                &bucket_record.name,
                &bucket_owner_account_id,
                &bucket_region,
                &queue.queue_arn,
            )?;
        }
        for topic in &configuration.topic_configurations {
            transport.validate_destination(
                scope,
                &bucket_record.name,
                &bucket_owner_account_id,
                &bucket_region,
                &topic.topic_arn,
            )?;
        }

        Ok(())
    }

    pub(crate) fn emit_object_created_notification(
        &self,
        scope: &S3Scope,
        event_name: &str,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) {
        let Ok(bucket_record) = self.ensure_bucket_owned(scope, bucket) else {
            return;
        };
        if bucket_record.notification_configuration().is_empty() {
            return;
        }
        let object = version_id
            .and_then(|version_id| {
                self.resolve_nullable_version(bucket, key, version_id)
            })
            .or_else(|| self.current_object_record(bucket, key));
        let Some(object) = object else {
            return;
        };
        self.publish_matching_notifications(
            scope,
            &bucket_record,
            ObjectNotificationDetails {
                etag: (!object.etag().is_empty()).then_some(object.etag()),
                event_name,
                event_time_epoch_seconds: object.last_modified_epoch_seconds(),
                key,
                size: object.size(),
                version_id: object.version_id().as_deref(),
            },
        );
    }

    pub(crate) fn emit_object_removed_notification(
        &self,
        scope: &S3Scope,
        bucket_record: &BucketRecord,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        delete_marker: bool,
    ) {
        if bucket_record.notification_configuration().is_empty() {
            return;
        }
        let sequencer = version_id
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{:016X}", key.len()));
        let Ok((bucket_owner_account_id, bucket_region)) =
            bucket_scope(bucket_record)
        else {
            return;
        };
        let transport = self.notification_transport();

        for queue in
            &bucket_record.notification_configuration().queue_configurations
        {
            if !notification_matches(
                &queue.events,
                "ObjectRemoved:Delete",
                key,
                queue.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn(bucket),
                bucket_name: bucket.to_owned(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: queue.id.clone(),
                destination_arn: queue.queue_arn.clone(),
                etag: None,
                event_name: "ObjectRemoved:Delete".to_owned(),
                event_time_epoch_seconds: bucket_record
                    .created_at_epoch_seconds(),
                key: key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: 0,
                version_id: version_id
                    .map(str::to_owned)
                    .filter(|_| !delete_marker),
            });
        }

        for topic in
            &bucket_record.notification_configuration().topic_configurations
        {
            if !notification_matches(
                &topic.events,
                "ObjectRemoved:Delete",
                key,
                topic.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn(bucket),
                bucket_name: bucket.to_owned(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: topic.id.clone(),
                destination_arn: topic.topic_arn.clone(),
                etag: None,
                event_name: "ObjectRemoved:Delete".to_owned(),
                event_time_epoch_seconds: bucket_record
                    .created_at_epoch_seconds(),
                key: key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: 0,
                version_id: version_id
                    .map(str::to_owned)
                    .filter(|_| !delete_marker),
            });
        }
    }

    fn publish_matching_notifications(
        &self,
        scope: &S3Scope,
        bucket_record: &BucketRecord,
        details: ObjectNotificationDetails<'_>,
    ) {
        let transport = self.notification_transport();
        let bucket_arn = bucket_arn(&bucket_record.name);
        let Ok((bucket_owner_account_id, bucket_region)) =
            bucket_scope(bucket_record)
        else {
            return;
        };
        let sequencer = notification_sequencer(
            details.version_id,
            details.event_time_epoch_seconds,
            details.key,
        );

        for queue in
            &bucket_record.notification_configuration().queue_configurations
        {
            if !notification_matches(
                &queue.events,
                details.event_name,
                details.key,
                queue.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn.clone(),
                bucket_name: bucket_record.name.clone(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: queue.id.clone(),
                destination_arn: queue.queue_arn.clone(),
                etag: details.etag.map(str::to_owned),
                event_name: details.event_name.to_owned(),
                event_time_epoch_seconds: details.event_time_epoch_seconds,
                key: details.key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: details.size,
                version_id: details.version_id.map(str::to_owned),
            });
        }

        for topic in
            &bucket_record.notification_configuration().topic_configurations
        {
            if !notification_matches(
                &topic.events,
                details.event_name,
                details.key,
                topic.filter.as_ref(),
            ) {
                continue;
            }
            transport.publish(&S3EventNotification {
                bucket_arn: bucket_arn.clone(),
                bucket_name: bucket_record.name.clone(),
                bucket_owner_account_id: bucket_owner_account_id.clone(),
                configuration_id: topic.id.clone(),
                destination_arn: topic.topic_arn.clone(),
                etag: details.etag.map(str::to_owned),
                event_name: details.event_name.to_owned(),
                event_time_epoch_seconds: details.event_time_epoch_seconds,
                key: details.key.to_owned(),
                region: bucket_region.clone(),
                requester_account_id: scope.account_id().clone(),
                sequencer: sequencer.clone(),
                size: details.size,
                version_id: details.version_id.map(str::to_owned),
            });
        }
    }
}

fn bucket_arn(bucket: &str) -> Arn {
    Arn::trusted_new(
        Partition::aws(),
        ServiceName::S3,
        None,
        None,
        ArnResource::Generic(bucket.to_owned()),
    )
}

fn bucket_scope(
    bucket_record: &BucketRecord,
) -> Result<(AccountId, RegionId), S3Error> {
    let owner_account_id =
        bucket_record.owner_account_id.parse().map_err(|error| {
            S3Error::Internal {
                message: format!(
                    "bucket {} stored an invalid owner account id {}: {error}",
                    bucket_record.name, bucket_record.owner_account_id
                ),
            }
        })?;
    let region =
        bucket_record.region.parse().map_err(|error| S3Error::Internal {
            message: format!(
                "bucket {} stored an invalid region {}: {error}",
                bucket_record.name, bucket_record.region
            ),
        })?;

    Ok((owner_account_id, region))
}

fn notification_matches(
    configured_events: &[String],
    event_name: &str,
    key: &str,
    filter: Option<&NotificationFilter>,
) -> bool {
    configured_events
        .iter()
        .any(|configured| notification_event_matches(configured, event_name))
        && notification_filter_matches(filter, key)
}

fn notification_event_matches(configured: &str, event_name: &str) -> bool {
    let configured = configured.strip_prefix("s3:").unwrap_or(configured);
    if let Some(prefix) = configured.strip_suffix('*') {
        event_name.starts_with(prefix)
    } else {
        configured == event_name
    }
}

fn notification_filter_matches(
    filter: Option<&NotificationFilter>,
    key: &str,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter.prefix.as_deref().is_some_and(|prefix| !key.starts_with(prefix))
    {
        return false;
    }
    if filter.suffix.as_deref().is_some_and(|suffix| !key.ends_with(suffix)) {
        return false;
    }
    true
}

fn notification_sequencer(
    version_id: Option<&str>,
    event_time_epoch_seconds: u64,
    key: &str,
) -> String {
    let seed = format!(
        "{}:{event_time_epoch_seconds}:{key}",
        version_id.unwrap_or("null")
    );
    format!("{:x}", md5::compute(seed)).to_uppercase()
}

fn validate_notification_configuration(
    configuration: &BucketNotificationConfiguration,
) -> Result<(), S3Error> {
    for queue in &configuration.queue_configurations {
        if queue.events.is_empty() {
            return Err(malformed_xml_s3_error());
        }
        validate_notification_events(&queue.events)?;
        validate_notification_filter(queue.filter.as_ref())?;
    }
    for topic in &configuration.topic_configurations {
        if topic.events.is_empty() {
            return Err(malformed_xml_s3_error());
        }
        validate_notification_events(&topic.events)?;
        validate_notification_filter(topic.filter.as_ref())?;
    }

    Ok(())
}

fn validate_notification_events(events: &[String]) -> Result<(), S3Error> {
    for event in events {
        if !event.starts_with("s3:") {
            return Err(S3Error::InvalidArgument {
                code: "InvalidArgument",
                message: format!("Unsupported notification event `{event}`."),
                status_code: 400,
            });
        }
    }
    Ok(())
}

fn validate_notification_filter(
    filter: Option<&NotificationFilter>,
) -> Result<(), S3Error> {
    let Some(filter) = filter else {
        return Ok(());
    };
    if filter.prefix.as_deref().is_some_and(|prefix| prefix.is_empty()) {
        return Err(malformed_xml_s3_error());
    }
    if filter.suffix.as_deref().is_some_and(|suffix| suffix.is_empty()) {
        return Err(malformed_xml_s3_error());
    }
    Ok(())
}
