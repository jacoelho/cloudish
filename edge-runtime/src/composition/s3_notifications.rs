use aws::{Arn, ServiceName};
use s3::{S3Error, S3EventNotification, S3NotificationTransport, S3Scope};
use sns::{PublishInput, SnsService};
use sqs::{SendMessageInput, SqsService};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Default)]
pub struct S3NotificationDispatcher {
    pub sns: Option<SnsService>,
    pub sqs: Option<SqsService>,
}

impl S3NotificationTransport for S3NotificationDispatcher {
    fn publish(&self, notification: &S3EventNotification) {
        let body = s3_notification_message_body(notification);
        match notification.destination_arn.service() {
            ServiceName::Sns => {
                let Some(sns) = self.sns.as_ref() else {
                    return;
                };
                let _ = sns.publish(PublishInput {
                    message: body,
                    message_attributes: BTreeMap::new(),
                    message_deduplication_id: None,
                    message_group_id: None,
                    subject: None,
                    target_arn: None,
                    topic_arn: Some(notification.destination_arn.clone()),
                });
            }
            ServiceName::Sqs => {
                let Some(sqs) = self.sqs.as_ref() else {
                    return;
                };
                let Ok(queue) = sqs::SqsQueueIdentity::from_arn(
                    &notification.destination_arn,
                ) else {
                    return;
                };
                let _ = sqs.send_message(
                    &queue,
                    SendMessageInput {
                        body,
                        delay_seconds: None,
                        message_deduplication_id: None,
                        message_group_id: None,
                    },
                );
            }
            _ => {}
        }
    }

    fn validate_destination(
        &self,
        _scope: &S3Scope,
        _bucket_name: &str,
        _bucket_owner_account_id: &aws::AccountId,
        _bucket_region: &aws::RegionId,
        destination_arn: &Arn,
    ) -> Result<(), S3Error> {
        match destination_arn.service() {
            ServiceName::Sns => {
                let sns = self
                    .sns
                    .as_ref()
                    .ok_or_else(|| invalid_destination(destination_arn))?;
                sns.get_topic_attributes(destination_arn)
                    .map(|_| ())
                    .map_err(|_| invalid_destination(destination_arn))
            }
            ServiceName::Sqs => {
                let sqs = self
                    .sqs
                    .as_ref()
                    .ok_or_else(|| invalid_destination(destination_arn))?;
                let queue =
                    sqs::SqsQueueIdentity::from_arn(destination_arn)
                        .map_err(|_| invalid_destination(destination_arn))?;
                sqs.get_queue_attributes(&queue, &Vec::new())
                    .map(|_| ())
                    .map_err(|_| invalid_destination(destination_arn))
            }
            _ => Err(invalid_destination(destination_arn)),
        }
    }
}

fn invalid_destination(destination_arn: &Arn) -> S3Error {
    S3Error::InvalidArgument {
        code: "InvalidArgument",
        message: format!("Unable to validate destination {destination_arn}."),
        status_code: 400,
    }
}

fn s3_notification_message_body(notification: &S3EventNotification) -> String {
    serde_json::json!({
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": notification.region,
            "eventTime": format_epoch_rfc3339(notification.event_time_epoch_seconds),
            "eventName": notification.event_name,
            "userIdentity": {
                "principalId": notification.requester_account_id,
            },
            "requestParameters": {
                "sourceIPAddress": "127.0.0.1",
            },
            "responseElements": {
                "x-amz-request-id": "0000000000000000",
                "x-amz-id-2": "0000000000000000",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": notification.configuration_id,
                "bucket": {
                    "name": notification.bucket_name,
                    "ownerIdentity": {
                        "principalId": notification.bucket_owner_account_id,
                    },
                    "arn": notification.bucket_arn,
                },
                "object": {
                    "key": urlencoding::encode(&notification.key).into_owned(),
                    "size": notification.size,
                    "eTag": notification.etag,
                    "versionId": notification.version_id,
                    "sequencer": notification.sequencer,
                },
            },
        }],
    })
    .to_string()
}

fn format_epoch_rfc3339(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}
