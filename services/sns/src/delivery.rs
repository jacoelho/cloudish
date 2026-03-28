use crate::SnsRegistry;
use crate::errors::SnsError;
use crate::subscriptions::{
    ConfirmationDelivery, ParsedHttpEndpoint, SubscriptionRecordState,
};
use crate::topics::TopicKey;
use aws::Arn;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAttributeValue {
    pub data_type: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishInput {
    pub message: String,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
    pub subject: Option<String>,
    pub target_arn: Option<Arn>,
    pub topic_arn: Option<Arn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOutput {
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchEntryInput {
    pub id: String,
    pub message: String,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
    pub subject: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchSuccess {
    pub id: String,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchFailure {
    pub code: String,
    pub id: String,
    pub message: String,
    pub sender_fault: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchOutput {
    pub failed: Vec<PublishBatchFailure>,
    pub successful: Vec<PublishBatchSuccess>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishPlan {
    pub(crate) deliveries: Vec<PlannedDelivery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedDelivery {
    pub endpoint: DeliveryEndpoint,
    pub raw_message_delivery: bool,
    pub payload: NotificationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryEndpoint {
    Http(ParsedHttpEndpoint),
    Lambda(Arn),
    Sqs(Arn),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationPayload {
    pub message: String,
    pub message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
    pub message_id: String,
    pub subject: Option<String>,
    pub subscription_arn: Arn,
    pub timestamp: String,
    pub topic_arn: Arn,
}

impl NotificationPayload {
    pub fn http_body(&self, raw_message_delivery: bool) -> Vec<u8> {
        if raw_message_delivery {
            return self.message.as_bytes().to_vec();
        }

        self.notification_json().to_string().into_bytes()
    }

    pub fn lambda_event(&self) -> Vec<u8> {
        json!({
            "Records": [{
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": self.subscription_arn,
                "Sns": {
                    "Type": "Notification",
                    "MessageId": self.message_id,
                    "TopicArn": self.topic_arn,
                    "Subject": self.subject,
                    "Message": self.message,
                    "Timestamp": self.timestamp,
                    "SignatureVersion": "1",
                    "Signature": "CLOUDISH",
                    "SigningCertUrl": "https://cloudish.invalid/sns.pem",
                    "UnsubscribeUrl": format!(
                        "http://localhost:4566/?Action=Unsubscribe&SubscriptionArn={}",
                        percent_encode(&self.subscription_arn.to_string())
                    ),
                    "MessageAttributes": notification_message_attributes(&self.message_attributes),
                },
            }],
        })
        .to_string()
        .into_bytes()
    }

    pub fn sqs_body(&self, raw_message_delivery: bool) -> String {
        if raw_message_delivery {
            self.message.clone()
        } else {
            self.notification_json().to_string()
        }
    }

    fn notification_json(&self) -> Value {
        json!({
            "Type": "Notification",
            "MessageId": self.message_id,
            "TopicArn": self.topic_arn,
            "Subject": self.subject,
            "Message": self.message,
            "Timestamp": self.timestamp,
            "SignatureVersion": "1",
            "Signature": "CLOUDISH",
            "SigningCertURL": "https://cloudish.invalid/sns.pem",
            "UnsubscribeURL": format!(
                "http://localhost:4566/?Action=Unsubscribe&SubscriptionArn={}",
                percent_encode(&self.subscription_arn.to_string())
            ),
            "MessageAttributes": notification_message_attributes(&self.message_attributes),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishRequest {
    pub(crate) message: String,
    pub(crate) message_attributes: BTreeMap<String, MessageAttributeValue>,
    pub(crate) message_deduplication_id: Option<String>,
    pub(crate) message_group_id: Option<String>,
    pub(crate) message_id: String,
    pub(crate) subject: Option<String>,
    pub(crate) timestamp: String,
    pub(crate) topic_arn: Arn,
}

pub trait SnsDeliveryTransport {
    fn deliver_confirmation(
        &self,
        delivery: &ConfirmationDelivery,
        confirmation_base_url: Option<&str>,
        message_id: String,
        timestamp: String,
    );

    fn deliver_notification(&self, delivery: &PlannedDelivery);
}

#[derive(Debug, Default)]
pub(crate) struct NoopSnsDeliveryTransport;

impl SnsDeliveryTransport for NoopSnsDeliveryTransport {
    fn deliver_confirmation(
        &self,
        _delivery: &ConfirmationDelivery,
        _confirmation_base_url: Option<&str>,
        _message_id: String,
        _timestamp: String,
    ) {
    }

    fn deliver_notification(&self, _delivery: &PlannedDelivery) {}
}

impl SnsRegistry {
    pub(crate) fn ensure_publish_topic(
        &self,
        topic_arn: &Arn,
    ) -> Result<(), SnsError> {
        let topic = TopicKey::from_publish_target(topic_arn)?;
        let _ = self.topic_record(&topic)?;

        Ok(())
    }

    pub(crate) fn publish(
        &self,
        input: PublishInput,
        message_id: &str,
        timestamp: &str,
    ) -> Result<PublishPlan, SnsError> {
        if input.message.is_empty() {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: Empty message".to_owned(),
            });
        }
        let topic_arn = publish_target_arn(&input)?;
        let request = PublishRequest {
            message: input.message,
            message_attributes: input.message_attributes,
            message_deduplication_id: input.message_deduplication_id,
            message_group_id: input.message_group_id,
            message_id: message_id.to_owned(),
            subject: input.subject,
            timestamp: timestamp.to_owned(),
            topic_arn,
        };

        self.build_publish_plan(request)
    }

    pub(crate) fn publish_batch_entry(
        &self,
        topic_arn: &Arn,
        entry: PublishBatchEntryInput,
        message_id: &str,
        timestamp: &str,
    ) -> Result<PublishPlan, SnsError> {
        let request = PublishRequest {
            message: entry.message,
            message_attributes: entry.message_attributes,
            message_deduplication_id: entry.message_deduplication_id,
            message_group_id: entry.message_group_id,
            message_id: message_id.to_owned(),
            subject: entry.subject,
            timestamp: timestamp.to_owned(),
            topic_arn: topic_arn.clone(),
        };

        self.build_publish_plan(request)
    }

    fn build_publish_plan(
        &self,
        request: PublishRequest,
    ) -> Result<PublishPlan, SnsError> {
        let topic = TopicKey::from_publish_target(&request.topic_arn)?;
        let topic_record = self.topic_record(&topic)?;

        if topic.is_fifo_topic(topic_record)
            && request.message_group_id.as_deref().is_none()
        {
            return Err(SnsError::InvalidParameter {
                message:
                    "The MessageGroupId parameter is required for FIFO topics"
                        .to_owned(),
            });
        }

        let deliveries = self
            .subscriptions
            .values()
            .filter(|subscription| subscription.topic == topic)
            .filter(|subscription| {
                matches!(
                    subscription.state,
                    SubscriptionRecordState::Confirmed
                )
            })
            .filter(|subscription| subscription.matches(&request))
            .map(|subscription| subscription.plan_delivery(&request))
            .collect();

        Ok(PublishPlan { deliveries })
    }
}

fn notification_message_attributes(
    attributes: &BTreeMap<String, MessageAttributeValue>,
) -> BTreeMap<String, Value> {
    attributes
        .iter()
        .map(|(name, attribute)| {
            (
                name.clone(),
                json!({
                    "Type": attribute.data_type,
                    "Value": attribute.value,
                }),
            )
        })
        .collect()
}

pub(crate) fn validate_publish_batch_entries(
    entries: &[PublishBatchEntryInput],
) -> Result<(), SnsError> {
    if entries.len() > 10 {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: PublishBatchRequestEntries"
                .to_owned(),
        });
    }

    let mut ids = BTreeSet::new();
    for entry in entries {
        if entry.id.is_empty()
            || entry.id.len() > 80
            || entry.id.chars().any(|character| {
                !character.is_ascii_alphanumeric()
                    && character != '-'
                    && character != '_'
            })
        {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: Id".to_owned(),
            });
        }
        if !ids.insert(entry.id.clone()) {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: Id".to_owned(),
            });
        }
    }

    Ok(())
}

fn publish_target_arn(input: &PublishInput) -> Result<Arn, SnsError> {
    match (&input.topic_arn, &input.target_arn) {
        (Some(topic_arn), None) => Ok(topic_arn.clone()),
        (None, Some(target_arn)) => Ok(target_arn.clone()),
        (Some(topic_arn), Some(_)) => Ok(topic_arn.clone()),
        (None, None) => Err(SnsError::InvalidParameter {
            message: "Invalid parameter: TopicArn".to_owned(),
        }),
    }
}

pub(crate) fn formatted_timestamp(time: SystemTime) -> String {
    let seconds =
        time.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let timestamp = OffsetDateTime::from_unix_timestamp(seconds as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);

    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

pub(crate) fn percent_encode(value: &str) -> String {
    let mut encoded = String::new();

    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
        {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }

    encoded
}
