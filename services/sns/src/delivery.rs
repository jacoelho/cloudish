use crate::SnsRegistry;
use crate::errors::SnsError;
use crate::subscriptions::{
    ConfirmationDelivery, ParsedHttpEndpoint, SubscriptionRecordState,
};
use crate::topics::TopicKey;
use aws::{AdvertisedEdge, Arn};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnsHttpRequest {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
}

impl SnsHttpRequest {
    fn new(body: Vec<u8>, headers: Vec<(String, String)>) -> Self {
        Self { body, headers }
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }
}

pub trait SnsHttpSigner: Send + Sync {
    fn sign(&self, string_to_sign: &str) -> String;

    fn signing_cert_url(&self) -> String;
}

struct SignedNotificationView<'a> {
    message_attributes: BTreeMap<String, Value>,
    payload: &'a NotificationPayload,
    signature: String,
    signature_version: &'static str,
    signing_cert_url: String,
    unsubscribe_url: String,
}

impl SignedNotificationView<'_> {
    fn notification_json(&self) -> Value {
        let mut body = serde_json::Map::from_iter([
            ("Type".to_owned(), json!("Notification")),
            ("MessageId".to_owned(), json!(&self.payload.message_id)),
            ("TopicArn".to_owned(), json!(&self.payload.topic_arn)),
            ("Message".to_owned(), json!(&self.payload.message)),
            ("Timestamp".to_owned(), json!(&self.payload.timestamp)),
            ("SignatureVersion".to_owned(), json!(self.signature_version)),
            ("Signature".to_owned(), json!(&self.signature)),
            ("SigningCertURL".to_owned(), json!(&self.signing_cert_url)),
            ("UnsubscribeURL".to_owned(), json!(&self.unsubscribe_url)),
            ("MessageAttributes".to_owned(), json!(&self.message_attributes)),
        ]);
        if let Some(subject) = self.payload.subject.as_deref() {
            body.insert("Subject".to_owned(), json!(subject));
        }

        Value::Object(body)
    }

    fn lambda_event_json(&self) -> Value {
        json!({
            "Records": [{
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": &self.payload.subscription_arn,
                "Sns": {
                    "Type": "Notification",
                    "MessageId": &self.payload.message_id,
                    "TopicArn": &self.payload.topic_arn,
                    "Subject": &self.payload.subject,
                    "Message": &self.payload.message,
                    "Timestamp": &self.payload.timestamp,
                    "SignatureVersion": self.signature_version,
                    "Signature": &self.signature,
                    "SigningCertUrl": &self.signing_cert_url,
                    "UnsubscribeUrl": &self.unsubscribe_url,
                    "MessageAttributes": &self.message_attributes,
                },
            }],
        })
    }
}

impl NotificationPayload {
    pub fn http_body(
        &self,
        raw_message_delivery: bool,
        advertised_edge: &AdvertisedEdge,
        signer: &dyn SnsHttpSigner,
    ) -> Vec<u8> {
        if raw_message_delivery {
            return self.message.as_bytes().to_vec();
        }

        self.signed_notification_view(advertised_edge, signer)
            .notification_json()
            .to_string()
            .into_bytes()
    }

    pub fn http_request(
        &self,
        raw_message_delivery: bool,
        advertised_edge: &AdvertisedEdge,
        signer: &dyn SnsHttpSigner,
    ) -> SnsHttpRequest {
        let headers = http_headers(
            "Notification",
            &self.message_id,
            &self.topic_arn,
            Some(&self.subscription_arn),
            raw_message_delivery,
        );
        let body = if raw_message_delivery {
            self.message.as_bytes().to_vec()
        } else {
            self.signed_notification_view(advertised_edge, signer)
                .notification_json()
                .to_string()
                .into_bytes()
        };

        SnsHttpRequest::new(body, headers)
    }

    pub fn lambda_event(
        &self,
        advertised_edge: &AdvertisedEdge,
        signer: &dyn SnsHttpSigner,
    ) -> Vec<u8> {
        self.signed_notification_view(advertised_edge, signer)
            .lambda_event_json()
            .to_string()
            .into_bytes()
    }

    pub fn sqs_body(
        &self,
        raw_message_delivery: bool,
        advertised_edge: &AdvertisedEdge,
        signer: &dyn SnsHttpSigner,
    ) -> String {
        if raw_message_delivery {
            self.message.clone()
        } else {
            self.signed_notification_view(advertised_edge, signer)
                .notification_json()
                .to_string()
        }
    }

    fn signed_notification_view(
        &self,
        advertised_edge: &AdvertisedEdge,
        signer: &dyn SnsHttpSigner,
    ) -> SignedNotificationView<'_> {
        SignedNotificationView {
            message_attributes: notification_message_attributes(
                &self.message_attributes,
            ),
            payload: self,
            signature: signer.sign(&notification_string_to_sign(self)),
            signature_version: "1",
            signing_cert_url: signer.signing_cert_url(),
            unsubscribe_url: advertised_edge
                .sns_unsubscribe_url(&self.subscription_arn),
        }
    }
}

impl ConfirmationDelivery {
    pub fn http_request(
        &self,
        message_id: &str,
        timestamp: &str,
        advertised_edge: &AdvertisedEdge,
        signer: &dyn SnsHttpSigner,
    ) -> SnsHttpRequest {
        let headers = http_headers(
            "SubscriptionConfirmation",
            message_id,
            &self.topic_arn,
            None,
            false,
        );
        let subscribe_url = advertised_edge
            .sns_confirm_subscription_url(&self.topic_arn, &self.token);
        let signature = signer.sign(&confirmation_string_to_sign(
            &confirmation_message(&self.topic_arn),
            message_id,
            &subscribe_url,
            timestamp,
            &self.token,
            &self.topic_arn,
        ));
        let body = json!({
            "Type": "SubscriptionConfirmation",
            "MessageId": message_id,
            "Token": self.token,
            "TopicArn": self.topic_arn,
            "Message": confirmation_message(&self.topic_arn),
            "SubscribeURL": subscribe_url,
            "Timestamp": timestamp,
            "SignatureVersion": "1",
            "Signature": signature,
            "SigningCertURL": signer.signing_cert_url(),
        })
        .to_string()
        .into_bytes();

        SnsHttpRequest::new(body, headers)
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

fn http_headers(
    message_type: &str,
    message_id: &str,
    topic_arn: &Arn,
    subscription_arn: Option<&Arn>,
    raw_message_delivery: bool,
) -> Vec<(String, String)> {
    let mut headers = vec![
        ("x-amz-sns-message-type".to_owned(), message_type.to_owned()),
        ("x-amz-sns-message-id".to_owned(), message_id.to_owned()),
        ("x-amz-sns-topic-arn".to_owned(), topic_arn.to_string()),
        ("Content-Type".to_owned(), "text/plain; charset=UTF-8".to_owned()),
    ];
    if raw_message_delivery {
        headers.push(("x-amz-sns-rawdelivery".to_owned(), "true".to_owned()));
    }
    if let Some(subscription_arn) = subscription_arn {
        headers.push((
            "x-amz-sns-subscription-arn".to_owned(),
            subscription_arn.to_string(),
        ));
    }

    headers
}

fn confirmation_message(topic_arn: &Arn) -> String {
    format!(
        "You have chosen to subscribe to the topic {topic_arn}.\nTo confirm the subscription, visit the SubscribeURL included in this message."
    )
}

fn notification_string_to_sign(payload: &NotificationPayload) -> String {
    let mut string_to_sign = format!(
        "Message\n{}\nMessageId\n{}\n",
        payload.message, payload.message_id
    );
    if let Some(subject) = &payload.subject {
        string_to_sign.push_str(&format!("Subject\n{subject}\n"));
    }
    string_to_sign.push_str(&format!(
        "Timestamp\n{}\nTopicArn\n{}\nType\nNotification\n",
        payload.timestamp, payload.topic_arn
    ));

    string_to_sign
}

fn confirmation_string_to_sign(
    message: &str,
    message_id: &str,
    subscribe_url: &str,
    timestamp: &str,
    token: &str,
    topic_arn: &Arn,
) -> String {
    format!(
        "Message\n{message}\nMessageId\n{message_id}\nSubscribeURL\n{subscribe_url}\nTimestamp\n{timestamp}\nToken\n{token}\nTopicArn\n{topic_arn}\nType\nSubscriptionConfirmation\n"
    )
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
        (Some(_), Some(_)) => Err(SnsError::InvalidParameter {
            message:
                "Invalid parameter: TopicArn and TargetArn cannot both be set."
                    .to_owned(),
        }),
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

#[cfg(test)]
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
