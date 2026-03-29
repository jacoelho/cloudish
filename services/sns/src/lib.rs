mod delivery;
mod errors;
mod scope;
mod subscriptions;
mod topics;

pub use delivery::{
    DeliveryEndpoint, MessageAttributeValue, NotificationPayload,
    PlannedDelivery, PublishBatchEntryInput, PublishBatchFailure,
    PublishBatchOutput, PublishBatchSuccess, PublishInput, PublishOutput,
    SnsDeliveryTransport,
};
pub use errors::SnsError;
pub use scope::SnsScope;
pub use subscriptions::{
    ConfirmationDelivery, ListedSubscription, ParsedHttpEndpoint,
    SubscribeInput, SubscribeOutput, SubscriptionProtocol, SubscriptionState,
};
pub use topics::CreateTopicInput;

use aws::Arn;
#[cfg(test)]
use delivery::percent_encode;
use delivery::{
    NoopSnsDeliveryTransport, formatted_timestamp,
    validate_publish_batch_entries,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
#[cfg(test)]
use subscriptions::{SUBSCRIBE_PENDING_CONFIRMATION_ARN, parse_http_endpoint};
use subscriptions::{SubscribedRecord, SubscriptionRecord};
use topics::{TopicKey, TopicRecord};
pub trait SnsIdentifierSource: Send + Sync {
    fn next_confirmation_token(&self) -> String;
    fn next_message_id(&self) -> String;
    fn next_subscription_id(&self) -> String;
}

#[derive(Debug, Default)]
pub struct SequentialSnsIdentifierSource {
    next_confirmation_token: AtomicU64,
    next_message_id: AtomicU64,
    next_subscription_id: AtomicU64,
}

impl SnsIdentifierSource for SequentialSnsIdentifierSource {
    fn next_confirmation_token(&self) -> String {
        let id =
            self.next_confirmation_token.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{id:032x}")
    }

    fn next_message_id(&self) -> String {
        let id = self.next_message_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("00000000-0000-0000-0000-{id:012}")
    }

    fn next_subscription_id(&self) -> String {
        let id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("00000000-0000-0000-0000-{id:012}")
    }
}

#[derive(Clone)]
pub struct SnsService {
    delivery_transport: Arc<dyn SnsDeliveryTransport + Send + Sync>,
    identifier_source: Arc<dyn SnsIdentifierSource + Send + Sync>,
    state: Arc<Mutex<SnsRegistry>>,
    time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl Default for SnsService {
    fn default() -> Self {
        Self::new()
    }
}

impl SnsService {
    pub fn new() -> Self {
        Self::with_transport(
            Arc::new(SystemTime::now),
            Arc::new(SequentialSnsIdentifierSource::default()),
            Arc::new(NoopSnsDeliveryTransport),
        )
    }

    pub fn with_transport(
        time_source: Arc<dyn Fn() -> SystemTime + Send + Sync>,
        identifier_source: Arc<dyn SnsIdentifierSource + Send + Sync>,
        delivery_transport: Arc<dyn SnsDeliveryTransport + Send + Sync>,
    ) -> Self {
        Self {
            delivery_transport,
            identifier_source,
            state: Arc::default(),
            time_source,
        }
    }

    /// Creates or idempotently returns an SNS topic in the requested scope.
    ///
    /// # Errors
    ///
    /// Returns validation errors when the topic name or attributes are invalid
    /// for SNS.
    pub fn create_topic(
        &self,
        scope: &SnsScope,
        input: CreateTopicInput,
    ) -> Result<Arn, SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.create_topic(scope, input)
    }

    /// Deletes an existing SNS topic and its subscription records.
    ///
    /// # Errors
    ///
    /// Returns an SNS not-found or validation error when the ARN does not
    /// identify a managed topic.
    pub fn delete_topic(&self, topic_arn: &Arn) -> Result<(), SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.delete_topic(topic_arn)
    }

    pub fn list_topics(&self, scope: &SnsScope) -> Vec<Arn> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_topics(scope)
    }

    /// Returns the stored attribute map for a topic.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic ARN is
    /// invalid or unknown.
    pub fn get_topic_attributes(
        &self,
        topic_arn: &Arn,
    ) -> Result<BTreeMap<String, String>, SnsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.get_topic_attributes(topic_arn)
    }

    /// Updates a mutable SNS topic attribute.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic or
    /// attribute name is invalid.
    pub fn set_topic_attributes(
        &self,
        topic_arn: &Arn,
        attribute_name: &str,
        attribute_value: Option<&str>,
    ) -> Result<(), SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.set_topic_attributes(topic_arn, attribute_name, attribute_value)
    }

    /// Creates or reuses a subscription and schedules any required
    /// confirmation delivery.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic, protocol,
    /// endpoint, or requested attributes are invalid.
    pub fn subscribe(
        &self,
        input: SubscribeInput,
    ) -> Result<SubscribeOutput, SnsError> {
        let subscription_id = self.identifier_source.next_subscription_id();
        let confirmation_token =
            self.identifier_source.next_confirmation_token();
        let SubscribedRecord {
            confirmation_delivery,
            response_subscription_arn,
            state,
            subscription_arn,
        } = {
            let mut state =
                self.state.lock().unwrap_or_else(|poison| poison.into_inner());

            state.subscribe_record(
                input,
                &subscription_id,
                &confirmation_token,
            )?
        };

        if let Some(delivery) = confirmation_delivery.as_ref() {
            self.deliver_confirmation(delivery);
        }

        Ok(SubscribeOutput {
            response_subscription_arn,
            state,
            subscription_arn,
        })
    }

    /// Deletes an SNS subscription.
    ///
    /// # Errors
    ///
    /// Returns an SNS not-found or validation error when the ARN does not
    /// identify a managed subscription.
    pub fn unsubscribe(&self, subscription_arn: &Arn) -> Result<(), SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.unsubscribe(subscription_arn)
    }

    /// Confirms a pending subscription using the delivery token.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic or token is
    /// invalid.
    pub fn confirm_subscription(
        &self,
        topic_arn: &Arn,
        token: &str,
    ) -> Result<Arn, SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.confirm_subscription(topic_arn, token)
    }

    pub fn list_subscriptions(
        &self,
        scope: &SnsScope,
    ) -> Vec<ListedSubscription> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_subscriptions(scope)
    }

    /// Lists every subscription currently attached to a topic.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic ARN is
    /// invalid or unknown.
    pub fn list_subscriptions_by_topic(
        &self,
        topic_arn: &Arn,
    ) -> Result<Vec<ListedSubscription>, SnsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_subscriptions_by_topic(topic_arn)
    }

    /// Returns the stored attribute map for a subscription.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the subscription ARN
    /// is invalid or unknown.
    pub fn get_subscription_attributes(
        &self,
        subscription_arn: &Arn,
    ) -> Result<BTreeMap<String, String>, SnsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.get_subscription_attributes(subscription_arn)
    }

    /// Updates a mutable subscription attribute.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the subscription,
    /// attribute name, or attribute value is invalid.
    pub fn set_subscription_attributes(
        &self,
        subscription_arn: &Arn,
        attribute_name: &str,
        attribute_value: &str,
    ) -> Result<(), SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.set_subscription_attributes(
            subscription_arn,
            attribute_name,
            attribute_value,
        )
    }

    /// Publishes one SNS message and dispatches the planned deliveries.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the publish target or
    /// message payload is invalid.
    pub fn publish(
        &self,
        input: PublishInput,
    ) -> Result<PublishOutput, SnsError> {
        let message_id = self.identifier_source.next_message_id();
        let timestamp = formatted_timestamp((self.time_source)());
        let planned = {
            let state =
                self.state.lock().unwrap_or_else(|poison| poison.into_inner());

            state.publish(input, &message_id, &timestamp)?
        };
        self.deliver_notifications(&planned.deliveries);

        Ok(PublishOutput { message_id })
    }

    /// Publishes multiple SNS messages in a single batch.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic ARN or
    /// batch envelope is invalid.
    pub fn publish_batch(
        &self,
        topic_arn: &Arn,
        entries: Vec<PublishBatchEntryInput>,
    ) -> Result<PublishBatchOutput, SnsError> {
        validate_publish_batch_entries(&entries)?;
        let timestamp = formatted_timestamp((self.time_source)());
        let mut deliveries = Vec::new();
        let mut failed = Vec::new();
        let mut successful = Vec::new();

        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());
        state.ensure_publish_topic(topic_arn)?;

        for entry in entries {
            if entry.message.is_empty() {
                failed.push(PublishBatchFailure {
                    code: "InvalidParameter".to_owned(),
                    id: entry.id,
                    message: "Invalid parameter: Message".to_owned(),
                    sender_fault: true,
                });
                continue;
            }

            let message_id = self.identifier_source.next_message_id();
            let planned = state.publish_batch_entry(
                topic_arn,
                entry.clone(),
                &message_id,
                &timestamp,
            )?;
            deliveries.extend(planned.deliveries);
            successful.push(PublishBatchSuccess { id: entry.id, message_id });
        }
        drop(state);

        self.deliver_notifications(&deliveries);

        Ok(PublishBatchOutput { failed, successful })
    }

    /// Adds or replaces tags on an SNS topic.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic ARN or tag
    /// set is invalid.
    pub fn tag_resource(
        &self,
        topic_arn: &Arn,
        tags: BTreeMap<String, String>,
    ) -> Result<(), SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.tag_resource(topic_arn, tags)
    }

    /// Removes tags from an SNS topic.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic ARN is
    /// invalid or unknown.
    pub fn untag_resource(
        &self,
        topic_arn: &Arn,
        tag_keys: &[String],
    ) -> Result<(), SnsError> {
        let mut state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.untag_resource(topic_arn, tag_keys)
    }

    /// Lists all tags stored on an SNS topic.
    ///
    /// # Errors
    ///
    /// Returns an SNS validation or not-found error when the topic ARN is
    /// invalid or unknown.
    pub fn list_tags_for_resource(
        &self,
        topic_arn: &Arn,
    ) -> Result<BTreeMap<String, String>, SnsError> {
        let state =
            self.state.lock().unwrap_or_else(|poison| poison.into_inner());

        state.list_tags_for_resource(topic_arn)
    }

    fn deliver_confirmation(&self, delivery: &ConfirmationDelivery) {
        self.delivery_transport.deliver_confirmation(
            delivery,
            self.identifier_source.next_message_id(),
            formatted_timestamp((self.time_source)()),
        );
    }

    fn deliver_notifications(&self, deliveries: &[PlannedDelivery]) {
        for delivery in deliveries {
            self.delivery_transport.deliver_notification(delivery);
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SnsRegistry {
    pub(crate) subscriptions: BTreeMap<Arn, SubscriptionRecord>,
    pub(crate) tokens: BTreeMap<String, Arn>,
    pub(crate) topics: BTreeMap<TopicKey, TopicRecord>,
}

#[cfg(test)]
mod tests {
    use super::{
        ConfirmationDelivery, CreateTopicInput, DeliveryEndpoint,
        ListedSubscription, MessageAttributeValue, NotificationPayload,
        PlannedDelivery, PublishBatchEntryInput, PublishInput,
        SUBSCRIBE_PENDING_CONFIRMATION_ARN, SequentialSnsIdentifierSource,
        SnsDeliveryTransport, SnsError, SnsScope, SnsService,
        SubscriptionState, formatted_timestamp, parse_http_endpoint,
        percent_encode,
    };
    use aws::{AccountId, Arn, ServiceName};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct RecordingTransport {
        confirmations: Mutex<Vec<ConfirmationDelivery>>,
        deliveries: Mutex<Vec<PlannedDelivery>>,
    }

    impl RecordingTransport {
        fn confirmations(&self) -> Vec<ConfirmationDelivery> {
            self.confirmations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        }

        fn deliveries(&self) -> Vec<PlannedDelivery> {
            self.deliveries
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        }
    }

    impl SnsDeliveryTransport for RecordingTransport {
        fn deliver_confirmation(
            &self,
            delivery: &ConfirmationDelivery,
            _message_id: String,
            _timestamp: String,
        ) {
            self.confirmations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(delivery.clone());
        }

        fn deliver_notification(&self, delivery: &PlannedDelivery) {
            self.deliveries
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(delivery.clone());
        }
    }

    fn scope() -> SnsScope {
        SnsScope::new(
            "000000000000".parse().expect("account should parse"),
            "eu-west-2".parse().expect("region should parse"),
        )
    }

    fn service_with_transport(
        now_epoch_seconds: u64,
    ) -> (SnsService, Arc<RecordingTransport>) {
        let transport = Arc::new(RecordingTransport::default());
        let service = SnsService::with_transport(
            Arc::new(move || {
                UNIX_EPOCH + Duration::from_secs(now_epoch_seconds)
            }),
            Arc::new(SequentialSnsIdentifierSource::default()),
            transport.clone(),
        );
        (service, transport)
    }

    fn create_topic(service: &SnsService, name: &str) -> Arn {
        service
            .create_topic(
                &scope(),
                CreateTopicInput {
                    attributes: BTreeMap::new(),
                    name: name.to_owned(),
                },
            )
            .expect("topic should be created")
    }

    fn listed_subscription_arns(
        subscriptions: &[ListedSubscription],
    ) -> Vec<String> {
        subscriptions
            .iter()
            .map(|subscription| {
                subscription
                    .state
                    .listed_subscription_arn(&subscription.subscription_arn)
                    .clone()
            })
            .collect()
    }

    fn queue_arn(queue_name: &str) -> Arn {
        format!("arn:aws:sqs:eu-west-2:000000000000:{queue_name}")
            .parse()
            .expect("queue ARN should parse")
    }

    fn lambda_arn(function_name: &str) -> Arn {
        format!(
            "arn:aws:lambda:eu-west-2:000000000000:function:{function_name}"
        )
        .parse()
        .expect("lambda ARN should parse")
    }

    #[test]
    fn sns_core_create_topic_is_idempotent_for_matching_attributes() {
        let service = SnsService::new();
        let first = service
            .create_topic(
                &scope(),
                CreateTopicInput {
                    attributes: BTreeMap::from([(
                        "DisplayName".to_owned(),
                        "Orders".to_owned(),
                    )]),
                    name: "orders".to_owned(),
                },
            )
            .expect("topic should be created");
        let second = service
            .create_topic(
                &scope(),
                CreateTopicInput {
                    attributes: BTreeMap::from([(
                        "DisplayName".to_owned(),
                        "Orders".to_owned(),
                    )]),
                    name: "orders".to_owned(),
                },
            )
            .expect("topic should reuse the existing ARN");

        assert_eq!(first, second);
        assert_eq!(service.list_topics(&scope()), vec![first]);
    }

    #[test]
    fn sns_core_create_topic_rejects_attribute_mismatch_on_existing_name() {
        let service = SnsService::new();
        let _ = create_topic(&service, "orders");
        let error = service
            .create_topic(
                &scope(),
                CreateTopicInput {
                    attributes: BTreeMap::from([(
                        "DisplayName".to_owned(),
                        "Different".to_owned(),
                    )]),
                    name: "orders".to_owned(),
                },
            )
            .expect_err("attribute mismatch should fail");

        assert_eq!(error.code(), "InvalidParameter");
        assert!(error.message().contains("different attributes"));
    }

    #[test]
    fn sns_core_topic_attributes_and_tags_round_trip() {
        let service = SnsService::new();
        let topic_arn = create_topic(&service, "orders");

        service
            .set_topic_attributes(&topic_arn, "DisplayName", Some("Orders"))
            .expect("attribute should be stored");
        service
            .tag_resource(
                &topic_arn,
                BTreeMap::from([
                    ("env".to_owned(), "dev".to_owned()),
                    ("team".to_owned(), "platform".to_owned()),
                ]),
            )
            .expect("tags should be stored");

        let attributes = service
            .get_topic_attributes(&topic_arn)
            .expect("attributes should load");
        let tags = service
            .list_tags_for_resource(&topic_arn)
            .expect("tags should load");
        let topic_arn_text = topic_arn.to_string();

        assert_eq!(
            attributes.get("DisplayName").map(String::as_str),
            Some("Orders")
        );
        assert_eq!(
            attributes.get("TopicArn").map(String::as_str),
            Some(topic_arn_text.as_str())
        );
        assert_eq!(
            attributes.get("Owner").map(String::as_str),
            Some("000000000000")
        );
        assert_eq!(
            attributes.get("SubscriptionsConfirmed").map(String::as_str),
            Some("0")
        );
        assert_eq!(tags.get("env").map(String::as_str), Some("dev"));
        assert_eq!(tags.get("team").map(String::as_str), Some("platform"));

        service
            .untag_resource(&topic_arn, &[String::from("env")])
            .expect("tag removal should succeed");
        let tags = service
            .list_tags_for_resource(&topic_arn)
            .expect("tags should reload");
        assert_eq!(tags.get("env"), None);
    }

    #[test]
    fn sns_core_subscribe_http_stays_pending_and_delivers_confirmation() {
        let (service, transport) = service_with_transport(10);
        let topic_arn = create_topic(&service, "orders");

        let subscribed = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "http://127.0.0.1:9010/subscription".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("subscription should be created");
        let subscriptions = service
            .list_subscriptions_by_topic(&topic_arn)
            .expect("subscriptions should list");

        assert_eq!(
            subscribed.response_subscription_arn,
            SUBSCRIBE_PENDING_CONFIRMATION_ARN
        );
        assert_eq!(subscribed.state, SubscriptionState::PendingConfirmation);
        assert_eq!(
            listed_subscription_arns(&subscriptions),
            vec![String::from("PendingConfirmation")]
        );

        let confirmations = transport.confirmations();
        assert_eq!(confirmations.len(), 1);
        assert_eq!(confirmations[0].endpoint.path, "/subscription");
        assert_eq!(confirmations[0].endpoint.endpoint.port(), 9010);
        assert_eq!(confirmations[0].topic_arn, topic_arn);
        assert!(!confirmations[0].token.is_empty());
    }

    #[test]
    fn sns_core_confirm_subscription_changes_list_visibility_and_is_idempotent()
     {
        let (service, transport) = service_with_transport(0);
        let topic_arn = create_topic(&service, "orders");
        let subscribed = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "http://127.0.0.1:9010/subscription".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: true,
                topic_arn: topic_arn.clone(),
            })
            .expect("subscription should be created");
        let token = transport.confirmations()[0].token.clone();

        let confirmed = service
            .confirm_subscription(&topic_arn, &token)
            .expect("subscription should confirm");
        let confirmed_again = service
            .confirm_subscription(&topic_arn, &token)
            .expect("confirm should be idempotent");
        let listed = service
            .list_subscriptions_by_topic(&topic_arn)
            .expect("subscriptions should list");

        assert_eq!(confirmed, subscribed.subscription_arn);
        assert_eq!(confirmed_again, subscribed.subscription_arn);
        assert_eq!(listed[0].state, SubscriptionState::Confirmed);
        assert_eq!(
            listed_subscription_arns(&listed),
            vec![subscribed.subscription_arn.to_string()]
        );
    }

    #[test]
    fn sns_core_subscribe_sqs_is_confirmed_immediately() {
        let service = SnsService::new();
        let topic_arn = create_topic(&service, "orders");
        let subscribed = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "arn:aws:sqs:eu-west-2:000000000000:orders-queue"
                    .to_owned(),
                protocol: "sqs".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("sqs subscription should auto confirm");

        assert_eq!(subscribed.state, SubscriptionState::Confirmed);
        assert_eq!(
            subscribed.response_subscription_arn,
            subscribed.subscription_arn.to_string()
        );
    }

    #[test]
    fn sns_core_invalid_confirmation_token_and_missing_topic_fail_explicitly()
    {
        let (service, _transport) = service_with_transport(0);
        let topic_arn = create_topic(&service, "orders");
        let _ = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "http://127.0.0.1:9010/subscription".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: true,
                topic_arn: topic_arn.clone(),
            })
            .expect("subscription should be created");

        let invalid_token = service
            .confirm_subscription(&topic_arn, "bad-token")
            .expect_err("invalid token should fail");
        let missing_topic = service
            .get_topic_attributes(
                &"arn:aws:sns:eu-west-2:000000000000:missing-topic"
                    .parse()
                    .expect("missing topic ARN should parse"),
            )
            .expect_err("missing topic should fail");

        assert_eq!(invalid_token.code(), "InvalidParameter");
        assert_eq!(missing_topic.code(), "NotFound");
    }

    #[test]
    fn sns_core_confirm_rejects_wrong_topic_even_when_token_exists() {
        let (service, transport) = service_with_transport(0);
        let topic_arn = create_topic(&service, "orders");
        let other_topic_arn = create_topic(&service, "other-orders");
        let _ = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "http://127.0.0.1:9010/subscription".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: true,
                topic_arn: topic_arn.clone(),
            })
            .expect("subscription should be created");
        let token = transport.confirmations()[0].token.clone();

        let error = service
            .confirm_subscription(&other_topic_arn, &token)
            .expect_err("wrong topic should fail");

        assert_eq!(error.code(), "InvalidParameter");
        assert!(error.message().contains("Topic"));
    }

    #[test]
    fn sns_core_publish_returns_message_id_without_subscribers() {
        let service = SnsService::new();
        let topic_arn = create_topic(&service, "orders");

        let published = service
            .publish(PublishInput {
                message: "payload".to_owned(),
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                subject: None,
                target_arn: None,
                topic_arn: Some(topic_arn),
            })
            .expect("publish should succeed");

        assert_eq!(
            published.message_id,
            "00000000-0000-0000-0000-000000000001"
        );
    }

    #[test]
    fn sns_core_publish_to_missing_topic_and_unsupported_transports_fail() {
        let service = SnsService::new();
        let missing = service
            .publish(PublishInput {
                message: "payload".to_owned(),
                message_attributes: BTreeMap::new(),
                message_deduplication_id: None,
                message_group_id: None,
                subject: None,
                target_arn: None,
                topic_arn: Some(
                    "arn:aws:sns:eu-west-2:000000000000:missing"
                        .parse()
                        .expect("missing topic ARN should parse"),
                ),
            })
            .expect_err("missing topic should fail");
        let topic_arn = create_topic(&service, "orders");
        let unsupported = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "user@example.com".to_owned(),
                protocol: "email".to_owned(),
                return_subscription_arn: false,
                topic_arn,
            })
            .expect_err("email should be unsupported");

        assert_eq!(missing.code(), "NotFound");
        assert_eq!(unsupported.code(), "UnsupportedOperation");
    }

    #[test]
    fn sns_delivery_planner_fans_out_to_confirmed_matching_subscriptions_only()
    {
        let transport = Arc::new(RecordingTransport::default());
        let service = SnsService::with_transport(
            Arc::new(|| UNIX_EPOCH),
            Arc::new(SequentialSnsIdentifierSource::default()),
            transport.clone(),
        );
        let topic_arn = create_topic(&service, "orders");

        service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::from([(
                    "FilterPolicy".to_owned(),
                    r#"{"store":["eu-west"]}"#.to_owned(),
                )]),
                endpoint: "http://127.0.0.1:9010/confirmed".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: true,
                topic_arn: topic_arn.clone(),
            })
            .expect("http subscription should be created");
        let confirmation_token = transport.confirmations()[0].token.clone();
        service
            .confirm_subscription(&topic_arn, &confirmation_token)
            .expect("http subscription should confirm");

        service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::from([
                    ("RawMessageDelivery".to_owned(), "true".to_owned()),
                    (
                        "FilterPolicy".to_owned(),
                        r#"{"store":["eu-west"]}"#.to_owned(),
                    ),
                ]),
                endpoint: queue_arn("orders-queue").to_string(),
                protocol: "sqs".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("sqs subscription should be created");
        service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::from([
                    (
                        "FilterPolicy".to_owned(),
                        r#"{"detail":{"kind":[{"prefix":"order-"}]}}"#
                            .to_owned(),
                    ),
                    ("FilterPolicyScope".to_owned(), "MessageBody".to_owned()),
                ]),
                endpoint: lambda_arn("processor").to_string(),
                protocol: "lambda".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("lambda subscription should be created");
        service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "http://127.0.0.1:9010/pending".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: true,
                topic_arn: topic_arn.clone(),
            })
            .expect("pending subscription should be created");

        service
            .publish(PublishInput {
                message: r#"{"detail":{"kind":"order-created"}}"#.to_owned(),
                message_attributes: BTreeMap::from([(
                    "store".to_owned(),
                    MessageAttributeValue {
                        data_type: "String".to_owned(),
                        value: "eu-west".to_owned(),
                    },
                )]),
                message_deduplication_id: None,
                message_group_id: None,
                subject: Some("Orders".to_owned()),
                target_arn: None,
                topic_arn: Some(topic_arn.clone()),
            })
            .expect("publish should succeed");

        let deliveries = transport.deliveries();
        assert_eq!(deliveries.len(), 3);
        assert!(deliveries.iter().all(|delivery| {
            !matches!(
                &delivery.endpoint,
                DeliveryEndpoint::Http(endpoint)
                    if endpoint.endpoint.host() == "127.0.0.1"
                        && endpoint.endpoint.port() == 9010
                        && endpoint.path == "/pending"
            )
        }));

        let http_delivery = deliveries
            .iter()
            .find(|delivery| {
                matches!(
                    &delivery.endpoint,
                    DeliveryEndpoint::Http(endpoint)
                        if endpoint.endpoint.host() == "127.0.0.1"
                            && endpoint.endpoint.port() == 9010
                            && endpoint.path == "/confirmed"
                )
            })
            .expect("confirmed http subscriber should receive a delivery");
        assert!(matches!(
            &http_delivery.endpoint,
            DeliveryEndpoint::Http(endpoint)
                if endpoint.endpoint.host() == "127.0.0.1"
                    && endpoint.endpoint.port() == 9010
                    && endpoint.path == "/confirmed"
        ));
        assert!(!http_delivery.raw_message_delivery);
        assert_eq!(http_delivery.payload.subject.as_deref(), Some("Orders"));

        let lambda_delivery = deliveries
            .iter()
            .find(|delivery| {
                matches!(
                    &delivery.endpoint,
                    DeliveryEndpoint::Lambda(endpoint)
                        if endpoint == &lambda_arn("processor")
                )
            })
            .expect("lambda subscriber should receive a delivery");
        assert!(matches!(
            &lambda_delivery.endpoint,
            DeliveryEndpoint::Lambda(endpoint)
                if endpoint == &lambda_arn("processor")
        ));
        assert_eq!(lambda_delivery.payload.topic_arn, topic_arn);

        let sqs_delivery = deliveries
            .iter()
            .find(|delivery| {
                matches!(
                    &delivery.endpoint,
                    DeliveryEndpoint::Sqs(queue)
                        if queue == &queue_arn("orders-queue")
                )
            })
            .expect("sqs subscriber should receive a delivery");
        assert!(sqs_delivery.raw_message_delivery);
        assert_eq!(
            sqs_delivery.payload.message,
            r#"{"detail":{"kind":"order-created"}}"#
        );
    }

    #[test]
    fn sns_delivery_payload_helpers_encode_http_sqs_and_lambda_messages() {
        let advertised_edge = aws::AdvertisedEdge::default();
        let payload = NotificationPayload {
            message: r#"{"hello":"world"}"#.to_owned(),
            message_attributes: BTreeMap::from([(
                "store".to_owned(),
                MessageAttributeValue {
                    data_type: "String".to_owned(),
                    value: "eu-west".to_owned(),
                },
            )]),
            message_deduplication_id: None,
            message_group_id: None,
            message_id: "00000000-0000-0000-0000-000000000999".to_owned(),
            subject: Some("Orders".to_owned()),
            subscription_arn:
                "arn:aws:sns:eu-west-2:000000000000:orders:sub-http"
                    .parse()
                    .expect("subscription ARN should parse"),
            timestamp: "1970-01-01T00:00:00Z".to_owned(),
            topic_arn: "arn:aws:sns:eu-west-2:000000000000:orders"
                .parse()
                .expect("topic ARN should parse"),
        };

        let http_body: serde_json::Value = serde_json::from_slice(
            &payload.http_body(false, &advertised_edge),
        )
        .expect("http body should be JSON");
        assert_eq!(http_body["Type"], "Notification");
        assert_eq!(http_body["Subject"], "Orders");
        assert_eq!(http_body["Message"], r#"{"hello":"world"}"#);
        assert_eq!(
            payload.sqs_body(true, &advertised_edge),
            r#"{"hello":"world"}"#
        );

        let sqs_body: serde_json::Value =
            serde_json::from_str(&payload.sqs_body(false, &advertised_edge))
                .expect("sqs body should be JSON");
        assert_eq!(sqs_body["Message"], r#"{"hello":"world"}"#);
        assert_eq!(sqs_body["MessageAttributes"]["store"]["Value"], "eu-west");

        let lambda_event: serde_json::Value =
            serde_json::from_slice(&payload.lambda_event(&advertised_edge))
                .expect("lambda event should be JSON");
        assert_eq!(lambda_event["Records"][0]["EventSource"], "aws:sns");
        assert_eq!(
            lambda_event["Records"][0]["Sns"]["Message"],
            r#"{"hello":"world"}"#
        );
    }

    #[test]
    fn sns_delivery_subscription_attributes_round_trip_and_invalid_filter_policy_fail()
     {
        let service = SnsService::new();
        let topic_arn = create_topic(&service, "orders");
        let subscribed = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::from([
                    ("RawMessageDelivery".to_owned(), "true".to_owned()),
                    (
                        "FilterPolicy".to_owned(),
                        r#"{"store":["eu-west"]}"#.to_owned(),
                    ),
                ]),
                endpoint: queue_arn("orders-queue").to_string(),
                protocol: "sqs".to_owned(),
                return_subscription_arn: true,
                topic_arn: topic_arn.clone(),
            })
            .expect("subscription should be created");

        let attributes = service
            .get_subscription_attributes(&subscribed.subscription_arn)
            .expect("subscription attributes should load");
        assert_eq!(
            attributes.get("RawMessageDelivery").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            attributes.get("FilterPolicy").map(String::as_str),
            Some(r#"{"store":["eu-west"]}"#)
        );
        assert_eq!(
            attributes.get("FilterPolicyScope").map(String::as_str),
            Some("MessageAttributes")
        );

        service
            .set_subscription_attributes(
                &subscribed.subscription_arn,
                "FilterPolicyScope",
                "MessageBody",
            )
            .expect("scope update should succeed");
        service
            .set_subscription_attributes(
                &subscribed.subscription_arn,
                "FilterPolicy",
                r#"{"detail":{"kind":[{"prefix":"order-"}]}}"#,
            )
            .expect("policy update should succeed");
        let updated = service
            .get_subscription_attributes(&subscribed.subscription_arn)
            .expect("updated subscription attributes should load");
        assert_eq!(
            updated.get("FilterPolicyScope").map(String::as_str),
            Some("MessageBody")
        );
        assert_eq!(
            updated.get("FilterPolicy").map(String::as_str),
            Some(r#"{"detail":{"kind":[{"prefix":"order-"}]}}"#)
        );

        let error = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::from([(
                    "FilterPolicy".to_owned(),
                    r#"{"store":[{"suffix":"west"}]}"#.to_owned(),
                )]),
                endpoint: queue_arn("other-queue").to_string(),
                protocol: "sqs".to_owned(),
                return_subscription_arn: false,
                topic_arn,
            })
            .expect_err("unsupported filter operators should fail");

        assert_eq!(error.code(), "InvalidParameter");
        assert!(error.message().contains("FilterPolicy"));
    }

    #[test]
    fn sns_delivery_publish_batch_reports_partial_failure() {
        let transport = Arc::new(RecordingTransport::default());
        let service = SnsService::with_transport(
            Arc::new(|| UNIX_EPOCH),
            Arc::new(SequentialSnsIdentifierSource::default()),
            transport.clone(),
        );
        let topic_arn = create_topic(&service, "orders");

        service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: queue_arn("orders-queue").to_string(),
                protocol: "sqs".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("sqs subscription should be created");

        let output = service
            .publish_batch(
                &topic_arn,
                vec![
                    PublishBatchEntryInput {
                        id: "ok".to_owned(),
                        message: "payload".to_owned(),
                        message_attributes: BTreeMap::new(),
                        message_deduplication_id: None,
                        message_group_id: None,
                        subject: None,
                    },
                    PublishBatchEntryInput {
                        id: "bad".to_owned(),
                        message: String::new(),
                        message_attributes: BTreeMap::new(),
                        message_deduplication_id: None,
                        message_group_id: None,
                        subject: None,
                    },
                ],
            )
            .expect("publish batch should succeed");

        assert_eq!(output.successful.len(), 1);
        assert_eq!(output.successful[0].id, "ok");
        assert_eq!(output.failed.len(), 1);
        assert_eq!(output.failed[0].id, "bad");
        assert_eq!(output.failed[0].code, "InvalidParameter");
        assert_eq!(transport.deliveries().len(), 1);
    }

    #[test]
    fn sns_core_delete_topic_removes_subscriptions_and_allows_recreate() {
        let service = SnsService::new();
        let topic_arn = create_topic(&service, "orders");
        let subscribed = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "arn:aws:sqs:eu-west-2:000000000000:orders-queue"
                    .to_owned(),
                protocol: "sqs".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("subscription should succeed");

        service.delete_topic(&topic_arn).expect("delete should succeed");
        let recreated = create_topic(&service, "orders");
        let listed = service.list_subscriptions(&scope());

        assert_eq!(recreated, topic_arn);
        assert!(listed.is_empty());
        service
            .unsubscribe(&subscribed.subscription_arn)
            .expect("unsubscribe should remain idempotent");
    }

    #[test]
    fn sns_core_helpers_cover_formatting_and_parsing_edges() {
        let parsed = parse_http_endpoint("http://localhost:8080/path")
            .expect("endpoint should parse");
        let timestamp =
            formatted_timestamp(UNIX_EPOCH + Duration::from_secs(1));
        let encoded =
            percent_encode("arn:aws:sns:eu-west-2:000000000000:orders");
        let (service, transport) = service_with_transport(0);
        let topic_arn = create_topic(&service, "orders");
        let _ = service
            .subscribe(super::SubscribeInput {
                attributes: BTreeMap::new(),
                endpoint: "http://127.0.0.1:9010/subscription".to_owned(),
                protocol: "http".to_owned(),
                return_subscription_arn: false,
                topic_arn: topic_arn.clone(),
            })
            .expect("failing forwarder should not fail subscribe");
        let bad_topic = service
            .create_topic(
                &scope(),
                CreateTopicInput {
                    attributes: BTreeMap::from([(
                        "FifoTopic".to_owned(),
                        "true".to_owned(),
                    )]),
                    name: "orders".to_owned(),
                },
            )
            .expect_err("fifo flag without .fifo suffix should fail");
        let missing_topic = service
            .list_tags_for_resource(
                &"arn:aws:sns:eu-west-2:000000000000:missing-topic"
                    .parse()
                    .expect("missing topic ARN should parse"),
            )
            .expect_err("missing topic should fail");

        assert_eq!(parsed.endpoint.host(), "localhost");
        assert_eq!(parsed.endpoint.port(), 8080);
        assert_eq!(parsed.path, "/path");
        assert_eq!(timestamp, "1970-01-01T00:00:01Z");
        assert!(encoded.contains("%3A"));
        assert_eq!(transport.confirmations().len(), 1);
        assert_eq!(bad_topic.code(), "InvalidParameter");
        assert_eq!(missing_topic.code(), "NotFound");
        assert_eq!(ServiceName::Sns.as_str(), "sns");
        assert_eq!(
            "000000000000".parse::<AccountId>().expect("account should parse"),
            scope().account_id().clone()
        );
    }

    #[test]
    fn sns_core_query_error_mapping_is_stable() {
        let error = SnsError::UnsupportedOperation {
            message: "Operation ConfirmSubscription is not supported."
                .to_owned(),
        }
        .to_aws_error();

        assert_eq!(error.code(), "UnsupportedOperation");
        assert_eq!(error.status_code(), 400);
        assert!(error.sender_fault());
    }
}
