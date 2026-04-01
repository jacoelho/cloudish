use crate::SnsRegistry;
use crate::delivery::{
    DeliveryEndpoint, MessageAttributeValue, NotificationPayload,
    PlannedDelivery, PublishRequest,
};
use crate::errors::SnsError;
use crate::pagination::{PaginatedList, PaginationContext, paginate};
use crate::scope::SnsScope;
use crate::topics::{TopicKey, validate_topic_name};
use aws::{AccountId, Arn, ArnResource, Endpoint, ServiceName};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) const LIST_PENDING_CONFIRMATION_ARN: &str = "PendingConfirmation";
pub(crate) const SUBSCRIBE_PENDING_CONFIRMATION_ARN: &str =
    "pending confirmation";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeInput {
    pub attributes: BTreeMap<String, String>,
    pub endpoint: String,
    pub protocol: String,
    pub return_subscription_arn: bool,
    pub topic_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeOutput {
    pub response_subscription_arn: String,
    pub state: SubscriptionState,
    pub subscription_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedSubscription {
    pub endpoint: String,
    pub owner: AccountId,
    pub protocol: String,
    pub state: SubscriptionState,
    pub subscription_arn: Arn,
    pub topic_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionState {
    Confirmed,
    PendingConfirmation,
}

impl SubscriptionState {
    pub fn as_attribute_value(&self) -> &'static str {
        match self {
            Self::Confirmed => "false",
            Self::PendingConfirmation => "true",
        }
    }

    pub fn listed_subscription_arn(&self, subscription_arn: &Arn) -> String {
        match self {
            Self::Confirmed => subscription_arn.to_string(),
            Self::PendingConfirmation => {
                LIST_PENDING_CONFIRMATION_ARN.to_owned()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct RequestedSubscriptionAttributes {
    filter_policy: Option<Option<FilterPolicy>>,
    filter_policy_json: Option<Option<String>>,
    filter_policy_scope: Option<FilterPolicyScope>,
    raw_message_delivery: Option<bool>,
}

impl RequestedSubscriptionAttributes {
    fn parse(attributes: &BTreeMap<String, String>) -> Result<Self, SnsError> {
        let mut requested = Self::default();

        for (name, value) in attributes {
            if name == "FilterPolicy" {
                continue;
            }
            requested.apply_pair(name, value)?;
        }
        if let Some(filter_policy) = attributes.get("FilterPolicy") {
            requested.apply_pair("FilterPolicy", filter_policy)?;
        }

        Ok(requested)
    }

    fn apply_pair(&mut self, name: &str, value: &str) -> Result<(), SnsError> {
        match name {
            "FilterPolicy" => {
                if value.is_empty() {
                    self.filter_policy = Some(None);
                    self.filter_policy_json = Some(None);
                } else {
                    let scope = self
                        .filter_policy_scope
                        .unwrap_or(FilterPolicyScope::MessageAttributes);
                    let policy = FilterPolicy::parse(value, scope)?;
                    self.filter_policy = Some(Some(policy));
                    self.filter_policy_json = Some(Some(value.to_owned()));
                }
            }
            "FilterPolicyScope" => {
                self.filter_policy_scope =
                    Some(FilterPolicyScope::parse(value)?);
            }
            "RawMessageDelivery" => {
                self.raw_message_delivery =
                    Some(parse_attribute_bool(name, value)?);
            }
            _ => {
                return Err(SnsError::InvalidParameter {
                    message: "Invalid parameter: AttributeName".to_owned(),
                });
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriptionAttributes {
    filter_policy: Option<FilterPolicy>,
    filter_policy_json: Option<String>,
    filter_policy_scope: FilterPolicyScope,
    pub(crate) raw_message_delivery: bool,
}

impl Default for SubscriptionAttributes {
    fn default() -> Self {
        Self {
            filter_policy: None,
            filter_policy_json: None,
            filter_policy_scope: FilterPolicyScope::MessageAttributes,
            raw_message_delivery: false,
        }
    }
}

impl SubscriptionAttributes {
    fn from_request(
        requested: RequestedSubscriptionAttributes,
    ) -> Result<Self, SnsError> {
        let mut attributes = Self::default();
        attributes.apply_request(requested)?;
        Ok(attributes)
    }

    fn apply_request(
        &mut self,
        requested: RequestedSubscriptionAttributes,
    ) -> Result<(), SnsError> {
        let reparsed_filter_policy = if requested.filter_policy.is_none() {
            if let Some(scope) = requested.filter_policy_scope {
                self.filter_policy_json
                    .as_deref()
                    .map(|value| FilterPolicy::parse(value, scope))
                    .transpose()?
            } else {
                None
            }
        } else {
            None
        };
        if let Some(scope) = requested.filter_policy_scope {
            self.filter_policy_scope = scope;
        }
        if let Some(raw_message_delivery) = requested.raw_message_delivery {
            self.raw_message_delivery = raw_message_delivery;
        }
        if let Some(filter_policy) = requested.filter_policy {
            self.filter_policy = filter_policy;
        } else if let Some(filter_policy) = reparsed_filter_policy {
            self.filter_policy = Some(filter_policy);
        }
        if let Some(filter_policy_json) = requested.filter_policy_json {
            self.filter_policy_json = filter_policy_json;
        }

        Ok(())
    }

    fn as_attribute_map(
        &self,
        subscription: &SubscriptionRecord,
    ) -> BTreeMap<String, String> {
        let mut attributes = BTreeMap::from([
            ("ConfirmationWasAuthenticated".to_owned(), "false".to_owned()),
            ("Endpoint".to_owned(), subscription.endpoint.clone()),
            ("Owner".to_owned(), subscription.owner.to_string()),
            (
                "PendingConfirmation".to_owned(),
                subscription.state.pending_flag(),
            ),
            ("Protocol".to_owned(), subscription.protocol.as_str().to_owned()),
            (
                "RawMessageDelivery".to_owned(),
                self.raw_message_delivery.to_string(),
            ),
            (
                "SubscriptionArn".to_owned(),
                subscription.subscription_arn.to_string(),
            ),
            (
                "TopicArn".to_owned(),
                subscription.topic.topic_arn().to_string(),
            ),
        ]);

        if let Some(filter_policy_json) = self.filter_policy_json.as_ref() {
            attributes
                .insert("FilterPolicy".to_owned(), filter_policy_json.clone());
            attributes.insert(
                "FilterPolicyScope".to_owned(),
                self.filter_policy_scope.as_str().to_owned(),
            );
        }

        attributes
    }

    fn matches_request(
        &self,
        requested: &RequestedSubscriptionAttributes,
    ) -> bool {
        if let Some(raw_message_delivery) = requested.raw_message_delivery
            && self.raw_message_delivery != raw_message_delivery
        {
            return false;
        }
        if let Some(filter_policy_scope) = requested.filter_policy_scope
            && self.filter_policy_scope != filter_policy_scope
        {
            return false;
        }
        if let Some(filter_policy_json) = requested.filter_policy_json.as_ref()
            && self.filter_policy_json.as_ref() != filter_policy_json.as_ref()
        {
            return false;
        }

        true
    }

    fn set(
        &mut self,
        attribute_name: &str,
        attribute_value: &str,
    ) -> Result<(), SnsError> {
        let mut requested = RequestedSubscriptionAttributes::default();
        if attribute_name == "FilterPolicy" {
            requested.filter_policy_scope = Some(self.filter_policy_scope);
        }
        requested.apply_pair(attribute_name, attribute_value)?;
        self.apply_request(requested)?;

        Ok(())
    }

    pub(crate) fn matches(&self, request: &PublishRequest) -> bool {
        let Some(filter_policy) = self.filter_policy.as_ref() else {
            return true;
        };

        filter_policy.matches(request)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterPolicyScope {
    MessageAttributes,
    MessageBody,
}

impl FilterPolicyScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::MessageAttributes => "MessageAttributes",
            Self::MessageBody => "MessageBody",
        }
    }

    fn parse(value: &str) -> Result<Self, SnsError> {
        match value {
            "MessageAttributes" => Ok(Self::MessageAttributes),
            "MessageBody" => Ok(Self::MessageBody),
            _ => Err(SnsError::InvalidParameter {
                message: "Invalid parameter: FilterPolicyScope".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilterPolicy {
    MessageAttributes(BTreeMap<String, Vec<FilterCondition>>),
    MessageBody(BTreeMap<String, FilterNode>),
}

impl FilterPolicy {
    fn parse(value: &str, scope: FilterPolicyScope) -> Result<Self, SnsError> {
        let parsed: Value = serde_json::from_str(value).map_err(|_| {
            invalid_filter_policy_error("failed to parse JSON.")
        })?;
        let Value::Object(root) = parsed else {
            return Err(invalid_filter_policy_error(
                "policy must be a JSON object.",
            ));
        };

        match scope {
            FilterPolicyScope::MessageAttributes => Ok(
                Self::MessageAttributes(parse_attribute_filter_policy(&root)?),
            ),
            FilterPolicyScope::MessageBody => {
                Ok(Self::MessageBody(parse_body_filter_policy(&root)?))
            }
        }
    }

    fn matches(&self, request: &PublishRequest) -> bool {
        match self {
            Self::MessageAttributes(policy) => {
                policy.iter().all(|(key, conditions)| {
                    evaluate_attribute_conditions(
                        conditions,
                        request.message_attributes.get(key),
                    )
                })
            }
            Self::MessageBody(policy) => {
                let Ok(body) = serde_json::from_str::<Value>(&request.message)
                else {
                    return false;
                };

                evaluate_body_filter_policy(policy, Some(&body))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilterNode {
    Conditions(Vec<FilterCondition>),
    Nested(BTreeMap<String, FilterNode>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilterCondition {
    AnythingBut(Vec<FilterLiteral>),
    Exact(FilterLiteral),
    Exists(bool),
    Numeric(Vec<NumericComparison>),
    Prefix(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilterLiteral {
    Bool(bool),
    Null,
    Number(String),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NumericComparison {
    Eq(String),
    Gt(String),
    Gte(String),
    Lt(String),
    Lte(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriptionRecord {
    pub(crate) attributes: SubscriptionAttributes,
    pub(crate) delivery_endpoint: DeliveryEndpoint,
    pub(crate) endpoint: String,
    pub(crate) owner: AccountId,
    pub(crate) protocol: SubscriptionProtocol,
    pub(crate) state: SubscriptionRecordState,
    pub(crate) subscription_arn: Arn,
    pub(crate) topic: TopicKey,
}

impl SubscriptionRecord {
    fn listed(&self) -> ListedSubscription {
        ListedSubscription {
            endpoint: self.endpoint.clone(),
            owner: self.owner.clone(),
            protocol: self.protocol.as_str().to_owned(),
            state: self.state.public_state(),
            subscription_arn: self.subscription_arn.clone(),
            topic_arn: self.topic.topic_arn(),
        }
    }

    fn pending_confirmation(&self) -> Option<ConfirmationDelivery> {
        let SubscriptionRecordState::PendingConfirmation { token } =
            &self.state
        else {
            return None;
        };

        let DeliveryEndpoint::Http(endpoint) = &self.delivery_endpoint else {
            return None;
        };

        Some(ConfirmationDelivery {
            endpoint: endpoint.clone(),
            token: token.clone(),
            topic_arn: self.topic.topic_arn(),
        })
    }

    pub(crate) fn matches(&self, request: &PublishRequest) -> bool {
        self.attributes.matches(request)
    }

    pub(crate) fn plan_delivery(
        &self,
        request: &PublishRequest,
    ) -> PlannedDelivery {
        PlannedDelivery {
            endpoint: self.delivery_endpoint.clone(),
            raw_message_delivery: self.attributes.raw_message_delivery,
            payload: NotificationPayload {
                message: request.message.clone(),
                message_attributes: request.message_attributes.clone(),
                message_deduplication_id: request
                    .message_deduplication_id
                    .clone(),
                message_group_id: request.message_group_id.clone(),
                message_id: request.message_id.clone(),
                subject: request.subject.clone(),
                subscription_arn: self.subscription_arn.clone(),
                timestamp: request.timestamp.clone(),
                topic_arn: request.topic_arn.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubscriptionRecordState {
    Confirmed,
    PendingConfirmation { token: String },
}

impl SubscriptionRecordState {
    pub(crate) fn public_state(&self) -> SubscriptionState {
        match self {
            Self::Confirmed => SubscriptionState::Confirmed,
            Self::PendingConfirmation { .. } => {
                SubscriptionState::PendingConfirmation
            }
        }
    }

    fn pending_flag(&self) -> String {
        match self {
            Self::Confirmed => "false".to_owned(),
            Self::PendingConfirmation { .. } => "true".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionProtocol {
    Http,
    Https,
    Lambda,
    Sqs,
}

impl SubscriptionProtocol {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Lambda => "lambda",
            Self::Sqs => "sqs",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionEndpoint {
    delivery_endpoint: DeliveryEndpoint,
    protocol: SubscriptionProtocol,
}

fn parse_subscription_endpoint(
    value: &str,
    endpoint: &str,
    standard_topic: bool,
) -> Result<SubscriptionEndpoint, SnsError> {
    match value {
        "http" => Ok(SubscriptionEndpoint {
            delivery_endpoint: DeliveryEndpoint::Http(
                parse_http_endpoint_for_protocol(endpoint, "http")?,
            ),
            protocol: SubscriptionProtocol::Http,
        }),
        "https" => Ok(SubscriptionEndpoint {
            delivery_endpoint: DeliveryEndpoint::Http(
                parse_http_endpoint_for_protocol(endpoint, "https")?,
            ),
            protocol: SubscriptionProtocol::Https,
        }),
        "lambda" => Ok(SubscriptionEndpoint {
            delivery_endpoint: DeliveryEndpoint::Lambda(
                validate_endpoint_arn(endpoint, ServiceName::Lambda)?,
            ),
            protocol: SubscriptionProtocol::Lambda,
        }),
        "sqs" => {
            let arn = validate_endpoint_arn(endpoint, ServiceName::Sqs)?;
            if standard_topic {
                let ArnResource::Sqs(resource) = arn.resource() else {
                    return Err(SnsError::InvalidParameter {
                        message: "Invalid parameter: SQS endpoint ARN"
                            .to_owned(),
                    });
                };
                if resource.queue_name().ends_with(".fifo") {
                    return Err(SnsError::InvalidParameter {
                        message: "Invalid parameter: Invalid parameter: \
                                  Endpoint Reason: FIFO SQS Queues can \
                                  not be subscribed to standard SNS topics"
                            .to_owned(),
                    });
                }
            }
            Ok(SubscriptionEndpoint {
                delivery_endpoint: DeliveryEndpoint::Sqs(arn),
                protocol: SubscriptionProtocol::Sqs,
            })
        }
        "email" | "email-json" | "sms" => {
            Err(SnsError::UnsupportedOperation {
                message: format!(
                    "Subscription protocol {value} is not supported."
                ),
            })
        }
        _ => Err(SnsError::InvalidParameter {
            message: format!(
                "Invalid parameter: Amazon SNS does not support this \
                 protocol string: {value}"
            ),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscribedRecord {
    pub(crate) confirmation_delivery: Option<ConfirmationDelivery>,
    pub(crate) response_subscription_arn: String,
    pub(crate) state: SubscriptionState,
    pub(crate) subscription_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmationDelivery {
    pub endpoint: ParsedHttpEndpoint,
    pub token: String,
    pub topic_arn: Arn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedHttpEndpoint {
    pub endpoint: Endpoint,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSubscriptionArn {
    _subscription_id: String,
    _topic: TopicKey,
}

impl ParsedSubscriptionArn {
    fn parse(arn: &Arn) -> Result<Self, SnsError> {
        if arn.service() != ServiceName::Sns {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: SubscriptionArn".to_owned(),
            });
        }
        let Some(region) = arn.region().cloned() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: SubscriptionArn".to_owned(),
            });
        };
        let Some(account_id) = arn.account_id().cloned() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: SubscriptionArn".to_owned(),
            });
        };
        let ArnResource::Generic(resource) = arn.resource() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: SubscriptionArn".to_owned(),
            });
        };
        let Some((topic_name, subscription_id)) = resource.split_once(':')
        else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: SubscriptionArn".to_owned(),
            });
        };
        if subscription_id.is_empty() {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: SubscriptionArn".to_owned(),
            });
        }
        validate_topic_name(topic_name, topic_name.ends_with(".fifo"))?;

        Ok(Self {
            _subscription_id: subscription_id.to_owned(),
            _topic: TopicKey {
                account_id,
                name: topic_name.to_owned(),
                region,
            },
        })
    }
}

impl SnsRegistry {
    pub(crate) fn subscribe_record(
        &mut self,
        input: SubscribeInput,
        subscription_id: &str,
        confirmation_token: &str,
    ) -> Result<SubscribedRecord, SnsError> {
        let topic = TopicKey::from_topic_arn(&input.topic_arn)?;
        let topic_record = self.topic_record(&topic)?;
        let endpoint = parse_subscription_endpoint(
            &input.protocol,
            &input.endpoint,
            topic.is_fifo_topic(topic_record),
        )?;
        let requested_attributes =
            RequestedSubscriptionAttributes::parse(&input.attributes)?;

        if let Some(existing) = self
            .subscriptions
            .values()
            .find(|subscription| {
                subscription.topic == topic
                    && subscription.endpoint == input.endpoint
                    && subscription.protocol == endpoint.protocol
            })
            .cloned()
        {
            if !existing.attributes.matches_request(&requested_attributes) {
                return Err(SnsError::InvalidParameter {
                    message: "Invalid parameter: Attributes Reason: \
                              Subscription already exists with different \
                              attributes"
                        .to_owned(),
                });
            }
            let response_subscription_arn = response_subscription_arn(
                &existing.state,
                &existing.subscription_arn,
                input.return_subscription_arn,
            );

            return Ok(SubscribedRecord {
                confirmation_delivery: existing.pending_confirmation(),
                response_subscription_arn,
                state: existing.state.public_state(),
                subscription_arn: existing.subscription_arn,
            });
        }

        let subscription_arn = topic.subscription_arn(subscription_id);
        let state = match endpoint.protocol {
            SubscriptionProtocol::Http | SubscriptionProtocol::Https => {
                SubscriptionRecordState::PendingConfirmation {
                    token: confirmation_token.to_owned(),
                }
            }
            SubscriptionProtocol::Lambda | SubscriptionProtocol::Sqs => {
                SubscriptionRecordState::Confirmed
            }
        };
        let attributes =
            SubscriptionAttributes::from_request(requested_attributes)?;
        let subscription = SubscriptionRecord {
            attributes,
            delivery_endpoint: endpoint.delivery_endpoint,
            endpoint: input.endpoint,
            owner: topic.account_id.clone(),
            protocol: endpoint.protocol,
            state: state.clone(),
            subscription_arn: subscription_arn.clone(),
            topic: topic.clone(),
        };
        if let SubscriptionRecordState::PendingConfirmation { token } = &state
        {
            self.tokens.insert(token.clone(), subscription_arn.clone());
        }

        let response_subscription_arn = response_subscription_arn(
            &state,
            &subscription_arn,
            input.return_subscription_arn,
        );
        let confirmation_delivery = subscription.pending_confirmation();
        self.subscriptions.insert(subscription_arn.clone(), subscription);

        Ok(SubscribedRecord {
            confirmation_delivery,
            response_subscription_arn,
            state: state.public_state(),
            subscription_arn,
        })
    }

    pub(crate) fn unsubscribe(
        &mut self,
        subscription_arn: &Arn,
    ) -> Result<(), SnsError> {
        let _ = ParsedSubscriptionArn::parse(subscription_arn)?;
        let Some(_subscription) = self.subscriptions.remove(subscription_arn)
        else {
            return Err(SnsError::NotFound {
                message: "Subscription does not exist".to_owned(),
            });
        };

        self.remove_tokens_for_subscription(subscription_arn);

        Ok(())
    }

    pub(crate) fn confirm_subscription(
        &mut self,
        topic_arn: &Arn,
        token: &str,
    ) -> Result<Arn, SnsError> {
        let topic = TopicKey::from_topic_arn(topic_arn)?;
        self.topic_record(&topic)?;

        let Some(subscription_arn) = self.tokens.get(token).cloned() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: Token".to_owned(),
            });
        };
        let Some(subscription) = self.subscriptions.get_mut(&subscription_arn)
        else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: Token".to_owned(),
            });
        };

        if subscription.topic != topic {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: Topic".to_owned(),
            });
        }

        if matches!(subscription.state, SubscriptionRecordState::Confirmed) {
            return Ok(subscription.subscription_arn.clone());
        }

        subscription.state = SubscriptionRecordState::Confirmed;

        Ok(subscription.subscription_arn.clone())
    }

    pub(crate) fn list_subscriptions(
        &self,
        scope: &SnsScope,
    ) -> Vec<ListedSubscription> {
        self.subscriptions
            .values()
            .filter(|subscription| subscription.topic.scope() == scope.clone())
            .map(SubscriptionRecord::listed)
            .collect()
    }

    pub(crate) fn list_subscriptions_page(
        &self,
        scope: &SnsScope,
        next_token: Option<&str>,
    ) -> Result<PaginatedList<ListedSubscription>, SnsError> {
        let subscriptions = self.list_subscriptions(scope);

        paginate(
            &subscriptions,
            &PaginationContext::Scope {
                operation: "ListSubscriptions",
                scope: scope.clone(),
            },
            next_token,
        )
    }

    pub(crate) fn list_subscriptions_by_topic(
        &self,
        topic_arn: &Arn,
    ) -> Result<Vec<ListedSubscription>, SnsError> {
        let topic = TopicKey::from_topic_arn(topic_arn)?;
        self.topic_record(&topic)?;

        Ok(self
            .subscriptions
            .values()
            .filter(|subscription| subscription.topic == topic)
            .map(SubscriptionRecord::listed)
            .collect())
    }

    pub(crate) fn list_subscriptions_by_topic_page(
        &self,
        topic_arn: &Arn,
        next_token: Option<&str>,
    ) -> Result<PaginatedList<ListedSubscription>, SnsError> {
        let subscriptions = self.list_subscriptions_by_topic(topic_arn)?;

        paginate(
            &subscriptions,
            &PaginationContext::Topic {
                operation: "ListSubscriptionsByTopic",
                topic_arn: topic_arn.clone(),
            },
            next_token,
        )
    }

    pub(crate) fn get_subscription_attributes(
        &self,
        subscription_arn: &Arn,
    ) -> Result<BTreeMap<String, String>, SnsError> {
        let subscription = self.subscription_record(subscription_arn)?;

        Ok(subscription.attributes.as_attribute_map(subscription))
    }

    pub(crate) fn set_subscription_attributes(
        &mut self,
        subscription_arn: &Arn,
        attribute_name: &str,
        attribute_value: &str,
    ) -> Result<(), SnsError> {
        let subscription = self.subscription_record_mut(subscription_arn)?;
        subscription.attributes.set(attribute_name, attribute_value)
    }

    fn subscription_record(
        &self,
        subscription_arn: &Arn,
    ) -> Result<&SubscriptionRecord, SnsError> {
        let _ = ParsedSubscriptionArn::parse(subscription_arn)?;

        self.subscriptions.get(subscription_arn).ok_or_else(|| {
            SnsError::NotFound {
                message: "Subscription does not exist".to_owned(),
            }
        })
    }

    fn subscription_record_mut(
        &mut self,
        subscription_arn: &Arn,
    ) -> Result<&mut SubscriptionRecord, SnsError> {
        let _ = ParsedSubscriptionArn::parse(subscription_arn)?;

        self.subscriptions.get_mut(subscription_arn).ok_or_else(|| {
            SnsError::NotFound {
                message: "Subscription does not exist".to_owned(),
            }
        })
    }

    pub(crate) fn remove_tokens_for_subscription(
        &mut self,
        subscription_arn: &Arn,
    ) {
        self.tokens.retain(|_, arn| arn != subscription_arn);
    }
}

fn validate_endpoint_arn(
    endpoint: &str,
    service: ServiceName,
) -> Result<Arn, SnsError> {
    let arn = endpoint.parse::<Arn>().map_err(|_| match service {
        ServiceName::Lambda => SnsError::InvalidParameter {
            message: "Invalid parameter: Lambda endpoint ARN".to_owned(),
        },
        ServiceName::Sqs => SnsError::InvalidParameter {
            message: "Invalid parameter: SQS endpoint ARN".to_owned(),
        },
        _ => SnsError::InvalidParameter {
            message: "Invalid parameter: Endpoint".to_owned(),
        },
    })?;
    if arn.service() != service {
        return Err(match service {
            ServiceName::Lambda => SnsError::InvalidParameter {
                message: "Invalid parameter: Lambda endpoint ARN".to_owned(),
            },
            ServiceName::Sqs => SnsError::InvalidParameter {
                message: "Invalid parameter: SQS endpoint ARN".to_owned(),
            },
            _ => SnsError::InvalidParameter {
                message: "Invalid parameter: Endpoint".to_owned(),
            },
        });
    }

    Ok(arn)
}

pub(crate) fn parse_http_endpoint(
    endpoint: &str,
) -> Result<ParsedHttpEndpoint, SnsError> {
    let (default_port, remainder) = if let Some(remainder) =
        endpoint.strip_prefix("http://")
    {
        (80, remainder)
    } else if let Some(remainder) = endpoint.strip_prefix("https://") {
        (443, remainder)
    } else {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: Endpoint must match the specified \
                      protocol"
                .to_owned(),
        });
    };
    let (host_port, path) =
        remainder.split_once('/').unwrap_or((remainder, ""));
    if host_port.is_empty() {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: Endpoint must match the specified \
                      protocol"
                .to_owned(),
        });
    }
    let (host, port) = if let Some((host, port)) = host_port.rsplit_once(':') {
        (
            host,
            port.parse::<u16>().map_err(|_| SnsError::InvalidParameter {
                message: "Invalid parameter: Endpoint must match the \
                          specified protocol"
                    .to_owned(),
            })?,
        )
    } else {
        (host_port, default_port)
    };
    if host.is_empty() {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: Endpoint must match the specified \
                      protocol"
                .to_owned(),
        });
    }

    Ok(ParsedHttpEndpoint {
        endpoint: Endpoint::new(host, port),
        path: if path.is_empty() {
            "/".to_owned()
        } else {
            format!("/{path}")
        },
    })
}

fn parse_http_endpoint_for_protocol(
    endpoint: &str,
    protocol: &str,
) -> Result<ParsedHttpEndpoint, SnsError> {
    let matches_protocol = match protocol {
        "http" => endpoint.starts_with("http://"),
        "https" => endpoint.starts_with("https://"),
        _ => false,
    };
    if !matches_protocol {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: Endpoint must match the specified \
                      protocol"
                .to_owned(),
        });
    }

    parse_http_endpoint(endpoint)
}

fn parse_attribute_bool(
    attribute_name: &str,
    value: &str,
) -> Result<bool, SnsError> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(SnsError::InvalidParameter {
            message: format!("Invalid parameter: {attribute_name}"),
        })
    }
}

fn invalid_filter_policy_error(detail: &str) -> SnsError {
    SnsError::InvalidParameter {
        message: format!("Invalid parameter: FilterPolicy: {detail}"),
    }
}

fn parse_attribute_filter_policy(
    root: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, Vec<FilterCondition>>, SnsError> {
    let mut policy = BTreeMap::new();

    for (key, value) in root {
        let conditions = match value {
            Value::Array(conditions) => parse_filter_conditions(conditions)?,
            Value::Object(_) => vec![parse_filter_condition(value)?],
            _ => {
                return Err(invalid_filter_policy_error(&format!(
                    "\"{key}\" must be an object or an array"
                )));
            }
        };
        policy.insert(key.clone(), conditions);
    }

    Ok(policy)
}

fn parse_body_filter_policy(
    root: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, FilterNode>, SnsError> {
    let mut policy = BTreeMap::new();

    for (key, value) in root {
        policy.insert(key.clone(), parse_filter_node(value)?);
    }

    Ok(policy)
}

fn parse_filter_node(value: &Value) -> Result<FilterNode, SnsError> {
    match value {
        Value::Array(conditions) => {
            Ok(FilterNode::Conditions(parse_filter_conditions(conditions)?))
        }
        Value::Object(map) if is_filter_condition_object(map) => {
            Ok(FilterNode::Conditions(vec![parse_filter_condition(value)?]))
        }
        Value::Object(map) => {
            let mut nested = BTreeMap::new();
            for (key, value) in map {
                nested.insert(key.clone(), parse_filter_node(value)?);
            }
            Ok(FilterNode::Nested(nested))
        }
        _ => Err(invalid_filter_policy_error(
            "filter rules must be arrays or objects.",
        )),
    }
}

fn is_filter_condition_object(map: &serde_json::Map<String, Value>) -> bool {
    matches!(
        map.keys().next().map(String::as_str),
        Some("anything-but" | "exists" | "numeric" | "prefix")
    ) && map.len() == 1
}

fn parse_filter_conditions(
    values: &[Value],
) -> Result<Vec<FilterCondition>, SnsError> {
    if values.is_empty() {
        return Err(invalid_filter_policy_error(
            "filter condition arrays must not be empty.",
        ));
    }

    values.iter().map(parse_filter_condition).collect()
}

fn parse_filter_condition(value: &Value) -> Result<FilterCondition, SnsError> {
    match value {
        Value::Bool(_) | Value::Null | Value::Number(_) | Value::String(_) => {
            Ok(FilterCondition::Exact(parse_filter_literal(value)?))
        }
        Value::Object(map) if map.len() == 1 => {
            let Some((operator, operand)) = map.iter().next() else {
                return Err(invalid_filter_policy_error(
                    "invalid filter condition.",
                ));
            };
            match operator.as_str() {
                "anything-but" => Ok(FilterCondition::AnythingBut(
                    parse_anything_but_literals(operand)?,
                )),
                "exists" => Ok(FilterCondition::Exists(
                    operand.as_bool().ok_or_else(|| {
                        invalid_filter_policy_error(
                            "`exists` must be a boolean.",
                        )
                    })?,
                )),
                "numeric" => Ok(FilterCondition::Numeric(
                    parse_numeric_comparisons(operand)?,
                )),
                "prefix" => Ok(FilterCondition::Prefix(
                    operand
                        .as_str()
                        .ok_or_else(|| {
                            invalid_filter_policy_error(
                                "`prefix` must be a string.",
                            )
                        })?
                        .to_owned(),
                )),
                _ => Err(invalid_filter_policy_error(
                    "unsupported filter operator.",
                )),
            }
        }
        _ => Err(invalid_filter_policy_error("invalid filter condition.")),
    }
}

fn parse_anything_but_literals(
    value: &Value,
) -> Result<Vec<FilterLiteral>, SnsError> {
    match value {
        Value::Array(values) => {
            values.iter().map(parse_filter_literal).collect()
        }
        _ => Ok(vec![parse_filter_literal(value)?]),
    }
}

fn parse_numeric_comparisons(
    value: &Value,
) -> Result<Vec<NumericComparison>, SnsError> {
    let Value::Array(values) = value else {
        return Err(invalid_filter_policy_error(
            "`numeric` must be an array of operator/value pairs.",
        ));
    };
    if values.is_empty() || values.len() % 2 != 0 {
        return Err(invalid_filter_policy_error(
            "`numeric` must contain operator/value pairs.",
        ));
    }

    let mut comparisons = Vec::new();
    for pair in values.chunks(2) {
        let [operator_value, operand_value] = pair else {
            return Err(invalid_filter_policy_error(
                "`numeric` must contain operator/value pairs.",
            ));
        };
        let operator = operator_value.as_str().ok_or_else(|| {
            invalid_filter_policy_error("`numeric` operators must be strings.")
        })?;
        let operand = match operand_value {
            Value::Number(number) => number.to_string(),
            Value::String(number) => number.clone(),
            _ => {
                return Err(invalid_filter_policy_error(
                    "`numeric` operands must be numbers.",
                ));
            }
        };

        comparisons.push(match operator {
            "=" => NumericComparison::Eq(operand),
            ">" => NumericComparison::Gt(operand),
            ">=" => NumericComparison::Gte(operand),
            "<" => NumericComparison::Lt(operand),
            "<=" => NumericComparison::Lte(operand),
            _ => {
                return Err(invalid_filter_policy_error(
                    "unsupported numeric operator.",
                ));
            }
        });
    }

    Ok(comparisons)
}

fn parse_filter_literal(value: &Value) -> Result<FilterLiteral, SnsError> {
    match value {
        Value::Bool(value) => Ok(FilterLiteral::Bool(*value)),
        Value::Null => Ok(FilterLiteral::Null),
        Value::Number(value) => Ok(FilterLiteral::Number(value.to_string())),
        Value::String(value) => Ok(FilterLiteral::String(value.clone())),
        _ => Err(invalid_filter_policy_error("unsupported filter literal.")),
    }
}

fn evaluate_attribute_conditions(
    conditions: &[FilterCondition],
    attribute: Option<&MessageAttributeValue>,
) -> bool {
    let field_exists = attribute.is_some();
    let Some(attribute) = attribute else {
        return conditions.iter().any(|condition| {
            evaluate_filter_condition(condition, None, false)
        });
    };

    attribute_candidate_values(attribute).into_iter().any(|value| {
        conditions.iter().any(|condition| {
            evaluate_filter_condition(condition, Some(&value), field_exists)
        })
    })
}

fn attribute_candidate_values(
    attribute: &MessageAttributeValue,
) -> Vec<Value> {
    if attribute.data_type == "String.Array" {
        return serde_json::from_str::<Value>(&attribute.value)
            .ok()
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
    }

    if attribute.data_type == "Number"
        && let Ok(number) = serde_json::from_str::<Value>(&attribute.value)
        && matches!(number, Value::Number(_))
    {
        return vec![number];
    }

    vec![Value::String(attribute.value.clone())]
}

fn evaluate_body_filter_policy(
    policy: &BTreeMap<String, FilterNode>,
    payload: Option<&Value>,
) -> bool {
    let object = payload.and_then(Value::as_object);

    policy.iter().all(|(key, node)| {
        evaluate_filter_node(node, object.and_then(|object| object.get(key)))
    })
}

fn evaluate_filter_node(node: &FilterNode, value: Option<&Value>) -> bool {
    match node {
        FilterNode::Conditions(conditions) => {
            evaluate_filter_conditions(conditions, value)
        }
        FilterNode::Nested(children) => {
            let object = value.and_then(Value::as_object);

            children.iter().all(|(key, child)| {
                evaluate_filter_node(
                    child,
                    object.and_then(|object| object.get(key)),
                )
            })
        }
    }
}

fn evaluate_filter_conditions(
    conditions: &[FilterCondition],
    value: Option<&Value>,
) -> bool {
    let field_exists = value.is_some();

    match value {
        Some(Value::Array(values)) => values.iter().any(|value| {
            conditions.iter().any(|condition| {
                evaluate_filter_condition(condition, Some(value), field_exists)
            })
        }),
        _ => conditions.iter().any(|condition| {
            evaluate_filter_condition(condition, value, field_exists)
        }),
    }
}

fn evaluate_filter_condition(
    condition: &FilterCondition,
    value: Option<&Value>,
    field_exists: bool,
) -> bool {
    match condition {
        FilterCondition::Exists(expected) => *expected == field_exists,
        _ => {
            let Some(value) = value else {
                return false;
            };
            match condition {
                FilterCondition::AnythingBut(values) => {
                    values.iter().all(|literal| {
                        !filter_literal_matches_value(literal, value)
                    })
                }
                FilterCondition::Exact(expected) => {
                    filter_literal_matches_value(expected, value)
                }
                FilterCondition::Numeric(comparisons) => {
                    filter_numeric_matches(comparisons, value)
                }
                FilterCondition::Prefix(prefix) => value
                    .as_str()
                    .is_some_and(|value| value.starts_with(prefix)),
                FilterCondition::Exists(_) => true,
            }
        }
    }
}

fn filter_literal_matches_value(
    literal: &FilterLiteral,
    value: &Value,
) -> bool {
    match literal {
        FilterLiteral::Bool(expected) => value.as_bool() == Some(*expected),
        FilterLiteral::Null => value.is_null(),
        FilterLiteral::Number(expected) => value_as_f64(value)
            .zip(expected.parse::<f64>().ok())
            .is_some_and(|(actual, expected)| actual == expected),
        FilterLiteral::String(expected) => value.as_str() == Some(expected),
    }
}

fn filter_numeric_matches(
    comparisons: &[NumericComparison],
    value: &Value,
) -> bool {
    let Some(actual) = value_as_f64(value) else {
        return false;
    };

    comparisons.iter().all(|comparison| match comparison {
        NumericComparison::Eq(expected) => expected
            .parse::<f64>()
            .ok()
            .is_some_and(|expected| actual == expected),
        NumericComparison::Gt(expected) => expected
            .parse::<f64>()
            .ok()
            .is_some_and(|expected| actual > expected),
        NumericComparison::Gte(expected) => expected
            .parse::<f64>()
            .ok()
            .is_some_and(|expected| actual >= expected),
        NumericComparison::Lt(expected) => expected
            .parse::<f64>()
            .ok()
            .is_some_and(|expected| actual < expected),
        NumericComparison::Lte(expected) => expected
            .parse::<f64>()
            .ok()
            .is_some_and(|expected| actual <= expected),
    })
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(number) => number.parse::<f64>().ok(),
        _ => None,
    }
}

fn response_subscription_arn(
    state: &SubscriptionRecordState,
    subscription_arn: &Arn,
    return_subscription_arn: bool,
) -> String {
    match state {
        SubscriptionRecordState::Confirmed => subscription_arn.to_string(),
        SubscriptionRecordState::PendingConfirmation { .. }
            if return_subscription_arn =>
        {
            subscription_arn.to_string()
        }
        SubscriptionRecordState::PendingConfirmation { .. } => {
            SUBSCRIBE_PENDING_CONFIRMATION_ARN.to_owned()
        }
    }
}
