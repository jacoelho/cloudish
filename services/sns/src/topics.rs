use crate::SnsRegistry;
use crate::errors::SnsError;
use crate::pagination::{PaginatedList, PaginationContext, paginate};
use crate::scope::SnsScope;
use crate::subscriptions::SubscriptionRecordState;
use aws::{AccountId, Arn, ArnResource, Partition, RegionId, ServiceName};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicInput {
    pub attributes: BTreeMap<String, String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TopicKey {
    pub(crate) account_id: AccountId,
    pub(crate) name: String,
    pub(crate) region: RegionId,
}

impl TopicKey {
    pub(crate) fn new(scope: &SnsScope, name: &str) -> Result<Self, SnsError> {
        validate_topic_name(name, false)?;

        Ok(Self {
            account_id: scope.account_id().clone(),
            name: name.to_owned(),
            region: scope.region().clone(),
        })
    }

    pub(crate) fn from_publish_target(
        topic_arn: &Arn,
    ) -> Result<Self, SnsError> {
        Self::from_arn(topic_arn, PublishTargetKind::TargetArn)
    }

    pub(crate) fn from_topic_arn(topic_arn: &Arn) -> Result<Self, SnsError> {
        Self::from_arn(topic_arn, PublishTargetKind::TopicArn)
    }

    fn from_arn(arn: &Arn, kind: PublishTargetKind) -> Result<Self, SnsError> {
        if arn.service() != ServiceName::Sns {
            return Err(match kind {
                PublishTargetKind::TargetArn => SnsError::InvalidParameter {
                    message: "Invalid parameter: TargetArn".to_owned(),
                },
                PublishTargetKind::TopicArn => SnsError::InvalidParameter {
                    message: "Invalid parameter: TopicArn".to_owned(),
                },
            });
        }
        let Some(region) = arn.region().cloned() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: TopicArn".to_owned(),
            });
        };
        let Some(account_id) = arn.account_id().cloned() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: TopicArn".to_owned(),
            });
        };
        let ArnResource::Generic(resource) = arn.resource() else {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: TopicArn".to_owned(),
            });
        };
        if resource.contains(':')
            || resource.contains('/')
            || resource.is_empty()
        {
            return Err(match kind {
                PublishTargetKind::TargetArn => SnsError::InvalidParameter {
                    message: "Invalid parameter: TargetArn".to_owned(),
                },
                PublishTargetKind::TopicArn => SnsError::InvalidParameter {
                    message: "Invalid parameter: TopicArn".to_owned(),
                },
            });
        }
        validate_topic_name(resource, resource.ends_with(".fifo"))?;

        Ok(Self { account_id, name: resource.to_owned(), region })
    }

    pub(crate) fn is_fifo_topic(&self, topic: &TopicRecord) -> bool {
        topic
            .attributes
            .get("FifoTopic")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            || self.name.ends_with(".fifo")
    }

    pub(crate) fn scope(&self) -> SnsScope {
        SnsScope::new(self.account_id.clone(), self.region.clone())
    }

    pub(crate) fn subscription_arn(&self, subscription_id: &str) -> Arn {
        Arn::trusted_new(
            Partition::aws(),
            ServiceName::Sns,
            Some(self.region.clone()),
            Some(self.account_id.clone()),
            ArnResource::Generic(format!("{}:{subscription_id}", self.name)),
        )
    }

    pub(crate) fn topic_arn(&self) -> Arn {
        Arn::trusted_new(
            Partition::aws(),
            ServiceName::Sns,
            Some(self.region.clone()),
            Some(self.account_id.clone()),
            ArnResource::Generic(self.name.clone()),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicRecord {
    pub(crate) attributes: BTreeMap<String, String>,
    pub(crate) name: String,
    pub(crate) tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishTargetKind {
    TargetArn,
    TopicArn,
}

pub(crate) fn normalize_topic_attributes(
    name: &str,
    mut attributes: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, SnsError> {
    let fifo_requested = attributes
        .get("FifoTopic")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    validate_topic_name(name, fifo_requested)?;

    if fifo_requested {
        attributes.insert("FifoTopic".to_owned(), "true".to_owned());
    } else {
        attributes.remove("FifoTopic");
    }

    Ok(attributes)
}

pub(crate) fn validate_topic_name(
    name: &str,
    fifo_requested: bool,
) -> Result<(), SnsError> {
    let Some(base_name) = name.strip_suffix(".fifo").or(Some(name)) else {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: Topic Name".to_owned(),
        });
    };
    let is_fifo_name = name.ends_with(".fifo");
    if base_name.is_empty()
        || name.len() > 256
        || base_name.chars().any(|character| {
            !character.is_ascii_alphanumeric()
                && character != '-'
                && character != '_'
        })
        || (is_fifo_name && !fifo_requested)
        || (!is_fifo_name && fifo_requested)
    {
        return Err(SnsError::InvalidParameter {
            message: "Invalid parameter: Topic Name".to_owned(),
        });
    }

    Ok(())
}

pub(crate) fn not_found_topic_error() -> SnsError {
    SnsError::NotFound { message: "Topic does not exist".to_owned() }
}

pub(crate) fn resource_not_found_topic_error() -> SnsError {
    SnsError::ResourceNotFound { message: "Topic does not exist".to_owned() }
}

impl SnsRegistry {
    pub(crate) fn create_topic(
        &mut self,
        scope: &SnsScope,
        input: CreateTopicInput,
    ) -> Result<Arn, SnsError> {
        let normalized_attributes =
            normalize_topic_attributes(&input.name, input.attributes)?;
        let topic_key = TopicKey::new(scope, &input.name)?;

        if let Some(existing) = self.topics.get(&topic_key) {
            if existing.attributes != normalized_attributes {
                return Err(SnsError::InvalidParameter {
                    message: "Invalid parameter: Attributes Reason: Topic \
                              already exists with different attributes"
                        .to_owned(),
                });
            }

            return Ok(topic_key.topic_arn());
        }

        self.topics.insert(
            topic_key.clone(),
            TopicRecord {
                attributes: normalized_attributes,
                name: input.name,
                tags: BTreeMap::new(),
            },
        );

        Ok(topic_key.topic_arn())
    }

    pub(crate) fn delete_topic(
        &mut self,
        topic_arn: &Arn,
    ) -> Result<(), SnsError> {
        let topic_key = TopicKey::from_topic_arn(topic_arn)?;
        self.topics.remove(&topic_key);

        let removed = self
            .subscriptions
            .iter()
            .filter(|(_, subscription)| subscription.topic == topic_key)
            .map(|(subscription_arn, subscription)| {
                (subscription_arn.clone(), subscription.state.clone())
            })
            .collect::<Vec<_>>();

        for (subscription_arn, _state) in removed {
            self.subscriptions.remove(&subscription_arn);
            self.remove_tokens_for_subscription(&subscription_arn);
        }

        Ok(())
    }

    pub(crate) fn list_topics(&self, scope: &SnsScope) -> Vec<Arn> {
        self.topics
            .keys()
            .filter(|topic| topic.scope() == scope.clone())
            .map(TopicKey::topic_arn)
            .collect()
    }

    pub(crate) fn list_topics_page(
        &self,
        scope: &SnsScope,
        next_token: Option<&str>,
    ) -> Result<PaginatedList<Arn>, SnsError> {
        let topics = self.list_topics(scope);

        paginate(
            &topics,
            &PaginationContext::Scope {
                operation: "ListTopics",
                scope: scope.clone(),
            },
            next_token,
        )
    }

    pub(crate) fn get_topic_attributes(
        &self,
        topic_arn: &Arn,
    ) -> Result<BTreeMap<String, String>, SnsError> {
        let topic_key = TopicKey::from_topic_arn(topic_arn)?;
        let topic = self.topic_record(&topic_key)?;
        let mut attributes = topic.attributes.clone();
        let (confirmed, pending) = self.subscription_counts(&topic_key);

        attributes
            .insert("Owner".to_owned(), topic_key.account_id.to_string());
        attributes.insert(
            "SubscriptionsConfirmed".to_owned(),
            confirmed.to_string(),
        );
        attributes.insert("SubscriptionsDeleted".to_owned(), "0".to_owned());
        attributes
            .insert("SubscriptionsPending".to_owned(), pending.to_string());
        attributes
            .insert("TopicArn".to_owned(), topic_key.topic_arn().to_string());

        Ok(attributes)
    }

    pub(crate) fn set_topic_attributes(
        &mut self,
        topic_arn: &Arn,
        attribute_name: &str,
        attribute_value: Option<&str>,
    ) -> Result<(), SnsError> {
        if attribute_name == "FifoTopic" {
            return Err(SnsError::InvalidParameter {
                message: "Invalid parameter: AttributeName".to_owned(),
            });
        }

        let topic_key = TopicKey::from_topic_arn(topic_arn)?;
        let topic = self.topic_record_mut(&topic_key)?;

        match attribute_value {
            Some(attribute_value) => {
                topic.attributes.insert(
                    attribute_name.to_owned(),
                    attribute_value.to_owned(),
                );
            }
            None => {
                topic.attributes.remove(attribute_name);
            }
        }

        Ok(())
    }

    pub(crate) fn tag_resource(
        &mut self,
        topic_arn: &Arn,
        tags: BTreeMap<String, String>,
    ) -> Result<(), SnsError> {
        let topic = TopicKey::from_topic_arn(topic_arn)?;
        let record = self
            .topics
            .get_mut(&topic)
            .ok_or_else(resource_not_found_topic_error)?;

        for (key, value) in tags {
            record.tags.insert(key, value);
        }

        Ok(())
    }

    pub(crate) fn untag_resource(
        &mut self,
        topic_arn: &Arn,
        tag_keys: &[String],
    ) -> Result<(), SnsError> {
        let topic = TopicKey::from_topic_arn(topic_arn)?;
        let record = self
            .topics
            .get_mut(&topic)
            .ok_or_else(resource_not_found_topic_error)?;

        for tag_key in tag_keys {
            record.tags.remove(tag_key);
        }

        Ok(())
    }

    pub(crate) fn list_tags_for_resource(
        &self,
        topic_arn: &Arn,
    ) -> Result<BTreeMap<String, String>, SnsError> {
        let topic = TopicKey::from_topic_arn(topic_arn)?;

        Ok(self
            .topics
            .get(&topic)
            .ok_or_else(resource_not_found_topic_error)?
            .tags
            .clone())
    }

    fn subscription_counts(&self, topic: &TopicKey) -> (usize, usize) {
        self.subscriptions
            .values()
            .filter(|subscription| subscription.topic == *topic)
            .fold((0, 0), |(confirmed, pending), subscription| {
                if matches!(
                    subscription.state,
                    SubscriptionRecordState::Confirmed
                ) {
                    (confirmed.saturating_add(1), pending)
                } else {
                    (confirmed, pending.saturating_add(1))
                }
            })
    }

    pub(crate) fn topic_record(
        &self,
        topic: &TopicKey,
    ) -> Result<&TopicRecord, SnsError> {
        self.topics.get(topic).ok_or_else(not_found_topic_error)
    }

    pub(crate) fn topic_record_mut(
        &mut self,
        topic: &TopicKey,
    ) -> Result<&mut TopicRecord, SnsError> {
        self.topics.get_mut(topic).ok_or_else(not_found_topic_error)
    }
}
