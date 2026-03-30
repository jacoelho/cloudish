use crate::attributes::validate_queue_name;
use crate::errors::SqsError;
use aws::{AccountId, Arn, RegionId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqsScope {
    account_id: AccountId,
    region: RegionId,
}

impl SqsScope {
    pub fn new(account_id: AccountId, region: RegionId) -> Self {
        Self { account_id, region }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqsQueueIdentity {
    account_id: AccountId,
    queue_name: String,
    region: RegionId,
}

impl SqsQueueIdentity {
    /// Creates a scoped queue identity from explicit account, region, and queue name parts.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::InvalidParameterValue`] when the queue name is not valid for SQS.
    pub fn new(
        account_id: AccountId,
        region: RegionId,
        queue_name: impl Into<String>,
    ) -> Result<Self, SqsError> {
        let queue_name = queue_name.into();
        validate_queue_name(&queue_name)?;

        Ok(Self { account_id, queue_name, region })
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }

    /// Rebuilds a scoped queue identity from an SQS queue ARN.
    ///
    /// # Errors
    ///
    /// Returns [`SqsError::InvalidParameterValue`] when the ARN is missing its
    /// account, region, or queue resource information.
    pub fn from_arn(arn: &Arn) -> Result<Self, SqsError> {
        let account_id = arn.account_id().cloned().ok_or_else(|| {
            SqsError::InvalidParameterValue {
                message: "Queue ARN must include an AWS account identifier."
                    .to_owned(),
            }
        })?;
        let region = arn.region().cloned().ok_or_else(|| {
            SqsError::InvalidParameterValue {
                message: "Queue ARN must include an AWS region.".to_owned(),
            }
        })?;
        let aws::ArnResource::Sqs(resource) = arn.resource() else {
            return Err(SqsError::InvalidParameterValue {
                message: "Queue ARN must reference an SQS queue.".to_owned(),
            });
        };

        Self::new(account_id, region, resource.queue_name())
    }
}
