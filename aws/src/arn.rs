use crate::{
    AccountId, AccountIdError, RegionId, RegionIdError, ServiceName,
    ServiceNameError,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct Partition(String);

impl Partition {
    pub fn aws() -> Self {
        Self("aws".to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Partition {
    type Err = PartitionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(PartitionError::Empty);
        }

        let last_index = value.len() - 1;
        let mut previous_was_separator = false;

        for (index, character) in value.char_indices() {
            match character {
                'a'..='z' | '0'..='9' => previous_was_separator = false,
                '-' => {
                    if index == 0
                        || index == last_index
                        || previous_was_separator
                    {
                        return Err(PartitionError::InvalidSeparator {
                            index,
                        });
                    }

                    previous_was_separator = true;
                }
                _ => {
                    return Err(PartitionError::InvalidCharacter {
                        index,
                        character,
                    });
                }
            }
        }

        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for Partition {
    type Error = PartitionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Partition> for String {
    fn from(value: Partition) -> Self {
        value.0
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct Arn {
    partition: Partition,
    service: ServiceName,
    region: Option<RegionId>,
    account_id: Option<AccountId>,
    resource: ArnResource,
}

impl Arn {
    /// Constructs a typed ARN from validated components.
    ///
    /// # Errors
    ///
    /// Returns `ArnError` when the resource shape is invalid for the selected
    /// service or when the service requires missing scope fields.
    pub fn new(
        partition: Partition,
        service: ServiceName,
        region: Option<RegionId>,
        account_id: Option<AccountId>,
        resource: ArnResource,
    ) -> Result<Self, ArnError> {
        resource.validate_for(service)?;

        if service == ServiceName::Sqs {
            if region.is_none() {
                return Err(ArnError::MissingRegion { service });
            }

            if account_id.is_none() {
                return Err(ArnError::MissingAccountId { service });
            }
        }

        Ok(Self { partition, service, region, account_id, resource })
    }

    /// Constructs an ARN from components that have already been validated by a
    /// higher-level owner.
    pub fn trusted_new(
        partition: Partition,
        service: ServiceName,
        region: Option<RegionId>,
        account_id: Option<AccountId>,
        resource: ArnResource,
    ) -> Self {
        Self { partition, service, region, account_id, resource }
    }

    /// Constructs an S3 bucket ARN.
    ///
    /// # Errors
    ///
    /// Returns `ArnError` when the bucket name cannot be represented as an S3
    /// ARN resource.
    pub fn s3_bucket(bucket: impl Into<String>) -> Result<Self, ArnError> {
        Self::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::S3(S3ArnResource::Bucket { bucket: bucket.into() }),
        )
    }

    /// Constructs an S3 object ARN.
    ///
    /// # Errors
    ///
    /// Returns `ArnError` when the bucket or object key cannot be represented
    /// as an S3 ARN resource.
    pub fn s3_object(
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, ArnError> {
        Self::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::S3(S3ArnResource::Object {
                bucket: bucket.into(),
                key: key.into(),
            }),
        )
    }

    /// Constructs an SQS queue ARN.
    ///
    /// # Errors
    ///
    /// Returns `ArnError` when the queue name is invalid for an SQS ARN.
    pub fn sqs(
        region: RegionId,
        account_id: AccountId,
        queue_name: impl Into<String>,
    ) -> Result<Self, ArnError> {
        Self::new(
            Partition::aws(),
            ServiceName::Sqs,
            Some(region),
            Some(account_id),
            ArnResource::Sqs(SqsArnResource::new(queue_name)?),
        )
    }

    pub fn partition(&self) -> &Partition {
        &self.partition
    }

    pub fn service(&self) -> ServiceName {
        self.service
    }

    pub fn region(&self) -> Option<&RegionId> {
        self.region.as_ref()
    }

    pub fn account_id(&self) -> Option<&AccountId> {
        self.account_id.as_ref()
    }

    pub fn resource(&self) -> &ArnResource {
        &self.resource
    }
}

impl fmt::Display for Arn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "arn:{}:{}:{}:{}:{}",
            self.partition,
            self.service,
            self.region.as_ref().map(RegionId::as_str).unwrap_or(""),
            self.account_id.as_ref().map(AccountId::as_str).unwrap_or(""),
            self.resource
        )
    }
}

impl FromStr for Arn {
    type Err = ArnError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let sections: Vec<_> = value.splitn(6, ':').collect();
        let [prefix, partition, service, region, account_id, resource] =
            sections.as_slice()
        else {
            return Err(ArnError::InvalidSectionCount {
                actual: sections.len(),
            });
        };

        if *prefix != "arn" {
            return Err(ArnError::InvalidPrefix {
                actual: (*prefix).to_owned(),
            });
        }

        let partition =
            partition.parse().map_err(ArnError::InvalidPartition)?;
        let service = service.parse().map_err(ArnError::InvalidServiceName)?;
        let region = if region.is_empty() {
            None
        } else {
            Some(region.parse().map_err(ArnError::InvalidRegion)?)
        };
        let account_id = if account_id.is_empty() {
            None
        } else {
            Some(account_id.parse().map_err(ArnError::InvalidAccountId)?)
        };
        let resource = ArnResource::parse(service, resource)?;

        Self::new(partition, service, region, account_id, resource)
    }
}

impl TryFrom<String> for Arn {
    type Error = ArnError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Arn> for String {
    fn from(value: Arn) -> Self {
        value.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ArnResource {
    S3(S3ArnResource),
    Sqs(SqsArnResource),
    Generic(String),
}

impl ArnResource {
    fn parse(service: ServiceName, resource: &str) -> Result<Self, ArnError> {
        if resource.is_empty() {
            return Err(ArnError::MissingResource);
        }

        match service {
            ServiceName::S3 => Ok(Self::S3(resource.parse()?)),
            ServiceName::Sqs => Ok(Self::Sqs(resource.parse()?)),
            _ => Ok(Self::Generic(resource.to_owned())),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::S3(_) => "s3",
            Self::Sqs(_) => "sqs",
            Self::Generic(_) => "generic",
        }
    }

    fn validate_for(&self, service: ServiceName) -> Result<(), ArnError> {
        match (service, self) {
            (ServiceName::S3, Self::S3(resource)) => resource.validate(),
            (ServiceName::Sqs, Self::Sqs(resource)) => resource.validate(),
            (_, Self::Generic(resource)) if resource.is_empty() => {
                Err(ArnError::MissingResource)
            }
            (_, Self::Generic(_)) => Ok(()),
            (_, Self::S3(_)) | (_, Self::Sqs(_)) => {
                Err(ArnError::ResourceServiceMismatch {
                    service,
                    resource_kind: self.kind_name(),
                })
            }
        }
    }
}

impl fmt::Display for ArnResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::S3(resource) => write!(formatter, "{resource}"),
            Self::Sqs(resource) => write!(formatter, "{resource}"),
            Self::Generic(resource) => formatter.write_str(resource),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum S3ArnResource {
    Bucket { bucket: String },
    Object { bucket: String, key: String },
    Raw(String),
}

impl S3ArnResource {
    fn validate(&self) -> Result<(), ArnError> {
        match self {
            Self::Bucket { bucket } if bucket.is_empty() => {
                Err(ArnError::InvalidS3Resource(
                    S3ArnResourceError::MissingBucketName,
                ))
            }
            Self::Object { bucket, key: _ } if bucket.is_empty() => {
                Err(ArnError::InvalidS3Resource(
                    S3ArnResourceError::MissingBucketName,
                ))
            }
            Self::Object { bucket: _, key } if key.is_empty() => {
                Err(ArnError::InvalidS3Resource(
                    S3ArnResourceError::MissingObjectKey,
                ))
            }
            Self::Raw(value) if value.is_empty() => {
                Err(ArnError::InvalidS3Resource(S3ArnResourceError::Empty))
            }
            _ => Ok(()),
        }
    }
}

impl fmt::Display for S3ArnResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bucket { bucket } => formatter.write_str(bucket),
            Self::Object { bucket, key } => {
                write!(formatter, "{bucket}/{key}")
            }
            Self::Raw(value) => formatter.write_str(value),
        }
    }
}

impl FromStr for S3ArnResource {
    type Err = S3ArnResourceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(S3ArnResourceError::Empty);
        }

        if value.contains(':') {
            return Ok(Self::Raw(value.to_owned()));
        }

        if let Some((bucket, key)) = value.split_once('/') {
            if bucket.is_empty() {
                return Err(S3ArnResourceError::MissingBucketName);
            }

            if key.is_empty() {
                return Err(S3ArnResourceError::MissingObjectKey);
            }

            return Ok(Self::Object {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
            });
        }

        Ok(Self::Bucket { bucket: value.to_owned() })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SqsArnResource {
    queue_name: String,
}

impl SqsArnResource {
    /// Constructs an SQS ARN resource from a queue name.
    ///
    /// # Errors
    ///
    /// Returns `ArnError` when the queue name is empty or contains invalid
    /// separators.
    pub fn new(queue_name: impl Into<String>) -> Result<Self, ArnError> {
        let queue_name = queue_name.into();
        let resource: Self = queue_name.parse()?;
        Ok(resource)
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    fn validate(&self) -> Result<(), ArnError> {
        if self.queue_name.is_empty() {
            return Err(ArnError::InvalidSqsResource(
                SqsArnResourceError::EmptyQueueName,
            ));
        }

        if let Some(character) = self
            .queue_name
            .chars()
            .find(|character| matches!(character, ':' | '/'))
        {
            return Err(ArnError::InvalidSqsResource(
                SqsArnResourceError::InvalidSeparator { character },
            ));
        }

        Ok(())
    }
}

impl fmt::Display for SqsArnResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.queue_name())
    }
}

impl FromStr for SqsArnResource {
    type Err = SqsArnResourceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(SqsArnResourceError::EmptyQueueName);
        }

        if let Some(character) =
            value.chars().find(|character| matches!(character, ':' | '/'))
        {
            return Err(SqsArnResourceError::InvalidSeparator { character });
        }

        Ok(Self { queue_name: value.to_owned() })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PartitionError {
    #[error("partition cannot be empty")]
    Empty,
    #[error("partition contains an invalid '-' separator at index {index}")]
    InvalidSeparator { index: usize },
    #[error(
        "partition must contain only lowercase ASCII letters, digits, and '-', found {character:?} at index {index}"
    )]
    InvalidCharacter { index: usize, character: char },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArnError {
    #[error("ARN must start with \"arn\", found {actual:?}")]
    InvalidPrefix { actual: String },
    #[error("ARN must have 6 colon-delimited sections, found {actual}")]
    InvalidSectionCount { actual: usize },
    #[error("invalid ARN partition: {0}")]
    InvalidPartition(PartitionError),
    #[error("invalid ARN service: {0}")]
    InvalidServiceName(ServiceNameError),
    #[error("invalid ARN region: {0}")]
    InvalidRegion(RegionIdError),
    #[error("invalid ARN account id: {0}")]
    InvalidAccountId(AccountIdError),
    #[error("ARN resource section cannot be empty")]
    MissingResource,
    #[error("ARN for service {service} requires a region segment")]
    MissingRegion { service: ServiceName },
    #[error("ARN for service {service} requires an account id segment")]
    MissingAccountId { service: ServiceName },
    #[error(
        "resource kind {resource_kind:?} does not match ARN service {service}"
    )]
    ResourceServiceMismatch {
        service: ServiceName,
        resource_kind: &'static str,
    },
    #[error("invalid S3 ARN resource: {0}")]
    InvalidS3Resource(S3ArnResourceError),
    #[error("invalid SQS ARN resource: {0}")]
    InvalidSqsResource(SqsArnResourceError),
}

impl From<S3ArnResourceError> for ArnError {
    fn from(value: S3ArnResourceError) -> Self {
        Self::InvalidS3Resource(value)
    }
}

impl From<SqsArnResourceError> for ArnError {
    fn from(value: SqsArnResourceError) -> Self {
        Self::InvalidSqsResource(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum S3ArnResourceError {
    #[error("S3 ARN resource cannot be empty")]
    Empty,
    #[error("S3 ARN resource must include a bucket name")]
    MissingBucketName,
    #[error("S3 object ARN resource must include an object key")]
    MissingObjectKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SqsArnResourceError {
    #[error("SQS ARN resource must include a queue name")]
    EmptyQueueName,
    #[error("SQS ARN queue name cannot contain {character:?}")]
    InvalidSeparator { character: char },
}

#[cfg(test)]
mod tests {
    use super::{
        Arn, ArnError, ArnResource, Partition, PartitionError, S3ArnResource,
        S3ArnResourceError, SqsArnResource, SqsArnResourceError,
    };
    use crate::{AccountId, RegionId, ServiceName, ServiceNameError};

    #[test]
    fn parse_partition_identifiers() {
        let partition: Partition =
            "aws-us-gov".parse().expect("partition should parse");

        assert_eq!(partition.as_str(), "aws-us-gov");
        assert_eq!(Partition::aws().to_string(), "aws");
    }

    #[test]
    fn partition_and_arn_support_string_conversions() {
        let partition = Partition::try_from("aws".to_owned())
            .expect("partition should convert from String");
        let arn = Arn::try_from(
            "arn:aws:lambda:eu-west-2:123456789012:function:demo".to_owned(),
        )
        .expect("ARN should convert from String");

        assert_eq!(String::from(partition.clone()), "aws");
        assert_eq!(partition.as_str(), "aws");
        assert_eq!(String::from(arn.clone()), arn.to_string());
        assert_eq!(arn.partition().as_str(), "aws");
        assert_eq!(arn.resource().to_string(), "function:demo");
    }

    #[test]
    fn reject_invalid_partition_identifiers() {
        let error = "Aws"
            .parse::<Partition>()
            .expect_err("partition with uppercase must fail");

        assert_eq!(
            error,
            PartitionError::InvalidCharacter { index: 0, character: 'A' }
        );
        assert_eq!(
            error.to_string(),
            "partition must contain only lowercase ASCII letters, digits, and '-', found 'A' at index 0"
        );
    }

    #[test]
    fn reject_empty_and_badly_separated_partitions() {
        let empty =
            "".parse::<Partition>().expect_err("empty partition must fail");
        let separator = "aws-"
            .parse::<Partition>()
            .expect_err("partition with trailing separator must fail");

        assert_eq!(empty, PartitionError::Empty);
        assert_eq!(separator, PartitionError::InvalidSeparator { index: 3 });
    }

    #[test]
    fn build_and_round_trip_sqs_arn() {
        let arn = Arn::sqs(
            "eu-west-2".parse::<RegionId>().expect("region should parse"),
            "123456789012".parse::<AccountId>().expect("account should parse"),
            "orders",
        )
        .expect("SQS ARN should build");

        assert_eq!(
            arn.to_string(),
            "arn:aws:sqs:eu-west-2:123456789012:orders"
        );

        let parsed: Arn =
            arn.to_string().parse().expect("serialized ARN should parse");

        assert_eq!(parsed.service(), ServiceName::Sqs);
        assert_eq!(
            parsed.region().expect("region should exist").as_str(),
            "eu-west-2"
        );
        assert_eq!(
            parsed.account_id().expect("account should exist").as_str(),
            "123456789012"
        );
        assert_eq!(parsed.resource(), arn.resource());
    }

    #[test]
    fn parse_fifo_sqs_arn_into_typed_components() {
        let arn: Arn = "arn:aws:sqs:eu-west-2:123456789012:orders.fifo"
            .parse()
            .expect("FIFO SQS ARN should parse");

        assert_eq!(arn.partition().as_str(), "aws");
        assert_eq!(arn.service(), ServiceName::Sqs);
        assert_eq!(
            arn.region().expect("region should exist").as_str(),
            "eu-west-2"
        );
        assert_eq!(
            arn.account_id().expect("account should exist").as_str(),
            "123456789012"
        );
        assert_eq!(
            arn.resource(),
            &ArnResource::Sqs(
                "orders.fifo"
                    .parse::<SqsArnResource>()
                    .expect("FIFO queue resource should parse")
            )
        );
    }

    #[test]
    fn parse_and_round_trip_s3_bucket_and_object_arns() {
        let bucket_arn: Arn = "arn:aws:s3:::demo-bucket"
            .parse()
            .expect("bucket ARN should parse");
        let object_arn = Arn::s3_object("demo-bucket", "objects/key.txt")
            .expect("object ARN should build");

        assert_eq!(bucket_arn.to_string(), "arn:aws:s3:::demo-bucket");
        assert_eq!(
            object_arn.to_string(),
            "arn:aws:s3:::demo-bucket/objects/key.txt"
        );
        assert_eq!(bucket_arn.service(), ServiceName::S3);
        assert_eq!(bucket_arn.region(), None);
        assert_eq!(bucket_arn.account_id(), None);
    }

    #[test]
    fn build_s3_bucket_and_parse_s3_resource_shapes() {
        let bucket_arn =
            Arn::s3_bucket("demo-bucket").expect("bucket ARN should build");
        let object = "demo-bucket/path/key.txt"
            .parse::<S3ArnResource>()
            .expect("S3 object resource should parse");
        let empty = ""
            .parse::<S3ArnResource>()
            .expect_err("empty S3 resource must fail");
        let missing_bucket = "/key"
            .parse::<S3ArnResource>()
            .expect_err("S3 object resource without bucket must fail");
        let missing_key = "bucket/"
            .parse::<S3ArnResource>()
            .expect_err("S3 object resource without key must fail");

        assert_eq!(bucket_arn.partition().as_str(), "aws");
        assert_eq!(bucket_arn.resource().to_string(), "demo-bucket");
        assert_eq!(
            object,
            S3ArnResource::Object {
                bucket: "demo-bucket".to_owned(),
                key: "path/key.txt".to_owned(),
            }
        );
        assert_eq!(empty, S3ArnResourceError::Empty);
        assert_eq!(missing_bucket, S3ArnResourceError::MissingBucketName);
        assert_eq!(missing_key, S3ArnResourceError::MissingObjectKey);
    }

    #[test]
    fn preserve_raw_s3_resource_shapes() {
        let arn: Arn = "arn:aws:s3:eu-west-2:123456789012:accesspoint:demo"
            .parse()
            .expect("raw S3 ARN should parse");

        assert_eq!(
            arn.resource(),
            &ArnResource::S3(S3ArnResource::Raw(
                "accesspoint:demo".to_owned()
            ))
        );
        assert_eq!(
            arn.to_string(),
            "arn:aws:s3:eu-west-2:123456789012:accesspoint:demo"
        );
    }

    #[test]
    fn parse_generic_service_arn_resources() {
        let arn: Arn = "arn:aws:lambda:eu-west-2:123456789012:function:demo"
            .parse()
            .expect("generic ARN should parse");

        assert_eq!(arn.service(), ServiceName::Lambda);
        assert_eq!(
            arn.resource(),
            &ArnResource::Generic("function:demo".to_owned())
        );
        assert_eq!(
            arn.to_string(),
            "arn:aws:lambda:eu-west-2:123456789012:function:demo"
        );
    }

    #[test]
    fn reject_invalid_arn_prefix_and_section_count() {
        let prefix_error = "not-arn:aws:sqs:eu-west-2:123456789012:orders"
            .parse::<Arn>()
            .expect_err("ARN prefix must be validated");
        let section_error = "arn:aws:sqs:eu-west-2:123456789012"
            .parse::<Arn>()
            .expect_err("ARN section count must be validated");

        assert_eq!(
            prefix_error,
            ArnError::InvalidPrefix { actual: "not-arn".to_owned() }
        );
        assert_eq!(section_error, ArnError::InvalidSectionCount { actual: 5 });
    }

    #[test]
    fn reject_arns_with_missing_resource_sections() {
        let error = "arn:aws:lambda:eu-west-2:123456789012:"
            .parse::<Arn>()
            .expect_err("missing ARN resource must fail");

        assert_eq!(error, ArnError::MissingResource);
    }

    #[test]
    fn reject_arns_with_invalid_service_identifiers() {
        let error = "arn:aws:ec2:eu-west-2:123456789012:instance/i-123"
            .parse::<Arn>()
            .expect_err("unsupported service should fail");

        assert_eq!(
            error,
            ArnError::InvalidServiceName(ServiceNameError::Unsupported {
                value: "ec2".to_owned(),
            })
        );
    }

    #[test]
    fn reject_arns_with_invalid_region_and_account_ids() {
        let region_error = "arn:aws:sqs:invalid:123456789012:orders"
            .parse::<Arn>()
            .expect_err("invalid region must fail");
        let account_error = "arn:aws:sqs:eu-west-2:account:orders"
            .parse::<Arn>()
            .expect_err("invalid account must fail");

        assert_eq!(
            region_error,
            ArnError::InvalidRegion(crate::RegionIdError::MissingSeparator)
        );
        assert_eq!(
            account_error,
            ArnError::InvalidAccountId(crate::AccountIdError::InvalidLength {
                actual: 7,
            })
        );
    }

    #[test]
    fn reject_sqs_arns_missing_required_scope() {
        let missing_region = "arn:aws:sqs::123456789012:orders"
            .parse::<Arn>()
            .expect_err("SQS ARN without region must fail");
        let missing_account = "arn:aws:sqs:eu-west-2::orders"
            .parse::<Arn>()
            .expect_err("SQS ARN without account must fail");

        assert_eq!(
            missing_region,
            ArnError::MissingRegion { service: ServiceName::Sqs }
        );
        assert_eq!(
            missing_account,
            ArnError::MissingAccountId { service: ServiceName::Sqs }
        );
    }

    #[test]
    fn arn_resource_helpers_cover_kind_names() {
        let s3 = ArnResource::S3(S3ArnResource::Bucket {
            bucket: "demo".to_owned(),
        });
        let sqs = ArnResource::Sqs(
            "orders".parse::<SqsArnResource>().expect("valid resource"),
        );
        let generic = ArnResource::Generic("function:demo".to_owned());

        assert_eq!(s3.kind_name(), "s3");
        assert_eq!(sqs.kind_name(), "sqs");
        assert_eq!(generic.kind_name(), "generic");
        assert_eq!(generic.validate_for(ServiceName::Lambda), Ok(()));
        assert_eq!(
            ArnResource::Generic(String::new())
                .validate_for(ServiceName::Lambda),
            Err(ArnError::MissingResource)
        );
    }

    #[test]
    fn reject_invalid_s3_and_sqs_resources() {
        let s3_error = "arn:aws:s3:::bucket/"
            .parse::<Arn>()
            .expect_err("S3 object without key must fail");
        let sqs_error = "arn:aws:sqs:eu-west-2:123456789012:orders/extra"
            .parse::<Arn>()
            .expect_err("SQS ARN with slash must fail");

        assert_eq!(
            s3_error,
            ArnError::InvalidS3Resource(S3ArnResourceError::MissingObjectKey)
        );
        assert_eq!(
            sqs_error,
            ArnError::InvalidSqsResource(
                SqsArnResourceError::InvalidSeparator { character: '/' }
            )
        );
    }

    #[test]
    fn reject_invalid_constructed_s3_resources() {
        let empty_bucket = Arn::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::S3(S3ArnResource::Bucket { bucket: String::new() }),
        )
        .expect_err("empty S3 bucket must fail");
        let empty_object_bucket = Arn::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::S3(S3ArnResource::Object {
                bucket: String::new(),
                key: "key".to_owned(),
            }),
        )
        .expect_err("empty S3 object bucket must fail");
        let empty_object_key = Arn::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::S3(S3ArnResource::Object {
                bucket: "bucket".to_owned(),
                key: String::new(),
            }),
        )
        .expect_err("empty S3 object key must fail");
        let empty_raw = Arn::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::S3(S3ArnResource::Raw(String::new())),
        )
        .expect_err("empty S3 raw resource must fail");

        assert_eq!(
            empty_bucket,
            ArnError::InvalidS3Resource(S3ArnResourceError::MissingBucketName)
        );
        assert_eq!(
            empty_object_bucket,
            ArnError::InvalidS3Resource(S3ArnResourceError::MissingBucketName)
        );
        assert_eq!(
            empty_object_key,
            ArnError::InvalidS3Resource(S3ArnResourceError::MissingObjectKey)
        );
        assert_eq!(
            empty_raw,
            ArnError::InvalidS3Resource(S3ArnResourceError::Empty)
        );
    }

    #[test]
    fn sqs_resource_helpers_cover_validation_and_display() {
        let valid = SqsArnResource::new("orders")
            .expect("valid SQS resource should build");
        let empty_parse = ""
            .parse::<SqsArnResource>()
            .expect_err("empty SQS resource parse must fail");
        let empty_validate = SqsArnResource { queue_name: String::new() }
            .validate()
            .expect_err("empty SQS resource must fail validation");
        let invalid_validate =
            SqsArnResource { queue_name: "orders:dead".to_owned() }
                .validate()
                .expect_err(
                    "SQS resource with separators must fail validation",
                );

        assert_eq!(valid.queue_name(), "orders");
        assert_eq!(valid.to_string(), "orders");
        assert_eq!(empty_parse, SqsArnResourceError::EmptyQueueName);
        assert_eq!(
            empty_validate,
            ArnError::InvalidSqsResource(SqsArnResourceError::EmptyQueueName)
        );
        assert_eq!(
            invalid_validate,
            ArnError::InvalidSqsResource(
                SqsArnResourceError::InvalidSeparator { character: ':' }
            )
        );
    }

    #[test]
    fn reject_resource_service_mismatches() {
        let error = Arn::new(
            Partition::aws(),
            ServiceName::S3,
            None,
            None,
            ArnResource::Sqs(
                "orders"
                    .parse::<SqsArnResource>()
                    .expect("resource should parse"),
            ),
        )
        .expect_err("resource family must match ARN service");

        assert_eq!(
            error,
            ArnError::ResourceServiceMismatch {
                service: ServiceName::S3,
                resource_kind: "sqs",
            }
        );
    }
}
