use crate::KinesisError;
use aws::{Arn, ArnResource, ServiceName};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct KinesisStreamName(String);

impl KinesisStreamName {
    /// # Errors
    ///
    /// Returns [`KinesisError::InvalidArgument`] when the stream name is blank,
    /// too long, or contains unsupported characters.
    pub fn new(value: impl AsRef<str>) -> Result<Self, KinesisError> {
        validate_stream_name(value.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for KinesisStreamName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for KinesisStreamName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PartialEq<&str> for KinesisStreamName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for KinesisStreamName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl From<KinesisStreamName> for String {
    fn from(value: KinesisStreamName) -> Self {
        value.0
    }
}

impl TryFrom<String> for KinesisStreamName {
    type Error = KinesisError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KinesisStreamIdentifier {
    stream_arn: Option<Arn>,
    stream_name: Option<KinesisStreamName>,
}

impl KinesisStreamIdentifier {
    pub fn new(
        stream_arn: Option<Arn>,
        stream_name: Option<KinesisStreamName>,
    ) -> Self {
        Self { stream_arn, stream_name }
    }

    pub fn stream_arn(&self) -> Option<&Arn> {
        self.stream_arn.as_ref()
    }

    pub fn stream_name(&self) -> Option<&KinesisStreamName> {
        self.stream_name.as_ref()
    }
}

/// # Errors
///
/// Returns [`KinesisError::InvalidArgument`] when the stream name is blank,
/// too long, or contains unsupported characters.
pub fn validate_stream_name(value: &str) -> Result<String, KinesisError> {
    if value.is_empty() || value.len() > 128 {
        return Err(KinesisError::InvalidArgument {
            message: "StreamName must be between 1 and 128 characters."
                .to_owned(),
        });
    }
    if !value.chars().all(|character| {
        character.is_ascii_alphanumeric()
            || matches!(character, '_' | '-' | '.')
    }) {
        return Err(KinesisError::InvalidArgument {
            message:
                "StreamName may only contain alphanumeric characters, hyphens, underscores, and periods."
                    .to_owned(),
        });
    }

    Ok(value.to_owned())
}

pub(crate) fn stream_name_from_arn(
    arn: &Arn,
) -> Result<KinesisStreamName, KinesisError> {
    if arn.service() != ServiceName::Kinesis {
        return Err(KinesisError::InvalidArgument {
            message: "StreamARN must be a Kinesis ARN.".to_owned(),
        });
    }

    let ArnResource::Generic(resource) = arn.resource() else {
        return Err(KinesisError::InvalidArgument {
            message: "StreamARN must contain a Kinesis stream resource."
                .to_owned(),
        });
    };
    let Some(stream_name) = resource.strip_prefix("stream/") else {
        return Err(KinesisError::InvalidArgument {
            message: "StreamARN must contain a stream resource.".to_owned(),
        });
    };

    KinesisStreamName::new(stream_name)
}

#[cfg(test)]
mod tests {
    use super::{
        KinesisStreamIdentifier, KinesisStreamName, stream_name_from_arn,
        validate_stream_name,
    };
    use crate::KinesisError;
    use aws::Arn;

    #[test]
    fn kinesis_stream_names_and_identifiers_round_trip_through_typed_contracts()
     {
        let stream_name =
            KinesisStreamName::new("orders-stream").expect("stream name");
        let stream_arn: Arn =
            "arn:aws:kinesis:eu-west-2:000000000000:stream/orders-stream"
                .parse()
                .expect("stream ARN should parse");
        let identifier = KinesisStreamIdentifier::new(
            Some(stream_arn.clone()),
            Some(stream_name.clone()),
        );

        assert_eq!(stream_name, "orders-stream");
        assert_eq!(stream_name, "orders-stream".to_owned());
        assert_eq!(stream_name.as_ref(), "orders-stream");
        assert_eq!(stream_name.to_string(), "orders-stream");
        assert_eq!(
            KinesisStreamName::try_from("orders-stream".to_owned())
                .expect("stream name should convert from String"),
            stream_name
        );
        assert_eq!(
            String::from(stream_name.clone()),
            "orders-stream".to_owned()
        );
        assert_eq!(stream_name_from_arn(&stream_arn), Ok(stream_name));
        assert_eq!(identifier.stream_arn(), Some(&stream_arn));
        assert_eq!(
            identifier.stream_name(),
            Some(
                &identifier.stream_name().cloned().expect("name should exist")
            )
        );
    }

    #[test]
    fn kinesis_stream_name_validation_rejects_invalid_values_at_the_boundary()
    {
        let blank = validate_stream_name("")
            .expect_err("blank stream names should fail");
        let invalid_name = KinesisStreamName::new("bad name")
            .expect_err("stream names with spaces should fail");
        let wrong_service = stream_name_from_arn(
            &"arn:aws:sqs:eu-west-2:000000000000:orders"
                .parse::<Arn>()
                .expect("SQS ARN should parse"),
        )
        .expect_err("non-kinesis ARNs should fail");
        let wrong_resource = stream_name_from_arn(
            &"arn:aws:kinesis:eu-west-2:000000000000:consumer/orders:1"
                .parse::<Arn>()
                .expect("Kinesis ARN should parse"),
        )
        .expect_err("non-stream resources should fail");

        assert_eq!(
            blank,
            KinesisError::InvalidArgument {
                message: "StreamName must be between 1 and 128 characters."
                    .to_owned(),
            }
        );
        assert_eq!(
            invalid_name,
            KinesisError::InvalidArgument {
                message:
                    "StreamName may only contain alphanumeric characters, hyphens, underscores, and periods."
                        .to_owned(),
            }
        );
        assert_eq!(
            wrong_service,
            KinesisError::InvalidArgument {
                message: "StreamARN must be a Kinesis ARN.".to_owned(),
            }
        );
        assert_eq!(
            wrong_resource,
            KinesisError::InvalidArgument {
                message: "StreamARN must contain a stream resource."
                    .to_owned(),
            }
        );
    }
}
