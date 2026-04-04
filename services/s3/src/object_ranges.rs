use crate::S3Error;
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectRange {
    Start { start: u64 },
    StartEnd { end: u64, start: u64 },
    Suffix { length: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IfRangeCondition {
    ETag(String),
    LastModified(std::time::SystemTime),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedObjectRange {
    end: u64,
    size: u64,
    start: u64,
}

pub(crate) struct RangedBody {
    pub(crate) body: Vec<u8>,
    pub(crate) content_length: u64,
    pub(crate) content_range: Option<String>,
}

impl ResolvedObjectRange {
    pub(crate) fn content_length(&self) -> u64 {
        self.end - self.start + 1
    }

    pub(crate) fn content_range(self) -> String {
        format!("bytes {}-{}/{}", self.start, self.end, self.size)
    }
}

pub(crate) fn apply_object_range(
    body: &[u8],
    range: Option<&ObjectRange>,
) -> Result<RangedBody, S3Error> {
    let Some(range) = range else {
        return Ok(RangedBody {
            body: body.to_vec(),
            content_length: body.len() as u64,
            content_range: None,
        });
    };
    let resolved = resolve_object_range(range, body.len() as u64)?;
    let start =
        usize::try_from(resolved.start).map_err(|_| S3Error::Internal {
            message: "Object range start did not fit in usize.".to_owned(),
        })?;
    let end =
        usize::try_from(resolved.end).map_err(|_| S3Error::Internal {
            message: "Object range end did not fit in usize.".to_owned(),
        })?;

    Ok(RangedBody {
        body: body[start..=end].to_vec(),
        content_length: resolved.content_length(),
        content_range: Some(resolved.content_range()),
    })
}

pub fn eligible_object_range<'a>(
    etag: &str,
    last_modified_epoch_seconds: u64,
    range: Option<&'a ObjectRange>,
    if_range: Option<&IfRangeCondition>,
) -> Option<&'a ObjectRange> {
    let range = range?;
    let Some(if_range) = if_range else {
        return Some(range);
    };
    let object_time = UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_epoch_seconds);
    match if_range {
        IfRangeCondition::ETag(if_range_etag)
            if crate::object_read_conditions::strong_etag_condition_matches(
                if_range_etag,
                etag,
            ) =>
        {
            Some(range)
        }
        IfRangeCondition::LastModified(timestamp)
            if object_time <= *timestamp =>
        {
            Some(range)
        }
        _ => None,
    }
}

pub(crate) fn resolve_object_range(
    range: &ObjectRange,
    size: u64,
) -> Result<ResolvedObjectRange, S3Error> {
    if size == 0 {
        return Err(S3Error::InvalidArgument {
            code: "InvalidRange",
            message: "The requested range is not satisfiable.".to_owned(),
            status_code: 416,
        });
    }

    let (start, end) = match range {
        ObjectRange::Start { start } if *start < size => (*start, size - 1),
        ObjectRange::StartEnd { start, end }
            if start <= end && *start < size =>
        {
            (*start, (*end).min(size - 1))
        }
        ObjectRange::Suffix { length } if *length > 0 => {
            let content_length = (*length).min(size);
            (size - content_length, size - 1)
        }
        _ => {
            return Err(S3Error::InvalidArgument {
                code: "InvalidRange",
                message: "The requested range is not satisfiable.".to_owned(),
                status_code: 416,
            });
        }
    };

    Ok(ResolvedObjectRange { end, size, start })
}

#[cfg(test)]
mod tests {
    use super::{
        IfRangeCondition, ObjectRange, apply_object_range,
        eligible_object_range, resolve_object_range,
    };
    use crate::S3Error;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn object_reads_apply_range_returns_expected_slice() {
        let ranged = apply_object_range(
            b"hello world",
            Some(&ObjectRange::StartEnd { start: 6, end: 10 }),
        )
        .expect("range should resolve");

        assert_eq!(ranged.body, b"world");
        assert_eq!(ranged.content_length, 5);
        assert_eq!(ranged.content_range.as_deref(), Some("bytes 6-10/11"));
    }

    #[test]
    fn object_reads_suffix_range_clamps_to_body_size() {
        let resolved =
            resolve_object_range(&ObjectRange::Suffix { length: 10 }, 4)
                .expect("suffix range should resolve");

        assert_eq!(resolved.content_length(), 4);
        assert_eq!(resolved.content_range(), "bytes 0-3/4");
    }

    #[test]
    fn object_reads_reject_invalid_ranges() {
        let error = resolve_object_range(&ObjectRange::Start { start: 5 }, 5)
            .expect_err("range should be rejected");

        assert!(matches!(
            error,
            S3Error::InvalidArgument {
                code: "InvalidRange",
                message,
                status_code: 416,
            } if message == "The requested range is not satisfiable."
        ));
    }

    #[test]
    fn object_reads_if_range_respects_strong_etags_and_last_modified() {
        let object = fixture_object();
        let range = Some(&ObjectRange::StartEnd { start: 1, end: 2 });
        let matching = Some(&IfRangeCondition::ETag("\"etag\"".to_owned()));
        let weak = Some(&IfRangeCondition::ETag("W/\"etag\"".to_owned()));
        let future = Some(&IfRangeCondition::LastModified(
            UNIX_EPOCH
                + Duration::from_secs(object.last_modified_epoch_seconds + 1),
        ));

        assert!(
            eligible_object_range(
                &object.etag,
                object.last_modified_epoch_seconds,
                range,
                matching,
            )
            .is_some()
        );
        assert!(
            eligible_object_range(
                &object.etag,
                object.last_modified_epoch_seconds,
                range,
                weak,
            )
            .is_none()
        );
        assert!(
            eligible_object_range(
                &object.etag,
                object.last_modified_epoch_seconds,
                range,
                future,
            )
            .is_some()
        );
    }

    fn fixture_object() -> FixtureObject {
        FixtureObject {
            etag: "\"etag\"".to_owned(),
            last_modified_epoch_seconds: 1_700_000_000,
        }
    }

    struct FixtureObject {
        etag: String,
        last_modified_epoch_seconds: u64,
    }
}
