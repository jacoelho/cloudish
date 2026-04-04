use crate::{
    errors::S3Error,
    object::{
        ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput,
        ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output,
        ListedObject, ListedVersionEntry,
    },
    scope::S3Scope,
    state::S3Service,
};
use base64::Engine as _;
use std::collections::BTreeMap;

const DEFAULT_MAX_KEYS: usize = 1_000;
const MAX_KEYS_LIMIT: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListPage {
    common_prefixes: Vec<String>,
    contents: Vec<ListedObject>,
    is_truncated: bool,
    max_keys: usize,
    next_anchor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListedEntry {
    kind: ListedEntryKind,
    sort_key: String,
}

impl ListedEntry {
    fn object(value: ListedObject) -> Self {
        Self {
            sort_key: value.key.clone(),
            kind: ListedEntryKind::Object(value),
        }
    }

    fn prefix(value: String) -> Self {
        Self {
            sort_key: value.clone(),
            kind: ListedEntryKind::CommonPrefix(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ListedEntryKind {
    CommonPrefix(String),
    Object(ListedObject),
}

impl S3Service {
    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_objects(
        &self,
        scope: &S3Scope,
        input: ListObjectsInput,
    ) -> Result<ListObjectsOutput, S3Error> {
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let page = self.collect_page(
            &input.bucket,
            input.prefix.clone(),
            input.delimiter.clone(),
            input.max_keys,
            input.marker.clone(),
        )?;

        Ok(ListObjectsOutput {
            bucket: input.bucket,
            common_prefixes: page.common_prefixes,
            contents: page.contents,
            delimiter: input.delimiter,
            is_truncated: page.is_truncated,
            marker: input.marker,
            max_keys: page.max_keys,
            next_marker: page.next_anchor,
            prefix: input.prefix,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_objects_v2(
        &self,
        scope: &S3Scope,
        input: ListObjectsV2Input,
    ) -> Result<ListObjectsV2Output, S3Error> {
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let anchor = match input.continuation_token.as_deref() {
            Some(token) => Some(decode_continuation_token(token)?),
            None => input.start_after.clone(),
        };
        let page = self.collect_page(
            &input.bucket,
            input.prefix.clone(),
            input.delimiter.clone(),
            input.max_keys,
            anchor,
        )?;
        let key_count = page.contents.len().saturating_add(
            page.common_prefixes.len(),
        );

        Ok(ListObjectsV2Output {
            bucket: input.bucket,
            common_prefixes: page.common_prefixes,
            contents: page.contents,
            continuation_token: input.continuation_token,
            delimiter: input.delimiter,
            is_truncated: page.is_truncated,
            key_count,
            max_keys: page.max_keys,
            next_continuation_token: page
                .next_anchor
                .as_deref()
                .map(encode_continuation_token),
            prefix: input.prefix,
            start_after: input.start_after,
        })
    }

    /// # Errors
    ///
    /// Returns operation-specific validation, ownership, unsupported-path, or persistence errors when the request cannot be completed.
    pub fn list_object_versions(
        &self,
        scope: &S3Scope,
        input: ListObjectVersionsInput,
    ) -> Result<ListObjectVersionsOutput, S3Error> {
        self.ensure_bucket_owned(scope, &input.bucket)?;
        let prefix = input.prefix.clone().unwrap_or_default();
        let max_keys = normalize_max_keys(input.max_keys);
        let mut entries = self
            .bucket_object_records(&input.bucket)
            .into_iter()
            .filter(|object| object.key().starts_with(&prefix))
            .filter(|object| object.has_version_id())
            .map(|object| {
                if object.is_delete_marker() {
                    ListedVersionEntry::DeleteMarker(
                        object.into_delete_marker(),
                    )
                } else {
                    ListedVersionEntry::Version(object.into_listed_version())
                }
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.sort_key().cmp(right.sort_key()).then_with(|| {
                right.version_sort_key().cmp(left.version_sort_key())
            })
        });

        let is_truncated = entries.len() > max_keys;
        entries.truncate(max_keys);

        Ok(ListObjectVersionsOutput {
            bucket: input.bucket,
            entries,
            is_truncated,
            max_keys,
            prefix: input.prefix,
        })
    }

    fn collect_page(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        max_keys: Option<usize>,
        anchor: Option<String>,
    ) -> Result<ListPage, S3Error> {
        let prefix = prefix.unwrap_or_default();
        let max_keys = normalize_max_keys(max_keys);
        let mut entries = Vec::new();
        let mut common_prefixes = BTreeMap::new();

        for object in self
            .bucket_object_records(bucket)
            .into_iter()
            .filter(|object| object.is_current_visible())
        {
            if !object.key().starts_with(&prefix) {
                continue;
            }

            if anchor.as_ref().is_some_and(|anchor| object.key() <= anchor) {
                continue;
            }

            if let Some(delimiter) = delimiter.as_deref() {
                let Some(remainder) = object.key().get(prefix.len()..) else {
                    continue;
                };
                if let Some(index) = remainder.find(delimiter) {
                    let Some(prefix_end) =
                        index.checked_add(delimiter.len())
                    else {
                        continue;
                    };
                    let common_prefix = format!(
                        "{prefix}{}",
                        remainder.get(..prefix_end).unwrap_or_default()
                    );
                    common_prefixes
                        .entry(common_prefix.clone())
                        .or_insert_with(|| ListedEntry::prefix(common_prefix));
                    continue;
                }
            }

            entries.push(ListedEntry::object(object.into_listed()));
        }

        entries.extend(common_prefixes.into_values());
        entries.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));

        let is_truncated = entries.len() > max_keys;
        let page_entries =
            entries.into_iter().take(max_keys).collect::<Vec<_>>();
        let next_anchor = if is_truncated {
            page_entries.last().map(|entry| entry.sort_key.clone())
        } else {
            None
        };
        let mut contents = Vec::new();
        let mut prefixes = Vec::new();

        for entry in page_entries {
            match entry.kind {
                ListedEntryKind::CommonPrefix(value) => prefixes.push(value),
                ListedEntryKind::Object(value) => contents.push(value),
            }
        }

        Ok(ListPage {
            common_prefixes: prefixes,
            contents,
            is_truncated,
            max_keys,
            next_anchor,
        })
    }
}

fn decode_continuation_token(token: &str) -> Result<String, S3Error> {
    base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_| invalid_continuation_token_error())
        .and_then(|bytes| {
            String::from_utf8(bytes)
                .map_err(|_| invalid_continuation_token_error())
        })
}

fn encode_continuation_token(value: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

fn invalid_continuation_token_error() -> S3Error {
    S3Error::InvalidArgument {
        code: "InvalidArgument",
        message: "The continuation token is not valid.".to_owned(),
        status_code: 400,
    }
}

fn normalize_max_keys(max_keys: Option<usize>) -> usize {
    max_keys.unwrap_or(DEFAULT_MAX_KEYS).clamp(1, MAX_KEYS_LIMIT)
}
