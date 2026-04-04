use crate::{
    BucketNotificationConfiguration, BucketVersioningStatus, CannedAcl,
    CompleteMultipartUploadInput, CompletedMultipartPart, CopyObjectInput,
    CreateBucketInput, CreateMultipartUploadInput, DeleteObjectInput,
    HeadObjectOutput, ListObjectVersionsInput, ListObjectsInput,
    ListObjectsV2Input, ListedVersionEntry, NotificationFilter, ObjectRange,
    ObjectReadMetadata, PutObjectInput, QueueNotificationConfiguration,
    S3Error, StoredBucketAclInput, TaggingInput,
    TopicNotificationConfiguration, UploadPartInput,
    object_read_model::ObjectReadRequest,
    retention::{
        BucketObjectLockConfiguration, DefaultObjectLockRetention,
        DefaultRetentionPeriod, LegalHoldStatus, ObjectLockMode,
        ObjectRetention, PutObjectLegalHoldInput, PutObjectRetentionInput,
    },
    serialization::{
        CsvFileHeaderInfo, CsvInputSerialization, CsvOutputSerialization,
        SelectObjectContentInput,
    },
    state_test_support::{
        CountingBlobStore, RecordingNotificationTransport, create_bucket,
        object_read, scope, scope_in_account_region, scope_in_region, service,
        service_with_blob_store,
    },
};
use std::collections::BTreeMap;
use std::sync::Arc;

#[test]
fn s3_core_get_and_head_share_metadata_but_get_returns_payload() {
    let service = service(1_710_000_000);
    let scope = scope();
    create_bucket(&service, &scope, "demo");

    let put = service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "notes.txt".to_owned(),
                metadata: BTreeMap::from([(
                    "Trace".to_owned(),
                    "abc123".to_owned(),
                )]),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("object put should succeed");
    let head = service
        .head_object(&scope, &object_read("demo", "notes.txt", None, None))
        .expect("head should succeed");
    let get = service
        .get_object(&scope, &object_read("demo", "notes.txt", None, None))
        .expect("get should succeed");

    assert_eq!(put.etag, head.metadata.etag);
    assert_eq!(
        head,
        HeadObjectOutput {
            content_length: get.content_length,
            metadata: get.metadata.clone(),
        }
    );
    assert_eq!(head.metadata.content_type, "text/plain");
    assert_eq!(head.metadata.version_id, None);
    assert_eq!(
        head.metadata.metadata,
        BTreeMap::from([("trace".to_owned(), "abc123".to_owned())])
    );
    assert_eq!(get.body, b"payload".to_vec());
}

#[test]
fn s3_core_get_and_head_honor_expected_bucket_owner() {
    let service = service(1_710_000_000);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "notes.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("object put should succeed");

    let mismatch = ObjectReadRequest {
        bucket: "demo".to_owned(),
        expected_bucket_owner: Some("111111111111".to_owned()),
        key: "notes.txt".to_owned(),
        ..ObjectReadRequest::default()
    };

    let get = service
        .get_object(&scope, &mismatch)
        .expect_err("get with expected-owner mismatch should fail");
    let head = service
        .head_object(&scope, &mismatch)
        .expect_err("head with expected-owner mismatch should fail");

    assert!(matches!(get, S3Error::AccessDenied { .. }));
    assert!(matches!(head, S3Error::AccessDenied { .. }));
}

#[test]
fn s3_core_metadata_reads_do_not_fetch_object_bodies() {
    let blob_store = Arc::new(CountingBlobStore::default());
    let service = service_with_blob_store(1_710_000_000, blob_store.clone());
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "notes.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("object put should succeed");

    let metadata = service
        .read_object_metadata(
            &scope,
            &object_read("demo", "notes.txt", None, None),
        )
        .expect("metadata read should succeed");

    assert_eq!(metadata.content_length, 7);
    assert_eq!(blob_store.get_calls(), 0);

    let get = service
        .get_object(&scope, &object_read("demo", "notes.txt", None, None))
        .expect("get should succeed");

    assert_eq!(get.body, b"payload".to_vec());
    assert_eq!(blob_store.get_calls(), 1);
}

#[test]
fn s3_core_rejects_duplicate_buckets_and_non_empty_bucket_delete() {
    let service = service(1_710_000_001);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    let duplicate = service
        .create_bucket(
            &scope,
            CreateBucketInput {
                name: "demo".to_owned(),
                object_lock_enabled: false,
                region: scope.region().clone(),
            },
        )
        .expect_err("duplicate bucket should fail");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "notes.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("object put should succeed");
    let delete = service
        .delete_bucket(&scope, "demo")
        .expect_err("non-empty bucket should fail");

    assert!(matches!(duplicate, S3Error::BucketAlreadyOwnedByYou { .. }));
    assert!(matches!(delete, S3Error::BucketNotEmpty { .. }));
}

#[test]
fn s3_core_rejects_bucket_access_from_the_wrong_region() {
    let service = service(1_710_000_001);
    let bucket_scope = scope();
    let wrong_region_scope = scope_in_region("us-east-1");
    create_bucket(&service, &bucket_scope, "demo");

    let error = service
        .get_bucket_location(&wrong_region_scope, "demo")
        .expect_err("wrong-region bucket access should fail");

    assert!(matches!(
        error,
        S3Error::WrongRegion { bucket, region }
            if bucket == "demo" && region == "eu-west-2"
    ));
}

#[test]
fn s3_core_head_bucket_reports_probe_outcomes_without_ownership_errors() {
    let service = service(1_710_000_001);
    let owner_scope = scope();
    let wrong_region_scope = scope_in_region("us-east-1");
    let other_account_scope =
        scope_in_account_region("111111111111", "eu-west-2");
    let other_account_wrong_region_scope =
        scope_in_account_region("111111111111", "us-east-1");
    create_bucket(&service, &owner_scope, "demo");

    assert_eq!(
        service.head_bucket(
            &owner_scope,
            "demo",
            &crate::bucket::HeadBucketInput::default(),
        ),
        crate::bucket::HeadBucketOutput::Found {
            region: "eu-west-2".to_owned(),
        }
    );
    assert_eq!(
        service.head_bucket(
            &wrong_region_scope,
            "demo",
            &crate::bucket::HeadBucketInput::default(),
        ),
        crate::bucket::HeadBucketOutput::WrongRegion {
            region: "eu-west-2".to_owned(),
        }
    );
    assert_eq!(
        service.head_bucket(
            &other_account_scope,
            "demo",
            &crate::bucket::HeadBucketInput::default(),
        ),
        crate::bucket::HeadBucketOutput::Forbidden {
            region: "eu-west-2".to_owned(),
        }
    );
    assert_eq!(
        service.head_bucket(
            &other_account_wrong_region_scope,
            "demo",
            &crate::bucket::HeadBucketInput::default(),
        ),
        crate::bucket::HeadBucketOutput::Forbidden {
            region: "eu-west-2".to_owned(),
        }
    );
    assert_eq!(
        service.head_bucket(
            &owner_scope,
            "missing",
            &crate::bucket::HeadBucketInput::default(),
        ),
        crate::bucket::HeadBucketOutput::Missing
    );
    assert_eq!(
        service.head_bucket(
            &owner_scope,
            "demo",
            &crate::bucket::HeadBucketInput {
                expected_bucket_owner: Some("111111111111".to_owned()),
            },
        ),
        crate::bucket::HeadBucketOutput::Forbidden {
            region: "eu-west-2".to_owned(),
        }
    );
}

#[test]
fn s3_core_listing_supports_prefix_delimiter_and_continuation_tokens() {
    let service = service(1_710_000_002);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    for key in [
        "logs/2026/a.txt",
        "logs/2026/b.txt",
        "logs/2026/c.txt",
        "logs/2027/d.txt",
    ] {
        service
            .put_object(
                &scope,
                PutObjectInput {
                    body: key.as_bytes().to_vec(),
                    bucket: "demo".to_owned(),
                    content_type: None,
                    key: key.to_owned(),
                    metadata: BTreeMap::new(),
                    object_lock: None,
                    tags: BTreeMap::new(),
                },
            )
            .expect("object put should succeed");
    }

    let page = service
        .list_objects(
            &scope,
            ListObjectsInput {
                bucket: "demo".to_owned(),
                delimiter: Some("/".to_owned()),
                marker: None,
                max_keys: Some(10),
                prefix: Some("logs/".to_owned()),
            },
        )
        .expect("list objects should succeed");
    let v2 = service
        .list_objects_v2(
            &scope,
            ListObjectsV2Input {
                bucket: "demo".to_owned(),
                continuation_token: None,
                delimiter: None,
                max_keys: Some(2),
                prefix: Some("logs/2026/".to_owned()),
                start_after: None,
            },
        )
        .expect("list objects v2 should succeed");
    let continued = service
        .list_objects_v2(
            &scope,
            ListObjectsV2Input {
                bucket: "demo".to_owned(),
                continuation_token: v2.next_continuation_token.clone(),
                delimiter: None,
                max_keys: Some(2),
                prefix: Some("logs/2026/".to_owned()),
                start_after: None,
            },
        )
        .expect("continuation token should succeed");

    assert_eq!(
        page.common_prefixes,
        vec!["logs/2026/".to_owned(), "logs/2027/".to_owned()]
    );
    assert!(page.contents.is_empty());
    assert_eq!(
        v2.contents.iter().map(|item| item.key.as_str()).collect::<Vec<_>>(),
        vec!["logs/2026/a.txt", "logs/2026/b.txt"]
    );
    assert!(v2.next_continuation_token.is_some());
    assert_eq!(
        continued
            .contents
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        vec!["logs/2026/c.txt"]
    );
    let grouped = service
        .list_objects_v2(
            &scope,
            ListObjectsV2Input {
                bucket: "demo".to_owned(),
                continuation_token: None,
                delimiter: Some("/".to_owned()),
                max_keys: Some(10),
                prefix: Some("logs/".to_owned()),
                start_after: None,
            },
        )
        .expect("grouped list objects v2 should succeed");

    assert_eq!(grouped.common_prefixes.len(), 2);
    assert_eq!(grouped.key_count, 2);
}

#[test]
fn s3_core_copy_and_delete_object_cleanup_round_trips() {
    let service = service(1_710_000_003);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "src.txt".to_owned(),
                metadata: BTreeMap::from([(
                    "env".to_owned(),
                    "dev".to_owned(),
                )]),
                object_lock: None,
                tags: BTreeMap::from([(
                    "team".to_owned(),
                    "platform".to_owned(),
                )]),
            },
        )
        .expect("source object should be written");

    let copied = service
        .copy_object(
            &scope,
            CopyObjectInput {
                destination_bucket: "demo".to_owned(),
                destination_key: "dst.txt".to_owned(),
                source_bucket: "demo".to_owned(),
                source_key: "src.txt".to_owned(),
                source_version_id: None,
            },
        )
        .expect("copy should succeed");
    let head = service
        .head_object(&scope, &object_read("demo", "dst.txt", None, None))
        .expect("copied object should exist");
    let tags = service
        .get_object_tagging(&scope, "demo", "dst.txt", None)
        .expect("copied tags should exist");

    service
        .delete_object(
            &scope,
            DeleteObjectInput {
                bypass_governance: false,
                bypass_governance_authorized: false,
                bucket: "demo".to_owned(),
                key: "src.txt".to_owned(),
                version_id: None,
            },
        )
        .expect("delete should succeed");
    let missing = service
        .get_object(&scope, &object_read("demo", "src.txt", None, None))
        .expect_err("deleted object should be missing");

    assert_eq!(copied.etag, head.metadata.etag);
    assert_eq!(head.metadata.metadata.get("env"), Some(&"dev".to_owned()));
    assert_eq!(tags.tags.get("team"), Some(&"platform".to_owned()));
    assert!(matches!(missing, S3Error::NoSuchKey { .. }));
}

#[test]
fn s3_advanced_copy_object_can_target_a_noncurrent_version() {
    let service = service(1_710_000_000);
    let scope = scope();

    create_bucket(&service, &scope, "demo");
    service
        .put_bucket_versioning(&scope, "demo", BucketVersioningStatus::Enabled)
        .expect("versioning should enable");

    let first = service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"first".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "src.txt".to_owned(),
                metadata: BTreeMap::from([(
                    "env".to_owned(),
                    "dev".to_owned(),
                )]),
                object_lock: None,
                tags: BTreeMap::from([(
                    "team".to_owned(),
                    "platform".to_owned(),
                )]),
            },
        )
        .expect("first version should write");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"second".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "src.txt".to_owned(),
                metadata: BTreeMap::from([(
                    "env".to_owned(),
                    "prod".to_owned(),
                )]),
                object_lock: None,
                tags: BTreeMap::from([("team".to_owned(), "data".to_owned())]),
            },
        )
        .expect("second version should write");

    service
        .copy_object(
            &scope,
            CopyObjectInput {
                destination_bucket: "demo".to_owned(),
                destination_key: "dst.txt".to_owned(),
                source_bucket: "demo".to_owned(),
                source_key: "src.txt".to_owned(),
                source_version_id: first.version_id,
            },
        )
        .expect("historical version copy should succeed");

    let copied = service
        .get_object(&scope, &object_read("demo", "dst.txt", None, None))
        .expect("copied object should exist");
    let copied_tags = service
        .get_object_tagging(&scope, "demo", "dst.txt", None)
        .expect("copied tags should exist");

    assert_eq!(copied.body, b"first");
    assert_eq!(copied.metadata.metadata.get("env"), Some(&"dev".to_owned()));
    assert_eq!(copied_tags.tags.get("team"), Some(&"platform".to_owned()));
}

#[test]
fn s3_core_head_output_matches_expected_shape() {
    let head = HeadObjectOutput {
        content_length: 7,
        metadata: ObjectReadMetadata {
            content_type: "text/plain".to_owned(),
            delete_marker: false,
            etag: "\"abc\"".to_owned(),
            key: "demo.txt".to_owned(),
            last_modified_epoch_seconds: 1,
            metadata: BTreeMap::new(),
            object_lock_legal_hold_status: None,
            object_lock_mode: None,
            object_lock_retain_until_epoch_seconds: None,
            version_id: Some("1".to_owned()),
        },
    };

    assert_eq!(head.metadata.content_type, "text/plain");
    assert_eq!(head.metadata.etag, "\"abc\"");
    assert_eq!(head.content_length, 7);
    assert_eq!(head.metadata.version_id.as_deref(), Some("1"));
}

#[test]
fn s3_advanced_versioning_delete_markers_and_historical_reads() {
    let service = service(1_710_000_010);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_bucket_versioning(&scope, "demo", BucketVersioningStatus::Enabled)
        .expect("versioning should enable");

    let first = service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"first".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "doc.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("first version should write");
    let second = service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"second".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "doc.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("second version should write");
    let delete = service
        .delete_object(
            &scope,
            DeleteObjectInput {
                bypass_governance: false,
                bypass_governance_authorized: false,
                bucket: "demo".to_owned(),
                key: "doc.txt".to_owned(),
                version_id: None,
            },
        )
        .expect("delete should create a marker");
    let current = service
        .get_object(&scope, &object_read("demo", "doc.txt", None, None))
        .expect_err("latest delete marker should hide the object");
    let current_head = service
        .head_object(&scope, &object_read("demo", "doc.txt", None, None))
        .expect_err("latest delete marker should hide head reads");
    let delete_marker_get = service
        .get_object(
            &scope,
            &object_read(
                "demo",
                "doc.txt",
                delete.version_id.as_deref(),
                None,
            ),
        )
        .expect_err("delete marker version should fail");
    let delete_marker_head = service
        .head_object(
            &scope,
            &object_read(
                "demo",
                "doc.txt",
                delete.version_id.as_deref(),
                None,
            ),
        )
        .expect_err("delete marker head version should fail");
    let historical = service
        .get_object(
            &scope,
            &object_read("demo", "doc.txt", first.version_id.as_deref(), None),
        )
        .expect("first version should remain readable");
    let newer = service
        .get_object(
            &scope,
            &object_read(
                "demo",
                "doc.txt",
                second.version_id.as_deref(),
                None,
            ),
        )
        .expect("second version should remain readable");
    let versions = service
        .list_object_versions(
            &scope,
            ListObjectVersionsInput {
                bucket: "demo".to_owned(),
                max_keys: Some(10),
                prefix: Some("doc".to_owned()),
            },
        )
        .expect("version list should succeed");

    assert!(matches!(current, S3Error::CurrentVersionIsDeleteMarker { .. }));
    assert!(matches!(
        current_head,
        S3Error::CurrentVersionIsDeleteMarker { .. }
    ));
    assert!(matches!(
        delete_marker_get,
        S3Error::RequestedVersionIsDeleteMarker { .. }
    ));
    assert!(matches!(
        delete_marker_head,
        S3Error::RequestedVersionIsDeleteMarker { .. }
    ));
    assert_eq!(historical.body, b"first".to_vec());
    assert_eq!(newer.body, b"second".to_vec());
    assert!(delete.delete_marker);
    assert_eq!(versions.entries.len(), 3);
    assert!(matches!(
        versions.entries.first(),
        Some(ListedVersionEntry::DeleteMarker(_))
    ));
}

#[test]
fn s3_core_get_and_head_support_single_ranges() {
    let service = service(1_710_000_030);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"abcdefghij".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "notes.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("object put should succeed");

    let range = ObjectRange::StartEnd { end: 5, start: 2 };
    let ranged = service
        .get_object(
            &scope,
            &object_read("demo", "notes.txt", None, Some(&range)),
        )
        .expect("range get should succeed");
    let head = service
        .head_object(
            &scope,
            &object_read("demo", "notes.txt", None, Some(&range)),
        )
        .expect("range head should succeed");

    assert_eq!(ranged.body, b"cdef".to_vec());
    assert!(ranged.is_partial);
    assert_eq!(ranged.content_range.as_deref(), Some("bytes 2-5/10"));
    assert_eq!(ranged.content_length, 4);
    assert_eq!(head.content_length, 4);
}

#[test]
fn s3_advanced_multipart_completion_and_abort_cleanup() {
    let service = service(1_710_000_011);
    let scope = scope();
    create_bucket(&service, &scope, "demo");

    let upload = service
        .create_multipart_upload(
            &scope,
            CreateMultipartUploadInput {
                bucket: "demo".to_owned(),
                content_type: Some("text/plain".to_owned()),
                key: "large.txt".to_owned(),
                metadata: BTreeMap::from([(
                    "trace".to_owned(),
                    "abc123".to_owned(),
                )]),
                tags: BTreeMap::new(),
            },
        )
        .expect("multipart upload should start");
    let part = service
        .upload_part(
            &scope,
            UploadPartInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                key: "large.txt".to_owned(),
                part_number: 1,
                upload_id: upload.upload_id.clone(),
            },
        )
        .expect("part should upload");
    let complete = service
        .complete_multipart_upload(
            &scope,
            CompleteMultipartUploadInput {
                bucket: "demo".to_owned(),
                key: "large.txt".to_owned(),
                parts: vec![CompletedMultipartPart {
                    etag: Some(part.etag),
                    part_number: 1,
                }],
                upload_id: upload.upload_id,
            },
        )
        .expect("multipart upload should complete");
    let head = service
        .head_object(&scope, &object_read("demo", "large.txt", None, None))
        .expect("completed object should exist");
    let uploads = service
        .list_multipart_uploads(&scope, "demo")
        .expect("multipart listing should succeed");

    assert_eq!(head.metadata.etag, complete.etag);
    assert!(uploads.uploads.is_empty());

    let aborted = service
        .create_multipart_upload(
            &scope,
            CreateMultipartUploadInput {
                bucket: "demo".to_owned(),
                content_type: None,
                key: "aborted.txt".to_owned(),
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
            },
        )
        .expect("abortable upload should start");
    service
        .upload_part(
            &scope,
            UploadPartInput {
                body: b"chunk".to_vec(),
                bucket: "demo".to_owned(),
                key: "aborted.txt".to_owned(),
                part_number: 1,
                upload_id: aborted.upload_id.clone(),
            },
        )
        .expect("abortable part should upload");
    service
        .abort_multipart_upload(
            &scope,
            "demo",
            "aborted.txt",
            &aborted.upload_id,
        )
        .expect("abort should succeed");
    let after_abort = service
        .list_multipart_uploads(&scope, "demo")
        .expect("multipart listing after abort should succeed");
    let complete_after_abort = service
        .complete_multipart_upload(
            &scope,
            CompleteMultipartUploadInput {
                bucket: "demo".to_owned(),
                key: "aborted.txt".to_owned(),
                parts: vec![CompletedMultipartPart {
                    etag: None,
                    part_number: 1,
                }],
                upload_id: aborted.upload_id,
            },
        )
        .expect_err("aborted upload should be gone");

    assert!(after_abort.uploads.is_empty());
    assert!(matches!(complete_after_abort, S3Error::NoSuchUpload { .. }));
}

#[test]
fn s3_advanced_upload_part_rejects_part_numbers_above_ten_thousand() {
    let service = service(1_710_000_011);
    let scope = scope();
    create_bucket(&service, &scope, "demo");

    let upload = service
        .create_multipart_upload(
            &scope,
            CreateMultipartUploadInput {
                bucket: "demo".to_owned(),
                content_type: None,
                key: "large.txt".to_owned(),
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
            },
        )
        .expect("multipart upload should start");

    let error = service
        .upload_part(
            &scope,
            UploadPartInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                key: "large.txt".to_owned(),
                part_number: 10_001,
                upload_id: upload.upload_id,
            },
        )
        .expect_err("part number above 10000 should fail");

    assert!(matches!(
        error,
        S3Error::InvalidArgument { code: "InvalidArgument", .. }
    ));
}

#[test]
fn s3_advanced_multipart_rejects_invalid_completion_shapes() {
    let service = service(1_710_000_012);
    let scope = scope();
    create_bucket(&service, &scope, "demo");

    let upload = service
        .create_multipart_upload(
            &scope,
            CreateMultipartUploadInput {
                bucket: "demo".to_owned(),
                content_type: None,
                key: "invalid.txt".to_owned(),
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
            },
        )
        .expect("upload should start");
    let first = service
        .upload_part(
            &scope,
            UploadPartInput {
                body: b"first".to_vec(),
                bucket: "demo".to_owned(),
                key: "invalid.txt".to_owned(),
                part_number: 1,
                upload_id: upload.upload_id.clone(),
            },
        )
        .expect("first part should upload");
    let _second = service
        .upload_part(
            &scope,
            UploadPartInput {
                body: b"second".to_vec(),
                bucket: "demo".to_owned(),
                key: "invalid.txt".to_owned(),
                part_number: 2,
                upload_id: upload.upload_id.clone(),
            },
        )
        .expect("second part should upload");

    let upload_id = upload.upload_id;
    let wrong_part = service
        .complete_multipart_upload(
            &scope,
            CompleteMultipartUploadInput {
                bucket: "demo".to_owned(),
                key: "invalid.txt".to_owned(),
                parts: vec![CompletedMultipartPart {
                    etag: Some(first.etag),
                    part_number: 3,
                }],
                upload_id: upload_id.clone(),
            },
        )
        .expect_err("missing part should fail");
    let unordered = service
        .complete_multipart_upload(
            &scope,
            CompleteMultipartUploadInput {
                bucket: "demo".to_owned(),
                key: "invalid.txt".to_owned(),
                parts: vec![
                    CompletedMultipartPart { etag: None, part_number: 2 },
                    CompletedMultipartPart { etag: None, part_number: 1 },
                ],
                upload_id,
            },
        )
        .expect_err("unordered parts should fail");

    assert!(matches!(
        wrong_part,
        S3Error::InvalidArgument { code: "InvalidPart", .. }
    ));
    assert!(matches!(
        unordered,
        S3Error::InvalidArgument { code: "InvalidPartOrder", .. }
    ));
}

#[test]
fn s3_advanced_tagging_and_bucket_subresource_storage_round_trip() {
    let service = service(1_710_000_013);
    let scope = scope();
    create_bucket(&service, &scope, "demo");

    service
        .put_bucket_tagging(
            &scope,
            "demo",
            BTreeMap::from([("env".to_owned(), "dev".to_owned())]),
        )
        .expect("bucket tags should store");
    let bucket_tags = service
        .get_bucket_tagging(&scope, "demo")
        .expect("bucket tags should round-trip");

    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "object.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::from([("seed".to_owned(), "true".to_owned())]),
            },
        )
        .expect("object should store");
    let object_tags = service
        .put_object_tagging(
            &scope,
            TaggingInput {
                bucket: "demo".to_owned(),
                key: Some("object.txt".to_owned()),
                tags: BTreeMap::from([(
                    "team".to_owned(),
                    "storage".to_owned(),
                )]),
                version_id: None,
            },
        )
        .expect("object tags should store");
    let stored_tags = service
        .get_object_tagging(&scope, "demo", "object.txt", None)
        .expect("object tags should round-trip");

    service
        .put_bucket_policy(
            &scope,
            "demo",
            "{\"Version\":\"2012-10-17\"}".to_owned(),
        )
        .expect("bucket policy should store");
    service
            .put_bucket_cors(
                &scope,
                "demo",
                "<CORSConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>".to_owned(),
            )
            .expect("bucket cors should store");
    service
            .put_bucket_lifecycle(
                &scope,
                "demo",
                "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>expire</ID><Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>".to_owned(),
            )
            .expect("bucket lifecycle should store");
    service
            .put_bucket_encryption(
                &scope,
                "demo",
                "<ServerSideEncryptionConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>".to_owned(),
            )
            .expect("bucket encryption should store");
    service
        .put_bucket_acl(
            &scope,
            "demo",
            StoredBucketAclInput::Canned(CannedAcl::PublicRead),
        )
        .expect("bucket ACL should store");

    assert_eq!(bucket_tags.tags.get("env"), Some(&"dev".to_owned()));
    assert_eq!(object_tags.tags.get("team"), Some(&"storage".to_owned()));
    assert_eq!(stored_tags.tags.get("team"), Some(&"storage".to_owned()));
    assert!(
        service
            .get_bucket_policy(&scope, "demo")
            .expect("stored policy should read")
            .contains("\"Version\"")
    );
    assert!(
        service
            .get_bucket_cors(&scope, "demo")
            .expect("stored cors should read")
            .contains("CORSConfiguration")
    );
    assert!(
        service
            .get_bucket_lifecycle(&scope, "demo")
            .expect("stored lifecycle should read")
            .contains("LifecycleConfiguration")
    );
    assert!(
        service
            .get_bucket_encryption(&scope, "demo")
            .expect("stored encryption should read")
            .contains("ServerSideEncryptionConfiguration")
    );
    assert!(
        service
            .get_bucket_acl(&scope, "demo")
            .expect("stored ACL should read")
            .contains("AllUsers")
    );
}

#[test]
fn s3_notifications_emit_matching_events_via_explicit_transport() {
    let service = service(1_710_000_020);
    let scope = scope();
    let transport = Arc::new(RecordingNotificationTransport::default());
    service.set_notification_transport(transport.clone());
    create_bucket(&service, &scope, "demo");
    service
        .put_bucket_notification_configuration(
            &scope,
            "demo",
            BucketNotificationConfiguration {
                queue_configurations: vec![QueueNotificationConfiguration {
                    events: vec!["s3:ObjectCreated:*".to_owned()],
                    filter: Some(NotificationFilter {
                        prefix: Some("reports/".to_owned()),
                        suffix: Some(".csv".to_owned()),
                    }),
                    id: Some("queue-config".to_owned()),
                    queue_arn: "arn:aws:sqs:eu-west-2:000000000000:demo"
                        .parse()
                        .expect("queue ARN should parse"),
                }],
                topic_configurations: vec![TopicNotificationConfiguration {
                    events: vec!["s3:ObjectCreated:Put".to_owned()],
                    filter: None,
                    id: Some("topic-config".to_owned()),
                    topic_arn: "arn:aws:sns:eu-west-2:000000000000:demo"
                        .parse()
                        .expect("topic ARN should parse"),
                }],
            },
        )
        .expect("notification config should store");

    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"name,age\njane,30\n".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/csv".to_owned()),
                key: "reports/data.csv".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("matching object write should succeed");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"ignored".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "notes.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("non-matching object write should succeed");

    let published = transport.published();
    assert_eq!(published.len(), 3);
    assert_eq!(published[0].event_name, "ObjectCreated:Put");
    assert_eq!(published[0].configuration_id.as_deref(), Some("queue-config"));
    assert_eq!(
        published[0].destination_arn,
        "arn:aws:sqs:eu-west-2:000000000000:demo"
            .parse()
            .expect("queue ARN should parse")
    );
    assert_eq!(published[0].key, "reports/data.csv");
    assert_eq!(published[1].configuration_id.as_deref(), Some("topic-config"));
    assert_eq!(
        published[1].destination_arn,
        "arn:aws:sns:eu-west-2:000000000000:demo"
            .parse()
            .expect("topic ARN should parse")
    );
    assert_eq!(published[2].event_name, "ObjectCreated:Put");
    assert_eq!(published[2].configuration_id.as_deref(), Some("topic-config"));
    assert_eq!(
        published[2].destination_arn,
        "arn:aws:sns:eu-west-2:000000000000:demo"
            .parse()
            .expect("topic ARN should parse")
    );
    assert_eq!(published[2].key, "notes.txt");
}

#[test]
fn s3_notifications_object_lock_rejects_unauthorized_governance_bypass() {
    let service = service(1_710_000_021);
    let scope = scope();
    service
        .create_bucket(
            &scope,
            CreateBucketInput {
                name: "locked".to_owned(),
                object_lock_enabled: true,
                region: scope.region().clone(),
            },
        )
        .expect("object lock bucket should be created");
    service
        .put_object_lock_configuration(
            &scope,
            "locked",
            BucketObjectLockConfiguration {
                object_lock_enabled: true,
                default_retention: Some(DefaultObjectLockRetention {
                    mode: ObjectLockMode::Governance,
                    period: DefaultRetentionPeriod::Days(1),
                }),
            },
        )
        .expect("object lock configuration should store");

    let put = service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "locked".to_owned(),
                content_type: None,
                key: "doc.txt".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("locked object should store");

    let retention_error = service
        .put_object_retention(
            &scope,
            PutObjectRetentionInput {
                bucket: "locked".to_owned(),
                bypass_governance: true,
                bypass_governance_authorized: false,
                key: "doc.txt".to_owned(),
                retention: ObjectRetention {
                    mode: Some(ObjectLockMode::Governance),
                    retain_until_epoch_seconds: Some(1_710_000_100),
                },
                version_id: put.version_id.clone(),
            },
        )
        .expect_err("unauthorized governance bypass should fail");
    service
        .put_object_legal_hold(
            &scope,
            PutObjectLegalHoldInput {
                bucket: "locked".to_owned(),
                key: "doc.txt".to_owned(),
                status: LegalHoldStatus::On,
                version_id: put.version_id.clone(),
            },
        )
        .expect("legal hold should store");
    let legal_hold = service
        .get_object_legal_hold(
            &scope,
            "locked",
            "doc.txt",
            put.version_id.as_deref(),
        )
        .expect("legal hold should read");
    let delete_error = service
        .delete_object(
            &scope,
            DeleteObjectInput {
                bucket: "locked".to_owned(),
                bypass_governance: false,
                bypass_governance_authorized: false,
                key: "doc.txt".to_owned(),
                version_id: put.version_id,
            },
        )
        .expect_err("legal hold should block deletion");

    assert!(matches!(retention_error, S3Error::AccessDenied { .. }));
    assert_eq!(legal_hold.status, LegalHoldStatus::On);
    assert!(matches!(delete_error, S3Error::AccessDenied { .. }));
}

#[test]
fn s3_notifications_select_filters_csv_rows() {
    let service = service(1_710_000_022);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"Name,Age\nJane,30\nBob,20\n".to_vec(),
                bucket: "demo".to_owned(),
                content_type: Some("text/csv".to_owned()),
                key: "records.csv".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("CSV object should store");

    let output = service
        .select_object_content(
            &scope,
            SelectObjectContentInput {
                bucket: "demo".to_owned(),
                csv_input: CsvInputSerialization {
                    file_header_info: CsvFileHeaderInfo::Use,
                    ..CsvInputSerialization::default()
                },
                csv_output: CsvOutputSerialization::default(),
                expression:
                    "SELECT s.\"Name\" FROM s3object s WHERE s.\"Age\" > 21"
                        .to_owned(),
                key: "records.csv".to_owned(),
                version_id: None,
            },
        )
        .expect("CSV select should succeed");

    assert_eq!(output.records, "Jane\n");
    assert!(output.stats.bytes_processed > 0);
    assert!(output.stats.bytes_returned > 0);
}

#[test]
fn s3_notifications_restore_reports_explicit_not_implemented() {
    let service = service(1_710_000_023);
    let scope = scope();
    create_bucket(&service, &scope, "demo");
    service
        .put_object(
            &scope,
            PutObjectInput {
                body: b"payload".to_vec(),
                bucket: "demo".to_owned(),
                content_type: None,
                key: "archive.bin".to_owned(),
                metadata: BTreeMap::new(),
                object_lock: None,
                tags: BTreeMap::new(),
            },
        )
        .expect("archive object should store");

    let error = service
        .restore_object(&scope, "demo", "archive.bin", None)
        .expect_err("restore should fail explicitly");

    assert!(matches!(
        error,
        S3Error::InvalidArgument {
            code: "NotImplemented",
            status_code: 501,
            ..
        }
    ));
}
