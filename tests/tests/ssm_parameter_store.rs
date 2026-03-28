#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_ssm::Client as SsmClient;
use aws_sdk_ssm::error::ProvideErrorMetadata;
use aws_sdk_ssm::types::{ParameterType, ResourceTypeForTagging, Tag};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("ssm_parameter_store");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk-ssm-{id}")
}

#[tokio::test]
async fn ssm_parameter_store_core_history_and_tags_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = SsmClient::new(&config);

    let root = unique_root();
    let host = format!("/{root}/db/host");
    let port = format!("/{root}/db/port");
    let nested = format!("/{root}/db/readonly/user");
    let missing = format!("/{root}/missing");

    let created = client
        .put_parameter()
        .name(&host)
        .value("localhost")
        .r#type(ParameterType::String)
        .send()
        .await
        .expect("host parameter should create");
    assert_eq!(created.version(), 1);

    client
        .put_parameter()
        .name(&port)
        .value("5432")
        .r#type(ParameterType::String)
        .send()
        .await
        .expect("port parameter should create");
    client
        .put_parameter()
        .name(&nested)
        .value("readonly")
        .r#type(ParameterType::String)
        .send()
        .await
        .expect("nested parameter should create");

    let fetched = client
        .get_parameter()
        .name(&host)
        .send()
        .await
        .expect("host parameter should fetch");
    assert_eq!(
        fetched.parameter().and_then(|parameter| parameter.value()),
        Some("localhost")
    );
    assert_eq!(
        fetched.parameter().map(|parameter| parameter.version()),
        Some(1)
    );

    let batch = client
        .get_parameters()
        .set_names(Some(vec![missing.clone(), port.clone(), host.clone()]))
        .send()
        .await
        .expect("batch get should succeed");
    let fetched_names = batch
        .parameters()
        .iter()
        .filter_map(|parameter| parameter.name())
        .collect::<Vec<_>>();
    assert_eq!(fetched_names, vec![host.as_str(), port.as_str()]);
    assert_eq!(batch.invalid_parameters(), [missing.as_str()]);

    let updated = client
        .put_parameter()
        .name(&host)
        .value("db.example.com")
        .overwrite(true)
        .r#type(ParameterType::String)
        .send()
        .await
        .expect("host parameter should update");
    assert_eq!(updated.version(), 2);

    let labeled = client
        .label_parameter_version()
        .name(&host)
        .parameter_version(1)
        .set_labels(Some(vec!["stable".to_owned()]))
        .send()
        .await
        .expect("label should attach");
    assert_eq!(labeled.parameter_version(), 1);
    assert!(labeled.invalid_labels().is_empty());

    let history = client
        .get_parameter_history()
        .name(&host)
        .send()
        .await
        .expect("parameter history should load");
    assert_eq!(
        history
            .parameters()
            .iter()
            .map(|parameter| parameter.version())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        history.parameters()[0].labels().to_vec(),
        vec!["stable".to_owned()]
    );

    let non_recursive = client
        .get_parameters_by_path()
        .path(format!("/{root}/db"))
        .recursive(false)
        .send()
        .await
        .expect("non-recursive path lookup should succeed");
    assert_eq!(
        non_recursive
            .parameters()
            .iter()
            .filter_map(|parameter| parameter.name())
            .collect::<Vec<_>>(),
        vec![host.as_str(), port.as_str()]
    );
    let recursive = client
        .get_parameters_by_path()
        .path(format!("/{root}/db"))
        .recursive(true)
        .send()
        .await
        .expect("recursive path lookup should succeed");
    assert_eq!(
        recursive
            .parameters()
            .iter()
            .filter_map(|parameter| parameter.name())
            .collect::<Vec<_>>(),
        vec![host.as_str(), port.as_str(), nested.as_str()]
    );

    let described = client
        .describe_parameters()
        .send()
        .await
        .expect("describe parameters should succeed");
    assert!(
        described
            .parameters()
            .iter()
            .any(|parameter| parameter.name() == Some(host.as_str()))
    );

    client
        .add_tags_to_resource()
        .resource_type(ResourceTypeForTagging::Parameter)
        .resource_id(&host)
        .tags(
            Tag::builder()
                .key("env")
                .value("dev")
                .build()
                .expect("tag should build"),
        )
        .send()
        .await
        .expect("tags should attach");
    let tags = client
        .list_tags_for_resource()
        .resource_type(ResourceTypeForTagging::Parameter)
        .resource_id(&host)
        .send()
        .await
        .expect("tags should list");
    assert!(
        tags.tag_list()
            .iter()
            .any(|tag| tag.key() == "env" && tag.value() == "dev")
    );

    client
        .remove_tags_from_resource()
        .resource_type(ResourceTypeForTagging::Parameter)
        .resource_id(&host)
        .tag_keys("env")
        .send()
        .await
        .expect("tags should remove");
    let tags = client
        .list_tags_for_resource()
        .resource_type(ResourceTypeForTagging::Parameter)
        .resource_id(&host)
        .send()
        .await
        .expect("tags should list after removal");
    assert!(tags.tag_list().is_empty());

    let deleted = client
        .delete_parameters()
        .set_names(Some(vec![host.clone(), missing.clone()]))
        .send()
        .await
        .expect("batch delete should succeed");
    assert_eq!(deleted.deleted_parameters(), [host.as_str()]);
    assert_eq!(deleted.invalid_parameters(), [missing.as_str()]);

    client
        .delete_parameter()
        .name(&port)
        .send()
        .await
        .expect("single delete should succeed");
    let deleted_error = client
        .get_parameter()
        .name(&port)
        .send()
        .await
        .expect_err("deleted parameter should not load");
    assert_eq!(deleted_error.code(), Some("ParameterNotFound"));

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn ssm_parameter_store_reports_explicit_errors() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = SsmClient::new(&config);

    let root = unique_root();
    let name = format!("/{root}/config");
    let missing = format!("/{root}/missing");

    client
        .put_parameter()
        .name(&name)
        .value("value")
        .r#type(ParameterType::String)
        .send()
        .await
        .expect("seed parameter should create");

    let duplicate = client
        .put_parameter()
        .name(&name)
        .value("value")
        .r#type(ParameterType::String)
        .send()
        .await
        .expect_err("duplicate put without overwrite should fail");
    assert_eq!(duplicate.code(), Some("ParameterAlreadyExists"));

    let missing_get = client
        .get_parameter()
        .name(&missing)
        .send()
        .await
        .expect_err("missing parameter should fail");
    assert_eq!(missing_get.code(), Some("ParameterNotFound"));

    let missing_version = client
        .label_parameter_version()
        .name(&name)
        .parameter_version(99)
        .set_labels(Some(vec!["stable".to_owned()]))
        .send()
        .await
        .expect_err("missing version label should fail");
    assert_eq!(missing_version.code(), Some("ParameterVersionNotFound"));

    let missing_tags = client
        .list_tags_for_resource()
        .resource_type(ResourceTypeForTagging::Parameter)
        .resource_id(&missing)
        .send()
        .await
        .expect_err("missing resource tag lookup should fail");
    assert_eq!(missing_tags.code(), Some("InvalidResourceId"));

    assert!(runtime.state_directory().exists());
}
