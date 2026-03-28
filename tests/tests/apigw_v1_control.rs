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

use aws_sdk_apigateway::Client as ApiGatewayClient;
use aws_sdk_apigateway::error::ProvideErrorMetadata;
use aws_sdk_apigateway::types::{
    ApiStage, AuthorizerType, ConnectionType, EndpointConfiguration,
    EndpointType, IntegrationType, SecurityPolicy,
};
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("apigw_v1_control");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk-apigw-v1-control-{id}")
}

fn authorizer_uri(name: &str) -> String {
    format!(
        "arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/\
         arn:aws:lambda:eu-west-2:000000000000:function:{name}/invocations"
    )
}

#[tokio::test]
async fn apigw_v1_control_rest_api_crud_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ApiGatewayClient::new(&config);

    let root = unique_root();
    let api_name = format!("{root}-api");
    let authorizer_name = format!("{root}-authorizer");
    let validator_name = format!("{root}-validator");
    let created_api = client
        .create_rest_api()
        .name(&api_name)
        .description("demo")
        .tags("env", "test")
        .send()
        .await
        .expect("REST API should create");
    let api_id =
        created_api.id().expect("api id should be present").to_owned();
    let root_resource_id = created_api
        .root_resource_id()
        .expect("root resource id should be present")
        .to_owned();

    let fetched_api = client
        .get_rest_api()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("REST API should load");
    assert_eq!(fetched_api.name(), Some(api_name.as_str()));

    let listed_apis =
        client.get_rest_apis().send().await.expect("REST APIs should list");
    assert!(
        listed_apis
            .items()
            .iter()
            .any(|api| api.id() == Some(api_id.as_str()))
    );

    let created_resource = client
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("pets")
        .send()
        .await
        .expect("resource should create");
    let resource_id = created_resource
        .id()
        .expect("resource id should be present")
        .to_owned();
    assert_eq!(created_resource.path(), Some("/pets"));

    let authorizer = client
        .create_authorizer()
        .rest_api_id(&api_id)
        .name(&authorizer_name)
        .r#type(AuthorizerType::Token)
        .authorizer_uri(authorizer_uri(&authorizer_name))
        .identity_source("method.request.header.Authorization")
        .authorizer_result_ttl_in_seconds(300)
        .send()
        .await
        .expect("authorizer should create");
    let authorizer_id =
        authorizer.id().expect("authorizer id should be present").to_owned();

    let request_validator = client
        .create_request_validator()
        .rest_api_id(&api_id)
        .name(&validator_name)
        .validate_request_parameters(true)
        .send()
        .await
        .expect("request validator should create");
    let request_validator_id = request_validator
        .id()
        .expect("request validator id should be present")
        .to_owned();

    let created_method = client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&resource_id)
        .http_method("GET")
        .authorization_type("CUSTOM")
        .authorizer_id(&authorizer_id)
        .request_validator_id(&request_validator_id)
        .operation_name("ListPets")
        .request_parameters("method.request.header.Accept", false)
        .send()
        .await
        .expect("method should create");
    assert_eq!(created_method.http_method(), Some("GET"));
    assert_eq!(created_method.authorizer_id(), Some(authorizer_id.as_str()));
    assert_eq!(
        created_method.request_validator_id(),
        Some(request_validator_id.as_str())
    );

    let method_response = client
        .put_method_response()
        .rest_api_id(&api_id)
        .resource_id(&resource_id)
        .http_method("GET")
        .status_code("200")
        .response_models("application/json", "Empty")
        .response_parameters("method.response.header.Content-Type", true)
        .send()
        .await
        .expect("method response should create");
    assert_eq!(method_response.status_code(), Some("200"));

    client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&resource_id)
        .http_method("GET")
        .r#type(IntegrationType::HttpProxy)
        .integration_http_method("GET")
        .uri("https://example.com/pets")
        .passthrough_behavior("WHEN_NO_MATCH")
        .send()
        .await
        .expect("integration should create");

    let deployment = client
        .create_deployment()
        .rest_api_id(&api_id)
        .description("first")
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();

    let created_stage = client
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .description("dev stage")
        .variables("lambdaAlias", "live")
        .tags("stage", "dev")
        .send()
        .await
        .expect("stage should create");
    assert_eq!(created_stage.stage_name(), Some("dev"));

    let fetched_stage = client
        .get_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .send()
        .await
        .expect("stage should load");
    assert_eq!(fetched_stage.deployment_id(), Some(deployment_id.as_str()));
    assert_eq!(fetched_stage.description(), Some("dev stage"));
    assert_eq!(
        fetched_stage
            .variables()
            .and_then(|variables| variables.get("lambdaAlias"))
            .map(String::as_str),
        Some("live")
    );
    assert_eq!(
        fetched_stage
            .tags()
            .and_then(|tags| tags.get("stage"))
            .map(String::as_str),
        Some("dev")
    );

    let fetched_method = client
        .get_method()
        .rest_api_id(&api_id)
        .resource_id(&resource_id)
        .http_method("GET")
        .send()
        .await
        .expect("method should load");
    assert_eq!(fetched_method.operation_name(), Some("ListPets"));
    assert_eq!(fetched_method.authorizer_id(), Some(authorizer_id.as_str()));
    assert_eq!(
        fetched_method.request_validator_id(),
        Some(request_validator_id.as_str())
    );
    assert_eq!(
        fetched_method
            .method_integration()
            .and_then(|integration| integration.r#type())
            .map(|type_| type_.as_str()),
        Some("HTTP_PROXY")
    );
    assert!(
        fetched_method
            .method_responses()
            .is_some_and(|responses| responses.contains_key("200"))
    );

    let listed_resources = client
        .get_resources()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("resources should list");
    assert_eq!(listed_resources.items().len(), 2);

    let listed_authorizers = client
        .get_authorizers()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("authorizers should list");
    assert_eq!(listed_authorizers.items().len(), 1);

    let listed_validators = client
        .get_request_validators()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("request validators should list");
    assert_eq!(listed_validators.items().len(), 1);

    let api_arn = format!("arn:aws:apigateway:eu-west-2::/restapis/{api_id}");
    client
        .tag_resource()
        .resource_arn(&api_arn)
        .tags("team", "edge")
        .send()
        .await
        .expect("tags should attach");
    let tags = client
        .get_tags()
        .resource_arn(&api_arn)
        .send()
        .await
        .expect("tags should load");
    assert_eq!(
        tags.tags().and_then(|tags| tags.get("env")).map(String::as_str),
        Some("test")
    );
    assert_eq!(
        tags.tags().and_then(|tags| tags.get("team")).map(String::as_str),
        Some("edge")
    );

    client
        .untag_resource()
        .resource_arn(&api_arn)
        .tag_keys("team")
        .send()
        .await
        .expect("tags should detach");
    let tags = client
        .get_tags()
        .resource_arn(&api_arn)
        .send()
        .await
        .expect("tags should reload");
    assert_eq!(
        tags.tags().and_then(|tags| tags.get("team")).map(String::as_str),
        None
    );

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn apigw_v1_control_api_keys_usage_plans_and_domains_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ApiGatewayClient::new(&config);

    let root = unique_root();
    let api_name = format!("{root}-api");
    let domain_name = format!("{root}.example.test");
    let created_api = client
        .create_rest_api()
        .name(&api_name)
        .send()
        .await
        .expect("REST API should create");
    let api_id =
        created_api.id().expect("api id should be present").to_owned();

    let deployment = client
        .create_deployment()
        .rest_api_id(&api_id)
        .description("first")
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();

    client
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("stage should create");

    let created_api_key = client
        .create_api_key()
        .name(format!("{root}-key"))
        .description("client key")
        .value("demo-secret")
        .enabled(true)
        .tags("scope", "qa")
        .send()
        .await
        .expect("API key should create");
    let api_key_id =
        created_api_key.id().expect("api key id should be present").to_owned();
    assert_eq!(created_api_key.value(), Some("demo-secret"));

    let listed_api_keys = client
        .get_api_keys()
        .include_values(true)
        .send()
        .await
        .expect("API keys should list");
    assert!(
        listed_api_keys
            .items()
            .iter()
            .any(|api_key| api_key.id() == Some(api_key_id.as_str()))
    );

    let usage_plan = client
        .create_usage_plan()
        .name(format!("{root}-plan"))
        .description("basic")
        .api_stages(ApiStage::builder().api_id(&api_id).stage("dev").build())
        .tags("tier", "basic")
        .send()
        .await
        .expect("usage plan should create");
    let usage_plan_id =
        usage_plan.id().expect("usage plan id should be present").to_owned();
    assert_eq!(
        usage_plan.api_stages().first().and_then(|stage| stage.stage()),
        Some("dev")
    );

    let usage_plan_key = client
        .create_usage_plan_key()
        .usage_plan_id(&usage_plan_id)
        .key_id(&api_key_id)
        .key_type("API_KEY")
        .send()
        .await
        .expect("usage plan key should create");
    assert_eq!(usage_plan_key.id(), Some(api_key_id.as_str()));

    let listed_usage_plan_keys = client
        .get_usage_plan_keys()
        .usage_plan_id(&usage_plan_id)
        .send()
        .await
        .expect("usage plan keys should list");
    assert_eq!(listed_usage_plan_keys.items().len(), 1);

    let created_domain = client
        .create_domain_name()
        .domain_name(&domain_name)
        .certificate_name("cert")
        .security_policy(SecurityPolicy::Tls12)
        .endpoint_configuration(
            EndpointConfiguration::builder()
                .types(EndpointType::Regional)
                .build(),
        )
        .tags("owner", "qa")
        .send()
        .await
        .expect("domain should create");
    assert_eq!(created_domain.domain_name(), Some(domain_name.as_str()));
    assert_eq!(
        created_domain.security_policy().map(SecurityPolicy::as_str),
        Some("TLS_1_2")
    );

    let created_mapping = client
        .create_base_path_mapping()
        .domain_name(&domain_name)
        .base_path("v1")
        .rest_api_id(&api_id)
        .stage("dev")
        .send()
        .await
        .expect("base path mapping should create");
    assert_eq!(created_mapping.base_path(), Some("v1"));

    let fetched_domain = client
        .get_domain_name()
        .domain_name(&domain_name)
        .send()
        .await
        .expect("domain should load");
    assert_eq!(fetched_domain.domain_name(), Some(domain_name.as_str()));
    assert_eq!(
        fetched_domain.regional_hosted_zone_id(),
        Some("Z2OJLYMUO9EFXC")
    );

    let fetched_mapping = client
        .get_base_path_mapping()
        .domain_name(&domain_name)
        .base_path("v1")
        .send()
        .await
        .expect("base path mapping should load");
    assert_eq!(fetched_mapping.rest_api_id(), Some(api_id.as_str()));
    assert_eq!(fetched_mapping.stage(), Some("dev"));

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn apigw_v1_control_reports_explicit_errors() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ApiGatewayClient::new(&config);

    let root = unique_root();
    let created_api = client
        .create_rest_api()
        .name(format!("{root}-api"))
        .send()
        .await
        .expect("REST API should create");
    let api_id =
        created_api.id().expect("api id should be present").to_owned();
    let root_resource_id = created_api
        .root_resource_id()
        .expect("root resource id should be present")
        .to_owned();

    client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&root_resource_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .expect("method should create");

    let duplicate_error = client
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(&root_resource_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .expect_err("duplicate method should fail");
    assert_eq!(duplicate_error.code(), Some("ConflictException"));
    assert!(
        duplicate_error
            .message()
            .unwrap_or_default()
            .contains("Method GET for resource")
    );

    let private_integration_error = client
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(&root_resource_id)
        .http_method("GET")
        .r#type(IntegrationType::HttpProxy)
        .integration_http_method("GET")
        .uri("https://example.com/private")
        .connection_type(ConnectionType::VpcLink)
        .connection_id("vpclink-123")
        .send()
        .await
        .expect_err("private integration should fail");
    assert_eq!(private_integration_error.code(), Some("BadRequestException"));
    assert!(
        private_integration_error
            .message()
            .unwrap_or_default()
            .contains("private integrations")
    );

    let deployment = client
        .create_deployment()
        .rest_api_id(&api_id)
        .description("first")
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();

    let cache_stage_error = client
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .cache_cluster_enabled(true)
        .send()
        .await
        .expect_err("cache configuration should fail");
    assert_eq!(cache_stage_error.code(), Some("BadRequestException"));
    assert!(
        cache_stage_error
            .message()
            .unwrap_or_default()
            .contains("cache configuration")
    );

    assert!(runtime.state_directory().exists());
}
