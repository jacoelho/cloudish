#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::lambda as lambda_fixture;
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_apigateway::Client as ApiGatewayClient;
use aws_sdk_apigateway::types::{ApiStage, IntegrationType};
use aws_sdk_iam::Client as IamClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime};
use reqwest::header::HOST;
use runtime::SharedRuntimeLease;
use sdk::SdkSmokeTarget;

static SHARED_RUNTIME: runtime::SharedRuntime =
    runtime::SharedRuntime::new("apigw_v1_runtime");

async fn shared_runtime() -> SharedRuntimeLease<'static> {
    SHARED_RUNTIME.acquire().await
}

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
}

fn lambda_integration_uri(function_name: &str) -> String {
    format!(
        "arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/\
         arn:aws:lambda:eu-west-2:000000000000:function:{function_name}/\
         invocations"
    )
}

#[tokio::test]
async fn apigw_v1_runtime_lambda_proxy_round_trip() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let apigw = ApiGatewayClient::new(&config);
    let iam = IamClient::new(&config);
    let lambda = LambdaClient::new(&config);
    let role_name = lambda_fixture::unique_name("apigw-runtime-role");
    let role_arn = lambda_fixture::create_lambda_role(&iam, &role_name).await;
    let function_name = lambda_fixture::unique_name("apigw-runtime-proxy");
    let root = unique_root("apigw-v1-runtime-lambda");

    lambda
        .create_function()
        .function_name(&function_name)
        .role(&role_arn)
        .runtime(Runtime::from("provided.al2"))
        .handler("bootstrap.handler")
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(lambda_fixture::proxy_bootstrap_zip()))
                .build(),
        )
        .send()
        .await
        .expect("lambda function should create");

    let api = apigw
        .create_rest_api()
        .name(format!("{root}-api"))
        .send()
        .await
        .expect("REST API should create");
    let api_id = api.id().expect("api id should be present").to_owned();
    let root_resource_id = api
        .root_resource_id()
        .expect("root resource should be present")
        .to_owned();
    let resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("pets")
        .send()
        .await
        .expect("resource should create");
    let resource_id = resource.id().expect("resource id should be present");

    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(resource_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .expect("method should create");
    apigw
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(resource_id)
        .http_method("GET")
        .r#type(IntegrationType::AwsProxy)
        .integration_http_method("POST")
        .uri(lambda_integration_uri(&function_name))
        .send()
        .await
        .expect("lambda proxy integration should create");
    let deployment = apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();
    apigw
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("stage should create");

    lambda
        .add_permission()
        .function_name(&function_name)
        .statement_id("allow-apigw")
        .action("lambda:InvokeFunction")
        .principal("apigateway.amazonaws.com")
        .source_arn(format!(
            "arn:aws:execute-api:eu-west-2:000000000000:{api_id}/dev/GET/pets"
        ))
        .send()
        .await
        .expect("API Gateway permission should be added");

    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/__aws/execute-api/{api_id}/dev/pets?view=full",
            runtime.address()
        ))
        .header("X-Test", "true")
        .send()
        .await
        .expect("execute-api request should succeed");

    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("x-apigw-runtime")
            .and_then(|value| value.to_str().ok()),
        Some("lambda-proxy")
    );
    let event: serde_json::Value = serde_json::from_str(
        &response.text().await.expect("body should decode"),
    )
    .expect("lambda proxy body should decode");
    assert_eq!(event["resource"], "/pets");
    assert_eq!(event["path"], "/pets");
    assert_eq!(event["httpMethod"], "GET");
    assert_eq!(event["headers"]["x-test"], "true");
    assert_eq!(event["queryStringParameters"]["view"], "full");
    assert_eq!(event["requestContext"]["stage"], "dev");
    assert_eq!(event["requestContext"]["identity"]["sourceIp"], "127.0.0.1");

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn apigw_v1_runtime_http_proxy_custom_hosts_fall_through() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let apigw = ApiGatewayClient::new(&config);
    let root = unique_root("apigw-v1-runtime-domain");

    let api = apigw
        .create_rest_api()
        .name(format!("{root}-api"))
        .send()
        .await
        .expect("REST API should create");
    let api_id = api.id().expect("api id should be present").to_owned();
    let root_resource_id = api
        .root_resource_id()
        .expect("root resource should be present")
        .to_owned();
    let resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("pets")
        .send()
        .await
        .expect("resource should create");
    let resource_id = resource.id().expect("resource id should be present");

    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(resource_id)
        .http_method("GET")
        .authorization_type("NONE")
        .send()
        .await
        .expect("method should create");
    apigw
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(resource_id)
        .http_method("GET")
        .r#type(IntegrationType::HttpProxy)
        .integration_http_method("GET")
        .uri("http://127.0.0.1:65535/backend")
        .send()
        .await
        .expect("HTTP proxy integration should create");
    let deployment = apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();
    apigw
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("stage should create");
    let custom_host = reqwest::Client::new()
        .get(format!("http://{}/pets", runtime.address()))
        .header(HOST, "api.example.test")
        .send()
        .await
        .expect("custom-host request should return a generic router error");

    assert_eq!(custom_host.status(), 404);
    let custom_host_body: serde_json::Value = serde_json::from_str(
        &custom_host.text().await.expect("error body should decode"),
    )
    .expect("error body should decode");
    assert_eq!(custom_host_body["message"], "not found");

    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn apigw_v1_runtime_required_api_key_rejects_missing_and_invalid_values_and_accepts_bound_key()
 {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let apigw = ApiGatewayClient::new(&config);
    let root = unique_root("apigw-v1-runtime-api-key");

    let api = apigw
        .create_rest_api()
        .name(format!("{root}-api"))
        .send()
        .await
        .expect("REST API should create");
    let api_id = api.id().expect("api id should be present").to_owned();
    let root_resource_id = api
        .root_resource_id()
        .expect("root resource should be present")
        .to_owned();
    let resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("pets")
        .send()
        .await
        .expect("resource should create");
    let resource_id = resource.id().expect("resource id should be present");

    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(resource_id)
        .http_method("GET")
        .authorization_type("NONE")
        .api_key_required(true)
        .send()
        .await
        .expect("method should create");
    apigw
        .put_integration()
        .rest_api_id(&api_id)
        .resource_id(resource_id)
        .http_method("GET")
        .r#type(IntegrationType::Mock)
        .request_templates(
            "application/json",
            r#"{"statusCode":200,"body":"ok"}"#,
        )
        .send()
        .await
        .expect("mock integration should create");
    let deployment = apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();
    apigw
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("stage should create");

    let api_key = apigw
        .create_api_key()
        .name(format!("{root}-key"))
        .value("demo-secret")
        .enabled(true)
        .send()
        .await
        .expect("API key should create");
    let api_key_id =
        api_key.id().expect("api key id should be present").to_owned();
    let usage_plan = apigw
        .create_usage_plan()
        .name(format!("{root}-plan"))
        .api_stages(ApiStage::builder().api_id(&api_id).stage("dev").build())
        .send()
        .await
        .expect("usage plan should create");
    let usage_plan_id =
        usage_plan.id().expect("usage plan id should be present").to_owned();
    apigw
        .create_usage_plan_key()
        .usage_plan_id(&usage_plan_id)
        .key_id(&api_key_id)
        .key_type("API_KEY")
        .send()
        .await
        .expect("usage plan key should create");

    let client = reqwest::Client::new();
    let url = format!(
        "http://{}/__aws/execute-api/{api_id}/dev/pets",
        runtime.address()
    );
    let missing_key =
        client.get(&url).send().await.expect("request should complete");
    let invalid_key = client
        .get(&url)
        .header("x-api-key", "invalid")
        .send()
        .await
        .expect("request should complete");
    let valid_key = client
        .get(&url)
        .header("x-api-key", "demo-secret")
        .send()
        .await
        .expect("request should complete");

    assert_eq!(missing_key.status(), 403);
    assert_eq!(invalid_key.status(), 403);
    assert_eq!(valid_key.status(), 200);
    assert_eq!(valid_key.text().await.expect("body should decode"), "ok");
    assert!(runtime.state_directory().exists());
}

#[tokio::test]
async fn apigw_v1_runtime_request_validators_enforce_required_inputs() {
    let runtime = shared_runtime().await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let apigw = ApiGatewayClient::new(&config);
    let root = unique_root("apigw-v1-runtime-validator");

    let api = apigw
        .create_rest_api()
        .name(format!("{root}-api"))
        .send()
        .await
        .expect("REST API should create");
    let api_id = api.id().expect("api id should be present").to_owned();
    let root_resource_id = api
        .root_resource_id()
        .expect("root resource should be present")
        .to_owned();
    let header_resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("header")
        .send()
        .await
        .expect("header resource should create");
    let query_resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("query")
        .send()
        .await
        .expect("query resource should create");
    let path_parent = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("path")
        .send()
        .await
        .expect("path parent should create");
    let path_resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(path_parent.id().expect("path parent id should be present"))
        .path_part("{pet_id}")
        .send()
        .await
        .expect("path resource should create");
    let body_required_resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("body-required")
        .send()
        .await
        .expect("body-required resource should create");
    let body_optional_resource = apigw
        .create_resource()
        .rest_api_id(&api_id)
        .parent_id(&root_resource_id)
        .path_part("body-optional")
        .send()
        .await
        .expect("body-optional resource should create");

    let parameter_validator = apigw
        .create_request_validator()
        .rest_api_id(&api_id)
        .name(format!("{root}-params"))
        .validate_request_parameters(true)
        .send()
        .await
        .expect("parameter validator should create");
    let parameter_validator_id = parameter_validator
        .id()
        .expect("parameter validator id should be present")
        .to_owned();
    let body_validator = apigw
        .create_request_validator()
        .rest_api_id(&api_id)
        .name(format!("{root}-body"))
        .validate_request_body(true)
        .send()
        .await
        .expect("body validator should create");
    let body_validator_id =
        body_validator.id().expect("body validator id should be present");
    let optional_body_validator = apigw
        .create_request_validator()
        .rest_api_id(&api_id)
        .name(format!("{root}-optional-body"))
        .validate_request_body(false)
        .send()
        .await
        .expect("optional body validator should create");
    let optional_body_validator_id = optional_body_validator
        .id()
        .expect("optional body validator id should be present");

    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(
            header_resource.id().expect("header resource id should exist"),
        )
        .http_method("GET")
        .authorization_type("NONE")
        .request_validator_id(&parameter_validator_id)
        .request_parameters("method.request.header.X-Test", true)
        .send()
        .await
        .expect("header method should create");
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(
            query_resource.id().expect("query resource id should exist"),
        )
        .http_method("GET")
        .authorization_type("NONE")
        .request_validator_id(&parameter_validator_id)
        .request_parameters("method.request.querystring.mode", true)
        .send()
        .await
        .expect("query method should create");
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(
            path_resource.id().expect("path resource id should exist"),
        )
        .http_method("GET")
        .authorization_type("NONE")
        .request_validator_id(&parameter_validator_id)
        .request_parameters("method.request.path.other", true)
        .send()
        .await
        .expect("path method should create");
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(
            body_required_resource
                .id()
                .expect("body-required resource id should exist"),
        )
        .http_method("POST")
        .authorization_type("NONE")
        .request_validator_id(body_validator_id)
        .send()
        .await
        .expect("body-required method should create");
    apigw
        .put_method()
        .rest_api_id(&api_id)
        .resource_id(
            body_optional_resource
                .id()
                .expect("body-optional resource id should exist"),
        )
        .http_method("POST")
        .authorization_type("NONE")
        .request_validator_id(optional_body_validator_id)
        .send()
        .await
        .expect("body-optional method should create");

    for (resource_id, method) in [
        (
            header_resource.id().expect("header resource id should exist"),
            "GET",
        ),
        (query_resource.id().expect("query resource id should exist"), "GET"),
        (path_resource.id().expect("path resource id should exist"), "GET"),
        (
            body_required_resource
                .id()
                .expect("body-required resource id should exist"),
            "POST",
        ),
        (
            body_optional_resource
                .id()
                .expect("body-optional resource id should exist"),
            "POST",
        ),
    ] {
        apigw
            .put_integration()
            .rest_api_id(&api_id)
            .resource_id(resource_id)
            .http_method(method)
            .r#type(IntegrationType::Mock)
            .request_templates(
                "application/json",
                r#"{"statusCode":200,"body":"ok"}"#,
            )
            .send()
            .await
            .expect("mock integration should create");
    }

    let deployment = apigw
        .create_deployment()
        .rest_api_id(&api_id)
        .send()
        .await
        .expect("deployment should create");
    let deployment_id =
        deployment.id().expect("deployment id should be present").to_owned();
    apigw
        .create_stage()
        .rest_api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("stage should create");

    let client = reqwest::Client::new();
    let base_url =
        format!("http://{}/__aws/execute-api/{api_id}/dev", runtime.address());
    let missing_header = client
        .get(format!("{base_url}/header"))
        .send()
        .await
        .expect("header request should complete");
    let missing_query = client
        .get(format!("{base_url}/query"))
        .send()
        .await
        .expect("query request should complete");
    let missing_path = client
        .get(format!("{base_url}/path/value"))
        .send()
        .await
        .expect("path request should complete");
    let missing_body = client
        .post(format!("{base_url}/body-required"))
        .send()
        .await
        .expect("body-required request should complete");
    let optional_body = client
        .post(format!("{base_url}/body-optional"))
        .send()
        .await
        .expect("body-optional request should complete");
    let valid_header = client
        .get(format!("{base_url}/header"))
        .header("x-test", "true")
        .send()
        .await
        .expect("header request should complete");
    let valid_query = client
        .get(format!("{base_url}/query?mode=test"))
        .send()
        .await
        .expect("query request should complete");

    assert_eq!(missing_header.status(), 400);
    assert_eq!(missing_query.status(), 400);
    assert_eq!(missing_path.status(), 400);
    assert_eq!(missing_body.status(), 400);
    assert_eq!(optional_body.status(), 200);
    assert_eq!(valid_header.status(), 200);
    assert_eq!(valid_query.status(), 200);
    assert_eq!(optional_body.text().await.expect("body should decode"), "ok");
    assert_eq!(valid_header.text().await.expect("body should decode"), "ok");
    assert_eq!(valid_query.text().await.expect("body should decode"), "ok");
    assert!(runtime.state_directory().exists());
}
