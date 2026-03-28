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
#[path = "common/http_fixture.rs"]
mod http_fixture;
#[path = "common/lambda.rs"]
mod lambda_fixture;
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_apigateway::Client as ApiGatewayClient;
use aws_sdk_apigateway::types::IntegrationType;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime};
use reqwest::header::HOST;
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
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
    let runtime = RuntimeServer::spawn("sdk-apigw-v1-runtime-lambda").await;
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
        .get(format!("http://{}/dev/pets?view=full", runtime.address()))
        .header(HOST, format!("{api_id}.execute-api.localhost"))
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

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}

#[tokio::test]
async fn apigw_v1_runtime_http_proxy_custom_domain_and_missing_mapping() {
    let runtime = RuntimeServer::spawn("sdk-apigw-v1-runtime-domain").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let apigw = ApiGatewayClient::new(&config);
    let root = unique_root("apigw-v1-runtime-domain");
    let backend = http_fixture::OneShotHttpServer::spawn(
        b"HTTP/1.1 201 Created\r\nContent-Type: text/plain\r\nX-Upstream: ok\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_vec(),
    );

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
        .uri(format!("http://127.0.0.1:{}/backend", backend.address().port()))
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
    apigw
        .create_domain_name()
        .domain_name("api.example.test")
        .send()
        .await
        .expect("custom domain should create");
    apigw
        .create_base_path_mapping()
        .domain_name("api.example.test")
        .base_path("v1")
        .rest_api_id(&api_id)
        .stage("dev")
        .send()
        .await
        .expect("base path mapping should create");
    apigw
        .create_domain_name()
        .domain_name("orphan.example.test")
        .send()
        .await
        .expect("orphan domain should create");

    let response = reqwest::Client::new()
        .get(format!("http://{}/v1/pets?mode=test", runtime.address()))
        .header(HOST, "api.example.test")
        .send()
        .await
        .expect("custom-domain execute-api request should succeed");
    let missing_mapping = reqwest::Client::new()
        .get(format!("http://{}/pets", runtime.address()))
        .header(HOST, "orphan.example.test")
        .send()
        .await
        .expect("orphan custom-domain request should return an API error");

    assert_eq!(response.status(), 201);
    assert_eq!(
        response
            .headers()
            .get("x-upstream")
            .and_then(|value| value.to_str().ok()),
        Some("ok")
    );
    assert_eq!(response.text().await.expect("body should decode"), "ok");
    assert_eq!(missing_mapping.status(), 404);
    let missing_mapping_body: serde_json::Value = serde_json::from_str(
        &missing_mapping.text().await.expect("error body should decode"),
    )
    .expect("error body should decode");
    assert_eq!(missing_mapping_body["message"], "Not Found");

    backend.join();
    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}
