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

use aws_sdk_apigatewayv2::Client as ApiGatewayV2Client;
use aws_sdk_apigatewayv2::error::ProvideErrorMetadata;
use aws_sdk_apigatewayv2::types::{
    AuthorizerType, IntegrationType, ProtocolType,
};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk-apigw-v2-control-{id}")
}

fn authorizer_uri(name: &str) -> String {
    format!(
        "arn:aws:apigateway:eu-west-2:lambda:path/2015-03-31/functions/\
         arn:aws:lambda:eu-west-2:000000000000:function:{name}/invocations"
    )
}

#[tokio::test]
async fn apigw_v2_control_http_api_crud_round_trip() {
    let runtime = RuntimeServer::spawn("sdk-apigw-v2-control").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = ApiGatewayV2Client::new(&config);

    let root = unique_root();
    let api_name = format!("{root}-api");
    let authorizer_name = format!("{root}-authorizer");
    let created_api = client
        .create_api()
        .name(&api_name)
        .protocol_type(ProtocolType::Http)
        .send()
        .await
        .expect("HTTP API should create");
    let api_id =
        created_api.api_id().expect("api id should be present").to_owned();

    let fetched_api = client
        .get_api()
        .api_id(&api_id)
        .send()
        .await
        .expect("HTTP API should load");
    assert_eq!(fetched_api.name(), Some(api_name.as_str()));

    let listed_apis =
        client.get_apis().send().await.expect("HTTP APIs should list");
    assert!(
        listed_apis
            .items()
            .iter()
            .any(|api| api.api_id() == Some(api_id.as_str()))
    );

    let integration = client
        .create_integration()
        .api_id(&api_id)
        .integration_type(IntegrationType::HttpProxy)
        .integration_uri("http://example.com/pets")
        .send()
        .await
        .expect("integration should create");
    let integration_id = integration
        .integration_id()
        .expect("integration id should be present")
        .to_owned();

    let authorizer = client
        .create_authorizer()
        .api_id(&api_id)
        .name(&authorizer_name)
        .authorizer_type(AuthorizerType::Request)
        .authorizer_uri(authorizer_uri(&authorizer_name))
        .set_identity_source(Some(vec![
            "$request.header.Authorization".to_owned(),
        ]))
        .authorizer_payload_format_version("2.0")
        .send()
        .await
        .expect("authorizer should create");
    let authorizer_id = authorizer
        .authorizer_id()
        .expect("authorizer id should be present")
        .to_owned();

    let route = client
        .create_route()
        .api_id(&api_id)
        .route_key("GET /pets")
        .target(format!("integrations/{integration_id}"))
        .send()
        .await
        .expect("route should create");
    let route_id =
        route.route_id().expect("route id should be present").to_owned();

    let deployment = client
        .create_deployment()
        .api_id(&api_id)
        .description("first")
        .send()
        .await
        .expect("deployment should create");
    let deployment_id = deployment
        .deployment_id()
        .expect("deployment id should be present")
        .to_owned();

    let stage = client
        .create_stage()
        .api_id(&api_id)
        .stage_name("dev")
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("stage should create");
    assert_eq!(stage.stage_name(), Some("dev"));

    let fetched_route = client
        .get_route()
        .api_id(&api_id)
        .route_id(&route_id)
        .send()
        .await
        .expect("route should load");
    assert_eq!(fetched_route.route_key(), Some("GET /pets"));

    let listed_routes = client
        .get_routes()
        .api_id(&api_id)
        .send()
        .await
        .expect("routes should list");
    assert_eq!(listed_routes.items().len(), 1);

    let listed_integrations = client
        .get_integrations()
        .api_id(&api_id)
        .send()
        .await
        .expect("integrations should list");
    assert_eq!(listed_integrations.items().len(), 1);

    let listed_authorizers = client
        .get_authorizers()
        .api_id(&api_id)
        .send()
        .await
        .expect("authorizers should list");
    assert_eq!(listed_authorizers.items().len(), 1);

    let listed_stages = client
        .get_stages()
        .api_id(&api_id)
        .send()
        .await
        .expect("stages should list");
    assert_eq!(listed_stages.items().len(), 1);

    let listed_deployments = client
        .get_deployments()
        .api_id(&api_id)
        .send()
        .await
        .expect("deployments should list");
    assert_eq!(listed_deployments.items().len(), 1);

    let invalid_route = client
        .create_route()
        .api_id(&api_id)
        .route_key("GET pets")
        .send()
        .await
        .expect_err("invalid route key should fail");
    assert_eq!(invalid_route.code(), Some("BadRequestException"));
    assert!(
        invalid_route
            .message()
            .unwrap_or_default()
            .contains("invalid routeKey")
    );

    client
        .delete_route()
        .api_id(&api_id)
        .route_id(&route_id)
        .send()
        .await
        .expect("route should delete");
    client
        .delete_authorizer()
        .api_id(&api_id)
        .authorizer_id(&authorizer_id)
        .send()
        .await
        .expect("authorizer should delete");
    client
        .delete_stage()
        .api_id(&api_id)
        .stage_name("dev")
        .send()
        .await
        .expect("stage should delete");
    client
        .delete_deployment()
        .api_id(&api_id)
        .deployment_id(&deployment_id)
        .send()
        .await
        .expect("deployment should delete");
    client
        .delete_integration()
        .api_id(&api_id)
        .integration_id(&integration_id)
        .send()
        .await
        .expect("integration should delete");
    client
        .delete_api()
        .api_id(&api_id)
        .send()
        .await
        .expect("api should delete");

    runtime.shutdown().await;
}
