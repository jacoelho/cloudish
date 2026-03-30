use crate::runtime::EdgeRouter;
use auth::Authenticator;
use aws::{RuntimeDefaults, ServiceName, SharedAdvertisedEdge};
#[cfg(test)]
pub(crate) use edge_runtime::FixedClock;
#[cfg(any(feature = "apigateway", feature = "sns"))]
use edge_runtime::HttpForwarder;
#[cfg(feature = "lambda")]
use edge_runtime::LambdaExecutor;
use edge_runtime::{RuntimeServices, TestRuntimeBuilder};
use std::sync::Arc;

pub(crate) fn runtime_defaults(label: &str) -> RuntimeDefaults {
    RuntimeDefaults::try_new(
        Some("000000000000".to_owned()),
        Some("eu-west-2".to_owned()),
        Some(format!("/tmp/{label}")),
    )
    .expect("test defaults should be valid")
}

pub(crate) fn router(label: &str) -> EdgeRouter {
    build_router(TestRuntimeBuilder::new(label), label)
}

pub(crate) fn router_with_services(
    label: &str,
    enabled_services: &[ServiceName],
) -> EdgeRouter {
    build_router(
        TestRuntimeBuilder::new(label)
            .with_enabled_services(enabled_services.iter().copied()),
        label,
    )
}

pub(crate) fn router_with_runtime(
    label: &str,
) -> (EdgeRouter, RuntimeServices) {
    build_router_with_runtime(TestRuntimeBuilder::new(label), label)
}

#[cfg(any(feature = "apigateway", feature = "sns"))]
pub(crate) fn router_with_http_forwarder(
    label: &str,
    http_forwarder: Option<Arc<dyn HttpForwarder + Send + Sync>>,
) -> EdgeRouter {
    build_router(
        TestRuntimeBuilder::new(label).with_http_forwarder(http_forwarder),
        label,
    )
}

#[cfg(feature = "lambda")]
pub(crate) fn router_with_lambda_executor(
    label: &str,
    executor: Arc<dyn LambdaExecutor + Send + Sync>,
) -> (EdgeRouter, RuntimeServices) {
    build_router_with_runtime(
        TestRuntimeBuilder::new(label).with_lambda_executor(executor),
        label,
    )
}

fn build_router(builder: TestRuntimeBuilder, label: &str) -> EdgeRouter {
    let (router, _) = build_router_with_runtime(builder, label);
    router
}

fn build_router_with_runtime(
    builder: TestRuntimeBuilder,
    label: &str,
) -> (EdgeRouter, RuntimeServices) {
    let defaults = runtime_defaults(label);
    let authenticator = Authenticator::new(defaults.clone());
    let assembly = builder.build().expect("test runtime should build");
    let (services, runtime) = assembly.into_parts();
    let router = EdgeRouter::new(
        defaults,
        SharedAdvertisedEdge::default(),
        authenticator,
        services,
        runtime.clone(),
    );

    (router, runtime)
}
