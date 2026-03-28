use crate::{ApiGatewayError, ExecuteApiError, ROOT_PATH};
use std::collections::BTreeMap;

pub use crate::v2::{
    CreateHttpApiAuthorizerInput, CreateHttpApiDeploymentInput,
    CreateHttpApiInput, CreateHttpApiIntegrationInput,
    CreateHttpApiRouteInput, CreateHttpApiStageInput, HttpApi,
    HttpApiAuthorizer, HttpApiCollection, HttpApiDeployment,
    HttpApiIntegration, HttpApiRoute, HttpApiStage, JwtConfiguration,
    UpdateHttpApiAuthorizerInput, UpdateHttpApiInput,
    UpdateHttpApiIntegrationInput, UpdateHttpApiRouteInput,
    UpdateHttpApiStageInput, map_lambda_proxy_response_v2,
};

pub(crate) trait HttpApiRouteDefinition {
    fn authorization_type(&self) -> &str;
    fn route_id(&self) -> &str;
    fn route_key(&self) -> &str;
    fn target(&self) -> Option<&str>;
}

pub(crate) fn normalize_route_key(
    route_key: &str,
) -> Result<String, ApiGatewayError> {
    if route_key == "$default" {
        return Ok("$default".to_owned());
    }
    let (method, raw_path) = route_key.split_once(' ').ok_or_else(|| {
        ApiGatewayError::Validation {
            message: format!("invalid routeKey {route_key:?}"),
        }
    })?;
    let method = crate::normalize_http_method(method)?;
    if !raw_path.starts_with('/') {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid routeKey {route_key:?}"),
        });
    }
    let path = normalize_route_path_pattern(raw_path)?;
    Ok(format!("{method} {path}"))
}

pub(crate) fn match_http_route<'a, Route>(
    routes: &'a BTreeMap<String, Route>,
    request_method: &str,
    request_path: &str,
) -> Result<MatchedHttpRoute<'a, Route>, ExecuteApiError>
where
    Route: HttpApiRouteDefinition,
{
    let request_segments = route_request_segments(request_path);
    let mut full_matches = Vec::new();
    let mut greedy_matches = Vec::new();
    let mut default_route = None;

    for route in routes.values() {
        let parsed = parse_normalized_route_key(route.route_key())?;
        if parsed.route_key == "$default" {
            default_route = Some(MatchedHttpRoute::new(
                route,
                request_path.to_owned(),
                BTreeMap::new(),
            ));
            continue;
        }
        if !route_method_matches(parsed.method.as_deref(), request_method) {
            continue;
        }
        let Some(candidate) = match_route_segments(
            &parsed.segments,
            &request_segments,
            parsed.method.as_deref() != Some("ANY"),
        ) else {
            continue;
        };
        let matched = MatchedHttpRoute::new(
            route,
            request_path.to_owned(),
            candidate.path_parameters,
        );
        match candidate.kind {
            RouteMatchKind::Full {
                static_segments,
                total_segments,
                method_specific,
            } => {
                full_matches.push((
                    method_specific,
                    static_segments,
                    total_segments,
                    matched,
                ));
            }
            RouteMatchKind::Greedy {
                static_segments,
                total_segments,
                method_specific,
            } => {
                greedy_matches.push((
                    method_specific,
                    static_segments,
                    total_segments,
                    matched,
                ));
            }
        }
    }

    full_matches.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then(right.1.cmp(&left.1))
            .then(right.2.cmp(&left.2))
    });
    greedy_matches.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then(right.1.cmp(&left.1))
            .then(right.2.cmp(&left.2))
    });

    full_matches
        .into_iter()
        .map(|(_, _, _, matched)| matched)
        .next()
        .or_else(|| {
            greedy_matches.into_iter().map(|(_, _, _, matched)| matched).next()
        })
        .or(default_route)
        .ok_or(ExecuteApiError::NotFound)
}

pub(crate) fn select_http_stage<'a, Stage>(
    stages: &'a BTreeMap<String, Stage>,
    request_path: &str,
) -> Result<SelectedHttpStage<'a, Stage>, ExecuteApiError> {
    let trimmed = request_path.trim_start_matches('/');
    if !trimmed.is_empty() {
        let mut segments = trimmed.splitn(2, '/');
        let stage_name = segments.next().unwrap_or_default();
        if stage_name != "$default"
            && let Some(stage) = stages.get(stage_name)
        {
            let routed_path = segments
                .next()
                .map(|path| format!("/{path}"))
                .unwrap_or_else(|| ROOT_PATH.to_owned());
            return Ok(SelectedHttpStage::new(stage, routed_path));
        }
    }
    let stage = stages.get("$default").ok_or(ExecuteApiError::NotFound)?;
    Ok(SelectedHttpStage::new(stage, request_path.to_owned()))
}

#[derive(Debug, Clone)]
pub(crate) struct MatchedHttpRoute<'a, Route> {
    path_parameters: BTreeMap<String, String>,
    request_path: String,
    route: &'a Route,
}

impl<'a, Route> MatchedHttpRoute<'a, Route>
where
    Route: HttpApiRouteDefinition,
{
    pub(crate) fn new(
        route: &'a Route,
        request_path: String,
        path_parameters: BTreeMap<String, String>,
    ) -> Self {
        Self { path_parameters, request_path, route }
    }

    pub(crate) fn authorization_type(&self) -> &str {
        self.route.authorization_type()
    }

    pub(crate) fn path_parameters(&self) -> &BTreeMap<String, String> {
        &self.path_parameters
    }

    pub(crate) fn request_path(&self) -> &str {
        &self.request_path
    }

    pub(crate) fn route_id(&self) -> &str {
        self.route.route_id()
    }

    pub(crate) fn route_key(&self) -> &str {
        self.route.route_key()
    }

    pub(crate) fn target(&self) -> Option<&str> {
        self.route.target()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SelectedHttpStage<'a, Stage> {
    routed_path: String,
    stage: &'a Stage,
}

impl<'a, Stage> SelectedHttpStage<'a, Stage> {
    fn new(stage: &'a Stage, routed_path: String) -> Self {
        Self { routed_path, stage }
    }

    pub(crate) fn into_parts(self) -> (&'a Stage, String) {
        (self.stage, self.routed_path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RouteSegment {
    Greedy(String),
    Static(String),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedRouteKey {
    method: Option<String>,
    route_key: String,
    segments: Vec<RouteSegment>,
}

fn normalize_route_path_pattern(
    path: &str,
) -> Result<String, ApiGatewayError> {
    if path == ROOT_PATH {
        return Ok(ROOT_PATH.to_owned());
    }
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid route path {path:?}"),
        });
    }
    let mut normalized = Vec::with_capacity(segments.len());
    for (index, segment) in segments.iter().enumerate() {
        let parsed = parse_route_segment(segment)?;
        if matches!(parsed, RouteSegment::Greedy(_))
            && index + 1 != segments.len()
        {
            return Err(ApiGatewayError::Validation {
                message: format!("invalid routeKey {path:?}"),
            });
        }
        normalized.push(match parsed {
            RouteSegment::Static(value) => value,
            RouteSegment::Variable(name) => format!("{{{name}}}"),
            RouteSegment::Greedy(name) => format!("{{{name}+}}"),
        });
    }
    Ok(format!("/{}", normalized.join("/")))
}

fn parse_route_segment(
    segment: &str,
) -> Result<RouteSegment, ApiGatewayError> {
    if !segment.starts_with('{') {
        if segment.contains(' ')
            || segment.contains('{')
            || segment.contains('}')
        {
            return Err(ApiGatewayError::Validation {
                message: format!("invalid route segment {segment:?}"),
            });
        }
        return Ok(RouteSegment::Static(segment.to_owned()));
    }
    if !segment.ends_with('}') {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid route segment {segment:?}"),
        });
    }
    let mut name =
        segment.trim_start_matches('{').trim_end_matches('}').to_owned();
    if name.is_empty() {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid route segment {segment:?}"),
        });
    }
    let greedy = name.ends_with('+');
    if greedy {
        name.pop();
    }
    if name.is_empty()
        || !name.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_'
        })
    {
        return Err(ApiGatewayError::Validation {
            message: format!("invalid route segment {segment:?}"),
        });
    }
    if greedy {
        Ok(RouteSegment::Greedy(name))
    } else {
        Ok(RouteSegment::Variable(name))
    }
}

fn parse_normalized_route_key(
    route_key: &str,
) -> Result<ParsedRouteKey, ExecuteApiError> {
    if route_key == "$default" {
        return Ok(ParsedRouteKey {
            method: None,
            route_key: "$default".to_owned(),
            segments: Vec::new(),
        });
    }
    let (method, path) = route_key.split_once(' ').ok_or_else(|| {
        ExecuteApiError::IntegrationFailure {
            message: format!("stored route key {route_key:?} is invalid"),
            status_code: 500,
        }
    })?;
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| parse_route_segment(segment).map_err(runtime_error))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ParsedRouteKey {
        method: Some(method.to_owned()),
        route_key: route_key.to_owned(),
        segments,
    })
}

fn route_method_matches(
    route_method: Option<&str>,
    request_method: &str,
) -> bool {
    route_method.is_some_and(|route_method| {
        route_method == "ANY"
            || route_method.eq_ignore_ascii_case(request_method)
    })
}

fn route_request_segments(path: &str) -> Vec<String> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RouteMatchKind {
    Full {
        method_specific: bool,
        static_segments: usize,
        total_segments: usize,
    },
    Greedy {
        method_specific: bool,
        static_segments: usize,
        total_segments: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteMatchCandidate {
    kind: RouteMatchKind,
    path_parameters: BTreeMap<String, String>,
}

fn match_route_segments(
    route_segments: &[RouteSegment],
    request_segments: &[String],
    method_specific: bool,
) -> Option<RouteMatchCandidate> {
    let mut path_parameters = BTreeMap::new();
    let mut static_segments = 0usize;
    let has_greedy =
        matches!(route_segments.last(), Some(RouteSegment::Greedy(_)));
    if !has_greedy && route_segments.len() != request_segments.len() {
        return None;
    }
    if has_greedy && request_segments.len() < route_segments.len() {
        return None;
    }

    for (index, segment) in route_segments.iter().enumerate() {
        match segment {
            RouteSegment::Static(expected) => {
                if request_segments.get(index)? != expected {
                    return None;
                }
                static_segments += 1;
            }
            RouteSegment::Variable(name) => {
                path_parameters.insert(
                    name.clone(),
                    request_segments.get(index)?.clone(),
                );
            }
            RouteSegment::Greedy(name) => {
                let remaining = request_segments.get(index..)?.join("/");
                if remaining.is_empty() {
                    return None;
                }
                path_parameters.insert(name.clone(), remaining);
                return Some(RouteMatchCandidate {
                    kind: RouteMatchKind::Greedy {
                        method_specific,
                        static_segments,
                        total_segments: route_segments.len(),
                    },
                    path_parameters,
                });
            }
        }
    }

    Some(RouteMatchCandidate {
        kind: RouteMatchKind::Full {
            method_specific,
            static_segments,
            total_segments: route_segments.len(),
        },
        path_parameters,
    })
}

fn runtime_error(error: ApiGatewayError) -> ExecuteApiError {
    ExecuteApiError::IntegrationFailure {
        message: error.to_string(),
        status_code: 500,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestRoute {
        authorization_type: String,
        route_id: String,
        route_key: String,
        target: Option<String>,
    }

    impl HttpApiRouteDefinition for TestRoute {
        fn authorization_type(&self) -> &str {
            &self.authorization_type
        }

        fn route_id(&self) -> &str {
            &self.route_id
        }

        fn route_key(&self) -> &str {
            &self.route_key
        }

        fn target(&self) -> Option<&str> {
            self.target.as_deref()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestStage {
        name: String,
    }

    #[test]
    fn route_key_validation_and_normalization_cover_http_api_paths() {
        assert_eq!(
            normalize_route_key("get /pets/{proxy+}")
                .expect("route key should normalize"),
            "GET /pets/{proxy+}"
        );
        assert!(matches!(
            normalize_route_key("GET pets"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("routeKey")
        ));
        assert_eq!(
            normalize_route_path_pattern(ROOT_PATH)
                .expect("root path should normalize"),
            ROOT_PATH
        );
        assert!(matches!(
            normalize_route_path_pattern("/pets/{proxy+}/tail"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("routeKey")
        ));
        assert!(matches!(
            normalize_route_path_pattern("//"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("route path")
        ));

        assert_eq!(
            parse_route_segment("pets").expect("static segment should parse"),
            RouteSegment::Static("pets".to_owned())
        );
        assert_eq!(
            parse_route_segment("{id}")
                .expect("variable segment should parse"),
            RouteSegment::Variable("id".to_owned())
        );
        assert_eq!(
            parse_route_segment("{proxy+}")
                .expect("greedy segment should parse"),
            RouteSegment::Greedy("proxy".to_owned())
        );
        assert!(matches!(
            parse_route_segment("{bad-name}"),
            Err(ApiGatewayError::Validation { message })
                if message.contains("route segment")
        ));
    }

    #[test]
    fn route_matching_preserves_method_specificity_and_precedence() {
        assert_eq!(
            parse_normalized_route_key("$default")
                .expect("default route should parse")
                .route_key,
            "$default"
        );
        assert!(matches!(
            parse_normalized_route_key("BROKEN"),
            Err(ExecuteApiError::IntegrationFailure { status_code, .. })
                if status_code == 500
        ));
        assert!(route_method_matches(Some("ANY"), "POST"));
        assert!(route_method_matches(Some("get"), "GET"));
        assert!(!route_method_matches(None, "GET"));
        assert_eq!(
            route_request_segments("/a//b/"),
            vec!["a".to_owned(), "b".to_owned()]
        );

        let full_match = match_route_segments(
            &[RouteSegment::Static("pets".to_owned())],
            &[String::from("pets")],
            true,
        )
        .expect("full route should match");
        assert!(matches!(full_match.kind, RouteMatchKind::Full { .. }));
        let greedy_match = match_route_segments(
            &[
                RouteSegment::Static("pets".to_owned()),
                RouteSegment::Greedy("proxy".to_owned()),
            ],
            &[String::from("pets"), String::from("cat"), String::from("2")],
            false,
        )
        .expect("greedy route should match");
        assert_eq!(
            greedy_match.path_parameters.get("proxy").map(String::as_str),
            Some("cat/2")
        );
        assert!(
            match_route_segments(
                &[RouteSegment::Greedy("proxy".to_owned())],
                &[],
                true,
            )
            .is_none()
        );

        let exact_route = TestRoute {
            authorization_type: "NONE".to_owned(),
            route_id: "route-exact".to_owned(),
            route_key: "GET /pets/dog".to_owned(),
            target: Some("integrations/exact".to_owned()),
        };
        let method_specific_route = TestRoute {
            authorization_type: "NONE".to_owned(),
            route_id: "route-method".to_owned(),
            route_key: "GET /pets".to_owned(),
            target: Some("integrations/method".to_owned()),
        };
        let any_route = TestRoute {
            authorization_type: "NONE".to_owned(),
            route_id: "route-any".to_owned(),
            route_key: "ANY /pets".to_owned(),
            target: Some("integrations/any".to_owned()),
        };
        let greedy_route = TestRoute {
            authorization_type: "NONE".to_owned(),
            route_id: "route-greedy".to_owned(),
            route_key: "ANY /pets/{proxy+}".to_owned(),
            target: Some("integrations/greedy".to_owned()),
        };
        let default_route = TestRoute {
            authorization_type: "NONE".to_owned(),
            route_id: "route-default".to_owned(),
            route_key: "$default".to_owned(),
            target: Some("integrations/default".to_owned()),
        };
        let routes = BTreeMap::from([
            (exact_route.route_id.clone(), exact_route.clone()),
            (method_specific_route.route_id.clone(), method_specific_route),
            (any_route.route_id.clone(), any_route),
            (greedy_route.route_id.clone(), greedy_route),
            (default_route.route_id.clone(), default_route.clone()),
        ]);

        let exact_match = match_http_route(&routes, "GET", "/pets/dog")
            .expect("exact route should win");
        assert_eq!(exact_match.route_id(), exact_route.route_id);

        let method_match = match_http_route(&routes, "GET", "/pets")
            .expect("method-specific full route should beat ANY");
        assert_eq!(method_match.route_id(), "route-method");

        let default_only =
            BTreeMap::from([(default_route.route_id.clone(), default_route)]);
        let matched_default =
            match_http_route(&default_only, "POST", "/orders")
                .expect("default route should match");
        assert_eq!(matched_default.route_key(), "$default");
    }

    #[test]
    fn stage_selection_prefers_named_stage_before_default() {
        let stages = BTreeMap::from([
            ("$default".to_owned(), TestStage { name: "$default".to_owned() }),
            ("dev".to_owned(), TestStage { name: "dev".to_owned() }),
        ]);

        let (named_stage, named_path) =
            select_http_stage(&stages, "/dev/orders")
                .expect("named stage should resolve")
                .into_parts();
        assert_eq!(named_stage.name, "dev");
        assert_eq!(named_path, "/orders");

        let (default_stage, default_path) =
            select_http_stage(&stages, "/orders")
                .expect("default stage should resolve")
                .into_parts();
        assert_eq!(default_stage.name, "$default");
        assert_eq!(default_path, "/orders");

        assert!(matches!(
            select_http_stage(
                &BTreeMap::<String, TestStage>::new(),
                "/orders"
            ),
            Err(ExecuteApiError::NotFound)
        ));
    }
}
