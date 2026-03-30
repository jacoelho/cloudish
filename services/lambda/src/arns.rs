use crate::scope::LambdaScope;

pub(crate) fn function_arn(
    scope: &LambdaScope,
    function_name: &str,
) -> String {
    format!(
        "arn:aws:lambda:{}:{}:function:{function_name}",
        scope.region().as_str(),
        scope.account_id().as_str(),
    )
}

pub(crate) fn function_arn_with_qualifier(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: &str,
) -> String {
    format!("{}:{qualifier}", function_arn(scope, function_name))
}

pub(crate) fn function_arn_with_optional_qualifier(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: Option<&str>,
) -> String {
    qualifier
        .map(|qualifier| {
            function_arn_with_qualifier(scope, function_name, qualifier)
        })
        .unwrap_or_else(|| function_arn(scope, function_name))
}

pub(crate) fn function_arn_with_optional_latest(
    scope: &LambdaScope,
    function_name: &str,
    qualifier: Option<&str>,
) -> String {
    function_arn_with_qualifier(
        scope,
        function_name,
        qualifier.unwrap_or("$LATEST"),
    )
}

pub(crate) fn function_arn_for_version(
    scope: &LambdaScope,
    function_name: &str,
    version: &str,
) -> String {
    if version == "$LATEST" {
        function_arn(scope, function_name)
    } else {
        function_arn_with_qualifier(scope, function_name, version)
    }
}

pub(crate) fn event_source_mapping_arn(
    scope: &LambdaScope,
    uuid: &str,
) -> String {
    format!(
        "arn:aws:lambda:{}:{}:event-source-mapping:{uuid}",
        scope.region().as_str(),
        scope.account_id().as_str(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        event_source_mapping_arn, function_arn, function_arn_for_version,
        function_arn_with_optional_latest,
        function_arn_with_optional_qualifier, function_arn_with_qualifier,
    };
    use crate::scope::LambdaScope;
    use aws::{AccountId, RegionId};

    fn scope() -> LambdaScope {
        LambdaScope::new(
            "000000000000".parse::<AccountId>().unwrap(),
            "eu-west-2".parse::<RegionId>().unwrap(),
        )
    }

    #[test]
    fn lambda_identifier_helpers_shape_function_arns() {
        assert_eq!(
            function_arn(&scope(), "demo"),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo"
        );
        assert_eq!(
            function_arn_with_qualifier(&scope(), "demo", "live"),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
        );
        assert_eq!(
            function_arn_with_optional_qualifier(&scope(), "demo", None),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo"
        );
        assert_eq!(
            function_arn_with_optional_qualifier(
                &scope(),
                "demo",
                Some("live"),
            ),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:live"
        );
        assert_eq!(
            function_arn_with_optional_latest(&scope(), "demo", None),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:$LATEST"
        );
        assert_eq!(
            function_arn_with_optional_latest(&scope(), "demo", Some("3")),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:3"
        );
        assert_eq!(
            function_arn_for_version(&scope(), "demo", "$LATEST"),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo"
        );
        assert_eq!(
            function_arn_for_version(&scope(), "demo", "7"),
            "arn:aws:lambda:eu-west-2:000000000000:function:demo:7"
        );
    }

    #[test]
    fn lambda_identifier_helpers_shape_event_source_mapping_arns() {
        assert_eq!(
            event_source_mapping_arn(&scope(), "uuid-1"),
            "arn:aws:lambda:eu-west-2:000000000000:event-source-mapping:uuid-1"
        );
    }
}
