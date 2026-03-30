pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{QueryParameters, missing_action_error};
use crate::request::HttpRequest;
use crate::xml::XmlBuilder;
use aws::{AwsError, RequestContext};
use elasticache::{ElastiCacheReplicationGroupId, ElastiCacheUserId};
use services::{
    CreateElastiCacheReplicationGroupInput, CreateElastiCacheUserInput,
    ElastiCacheAuthenticationModeInput, ElastiCacheError, ElastiCacheScope,
    ElastiCacheService, ModifyElastiCacheUserInput, ReplicationGroup,
};

const ELASTICACHE_XMLNS: &str =
    "http://elasticache.amazonaws.com/doc/2015-02-02/";
const REQUEST_ID: &str = "0000000000000000";
pub(crate) const ELASTICACHE_QUERY_VERSION: &str = "2015-02-02";

pub(crate) fn is_elasticache_action(action: &str) -> bool {
    matches!(
        action,
        "CreateReplicationGroup"
            | "DescribeReplicationGroups"
            | "DeleteReplicationGroup"
            | "ModifyReplicationGroup"
            | "CreateUser"
            | "DescribeUsers"
            | "ModifyUser"
            | "DeleteUser"
            | "ValidateIamAuthToken"
    )
}

pub(crate) fn action_matches_version(
    action: &str,
    version: Option<&str>,
) -> bool {
    matches!(
        action,
        "CreateReplicationGroup"
            | "DescribeReplicationGroups"
            | "DeleteReplicationGroup"
            | "ModifyReplicationGroup"
            | "DescribeUsers"
            | "ModifyUser"
            | "ValidateIamAuthToken"
    ) || (matches!(action, "CreateUser" | "DeleteUser")
        && version == Some(ELASTICACHE_QUERY_VERSION))
}

pub(crate) fn handle_query(
    elasticache: &ElastiCacheService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(request.body())?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope = ElastiCacheScope::new(
        context.account_id().clone(),
        context.region().clone(),
    );

    match action {
        "CreateReplicationGroup" => Ok(replication_group_response(
            action,
            &elasticache
                .create_replication_group(
                    &scope,
                    parse_create_replication_group_input(&params)?,
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeReplicationGroups" => {
            let replication_group_id = params
                .optional("ReplicationGroupId")
                .map(parse_replication_group_id)
                .transpose()?;
            Ok(describe_replication_groups_response(
                &elasticache
                    .describe_replication_groups(
                        &scope,
                        replication_group_id.as_ref(),
                    )
                    .map_err(|error| error.to_aws_error())?,
            ))
        }
        "DeleteReplicationGroup" => {
            reject_snapshot_delete_options(&params)?;
            let replication_group_id = parse_replication_group_id(
                params.required("ReplicationGroupId")?,
            )?;
            Ok(replication_group_response(
                action,
                &elasticache
                    .delete_replication_group(&scope, &replication_group_id)
                    .map_err(|error| error.to_aws_error())?,
            ))
        }
        "ModifyReplicationGroup" => Err(unsupported_operation_error(
            "ModifyReplicationGroup is not supported by Cloudish ElastiCache.",
        )),
        "CreateUser" => Ok(user_response(
            action,
            &elasticache
                .create_user(&scope, parse_create_user_input(&params)?)
                .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeUsers" => {
            let user_id =
                params.optional("UserId").map(parse_user_id).transpose()?;
            Ok(describe_users_response(
                &elasticache
                    .describe_users(&scope, user_id.as_ref())
                    .map_err(|error| error.to_aws_error())?,
            ))
        }
        "ModifyUser" => Ok(user_response(
            action,
            &elasticache
                .modify_user(
                    &scope,
                    &parse_user_id(params.required("UserId")?)?,
                    parse_modify_user_input(&params),
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "DeleteUser" => Ok(user_response(
            action,
            &elasticache
                .delete_user(
                    &scope,
                    &parse_user_id(params.required("UserId")?)?,
                )
                .map_err(|error| error.to_aws_error())?,
        )),
        "ValidateIamAuthToken" => Err(unsupported_operation_error(
            "ValidateIamAuthToken is a Floci helper and is not part of the \
             Cloudish ElastiCache compatibility contract.",
        )),
        _ => Err(unsupported_operation_error(&format!(
            "ElastiCache action {action} is not supported."
        ))),
    }
}

fn parse_create_replication_group_input(
    params: &QueryParameters,
) -> Result<CreateElastiCacheReplicationGroupInput, AwsError> {
    Ok(CreateElastiCacheReplicationGroupInput {
        auth_token: optional_string(params, "AuthToken"),
        cluster_mode: optional_string(params, "ClusterMode"),
        engine: optional_string(params, "Engine"),
        engine_version: optional_string(params, "EngineVersion"),
        multi_az_enabled: optional_bool(params, "MultiAZEnabled")?,
        num_cache_clusters: optional_u32(params, "NumCacheClusters")?,
        num_node_groups: optional_u32(params, "NumNodeGroups")?,
        replicas_per_node_group: optional_u32(params, "ReplicasPerNodeGroup")?,
        replication_group_description: params
            .required("ReplicationGroupDescription")?
            .to_owned(),
        replication_group_id: parse_replication_group_id(
            params.required("ReplicationGroupId")?,
        )?,
        transit_encryption_enabled: optional_bool(
            params,
            "TransitEncryptionEnabled",
        )?,
        has_node_group_configuration: has_prefix(
            params,
            "NodeGroupConfiguration.",
        ),
    })
}

fn parse_create_user_input(
    params: &QueryParameters,
) -> Result<CreateElastiCacheUserInput, AwsError> {
    Ok(CreateElastiCacheUserInput {
        access_string: optional_string(params, "AccessString"),
        authentication_mode: parse_authentication_mode(params),
        engine: optional_string(params, "Engine"),
        no_password_required: optional_bool(params, "NoPasswordRequired")?,
        passwords: string_list(params, "Passwords.member"),
        user_id: parse_user_id(params.required("UserId")?)?,
        user_name: params.required("UserName")?.to_owned(),
    })
}

fn parse_replication_group_id(
    value: &str,
) -> Result<ElastiCacheReplicationGroupId, AwsError> {
    ElastiCacheReplicationGroupId::new(value)
        .map_err(|error| error.to_aws_error())
}

fn parse_user_id(value: &str) -> Result<ElastiCacheUserId, AwsError> {
    ElastiCacheUserId::new(value).map_err(|error| error.to_aws_error())
}

fn parse_modify_user_input(
    params: &QueryParameters,
) -> ModifyElastiCacheUserInput {
    ModifyElastiCacheUserInput {
        access_string: optional_string(params, "AccessString"),
        authentication_mode: parse_authentication_mode(params),
        no_password_required: optional_bool(params, "NoPasswordRequired")
            .ok()
            .flatten(),
        passwords: string_list(params, "Passwords.member"),
    }
}

fn parse_authentication_mode(
    params: &QueryParameters,
) -> Option<ElastiCacheAuthenticationModeInput> {
    let passwords = string_list(params, "AuthenticationMode.Passwords.member");
    let type_ = optional_string(params, "AuthenticationMode.Type");
    (!passwords.is_empty() || type_.is_some())
        .then_some(ElastiCacheAuthenticationModeInput { passwords, type_ })
}

fn reject_snapshot_delete_options(
    params: &QueryParameters,
) -> Result<(), AwsError> {
    if params.optional("FinalSnapshotIdentifier").is_some()
        || params.optional("RetainPrimaryCluster").is_some()
    {
        return Err(unsupported_operation_error(
            "Final snapshots and RetainPrimaryCluster are not supported by \
             Cloudish ElastiCache.",
        ));
    }

    Ok(())
}

fn replication_group_response(
    action: &str,
    group: &ReplicationGroup,
) -> String {
    response_with_result(action, &replication_group_xml(group))
}

fn describe_replication_groups_response(
    groups: &[ReplicationGroup],
) -> String {
    let mut xml = XmlBuilder::new().start("ReplicationGroups", None);
    for group in groups {
        xml = xml.raw(&replication_group_xml(group));
    }
    xml = xml.end("ReplicationGroups").elem("Marker", "");

    response_with_result("DescribeReplicationGroups", &xml.build())
}

fn user_response(action: &str, user: &services::ElastiCacheUser) -> String {
    response_with_result(action, &user_xml(user))
}

fn describe_users_response(users: &[services::ElastiCacheUser]) -> String {
    let mut xml = XmlBuilder::new().start("Users", None);
    for user in users {
        xml = xml.start("member", None).raw(&user_xml(user)).end("member");
    }
    xml = xml.end("Users").elem("Marker", "");

    response_with_result("DescribeUsers", &xml.build())
}

fn response_with_result(action: &str, result: &str) -> String {
    let response_name = format!("{action}Response");
    let result_name = format!("{action}Result");
    XmlBuilder::new()
        .start(&response_name, Some(ELASTICACHE_XMLNS))
        .start(&result_name, None)
        .raw(result)
        .end(&result_name)
        .raw(&response_metadata_xml())
        .end(&response_name)
        .build()
}

fn response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn replication_group_xml(group: &ReplicationGroup) -> String {
    let mut xml = XmlBuilder::new().start("ReplicationGroup", None);
    xml = xml
        .elem("ReplicationGroupId", group.replication_group_id.as_str())
        .elem("Status", group.status.as_str())
        .elem("Description", &group.description)
        .elem("AuthTokenEnabled", bool_text(group.auth_token_enabled))
        .elem(
            "TransitEncryptionEnabled",
            bool_text(group.transit_encryption_enabled),
        )
        .elem(
            "AtRestEncryptionEnabled",
            bool_text(group.at_rest_encryption_enabled),
        )
        .elem("ClusterEnabled", bool_text(group.cluster_enabled))
        .elem("MultiAZ", &group.multi_az)
        .elem("AutomaticFailover", &group.automatic_failover)
        .elem(
            "SnapshotRetentionLimit",
            &group.snapshot_retention_limit.to_string(),
        )
        .start("ConfigurationEndpoint", None)
        .elem("Address", &group.configuration_endpoint.address)
        .elem("Port", &group.configuration_endpoint.port.to_string())
        .end("ConfigurationEndpoint")
        .start("MemberClusters", None);
    for member_cluster in &group.member_clusters {
        xml = xml.elem("ClusterId", member_cluster);
    }
    xml = xml.end("MemberClusters").start("NodeGroups", None);
    for node_group in &group.node_groups {
        xml = xml
            .start("NodeGroup", None)
            .elem("NodeGroupId", &node_group.node_group_id)
            .start("PrimaryEndpoint", None)
            .elem("Address", &node_group.primary_endpoint.address)
            .elem("Port", &node_group.primary_endpoint.port.to_string())
            .end("PrimaryEndpoint")
            .elem("Status", node_group.status.as_str())
            .start("NodeGroupMembers", None);
        for member in &node_group.node_group_members {
            xml = xml
                .start("NodeGroupMember", None)
                .elem("CacheClusterId", &member.cache_cluster_id)
                .start("ReadEndpoint", None)
                .elem("Address", &member.read_endpoint.address)
                .elem("Port", &member.read_endpoint.port.to_string())
                .end("ReadEndpoint")
                .elem("CacheNodeId", &member.cache_node_id)
                .elem("CurrentRole", &member.current_role)
                .end("NodeGroupMember");
        }
        xml = xml.end("NodeGroupMembers").end("NodeGroup");
    }
    xml = xml
        .end("NodeGroups")
        .raw("<PendingModifiedValues />")
        .end("ReplicationGroup");

    xml.build()
}

fn user_xml(user: &services::ElastiCacheUser) -> String {
    XmlBuilder::new()
        .elem("UserId", user.user_id.as_str())
        .elem("UserName", &user.user_name)
        .elem("Status", &user.status)
        .elem("AccessString", &user.access_string)
        .start("Authentication", None)
        .elem("Type", user.authentication.type_.as_output_str())
        .elem("PasswordCount", &user.authentication.password_count.to_string())
        .end("Authentication")
        .elem("Engine", user.engine.as_str())
        .elem("MinimumEngineVersion", &user.minimum_engine_version)
        .start("UserGroupIds", None)
        .end("UserGroupIds")
        .elem("ARN", &user.arn)
        .build()
}

fn string_list(params: &QueryParameters, prefix: &str) -> Vec<String> {
    let mut values = Vec::new();
    for index in 1.. {
        let name = format!("{prefix}.{index}");
        match params.optional(&name) {
            Some(value) => values.push(value.to_owned()),
            None => break,
        }
    }
    values
}

fn has_prefix(params: &QueryParameters, prefix: &str) -> bool {
    params.iter().any(|(name, _)| name.starts_with(prefix))
}

fn optional_string(params: &QueryParameters, name: &str) -> Option<String> {
    params.optional(name).map(str::to_owned)
}

fn optional_bool(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<bool>, AwsError> {
    params
        .optional(name)
        .map(|value| match value {
            "true" | "TRUE" | "True" => Ok(true),
            "false" | "FALSE" | "False" => Ok(false),
            _ => Err(invalid_parameter_value(name)),
        })
        .transpose()
}

fn optional_u32(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<u32>, AwsError> {
    params
        .optional(name)
        .map(|value| {
            value.parse::<u32>().map_err(|_| invalid_parameter_value(name))
        })
        .transpose()
}

fn invalid_parameter_value(name: &str) -> AwsError {
    ElastiCacheError::InvalidParameterValue {
        message: format!("{name} has an invalid value."),
    }
    .to_aws_error()
}

fn bool_text(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn unsupported_operation_error(message: &str) -> AwsError {
    ElastiCacheError::UnsupportedOperation { message: message.to_owned() }
        .to_aws_error()
}

#[cfg(test)]
mod tests {
    use super::{
        ELASTICACHE_QUERY_VERSION, action_matches_version, bool_text,
        handle_query, is_elasticache_action, parse_authentication_mode,
        parse_create_replication_group_input, parse_create_user_input,
        parse_modify_user_input, string_list, unsupported_operation_error,
    };
    use crate::query::QueryParameters;
    use crate::request::HttpRequest;
    use aws::{ProtocolFamily, RequestContext, ServiceName};
    use services::{
        ElastiCacheNodeRuntime, ElastiCacheNodeSpec, ElastiCacheProxyRuntime,
        ElastiCacheProxySpec, ElastiCacheService,
        ElastiCacheServiceDependencies, RunningElastiCacheNode,
        RunningElastiCacheProxy,
    };
    use std::sync::Arc;
    use std::time::SystemTime;
    use storage::{StorageConfig, StorageFactory, StorageMode};

    #[derive(Debug, Clone, Copy)]
    struct StaticClock;

    impl services::Clock for StaticClock {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }
    }

    struct NullNodeRuntime;

    impl ElastiCacheNodeRuntime for NullNodeRuntime {
        fn start(
            &self,
            _spec: &ElastiCacheNodeSpec,
        ) -> Result<
            Box<dyn RunningElastiCacheNode>,
            services::InfrastructureError,
        > {
            Ok(Box::new(NullNode))
        }
    }

    struct NullNode;

    impl RunningElastiCacheNode for NullNode {
        fn listen_endpoint(&self) -> services::Endpoint {
            services::Endpoint::localhost(7001)
        }

        fn stop(&self) -> Result<(), services::InfrastructureError> {
            Ok(())
        }
    }

    struct NullProxyRuntime;

    impl ElastiCacheProxyRuntime for NullProxyRuntime {
        fn start(
            &self,
            _spec: &ElastiCacheProxySpec,
        ) -> Result<
            Box<dyn RunningElastiCacheProxy>,
            services::InfrastructureError,
        > {
            Ok(Box::new(NullProxy))
        }
    }

    struct NullProxy;

    impl RunningElastiCacheProxy for NullProxy {
        fn listen_endpoint(&self) -> services::Endpoint {
            services::Endpoint::localhost(8001)
        }

        fn stop(&self) -> Result<(), services::InfrastructureError> {
            Ok(())
        }
    }

    fn service(label: &str) -> ElastiCacheService {
        let path = std::env::temp_dir()
            .join(format!("cloudish-http-elasticache-{label}"));
        if path.exists() {
            let _ = std::fs::remove_dir_all(&path);
        }
        std::fs::create_dir_all(&path)
            .expect("HTTP adapter test state directory should exist");
        ElastiCacheService::new(
            &StorageFactory::new(StorageConfig::new(
                path,
                StorageMode::Memory,
            )),
            ElastiCacheServiceDependencies {
                clock: Arc::new(StaticClock),
                iam_token_validator: Arc::new(
                    services::RejectAllElastiCacheIamTokenValidator,
                ),
                node_runtime: Arc::new(NullNodeRuntime),
                proxy_runtime: Arc::new(NullProxyRuntime),
            },
        )
    }

    fn context(action: &str) -> RequestContext {
        RequestContext::try_new(
            "000000000000".parse().expect("account id should parse"),
            "eu-west-2".parse().expect("region should parse"),
            ServiceName::ElastiCache,
            ProtocolFamily::Query,
            action,
            None,
            true,
        )
        .expect("request context should build")
    }

    fn request(body: &str) -> HttpRequest<'static> {
        let raw = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
        .into_boxed_slice();
        HttpRequest::parse(Box::leak(raw)).expect("HTTP request should parse")
    }

    #[test]
    fn elasticache_query_adapter_parses_replication_groups_and_users() {
        let create_group = QueryParameters::parse(
            b"Action=CreateReplicationGroup&ReplicationGroupId=cache-a&ReplicationGroupDescription=demo&TransitEncryptionEnabled=true",
        )
        .expect("query parameters should parse");
        let group_input = parse_create_replication_group_input(&create_group)
            .expect("replication group input should parse");
        assert_eq!(group_input.replication_group_id, "cache-a");
        assert_eq!(group_input.replication_group_description, "demo");
        assert_eq!(group_input.transit_encryption_enabled, Some(true));

        let create_group_false = QueryParameters::parse(
            b"Action=CreateReplicationGroup&ReplicationGroupId=cache-b&ReplicationGroupDescription=demo&TransitEncryptionEnabled=false&MultiAZEnabled=false",
        )
        .expect("query parameters should parse");
        let group_input_false =
            parse_create_replication_group_input(&create_group_false)
                .expect("replication group false flags should parse");
        assert_eq!(group_input_false.transit_encryption_enabled, Some(false));
        assert_eq!(group_input_false.multi_az_enabled, Some(false));

        let create_user = QueryParameters::parse(
            b"Action=CreateUser&UserId=user-a&UserName=alice&AccessString=on+~*+%2B%40all&AuthenticationMode.Type=password&AuthenticationMode.Passwords.member.1=secret",
        )
        .expect("query parameters should parse");
        let user_input = parse_create_user_input(&create_user)
            .expect("user input should parse");
        assert_eq!(user_input.user_id, "user-a");
        assert_eq!(user_input.user_name, "alice");
        assert_eq!(
            user_input
                .authentication_mode
                .expect("authentication mode should exist")
                .passwords,
            vec!["secret".to_owned()]
        );

        let modify_user = QueryParameters::parse(
            b"Action=ModifyUser&UserId=user-a&NoPasswordRequired=true",
        )
        .expect("query parameters should parse");
        let modify_input = parse_modify_user_input(&modify_user);
        assert_eq!(modify_input.no_password_required, Some(true));
    }

    #[test]
    fn elasticache_query_adapter_helpers_cover_version_and_lists() {
        let params = QueryParameters::parse(
            b"Passwords.member.1=one&Passwords.member.2=two&NodeGroupConfiguration.1.Slots=0-1",
        )
        .expect("query parameters should parse");
        assert_eq!(
            string_list(&params, "Passwords.member"),
            vec!["one".to_owned(), "two".to_owned()]
        );
        assert!(action_matches_version("CreateReplicationGroup", None));
        assert!(action_matches_version(
            "CreateUser",
            Some(ELASTICACHE_QUERY_VERSION)
        ));
        assert!(action_matches_version(
            "DeleteUser",
            Some(ELASTICACHE_QUERY_VERSION)
        ));
        assert!(!action_matches_version("CreateUser", Some("2010-05-08")));
        assert_eq!(bool_text(true), "true");
        assert_eq!(bool_text(false), "false");
        assert_eq!(
            unsupported_operation_error("gap").code(),
            "UnsupportedOperation"
        );
        assert!(parse_authentication_mode(&params).is_none());
        assert!(is_elasticache_action("CreateReplicationGroup"));
        assert!(!is_elasticache_action("CreateCluster"));
    }

    #[test]
    fn elasticache_query_adapter_handles_runtime_paths_and_explicit_errors() {
        let elasticache = service("handle-query");

        let missing_action = handle_query(
            &elasticache,
            &request("Version=2015-02-02"),
            &context("MissingAction"),
        )
        .expect_err("missing actions should fail");
        assert_eq!(missing_action.code(), "MissingAction");

        let create_group_response = handle_query(
            &elasticache,
            &request(
                "Action=CreateReplicationGroup&Version=2015-02-02&ReplicationGroupId=cache-a&ReplicationGroupDescription=demo",
            ),
            &context("CreateReplicationGroup"),
        )
        .expect("create group should succeed");
        assert!(
            create_group_response.contains("<CreateReplicationGroupResponse")
        );
        assert!(
            create_group_response
                .contains("<ReplicationGroupId>cache-a</ReplicationGroupId>")
        );

        let describe_group_response = handle_query(
            &elasticache,
            &request(
                "Action=DescribeReplicationGroups&Version=2015-02-02&ReplicationGroupId=cache-a",
            ),
            &context("DescribeReplicationGroups"),
        )
        .expect("describe group should succeed");
        assert!(
            describe_group_response
                .contains("<DescribeReplicationGroupsResult>")
        );
        assert!(describe_group_response.contains("<MemberClusters>"));

        let create_user_response = handle_query(
            &elasticache,
            &request(
                "Action=CreateUser&Version=2015-02-02&UserId=user-a&UserName=alice&AuthenticationMode.Type=no-password-required",
            ),
            &context("CreateUser"),
        )
        .expect("create user should succeed");
        assert!(create_user_response.contains("<CreateUserResponse"));
        assert!(create_user_response.contains("<UserName>alice</UserName>"));

        let describe_user_response = handle_query(
            &elasticache,
            &request("Action=DescribeUsers&Version=2015-02-02&UserId=user-a"),
            &context("DescribeUsers"),
        )
        .expect("describe users should succeed");
        assert!(describe_user_response.contains("<DescribeUsersResult>"));
        assert!(describe_user_response.contains("<Authentication>"));

        let modify_user_response = handle_query(
            &elasticache,
            &request(
                "Action=ModifyUser&Version=2015-02-02&UserId=user-a&AuthenticationMode.Type=iam",
            ),
            &context("ModifyUser"),
        )
        .expect("modify user should succeed");
        assert!(modify_user_response.contains("<ModifyUserResponse"));
        assert!(modify_user_response.contains("<Type>iam</Type>"));

        let delete_user_response = handle_query(
            &elasticache,
            &request("Action=DeleteUser&Version=2015-02-02&UserId=user-a"),
            &context("DeleteUser"),
        )
        .expect("delete user should succeed");
        assert!(delete_user_response.contains("<DeleteUserResponse"));
        assert!(delete_user_response.contains("<Status>deleting</Status>"));

        let delete_group_response = handle_query(
            &elasticache,
            &request(
                "Action=DeleteReplicationGroup&Version=2015-02-02&ReplicationGroupId=cache-a",
            ),
            &context("DeleteReplicationGroup"),
        )
        .expect("delete group should succeed");
        assert!(
            delete_group_response.contains("<DeleteReplicationGroupResponse")
        );
        assert!(delete_group_response.contains("<Status>deleting</Status>"));

        let snapshot_error = handle_query(
            &elasticache,
            &request(
                "Action=DeleteReplicationGroup&Version=2015-02-02&ReplicationGroupId=cache-a&FinalSnapshotIdentifier=snap",
            ),
            &context("DeleteReplicationGroup"),
        )
        .expect_err("snapshot delete options should fail");
        assert_eq!(snapshot_error.code(), "UnsupportedOperation");

        let modify_group_error = handle_query(
            &elasticache,
            &request(
                "Action=ModifyReplicationGroup&Version=2015-02-02&ReplicationGroupId=cache-a",
            ),
            &context("ModifyReplicationGroup"),
        )
        .expect_err("unsupported modify should fail");
        assert_eq!(modify_group_error.code(), "UnsupportedOperation");

        let validate_token_error = handle_query(
            &elasticache,
            &request("Action=ValidateIamAuthToken&Version=2015-02-02"),
            &context("ValidateIamAuthToken"),
        )
        .expect_err("unsupported helpers should fail");
        assert_eq!(validate_token_error.code(), "UnsupportedOperation");

        let unknown_action_error = handle_query(
            &elasticache,
            &request("Action=RebootCacheCluster&Version=2015-02-02"),
            &context("RebootCacheCluster"),
        )
        .expect_err("unknown actions should fail");
        assert_eq!(unknown_action_error.code(), "UnsupportedOperation");
    }

    #[test]
    fn elasticache_query_adapter_reports_invalid_parameter_values() {
        let elasticache = service("invalid-parameters");

        let invalid_bool = handle_query(
            &elasticache,
            &request(
                "Action=CreateReplicationGroup&Version=2015-02-02&ReplicationGroupId=cache-b&ReplicationGroupDescription=demo&TransitEncryptionEnabled=maybe",
            ),
            &context("CreateReplicationGroup"),
        )
        .expect_err("invalid booleans should fail");
        assert_eq!(invalid_bool.code(), "InvalidParameterValue");

        let invalid_integer = handle_query(
            &elasticache,
            &request(
                "Action=CreateReplicationGroup&Version=2015-02-02&ReplicationGroupId=cache-b&ReplicationGroupDescription=demo&NumCacheClusters=abc",
            ),
            &context("CreateReplicationGroup"),
        )
        .expect_err("invalid integers should fail");
        assert_eq!(invalid_integer.code(), "InvalidParameterValue");

        let invalid_user_bool = handle_query(
            &elasticache,
            &request(
                "Action=CreateUser&Version=2015-02-02&UserId=user-b&UserName=bob&NoPasswordRequired=not-bool",
            ),
            &context("CreateUser"),
        )
        .expect_err("invalid user booleans should fail");
        assert_eq!(invalid_user_bool.code(), "InvalidParameterValue");
    }
}
