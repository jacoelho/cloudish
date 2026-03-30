pub(crate) use crate::aws_error_shape::AwsErrorShape;
use crate::query::{
    QueryParameters, missing_action_error, missing_parameter_error,
};
use crate::request::HttpRequest;
use crate::xml::XmlBuilder;
use aws::{AwsError, AwsErrorFamily, RequestContext};
use rds::{
    DbClusterIdentifier, DbInstanceIdentifier, DbParameterGroupFamily,
    DbParameterGroupName,
};
use services::{
    CreateDbClusterInput, CreateDbInstanceInput, CreateDbParameterGroupInput,
    DbCluster, DbInstance, DbParameter, DbParameterGroup,
    ModifyDbClusterInput, ModifyDbInstanceInput, ModifyDbParameterGroupInput,
    RdsError, RdsScope, RdsService,
};
use std::collections::BTreeMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const RDS_XMLNS: &str = "http://rds.amazonaws.com/doc/2014-10-31/";
const REQUEST_ID: &str = "0000000000000000";
pub(crate) const RDS_QUERY_VERSION: &str = "2014-10-31";

pub(crate) fn is_rds_action(action: &str) -> bool {
    matches!(
        action,
        "CreateDBInstance"
            | "DescribeDBInstances"
            | "ModifyDBInstance"
            | "RebootDBInstance"
            | "DeleteDBInstance"
            | "CreateDBCluster"
            | "DescribeDBClusters"
            | "ModifyDBCluster"
            | "DeleteDBCluster"
            | "CreateDBParameterGroup"
            | "DescribeDBParameterGroups"
            | "ModifyDBParameterGroup"
            | "DeleteDBParameterGroup"
            | "DescribeDBParameters"
    )
}

pub(crate) fn handle_query(
    rds: &RdsService,
    request: &HttpRequest<'_>,
    context: &RequestContext,
) -> Result<String, AwsError> {
    let params = QueryParameters::parse(request.body())?;
    let Some(action) = params.action() else {
        return Err(missing_action_error());
    };
    let scope =
        RdsScope::new(context.account_id().clone(), context.region().clone());

    match action {
        "CreateDBInstance" => Ok(instance_response(
            action,
            &rds.create_db_instance(
                &scope,
                parse_create_db_instance_input(&params)?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeDBInstances" => Ok(describe_instances_response(
            &rds.describe_db_instances(
                &scope,
                optional_db_instance_identifier(
                    &params,
                    "DBInstanceIdentifier",
                )?
                .as_ref(),
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "ModifyDBInstance" => Ok(instance_response(
            action,
            &rds.modify_db_instance(
                &scope,
                &db_instance_identifier(
                    params.required("DBInstanceIdentifier")?,
                )?,
                parse_modify_db_instance_input(&params)?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "RebootDBInstance" => Ok(instance_response(
            action,
            &rds.reboot_db_instance(
                &scope,
                &db_instance_identifier(
                    params.required("DBInstanceIdentifier")?,
                )?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "DeleteDBInstance" => Ok(instance_response(
            action,
            &rds.delete_db_instance(&scope, &{
                reject_unsupported_snapshot_fields(
                    &params,
                    "DeleteDBInstance",
                )?;
                db_instance_identifier(
                    params.required("DBInstanceIdentifier")?,
                )?
            })
            .map_err(|error| error.to_aws_error())?,
        )),
        "CreateDBCluster" => Ok(cluster_response(
            action,
            &rds.create_db_cluster(
                &scope,
                parse_create_db_cluster_input(&params)?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeDBClusters" => Ok(describe_clusters_response(
            &rds.describe_db_clusters(
                &scope,
                optional_db_cluster_identifier(
                    &params,
                    "DBClusterIdentifier",
                )?
                .as_ref(),
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "ModifyDBCluster" => Ok(cluster_response(
            action,
            &rds.modify_db_cluster(
                &scope,
                &db_cluster_identifier(
                    params.required("DBClusterIdentifier")?,
                )?,
                parse_modify_db_cluster_input(&params)?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "DeleteDBCluster" => Ok(cluster_response(
            action,
            &rds.delete_db_cluster(&scope, &{
                reject_unsupported_snapshot_fields(
                    &params,
                    "DeleteDBCluster",
                )?;
                db_cluster_identifier(params.required("DBClusterIdentifier")?)?
            })
            .map_err(|error| error.to_aws_error())?,
        )),
        "CreateDBParameterGroup" => Ok(parameter_group_response(
            action,
            &rds.create_db_parameter_group(
                &scope,
                parse_create_db_parameter_group_input(&params)?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "DescribeDBParameterGroups" => Ok(describe_parameter_groups_response(
            &rds.describe_db_parameter_groups(
                &scope,
                optional_db_parameter_group_name(
                    &params,
                    "DBParameterGroupName",
                )?
                .as_ref(),
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "ModifyDBParameterGroup" => Ok(modify_parameter_group_response(
            &rds.modify_db_parameter_group(
                &scope,
                &db_parameter_group_name(
                    params.required("DBParameterGroupName")?,
                )?,
                parse_modify_db_parameter_group_input(&params)?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        "DeleteDBParameterGroup" => {
            rds.delete_db_parameter_group(
                &scope,
                &db_parameter_group_name(
                    params.required("DBParameterGroupName")?,
                )?,
            )
            .map_err(|error| error.to_aws_error())?;

            Ok(response_without_result(action))
        }
        "DescribeDBParameters" => Ok(describe_parameters_response(
            &rds.describe_db_parameters(
                &scope,
                &db_parameter_group_name(
                    params.required("DBParameterGroupName")?,
                )?,
            )
            .map_err(|error| error.to_aws_error())?,
        )),
        other => Err(invalid_action_error(other)),
    }
}

fn parse_create_db_instance_input(
    params: &QueryParameters,
) -> Result<CreateDbInstanceInput, AwsError> {
    Ok(CreateDbInstanceInput {
        allocated_storage: optional_u32(params, "AllocatedStorage")?,
        db_cluster_identifier: optional_db_cluster_identifier(
            params,
            "DBClusterIdentifier",
        )?,
        db_instance_class: optional_string(params, "DBInstanceClass"),
        db_instance_identifier: db_instance_identifier(
            params.required("DBInstanceIdentifier")?,
        )?,
        db_name: optional_string(params, "DBName"),
        db_parameter_group_name: optional_db_parameter_group_name(
            params,
            "DBParameterGroupName",
        )?,
        engine: optional_string(params, "Engine"),
        engine_version: optional_string(params, "EngineVersion"),
        enable_iam_database_authentication: optional_bool(
            params,
            "EnableIAMDatabaseAuthentication",
        )?,
        master_user_password: optional_string(params, "MasterUserPassword"),
        master_username: optional_string(params, "MasterUsername"),
    })
}

fn parse_modify_db_instance_input(
    params: &QueryParameters,
) -> Result<ModifyDbInstanceInput, AwsError> {
    Ok(ModifyDbInstanceInput {
        db_parameter_group_name: optional_db_parameter_group_name(
            params,
            "DBParameterGroupName",
        )?,
        enable_iam_database_authentication: optional_bool(
            params,
            "EnableIAMDatabaseAuthentication",
        )?,
        master_user_password: optional_string(params, "MasterUserPassword"),
    })
}

fn parse_create_db_cluster_input(
    params: &QueryParameters,
) -> Result<CreateDbClusterInput, AwsError> {
    Ok(CreateDbClusterInput {
        database_name: optional_string(params, "DatabaseName"),
        db_cluster_identifier: db_cluster_identifier(
            params.required("DBClusterIdentifier")?,
        )?,
        db_cluster_parameter_group_name: optional_db_parameter_group_name(
            params,
            "DBClusterParameterGroupName",
        )?,
        engine: params.required("Engine")?.to_owned(),
        engine_version: optional_string(params, "EngineVersion"),
        enable_iam_database_authentication: optional_bool(
            params,
            "EnableIAMDatabaseAuthentication",
        )?,
        master_user_password: params
            .required("MasterUserPassword")?
            .to_owned(),
        master_username: params.required("MasterUsername")?.to_owned(),
    })
}

fn parse_modify_db_cluster_input(
    params: &QueryParameters,
) -> Result<ModifyDbClusterInput, AwsError> {
    Ok(ModifyDbClusterInput {
        db_cluster_parameter_group_name: optional_db_parameter_group_name(
            params,
            "DBClusterParameterGroupName",
        )?,
        enable_iam_database_authentication: optional_bool(
            params,
            "EnableIAMDatabaseAuthentication",
        )?,
        master_user_password: optional_string(params, "MasterUserPassword"),
    })
}

fn parse_create_db_parameter_group_input(
    params: &QueryParameters,
) -> Result<CreateDbParameterGroupInput, AwsError> {
    Ok(CreateDbParameterGroupInput {
        db_parameter_group_family: db_parameter_group_family(
            params.required("DBParameterGroupFamily")?,
        )?,
        db_parameter_group_name: db_parameter_group_name(
            params.required("DBParameterGroupName")?,
        )?,
        description: params.required("Description")?.to_owned(),
    })
}

fn db_instance_identifier(
    value: &str,
) -> Result<DbInstanceIdentifier, AwsError> {
    DbInstanceIdentifier::new(value).map_err(|error| error.to_aws_error())
}

fn optional_db_instance_identifier(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<DbInstanceIdentifier>, AwsError> {
    params.optional(name).map(db_instance_identifier).transpose()
}

fn db_cluster_identifier(
    value: &str,
) -> Result<DbClusterIdentifier, AwsError> {
    DbClusterIdentifier::new(value).map_err(|error| error.to_aws_error())
}

fn optional_db_cluster_identifier(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<DbClusterIdentifier>, AwsError> {
    params.optional(name).map(db_cluster_identifier).transpose()
}

fn db_parameter_group_name(
    value: &str,
) -> Result<DbParameterGroupName, AwsError> {
    DbParameterGroupName::new(value).map_err(|error| error.to_aws_error())
}

fn optional_db_parameter_group_name(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<DbParameterGroupName>, AwsError> {
    params.optional(name).map(db_parameter_group_name).transpose()
}

fn db_parameter_group_family(
    value: &str,
) -> Result<DbParameterGroupFamily, AwsError> {
    DbParameterGroupFamily::new(value).map_err(|error| error.to_aws_error())
}

fn parse_modify_db_parameter_group_input(
    params: &QueryParameters,
) -> Result<ModifyDbParameterGroupInput, AwsError> {
    let mut entries = BTreeMap::<usize, QueryParameterOverride>::new();

    for (name, value) in params.iter() {
        let Some(rest) = name.strip_prefix("Parameters.Parameter.") else {
            continue;
        };
        let Some((index, field)) = rest.split_once('.') else {
            continue;
        };
        let entry = entries.entry(parse_member_index(index)?).or_default();
        match field {
            "ParameterName" => entry.name = Some(value.to_owned()),
            "ParameterValue" => entry.value = Some(value.to_owned()),
            _ => {}
        }
    }

    let parameters = entries
        .into_values()
        .map(|entry| {
            let name = entry.name.ok_or_else(|| {
                missing_parameter_error("Parameters.Parameter.N.ParameterName")
            })?;
            let value = entry.value.ok_or_else(|| {
                missing_parameter_error(
                    "Parameters.Parameter.N.ParameterValue",
                )
            })?;
            Ok((name, value))
        })
        .collect::<Result<BTreeMap<_, _>, AwsError>>()?;

    Ok(ModifyDbParameterGroupInput { parameters })
}

#[derive(Default)]
struct QueryParameterOverride {
    name: Option<String>,
    value: Option<String>,
}

fn instance_response(action: &str, instance: &DbInstance) -> String {
    response_with_result(
        action,
        &XmlBuilder::new().raw(&db_instance_xml(instance)).build(),
    )
}

fn describe_instances_response(instances: &[DbInstance]) -> String {
    let mut xml = XmlBuilder::new().start("DBInstances", None);
    for instance in instances {
        xml = xml.raw(&db_instance_xml(instance));
    }

    response_with_result(
        "DescribeDBInstances",
        &xml.end("DBInstances").build(),
    )
}

fn cluster_response(action: &str, cluster: &DbCluster) -> String {
    response_with_result(
        action,
        &XmlBuilder::new().raw(&db_cluster_xml(cluster)).build(),
    )
}

fn describe_clusters_response(clusters: &[DbCluster]) -> String {
    let mut xml = XmlBuilder::new().start("DBClusters", None);
    for cluster in clusters {
        xml = xml.raw(&db_cluster_xml(cluster));
    }

    response_with_result("DescribeDBClusters", &xml.end("DBClusters").build())
}

fn parameter_group_response(action: &str, group: &DbParameterGroup) -> String {
    response_with_result(
        action,
        &XmlBuilder::new().raw(&db_parameter_group_xml(group)).build(),
    )
}

fn describe_parameter_groups_response(groups: &[DbParameterGroup]) -> String {
    let mut xml = XmlBuilder::new().start("DBParameterGroups", None);
    for group in groups {
        xml = xml.raw(&db_parameter_group_xml(group));
    }

    response_with_result(
        "DescribeDBParameterGroups",
        &xml.end("DBParameterGroups").build(),
    )
}

fn modify_parameter_group_response(group: &DbParameterGroup) -> String {
    response_with_result(
        "ModifyDBParameterGroup",
        &XmlBuilder::new()
            .elem(
                "DBParameterGroupName",
                group.db_parameter_group_name.as_str(),
            )
            .build(),
    )
}

fn describe_parameters_response(parameters: &[DbParameter]) -> String {
    let mut xml = XmlBuilder::new().start("Parameters", None);
    for parameter in parameters {
        xml = xml.raw(&db_parameter_xml(parameter));
    }

    response_with_result(
        "DescribeDBParameters",
        &xml.end("Parameters").build(),
    )
}

fn db_instance_xml(instance: &DbInstance) -> String {
    let mut xml = XmlBuilder::new().start("DBInstance", None);
    xml = xml
        .elem("DBInstanceIdentifier", instance.db_instance_identifier.as_str())
        .elem("DBInstanceClass", &instance.db_instance_class)
        .elem("Engine", instance.engine.as_str())
        .elem("DBInstanceStatus", instance.db_instance_status.as_str())
        .elem("MasterUsername", &instance.master_username)
        .elem("AllocatedStorage", &instance.allocated_storage.to_string())
        .elem(
            "InstanceCreateTime",
            &rfc3339_timestamp(instance.instance_create_time_epoch_seconds),
        )
        .elem("EngineVersion", &instance.engine_version)
        .elem("DBInstancePort", &instance.endpoint.port.to_string())
        .elem("DbiResourceId", &instance.dbi_resource_id)
        .elem("DBInstanceArn", &instance.db_instance_arn)
        .elem(
            "IAMDatabaseAuthenticationEnabled",
            xml_bool(instance.iam_database_authentication_enabled),
        );
    if let Some(db_name) = instance.db_name.as_deref() {
        xml = xml.elem("DBName", db_name);
    }
    if let Some(cluster_identifier) = instance.db_cluster_identifier.as_ref() {
        xml = xml.elem("DBClusterIdentifier", cluster_identifier.as_str());
    }
    xml = xml.raw(&endpoint_xml(
        "Endpoint",
        &instance.endpoint.address,
        instance.endpoint.port,
    ));
    if let Some(parameter_group_name) =
        instance.db_parameter_group_name.as_ref()
    {
        xml = xml.raw(
            &XmlBuilder::new()
                .start("DBParameterGroups", None)
                .start("DBParameterGroup", None)
                .elem("DBParameterGroupName", parameter_group_name.as_str())
                .elem("ParameterApplyStatus", "in-sync")
                .end("DBParameterGroup")
                .end("DBParameterGroups")
                .build(),
        );
    }

    xml.end("DBInstance").build()
}

fn db_cluster_xml(cluster: &DbCluster) -> String {
    let mut xml = XmlBuilder::new().start("DBCluster", None);
    xml = xml
        .elem("DBClusterIdentifier", cluster.db_cluster_identifier.as_str())
        .elem("Status", cluster.status.as_str())
        .elem("Endpoint", &cluster.endpoint.address)
        .elem("Engine", cluster.engine.as_str())
        .elem("EngineVersion", &cluster.engine_version)
        .elem("Port", &cluster.endpoint.port.to_string())
        .elem("MasterUsername", &cluster.master_username)
        .elem("DBClusterResourceId", &cluster.db_cluster_resource_id)
        .elem("DBClusterArn", &cluster.db_cluster_arn)
        .elem(
            "IAMDatabaseAuthenticationEnabled",
            xml_bool(cluster.iam_database_authentication_enabled),
        );
    if let Some(database_name) = cluster.database_name.as_deref() {
        xml = xml.elem("DatabaseName", database_name);
    }
    if let Some(parameter_group_name) =
        cluster.db_cluster_parameter_group.as_ref()
    {
        xml =
            xml.elem("DBClusterParameterGroup", parameter_group_name.as_str());
    }
    if let Some(reader_endpoint) = cluster.reader_endpoint.as_ref() {
        xml = xml.elem("ReaderEndpoint", &reader_endpoint.address);
    }
    let mut members = XmlBuilder::new().start("DBClusterMembers", None);
    for member in &cluster.db_cluster_members {
        members = members.raw(
            &XmlBuilder::new()
                .start("DBClusterMember", None)
                .elem(
                    "DBInstanceIdentifier",
                    member.db_instance_identifier.as_str(),
                )
                .elem("IsClusterWriter", xml_bool(member.is_cluster_writer))
                .elem("DBClusterParameterGroupStatus", "in-sync")
                .elem(
                    "PromotionTier",
                    if member.is_cluster_writer { "1" } else { "2" },
                )
                .end("DBClusterMember")
                .build(),
        );
    }
    xml = xml.raw(&members.end("DBClusterMembers").build());

    xml.end("DBCluster").build()
}

fn db_parameter_group_xml(group: &DbParameterGroup) -> String {
    XmlBuilder::new()
        .start("DBParameterGroup", None)
        .elem("DBParameterGroupName", group.db_parameter_group_name.as_str())
        .elem(
            "DBParameterGroupFamily",
            group.db_parameter_group_family.as_str(),
        )
        .elem("Description", &group.description)
        .elem("DBParameterGroupArn", &group.db_parameter_group_arn)
        .end("DBParameterGroup")
        .build()
}

fn db_parameter_xml(parameter: &DbParameter) -> String {
    XmlBuilder::new()
        .start("Parameter", None)
        .elem("ParameterName", &parameter.parameter_name)
        .elem("ParameterValue", &parameter.parameter_value)
        .elem("Description", &parameter.description)
        .elem("Source", &parameter.source)
        .elem("ApplyType", &parameter.apply_type)
        .elem("DataType", &parameter.data_type)
        .elem("IsModifiable", xml_bool(parameter.is_modifiable))
        .elem("ApplyMethod", "immediate")
        .end("Parameter")
        .build()
}

fn endpoint_xml(name: &str, address: &str, port: u16) -> String {
    XmlBuilder::new()
        .start(name, None)
        .elem("Address", address)
        .elem("Port", &port.to_string())
        .end(name)
        .build()
}

fn response_with_result(action: &str, result: &str) -> String {
    XmlBuilder::new()
        .start(&format!("{action}Response"), Some(RDS_XMLNS))
        .start(&format!("{action}Result"), None)
        .raw(result)
        .end(&format!("{action}Result"))
        .raw(&response_metadata_xml())
        .end(&format!("{action}Response"))
        .build()
}

fn response_without_result(action: &str) -> String {
    response_with_result(action, "")
}

fn response_metadata_xml() -> String {
    XmlBuilder::new()
        .start("ResponseMetadata", None)
        .elem("RequestId", REQUEST_ID)
        .end("ResponseMetadata")
        .build()
}

fn rfc3339_timestamp(epoch_seconds: u64) -> String {
    OffsetDateTime::from_unix_timestamp(epoch_seconds as i64)
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

fn optional_string(params: &QueryParameters, name: &str) -> Option<String> {
    params.optional(name).map(str::to_owned)
}

fn optional_u32(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<u32>, AwsError> {
    params
        .optional(name)
        .map(|value| {
            value.parse::<u32>().map_err(|_| {
                validation_error(format!(
                    "Parameter {name} must be an unsigned integer."
                ))
            })
        })
        .transpose()
}

fn optional_bool(
    params: &QueryParameters,
    name: &str,
) -> Result<Option<bool>, AwsError> {
    params.optional(name).map(parse_bool).transpose().map_err(validation_error)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "1" | "true" | "True" | "TRUE" => Ok(true),
        "0" | "false" | "False" | "FALSE" => Ok(false),
        _ => Err(format!("Boolean parameter {value} must be true or false.")),
    }
}

fn parse_member_index(index: &str) -> Result<usize, AwsError> {
    index.parse::<usize>().map_err(|_| {
        validation_error(format!("Query member index {index} is invalid."))
    })
}

fn reject_unsupported_snapshot_fields(
    params: &QueryParameters,
    action: &str,
) -> Result<(), AwsError> {
    if params.optional("FinalDBSnapshotIdentifier").is_some() {
        return Err(RdsError::InvalidParameterCombination {
            message: format!(
                "{action} does not support final snapshot workflows in the local contract."
            ),
        }
        .to_aws_error());
    }

    Ok(())
}

fn xml_bool(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn invalid_action_error(action: &str) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "InvalidAction",
        format!("Unknown action {action} for service rds."),
        400,
        true,
    )
}

fn validation_error(message: impl Into<String>) -> AwsError {
    AwsError::trusted_custom(
        AwsErrorFamily::Validation,
        "ValidationError",
        message,
        400,
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        db_cluster_xml, db_instance_xml, db_parameter_group_xml,
        db_parameter_xml, response_with_result,
    };
    use rds::{
        DbClusterIdentifier, DbInstanceIdentifier, DbParameterGroupFamily,
        DbParameterGroupName,
    };
    use services::{
        DbCluster, DbClusterMember, DbInstance, DbLifecycleStatus,
        DbParameter, DbParameterGroup, RdsEndpoint, RdsEngine,
    };

    fn db_instance_identifier(value: &str) -> DbInstanceIdentifier {
        DbInstanceIdentifier::new(value)
            .expect("test db instance identifiers should be valid")
    }

    fn db_cluster_identifier(value: &str) -> DbClusterIdentifier {
        DbClusterIdentifier::new(value)
            .expect("test db cluster identifiers should be valid")
    }

    fn db_parameter_group_name(value: &str) -> DbParameterGroupName {
        DbParameterGroupName::new(value)
            .expect("test db parameter group names should be valid")
    }

    fn db_parameter_group_family(value: &str) -> DbParameterGroupFamily {
        DbParameterGroupFamily::new(value)
            .expect("test db parameter group families should be valid")
    }

    #[test]
    fn rds_query_instance_xml_contains_endpoint_and_parameter_groups() {
        let xml = db_instance_xml(&DbInstance {
            allocated_storage: 20,
            db_cluster_identifier: Some(db_cluster_identifier("cluster-demo")),
            db_instance_arn: "arn:aws:rds:eu-west-2:000000000000:db:demo"
                .to_owned(),
            db_instance_class: "db.t3.micro".to_owned(),
            db_instance_identifier: db_instance_identifier("demo"),
            db_instance_status: DbLifecycleStatus::Available,
            db_name: Some("app".to_owned()),
            db_parameter_group_name: Some(db_parameter_group_name(
                "postgres16-demo",
            )),
            dbi_resource_id: "dbi-cloudish-00000001".to_owned(),
            endpoint: RdsEndpoint::loopback(15432),
            engine: RdsEngine::Postgres,
            engine_version: "16.3".to_owned(),
            iam_database_authentication_enabled: true,
            instance_create_time_epoch_seconds: 0,
            master_username: "postgres".to_owned(),
        });

        assert!(xml.contains("<Endpoint><Address>127.0.0.1</Address><Port>15432</Port></Endpoint>"));
        assert!(xml.contains(
            "<DBParameterGroupName>postgres16-demo</DBParameterGroupName>"
        ));
        assert!(xml.contains("<IAMDatabaseAuthenticationEnabled>true</IAMDatabaseAuthenticationEnabled>"));
    }

    #[test]
    fn rds_query_cluster_and_parameter_xml_use_expected_member_tags() {
        let cluster_xml = db_cluster_xml(&DbCluster {
            database_name: Some("app".to_owned()),
            db_cluster_arn: "arn:aws:rds:eu-west-2:000000000000:cluster:demo"
                .to_owned(),
            db_cluster_identifier: db_cluster_identifier("demo"),
            db_cluster_members: vec![DbClusterMember {
                db_instance_identifier: db_instance_identifier("demo-1"),
                is_cluster_writer: true,
            }],
            db_cluster_parameter_group: Some(db_parameter_group_name(
                "aurora-demo",
            )),
            db_cluster_resource_id: "cluster-cloudish-00000001".to_owned(),
            endpoint: RdsEndpoint::loopback(15433),
            engine: RdsEngine::AuroraPostgresql,
            engine_version: "16.3".to_owned(),
            iam_database_authentication_enabled: true,
            master_username: "postgres".to_owned(),
            reader_endpoint: Some(RdsEndpoint::loopback(15433)),
            status: DbLifecycleStatus::Available,
        });
        let group_xml = db_parameter_group_xml(&DbParameterGroup {
            db_parameter_group_arn:
                "arn:aws:rds:eu-west-2:000000000000:pg:demo".to_owned(),
            db_parameter_group_family: db_parameter_group_family("postgres16"),
            db_parameter_group_name: db_parameter_group_name("demo"),
            description: "demo".to_owned(),
        });
        let parameter_xml = db_parameter_xml(&DbParameter {
            apply_type: "dynamic".to_owned(),
            data_type: "string".to_owned(),
            description: "demo".to_owned(),
            is_modifiable: true,
            parameter_name: "work_mem".to_owned(),
            parameter_value: "4MB".to_owned(),
            source: "user".to_owned(),
        });
        let response =
            response_with_result("DescribeDBClusters", &cluster_xml);

        assert!(cluster_xml.contains("<DBClusterMember><DBInstanceIdentifier>demo-1</DBInstanceIdentifier>"));
        assert!(group_xml.contains("<DBParameterGroupArn>arn:aws:rds:eu-west-2:000000000000:pg:demo</DBParameterGroupArn>"));
        assert!(
            parameter_xml.contains("<ParameterName>work_mem</ParameterName>")
        );
        assert!(response.contains("<DescribeDBClustersResponse xmlns=\"http://rds.amazonaws.com/doc/2014-10-31/\">"));
    }
}
