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
#[path = "common/rds.rs"]
mod rds;
#[path = "common/runtime.rs"]
mod runtime;
#[path = "common/sdk.rs"]
mod sdk;

use aws_sdk_rds::Client as RdsClient;
use aws_sdk_rds::error::ProvideErrorMetadata;
use aws_sdk_rds::types::{ApplyMethod, Parameter};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;

#[tokio::test]
async fn instances_and_parameter_groups_round_trip() {
    let runtime = RuntimeServer::spawn("tests-rds-control-instance").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = RdsClient::new(&config);

    let parameter_group_name = rds::unique_name("sdk-pg");
    let instance_identifier = rds::unique_name("sdk-db");
    let master_password = "PgPassword123!";

    let parameter_group = client
        .create_db_parameter_group()
        .db_parameter_group_name(&parameter_group_name)
        .db_parameter_group_family("postgres16")
        .description("demo")
        .send()
        .await
        .expect("parameter group should create");
    assert_eq!(
        parameter_group
            .db_parameter_group()
            .and_then(|group| group.db_parameter_group_name()),
        Some(parameter_group_name.as_str())
    );

    client
        .modify_db_parameter_group()
        .db_parameter_group_name(&parameter_group_name)
        .parameters(
            Parameter::builder()
                .parameter_name("max_connections")
                .parameter_value("250")
                .apply_method(ApplyMethod::Immediate)
                .build(),
        )
        .send()
        .await
        .expect("parameter group should update");

    let described_parameters = client
        .describe_db_parameters()
        .db_parameter_group_name(&parameter_group_name)
        .send()
        .await
        .expect("parameters should describe");
    assert!(described_parameters.parameters().iter().any(|parameter| {
        parameter.parameter_name() == Some("max_connections")
            && parameter.parameter_value() == Some("250")
            && parameter.source() == Some("user")
    }));

    let created = client
        .create_db_instance()
        .db_instance_identifier(&instance_identifier)
        .engine("postgres")
        .master_username("postgres")
        .master_user_password(master_password)
        .db_name("app")
        .db_parameter_group_name(&parameter_group_name)
        .enable_iam_database_authentication(true)
        .send()
        .await
        .expect("instance should create");
    let instance = created
        .db_instance()
        .expect("create output should include a DB instance");
    let endpoint = instance
        .endpoint()
        .expect("created instance should expose an endpoint");
    let host = endpoint.address().expect("endpoint should include an address");
    let port = endpoint.port().expect("endpoint should include a port") as u16;

    assert_eq!(
        rds::postgres_query_eventually(
            host,
            port,
            "postgres",
            master_password,
            Some("app"),
        )
        .await,
        "1"
    );

    let described_instance = client
        .describe_db_instances()
        .db_instance_identifier(&instance_identifier)
        .send()
        .await
        .expect("instance should describe");
    let described = described_instance
        .db_instances()
        .first()
        .expect("describe should return the created instance");
    assert_eq!(described.engine(), Some("postgres"));
    assert_eq!(described.db_instance_status(), Some("available"));
    assert_eq!(described.iam_database_authentication_enabled(), Some(true));
    assert!(described.db_parameter_groups().iter().any(|group| {
        group.db_parameter_group_name() == Some(parameter_group_name.as_str())
    }));

    client
        .reboot_db_instance()
        .db_instance_identifier(&instance_identifier)
        .send()
        .await
        .expect("instance should reboot");
    assert_eq!(
        rds::postgres_query_eventually(
            host,
            port,
            "postgres",
            master_password,
            Some("app"),
        )
        .await,
        "1"
    );

    client
        .delete_db_instance()
        .db_instance_identifier(&instance_identifier)
        .send()
        .await
        .expect("instance should delete");
    rds::wait_for_port_closed(host, port).await;

    client
        .delete_db_parameter_group()
        .db_parameter_group_name(&parameter_group_name)
        .send()
        .await
        .expect("parameter group should delete");

    let unsupported = client
        .create_db_instance()
        .db_instance_identifier(rds::unique_name("sdk-unsupported"))
        .engine("oracle")
        .master_username("admin")
        .master_user_password("Secret123!")
        .send()
        .await
        .expect_err("unsupported engines should fail");
    assert_eq!(unsupported.code(), Some("InvalidParameterValue"));

    runtime.shutdown().await;
}

#[tokio::test]
async fn clusters_reject_invalid_delete_state() {
    let runtime = RuntimeServer::spawn("tests-rds-control-cluster").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = RdsClient::new(&config);

    let cluster_identifier = rds::unique_name("sdk-cluster");
    let member_identifier = rds::unique_name("sdk-member");

    let created_cluster = client
        .create_db_cluster()
        .db_cluster_identifier(&cluster_identifier)
        .engine("aurora-postgresql")
        .master_username("postgres")
        .master_user_password("ClusterPass123!")
        .database_name("app")
        .enable_iam_database_authentication(true)
        .send()
        .await
        .expect("cluster should create");
    let cluster = created_cluster
        .db_cluster()
        .expect("create output should include a DB cluster");
    let host = cluster.endpoint().expect("cluster should expose an endpoint");
    let port = cluster.port().expect("cluster should expose a port") as u16;

    assert_eq!(
        rds::postgres_query_eventually(
            host,
            port,
            "postgres",
            "ClusterPass123!",
            Some("app"),
        )
        .await,
        "1"
    );

    client
        .create_db_instance()
        .db_cluster_identifier(&cluster_identifier)
        .db_instance_identifier(&member_identifier)
        .send()
        .await
        .expect("cluster member should create");

    let described_cluster = client
        .describe_db_clusters()
        .db_cluster_identifier(&cluster_identifier)
        .send()
        .await
        .expect("cluster should describe");
    assert!(
        described_cluster.db_clusters()[0].db_cluster_members().iter().any(
            |member| {
                member.db_instance_identifier()
                    == Some(member_identifier.as_str())
            }
        )
    );

    let delete_cluster = client
        .delete_db_cluster()
        .db_cluster_identifier(&cluster_identifier)
        .send()
        .await
        .expect_err("clusters with members should fail delete");
    assert_eq!(delete_cluster.code(), Some("InvalidDBClusterStateFault"));

    client
        .delete_db_instance()
        .db_instance_identifier(&member_identifier)
        .send()
        .await
        .expect("member should delete");
    client
        .delete_db_cluster()
        .db_cluster_identifier(&cluster_identifier)
        .send()
        .await
        .expect("cluster should delete");
    rds::wait_for_port_closed(host, port).await;

    runtime.shutdown().await;
}

#[tokio::test]
async fn postgres_and_mysql_proxy_auth_round_trip() {
    let runtime = RuntimeServer::spawn("tests-rds-proxy-auth").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = RdsClient::new(&config);

    let postgres_identifier = rds::unique_name("tests-pg");
    let postgres_password = "PgPassword123!";
    let postgres_instance = client
        .create_db_instance()
        .db_instance_identifier(&postgres_identifier)
        .engine("postgres")
        .master_username("postgres")
        .master_user_password(postgres_password)
        .db_name("app")
        .enable_iam_database_authentication(true)
        .send()
        .await
        .expect("postgres instance should create");
    let postgres_endpoint = postgres_instance
        .db_instance()
        .and_then(|instance| instance.endpoint())
        .expect("postgres instance should expose an endpoint");
    let postgres_host = postgres_endpoint
        .address()
        .expect("postgres endpoint should include an address")
        .to_owned();
    let postgres_port = postgres_endpoint
        .port()
        .expect("postgres endpoint should include a port")
        as u16;

    assert_eq!(
        rds::postgres_query_eventually(
            &postgres_host,
            postgres_port,
            "postgres",
            postgres_password,
            Some("app"),
        )
        .await,
        "1"
    );
    assert!(
        rds::postgres_query(
            &postgres_host,
            postgres_port,
            "postgres",
            "wrong-password",
            Some("app"),
        )
        .await
        .is_err()
    );

    let iam_token = rds::postgres_iam_token(
        &postgres_host,
        postgres_port,
        "eu-west-2",
        "postgres",
    );
    assert_eq!(
        rds::postgres_query_eventually(
            &postgres_host,
            postgres_port,
            "postgres",
            &iam_token,
            Some("app"),
        )
        .await,
        "1"
    );
    let broken_iam_token = format!("{iam_token}-broken");
    assert!(
        rds::postgres_query(
            &postgres_host,
            postgres_port,
            "postgres",
            &broken_iam_token,
            Some("app"),
        )
        .await
        .is_err()
    );

    client
        .delete_db_instance()
        .db_instance_identifier(&postgres_identifier)
        .send()
        .await
        .expect("postgres instance should delete");
    rds::wait_for_port_closed(&postgres_host, postgres_port).await;

    let unsupported_mysql_iam = client
        .create_db_instance()
        .db_instance_identifier(rds::unique_name("tests-mysql-iam"))
        .engine("mysql")
        .master_username("admin")
        .master_user_password("MyPassword123!")
        .enable_iam_database_authentication(true)
        .send()
        .await
        .expect_err("mysql IAM auth should fail");
    assert_eq!(
        unsupported_mysql_iam.code(),
        Some("InvalidParameterCombination")
    );

    let mysql_identifier = rds::unique_name("tests-mysql");
    let mysql_password = "MyPassword123!";
    let mysql_instance = client
        .create_db_instance()
        .db_instance_identifier(&mysql_identifier)
        .engine("mysql")
        .master_username("admin")
        .master_user_password(mysql_password)
        .db_name("app")
        .send()
        .await
        .expect("mysql instance should create");
    let mysql_endpoint = mysql_instance
        .db_instance()
        .and_then(|instance| instance.endpoint())
        .expect("mysql instance should expose an endpoint");
    let mysql_host = mysql_endpoint
        .address()
        .expect("mysql endpoint should include an address")
        .to_owned();
    let mysql_port =
        mysql_endpoint.port().expect("mysql endpoint should include a port")
            as u16;

    assert_eq!(
        rds::mysql_query_eventually(
            &mysql_host,
            mysql_port,
            "admin",
            mysql_password,
            Some("app"),
        )
        .await,
        1
    );
    assert!(
        rds::mysql_query(
            &mysql_host,
            mysql_port,
            "admin",
            "wrong-password",
            Some("app"),
        )
        .await
        .is_err()
    );

    client
        .delete_db_instance()
        .db_instance_identifier(&mysql_identifier)
        .send()
        .await
        .expect("mysql instance should delete");
    rds::wait_for_port_closed(&mysql_host, mysql_port).await;

    runtime.shutdown().await;
}
