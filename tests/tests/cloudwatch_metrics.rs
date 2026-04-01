#![allow(
    clippy::unreachable,
    clippy::assertions_on_constants,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::expect_used,
    clippy::panic
)]
use tests::common::runtime;
use tests::common::sdk;

use aws_sdk_cloudwatch::Client as CloudWatchClient;
use aws_sdk_cloudwatch::error::ProvideErrorMetadata;
use aws_sdk_cloudwatch::primitives::DateTime;
use aws_sdk_cloudwatch::types::{
    ComparisonOperator, Dimension, Metric, MetricDataQuery, MetricDatum,
    MetricStat, ScanBy, StandardUnit, StateValue, Statistic,
};
use runtime::RuntimeServer;
use sdk::SdkSmokeTarget;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

fn unique_root() -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("sdk-cloudwatch-{id}")
}

#[tokio::test]
async fn cloudwatch_metrics_core_lifecycle_round_trips() {
    let runtime = RuntimeServer::spawn("sdk-cloudwatch-metrics").await;
    let target = SdkSmokeTarget::new(
        format!("http://{}", runtime.address()),
        "eu-west-2",
    );
    let config = target.load().await;
    let client = CloudWatchClient::new(&config);

    let root = unique_root();
    let namespace = format!("Demo/{root}");
    let alarm_name = format!("{root}-latency-alarm");
    let dimension = Dimension::builder().name("Service").value("api").build();

    client
        .put_metric_data()
        .namespace(&namespace)
        .metric_data(
            MetricDatum::builder()
                .metric_name("Latency")
                .dimensions(dimension.clone())
                .timestamp(DateTime::from_secs(60))
                .unit(StandardUnit::Count)
                .value(3.0)
                .build(),
        )
        .metric_data(
            MetricDatum::builder()
                .metric_name("Latency")
                .dimensions(dimension.clone())
                .timestamp(DateTime::from_secs(120))
                .unit(StandardUnit::Count)
                .value(5.0)
                .build(),
        )
        .send()
        .await
        .expect("metric data should write");

    let listed = client
        .list_metrics()
        .namespace(&namespace)
        .metric_name("Latency")
        .send()
        .await
        .expect("metrics should list");
    assert!(listed.metrics().iter().any(|metric| {
        metric.namespace() == Some(namespace.as_str())
            && metric.metric_name() == Some("Latency")
    }));

    let stats = client
        .get_metric_statistics()
        .namespace(&namespace)
        .metric_name("Latency")
        .dimensions(dimension.clone())
        .start_time(DateTime::from_secs(0))
        .end_time(DateTime::from_secs(180))
        .period(60)
        .statistics(Statistic::Average)
        .send()
        .await
        .expect("metric statistics should load");
    let mut averages = stats
        .datapoints()
        .iter()
        .filter_map(|point| point.average())
        .collect::<Vec<_>>();
    averages.sort_by(f64::total_cmp);
    assert_eq!(averages, vec![3.0, 5.0]);

    let metric_data = client
        .get_metric_data()
        .start_time(DateTime::from_secs(0))
        .end_time(DateTime::from_secs(180))
        .scan_by(ScanBy::TimestampAscending)
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("m1")
                .metric_stat(
                    MetricStat::builder()
                        .metric(
                            Metric::builder()
                                .namespace(&namespace)
                                .metric_name("Latency")
                                .dimensions(dimension.clone())
                                .build(),
                        )
                        .period(60)
                        .stat("Average")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("metric data query should succeed");
    assert_eq!(metric_data.metric_data_results().len(), 1);
    assert_eq!(metric_data.metric_data_results()[0].label(), Some("Latency"));
    assert_eq!(metric_data.metric_data_results()[0].values(), &[3.0, 5.0]);

    client
        .put_metric_alarm()
        .alarm_name(&alarm_name)
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .evaluation_periods(1)
        .metric_name("Latency")
        .namespace(&namespace)
        .period(60)
        .statistic(Statistic::Average)
        .threshold(4.0)
        .send()
        .await
        .expect("alarm should create");

    let described = client
        .describe_alarms()
        .alarm_names(alarm_name.clone())
        .send()
        .await
        .expect("alarms should describe");
    assert_eq!(described.metric_alarms().len(), 1);
    assert_eq!(
        described.metric_alarms()[0].alarm_name(),
        Some(alarm_name.as_str())
    );

    client
        .set_alarm_state()
        .alarm_name(&alarm_name)
        .state_reason("manual override")
        .state_value(StateValue::Alarm)
        .send()
        .await
        .expect("alarm state should update");

    let described = client
        .describe_alarms()
        .alarm_names(alarm_name.clone())
        .send()
        .await
        .expect("alarms should re-describe");
    assert_eq!(
        described.metric_alarms()[0].state_value(),
        Some(&StateValue::Alarm)
    );
    assert_eq!(
        described.metric_alarms()[0].state_reason(),
        Some("manual override")
    );

    client
        .delete_alarms()
        .alarm_names(alarm_name.clone())
        .send()
        .await
        .expect("alarm should delete");

    let missing_alarm = client
        .set_alarm_state()
        .alarm_name(&alarm_name)
        .state_reason("missing")
        .state_value(StateValue::Ok)
        .send()
        .await
        .expect_err("missing alarm mutation should fail");
    assert_eq!(missing_alarm.code(), Some("ResourceNotFound"));

    assert!(runtime.state_directory().exists());
    runtime.shutdown().await;
}
