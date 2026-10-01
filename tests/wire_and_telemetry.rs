use lyra_meta::metadata::{MemoryMetadata, Metadata};
use lyra_meta::proto::pb_meta::{
    Allocator, ComponentRegistration, Database, Instance, ScramSha256Verifier, User,
};
use lyra_meta::utils::verifier::make_verifier;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use prost::Message;
use std::collections::BTreeSet;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::instrument::WithSubscriber;

#[test]
fn stable_wire_tags_and_redacted_credentials() {
    assert!(Instance { initialized: None }.encode_to_vec().is_empty());
    assert_eq!(
        Instance {
            initialized: Some(false)
        }
        .encode_to_vec(),
        [8, 0]
    );
    assert_eq!(
        Instance {
            initialized: Some(true)
        }
        .encode_to_vec(),
        [8, 1]
    );
    assert!(ComponentRegistration {}.encode_to_vec().is_empty());
    assert_eq!(Allocator { last_allocated: 7 }.encode_to_vec(), [8, 7]);
    let database = Database {
        name: "db".into(),
        owner_user_id: 7,
        id: 9,
        ..Default::default()
    };
    assert_eq!(database.encode_to_vec(), [10, 2, b'd', b'b', 16, 7, 48, 9]);
    assert!(database.accepts_connections());
    assert_eq!(database.effective_connection_limit(), -1);
    let database = Database {
        allow_connections: Some(false),
        connection_limit: Some(0),
        state: 2,
        ..database
    };
    assert_eq!(
        database.encode_to_vec(),
        [10, 2, b'd', b'b', 16, 7, 24, 0, 32, 0, 40, 2, 48, 9]
    );
    let fixture = ScramSha256Verifier {
        salt: vec![1].into(),
        iterations: 4096,
        stored_key: vec![2].into(),
        server_key: vec![3].into(),
    };
    assert_eq!(
        fixture.encode_to_vec(),
        [10, 1, 1, 16, 128, 32, 26, 1, 2, 34, 1, 3]
    );
    let user = User {
        id: 1,
        name: "private-name".into(),
        password_verifier: Some(make_verifier("password-not-to-log").unwrap()),
    };
    assert_eq!(User::decode(user.encode_to_vec().as_slice()).unwrap(), user);
    assert_eq!(format!("{user:?}"), "User { [REDACTED] }");
    assert_eq!(
        format!("{:?}", user.password_verifier.as_ref().unwrap()),
        "ScramSha256Verifier { [REDACTED] }"
    );
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn production_metrics_and_logs_are_bounded_and_redacted() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(exporter.clone())
                .with_interval(Duration::from_secs(3600))
                .build(),
        )
        .build();
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    async {
        let metadata = MemoryMetadata::with_meter(provider.meter("lyra-meta-test"));
        metadata
            .initialize(make_verifier("password-not-to-log").unwrap())
            .await
            .unwrap();
        let owner = metadata.get_user("lyrasys").await.unwrap().unwrap();
        metadata
            .create_database(Database::new("private-db-name", owner.id()))
            .await
            .unwrap();
        assert!(
            metadata
                .create_database(Database::new("private-db-name", owner.id()))
                .await
                .is_err()
        );
        let registration = metadata.register_component("catalog").await.unwrap();
        assert!(registration.is_registered().await.unwrap());
        registration.unregister().await.unwrap();
    }
    .with_subscriber(subscriber)
    .await;
    provider.force_flush().unwrap();
    let exported = exporter.get_finished_metrics().unwrap();
    let mut names = BTreeSet::new();
    let mut operations = 0;
    let mut samples = 0;
    let mut errors = 0;
    for metric in exported
        .iter()
        .flat_map(|r| r.scope_metrics())
        .flat_map(|s| s.metrics())
    {
        names.insert(metric.name().to_string());
        match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                for point in sum.data_points() {
                    for a in point.attributes() {
                        assert!(matches!(
                            a.key.as_str(),
                            "operation" | "backend" | "outcome" | "component_type"
                        ));
                    }
                    if metric.name() == "lyra_meta_operations_total" {
                        operations += point.value();
                        if point
                            .attributes()
                            .any(|a| a.key.as_str() == "outcome" && a.value.as_str() == "error")
                        {
                            errors += point.value();
                        }
                    }
                }
            }
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                for point in histogram.data_points() {
                    samples += point.count();
                }
            }
            AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                for point in gauge.data_points() {
                    assert_eq!(point.value(), 0);
                }
            }
            _ => panic!("unexpected metric aggregation"),
        }
    }
    assert_eq!(operations, samples);
    assert!(operations > 0 && errors > 0);
    assert_eq!(
        names,
        BTreeSet::from([
            "lyra_meta_operations_total".into(),
            "lyra_meta_operation_duration_seconds".into(),
            "lyra_component_registration_operations_total".into(),
            "lyra_component_registered".into(),
        ])
    );
    let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("metadata initialization completed"));
    assert!(logs.contains("metadata operation failed"));
    assert!(!logs.contains("private-db-name"));
    assert!(!logs.contains("password-not-to-log"));
    assert!(!logs.contains("stored_key"));
    assert!(!logs.contains("server_key"));
}
