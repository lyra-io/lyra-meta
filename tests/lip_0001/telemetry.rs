//! Prototype instrumentation only. No global providers, OTLP transport, Collector,
//! Prometheus scrape, or production Catalog instruments are claimed by these tests.

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use std::collections::BTreeSet;
use std::io::{Result, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::Dispatch;

pub struct Telemetry {
    // Immutable state
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
    backend: &'static str,
    operations: Counter<u64>,
    duration: Histogram<f64>,
    registrations: Counter<u64>,
    registered: Gauge<u64>,
}

impl Telemetry {
    pub fn new(backend: &'static str) -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(exporter.clone())
                    .with_interval(Duration::from_secs(3600))
                    .build(),
            )
            .build();
        let meter = provider.meter("lyra.meta.mvp");
        Self {
            provider,
            exporter,
            backend,
            operations: meter.u64_counter("lyra_meta_operations_total").build(),
            duration: meter
                .f64_histogram("lyra_meta_operation_duration_seconds")
                .with_unit("s")
                .build(),
            registrations: meter
                .u64_counter("lyra_component_registration_operations_total")
                .build(),
            registered: meter.u64_gauge("lyra_component_registered").build(),
        }
    }

    pub fn operation(&self, operation: &'static str, start: Instant, success: bool) {
        let outcome = if success { "success" } else { "error" };
        let attributes = [
            KeyValue::new("operation", operation),
            KeyValue::new("backend", self.backend),
            KeyValue::new("outcome", outcome),
        ];
        self.operations.add(1, &attributes);
        self.duration
            .record(start.elapsed().as_secs_f64(), &attributes);
        // Never format input, values, backend error strings, SQL, or names.
        if success {
            tracing::info!(target: "lyra_mvp", operation, backend = self.backend, outcome, "metadata mutation");
        } else {
            tracing::warn!(target: "lyra_mvp", operation, backend = self.backend, outcome, "metadata mutation failed");
        }
    }

    pub fn registration(&self, operation: &'static str, present: bool) {
        self.registrations.add(
            1,
            &[
                KeyValue::new("component_type", "catalog"),
                KeyValue::new("operation", operation),
                KeyValue::new("outcome", "success"),
            ],
        );
        self.registered.record(
            u64::from(present),
            &[KeyValue::new("component_type", "catalog")],
        );
        tracing::info!(target: "lyra_mvp", component_type = "catalog", operation, outcome = "success", "registration transition");
    }

    pub fn assert_registered(&self, present: bool) {
        self.provider.force_flush().unwrap();
        let exported = self.exporter.get_finished_metrics().unwrap();
        let metric = exported
            .iter()
            .flat_map(|r| r.scope_metrics())
            .flat_map(|s| s.metrics())
            .find(|m| m.name() == "lyra_component_registered")
            .unwrap();
        let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
            panic!("registration must use a gauge");
        };
        let points: Vec<_> = gauge.data_points().collect();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value(), u64::from(present));
        assert_attributes(points[0].attributes());
        // The SDK is cumulative; keep just the next snapshot for final assertions.
        self.exporter.reset();
    }

    pub fn assert_exported(&self, registration: bool) {
        self.provider.force_flush().unwrap();
        let exported = self.exporter.get_finished_metrics().unwrap();
        let mut names = BTreeSet::new();
        let mut success = 0;
        let mut errors = 0;
        let mut samples = 0;
        let mut registrations = 0;
        for resource in &exported {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    names.insert(metric.name().to_string());
                    match metric.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                assert_attributes(point.attributes());
                                if metric.name() == "lyra_meta_operations_total" {
                                    if point.attributes().any(|a| {
                                        a.key.as_str() == "outcome" && a.value.as_str() == "error"
                                    }) {
                                        errors += point.value();
                                    } else {
                                        success += point.value();
                                    }
                                } else if metric.name()
                                    == "lyra_component_registration_operations_total"
                                {
                                    registrations += point.value();
                                }
                            }
                        }
                        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                            assert_eq!(metric.unit(), "s");
                            for point in histogram.data_points() {
                                assert_attributes(point.attributes());
                                assert!(point.sum().is_finite() && point.sum() >= 0.0);
                                samples += point.count();
                            }
                        }
                        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                            for point in gauge.data_points() {
                                assert_attributes(point.attributes());
                                assert_eq!(point.value(), 0, "registration must be absent at end");
                            }
                        }
                        _ => panic!("unexpected metric aggregation"),
                    }
                }
            }
        }
        let mut expected = BTreeSet::from([
            "lyra_meta_operations_total".to_string(),
            "lyra_meta_operation_duration_seconds".to_string(),
        ]);
        if registration {
            expected.insert("lyra_component_registration_operations_total".into());
            expected.insert("lyra_component_registered".into());
        }
        assert_eq!(names, expected);
        assert!(success > 0 && errors > 0);
        assert_eq!(samples, success + errors);
        assert_eq!(registrations, if registration { 2 } else { 0 });
    }
}

fn assert_attributes<'a>(attributes: impl Iterator<Item = &'a KeyValue>) {
    for attribute in attributes {
        let allowed: &[&str] = match attribute.key.as_str() {
            "operation" => &[
                "get",
                "list",
                "create",
                "update",
                "delete",
                "register",
                "unregister",
            ],
            "backend" => &["memory", "oxia"],
            "outcome" => &["success", "error"],
            "component_type" => &["catalog"],
            _ => panic!("unbounded metric attribute"),
        };
        assert!(allowed.contains(&attribute.value.as_str().as_ref()));
    }
}

#[derive(Clone, Default)]
pub struct Logs {
    // Mutable state
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Logs {
    pub fn dispatch(&self) -> Dispatch {
        let writer = self.clone();
        Dispatch::new(
            tracing_subscriber::fmt()
                .json()
                .with_ansi(false)
                .without_time()
                .with_max_level(tracing::Level::INFO)
                .with_writer(move || writer.clone())
                .finish(),
        )
    }

    pub fn text(&self) -> String {
        String::from_utf8(self.bytes.lock().unwrap().clone()).unwrap()
    }
}
