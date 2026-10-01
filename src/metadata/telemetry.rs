use super::Result;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct Metrics {
    // Immutable state
    backend: &'static str,
    operations: Counter<u64>,
    duration: Histogram<f64>,
    registrations: Counter<u64>,
    registered: Gauge<u64>,
    // Mutable state
    last_error_log: Arc<Mutex<Option<Instant>>>,
}

impl Metrics {
    pub(crate) fn new(backend: &'static str, meter: Option<Meter>) -> Self {
        let meter = meter.unwrap_or_else(|| global::meter("lyra.meta"));
        Self {
            backend,
            operations: meter.u64_counter("lyra_meta_operations_total").build(),
            duration: meter
                .f64_histogram("lyra_meta_operation_duration_seconds")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
                    60.0,
                ])
                .build(),
            registrations: meter
                .u64_counter("lyra_component_registration_operations_total")
                .build(),
            registered: meter.u64_gauge("lyra_component_registered").build(),
            last_error_log: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn observe<T>(
        &self,
        operation: &'static str,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let start = Instant::now();
        let result = future.await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        let labels = [
            KeyValue::new("operation", operation),
            KeyValue::new("backend", self.backend),
            KeyValue::new("outcome", outcome),
        ];
        self.operations.add(1, &labels);
        self.duration.record(start.elapsed().as_secs_f64(), &labels);
        if result.is_err() {
            let mut last = self
                .last_error_log
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(5)) {
                tracing::warn!(
                    operation,
                    backend = self.backend,
                    outcome,
                    error_class = "metadata",
                    "metadata operation failed; mutation outcome may require reconciliation"
                );
                *last = Some(Instant::now());
            }
        } else {
            tracing::debug!(
                operation,
                backend = self.backend,
                outcome,
                "metadata operation completed"
            );
        }
        result
    }

    pub(crate) fn registration(&self, component: &str, operation: &'static str, success: bool) {
        let outcome = if success { "success" } else { "error" };
        self.registrations.add(
            1,
            &[
                KeyValue::new("component_type", component_label(component)),
                KeyValue::new("operation", operation),
                KeyValue::new("outcome", outcome),
            ],
        );
        tracing::info!(
            component_type = component_label(component),
            operation,
            outcome,
            event = "component_registration",
            "component registration transition"
        );
    }

    pub(crate) fn registered(&self, component: &str, present: bool) {
        self.registered.record(
            u64::from(present),
            &[KeyValue::new("component_type", component_label(component))],
        );
    }

    pub(crate) fn reconciled(&self, operation: &'static str) {
        let mut last = self
            .last_error_log
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(5)) {
            tracing::warn!(
                event = "metadata_write_reconciled",
                operation,
                outcome = "confirmed",
                "metadata reply was lost; write reconciled without retry"
            );
            *last = Some(Instant::now());
        }
    }
}

fn component_label(component: &str) -> &'static str {
    match component {
        "catalog" => "catalog",
        "func" => "func",
        "stream" => "stream",
        _ => "other",
    }
}
