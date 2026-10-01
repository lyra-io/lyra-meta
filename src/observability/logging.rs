use crate::config::LogLevel;
use crate::toolkit::ReloadError;
use opentelemetry::metrics::{Meter, ObservableCounter};
use serde_json::Value;
use std::fmt;
use std::io;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    filter::filter_fn,
    fmt::{
        FmtContext, FormatEvent, FormatFields,
        format::{self, Writer},
    },
    layer::SubscriberExt,
    registry::LookupSpan,
    reload,
    util::SubscriberInitExt,
};

struct ServiceJson;
impl<S, N> FormatEvent<S, N> for ServiceJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut line = String::new();
        format::format().json().flatten_event(true).format_event(
            context,
            Writer::new(&mut line),
            event,
        )?;
        let mut value: Value = serde_json::from_str(&line).map_err(|_| fmt::Error)?;
        value["service"] = Value::from("lyra-catalog");
        value["component"] = Value::from(
            event
                .metadata()
                .target()
                .split("::")
                .next()
                .unwrap_or("lyra"),
        );
        value["severity"] = Value::from(event.metadata().level().as_str());
        writeln!(writer, "{value}")
    }
}

pub(super) type DynamicLayer = Box<dyn Layer<Registry> + Send + Sync>;
pub(super) type ConsoleReload = reload::Handle<Option<DynamicLayer>, Registry>;
pub(super) struct Logging {
    // Control state
    monitor: JoinHandle<()>,
    _stdout: WorkerGuard,
    // Immutable state
    update: Box<dyn Fn(LogLevel) + Send + Sync>,
    pub(super) console: ConsoleReload,
    _dropped: ObservableCounter<u64>,
}

fn filter(level: LogLevel) -> EnvFilter {
    let level = match level {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    };
    // No third-party events, raw SQL, credentials, or Console per-poll events.
    EnvFilter::new(format!(
        "off,lyra_catalog={level},lyra_catalog_cli={level},lyra_meta={level}"
    ))
}
impl Logging {
    pub(super) fn new(meter: &Meter, level: LogLevel) -> Result<Self, ReloadError> {
        let (writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(1024)
            .lossy(true)
            .finish(io::stdout());
        let drops = writer.error_counter();
        let counter = drops.clone();
        let dropped = meter
            .u64_observable_counter("lyra_catalog_log_dropped_total")
            .with_callback(move |o| o.observe(counter.dropped_lines() as u64, &[]))
            .build();
        let (log_filter, update) = reload::Layer::new(filter(level));
        let log: DynamicLayer = tracing_subscriber::fmt::layer()
            .json()
            .event_format(ServiceJson)
            .with_writer(writer)
            .with_filter(log_filter)
            .boxed();
        let (console, handle): (_, ConsoleReload) = reload::Layer::new(None::<DynamicLayer>);
        // Layers remain at the same Registry type, allowing independent reloads.
        // Register the filter once, outside reload: inserting a new Filtered
        // layer later would not run on_layer() to allocate its FilterId.
        let console = console.with_filter(filter_fn(|metadata| {
            metadata.target().starts_with("tokio")
                || if metadata.is_event() {
                    metadata.target().starts_with("runtime")
                } else {
                    metadata.name().starts_with("runtime.")
                }
        }));
        let layers: Vec<DynamicLayer> = vec![log, console.boxed()];
        tracing_subscriber::registry()
            .with(layers)
            .try_init()
            .map_err(|_| ReloadError("logging_setup"))?;
        let monitor = tokio::spawn(async move {
            let mut previous = 0;
            let mut dropping = false;
            let mut ticks = tokio::time::interval(Duration::from_secs(5));
            loop {
                ticks.tick().await;
                let current = drops.dropped_lines();
                if current > previous {
                    tracing::warn!(
                        event = "log_records_dropped",
                        dropped_records = current - previous,
                        "stdout queue dropped log records"
                    );
                    dropping = true;
                } else if dropping {
                    tracing::info!(
                        event = "log_queue_recovered",
                        "no new stdout drops in the last interval"
                    );
                    dropping = false;
                }
                previous = current;
            }
        });
        Ok(Self {
            monitor,
            _stdout: guard,
            update: Box::new(move |level| {
                update
                    .reload(filter(level))
                    .expect("global logging subscriber remains installed");
            }),
            console: handle,
            _dropped: dropped,
        })
    }
    pub(super) fn update(&self, level: LogLevel) {
        (self.update)(level);
    }
}
impl Drop for Logging {
    fn drop(&mut self) {
        self.monitor.abort();
        let _ = self.console.reload(None);
    }
}
