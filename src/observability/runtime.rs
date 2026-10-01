use opentelemetry::{
    KeyValue,
    metrics::{Meter, ObservableCounter, ObservableGauge},
};
use tokio::runtime::Handle;

pub(super) struct Instruments {
    // Immutable state
    _cpu: ObservableCounter<f64>,
    _rss: ObservableGauge<u64>,
    _workers: ObservableGauge<u64>,
    _tasks: ObservableGauge<u64>,
    _queue: ObservableGauge<u64>,
    _busy: ObservableCounter<f64>,
}

impl Instruments {
    pub(super) fn new(meter: &Meter, runtime: &Handle) -> Self {
        if resident_memory().is_none() {
            tracing::warn!(
                event = "process_measurement_unavailable",
                measurement = "resident_memory",
                "unsupported process measurement omitted"
            );
        }
        let metrics = runtime.metrics();
        let workers = metrics.clone();
        let tasks = metrics.clone();
        let queue = metrics.clone();
        Self {
            _cpu: meter
                .f64_observable_counter("lyra_process_cpu_seconds_total")
                .with_callback(|o| {
                    if let Some(cpu) = cpu_seconds() {
                        o.observe(cpu, &[]);
                    }
                })
                .build(),
            _rss: meter
                .u64_observable_gauge("lyra_process_resident_memory_bytes")
                .with_callback(|o| {
                    if let Some(rss) = resident_memory() {
                        o.observe(rss, &[]);
                    }
                })
                .build(),
            _workers: meter
                .u64_observable_gauge("lyra_tokio_workers")
                .with_callback(move |o| o.observe(workers.num_workers() as u64, &[]))
                .build(),
            _tasks: meter
                .u64_observable_gauge("lyra_tokio_alive_tasks")
                .with_callback(move |o| o.observe(tasks.num_alive_tasks() as u64, &[]))
                .build(),
            _queue: meter
                .u64_observable_gauge("lyra_tokio_global_queue_depth")
                .with_callback(move |o| o.observe(queue.global_queue_depth() as u64, &[]))
                .build(),
            _busy: meter
                .f64_observable_counter("lyra_tokio_worker_busy_seconds_total")
                .with_callback(move |o| {
                    for worker in 0..metrics.num_workers() {
                        o.observe(
                            metrics.worker_total_busy_duration(worker).as_secs_f64(),
                            &[KeyValue::new("worker", worker as i64)],
                        );
                    }
                })
                .build(),
        }
    }
}

fn cpu_seconds() -> Option<f64> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the supplied rusage on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return None;
        }
        let usage = unsafe { usage.assume_init() };
        Some(
            usage.ru_utime.tv_sec as f64
                + usage.ru_utime.tv_usec as f64 / 1e6
                + usage.ru_stime.tv_sec as f64
                + usage.ru_stime.tv_usec as f64 / 1e6,
        )
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn resident_memory() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let value = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = value.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: sysconf has no pointer arguments or process mutations.
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if size <= 0 {
            return None;
        }
        pages.checked_mul(size as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
