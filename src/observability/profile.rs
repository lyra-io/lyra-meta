use crate::config::PprofSettings;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileError {
    Disabled,
    InvalidQuery,
    Busy,
    Failed,
    Cancelled,
}

pub struct Profiler {
    // Immutable state
    permit: Arc<Semaphore>,
    // Mutable state
    state: Mutex<State>,
}
struct State {
    config: PprofSettings,
    active: Option<CancellationToken>,
    last_finish: Option<Instant>,
}

impl Profiler {
    pub(super) fn new(mut config: PprofSettings, health: bool) -> Self {
        config.enabled &= health && cfg!(feature = "pprof");
        Self {
            permit: Arc::new(Semaphore::new(1)),
            state: Mutex::new(State {
                config,
                active: None,
                last_finish: None,
            }),
        }
    }
    pub(super) fn update(&self, mut config: PprofSettings, health: bool) {
        config.enabled &= health && cfg!(feature = "pprof");
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !config.enabled
            && let Some(active) = &state.active
        {
            active.cancel();
        }
        if state.config.enabled != config.enabled {
            tracing::info!(
                event = "pprof_availability_changed",
                enabled = config.enabled,
                "CPU profiling availability changed"
            );
        }
        state.config = config;
    }
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.config.enabled = false;
        if let Some(active) = &state.active {
            active.cancel();
        }
    }

    pub async fn capture(self: &Arc<Self>, query: Option<&str>) -> Result<Vec<u8>, ProfileError> {
        let (config, seconds, context, permit) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.config.enabled {
                return Err(ProfileError::Disabled);
            }
            let seconds = parse_query(query, &state.config)?;
            if state
                .last_finish
                .is_some_and(|at| at.elapsed() < Duration::from_secs(5))
            {
                return Err(ProfileError::Busy);
            }
            let permit = Arc::clone(&self.permit)
                .try_acquire_owned()
                .map_err(|_| ProfileError::Busy)?;
            let context = CancellationToken::new();
            state.active = Some(context.clone());
            (state.config.clone(), seconds, context, permit)
        };
        let cancel_on_disconnect = CancelOnDrop(context.clone());
        let owner = Arc::clone(self);
        let result = tokio::task::spawn_blocking(move || {
            let _capture = Capture {
                owner,
                _permit: permit,
            };
            tracing::info!(event = "cpu_capture_started", "CPU profiling started");
            let result = capture0(config, seconds, &context);
            match &result {
                Ok(_) => tracing::info!(event = "cpu_capture_completed", "CPU profile complete"),
                Err(ProfileError::Cancelled) => {
                    tracing::info!(event = "cpu_capture_cancelled", "CPU capture cancelled")
                }
                Err(_) => tracing::warn!(
                    event = "cpu_capture_failed",
                    reason = "capture_or_report",
                    "CPU capture failed"
                ),
            }
            result
        })
        .await
        .map_err(|_| ProfileError::Failed)?;
        drop(cancel_on_disconnect);
        result
    }
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct Capture {
    owner: Arc<Profiler>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Capture {
    fn drop(&mut self) {
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.last_finish = Some(Instant::now());
        state.active = None;
    }
}

fn parse_query(query: Option<&str>, config: &PprofSettings) -> Result<u64, ProfileError> {
    let Some(query) = query.filter(|q| !q.is_empty()) else {
        return Ok(config.default_duration_seconds);
    };
    let Some(value) = query.strip_prefix("seconds=") else {
        return Err(ProfileError::InvalidQuery);
    };
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ProfileError::InvalidQuery);
    }
    let seconds = value
        .parse::<u64>()
        .map_err(|_| ProfileError::InvalidQuery)?;
    if !(1..=config.max_duration_seconds).contains(&seconds) {
        return Err(ProfileError::InvalidQuery);
    }
    Ok(seconds)
}

#[cfg(feature = "pprof")]
fn capture0(
    config: PprofSettings,
    seconds: u64,
    context: &CancellationToken,
) -> Result<Vec<u8>, ProfileError> {
    use flate2::{Compression, write::GzEncoder};
    use pprof_rs::{ProfilerGuardBuilder, protos::Message};
    use std::io::Write;
    const MAX_OUTPUT: usize = 16 * 1024 * 1024;
    if context.is_cancelled() {
        return Err(ProfileError::Cancelled);
    }
    let sampler = ProfilerGuardBuilder::default()
        .frequency(config.frequency_hz as i32)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|_| ProfileError::Failed)?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if context.is_cancelled() {
            return Err(ProfileError::Cancelled);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    if context.is_cancelled() {
        return Err(ProfileError::Cancelled);
    }
    // Vendored pprof latches collector failures and bounds spill storage.
    let report = sampler.report().build().map_err(|_| ProfileError::Failed)?;
    drop(sampler);
    if context.is_cancelled() {
        return Err(ProfileError::Cancelled);
    }
    let profile = report.pprof().map_err(|_| ProfileError::Failed)?;
    if profile.encoded_len() > MAX_OUTPUT {
        return Err(ProfileError::Failed);
    }
    let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
    gzip.write_all(&profile.encode_to_vec())
        .map_err(|_| ProfileError::Failed)?;
    let bytes = gzip.finish().map_err(|_| ProfileError::Failed)?;
    if bytes.len() > MAX_OUTPUT {
        return Err(ProfileError::Failed);
    }
    if context.is_cancelled() {
        return Err(ProfileError::Cancelled);
    }
    Ok(bytes)
}
#[cfg(not(feature = "pprof"))]
fn capture0(_: PprofSettings, _: u64, _: &CancellationToken) -> Result<Vec<u8>, ProfileError> {
    Err(ProfileError::Disabled)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(all(target_os = "linux", feature = "pprof"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn linux_capture_has_symbolized_cpu_samples_and_cancels() {
        use flate2::read::GzDecoder;
        use pprof_rs::protos::{Message, Profile};
        use std::io::Read;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        use tokio::time::{sleep, timeout};

        #[inline(never)]
        fn lyra_profile_test_cpu_work(stop: &AtomicBool) {
            let mut value = 1u64;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..10_000 {
                    value = std::hint::black_box(
                        value.wrapping_mul(6364136223846793005).wrapping_add(1),
                    );
                }
            }
            std::hint::black_box(value);
        }
        let profiler = Arc::new(Profiler::new(
            PprofSettings {
                enabled: true,
                frequency_hz: 99,
                default_duration_seconds: 30,
                max_duration_seconds: 60,
            },
            true,
        ));
        let worker_stop = Arc::new(AtomicBool::new(false));
        let worker_flag = Arc::clone(&worker_stop);
        let worker = thread::spawn(move || lyra_profile_test_cpu_work(&worker_flag));
        let bytes = profiler.capture(Some("seconds=1")).await.unwrap();
        worker_stop.store(true, Ordering::Relaxed);
        worker.join().unwrap();
        let mut protobuf = Vec::new();
        GzDecoder::new(bytes.as_slice())
            .read_to_end(&mut protobuf)
            .unwrap();
        let profile = Profile::decode(protobuf.as_slice()).unwrap();
        assert!(!profile.sample.is_empty());
        assert!(
            profile
                .string_table
                .iter()
                .any(|symbol| symbol.contains("lyra_profile_test_cpu_work"))
        );
        assert_eq!(
            profiler.capture(Some("seconds=1")).await,
            Err(ProfileError::Busy)
        );
        sleep(Duration::from_secs(5)).await;
        let capturing = Arc::clone(&profiler);
        let task = tokio::spawn(async move { capturing.capture(Some("seconds=30")).await });
        sleep(Duration::from_millis(100)).await;
        task.abort();
        let _ = task.await;
        timeout(Duration::from_secs(3), async {
            while profiler.permit.available_permits() == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(profiler.state.lock().unwrap().active.is_none());
    }

    #[test]
    fn strict_query_bounds() {
        let config = PprofSettings {
            enabled: true,
            frequency_hz: 99,
            default_duration_seconds: 30,
            max_duration_seconds: 60,
        };
        assert_eq!(parse_query(None, &config), Ok(30));
        assert_eq!(parse_query(Some("seconds=1"), &config), Ok(1));
        for query in [
            "seconds=0",
            "seconds=61",
            "seconds=1&seconds=2",
            "foo=1",
            "seconds=-1",
            "seconds=1.5",
            "seconds=%31",
            "seconds=",
        ] {
            assert_eq!(
                parse_query(Some(query), &config),
                Err(ProfileError::InvalidQuery)
            );
        }
    }
}
