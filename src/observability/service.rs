#[cfg(feature = "tokio-console")]
use super::console::{ConsoleSession, PreparedConsole};
use super::logging::Logging;
use super::metrics::MetricsServer;
use super::profile::Profiler;
use super::runtime::Instruments;
use crate::config::Settings;
use crate::toolkit::ReloadError;
use opentelemetry::{
    global,
    metrics::{Meter, MeterProvider},
};
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use prometheus::Registry;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, OwnedMutexGuard};

pub struct Telemetry {
    // Immutable state
    provider: SdkMeterProvider,
    registry: Registry,
    logging: Logging,
    _runtime: Instruments,
    health_enabled: bool,
    console_address: Option<SocketAddr>,
    profiler: Arc<Profiler>,
    // Mutable state
    running: Arc<Mutex<Running>>,
}
struct Running {
    settings: Settings,
    metrics: Option<MetricsServer>,
    #[cfg(feature = "tokio-console")]
    console: Option<ConsoleSession>,
}
pub struct Prepared {
    guard: OwnedMutexGuard<Running>,
    settings: Settings,
    metrics: Option<TcpListener>,
    #[cfg(feature = "tokio-console")]
    console: Option<PreparedConsole>,
}

impl Telemetry {
    /// Explicit executable-level setup, never invoked by metadata constructors.
    /// Listener admission is separate so init can emit logs without serving HTTP.
    pub fn new(
        settings: Settings,
        health_enabled: bool,
        console_address: Option<SocketAddr>,
        application_runtime: &Handle,
    ) -> Result<Self, ReloadError> {
        Self::validate(&settings, health_enabled, console_address)?;
        let (provider, registry) = provider()?;
        let meter = provider.meter("lyra-catalog");
        let logging = Logging::new(&meter, settings.log_level)?;
        let runtime = Instruments::new(&meter, application_runtime);
        let profiler = Arc::new(Profiler::new(settings.pprof.clone(), health_enabled));
        global::set_meter_provider(provider.clone());
        Ok(Self {
            provider,
            registry,
            logging,
            _runtime: runtime,
            health_enabled,
            console_address,
            profiler,
            running: Arc::new(Mutex::new(Running {
                settings,
                metrics: None,
                #[cfg(feature = "tokio-console")]
                console: None,
            })),
        })
    }
    pub fn meter(&self) -> Meter {
        self.provider.meter("lyra-catalog")
    }
    pub fn profiler(&self) -> Arc<Profiler> {
        Arc::clone(&self.profiler)
    }

    pub fn validate(
        settings: &Settings,
        health: bool,
        console: Option<SocketAddr>,
    ) -> Result<(), ReloadError> {
        settings.validate().map_err(|e| ReloadError(e.0))?;
        if settings.pprof.enabled && health && !cfg!(feature = "pprof") {
            return Err(ReloadError("pprof_unavailable"));
        }
        if settings.tokio_console.enabled
            && (!cfg!(all(feature = "tokio-console", tokio_unstable)) || console.is_none())
        {
            return Err(ReloadError("tokio_console_unavailable"));
        }
        Ok(())
    }

    pub async fn prepare(&self, settings: Settings) -> Result<Prepared, ReloadError> {
        Self::validate(&settings, self.health_enabled, self.console_address)?;
        let guard = self.running.clone().lock_owned().await;
        if guard.settings.prometheus.listen != settings.prometheus.listen {
            return Err(ReloadError("restart_required"));
        }
        let metrics = if settings.prometheus.enabled && guard.metrics.is_none() {
            Some(
                TcpListener::bind(settings.prometheus.listen)
                    .await
                    .map_err(|_| ReloadError("metrics_bind"))?,
            )
        } else {
            None
        };
        #[cfg(feature = "tokio-console")]
        let console = if settings.tokio_console.enabled
            && (guard.console.is_none() || guard.settings.tokio_console != settings.tokio_console)
        {
            Some(
                PreparedConsole::new(
                    self.console_address
                        .ok_or(ReloadError("console_listener_required"))?,
                    &settings.tokio_console,
                    guard.console.as_ref(),
                )
                .await?,
            )
        } else {
            None
        };
        Ok(Prepared {
            guard,
            settings,
            metrics,
            #[cfg(feature = "tokio-console")]
            console,
        })
    }

    pub async fn commit(&self, mut prepared: Prepared) {
        let settings = &prepared.settings;
        self.logging.update(settings.log_level);
        self.profiler
            .update(settings.pprof.clone(), self.health_enabled);
        if !settings.prometheus.enabled {
            if let Some(mut server) = prepared.guard.metrics.take() {
                server.close().await;
            }
        } else if let Some(listener) = prepared.metrics.take() {
            prepared.guard.metrics = Some(MetricsServer::new(
                listener,
                self.registry.clone(),
                settings.prometheus.path.clone(),
            ));
        } else if let Some(server) = &prepared.guard.metrics {
            server.update_path(settings.prometheus.path.clone());
        }
        #[cfg(feature = "tokio-console")]
        if !settings.tokio_console.enabled || prepared.console.is_some() {
            // Detach collection before dropping old session resources. The socket
            // has already been cloned for replacement; no conflicting second bind.
            self.logging
                .console
                .reload(None)
                .expect("global subscriber remains installed");
            if let Some(mut console) = prepared.guard.console.take() {
                console.close().await;
            }
            if let Some(console) = prepared.console.take() {
                prepared.guard.console = Some(console.commit(&self.logging.console));
            }
        }
        prepared.guard.settings = prepared.settings;
    }

    pub async fn close(&self) {
        self.profiler.close();
        let mut state = self.running.lock().await;
        if let Some(mut server) = state.metrics.take() {
            server.close().await;
        }
        #[cfg(feature = "tokio-console")]
        {
            let _ = self.logging.console.reload(None);
            if let Some(mut console) = state.console.take() {
                console.close().await;
            }
        }
        drop(state);
        let provider = self.provider.clone();
        let _ = tokio::task::spawn_blocking(move || {
            provider.shutdown_with_timeout(Duration::from_secs(3))
        })
        .await;
    }
}

pub(super) fn provider() -> Result<(SdkMeterProvider, Registry), ReloadError> {
    let registry = Registry::new();
    let reader = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()
        .map_err(|_| ReloadError("metrics_provider"))?;
    let provider = SdkMeterProvider::builder()
        .with_resource(
            Resource::builder_empty()
                .with_service_name("lyra-catalog")
                .build(),
        )
        .with_reader(reader)
        .build();
    Ok((provider, registry))
}
