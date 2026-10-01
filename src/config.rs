//! Strict, reusable TOML input types. Parsing errors never contain input values.
use serde::Deserialize;
use std::net::{Ipv6Addr, SocketAddr};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub oxia: Oxia,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Oxia {
    pub endpoint: String,
    pub namespace: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub listen: SocketAddr,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observability {
    pub metrics: Option<Metrics>,
    pub log: Option<Log>,
    pub pprof: Option<Pprof>,
    pub tokio_console: Option<TokioConsole>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metrics {
    pub prometheus: Option<Prometheus>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prometheus {
    pub enabled: bool,
    pub listen: Option<SocketAddr>,
    pub path: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pprof {
    pub enabled: bool,
    pub frequency_hz: Option<u32>,
    pub default_duration_seconds: Option<u64>,
    pub max_duration_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokioConsole {
    pub enabled: bool,
    pub publish_interval_ms: Option<u64>,
    pub retention_seconds: Option<u64>,
    pub event_buffer_capacity: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Log {
    pub level: LogLevel,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub prometheus: PrometheusSettings,
    pub log_level: LogLevel,
    pub pprof: PprofSettings,
    pub tokio_console: ConsoleSettings,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrometheusSettings {
    pub enabled: bool,
    pub listen: SocketAddr,
    pub path: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PprofSettings {
    pub enabled: bool,
    pub frequency_hz: u32,
    pub default_duration_seconds: u64,
    pub max_duration_seconds: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsoleSettings {
    pub enabled: bool,
    pub publish_interval_ms: u64,
    pub retention_seconds: u64,
    pub event_buffer_capacity: usize,
}

#[derive(Debug, Error)]
#[error("invalid manifest: {0}")]
pub struct ConfigError(pub &'static str);

impl Observability {
    pub fn normalize(&self) -> Result<Settings, ConfigError> {
        let prom = self.metrics.as_ref().and_then(|m| m.prometheus.as_ref());
        let pprof = self.pprof.as_ref();
        let console = self.tokio_console.as_ref();
        let settings = Settings {
            prometheus: PrometheusSettings {
                enabled: prom.is_none_or(|p| p.enabled),
                listen: prom
                    .and_then(|p| p.listen)
                    .unwrap_or(SocketAddr::from(([127, 0, 0, 1], 9090))),
                path: prom
                    .and_then(|p| p.path.clone())
                    .unwrap_or_else(|| "/metrics".into()),
            },
            log_level: self.log.as_ref().map_or(LogLevel::Info, |l| l.level),
            pprof: PprofSettings {
                enabled: pprof.is_none_or(|p| p.enabled),
                frequency_hz: pprof.and_then(|p| p.frequency_hz).unwrap_or(99),
                default_duration_seconds: pprof
                    .and_then(|p| p.default_duration_seconds)
                    .unwrap_or(30),
                max_duration_seconds: pprof.and_then(|p| p.max_duration_seconds).unwrap_or(60),
            },
            tokio_console: ConsoleSettings {
                enabled: console.is_some_and(|c| c.enabled),
                publish_interval_ms: console.and_then(|c| c.publish_interval_ms).unwrap_or(1000),
                retention_seconds: console.and_then(|c| c.retention_seconds).unwrap_or(60),
                event_buffer_capacity: console
                    .and_then(|c| c.event_buffer_capacity)
                    .unwrap_or(10240),
            },
        };
        settings.validate()?;
        Ok(settings)
    }
}

impl Settings {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let path = &self.prometheus.path;
        if path.len() > 128
            || !path.starts_with('/')
            || path[1..].split('/').any(|s| {
                s.is_empty()
                    || !s
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
            })
        {
            return Err(ConfigError("metrics_path"));
        }
        let p = &self.pprof;
        if !(1..=1000).contains(&p.frequency_hz)
            || !(1..=60).contains(&p.max_duration_seconds)
            || !(1..=p.max_duration_seconds).contains(&p.default_duration_seconds)
        {
            return Err(ConfigError("pprof_bounds"));
        }
        let c = &self.tokio_console;
        if !(100..=60000).contains(&c.publish_interval_ms)
            || !(1..=3600).contains(&c.retention_seconds)
            || !(1024..=65536).contains(&c.event_buffer_capacity)
        {
            return Err(ConfigError("console_bounds"));
        }
        if self.prometheus.listen.port() == 0 {
            return Err(ConfigError("zero_port"));
        }
        Ok(())
    }
}

impl Metadata {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let namespace = &self.oxia.namespace;
        if namespace.is_empty()
            || namespace.len() > 128
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err(ConfigError("namespace"));
        }
        let endpoint = &self.oxia.endpoint;
        let Some((host, port)) = endpoint.rsplit_once(':') else {
            return Err(ConfigError("endpoint"));
        };
        let valid_host = if host.starts_with('[') && host.ends_with(']') {
            host[1..host.len() - 1].parse::<Ipv6Addr>().is_ok()
        } else {
            !host.is_empty()
                && host.len() <= 253
                && host.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
                })
        };
        if !valid_host
            || port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || !port.parse::<u16>().is_ok_and(|p| p > 0)
        {
            return Err(ConfigError("endpoint"));
        }
        Ok(())
    }
}

/// Conservative wildcard checking also treats IPv6 unspecified as dual-stack.
pub fn validate_listeners(listeners: &[SocketAddr]) -> Result<(), ConfigError> {
    for (i, address) in listeners.iter().enumerate() {
        if address.port() == 0 {
            return Err(ConfigError("zero_port"));
        }
        for other in &listeners[..i] {
            if address.port() == other.port()
                && (address.ip() == other.ip()
                    || address.ip().is_unspecified()
                    || other.ip().is_unspecified())
            {
                return Err(ConfigError("overlapping_listeners"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_defaults_and_validation() {
        let value: Observability = toml::from_str("").unwrap();
        assert_eq!(value.normalize().unwrap().pprof.frequency_hz, 99);
        for bad in [
            "[metrics.prometheus]\npath='/x'",
            "[pprof]\nenabled=false\nfrequency_hz=0",
            "[tokio_console]\nenabled=false\nevent_buffer_capacity=1",
            "[log]\nlevel='INFO'",
            "[pprof]\nenabled=true\nunknown=1",
        ] {
            assert!(
                toml::from_str::<Observability>(bad)
                    .map(|v| v.normalize())
                    .map_or(true, |v| v.is_err())
            );
        }
        assert!(
            validate_listeners(&["[::]:80".parse().unwrap(), "127.0.0.1:80".parse().unwrap()])
                .is_err()
        );
    }
}
