#![cfg(all(
    feature = "observability",
    feature = "tokio-console",
    feature = "pprof"
))]
use console_api::instrument::{InstrumentRequest, instrument_client::InstrumentClient};
use lyra_meta::config::Observability;
use lyra_meta::metadata::{MemoryMetadata, Metadata};
use lyra_meta::observability::{ProfileError, Telemetry};
use lyra_meta::utils::verifier::make_verifier;
use std::net::{SocketAddr, TcpListener as Socket};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::time::{sleep, timeout};

fn address() -> SocketAddr {
    Socket::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}
async fn get(address: SocketAddr, path: &str) -> String {
    timeout(Duration::from_secs(3), async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exporter_and_console_can_reload_without_replacing_instruments() {
    let mut settings = Observability::default().normalize().unwrap();
    settings.prometheus.listen = address();
    settings.pprof.enabled = false;
    settings.tokio_console.publish_interval_ms = 100;
    let console_address = address();
    let telemetry = Arc::new(
        Telemetry::new(
            settings.clone(),
            true,
            Some(console_address),
            &Handle::current(),
        )
        .unwrap(),
    );
    let meter = telemetry.meter();
    let meta = MemoryMetadata::with_meter(meter.clone());
    meta.initialize(make_verifier("test-only").unwrap())
        .await
        .unwrap();
    meta.register_catalog_component().await.unwrap();
    meter.u64_gauge("lyra_catalog_ready").build().record(1, &[]);
    meter
        .u64_gauge("lyra_catalog_manifest_generation")
        .build()
        .record(1, &[]);
    meter
        .u64_counter("lyra_catalog_manifest_reloads_total")
        .build()
        .add(1, &[]);
    telemetry
        .commit(telemetry.prepare(settings.clone()).await.unwrap())
        .await;
    let body = get(settings.prometheus.listen, "/metrics").await;
    assert!(body.starts_with("HTTP/1.1 200"));
    for metric in [
        "lyra_meta_operations_total",
        "lyra_meta_operation_duration_seconds",
        "lyra_component_registration_operations_total",
        "lyra_component_registered",
        "lyra_catalog_ready",
        "lyra_catalog_log_dropped_total",
        "lyra_process_cpu_seconds_total",
        "lyra_tokio_workers",
        "lyra_tokio_alive_tasks",
        "lyra_tokio_global_queue_depth",
        "lyra_tokio_worker_busy_seconds_total",
        "lyra_catalog_manifest_reloads_total",
        "lyra_catalog_manifest_generation",
    ] {
        assert!(
            body.contains(&format!("# TYPE {metric} ")),
            "missing {metric}: {body}"
        );
    }
    #[cfg(target_os = "linux")]
    assert!(body.contains("# TYPE lyra_process_resident_memory_bytes gauge"));
    assert!(!body.contains("_total_total"));
    assert!(!body.contains("registration_uuid="));
    settings.prometheus.path = "/changed".into();
    telemetry
        .commit(telemetry.prepare(settings.clone()).await.unwrap())
        .await;
    assert!(
        get(settings.prometheus.listen, "/metrics")
            .await
            .starts_with("HTTP/1.1 404")
    );
    assert!(
        get(settings.prometheus.listen, "/changed")
            .await
            .contains("lyra_meta_operations_total")
    );
    settings.prometheus.enabled = false;
    telemetry
        .commit(telemetry.prepare(settings.clone()).await.unwrap())
        .await;
    assert!(
        TcpStream::connect(settings.prometheus.listen)
            .await
            .is_err()
    );
    let occupied = Socket::bind(settings.prometheus.listen).unwrap();
    settings.prometheus.enabled = true;
    assert!(telemetry.prepare(settings.clone()).await.is_err());
    drop(occupied);
    telemetry
        .commit(telemetry.prepare(settings.clone()).await.unwrap())
        .await;
    assert!(
        get(settings.prometheus.listen, "/changed")
            .await
            .contains("lyra_catalog_manifest_generation")
    );

    let long_lived = tokio::spawn(async {
        loop {
            sleep(Duration::from_millis(10)).await;
        }
    });
    for retention in [60, 2] {
        settings.tokio_console.enabled = true;
        settings.tokio_console.retention_seconds = retention;
        telemetry
            .commit(telemetry.prepare(settings.clone()).await.unwrap())
            .await;
        let mut client = InstrumentClient::connect(format!("http://{console_address}"))
            .await
            .unwrap();
        let mut stream = client
            .watch_updates(InstrumentRequest {})
            .await
            .unwrap()
            .into_inner();
        timeout(Duration::from_secs(3), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    settings.tokio_console.enabled = false;
    telemetry
        .commit(telemetry.prepare(settings.clone()).await.unwrap())
        .await;
    assert!(TcpStream::connect(console_address).await.is_err());
    long_lived.abort();
    assert_eq!(
        telemetry.profiler().capture(None).await,
        Err(ProfileError::Disabled)
    );
    meta.close().await.unwrap();
    telemetry.close().await;
}
