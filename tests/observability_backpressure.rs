#![cfg(feature = "observability")]

use lyra_meta::config::Observability;
use lyra_meta::metadata::{MemoryMetadata, Metadata};
use lyra_meta::observability::Telemetry;
use lyra_meta::utils::verifier::make_verifier;
use std::io::Read;
use std::net::TcpListener as Socket;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Handle};
use tokio::time::timeout;

// A subprocess is essential: an intentionally unread stdout pipe must never
// wedge the test runner or hide shutdown failures behind its output capture.
#[test]
fn stalled_stdout_and_scrapers_do_not_block_metadata() {
    const CHILD: &str = "LYRA_TEST_STALLED_STDOUT";
    if std::env::var_os(CHILD).is_some() {
        Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(check_backpressure());
        // The harness would print its result into the deliberately full pipe.
        std::process::exit(0);
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "stalled_stdout_and_scrapers_do_not_block_metadata",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut diagnostics = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut diagnostics)
        .unwrap();
    assert!(
        status.is_some_and(|status| status.success()),
        "backpressure subprocess failed: {diagnostics}"
    );
}

async fn check_backpressure() {
    let mut settings = Observability::default().normalize().unwrap();
    settings.prometheus.listen = Socket::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    settings.pprof.enabled = false;
    let telemetry = Telemetry::new(settings.clone(), false, None, &Handle::current()).unwrap();
    telemetry
        .commit(telemetry.prepare(settings.clone()).await.unwrap())
        .await;

    let mut stalled = Vec::new();
    for _ in 0..16 {
        let mut socket = TcpStream::connect(settings.prometheus.listen)
            .await
            .unwrap();
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        stalled.push(socket);
    }
    let payload = "x".repeat(8192);
    for _ in 0..10_000 {
        tracing::info!(target: "lyra_meta", event = "backpressure_test", payload, "test-only log flood");
    }
    let meta = MemoryMetadata::with_meter(telemetry.meter());
    timeout(Duration::from_secs(3), async {
        meta.initialize(make_verifier("test-only").unwrap())
            .await
            .unwrap();
        meta.register_catalog_component().await.unwrap();
        assert!(meta.is_registered().await.unwrap());
        meta.close().await.unwrap();
    })
    .await
    .unwrap();

    let text = timeout(Duration::from_secs(3), async {
        let mut socket = TcpStream::connect(settings.prometheus.listen)
            .await
            .unwrap();
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap();
    assert!(text.starts_with("HTTP/1.1 200"));
    let dropped = text
        .lines()
        .find(|line| line.starts_with("lyra_catalog_log_dropped_total{"))
        .unwrap();
    assert!(
        dropped
            .split_whitespace()
            .last()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    timeout(Duration::from_secs(4), telemetry.close())
        .await
        .unwrap();
    for mut socket in stalled {
        let mut byte = [0];
        let read = timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(read, Ok(0) | Err(_)), "stalled scrape stayed open");
    }
    assert!(
        TcpStream::connect(settings.prometheus.listen)
            .await
            .is_err()
    );
}
