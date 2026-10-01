//! Bounded operational logging, including when stdout never becomes writable.
use std::fs::File;
use std::io::{self, Write};
#[cfg(unix)]
use std::os::fd::AsFd;
#[cfg(windows)]
use std::os::windows::io::AsHandle;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;

struct Queue {
    // Control state
    stopped: AtomicBool,
    // Immutable state
    sender: SyncSender<Vec<u8>>,
    // Mutable state
    dropped: AtomicU64,
}

#[derive(Clone)]
pub(super) struct BoundedWriter(Arc<Queue>);

pub(super) struct StdoutGuard {
    // Control state
    context: Arc<Queue>,
    thread: Option<JoinHandle<()>>,
    // Immutable state
    done: Mutex<Receiver<()>>,
}

pub(super) struct Record {
    // Immutable state
    queue: Arc<Queue>,
    // Mutable state
    bytes: Vec<u8>,
}

impl BoundedWriter {
    pub(super) fn new() -> io::Result<(Self, StdoutGuard)> {
        // Do not hold Rust's global Stdout lock in a potentially blocked worker:
        // runtime exit also takes that lock to flush the standard line writer.
        #[cfg(unix)]
        let mut sink = File::from(io::stdout().as_fd().try_clone_to_owned()?);
        #[cfg(windows)]
        let mut sink = File::from(io::stdout().as_handle().try_clone_to_owned()?);
        let (sender, receiver) = sync_channel::<Vec<u8>>(1024);
        let (completed, done) = sync_channel(1);
        let queue = Arc::new(Queue {
            stopped: AtomicBool::new(false),
            sender,
            dropped: AtomicU64::new(0),
        });
        let context = Arc::clone(&queue);
        let thread = Builder::new().name("lyra-stdout".into()).spawn(move || {
            while let Ok(bytes) = receiver.recv() {
                if sink.write_all(&bytes).is_err() {
                    context.record_drop();
                }
                if context.stopped.load(Ordering::Acquire) {
                    for bytes in receiver.try_iter() {
                        if sink.write_all(&bytes).is_err() {
                            context.record_drop();
                        }
                    }
                    break;
                }
            }
            let _ = completed.send(());
        })?;
        Ok((
            Self(Arc::clone(&queue)),
            StdoutGuard {
                context: queue,
                thread: Some(thread),
                done: Mutex::new(done),
            },
        ))
    }

    pub(super) fn dropped(&self) -> u64 {
        self.0.dropped.load(Ordering::Relaxed)
    }
}

impl Queue {
    fn record_drop(&self) {
        let _ = self
            .dropped
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            });
    }
}

impl<'a> MakeWriter<'a> for BoundedWriter {
    type Writer = Record;
    fn make_writer(&'a self) -> Record {
        Record {
            queue: Arc::clone(&self.0),
            bytes: Vec::new(),
        }
    }
}

impl Write for Record {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        if self.bytes.is_empty() {
            return;
        }
        if self.queue.stopped.load(Ordering::Acquire)
            || self
                .queue
                .sender
                .try_send(std::mem::take(&mut self.bytes))
                .is_err()
        {
            self.queue.record_drop();
        }
    }
}

impl Drop for StdoutGuard {
    fn drop(&mut self) {
        self.context.stopped.store(true, Ordering::Release);
        // Wake an idle worker, but never wait for queue capacity or print a
        // fallback warning to the same blocked destination during shutdown.
        let _ = self.context.sender.try_send(Vec::new());
        if self
            .done
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .recv_timeout(Duration::from_secs(1))
            .is_ok()
            && let Some(thread) = self.thread.take()
        {
            let _ = thread.join();
        }
        // A worker blocked in the OS write is detached. Process exit terminates
        // it; shutdown does not promise delivery of remaining queued records.
    }
}
