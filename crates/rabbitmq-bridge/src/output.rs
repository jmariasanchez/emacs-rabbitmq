//! Output side of the bridge: a single writer task owns stdout.
//!
//! * Responses go through an unbounded queue. They are naturally bounded by
//!   the maximum number of outstanding requests, and must never be dropped.
//! * Notifications go through a bounded queue. When it is full (Emacs is not
//!   reading fast enough) notifications are dropped and counted; the writer
//!   periodically emits a `$/event` warning with the drop count so the loss is
//!   visible. Adapters keep their own bounded buffers, so dropped delivery
//!   notifications can be recovered with `consumer.drain`.

use rabbitmq_core::events::EventSink;
use rabbitmq_protocol::{write_frame, Notification, Response};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

pub const NOTIFY_QUEUE: usize = 4096;

#[derive(Clone)]
pub struct Outbox {
    resp: mpsc::UnboundedSender<Response>,
    notes: mpsc::Sender<Notification>,
    dropped: Arc<AtomicU64>,
}

impl Outbox {
    pub fn respond(&self, r: Response) {
        let _ = self.resp.send(r);
    }

    pub fn notify(&self, method: &str, params: Value) {
        if self.notes.try_send(Notification::new(method, params)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl EventSink for Outbox {
    fn emit(&self, method: &str, params: Value) {
        self.notify(method, params);
    }
}

/// Spawn the writer. Returns the outbox and the writer's join handle; the
/// writer exits once every `Outbox` clone is dropped and queues are drained.
pub fn spawn_writer<W: AsyncWrite + Unpin + Send + 'static>(mut w: W) -> (Outbox, tokio::task::JoinHandle<()>) {
    let (rtx, mut rrx) = mpsc::unbounded_channel::<Response>();
    let (ntx, mut nrx) = mpsc::channel::<Notification>(NOTIFY_QUEUE);
    let dropped = Arc::new(AtomicU64::new(0));
    let outbox = Outbox { resp: rtx, notes: ntx, dropped: dropped.clone() };
    let handle = tokio::spawn(async move {
        let mut reported = 0u64;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
        let (mut resp_open, mut notes_open) = (true, true);
        while resp_open || notes_open {
            tokio::select! {
                biased;
                r = rrx.recv(), if resp_open => match r {
                    Some(r) => { if write_json(&mut w, &r).await.is_err() { return; } }
                    None => resp_open = false,
                },
                n = nrx.recv(), if notes_open => match n {
                    Some(n) => { if write_json(&mut w, &n).await.is_err() { return; } }
                    None => notes_open = false,
                },
                _ = tick.tick() => {
                    let d = dropped.load(Ordering::Relaxed);
                    if d > reported {
                        let n = Notification::new("$/event", json!({
                            "level": "warning", "source": "bridge",
                            "message": format!("{} notification(s) dropped because the client was not reading fast enough; use consumer.drain to recover buffered deliveries", d - reported),
                            "dropped_total": d, "at": rabbitmq_core::message::now_millis()
                        }));
                        reported = d;
                        if write_json(&mut w, &n).await.is_err() { return; }
                    }
                }
            }
        }
    });
    (outbox, handle)
}

async fn write_json<W: AsyncWrite + Unpin, T: serde::Serialize>(w: &mut W, v: &T) -> std::io::Result<()> {
    let body = serde_json::to_vec(v).map_err(std::io::Error::other)?;
    write_frame(w, &body).await
}
