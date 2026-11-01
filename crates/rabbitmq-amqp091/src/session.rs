//! AMQP 0-9-1 session: one broker connection plus the channels and
//! consumers opened on it.
//!
//! ## Delivery-tag ownership
//!
//! AMQP delivery tags are scoped to a channel. A tag acknowledged on the
//! wrong channel, or on a channel that was re-opened after a failure, can
//! acknowledge a *different* message or close the channel with
//! `PRECONDITION_FAILED`. To make that impossible:
//!
//! * Every inbound message receives a bridge-assigned id
//!   (`<session>/<epoch>/<channel>/<tag>`).
//! * The session keeps the lapin `Acker` for each unsettled delivery in a
//!   table keyed by that id. Settlement goes through the stored `Acker`,
//!   which is bound to the original channel.
//! * Each (re)connection increments the session `epoch`. When a connection
//!   or channel is lost, all unsettled deliveries from it are moved to a
//!   `stale` set and any later settlement attempt fails with
//!   `stale_delivery`. The broker will redeliver those messages.
//!
//! ## Publish outcome classification
//!
//! Publishing always enables publisher confirms on the publish channel unless
//! the caller explicitly selects a transaction. Outcomes are classified from
//! broker confirms only (see [`PublishStatus`]); if the connection drops
//! after the frame is written but before the confirm arrives, the outcome is
//! `uncertain`, and the bridge never retries it automatically.

use crate::convert::{from_basic_properties, json_to_table, to_basic_properties};
use crate::errors::{self, map};
use futures::StreamExt;
use lapin::message::{BasicReturnMessage, Delivery};
use lapin::options::*;
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind};
use rabbitmq_core::buffer::BoundedBuffer;
use rabbitmq_core::capabilities::QueueSpec;
use rabbitmq_core::error::{CoreError, ErrorCode, Result};
use rabbitmq_core::events::{self, names, SharedSink};
use rabbitmq_core::message::*;
use rabbitmq_core::profile::Profile;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Connecting,
    Connected,
    Blocked,
    Reconnecting,
    Closed,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub id: String,
    pub profile: String,
    pub uri: String,
    pub state: SessionState,
    pub epoch: u64,
    pub blocked_reason: Option<String>,
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
    pub server_properties: Value,
    pub consumers: Vec<ConsumerInfo>,
    pub unsettled: usize,
    pub stale_unsettled: usize,
    pub connected_at: Option<u64>,
    pub last_error: Option<String>,
    pub reconnects: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeOptions {
    pub queue: String,
    #[serde(default)]
    pub consumer_tag: Option<String>,
    /// Per-consumer prefetch. Defaults to 10 for interactive use.
    #[serde(default = "d_prefetch")]
    pub prefetch: u16,
    /// Automatic acknowledgement. Off by default: interactive consumption
    /// uses manual acknowledgement so nothing is lost by accident.
    #[serde(default)]
    pub auto_ack: bool,
    #[serde(default)]
    pub exclusive: bool,
    /// Consumer priority (`x-priority`).
    #[serde(default)]
    pub priority: Option<i32>,
    /// Extra consumer arguments (stream offset etc.).
    #[serde(default)]
    pub arguments: Map<String, Value>,
    /// Flush deliveries to the frontend every N ms or when `batch` is reached.
    #[serde(default = "d_flush")]
    pub flush_ms: u64,
    #[serde(default = "d_batch")]
    pub batch: usize,
}

fn d_prefetch() -> u16 {
    10
}
fn d_flush() -> u64 {
    100
}
fn d_batch() -> usize {
    50
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerInfo {
    pub tag: String,
    pub queue: String,
    pub channel: u16,
    pub prefetch: u16,
    pub auto_ack: bool,
    pub state: String,
    pub paused: bool,
    pub delivered: u64,
    pub buffered: usize,
    pub dropped: u64,
}

struct ConsumerHandle {
    info: ConsumerInfo,
    channel: Channel,
    task: tokio::task::JoinHandle<()>,
    buffer: Arc<Mutex<BoundedBuffer<InboundMessage>>>,
    paused: Arc<std::sync::atomic::AtomicBool>,
    cancelled_by_client: Arc<std::sync::atomic::AtomicBool>,
}

struct Unsettled {
    acker: lapin::Acker,
    epoch: u64,
    channel: u16,
    delivery_tag: u64,
    queue: String,
}

struct Inner {
    profile: Profile,
    id: String,
    conn: Option<Arc<Connection>>,
    /// Lazily opened confirm-mode publish channel.
    publish: Option<Channel>,
    /// Lazily opened channel for basic.get with manual ack.
    get: Option<Channel>,
    consumers: HashMap<String, ConsumerHandle>,
    state: SessionState,
    blocked_reason: Option<String>,
    connected_at: Option<u64>,
    last_error: Option<String>,
    reconnects: u64,
}

pub struct Session {
    inner: RwLock<Inner>,
    epoch: AtomicU64,
    unsettled: Mutex<HashMap<String, Unsettled>>,
    stale: Mutex<BoundedBuffer<String>>,
    seq: AtomicU64,
    sink: SharedSink,
    closing: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeDeclare {
    pub name: String,
    #[serde(rename = "type", default = "direct")]
    pub kind: String,
    #[serde(default = "yes")]
    pub durable: bool,
    #[serde(default)]
    pub auto_delete: bool,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub passive: bool,
    #[serde(default)]
    pub arguments: Map<String, Value>,
}

fn direct() -> String {
    "direct".into()
}
fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueDeclareOk {
    pub name: String,
    pub message_count: u32,
    pub consumer_count: u32,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetResult {
    pub messages: Vec<InboundMessage>,
    /// Remaining messages reported by the last basic.get-ok, if any.
    pub remaining: Option<u32>,
    pub empty: bool,
    /// Explicit reminder for the UI: this operation removed messages from
    /// the queue (or made them unacked).
    pub destructive: bool,
    pub note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SettleResult {
    pub id: String,
    pub settled: bool,
    pub outcome: Settlement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PublishMode {
    /// Publisher confirms (default).
    #[default]
    Confirm,
    /// AMQP transaction: all messages commit or roll back together.
    Transaction,
    /// Fire and forget; every outcome is reported as `unconfirmed`.
    NoConfirm,
}

impl Session {
    /// Connect using `profile`. Returns once the AMQP handshake completes.
    pub async fn connect(id: String, profile: Profile, sink: SharedSink) -> Result<Arc<Self>> {
        let s = Arc::new(Self {
            inner: RwLock::new(Inner {
                profile: profile.clone(),
                id: id.clone(),
                conn: None,
                publish: None,
                get: None,
                consumers: HashMap::new(),
                state: SessionState::Connecting,
                blocked_reason: None,
                connected_at: None,
                last_error: None,
                reconnects: 0,
            }),
            epoch: AtomicU64::new(0),
            unsettled: Mutex::new(HashMap::new()),
            stale: Mutex::new(BoundedBuffer::new(profile.limits.max_unsettled.max(100))),
            seq: AtomicU64::new(0),
            sink,
            closing: std::sync::atomic::AtomicBool::new(false),
        });
        let conn = Self::open_connection(&profile).await?;
        s.install_connection(conn).await;
        Ok(s)
    }

    async fn open_connection(p: &Profile) -> Result<Connection> {
        let props = ConnectionProperties::default()
            .with_connection_name(p.connection_name().into())
            .with_client_property("product".into(), "rabbitmq.el".into())
            .with_client_property("platform".into(), "Emacs / Rust (lapin)".into());
        let uri = p.amqp_uri();
        let fut = async {
            if p.tls.enabled {
                let tls = crate::tls::lapin_tls(p)?;
                Connection::connect_with_config(
                    &uri,
                    props,
                    tls,
                    lapin::runtime::default_runtime().map_err(|e| map(&e, "connect"))?,
                )
                .await
                .map_err(|e| map(&e, "connect"))
            } else {
                Connection::connect(&uri, props)
                    .await
                    .map_err(|e| map(&e, "connect"))
            }
        };
        tokio::time::timeout(Duration::from_millis(p.limits.timeout_ms), fut)
            .await
            .map_err(|_| {
                CoreError::new(
                    ErrorCode::Timeout,
                    format!("connect to {} timed out", p.display_uri()),
                )
                .retryable(true)
            })?
    }

    fn install_connection(
        self: &Arc<Self>,
        conn: Connection,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(self.install_connection_inner(conn))
    }

    async fn install_connection_inner(self: &Arc<Self>, conn: Connection) {
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let conn = Arc::new(conn);
        let events_stream = conn.events_listener();
        {
            let mut g = self.inner.write().await;
            g.conn = Some(conn.clone());
            g.publish = None;
            g.get = None;
            g.state = SessionState::Connected;
            g.blocked_reason = None;
            g.connected_at = Some(now_millis());
        }
        self.emit_state("connected", json!({ "epoch": epoch }));
        // Supervise connection events for this epoch.
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut events_stream = Box::pin(events_stream);
            while let Some(ev) = events_stream.next().await {
                let Some(s) = me.upgrade() else { break };
                if s.epoch.load(Ordering::SeqCst) != epoch {
                    break;
                }
                match ev {
                    lapin::Event::ConnectionBlocked(reason) => {
                        {
                            let mut g = s.inner.write().await;
                            g.state = SessionState::Blocked;
                            g.blocked_reason = Some(reason.clone());
                        }
                        s.emit_state("blocked", json!({ "reason": reason }));
                        events::log(&*s.sink, "warning", "amqp091", format!("broker blocked the connection: {reason} (publishing will stall; see memory/disk alarms)"));
                    }
                    lapin::Event::ConnectionUnblocked => {
                        {
                            let mut g = s.inner.write().await;
                            g.state = SessionState::Connected;
                            g.blocked_reason = None;
                        }
                        s.emit_state("unblocked", json!({}));
                    }
                    lapin::Event::Error(e) => {
                        // lapin also reports channel-level closes (404, 406, ...)
                        // here. Only a dead connection invalidates the epoch;
                        // channel errors are handled by the operation that
                        // caused them and by per-channel invalidation.
                        let conn_dead = {
                            let g = s.inner.read().await;
                            g.conn
                                .as_ref()
                                .map(|c| {
                                    let st = c.status();
                                    st.errored() || st.closed() || st.closing()
                                })
                                .unwrap_or(true)
                        };
                        let err = map(&e, "connection");
                        if !conn_dead {
                            events::log(
                                &*s.sink,
                                "warning",
                                "amqp091",
                                format!("channel error: {}", err.message),
                            );
                            continue;
                        }
                        s.on_connection_lost(epoch, err).await;
                        break;
                    }
                    _ => {}
                }
            }
        });
    }

    /// Handle loss of the connection for `epoch`: invalidate deliveries,
    /// mark consumers, and schedule reconnection if configured.
    async fn on_connection_lost(self: &Arc<Self>, epoch: u64, err: CoreError) {
        if self.epoch.load(Ordering::SeqCst) != epoch {
            return;
        }
        let closing = self.closing.load(Ordering::SeqCst);
        let stale = self.invalidate_epoch(epoch).await;
        let attempts = {
            let mut g = self.inner.write().await;
            g.last_error = Some(err.message.clone());
            g.conn = None;
            g.publish = None;
            g.get = None;
            for c in g.consumers.values_mut() {
                c.info.state = "lost".into();
                c.task.abort();
            }
            g.state = if closing {
                SessionState::Closed
            } else {
                SessionState::Reconnecting
            };
            g.profile.reconnect_attempts
        };
        if closing {
            return;
        }
        self.emit_state(
            "lost",
            json!({ "error": err, "epoch": epoch, "invalidated_deliveries": stale }),
        );
        events::log(
            &*self.sink,
            "error",
            "amqp091",
            format!("connection lost ({}); {stale} unsettled deliveries invalidated and will be redelivered by the broker", err.message),
        );
        if attempts == 0 {
            self.inner.write().await.state = SessionState::Failed;
            self.emit_state("failed", json!({ "error": err }));
            return;
        }
        let me = self.clone();
        tokio::spawn(async move { me.reconnect_loop(attempts).await });
    }

    async fn reconnect_loop(self: Arc<Self>, attempts: u32) {
        let profile = self.inner.read().await.profile.clone();
        let mut delay = Duration::from_millis(500);
        for attempt in 1..=attempts {
            if self.closing.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(delay).await;
            self.emit_state(
                "reconnecting",
                json!({ "attempt": attempt, "of": attempts }),
            );
            match Self::open_connection(&profile).await {
                Ok(conn) => {
                    self.inner.write().await.reconnects += 1;
                    self.install_connection(conn).await;
                    // Consumers are NOT restored automatically: resubscribing would
                    // silently resume destructive consumption. The frontend decides.
                    let lost: Vec<String> = self
                        .inner
                        .read()
                        .await
                        .consumers
                        .iter()
                        .filter(|(_, c)| c.info.state == "lost")
                        .map(|(k, _)| k.clone())
                        .collect();
                    if !lost.is_empty() {
                        events::log(
                            &*self.sink,
                            "info",
                            "amqp091",
                            format!(
                                "reconnected; {} consumer(s) need to be restarted explicitly",
                                lost.len()
                            ),
                        );
                    }
                    return;
                }
                Err(e) => {
                    self.inner.write().await.last_error = Some(e.message.clone());
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        self.inner.write().await.state = SessionState::Failed;
        self.emit_state("failed", json!({ "error": "reconnect attempts exhausted" }));
    }

    async fn invalidate_epoch(&self, epoch: u64) -> usize {
        let mut un = self.unsettled.lock().await;
        let ids: Vec<String> = un
            .iter()
            .filter(|(_, u)| u.epoch == epoch)
            .map(|(k, _)| k.clone())
            .collect();
        let mut stale = self.stale.lock().await;
        for id in &ids {
            un.remove(id);
            stale.push(id.clone());
        }
        ids.len()
    }

    async fn invalidate_channel(&self, epoch: u64, channel: u16) -> usize {
        let mut un = self.unsettled.lock().await;
        let ids: Vec<String> = un
            .iter()
            .filter(|(_, u)| u.epoch == epoch && u.channel == channel)
            .map(|(k, _)| k.clone())
            .collect();
        let mut stale = self.stale.lock().await;
        for id in &ids {
            un.remove(id);
            stale.push(id.clone());
        }
        ids.len()
    }

    fn emit_state(&self, state: &str, detail: Value) {
        let id = self
            .inner
            .try_read()
            .map(|g| g.id.clone())
            .unwrap_or_default();
        self.sink.emit(
            names::SESSION,
            json!({ "session": id, "protocol": "amqp091", "state": state, "detail": detail, "at": now_millis() }),
        );
    }

    async fn conn(&self) -> Result<Arc<Connection>> {
        let g = self.inner.read().await;
        match (&g.conn, g.state) {
            (Some(c), SessionState::Connected | SessionState::Blocked) => Ok(c.clone()),
            (_, SessionState::Reconnecting) => Err(CoreError::new(
                ErrorCode::ConnectionFailed,
                "session is reconnecting; retry when it is connected",
            )
            .retryable(true)),
            _ => Err(CoreError::new(
                ErrorCode::ConnectionFailed,
                "session is not connected",
            )),
        }
    }

    /// Channel slot selector.
    async fn channel(&self, slot: Slot) -> Result<Channel> {
        {
            let g = self.inner.read().await;
            let existing = match slot {
                Slot::Publish => &g.publish,
                Slot::Get => &g.get,
            };
            if let Some(ch) = existing {
                if ch.status().connected() {
                    return Ok(ch.clone());
                }
            }
        }
        let conn = self.conn().await?;
        let ch = conn
            .create_channel()
            .await
            .map_err(|e| map(&e, "channel.open"))?;
        if slot == Slot::Publish {
            // Returned (unroutable mandatory) messages arrive through the
            // PublisherConfirm of the publish that caused them
            // (Confirmation::Ack(Some(returned))), classified in `publish_confirm`.
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| map(&e, "confirm.select"))?;
        }
        let mut g = self.inner.write().await;
        let old = match slot {
            Slot::Publish => g.publish.replace(ch.clone()),
            Slot::Get => g.get.replace(ch.clone()),
        };
        drop(g);
        if let Some(old) = old {
            // A replaced get-channel takes its unsettled deliveries with it.
            let n = self
                .invalidate_channel(self.epoch.load(Ordering::SeqCst), old.id())
                .await;
            if n > 0 {
                events::log(
                    &*self.sink,
                    "warning",
                    "amqp091",
                    format!(
                        "channel {} closed; {n} unsettled deliveries invalidated",
                        old.id()
                    ),
                );
            }
        }
        Ok(ch)
    }

    /// Run an operation on a fresh, short-lived channel. Used for declares,
    /// deletes, and binds: a failure (404, 406, 403) closes the channel, and a
    /// scratch channel keeps that from disturbing publish/get/consumer channels.
    async fn with_scratch<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Channel) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let conn = self.conn().await?;
        let ch = conn
            .create_channel()
            .await
            .map_err(|e| map(&e, "channel.open"))?;
        let r = f(ch.clone()).await;
        if ch.status().connected() {
            let _ = ch.close(200, "OK".into()).await;
        }
        r
    }

    pub async fn info(&self) -> SessionInfo {
        let g = self.inner.read().await;
        let (channel_max, frame_max, heartbeat) = g
            .conn
            .as_ref()
            .map(|c| {
                let cfg = c.configuration();
                (cfg.channel_max(), cfg.frame_max(), cfg.heartbeat())
            })
            .unwrap_or((0, 0, 0));
        let mut consumers: Vec<ConsumerInfo> = Vec::new();
        for c in g.consumers.values() {
            let mut info = c.info.clone();
            if let Ok(b) = c.buffer.try_lock() {
                info.buffered = b.len();
                info.dropped = b.dropped();
            }
            info.paused = c.paused.load(Ordering::Relaxed);
            consumers.push(info);
        }
        SessionInfo {
            id: g.id.clone(),
            profile: g.profile.name.clone(),
            uri: g.profile.display_uri(),
            state: g.state,
            epoch: self.epoch.load(Ordering::SeqCst),
            blocked_reason: g.blocked_reason.clone(),
            channel_max,
            frame_max,
            heartbeat,
            server_properties: Value::Null,
            consumers,
            unsettled: self.unsettled.try_lock().map(|u| u.len()).unwrap_or(0),
            stale_unsettled: self.stale.try_lock().map(|s| s.len()).unwrap_or(0),
            connected_at: g.connected_at,
            last_error: g.last_error.clone(),
            reconnects: g.reconnects,
        }
    }

    pub async fn profile(&self) -> Profile {
        self.inner.read().await.profile.clone()
    }

    // ------------------------------------------------------------ topology

    pub async fn declare_exchange(&self, d: &ExchangeDeclare) -> Result<()> {
        let kind = match d.kind.as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "topic" => ExchangeKind::Topic,
            "headers" => ExchangeKind::Headers,
            other => ExchangeKind::Custom(other.to_string()),
        };
        let args = json_to_table(&d.arguments)?;
        let opts = ExchangeDeclareOptions {
            passive: d.passive,
            durable: d.durable,
            auto_delete: d.auto_delete,
            internal: d.internal,
            nowait: false,
        };
        let name = ss(&d.name)?;
        // Declares that conflict with an existing definition close the channel
        // with 406; use a scratch channel so that never disturbs other work.
        self.with_scratch(|ch| async move {
            ch.exchange_declare(name, kind, opts, args)
                .await
                .map_err(|e| map(&e, "exchange.declare"))
        })
        .await
    }

    pub async fn delete_exchange(&self, name: &str, if_unused: bool) -> Result<()> {
        let name = ss(name)?;
        self.with_scratch(|ch| async move {
            ch.exchange_delete(
                name,
                ExchangeDeleteOptions {
                    if_unused,
                    nowait: false,
                },
            )
            .await
            .map_err(|e| map(&e, "exchange.delete"))
        })
        .await
    }

    pub async fn declare_queue(&self, spec: QueueSpec, passive: bool) -> Result<QueueDeclareOk> {
        let warnings = spec.warnings();
        let spec = if passive { spec } else { spec.validate()? };
        let args = if passive {
            FieldTable::default()
        } else {
            json_to_table(&spec.arguments)?
        };
        let opts = QueueDeclareOptions {
            passive,
            durable: spec.durable,
            exclusive: spec.exclusive,
            auto_delete: spec.auto_delete,
            nowait: false,
        };
        let name = ss(&spec.name)?;
        // Exclusive queues belong to the declaring connection and die with
        // their channel's connection, not the channel, so a scratch channel is fine.
        let q = self
            .with_scratch(|ch| async move {
                ch.queue_declare(name, opts, args)
                    .await
                    .map_err(|e| map(&e, "queue.declare"))
            })
            .await?;
        Ok(QueueDeclareOk {
            name: q.name().as_str().to_string(),
            message_count: q.message_count(),
            consumer_count: q.consumer_count(),
            warnings,
        })
    }

    pub async fn delete_queue(&self, name: &str, if_unused: bool, if_empty: bool) -> Result<u32> {
        let name = ss(name)?;
        self.with_scratch(|ch| async move {
            ch.queue_delete(
                name,
                QueueDeleteOptions {
                    if_unused,
                    if_empty,
                    nowait: false,
                },
            )
            .await
            .map_err(|e| map(&e, "queue.delete"))
        })
        .await
    }

    pub async fn purge_queue(&self, name: &str) -> Result<u32> {
        let name = ss(name)?;
        self.with_scratch(|ch| async move {
            ch.queue_purge(name, QueuePurgeOptions { nowait: false })
                .await
                .map_err(|e| map(&e, "queue.purge"))
        })
        .await
    }

    pub async fn bind(
        &self,
        destination_type: &str,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: &Map<String, Value>,
        unbind: bool,
    ) -> Result<()> {
        let args = json_to_table(arguments)?;
        let (d, s, k) = (ss(destination)?, ss(source)?, ss(routing_key)?);
        let what = if unbind { "unbind" } else { "bind" };
        let is_queue = destination_type == "queue";
        self.with_scratch(|ch| async move {
            let r = match (is_queue, unbind) {
                (true, false) => {
                    ch.queue_bind(d, s, k, QueueBindOptions::default(), args)
                        .await
                }
                (true, true) => ch.queue_unbind(d, s, k, args).await,
                (false, false) => {
                    ch.exchange_bind(d, s, k, ExchangeBindOptions::default(), args)
                        .await
                }
                (false, true) => {
                    ch.exchange_unbind(d, s, k, ExchangeUnbindOptions::default(), args)
                        .await
                }
            };
            r.map_err(|e| map(&e, what))
        })
        .await
    }

    // ------------------------------------------------------------ publishing

    /// Publish a batch. Every message gets an outcome; the call itself only
    /// fails for errors that happen before anything was sent.
    pub async fn publish(
        &self,
        messages: &[OutboundMessage],
        mode: PublishMode,
        timeout: Duration,
    ) -> Result<Vec<PublishOutcome>> {
        let limits = self.inner.read().await.profile.limits;
        if messages.len() > limits.max_publish_batch {
            return Err(CoreError::new(
                ErrorCode::LimitExceeded,
                format!(
                    "batch of {} exceeds max_publish_batch={}",
                    messages.len(),
                    limits.max_publish_batch
                ),
            ));
        }
        // Validate everything before sending anything.
        let mut prepared = Vec::with_capacity(messages.len());
        for m in messages {
            prepared.push((
                ss(&m.exchange)?,
                ss(&m.routing_key)?,
                m.payload.to_bytes()?,
                to_basic_properties(&m.properties)?,
                m.mandatory,
            ));
        }
        match mode {
            PublishMode::Confirm => self.publish_confirm(prepared, timeout).await,
            PublishMode::NoConfirm => self.publish_noconfirm(prepared).await,
            PublishMode::Transaction => self.publish_tx(prepared).await,
        }
    }

    async fn publish_confirm(
        &self,
        prepared: Vec<Prepared>,
        timeout: Duration,
    ) -> Result<Vec<PublishOutcome>> {
        let ch = self.channel(Slot::Publish).await?;
        let mut pending = Vec::with_capacity(prepared.len());
        let mut outcomes: Vec<Option<PublishOutcome>> = vec![None; prepared.len()];
        for (i, (ex, rk, body, props, mandatory)) in prepared.into_iter().enumerate() {
            match ch
                .basic_publish(
                    ex,
                    rk,
                    BasicPublishOptions {
                        mandatory,
                        immediate: false,
                    },
                    &body,
                    props,
                )
                .await
            {
                Ok(confirm) => pending.push((i, confirm)),
                Err(e) => {
                    let status = if errors::is_pre_send(&e) {
                        PublishStatus::Failed
                    } else {
                        PublishStatus::Uncertain
                    };
                    outcomes[i] = Some(outcome(
                        i,
                        status,
                        None,
                        None,
                        Some(map(&e, "basic.publish").message),
                    ));
                }
            }
        }
        let deadline = tokio::time::Instant::now() + timeout;
        for (i, confirm) in pending {
            let r = tokio::time::timeout_at(deadline, confirm).await;
            outcomes[i] = Some(match r {
                Err(_) => outcome(
                    i,
                    PublishStatus::Uncertain,
                    None,
                    None,
                    Some("no confirm before timeout".into()),
                ),
                Ok(Err(e)) => outcome(
                    i,
                    PublishStatus::Uncertain,
                    None,
                    None,
                    Some(map(&e, "confirm").message),
                ),
                Ok(Ok(Confirmation::Ack(None))) => {
                    outcome(i, PublishStatus::Confirmed, None, None, None)
                }
                Ok(Ok(Confirmation::Ack(Some(ret)))) => self.returned(i, ret),
                Ok(Ok(Confirmation::Nack(ret))) => {
                    let mut o = outcome(
                        i,
                        PublishStatus::Nacked,
                        None,
                        None,
                        Some("broker nacked the message".into()),
                    );
                    if let Some(r) = ret {
                        o.reply_code = Some(r.reply_code);
                        o.reply_text = Some(r.reply_text.as_str().to_string());
                    }
                    o
                }
                Ok(Ok(Confirmation::NotRequested)) => {
                    outcome(i, PublishStatus::Unconfirmed, None, None, None)
                }
            });
        }
        Ok(outcomes
            .into_iter()
            .map(|o| o.expect("every index has an outcome"))
            .collect())
    }

    fn returned(&self, i: usize, ret: BasicReturnMessage) -> PublishOutcome {
        let id = self
            .inner
            .try_read()
            .map(|g| g.id.clone())
            .unwrap_or_default();
        self.sink.emit(
            names::RETURNED,
            json!({
                "session": id,
                "index": i,
                "exchange": ret.delivery.exchange.as_str(),
                "routing_key": ret.delivery.routing_key.as_str(),
                "reply_code": ret.reply_code,
                "reply_text": ret.reply_text.as_str(),
            }),
        );
        outcome(
            i,
            PublishStatus::Returned,
            Some(ret.reply_code),
            Some(ret.reply_text.as_str().to_string()),
            None,
        )
    }

    async fn publish_noconfirm(&self, prepared: Vec<Prepared>) -> Result<Vec<PublishOutcome>> {
        // Separate non-confirm channel so confirm sequencing is unaffected.
        let conn = self.conn().await?;
        let ch = conn
            .create_channel()
            .await
            .map_err(|e| map(&e, "channel.open"))?;
        let mut out = Vec::new();
        for (i, (ex, rk, body, props, mandatory)) in prepared.into_iter().enumerate() {
            let r = ch
                .basic_publish(
                    ex,
                    rk,
                    BasicPublishOptions {
                        mandatory,
                        immediate: false,
                    },
                    &body,
                    props,
                )
                .await;
            out.push(match r {
                Ok(_) => outcome(i, PublishStatus::Unconfirmed, None, None, None),
                Err(e) => outcome(
                    i,
                    if errors::is_pre_send(&e) {
                        PublishStatus::Failed
                    } else {
                        PublishStatus::Uncertain
                    },
                    None,
                    None,
                    Some(map(&e, "basic.publish").message),
                ),
            });
        }
        let _ = ch.close(200, "OK".into()).await;
        Ok(out)
    }

    async fn publish_tx(&self, prepared: Vec<Prepared>) -> Result<Vec<PublishOutcome>> {
        let n = prepared.len();
        let conn = self.conn().await?;
        let ch = conn
            .create_channel()
            .await
            .map_err(|e| map(&e, "channel.open"))?;
        ch.tx_select().await.map_err(|e| map(&e, "tx.select"))?;
        for (ex, rk, body, props, mandatory) in prepared {
            if let Err(e) = ch
                .basic_publish(
                    ex,
                    rk,
                    BasicPublishOptions {
                        mandatory,
                        immediate: false,
                    },
                    &body,
                    props,
                )
                .await
            {
                let _ = ch.tx_rollback().await;
                let err = map(&e, "basic.publish");
                return Ok((0..n)
                    .map(|i| {
                        outcome(
                            i,
                            PublishStatus::Failed,
                            None,
                            None,
                            Some(format!("transaction rolled back: {}", err.message)),
                        )
                    })
                    .collect());
            }
        }
        let r = ch.tx_commit().await;
        let _ = ch.close(200, "OK".into()).await;
        Ok(match r {
            Ok(()) => (0..n)
                .map(|i| outcome(i, PublishStatus::Committed, None, None, None))
                .collect(),
            // A failed commit after the frame was written is ambiguous.
            Err(e) => {
                let msg = map(&e, "tx.commit").message;
                (0..n)
                    .map(|i| outcome(i, PublishStatus::Uncertain, None, None, Some(msg.clone())))
                    .collect()
            }
        })
    }

    // ------------------------------------------------------------ pull (basic.get)

    /// Fetch up to `count` messages with basic.get. This is a destructive
    /// operation: with `auto_ack` messages are removed immediately; without it
    /// they become unacknowledged on this session and must be settled.
    pub async fn get(&self, queue: &str, count: usize, auto_ack: bool) -> Result<GetResult> {
        let limits = self.inner.read().await.profile.limits;
        if count == 0 || count > limits.max_pull {
            return Err(CoreError::invalid(format!(
                "count must be between 1 and {}",
                limits.max_pull
            )));
        }
        if !auto_ack {
            let un = self.unsettled.lock().await.len();
            if un + count > limits.max_unsettled {
                return Err(CoreError::new(
                    ErrorCode::LimitExceeded,
                    format!(
                        "{un} deliveries are already unsettled (limit {}); settle some first",
                        limits.max_unsettled
                    ),
                ));
            }
        }
        let ch = self.channel(Slot::Get).await?;
        let epoch = self.epoch.load(Ordering::SeqCst);
        let mut out = Vec::new();
        let mut remaining = None;
        let q = ss(queue)?;
        for _ in 0..count {
            match ch
                .basic_get(q.clone(), BasicGetOptions { no_ack: auto_ack })
                .await
            {
                Ok(Some(msg)) => {
                    remaining = Some(msg.message_count);
                    let mc = msg.message_count;
                    let m = self
                        .register(
                            msg.delivery,
                            ch.id(),
                            queue,
                            None,
                            epoch,
                            auto_ack,
                            limits.max_payload_preview,
                            Some(mc),
                        )
                        .await;
                    out.push(m);
                }
                Ok(None) => {
                    remaining = Some(0);
                    break;
                }
                Err(e) => {
                    let err = map(&e, "basic.get");
                    if out.is_empty() {
                        return Err(err);
                    }
                    // Partial success must be reported, not hidden behind an error.
                    events::log(
                        &*self.sink,
                        "error",
                        "amqp091",
                        format!(
                            "basic.get stopped after {} message(s): {}",
                            out.len(),
                            err.message
                        ),
                    );
                    break;
                }
            }
        }
        Ok(GetResult {
            empty: out.is_empty(),
            remaining,
            destructive: true,
            note: if auto_ack {
                "Messages were removed from the queue (auto-ack).".into()
            } else {
                "Messages are unacknowledged on this session: ack to remove, nack/reject with requeue to return them (they will be marked redelivered and may change position).".into()
            },
            messages: out,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn register(
        &self,
        d: Delivery,
        channel: u16,
        queue: &str,
        consumer_tag: Option<String>,
        epoch: u64,
        auto_ack: bool,
        preview: usize,
        message_count: Option<u32>,
    ) -> InboundMessage {
        let session = self.inner.read().await.id.clone();
        let id = format!("{session}/{epoch}/{channel}/{}", d.delivery_tag);
        let m = InboundMessage {
            id: id.clone(),
            source: DeliverySource::Amqp091 {
                queue: queue.to_string(),
                channel,
                delivery_tag: d.delivery_tag,
                connection_epoch: epoch,
                consumer_tag,
                auto_ack,
            },
            exchange: d.exchange.as_str().to_string(),
            routing_key: d.routing_key.as_str().to_string(),
            redelivered: d.redelivered,
            properties: from_basic_properties(&d.properties),
            payload: PayloadView::new(&d.data, preview),
            received_at: now_millis(),
            message_count,
        };
        if !auto_ack {
            self.unsettled.lock().await.insert(
                id,
                Unsettled {
                    acker: d.acker,
                    epoch,
                    channel,
                    delivery_tag: d.delivery_tag,
                    queue: queue.to_string(),
                },
            );
        }
        m
    }

    // ------------------------------------------------------------ settlement

    /// Settle a delivery by bridge id. Uses the original channel's acker; fails
    /// with `stale_delivery` if that channel or connection is gone.
    pub async fn settle(
        &self,
        id: &str,
        how: Settlement,
        multiple: bool,
    ) -> Result<Vec<SettleResult>> {
        let targets: Vec<(String, Unsettled)> = {
            let mut un = self.unsettled.lock().await;
            let Some(first) = un.get(id) else {
                let stale = self.stale.lock().await.iter().any(|s| s == id);
                return Err(if stale {
                    CoreError::new(
                        ErrorCode::StaleDelivery,
                        format!("delivery {id} belongs to a channel or connection that has closed; the broker will redeliver it"),
                    )
                } else {
                    CoreError::not_found(format!(
                        "no unsettled delivery {id} (already settled, auto-acked, or unknown)"
                    ))
                });
            };
            if !first.acker.usable() {
                let u = un.remove(id).unwrap();
                self.stale.lock().await.push(id.to_string());
                return Err(CoreError::new(
                    ErrorCode::StaleDelivery,
                    format!(
                        "delivery {id} can no longer be settled: channel {} closed",
                        u.channel
                    ),
                ));
            }
            let (epoch, channel, tag) = (first.epoch, first.channel, first.delivery_tag);
            let keys: Vec<String> = if multiple {
                un.iter()
                    .filter(|(_, u)| {
                        u.epoch == epoch && u.channel == channel && u.delivery_tag <= tag
                    })
                    .map(|(k, _)| k.clone())
                    .collect()
            } else {
                vec![id.to_string()]
            };
            keys.into_iter()
                .filter_map(|k| un.remove(&k).map(|u| (k, u)))
                .collect()
        };
        // Settle individually through each Acker: lapin's `multiple` acks are
        // tied to a single acker, and per-delivery settlement keeps the
        // bookkeeping exact.
        let mut results = Vec::new();
        for (k, u) in targets {
            let r = match how {
                Settlement::Ack => u.acker.ack(BasicAckOptions { multiple: false }).await,
                Settlement::Nack { requeue } => {
                    u.acker
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue,
                        })
                        .await
                }
                Settlement::Reject { requeue } => {
                    u.acker.reject(BasicRejectOptions { requeue }).await
                }
                other => {
                    // put back; AMQP 0-9-1 has no release/modify outcome.
                    self.unsettled.lock().await.insert(k.clone(), u);
                    return Err(CoreError::unsupported(format!(
                        "{other:?} is an AMQP 1.0 outcome; use ack, nack, or reject"
                    )));
                }
            };
            match r {
                Ok(true) => results.push(SettleResult {
                    id: k,
                    settled: true,
                    outcome: how,
                }),
                Ok(false) => {
                    self.stale.lock().await.push(k.clone());
                    results.push(SettleResult {
                        id: k,
                        settled: false,
                        outcome: how,
                    });
                }
                Err(e) => {
                    let err = map(&e, "settle");
                    events::log(
                        &*self.sink,
                        "error",
                        "amqp091",
                        format!("settling {k} on queue {} failed: {}", u.queue, err.message),
                    );
                    self.stale.lock().await.push(k.clone());
                    results.push(SettleResult {
                        id: k,
                        settled: false,
                        outcome: how,
                    });
                }
            }
        }
        Ok(results)
    }

    pub async fn unsettled_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.unsettled.lock().await.keys().cloned().collect();
        v.sort();
        v
    }

    // ------------------------------------------------------------ consumers

    pub async fn consume(self: &Arc<Self>, o: ConsumeOptions) -> Result<ConsumerInfo> {
        let limits = self.inner.read().await.profile.limits;
        {
            let g = self.inner.read().await;
            let active = g
                .consumers
                .values()
                .filter(|c| c.info.state == "active")
                .count();
            if active >= limits.max_consumers {
                return Err(CoreError::new(
                    ErrorCode::LimitExceeded,
                    format!("max_consumers={} reached", limits.max_consumers),
                ));
            }
        }
        let conn = self.conn().await?;
        // One channel per consumer: cancelling or losing one never invalidates
        // another consumer's delivery tags.
        let ch = conn
            .create_channel()
            .await
            .map_err(|e| map(&e, "channel.open"))?;
        if !o.auto_ack {
            ch.basic_qos(o.prefetch, BasicQosOptions { global: false })
                .await
                .map_err(|e| map(&e, "basic.qos"))?;
        }
        let mut args = o.arguments.clone();
        if let Some(p) = o.priority {
            args.insert("x-priority".into(), json!({"$type": "i32", "value": p}));
        }
        let tag = o
            .consumer_tag
            .clone()
            .unwrap_or_else(|| format!("rabbitmq.el-{}", self.seq.fetch_add(1, Ordering::Relaxed)));
        let consumer = ch
            .basic_consume(
                ss(&o.queue)?,
                ss(&tag)?,
                BasicConsumeOptions {
                    no_local: false,
                    no_ack: o.auto_ack,
                    exclusive: o.exclusive,
                    nowait: false,
                },
                json_to_table(&args)?,
            )
            .await
            .map_err(|e| map(&e, "basic.consume"))?;
        let epoch = self.epoch.load(Ordering::SeqCst);
        let info = ConsumerInfo {
            tag: tag.clone(),
            queue: o.queue.clone(),
            channel: ch.id(),
            prefetch: o.prefetch,
            auto_ack: o.auto_ack,
            state: "active".into(),
            paused: false,
            delivered: 0,
            buffered: 0,
            dropped: 0,
        };
        let buffer = Arc::new(Mutex::new(BoundedBuffer::new(
            limits.max_buffered_deliveries,
        )));
        let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task = tokio::spawn(consumer_loop(
            Arc::downgrade(self),
            consumer,
            tag.clone(),
            o.clone(),
            epoch,
            ch.id(),
            buffer.clone(),
            paused.clone(),
            cancelled.clone(),
            limits.max_payload_preview,
        ));
        self.inner.write().await.consumers.insert(
            tag.clone(),
            ConsumerHandle {
                info: info.clone(),
                channel: ch,
                task,
                buffer,
                paused,
                cancelled_by_client: cancelled,
            },
        );
        self.emit_consumer(&tag, "active", None);
        Ok(info)
    }

    fn emit_consumer(&self, tag: &str, state: &str, reason: Option<String>) {
        let id = self
            .inner
            .try_read()
            .map(|g| g.id.clone())
            .unwrap_or_default();
        self.sink.emit(
            names::CONSUMER,
            json!({ "session": id, "consumer": tag, "state": state, "reason": reason, "at": now_millis() }),
        );
    }

    /// Pause *display*: deliveries stay buffered (bounded) in the bridge and
    /// prefetch keeps the broker from sending more than `prefetch` unacked
    /// messages, so pausing applies natural backpressure in manual-ack mode.
    pub async fn set_paused(&self, tag: &str, paused: bool) -> Result<ConsumerInfo> {
        let g = self.inner.read().await;
        let c = g
            .consumers
            .get(tag)
            .ok_or_else(|| CoreError::not_found(format!("no consumer {tag}")))?;
        c.paused.store(paused, Ordering::SeqCst);
        let mut info = c.info.clone();
        info.paused = paused;
        drop(g);
        if !paused {
            self.flush_consumer(tag).await;
        }
        Ok(info)
    }

    /// Drain buffered deliveries for a consumer (used by the frontend after
    /// resuming, or to page through a paused consumer).
    pub async fn drain(&self, tag: &str, max: usize) -> Result<(Vec<InboundMessage>, u64)> {
        let g = self.inner.read().await;
        let c = g
            .consumers
            .get(tag)
            .ok_or_else(|| CoreError::not_found(format!("no consumer {tag}")))?;
        let mut b = c.buffer.lock().await;
        Ok((b.drain(max), b.dropped()))
    }

    async fn flush_consumer(&self, tag: &str) {
        let (session, msgs, dropped) = {
            let g = self.inner.read().await;
            let Some(c) = g.consumers.get(tag) else {
                return;
            };
            let mut b = c.buffer.lock().await;
            (g.id.clone(), b.drain(usize::MAX), b.dropped())
        };
        if !msgs.is_empty() {
            self.sink.emit(
                names::DELIVERIES,
                json!({ "session": session, "consumer": tag, "messages": msgs, "dropped": dropped }),
            );
        }
    }

    pub async fn cancel(&self, tag: &str) -> Result<ConsumerInfo> {
        let handle = self
            .inner
            .write()
            .await
            .consumers
            .remove(tag)
            .ok_or_else(|| CoreError::not_found(format!("no consumer {tag}")))?;
        handle.cancelled_by_client.store(true, Ordering::SeqCst);
        let mut info = handle.info.clone();
        if handle.channel.status().connected() {
            let _ = handle
                .channel
                .basic_cancel(ss(tag)?, BasicCancelOptions::default())
                .await;
        }
        handle.task.abort();
        // Unacked deliveries for this consumer stay settleable while the channel
        // is open; closing the channel returns them to the queue. We keep the
        // channel open only if there are unsettled deliveries on it.
        let epoch = self.epoch.load(Ordering::SeqCst);
        let has_unsettled = self
            .unsettled
            .lock()
            .await
            .values()
            .any(|u| u.epoch == epoch && u.channel == handle.channel.id());
        if !has_unsettled && handle.channel.status().connected() {
            let _ = handle.channel.close(200, "OK".into()).await;
        }
        info.state = "cancelled".into();
        self.emit_consumer(tag, "cancelled", Some("cancelled by client".into()));
        Ok(info)
    }

    /// Close a consumer's channel, returning all its unacked deliveries to the
    /// queue. Used after `cancel` when the user chooses to abandon unsettled
    /// messages rather than settle them.
    pub async fn release_channel(&self, channel: u16) -> Result<usize> {
        let epoch = self.epoch.load(Ordering::SeqCst);
        let n = self.invalidate_channel(epoch, channel).await;
        Ok(n)
    }

    pub async fn close(&self) -> Result<()> {
        self.closing.store(true, Ordering::SeqCst);
        let (conn, consumers) = {
            let mut g = self.inner.write().await;
            g.state = SessionState::Closed;
            (g.conn.take(), std::mem::take(&mut g.consumers))
        };
        for (_, c) in consumers {
            c.task.abort();
        }
        let epoch = self.epoch.load(Ordering::SeqCst);
        self.invalidate_epoch(epoch).await;
        if let Some(c) = conn {
            let _ =
                tokio::time::timeout(Duration::from_secs(5), c.close(200, "client closed".into()))
                    .await;
        }
        self.emit_state("closed", json!({}));
        Ok(())
    }
}

type Prepared = (
    ShortString,
    ShortString,
    Vec<u8>,
    lapin::BasicProperties,
    bool,
);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Slot {
    Publish,
    Get,
}

fn ss(s: &str) -> Result<ShortString> {
    ShortString::try_new(s.to_string())
        .map_err(|_| CoreError::invalid(format!("name \"{s}\" exceeds 255 bytes")))
}

fn outcome(
    i: usize,
    status: PublishStatus,
    code: Option<u16>,
    text: Option<String>,
    error: Option<String>,
) -> PublishOutcome {
    PublishOutcome {
        index: i,
        status,
        reply_code: code,
        reply_text: text,
        error,
    }
}

#[allow(clippy::too_many_arguments)]
async fn consumer_loop(
    session: std::sync::Weak<Session>,
    mut consumer: lapin::Consumer,
    tag: String,
    o: ConsumeOptions,
    epoch: u64,
    channel: u16,
    buffer: Arc<Mutex<BoundedBuffer<InboundMessage>>>,
    paused: Arc<std::sync::atomic::AtomicBool>,
    cancelled_by_client: Arc<std::sync::atomic::AtomicBool>,
    preview: usize,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(o.flush_ms.max(10)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pending = 0usize;
    let end_reason: Option<String>;
    loop {
        tokio::select! {
            next = consumer.next() => {
                let Some(s) = session.upgrade() else { return };
                match next {
                    Some(Ok(d)) => {
                        let msg = s.register(d, channel, &o.queue, Some(tag.clone()), epoch, o.auto_ack, preview, None).await;
                        {
                            let mut g = s.inner.write().await;
                            if let Some(c) = g.consumers.get_mut(&tag) { c.info.delivered += 1; }
                        }
                        let evicted = buffer.lock().await.push(msg);
                        if let Some(ev) = evicted {
                            // Buffer overflow (only possible with auto_ack or a very large
                            // prefetch). The message itself is still unsettled on the broker
                            // side in manual mode, so leave it settleable but tell the UI.
                            events::log(&*s.sink, "warning", "amqp091", format!("consumer {tag}: display buffer full; dropped {} from view (still unsettled: {})", ev.id, !o.auto_ack));
                        }
                        pending += 1;
                        if pending >= o.batch && !paused.load(Ordering::Relaxed) {
                            s.flush_consumer(&tag).await;
                            pending = 0;
                        }
                    }
                    Some(Err(e)) => {
                        let err = map(&e, "consumer");
                        end_reason = Some(err.message);
                        break;
                    }
                    None => {
                        end_reason = if cancelled_by_client.load(Ordering::SeqCst) { None } else {
                            Some("cancelled by broker (queue deleted, node failure, or leader change)".into())
                        };
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                if pending > 0 && !paused.load(Ordering::Relaxed) {
                    let Some(s) = session.upgrade() else { return };
                    s.flush_consumer(&tag).await;
                    pending = 0;
                }
            }
        }
    }
    let Some(s) = session.upgrade() else { return };
    s.flush_consumer(&tag).await;
    if let Some(reason) = end_reason {
        let n = s.invalidate_channel(epoch, channel).await;
        {
            let mut g = s.inner.write().await;
            if let Some(c) = g.consumers.get_mut(&tag) {
                c.info.state = "cancelled_by_broker".into();
            }
        }
        s.emit_consumer(
            &tag,
            "cancelled_by_broker",
            Some(format!("{reason}; {n} unsettled deliveries invalidated")),
        );
        events::log(
            &*s.sink,
            "warning",
            "amqp091",
            format!("consumer {tag} on {} ended: {reason}", o.queue),
        );
    }
}
