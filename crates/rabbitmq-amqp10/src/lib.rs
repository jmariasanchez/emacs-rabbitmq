//! # rabbitmq-amqp10
//!
//! AMQP 1.0 adapter for RabbitMQ 4.x, built on [fe2o3-amqp].
//!
//! AMQP 1.0 is modelled on its own terms, not as renamed 0-9-1:
//!
//! * a [`Amqp10Session`] owns one AMQP 1.0 *connection* and one *session*;
//! * senders and receivers are *links* attached to RabbitMQ v2 addresses
//!   (`/queues/{q}`, `/exchanges/{x}/{key}`, `/exchanges/{x}`);
//! * receivers use explicit **link credit** for flow control (no prefetch);
//! * deliveries are settled with AMQP 1.0 **outcomes**: `accepted`,
//!   `rejected`, `released`, `modified`; RabbitMQ maps `rejected` to
//!   dead-lettering and `released`/`modified` to requeue;
//! * publish outcomes come from the broker's disposition; `released` from a
//!   sender means the broker could not route the message.
//!
//! Each link runs in its own task that owns the link handle; the public API
//! talks to it over channels so callers never hold non-`Sync` handles.
//!
//! [fe2o3-amqp]: https://docs.rs/fe2o3-amqp

use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::types::messaging::{
    ApplicationProperties, Body, Data, Message, MessageAnnotations, Modified, Outcome, Properties,
};
use fe2o3_amqp::types::primitives::{Binary, SimpleValue, Symbol, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use rabbitmq_core::error::{CoreError, ErrorCode, Result};
use rabbitmq_core::events::{self, names, SharedSink};
use rabbitmq_core::message::*;
use rabbitmq_core::profile::{pct, Profile};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Build a RabbitMQ 4.x v2 address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Address {
    Queue { queue: String },
    Exchange { exchange: String, #[serde(default)] routing_key: Option<String> },
    /// A raw address string, used verbatim.
    Raw { address: String },
}

impl Address {
    pub fn render(&self) -> String {
        match self {
            Address::Queue { queue } => format!("/queues/{}", pct(queue)),
            Address::Exchange { exchange, routing_key: Some(k) } => format!("/exchanges/{}/{}", pct(exchange), pct(k)),
            Address::Exchange { exchange, routing_key: None } => format!("/exchanges/{}", pct(exchange)),
            Address::Raw { address } => address.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkInfo {
    pub id: String,
    pub role: &'static str,
    pub address: String,
    pub state: String,
    pub credit: Option<u32>,
    pub delivered: u64,
    pub unsettled: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReceiverOptions {
    pub address: Address,
    /// Initial link credit. Credit is the AMQP 1.0 flow-control mechanism:
    /// the broker sends at most this many messages before the client grants more.
    #[serde(default = "d_credit")]
    pub credit: u32,
    /// Automatically top up credit as deliveries are settled.
    #[serde(default = "yes")]
    pub auto_credit: bool,
    #[serde(default = "d_flush")]
    pub flush_ms: u64,
}

fn d_credit() -> u32 {
    10
}
fn yes() -> bool {
    true
}
fn d_flush() -> u64 {
    100
}

enum SendCmd {
    Send(Box<Message<Body<Value>>>, oneshot::Sender<std::result::Result<Outcome, String>>),
    Close(oneshot::Sender<()>),
}

enum RecvCmd {
    Settle(u32, Settlement, oneshot::Sender<Result<()>>),
    Credit(u32, oneshot::Sender<Result<()>>),
    Drain(oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<()>),
}

struct SenderLink {
    tx: mpsc::Sender<SendCmd>,
    info: Arc<Mutex<LinkInfo>>,
}

struct ReceiverLink {
    tx: mpsc::Sender<RecvCmd>,
    info: Arc<Mutex<LinkInfo>>,
}

pub struct Amqp10Session {
    id: String,
    profile: Profile,
    sink: SharedSink,
    senders: Mutex<HashMap<String, SenderLink>>,
    receivers: Mutex<HashMap<String, ReceiverLink>>,
    seq: AtomicU64,
}

fn url(p: &Profile) -> String {
    let scheme = if p.tls.enabled { "amqps" } else { "amqp" };
    format!(
        "{scheme}://{}:{}@{}:{}",
        pct(&p.username),
        pct(p.password.expose()),
        p.host,
        p.amqp10_port.unwrap_or(p.amqp_port)
    )
}

fn map_err(what: &str, e: impl std::fmt::Display) -> CoreError {
    let s = e.to_string();
    let code = if s.contains("not-found") || s.contains("NotFound") || s.contains("not found") {
        ErrorCode::NotFound
    } else if s.contains("unauthorized") || s.contains("access") || s.contains("Unauthorized") {
        ErrorCode::AccessRefused
    } else if s.contains("SaslError") || s.contains("auth") {
        ErrorCode::AuthenticationFailed
    } else if s.contains("Io") || s.contains("Connection") || s.contains("refused") {
        ErrorCode::ConnectionFailed
    } else {
        ErrorCode::Protocol
    };
    CoreError::new(code, format!("{what}: {s}"))
}

/// One fe2o3 connection + session, created inside the link task that owns it.
async fn open(p: &Profile, container: &str) -> Result<(fe2o3_amqp::connection::ConnectionHandle<()>, fe2o3_amqp::session::SessionHandle<()>)> {
    if p.tls.enabled {
        return Err(CoreError::unsupported(
            "AMQP 1.0 over TLS is not yet wired in this adapter; use the AMQP 0-9-1 adapter or a plain listener",
        ));
    }
    let vhost = format!("vhost:{}", p.vhost);
    let mut conn = tokio::time::timeout(
        Duration::from_millis(p.limits.timeout_ms),
        Connection::builder().container_id(container).hostname(vhost.as_str()).open(url(p).as_str()),
    )
    .await
    .map_err(|_| CoreError::new(ErrorCode::Timeout, "AMQP 1.0 connect timed out"))?
    .map_err(|e| map_err("amqp10 connect", e))?;
    let session = Session::begin(&mut conn).await.map_err(|e| map_err("amqp10 session", e))?;
    Ok((conn, session))
}

impl Amqp10Session {
    /// Validate connectivity and return a session factory. Links open their
    /// own connection so a failing link never takes others down.
    pub async fn connect(id: String, profile: Profile, sink: SharedSink) -> Result<Arc<Self>> {
        let (mut c, mut s) = open(&profile, &format!("{id}-probe")).await?;
        let _ = s.end().await;
        let _ = c.close().await;
        sink.emit(names::SESSION, json!({"session": id, "protocol": "amqp10", "state": "connected", "at": now_millis()}));
        Ok(Arc::new(Self {
            id,
            profile,
            sink,
            senders: Mutex::new(HashMap::new()),
            receivers: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
        }))
    }

    fn next_id(&self, role: &str) -> String {
        format!("{}/amqp10/{role}/{}", self.id, self.seq.fetch_add(1, Ordering::Relaxed))
    }

    // ------------------------------------------------------------ senders

    pub async fn open_sender(&self, address: &Address) -> Result<LinkInfo> {
        let id = self.next_id("sender");
        let addr = address.render();
        let profile = self.profile.clone();
        let (tx, mut rx) = mpsc::channel::<SendCmd>(64);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
        let info = Arc::new(Mutex::new(LinkInfo { id: id.clone(), role: "sender", address: addr.clone(), state: "attaching".into(), credit: None, delivered: 0, unsettled: 0 }));
        let info2 = info.clone();
        let name = id.clone();
        tokio::spawn(async move {
            let (mut conn, mut session) = match open(&profile, &name).await {
                Ok(x) => x,
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            let mut sender = match Sender::attach(&mut session, name.clone(), addr.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = ready_tx.send(Err(map_err("attach sender", e)));
                    let _ = session.end().await;
                    let _ = conn.close().await;
                    return;
                }
            };
            info2.lock().await.state = "attached".into();
            let _ = ready_tx.send(Ok(()));
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    SendCmd::Send(m, reply) => {
                        let r = sender.send(*m).await.map_err(|e| e.to_string());
                        info2.lock().await.delivered += 1;
                        let _ = reply.send(r);
                    }
                    SendCmd::Close(done) => {
                        let _ = sender.close().await;
                        let _ = session.end().await;
                        let _ = conn.close().await;
                        info2.lock().await.state = "closed".into();
                        let _ = done.send(());
                        return;
                    }
                }
            }
            let _ = sender.close().await;
            let _ = session.end().await;
            let _ = conn.close().await;
        });
        ready_rx.await.map_err(|_| CoreError::internal("sender task ended"))??;
        let snapshot = info.lock().await.clone();
        self.senders.lock().await.insert(id, SenderLink { tx, info });
        Ok(snapshot)
    }

    /// Send messages on a sender link and classify each disposition.
    pub async fn send(&self, link: &str, messages: &[(Payload, MessageProperties)], timeout: Duration) -> Result<Vec<PublishOutcome>> {
        let tx = {
            let s = self.senders.lock().await;
            s.get(link).ok_or_else(|| CoreError::not_found(format!("no sender link {link}")))?.tx.clone()
        };
        let mut out = Vec::new();
        for (i, (payload, props)) in messages.iter().enumerate() {
            let m = build_message(&payload.to_bytes()?, props);
            let (rtx, rrx) = oneshot::channel();
            if tx.send(SendCmd::Send(Box::new(m), rtx)).await.is_err() {
                out.push(PublishOutcome { index: i, status: PublishStatus::Failed, reply_code: None, reply_text: None, error: Some("sender link closed".into()) });
                continue;
            }
            let o = match tokio::time::timeout(timeout, rrx).await {
                Err(_) => PublishOutcome { index: i, status: PublishStatus::Uncertain, reply_code: None, reply_text: None, error: Some("no disposition before timeout".into()) },
                Ok(Err(_)) => PublishOutcome { index: i, status: PublishStatus::Uncertain, reply_code: None, reply_text: None, error: Some("sender task ended".into()) },
                Ok(Ok(Err(e))) => PublishOutcome { index: i, status: PublishStatus::Uncertain, reply_code: None, reply_text: None, error: Some(e) },
                Ok(Ok(Ok(outcome))) => classify(i, outcome),
            };
            out.push(o);
        }
        Ok(out)
    }

    pub async fn close_sender(&self, link: &str) -> Result<()> {
        let l = self.senders.lock().await.remove(link).ok_or_else(|| CoreError::not_found(format!("no sender link {link}")))?;
        let (d, w) = oneshot::channel();
        let _ = l.tx.send(SendCmd::Close(d)).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), w).await;
        Ok(())
    }

    // ------------------------------------------------------------ receivers

    /// Attach a receiver link. Consuming from a queue address removes
    /// messages once they are accepted; the bridge classifies this as `consume`.
    pub async fn open_receiver(self: &Arc<Self>, o: ReceiverOptions) -> Result<LinkInfo> {
        let id = self.next_id("receiver");
        let addr = o.address.render();
        let profile = self.profile.clone();
        let (tx, mut rx) = mpsc::channel::<RecvCmd>(256);
        let (ready_tx, ready_rx) = oneshot::channel::<Result<()>>();
        let info = Arc::new(Mutex::new(LinkInfo { id: id.clone(), role: "receiver", address: addr.clone(), state: "attaching".into(), credit: Some(o.credit), delivered: 0, unsettled: 0 }));
        let info2 = info.clone();
        let me = Arc::downgrade(self);
        let link_id = id.clone();
        tokio::spawn(async move {
            let (mut conn, mut session) = match open(&profile, &link_id).await {
                Ok(x) => x,
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            let mode = if o.auto_credit {
                fe2o3_amqp::link::receiver::CreditMode::Auto(o.credit)
            } else {
                fe2o3_amqp::link::receiver::CreditMode::Manual
            };
            let r = Receiver::builder()
                .name(link_id.clone())
                .source(addr.clone())
                .credit_mode(mode)
                .auto_accept(false)
                .attach(&mut session)
                .await;
            let mut receiver = match r {
                Ok(r) => r,
                Err(e) => {
                    let _ = ready_tx.send(Err(map_err("attach receiver", e)));
                    let _ = session.end().await;
                    let _ = conn.close().await;
                    return;
                }
            };
            if !o.auto_credit {
                if let Err(e) = receiver.set_credit(o.credit).await {
                    let _ = ready_tx.send(Err(map_err("grant credit", e)));
                    return;
                }
            }
            info2.lock().await.state = "attached".into();
            let _ = ready_tx.send(Ok(()));
            let preview = profile.limits.max_payload_preview;
            let mut unsettled: HashMap<u32, DeliveryInfo> = HashMap::new();
            let mut batch: Vec<InboundMessage> = Vec::new();
            let mut tick = tokio::time::interval(Duration::from_millis(o.flush_ms.max(10)));
            loop {
                tokio::select! {
                    d = receiver.recv::<Body<Value>>() => {
                        match d {
                            Ok(d) => {
                                let did = *d.delivery_id();
                                let m = inbound(&link_id, &addr, did, d.message(), preview);
                                unsettled.insert(did, DeliveryInfo::from(&d));
                                {
                                    let mut i = info2.lock().await;
                                    i.delivered += 1;
                                    i.unsettled = unsettled.len();
                                }
                                batch.push(m);
                            }
                            Err(e) => {
                                if let Some(s) = me.upgrade() {
                                    events::log(&*s.sink, "error", "amqp10", format!("receiver {link_id} detached: {e}"));
                                    s.sink.emit(names::CONSUMER, json!({"session": s.id, "consumer": link_id, "state": "detached", "reason": e.to_string(), "at": now_millis()}));
                                }
                                info2.lock().await.state = "detached".into();
                                break;
                            }
                        }
                    }
                    cmd = rx.recv() => {
                        match cmd {
                            Some(RecvCmd::Settle(did, how, reply)) => {
                                let Some(di) = unsettled.remove(&did) else {
                                    let _ = reply.send(Err(CoreError::not_found(format!("no unsettled delivery {did} on {link_id}"))));
                                    continue;
                                };
                                let r = match how {
                                    Settlement::Ack => receiver.accept(di).await,
                                    Settlement::Reject { .. } | Settlement::Nack { requeue: false } => receiver.reject(di, None).await,
                                    Settlement::Release | Settlement::Nack { requeue: true } => receiver.release(di).await,
                                    Settlement::Modify { delivery_failed, undeliverable_here } => {
                                        receiver.modify(di, Modified { delivery_failed: Some(delivery_failed), undeliverable_here: Some(undeliverable_here), message_annotations: None }).await
                                    }
                                };
                                info2.lock().await.unsettled = unsettled.len();
                                let _ = reply.send(r.map_err(|e| map_err("disposition", e)));
                            }
                            Some(RecvCmd::Credit(n, reply)) => {
                                let r = receiver.set_credit(n).await.map_err(|e| map_err("set credit", e));
                                info2.lock().await.credit = Some(n);
                                let _ = reply.send(r);
                            }
                            Some(RecvCmd::Drain(reply)) => {
                                let r = receiver.drain().await.map_err(|e| map_err("drain", e));
                                let _ = reply.send(r);
                            }
                            Some(RecvCmd::Close(done)) => {
                                // Unsettled deliveries are released back to the queue by the
                                // broker when the link detaches.
                                let _ = receiver.close().await;
                                let _ = session.end().await;
                                let _ = conn.close().await;
                                info2.lock().await.state = "closed".into();
                                let _ = done.send(());
                                break;
                            }
                            None => {
                                let _ = receiver.close().await;
                                let _ = session.end().await;
                                let _ = conn.close().await;
                                break;
                            }
                        }
                    }
                    _ = tick.tick() => {
                        if !batch.is_empty() {
                            if let Some(s) = me.upgrade() {
                                let msgs = std::mem::take(&mut batch);
                                s.sink.emit(names::DELIVERIES, json!({"session": s.id, "consumer": link_id, "messages": msgs, "dropped": 0, "protocol": "amqp10"}));
                            }
                        }
                    }
                }
            }
        });
        ready_rx.await.map_err(|_| CoreError::internal("receiver task ended"))??;
        let snapshot = info.lock().await.clone();
        self.receivers.lock().await.insert(id, ReceiverLink { tx, info });
        Ok(snapshot)
    }

    async fn recv_tx(&self, link: &str) -> Result<mpsc::Sender<RecvCmd>> {
        let r = self.receivers.lock().await;
        Ok(r.get(link).ok_or_else(|| CoreError::not_found(format!("no receiver link {link}")))?.tx.clone())
    }

    pub async fn settle(&self, link: &str, delivery_id: u32, how: Settlement) -> Result<()> {
        let tx = self.recv_tx(link).await?;
        let (rt, rr) = oneshot::channel();
        tx.send(RecvCmd::Settle(delivery_id, how, rt)).await.map_err(|_| CoreError::new(ErrorCode::ChannelClosed, "receiver link closed"))?;
        rr.await.map_err(|_| CoreError::new(ErrorCode::ChannelClosed, "receiver link closed"))?
    }

    pub async fn set_credit(&self, link: &str, credit: u32) -> Result<()> {
        let tx = self.recv_tx(link).await?;
        let (rt, rr) = oneshot::channel();
        tx.send(RecvCmd::Credit(credit, rt)).await.map_err(|_| CoreError::new(ErrorCode::ChannelClosed, "receiver link closed"))?;
        rr.await.map_err(|_| CoreError::new(ErrorCode::ChannelClosed, "receiver link closed"))?
    }

    pub async fn drain(&self, link: &str) -> Result<()> {
        let tx = self.recv_tx(link).await?;
        let (rt, rr) = oneshot::channel();
        tx.send(RecvCmd::Drain(rt)).await.map_err(|_| CoreError::new(ErrorCode::ChannelClosed, "receiver link closed"))?;
        rr.await.map_err(|_| CoreError::new(ErrorCode::ChannelClosed, "receiver link closed"))?
    }

    pub async fn close_receiver(&self, link: &str) -> Result<()> {
        let l = self.receivers.lock().await.remove(link).ok_or_else(|| CoreError::not_found(format!("no receiver link {link}")))?;
        let (d, w) = oneshot::channel();
        let _ = l.tx.send(RecvCmd::Close(d)).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), w).await;
        Ok(())
    }

    pub async fn links(&self) -> Vec<LinkInfo> {
        let mut v = Vec::new();
        for l in self.senders.lock().await.values() {
            v.push(l.info.lock().await.clone());
        }
        for l in self.receivers.lock().await.values() {
            v.push(l.info.lock().await.clone());
        }
        v
    }

    pub async fn close(&self) -> Result<()> {
        let s: Vec<String> = self.senders.lock().await.keys().cloned().collect();
        for k in s {
            let _ = self.close_sender(&k).await;
        }
        let r: Vec<String> = self.receivers.lock().await.keys().cloned().collect();
        for k in r {
            let _ = self.close_receiver(&k).await;
        }
        self.sink.emit(names::SESSION, json!({"session": self.id, "protocol": "amqp10", "state": "closed", "at": now_millis()}));
        Ok(())
    }
}

fn classify(i: usize, o: Outcome) -> PublishOutcome {
    let (status, text) = match o {
        Outcome::Accepted(_) => (PublishStatus::Confirmed, None),
        // RabbitMQ settles with `released` when a message cannot be routed.
        Outcome::Released(_) => (PublishStatus::Returned, Some("released: the broker could not route the message".to_string())),
        Outcome::Rejected(r) => (PublishStatus::Nacked, Some(format!("rejected: {:?}", r.error))),
        Outcome::Modified(m) => (PublishStatus::Nacked, Some(format!("modified: {m:?}"))),
        #[allow(unreachable_patterns)]
        other => (PublishStatus::Uncertain, Some(format!("{other:?}"))),
    };
    PublishOutcome { index: i, status, reply_code: None, reply_text: text, error: None }
}

fn build_message(body: &[u8], p: &MessageProperties) -> Message<Body<Value>> {
    let mut props = Properties::builder();
    if let Some(v) = &p.message_id {
        props = props.message_id(v.clone());
    }
    if let Some(v) = &p.correlation_id {
        props = props.correlation_id(v.clone());
    }
    if let Some(v) = &p.content_type {
        props = props.content_type(Symbol::new(v.clone()));
    }
    if let Some(v) = &p.content_encoding {
        props = props.content_encoding(Symbol::new(v.clone()));
    }
    if let Some(v) = &p.reply_to {
        props = props.reply_to(v.clone());
    }
    if let Some(v) = &p.kind {
        props = props.subject(v.clone());
    }
    if let Some(ts) = p.timestamp {
        props = props.creation_time(fe2o3_amqp::types::primitives::Timestamp::from_milliseconds(ts as i64 * 1000));
    }
    let mut ap = ApplicationProperties::builder();
    for (k, v) in &p.headers {
        ap = ap.insert(k.clone(), json_to_simple(v));
    }
    let mut ma = MessageAnnotations::builder();
    for (k, v) in &p.annotations {
        ma = ma.insert(Symbol::new(k.clone()), json_to_value(v));
    }
    let mut header = fe2o3_amqp::types::messaging::Header::default();
    header.durable = p.delivery_mode.map(|m| m == 2).unwrap_or(true);
    if let Some(pr) = p.priority {
        header.priority = pr.into();
    }
    if let Some(ttl) = p.expiration.as_ref().and_then(|e| e.parse::<u32>().ok()) {
        header.ttl = Some(ttl);
    }
    Message::builder()
        .header(header)
        .properties(props.build())
        .application_properties(ap.build())
        .message_annotations(ma.build())
        .body(Body::Data(vec![Data(Binary::from(body.to_vec()))].into()))
        .build()
}

fn json_to_simple(v: &serde_json::Value) -> SimpleValue {
    match v {
        serde_json::Value::Null => SimpleValue::String(String::new()),
        serde_json::Value::Bool(b) => SimpleValue::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => SimpleValue::Long(i),
            None => SimpleValue::Double(n.as_f64().unwrap_or(0.0).into()),
        },
        serde_json::Value::String(s) => SimpleValue::String(s.clone()),
        other => SimpleValue::String(other.to_string()),
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => Value::Long(i),
            None => Value::Double(n.as_f64().unwrap_or(0.0).into()),
        },
        serde_json::Value::String(s) => Value::String(s.clone()),
        other => Value::String(other.to_string()),
    }
}

fn simple_to_json(v: &SimpleValue) -> serde_json::Value {
    match v {
        SimpleValue::Bool(b) => json!(b),
        SimpleValue::Ubyte(n) => json!(n),
        SimpleValue::Ushort(n) => json!(n),
        SimpleValue::Uint(n) => json!(n),
        SimpleValue::Ulong(n) => json!(n),
        SimpleValue::Byte(n) => json!(n),
        SimpleValue::Short(n) => json!(n),
        SimpleValue::Int(n) => json!(n),
        SimpleValue::Long(n) => json!(n),
        SimpleValue::Float(f) => json!(f.0),
        SimpleValue::Double(f) => json!(f.0),
        SimpleValue::Char(c) => json!(c.to_string()),
        SimpleValue::Timestamp(t) => json!({"$type": "timestamp_ms", "value": t.milliseconds()}),
        SimpleValue::Uuid(u) => json!({"$type": "uuid", "value": format!("{u:?}")}),
        SimpleValue::Binary(b) => json!({"$type": "bytes", "base64": encode_b64(b)}),
        SimpleValue::String(s) => json!(s),
        SimpleValue::Symbol(s) => json!({"$type": "symbol", "value": s.as_str()}),
        other => json!({"$type": "debug", "value": format!("{other:?}")}),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => json!(b),
        Value::Ubyte(n) => json!(n),
        Value::Ushort(n) => json!(n),
        Value::Uint(n) => json!(n),
        Value::Ulong(n) => json!(n),
        Value::Byte(n) => json!(n),
        Value::Short(n) => json!(n),
        Value::Int(n) => json!(n),
        Value::Long(n) => json!(n),
        Value::Float(f) => json!(f.0),
        Value::Double(f) => json!(f.0),
        Value::String(s) => json!(s),
        Value::Symbol(s) => json!(s.as_str()),
        Value::Binary(b) => json!({"$type": "bytes", "base64": encode_b64(b)}),
        Value::Timestamp(t) => json!({"$type": "timestamp_ms", "value": t.milliseconds()}),
        Value::List(l) => serde_json::Value::Array(l.iter().map(value_to_json).collect()),
        other => json!({"$type": "debug", "value": format!("{other:?}")}),
    }
}

fn message_id_str(id: &fe2o3_amqp::types::messaging::MessageId) -> String {
    use fe2o3_amqp::types::messaging::MessageId as M;
    match id {
        M::String(s) => s.clone(),
        M::Ulong(n) => n.to_string(),
        M::Uuid(u) => format!("{u:?}"),
        M::Binary(b) => encode_b64(b),
    }
}

fn body_bytes(b: &Body<Value>) -> Vec<u8> {
    match b {
        Body::Data(batch) => batch.iter().flat_map(|d| d.0.to_vec()).collect(),
        Body::Value(v) => match &v.0 {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Binary(b) => b.to_vec(),
            other => value_to_json(other).to_string().into_bytes(),
        },
        Body::Sequence(seq) => serde_json::Value::Array(seq.iter().flat_map(|s| s.0.iter().map(value_to_json)).collect()).to_string().into_bytes(),
        Body::Empty => Vec::new(),
    }
}

fn inbound(link: &str, address: &str, delivery_id: u32, m: &Message<Body<Value>>, preview: usize) -> InboundMessage {
    let mut props = MessageProperties::default();
    if let Some(p) = &m.properties {
        props.message_id = p.message_id.as_ref().map(message_id_str);
        props.correlation_id = p.correlation_id.as_ref().map(message_id_str);
        props.content_type = p.content_type.as_ref().map(|s| s.as_str().to_string());
        props.content_encoding = p.content_encoding.as_ref().map(|s| s.as_str().to_string());
        props.reply_to = p.reply_to.clone();
        props.kind = p.subject.clone();
        props.timestamp = p.creation_time.as_ref().map(|t| (t.milliseconds() / 1000) as u64);
    }
    if let Some(h) = &m.header {
        props.delivery_mode = Some(if h.durable { 2 } else { 1 });
        props.priority = Some(h.priority.0);
        props.expiration = h.ttl.map(|t| t.to_string());
    }
    if let Some(ap) = &m.application_properties {
        for (k, v) in ap.0.iter() {
            props.headers.insert(k.clone(), simple_to_json(v));
        }
    }
    let mut redelivered = false;
    if let Some(ma) = &m.message_annotations {
        let mut ann = Map::new();
        for (k, v) in ma.0.iter() {
            ann.insert(format!("{k:?}").trim_start_matches("Symbol(Symbol(\"").trim_end_matches("\"))").to_string(), value_to_json(v));
        }
        props.annotations = ann;
    }
    if let Some(h) = &m.header {
        redelivered = h.delivery_count > 0;
    }
    let body = body_bytes(&m.body);
    InboundMessage {
        id: format!("{link}/{delivery_id}"),
        source: DeliverySource::Amqp10 { address: address.to_string(), link: link.to_string(), delivery_id, settled: false },
        exchange: String::new(),
        routing_key: String::new(),
        redelivered,
        properties: props,
        payload: PayloadView::new(&body, preview),
        received_at: now_millis(),
        message_count: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_addresses() {
        assert_eq!(Address::Queue { queue: "a b".into() }.render(), "/queues/a%20b");
        assert_eq!(Address::Exchange { exchange: "x".into(), routing_key: Some("k.1".into()) }.render(), "/exchanges/x/k.1");
        assert_eq!(Address::Exchange { exchange: "x".into(), routing_key: None }.render(), "/exchanges/x");
    }

    #[test]
    fn message_roundtrip() {
        let mut p = MessageProperties::default();
        p.message_id = Some("id-1".into());
        p.content_type = Some("text/plain".into());
        p.headers.insert("n".into(), json!(1));
        p.priority = Some(4);
        let m = build_message(b"hi", &p);
        let back = inbound("l", "/queues/q", 0, &m, 100);
        assert_eq!(back.payload.data, "hi");
        assert_eq!(back.properties.message_id.as_deref(), Some("id-1"));
        assert_eq!(back.properties.headers["n"], 1);
        assert_eq!(back.properties.priority, Some(4));
    }
}
