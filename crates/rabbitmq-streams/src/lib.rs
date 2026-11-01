//! # rabbitmq-streams
//!
//! Native RabbitMQ Stream protocol adapter.
//!
//! Streams are append-only logs. Reading from a stream does not remove or
//! reorder anything, so the operations here are classified as
//! **observation**: [`StreamSession::read`] replays a bounded window starting
//! at an offset or timestamp and returns it, with no settlement step.
//!
//! The adapter deliberately keeps streams separate from the ordinary-queue
//! model in `rabbitmq-amqp091`: there are no delivery tags, acks, or requeues
//! here, only offsets.
//!
//! Supported:
//! * stream / super-stream creation and deletion with retention settings
//! * publishing with confirms, optional named producer (deduplication)
//! * bounded replay from first / last / next / offset / timestamp
//! * server-side filtering (filter values) with client-side post-filter
//! * live subscriptions with batching and bounded buffers
//! * consumer offset tracking (store / query) for named consumers
//! * single-active-consumer for named consumers
//! * metadata (leader / replicas) and super-stream partitions and routes

use chrono::TimeZone;
use futures::StreamExt;
use rabbitmq_core::buffer::BoundedBuffer;
use rabbitmq_core::error::{CoreError, ErrorCode, Result};
use rabbitmq_core::events::{self, names, SharedSink};
use rabbitmq_core::message::*;
use rabbitmq_core::profile::Profile;
use rabbitmq_stream_client::error::{
    ClientError, ConsumerCreateError, ProducerCreateError, ProducerPublishError, StreamCreateError,
    StreamDeleteError,
};
use rabbitmq_stream_client::types::{
    ByteCapacity, Message, OffsetSpecification, ResponseCode, SimpleValue,
};
use rabbitmq_stream_client::{
    Client, ClientOptions, Environment, FilterConfiguration, TlsConfiguration,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Where to start reading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartAt {
    First,
    Last,
    Next,
    Offset {
        offset: u64,
    },
    /// Milliseconds since the Unix epoch. The broker starts at the first chunk
    /// whose timestamp is at or after this time, so a few earlier messages from
    /// the same chunk may be included.
    Timestamp {
        timestamp: i64,
    },
}

impl StartAt {
    fn spec(&self) -> OffsetSpecification {
        match self {
            StartAt::First => OffsetSpecification::First,
            StartAt::Last => OffsetSpecification::Last,
            StartAt::Next => OffsetSpecification::Next,
            StartAt::Offset { offset } => OffsetSpecification::Offset(*offset),
            StartAt::Timestamp { timestamp } => OffsetSpecification::Timestamp(*timestamp),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateStream {
    pub name: String,
    /// Retention by age, e.g. `"7D"`, `"12h"`, `"3600s"`.
    #[serde(default)]
    pub max_age: Option<String>,
    #[serde(default)]
    pub max_length_bytes: Option<u64>,
    #[serde(default)]
    pub max_segment_size_bytes: Option<u64>,
    /// Number of partitions for a super stream; absent for a plain stream.
    #[serde(default)]
    pub partitions: Option<usize>,
    /// Explicit binding keys for super stream partitions.
    #[serde(default)]
    pub binding_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadRequest {
    pub stream: String,
    pub start: StartAt,
    /// Maximum messages to return.
    #[serde(default = "d_max")]
    pub max: usize,
    /// Stop when no message arrives for this long (end of available data).
    #[serde(default = "d_idle")]
    pub idle_ms: u64,
    /// Server-side filter values (requires filter values on publish).
    #[serde(default)]
    pub filter: Option<Vec<String>>,
    #[serde(default)]
    pub match_unfiltered: bool,
}

fn d_max() -> usize {
    100
}
fn d_idle() -> u64 {
    500
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadResult {
    pub stream: String,
    pub messages: Vec<InboundMessage>,
    pub first_offset: Option<u64>,
    pub last_offset: Option<u64>,
    /// True when `max` was reached (more data may follow).
    pub more: bool,
    /// Reading a stream never modifies it.
    pub destructive: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamPublish {
    pub stream: String,
    pub messages: Vec<StreamMessage>,
    /// Named producer: enables broker-side deduplication by publishing id.
    #[serde(default)]
    pub producer_name: Option<String>,
    #[serde(default = "d_timeout")]
    pub timeout_ms: u64,
}

fn d_timeout() -> u64 {
    10_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreamMessage {
    pub payload: Payload,
    #[serde(default)]
    pub properties: MessageProperties,
    /// Explicit publishing id (for deduplication with a named producer).
    #[serde(default)]
    pub publishing_id: Option<u64>,
    /// Filter value for server-side filtering.
    #[serde(default)]
    pub filter_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubscribeRequest {
    pub stream: String,
    pub start: StartAt,
    /// Consumer name: required for offset tracking and single active consumer.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub single_active_consumer: bool,
    #[serde(default)]
    pub filter: Option<Vec<String>>,
    #[serde(default)]
    pub match_unfiltered: bool,
    #[serde(default = "d_flush")]
    pub flush_ms: u64,
    #[serde(default = "d_batch")]
    pub batch: usize,
}

fn d_flush() -> u64 {
    100
}
fn d_batch() -> usize {
    100
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamSubscription {
    pub id: String,
    pub stream: String,
    pub name: Option<String>,
    pub state: String,
    pub delivered: u64,
    pub last_offset: Option<u64>,
    pub paused: bool,
}

struct SubHandle {
    info: Arc<Mutex<StreamSubscription>>,
    task: tokio::task::JoinHandle<()>,
    handle: rabbitmq_stream_client::ConsumerHandle,
    paused: Arc<AtomicBool>,
}

pub struct StreamSession {
    id: String,
    profile: Profile,
    env: Environment,
    sink: SharedSink,
    subs: Mutex<HashMap<String, SubHandle>>,
    seq: AtomicU64,
}

pub fn map_client(e: &ClientError, what: &str) -> CoreError {
    match e {
        ClientError::RequestError(code) => map_code(code, what),
        other => CoreError::new(ErrorCode::ConnectionFailed, format!("{what}: {other}")),
    }
}

fn map_code(code: &ResponseCode, what: &str) -> CoreError {
    let c = match code {
        ResponseCode::StreamDoesNotExist => ErrorCode::NotFound,
        ResponseCode::StreamAlreadyExists => ErrorCode::PreconditionFailed,
        ResponseCode::AccessRefused => ErrorCode::AccessRefused,
        ResponseCode::AuthenticationFailure | ResponseCode::AuthenticationFailureLoopback => {
            ErrorCode::AuthenticationFailed
        }
        ResponseCode::PreconditionFailed => ErrorCode::PreconditionFailed,
        ResponseCode::StreamNotAvailable => ErrorCode::ConnectionFailed,
        ResponseCode::VirtualHostAccessFailure => ErrorCode::AccessRefused,
        _ => ErrorCode::Protocol,
    };
    CoreError::new(c, format!("{what}: {code:?}"))
}

fn map_debug(e: impl std::fmt::Display, what: &str) -> CoreError {
    let s = e.to_string();
    let code = if s.contains("StreamDoesNotExist") {
        ErrorCode::NotFound
    } else if s.contains("AccessRefused") {
        ErrorCode::AccessRefused
    } else if s.contains("Authentication") {
        ErrorCode::AuthenticationFailed
    } else {
        ErrorCode::Protocol
    };
    CoreError::new(code, format!("{what}: {s}"))
}

impl StreamSession {
    pub async fn connect(id: String, profile: Profile, sink: SharedSink) -> Result<Arc<Self>> {
        let mut b = Environment::builder()
            .host(&profile.host)
            .port(profile.stream_port)
            .username(&profile.username)
            .password(profile.password.expose())
            .virtual_host(&profile.vhost)
            .heartbeat(profile.heartbeat as u32)
            .client_provided_name(&format!("{} (stream)", profile.connection_name()));
        if profile.tls.enabled {
            b = b.tls(tls(&profile)?);
        }
        let env = tokio::time::timeout(Duration::from_millis(profile.limits.timeout_ms), b.build())
            .await
            .map_err(|_| CoreError::new(ErrorCode::Timeout, "stream connection timed out").retryable(true))?
            .map_err(|e| {
                map_client(&e, "stream connect").with_hint(format!(
                    "Is the rabbitmq_stream plugin enabled and listening on {}:{}? If the broker advertises a different host, set stream.advertised_host.",
                    profile.host, profile.stream_port
                ))
            })?;
        sink.emit(
            names::SESSION,
            json!({"session": id, "protocol": "stream", "state": "connected", "at": now_millis()}),
        );
        Ok(Arc::new(Self {
            id,
            profile,
            env,
            sink,
            subs: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
        }))
    }

    async fn client(&self) -> Result<Client> {
        let mut o = ClientOptions::builder()
            .host(&self.profile.host)
            .port(self.profile.stream_port)
            .user(&self.profile.username)
            .password(self.profile.password.expose())
            .v_host(&self.profile.vhost)
            .client_provided_name(format!("{} (stream-admin)", self.profile.connection_name()));
        if self.profile.tls.enabled {
            o = o.tls(tls(&self.profile)?);
        }
        Client::connect(o.build())
            .await
            .map_err(|e| map_client(&e, "stream client"))
    }

    // ------------------------------------------------------------ admin

    pub async fn create(&self, c: &CreateStream) -> Result<()> {
        let mut cr = self.env.stream_creator();
        if let Some(age) = &c.max_age {
            cr = cr.max_age(parse_age(age)?);
        }
        if let Some(b) = c.max_length_bytes {
            cr = cr.max_length(ByteCapacity::B(b));
        }
        if let Some(b) = c.max_segment_size_bytes {
            cr = cr.max_segment_size(ByteCapacity::B(b));
        }
        let r = match c.partitions {
            Some(n) => {
                cr.create_super_stream(&c.name, n, c.binding_keys.clone())
                    .await
            }
            None => cr.create(&c.name).await,
        };
        r.map_err(|e| match e {
            StreamCreateError::Create { status, .. } => map_code(&status, "create stream"),
            StreamCreateError::Client(c) => map_client(&c, "create stream"),
        })
    }

    pub async fn delete(&self, name: &str, super_stream: bool) -> Result<()> {
        let r = if super_stream {
            self.env.delete_super_stream(name).await
        } else {
            self.env.delete_stream(name).await
        };
        r.map_err(|e| match e {
            StreamDeleteError::Delete { status, .. } => map_code(&status, "delete stream"),
            StreamDeleteError::Client(c) => map_client(&c, "delete stream"),
        })
    }

    /// Leader / replica placement for streams.
    pub async fn metadata(&self, streams: &[String]) -> Result<Value> {
        let c = self.client().await?;
        let md = c
            .metadata(streams.to_vec())
            .await
            .map_err(|e| map_client(&e, "metadata"));
        let _ = c.close().await;
        let md = md?;
        let mut out = Map::new();
        for (k, m) in md {
            out.insert(
                k,
                json!({
                    "ok": m.response_code == ResponseCode::Ok,
                    "response_code": format!("{:?}", m.response_code),
                    "leader": {"host": m.leader.host, "port": m.leader.port},
                    "replicas": m.replicas.iter().map(|b| json!({"host": b.host, "port": b.port})).collect::<Vec<_>>(),
                }),
            );
        }
        Ok(Value::Object(out))
    }

    pub async fn partitions(&self, super_stream: &str) -> Result<Vec<String>> {
        let c = self.client().await?;
        let r = c
            .partitions(super_stream.to_string())
            .await
            .map_err(|e| map_client(&e, "partitions"));
        let _ = c.close().await;
        let r = r?;
        if !r.is_ok() {
            return Err(CoreError::not_found(format!(
                "super stream {super_stream} not found"
            )));
        }
        Ok(r.streams)
    }

    /// Which partitions a routing key maps to (routing-key strategy).
    pub async fn route(&self, super_stream: &str, routing_key: &str) -> Result<Vec<String>> {
        let c = self.client().await?;
        let r = c
            .route(routing_key.to_string(), super_stream.to_string())
            .await
            .map_err(|e| map_client(&e, "route"));
        let _ = c.close().await;
        Ok(r?.streams)
    }

    pub async fn query_offset(&self, consumer_name: &str, stream: &str) -> Result<Option<u64>> {
        let c = self.client().await?;
        let r = c.query_offset(consumer_name.to_string(), stream).await;
        let _ = c.close().await;
        match r {
            Ok(o) => Ok(Some(o)),
            Err(ClientError::RequestError(ResponseCode::OffsetNotFound)) => Ok(None),
            Err(e) => Err(map_client(&e, "query offset")),
        }
    }

    /// Store a tracked offset for a named consumer. This is the only stream
    /// operation that writes broker state besides publish/create/delete.
    pub async fn store_offset(&self, consumer_name: &str, stream: &str, offset: u64) -> Result<()> {
        let c = self.client().await?;
        let r = c
            .store_offset(consumer_name, stream, offset)
            .await
            .map_err(|e| map_client(&e, "store offset"));
        // store_offset is fire-and-forget in the protocol; query it back so the
        // caller gets a confirmed value rather than an assumption.
        let mut confirmed = None;
        if r.is_ok() {
            for _ in 0..20 {
                if let Ok(v) = c.query_offset(consumer_name.to_string(), stream).await {
                    if v == offset {
                        confirmed = Some(v);
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        let _ = c.close().await;
        r?;
        if confirmed.is_none() {
            return Err(CoreError::new(
                ErrorCode::Timeout,
                "offset stored but not yet visible; result uncertain",
            )
            .ambiguous(true));
        }
        Ok(())
    }

    /// Last publishing id seen by the broker for a named producer.
    pub async fn producer_sequence(&self, producer_name: &str, stream: &str) -> Result<u64> {
        let c = self.client().await?;
        let r = c
            .query_publisher_sequence(producer_name, stream)
            .await
            .map_err(|e| map_client(&e, "query publisher sequence"));
        let _ = c.close().await;
        r
    }

    // ------------------------------------------------------------ publish

    pub async fn publish(&self, req: &StreamPublish) -> Result<Vec<PublishOutcome>> {
        if req.messages.len() > self.profile.limits.max_publish_batch {
            return Err(CoreError::new(
                ErrorCode::LimitExceeded,
                "batch exceeds max_publish_batch",
            ));
        }
        let mut msgs = Vec::with_capacity(req.messages.len());
        let has_filter = req.messages.iter().any(|m| m.filter_value.is_some());
        for (i, m) in req.messages.iter().enumerate() {
            let body = m.payload.to_bytes()?;
            msgs.push(to_stream_message(
                &body,
                &m.properties,
                m.publishing_id,
                m.filter_value.as_deref(),
                i,
            ));
        }
        let timeout = Duration::from_millis(req.timeout_ms);
        let mut builder = self.env.producer().client_provided_name(&format!(
            "{} (stream-producer)",
            self.profile.connection_name()
        ));
        if has_filter {
            builder = builder.filter_value_extractor(|m: &Message| {
                m.application_properties()
                    .and_then(|p| p.get(FILTER_PROP))
                    .and_then(|v| {
                        if let SimpleValue::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default()
            });
        }
        let outcomes = match &req.producer_name {
            Some(name) => {
                let mut p = builder
                    .name(name)
                    .build(&req.stream)
                    .await
                    .map_err(producer_err)?;
                let r = send_all(&mut p, msgs, timeout).await;
                let _ = p.close().await;
                r
            }
            None => {
                let mut p = builder.build(&req.stream).await.map_err(producer_err)?;
                let r = send_all(&mut p, msgs, timeout).await;
                let _ = p.close().await;
                r
            }
        };
        Ok(outcomes)
    }

    // ------------------------------------------------------------ bounded replay

    /// Non-destructive bounded read. Opens a short-lived consumer from `start`,
    /// collects up to `max` messages (stopping when idle), then closes it.
    pub async fn read(&self, r: &ReadRequest) -> Result<ReadResult> {
        let max = r.max.clamp(1, self.profile.limits.max_pull);
        let mut b = self
            .env
            .consumer()
            .offset(r.start.spec())
            .client_provided_name(&format!("{} (stream-read)", self.profile.connection_name()));
        if let Some(f) = &r.filter {
            b = b.filter_input(Some(FilterConfiguration::new(
                f.clone(),
                r.match_unfiltered,
            )));
        }
        let mut consumer = b.build(&r.stream).await.map_err(consumer_err)?;
        let preview = self.profile.limits.max_payload_preview;
        let mut out = Vec::new();
        let idle = Duration::from_millis(r.idle_ms.max(50));
        let overall =
            tokio::time::Instant::now() + Duration::from_millis(self.profile.limits.timeout_ms);
        loop {
            if out.len() >= max {
                break;
            }
            let next = tokio::time::timeout(idle, consumer.next()).await;
            match next {
                Err(_) => break, // idle: reached end of available data
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    events::log(
                        &*self.sink,
                        "warning",
                        "stream",
                        format!("read on {} stopped: {e}", r.stream),
                    );
                    break;
                }
                Ok(Some(Ok(d))) => {
                    if let Some(f) = &r.filter {
                        // Server-side filtering is chunk based (bloom filter): apply
                        // the exact predicate client-side as the broker docs recommend.
                        let fv = filter_value(d.message());
                        let keep = match fv {
                            Some(v) => f.contains(&v),
                            None => r.match_unfiltered,
                        };
                        if !keep {
                            continue;
                        }
                    }
                    out.push(inbound(
                        &self.id,
                        &r.stream,
                        d.offset(),
                        d.message(),
                        preview,
                    ));
                }
            }
            if tokio::time::Instant::now() > overall {
                break;
            }
        }
        let _ = consumer.handle().close().await;
        Ok(ReadResult {
            stream: r.stream.clone(),
            first_offset: out.first().and_then(offset_of),
            last_offset: out.last().and_then(offset_of),
            more: out.len() >= max,
            destructive: false,
            messages: out,
        })
    }

    // ------------------------------------------------------------ live subscriptions

    pub async fn subscribe(self: &Arc<Self>, r: SubscribeRequest) -> Result<StreamSubscription> {
        if self.subs.lock().await.len() >= self.profile.limits.max_consumers {
            return Err(CoreError::new(
                ErrorCode::LimitExceeded,
                "max_consumers reached",
            ));
        }
        if r.single_active_consumer && r.name.is_none() {
            return Err(CoreError::invalid(
                "single_active_consumer requires a consumer name",
            ));
        }
        let mut b = self
            .env
            .consumer()
            .offset(r.start.spec())
            .name_optional(r.name.clone())
            .client_provided_name(&format!("{} (stream-sub)", self.profile.connection_name()));
        if r.single_active_consumer {
            b = b.enable_single_active_consumer(true);
            let me = Arc::downgrade(self);
            let stream = r.stream.clone();
            b = b.consumer_update(move |active, ctx| {
                let me = me.clone();
                let stream = stream.clone();
                async move {
                    if let Some(s) = me.upgrade() {
                        events::log(
                            &*s.sink,
                            "info",
                            "stream",
                            format!(
                                "single-active-consumer {} on {stream} is now {}",
                                ctx.name(),
                                if active == 1 { "active" } else { "inactive" }
                            ),
                        );
                    }
                    // Resume from the last stored offset when becoming active.
                    match ctx.client().query_offset(ctx.name(), &ctx.stream()).await {
                        Ok(o) => OffsetSpecification::Offset(o + 1),
                        Err(_) => OffsetSpecification::First,
                    }
                }
            });
        }
        if let Some(f) = &r.filter {
            b = b.filter_input(Some(FilterConfiguration::new(
                f.clone(),
                r.match_unfiltered,
            )));
        }
        let consumer = b.build(&r.stream).await.map_err(consumer_err)?;
        let id = format!(
            "{}/stream/{}",
            self.id,
            self.seq.fetch_add(1, Ordering::Relaxed)
        );
        let info = Arc::new(Mutex::new(StreamSubscription {
            id: id.clone(),
            stream: r.stream.clone(),
            name: r.name.clone(),
            state: "active".into(),
            delivered: 0,
            last_offset: None,
            paused: false,
        }));
        let handle = consumer.handle();
        let paused = Arc::new(AtomicBool::new(false));
        let buffer = Arc::new(Mutex::new(BoundedBuffer::<InboundMessage>::new(
            self.profile.limits.max_buffered_deliveries,
        )));
        let task = tokio::spawn(sub_loop(
            Arc::downgrade(self),
            consumer,
            r,
            id.clone(),
            info.clone(),
            paused.clone(),
            buffer,
        ));
        let snapshot = info.lock().await.clone();
        self.subs.lock().await.insert(
            id,
            SubHandle {
                info,
                task,
                handle,
                paused,
            },
        );
        Ok(snapshot)
    }

    pub async fn set_paused(&self, id: &str, paused: bool) -> Result<StreamSubscription> {
        let subs = self.subs.lock().await;
        let s = subs
            .get(id)
            .ok_or_else(|| CoreError::not_found(format!("no stream subscription {id}")))?;
        s.paused.store(paused, Ordering::SeqCst);
        let mut i = s.info.lock().await;
        i.paused = paused;
        Ok(i.clone())
    }

    pub async fn unsubscribe(&self, id: &str) -> Result<StreamSubscription> {
        let s = self
            .subs
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| CoreError::not_found(format!("no stream subscription {id}")))?;
        let _ = s.handle.close().await;
        s.task.abort();
        let mut i = s.info.lock().await.clone();
        i.state = "closed".into();
        Ok(i)
    }

    pub async fn subscriptions(&self) -> Vec<StreamSubscription> {
        let subs = self.subs.lock().await;
        let mut v = Vec::new();
        for s in subs.values() {
            v.push(s.info.lock().await.clone());
        }
        v
    }

    pub async fn close(&self) -> Result<()> {
        let subs: Vec<_> = self.subs.lock().await.drain().collect();
        for (_, s) in subs {
            let _ = s.handle.close().await;
            s.task.abort();
        }
        self.sink.emit(names::SESSION, json!({"session": self.id, "protocol": "stream", "state": "closed", "at": now_millis()}));
        Ok(())
    }
}

async fn sub_loop(
    session: std::sync::Weak<StreamSession>,
    mut consumer: rabbitmq_stream_client::Consumer,
    r: SubscribeRequest,
    id: String,
    info: Arc<Mutex<StreamSubscription>>,
    paused: Arc<AtomicBool>,
    buffer: Arc<Mutex<BoundedBuffer<InboundMessage>>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(r.flush_ms.max(10)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let preview = match session.upgrade() {
        Some(s) => s.profile.limits.max_payload_preview,
        None => return,
    };
    loop {
        tokio::select! {
            next = consumer.next() => {
                let Some(s) = session.upgrade() else { return };
                match next {
                    Some(Ok(d)) => {
                        if let Some(f) = &r.filter {
                            let keep = match filter_value(d.message()) { Some(v) => f.contains(&v), None => r.match_unfiltered };
                            if !keep { continue; }
                        }
                        let m = inbound(&s.id, &r.stream, d.offset(), d.message(), preview);
                        {
                            let mut i = info.lock().await;
                            i.delivered += 1;
                            i.last_offset = Some(d.offset());
                        }
                        buffer.lock().await.push(m);
                        if buffer.lock().await.len() >= r.batch && !paused.load(Ordering::Relaxed) {
                            flush(&s, &id, &buffer).await;
                        }
                    }
                    Some(Err(e)) => {
                        events::log(&*s.sink, "error", "stream", format!("subscription {id}: {e}"));
                    }
                    None => {
                        flush(&s, &id, &buffer).await;
                        info.lock().await.state = "closed_by_broker".into();
                        s.sink.emit(names::CONSUMER, json!({"session": s.id, "consumer": id, "state": "closed_by_broker", "at": now_millis()}));
                        return;
                    }
                }
            }
            _ = tick.tick() => {
                if !paused.load(Ordering::Relaxed) {
                    let Some(s) = session.upgrade() else { return };
                    flush(&s, &id, &buffer).await;
                }
            }
        }
    }
}

async fn flush(s: &StreamSession, id: &str, buffer: &Mutex<BoundedBuffer<InboundMessage>>) {
    let (msgs, dropped) = {
        let mut b = buffer.lock().await;
        (b.drain(usize::MAX), b.dropped())
    };
    if !msgs.is_empty() {
        s.sink.emit(names::DELIVERIES, json!({"session": s.id, "consumer": id, "messages": msgs, "dropped": dropped, "protocol": "stream"}));
    }
}

const FILTER_PROP: &str = "x-rabbitmq-el-filter";

fn filter_value(m: &Message) -> Option<String> {
    m.application_properties()
        .and_then(|p| p.get(FILTER_PROP))
        .and_then(|v| match v {
            SimpleValue::String(s) => Some(s.clone()),
            _ => None,
        })
}

/// `send` is defined separately on `Producer<Dedup>` and `Producer<NoDedup>`.
trait SendOne {
    fn send_one(
        &mut self,
        m: Message,
        tx: tokio::sync::mpsc::Sender<(
            usize,
            std::result::Result<rabbitmq_stream_client::ConfirmationStatus, ProducerPublishError>,
        )>,
        i: usize,
    ) -> impl std::future::Future<Output = std::result::Result<(), ProducerPublishError>> + Send;
}

macro_rules! impl_send_one {
    ($t:ty) => {
        impl SendOne for rabbitmq_stream_client::Producer<$t> {
            async fn send_one(
                &mut self,
                m: Message,
                tx: tokio::sync::mpsc::Sender<(
                    usize,
                    std::result::Result<
                        rabbitmq_stream_client::ConfirmationStatus,
                        ProducerPublishError,
                    >,
                )>,
                i: usize,
            ) -> std::result::Result<(), ProducerPublishError> {
                self.send(m, move |res| async move {
                    let _ = tx.send((i, res)).await;
                })
                .await
            }
        }
    };
}
impl_send_one!(rabbitmq_stream_client::Dedup);
impl_send_one!(rabbitmq_stream_client::NoDedup);

async fn send_all<P: SendOne>(
    p: &mut P,
    msgs: Vec<Message>,
    timeout: Duration,
) -> Vec<PublishOutcome> {
    let n = msgs.len();
    let (tx, mut rx) = tokio::sync::mpsc::channel(n.max(1));
    let mut out: Vec<Option<PublishOutcome>> = vec![None; n];
    for (i, m) in msgs.into_iter().enumerate() {
        let r = p.send_one(m, tx.clone(), i).await;
        if let Err(e) = r {
            let status = if matches!(e, ProducerPublishError::Closed) {
                PublishStatus::Failed
            } else {
                PublishStatus::Uncertain
            };
            out[i] = Some(PublishOutcome {
                index: i,
                status,
                reply_code: None,
                reply_text: None,
                error: Some(e.to_string()),
            });
        }
    }
    drop(tx);
    let deadline = tokio::time::Instant::now() + timeout;
    while out.iter().any(Option::is_none) {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some((i, res))) => {
                out[i] = Some(match res {
                    Ok(c) if c.confirmed() => PublishOutcome {
                        index: i,
                        status: PublishStatus::Confirmed,
                        reply_code: None,
                        reply_text: None,
                        error: None,
                    },
                    Ok(c) => PublishOutcome {
                        index: i,
                        status: PublishStatus::Nacked,
                        reply_code: None,
                        reply_text: Some(format!("{:?}", c.status())),
                        error: None,
                    },
                    Err(e) => PublishOutcome {
                        index: i,
                        status: PublishStatus::Uncertain,
                        reply_code: None,
                        reply_text: None,
                        error: Some(e.to_string()),
                    },
                });
            }
            Ok(None) | Err(_) => break,
        }
    }
    out.into_iter()
        .enumerate()
        .map(|(i, o)| {
            o.unwrap_or(PublishOutcome {
                index: i,
                status: PublishStatus::Uncertain,
                reply_code: None,
                reply_text: None,
                error: Some("no confirmation before timeout".into()),
            })
        })
        .collect()
}

fn producer_err(e: ProducerCreateError) -> CoreError {
    match e {
        ProducerCreateError::Create { status, .. } => map_code(&status, "create producer"),
        ProducerCreateError::StreamDoesNotExist { stream } => {
            CoreError::not_found(format!("stream {stream} does not exist"))
        }
        ProducerCreateError::Client(c) => map_client(&c, "create producer"),
        other => map_debug(other, "create producer"),
    }
}

fn consumer_err(e: ConsumerCreateError) -> CoreError {
    match e {
        ConsumerCreateError::Create { status, .. } => map_code(&status, "create consumer"),
        ConsumerCreateError::StreamDoesNotExist { stream } => {
            CoreError::not_found(format!("stream {stream} does not exist"))
        }
        ConsumerCreateError::Client(c) => map_client(&c, "create consumer"),
        other => map_debug(other, "create consumer"),
    }
}

fn offset_of(m: &InboundMessage) -> Option<u64> {
    match &m.source {
        DeliverySource::Stream { offset, .. } => Some(*offset),
        _ => None,
    }
}

/// Parse a retention age like `7D`, `12h`, `30m`, `3600s`, `1Y`, `2M`.
pub fn parse_age(s: &str) -> Result<Duration> {
    if !s.is_ascii() || s.len() < 2 {
        return Err(CoreError::invalid("invalid max_age"));
    }
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let n: u64 = num
        .parse()
        .map_err(|_| CoreError::invalid(format!("invalid max_age \"{s}\"")))?;
    let multiplier = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "D" => 86_400,
        "M" => 30 * 86_400,
        "Y" => 365 * 86_400,
        _ => {
            return Err(CoreError::invalid(format!(
                "invalid max_age unit in \"{s}\" (use s, m, h, D, M, Y)"
            )))
        }
    };
    let secs = n
        .checked_mul(multiplier)
        .ok_or_else(|| CoreError::invalid("max_age exceeds supported duration"))?;
    Ok(Duration::from_secs(secs))
}

fn tls(p: &Profile) -> Result<TlsConfiguration> {
    let mut b = TlsConfiguration::builder().enable(true);
    if let Some(ca) = &p.tls.ca_file {
        b = b.add_root_certificates(ca.clone());
    }
    if let (Some(c), Some(k)) = (&p.tls.client_cert_file, &p.tls.client_key_file) {
        b = b.add_client_certificates_keys(c.clone(), k.clone());
    }
    b.build()
        .map_err(|e| CoreError::invalid(format!("stream TLS: {e}")))
}

fn to_stream_message(
    body: &[u8],
    p: &MessageProperties,
    publishing_id: Option<u64>,
    filter: Option<&str>,
    _i: usize,
) -> Message {
    let mut b = Message::builder().body(body.to_vec());
    if let Some(id) = publishing_id {
        b = b.publishing_id(id);
    }
    let mut pb = b.properties();
    if let Some(v) = &p.message_id {
        pb = pb.message_id(v.clone());
    }
    if let Some(v) = &p.correlation_id {
        pb = pb.correlation_id(v.clone());
    }
    if let Some(v) = &p.content_type {
        pb = pb.content_type(v.as_str());
    }
    if let Some(v) = &p.content_encoding {
        pb = pb.content_encoding(v.as_str());
    }
    if let Some(v) = &p.reply_to {
        pb = pb.reply_to(v.as_str());
    }
    if let Some(v) = &p.user_id {
        pb = pb.user_id(v.as_bytes().to_vec());
    }
    if let Some(v) = &p.kind {
        pb = pb.subject(v.as_str());
    }
    if let Some(ts) = p.timestamp {
        if let Some(dt) = chrono::Utc.timestamp_opt(ts as i64, 0).single() {
            pb = pb.creation_time(dt);
        }
    }
    let mut ab = pb.message_builder().application_properties();
    for (k, v) in &p.headers {
        if let Some(sv) = json_to_simple(v) {
            ab = ab.insert(k.as_str(), sv);
        }
    }
    if let Some(f) = filter {
        ab = ab.insert(FILTER_PROP, f);
    }
    ab.message_builder().build()
}

fn json_to_simple(v: &Value) -> Option<SimpleValue> {
    Some(match v {
        Value::Null => SimpleValue::Null,
        Value::Bool(b) => SimpleValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SimpleValue::Long(i)
            } else {
                SimpleValue::Double(n.as_f64()?.into())
            }
        }
        Value::String(s) => SimpleValue::String(s.clone()),
        _ => SimpleValue::String(v.to_string()),
    })
}

fn simple_to_json(v: &SimpleValue) -> Value {
    match v {
        SimpleValue::Null => Value::Null,
        SimpleValue::Boolean(b) => json!(b),
        SimpleValue::Ubyte(n) => json!(n),
        SimpleValue::Ushort(n) => json!(n),
        SimpleValue::Uint(n) => json!(n),
        SimpleValue::Ulong(n) => json!(n),
        SimpleValue::Byte(n) => json!(n),
        SimpleValue::Short(n) => json!(n),
        SimpleValue::Int(n) => json!(n),
        SimpleValue::Long(n) => json!(n),
        SimpleValue::Char(c) => json!(c.to_string()),
        SimpleValue::Uuid(u) => json!({"$type": "uuid", "value": u.to_string()}),
        SimpleValue::Binary(b) => json!({"$type": "bytes", "base64": encode_b64(b)}),
        SimpleValue::String(s) => json!(s),
        SimpleValue::Symbol(s) => json!({"$type": "symbol", "value": s.to_string()}),
        // Float, Double, and Timestamp do not expose their inner values;
        // fall back to their Debug rendering so nothing is silently dropped.
        other => json!({"$type": "debug", "value": format!("{other:?}")}),
    }
}

fn inbound(
    session: &str,
    stream: &str,
    offset: u64,
    m: &Message,
    preview: usize,
) -> InboundMessage {
    let body = m.data().unwrap_or_default();
    let mut props = MessageProperties::default();
    if let Some(p) = m.properties() {
        props.message_id = p
            .message_id
            .as_ref()
            .map(|id| format!("{id:?}"))
            .map(strip_variant);
        props.correlation_id = p
            .correlation_id
            .as_ref()
            .map(|id| format!("{id:?}"))
            .map(strip_variant);
        props.content_type = p.content_type.as_ref().map(|s| s.to_string());
        props.content_encoding = p.content_encoding.as_ref().map(|s| s.to_string());
        props.reply_to = p.reply_to.clone();
        props.kind = p.subject.clone();
        props.user_id = p
            .user_id
            .as_ref()
            .map(|u| String::from_utf8_lossy(u).into_owned());
    }
    if let Some(ap) = m.application_properties() {
        for (k, v) in ap.iter() {
            if k == FILTER_PROP {
                props
                    .annotations
                    .insert("filter_value".into(), simple_to_json(v));
                continue;
            }
            props.headers.insert(k.clone(), simple_to_json(v));
        }
    }
    if let Some(id) = m.publishing_id() {
        props.annotations.insert("publishing_id".into(), json!(id));
    }
    InboundMessage {
        id: format!("{session}/stream/{stream}/{offset}"),
        source: DeliverySource::Stream {
            stream: stream.to_string(),
            offset,
            chunk_timestamp: None,
        },
        exchange: String::new(),
        routing_key: String::new(),
        redelivered: false,
        properties: props,
        payload: PayloadView::new(body, preview),
        received_at: now_millis(),
        message_count: None,
    }
}

/// `String("abc")` -> `abc`, `ULong(5)` -> `5`.
fn strip_variant(s: String) -> String {
    match (s.find('('), s.rfind(')')) {
        (Some(a), Some(b)) if b > a => s[a + 1..b].trim_matches('"').to_string(),
        _ => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages() {
        assert_eq!(parse_age("7D").unwrap(), Duration::from_secs(7 * 86400));
        assert_eq!(parse_age("90s").unwrap(), Duration::from_secs(90));
        assert!(parse_age("7days").is_err());
        assert!(parse_age("D").is_err());
        assert!(parse_age("é").is_err());
        assert!(parse_age("18446744073709551615Y").is_err());
    }

    #[test]
    fn message_roundtrip_properties() {
        let mut p = MessageProperties::default();
        p.message_id = Some("m-1".into());
        p.correlation_id = Some("c-1".into());
        p.content_type = Some("application/json".into());
        p.headers.insert("n".into(), json!(5));
        p.headers.insert("s".into(), json!("x"));
        let m = to_stream_message(b"{}", &p, Some(9), Some("eu"), 0);
        let back = inbound("s", "st", 3, &m, 100);
        assert_eq!(back.properties.message_id.as_deref(), Some("m-1"));
        assert_eq!(back.properties.correlation_id.as_deref(), Some("c-1"));
        assert_eq!(back.properties.headers["n"], 5);
        assert_eq!(back.properties.annotations["filter_value"], "eu");
        assert_eq!(back.properties.annotations["publishing_id"], 9);
        assert_eq!(filter_value(&m).as_deref(), Some("eu"));
        assert_eq!(back.id, "s/stream/st/3");
    }

    #[test]
    fn start_specs() {
        let s: StartAt = serde_json::from_value(json!({"type": "offset", "offset": 5})).unwrap();
        assert!(matches!(s.spec(), OffsetSpecification::Offset(5)));
        let s: StartAt =
            serde_json::from_value(json!({"type": "timestamp", "timestamp": 1})).unwrap();
        assert!(matches!(s.spec(), OffsetSpecification::Timestamp(1)));
    }
}
